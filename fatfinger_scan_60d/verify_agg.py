#!/usr/bin/env python3
"""
verify_agg.py —— 全市场候选 aggTrades 跨腿对齐终极判真假（skill §3.1/§3.2）
- 读取 drill_full.json（带 anchor_ms）
- 对每条候选：拉 发散腿(leg) + 该 base 其他稳定腿 的 ±90s aggTrades，严格 Δt<=500ms 对齐
- 取所有腿对的最大跨腿价差 max_spread 作为判定：
    >= TRUTH(5%)  -> 真乌龙指
    3%~5%         -> 弱（疑似）
  其余 -> 弱/死盘
输出：verify_results_full.json + 终端汇总
增量持久化：每条验证完即 append 到 verify_results_full.jsonl，进程被回收也不丢进度。
"""
import json, os, subprocess, statistics, time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
CACHE = "aggTrades_90s"
os.makedirs(CACHE, exist_ok=True)
PROXY = os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:7897"
DT_MAX = 500
TRUTH = 5.0
ALL_Q = ("USDT", "USDC", "FDUSD", "USD1")


def fetch_agg(sym, s, e):
    """拉 [s,e] 窗口内全部 aggTrades（分页拉满，避免 limit=1000 截断薄币窗口漏掉针）。
    缓存策略：仅当缓存已覆盖到窗口末尾(e-2s)才复用，否则重拉。"""
    fn = os.path.join(CACHE, f"{sym}_{s}_{e}.json")
    if os.path.exists(fn) and os.path.getsize(fn) > 100:
        try:
            d = json.load(open(fn))
            if isinstance(d, list) and d and d[-1]["T"] >= e - 2000:
                return d
        except Exception:
            pass
    allt = []
    cur = s
    pages = 0
    while True:
        url = (f"https://api.binance.com/api/v3/aggTrades?symbol={sym}"
               f"&startTime={cur}&endTime={e}&limit=1000")
        cmd = ["curl", "-s", "--max-time", "20", "-x", PROXY, url]
        data = None
        for _ in range(3):
            try:
                raw = subprocess.check_output(cmd, timeout=25)
                data = json.loads(raw)
                break
            except Exception:
                time.sleep(1.5)
        if not isinstance(data, list) or not data:
            break
        allt.extend(data)
        if len(data) < 1000:
            break
        last = data[-1]["T"]
        if last >= e - 1:
            break
        cur = last + 1
        pages += 1
        if pages >= 25:   # 上限 25k 笔，足够覆盖 90s 窗口
            break
    if allt:
        json.dump(allt, open(fn, "w"))
    return allt


def cross_align(a, b):
    spreads = []
    for x in a:
        for y in b:
            dt = abs(x["T"] - y["T"])
            if dt > DT_MAX:
                continue
            sp = (float(x["p"]) - float(y["p"])) / float(y["p"]) * 100
            spreads.append(abs(sp))
    if not spreads:
        return None
    ss = sorted(spreads)
    n = len(ss)
    return {"n": n, "max": max(ss), "p90": ss[min(n - 1, int(n * 0.9))],
            "med": statistics.median(ss)}


def main():
    cands = json.load(open("drill_full.json"))
    # 只终审「孤立尖针」候选：乌龙指=单根K线异常尖针，非缓慢/联动波动
    rows = [c for c in cands if c.get("anchor_ms") and c.get("single_needle")]
    rows.sort(key=lambda x: -x["cross_leg_excess"])

    # 增量持久化：每条验证完即 append 到 jsonl，进程被回收也不丢进度
    JSONL = "verify_results_full.jsonl"
    done = set()
    results = []
    if os.path.exists(JSONL):
        try:
            with open(JSONL, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line)
                    except Exception:
                        continue
                    results.append(r)
                    if r.get("anchor_ms"):
                        done.add((r["base"], r["leg"], r["anchor_ms"]))
        except Exception:
            pass

    pending = [c for c in rows if (c["base"], c["leg"], c["anchor_ms"]) not in done]
    print(f"=== 总 {len(rows)} 条, 已完成 {len(done)}, 待验证 {len(pending)} ===", flush=True)
    print(f"{'#':>3} {'base':6} {'leg':4} {'date':15} {'max%':>7} {'p90%':>6} {'n':>6} {'verdict':<10}", flush=True)

    real = weak = 0
    for r in results:
        if r.get("verdict") == "真":
            real += 1
        elif r.get("verdict") == "弱":
            weak += 1
    out = open(JSONL, "a", encoding="utf-8", buffering=1)
    start_idx = len(results)
    for i, c in enumerate(pending):
        s = c["anchor_ms"] - 90 * 1000
        e = c["anchor_ms"] + 90 * 1000
        base = c["base"]
        leg = c["leg"]
        legs_avail = [q for q in ALL_Q if os.path.exists(
            os.path.join(HERE, "klines_1h", f"{base}{q}.json"))]
        if leg not in legs_avail:
            legs_avail = [leg] + legs_avail
        a1 = fetch_agg(base + leg, s, e)
        best = None
        best_q = None
        for q in legs_avail:
            if q == leg:
                continue
            a2 = fetch_agg(base + q, s, e)
            if not a2:
                continue
            st = cross_align(a1, a2)
            if st and (best is None or st["max"] > best["max"]):
                best = st
                best_q = q
        if not best:
            v = "死盘/无成交"
            mx = p90 = n = 0
        else:
            mx = best["max"]
            p90 = best["p90"]
            n = best["n"]
            if mx >= TRUTH:
                v = "真"; real += 1
            elif mx >= 3.0:
                v = "弱"; weak += 1
            else:
                v = "弱"
        iso = c.get("anchor_iso") or "?"
        print(f"{start_idx+i+1:3} {base:6} {leg:4} {iso:15} {mx:7.2f} {p90:6.2f} {n:6} {v:<10}", flush=True)
        r = {**c, "verify_q": best_q, "peak_pct": round(mx, 3),
             "p90_pct": round(p90, 3), "n_pairs": n, "verdict": v}
        results.append(r)
        out.write(json.dumps(r, ensure_ascii=False) + "\n")
        out.flush()

    # 无 anchor_ms 候选（确定性，直接补）
    for c in cands:
        if not c.get("anchor_ms"):
            r = {**c, "verify_q": None, "peak_pct": 0,
                 "p90_pct": 0, "n_pairs": 0, "verdict": "无1m锚点"}
            results.append(r)
            out.write(json.dumps(r, ensure_ascii=False) + "\n")
            out.flush()
    out.close()

    # 合并去重落最终 json
    seen = set()
    final = []
    for r in results:
        key = (r.get("base"), r.get("leg"), r.get("anchor_ms"))
        if key in seen:
            continue
        seen.add(key)
        final.append(r)
    json.dump(final, open("verify_results_full.json", "w"), ensure_ascii=False)
    print(f"\n真乌龙指 (max>={TRUTH}%): {real}   弱(3~5%): {weak}   共{len(final)}", flush=True)
    print("saved verify_results_full.json", flush=True)


if __name__ == "__main__":
    main()
