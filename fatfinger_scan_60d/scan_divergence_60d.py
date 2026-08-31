#!/usr/bin/env python3
"""
scan_divergence_60d.py —— 1m 跨腿 divergence 预筛（报价背离型盲区覆盖）
目的：抓"基础资产 1h 单腿 wick<5% 但两腿瞬时背离>=5%"的盲区（如 SNX 08-28 07:00
      单腿 wick 仅 2.9%/2.1%，但 USDT/USDC 报价瞬间背离 4.93%）。
      主体 scan_1h_wicks 用 1h 粒度，会把这种瞬态平均掉，抓不到。1m 粒度可暴露。
方法：对每个有>=2条稳定腿的 base，拉每腿 1m K线(60天分页)，按分钟 open_time 对齐，
      算 USDT 腿 vs 每条其他腿的 close 价偏离 div=|p_usdt-p_other|/min*100，
      取该 base 全窗口最大 div；若 >= MIN_DIV 记候选（anchor_ms=该分钟）。
用法：python3 scan_divergence_60d.py [天数]  默认60
输出：divergence_candidates.json（[{base,leg,anchor_ms,anchor_iso,div_pct}...])
"""
import json, os, time, subprocess, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
PROXY = os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:7897"
CACHE = "klines_1m_60d"
os.makedirs(CACHE, exist_ok=True)

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 60
NOW = int(datetime.now(timezone.utc).timestamp() * 1000)
START = NOW - DAYS * 24 * 3600 * 1000
M1 = 60 * 1000
MIN_DIV = 4.0  # 预筛门槛（<5%真针门槛，留余量不漏 4.93% 类）


def ensure(sym):
    """拉或复用 1m 缓存；返回 (err_or_None, n_bars)。"""
    fn = f"{CACHE}/{sym}.json"
    if os.path.exists(fn):
        try:
            d = json.load(open(fn))
            if isinstance(d, list) and d and d[0][0] <= START and d[-1][0] >= NOW - M1:
                return None, len(d)
        except Exception:
            pass
    allb = []
    cur = START
    pages = 0
    while cur < NOW:
        url = (f"https://api.binance.com/api/v3/klines?symbol={sym}"
               f"&interval=1m&startTime={cur}&endTime={NOW}&limit=1000")
        cmd = ["curl", "-s", "--max-time", "25", "-x", PROXY, url]
        try:
            raw = subprocess.check_output(cmd, timeout=30)
            data = json.loads(raw)
        except Exception as e:
            return f"err {sym}: {e}", 0
        if not data or isinstance(data, dict):
            break
        allb.extend(data)
        if len(data) < 1000:
            break
        cur = data[-1][0] + M1
        pages += 1
        if pages >= 120:  # 60天1m约87页，留安全余量
            break
    if not allb:
        return f"empty {sym}", 0
    seen = {}
    for k in allb:
        seen[int(k[0])] = k
    allb = [seen[k] for k in sorted(seen)]
    json.dump(allb, open(fn, "w"))
    return None, len(allb)


def main():
    targets = json.load(open("targets.json"))
    # 仅保留有>=2条腿的 base
    multi = [t for t in targets if len(t["legs"]) >= 2]
    print(f"multi-leg bases: {len(multi)} ({DAYS}天, START={datetime.fromtimestamp(START/1000,timezone.utc).date()} NOW={datetime.fromtimestamp(NOW/1000,timezone.utc).date()})", flush=True)

    # 1) 并行拉取所有相关腿的 1m
    all_legs = []
    for t in multi:
        for q in t["legs"]:
            all_legs.append(t["base"] + q)
    # 去重
    all_legs = list(dict.fromkeys(all_legs))
    print(f"total 1m leg pulls: {len(all_legs)}", flush=True)
    cache = {}
    errs = []
    t0 = time.time()
    # 串行拉取（Windows 下多线程 subprocess 调 curl 会死锁，单发已验证正常）
    done = 0
    for s in all_legs:
        err, n = ensure(s)
        if err:
            errs.append(err)
        else:
            cache[s] = json.load(open(f"{CACHE}/{s}.json"))
        done += 1
        if done % 10 == 0:
            print(f"  pull {done}/{len(all_legs)} cost={time.time()-t0:.0f}s", flush=True)

    # 2) 逐 base 算最大跨腿 divergence
    cands = []
    for t in multi:
        base = t["base"]
        legs = t["legs"]
        if "USDT" not in legs:
            continue
        usdt = {k[0]: k for k in cache.get(base + "USDT", [])}
        if not usdt:
            continue
        for q in legs:
            if q == "USDT":
                continue
            other = {k[0]: k for k in cache.get(base + q, [])}
            if not other:
                continue
            best = 0.0
            best_t = None
            for tk, bar in usdt.items():
                if tk in other:
                    # 用两腿 high/low 极值组合捕捉"秒级插针留在 1m high/low 的痕迹"
                    # （close 价会把瞬态平均掉，抓不到 SNX 08-28 类盲区）
                    hu = float(bar[2]); lu = float(bar[3]); cu = float(bar[4])
                    ho = float(other[tk][2]); lo = float(other[tk][3]); co = float(other[tk][4])
                    ref = min(hu, lu, ho, lo)
                    if ref > 0:
                        div = max(abs(hu - lo), abs(lu - ho), abs(hu - ho), abs(lu - lo)) / ref * 100
                        if div > best:
                            best = div
                            best_t = tk
            if best >= MIN_DIV and best_t is not None:
                iso = datetime.fromtimestamp(best_t / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                cands.append({"base": base, "leg": q, "anchor_ms": best_t,
                              "anchor_iso": iso, "div_pct": round(best, 2)})

    cands.sort(key=lambda c: c["div_pct"], reverse=True)
    json.dump(cands, open("divergence_candidates.json", "w"), ensure_ascii=False, indent=1)
    print(f"\nDONE divergence candidates (>= {MIN_DIV}%): {len(cands)}  pull_errs={len(errs)}", flush=True)
    for c in cands[:20]:
        print(f"  {c['base']}{c['leg']} div={c['div_pct']}% @{c['anchor_iso']}", flush=True)
    if errs:
        print("ERRORS sample:", errs[:5], flush=True)


if __name__ == "__main__":
    main()
