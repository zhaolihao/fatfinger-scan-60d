#!/usr/bin/env python3
"""
scan_divergence_focus.py —— 聚焦版 1m 跨腿 divergence 盲区预筛（近 14 天）
优化点（相对 60d 全量版）：
  1. 固定绝对窗口 START~END（不随 NOW 漂移，缓存可稳定复用，不重拉）
  2. 只扫 verify 真/弱针出现过的「活跃 base」（盲区只发生在有成交的活跃对）
  3. requests 并发拉取（避免 subprocess curl 多线程死锁）
  4. 复用 klines_1m_60d 现有 86400 根全量缓存（仅补尾部缺段）
方法：每 base 取 USDT + 其他活跃腿，按分钟对齐，用两腿 high/low 极值组合算跨腿偏离
      div = max(|hu-lo|,|lu-ho|,|hu-ho|,|lu-ho|)/min(hu,lu,ho,lo)*100
      >= MIN_DIV(4%) 记候选（盲区：1h 单腿 wick<5% 但秒级跨腿>=5%）
输出：divergence_candidates.json
"""
import json, os, time, sys
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
PROXY = os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:7897"
os.environ["HTTPS_PROXY"] = PROXY
os.environ["HTTP_PROXY"] = PROXY
CACHE = "klines_1m_60d"
os.makedirs(CACHE, exist_ok=True)
M1 = 60 * 1000
# 固定 14 天窗口：2026-08-16 00:00Z ~ 2026-08-30 12:00Z（覆盖已知 SNX 08-22 盲区）
START = int(datetime(2026, 8, 16, tzinfo=timezone.utc).timestamp() * 1000)
END = int(datetime(2026, 8, 30, 12, tzinfo=timezone.utc).timestamp() * 1000)
MIN_DIV = 4.0
WORKERS = 8


def fetch_klines(sym, start, end):
    bars = []
    cur = start
    while cur < end:
        url = (f"https://api.binance.com/api/v3/klines?symbol={sym}"
               f"&interval=1m&startTime={cur}&endTime={end}&limit=1000")
        ok = False
        for _ in range(4):
            try:
                r = requests.get(url, proxies={"https": PROXY, "http": PROXY}, timeout=20)
                data = r.json()
                if isinstance(data, dict):
                    return bars
                if not data:
                    return bars
                bars.extend(data)
                cur = data[-1][0] + M1
                ok = True
                if len(data) < 1000:
                    return bars
                break
            except Exception:
                time.sleep(2)
        if not ok:
            return bars
    return bars


def ensure(sym):
    """返回 [START,END] 窗口内的 1m 棒；复用全量缓存，不足补拉。"""
    fn = f"{CACHE}/{sym}.json"
    d = None
    if os.path.exists(fn):
        try:
            d = json.load(open(fn))
            if d and d[0][0] <= START and d[-1][0] >= END - M1:
                return [k for k in d if START <= k[0] <= END]
        except Exception:
            d = None
    new = fetch_klines(sym, START if d is None else d[-1][0] + M1, END)
    merged = (d or []) + new
    seen = {}
    for k in merged:
        seen[int(k[0])] = k
    merged = [seen[k] for k in sorted(seen)]
    try:
        json.dump(merged, open(fn, "w"))
    except Exception:
        pass
    return [k for k in merged if START <= k[0] <= END]


def main():
    targets = json.load(open("targets.json"))
    tmap = {t["base"]: t["legs"] for t in targets}
    # 活跃 base 集：verify 真/弱针出现过的
    try:
        vr = json.load(open("verify_results_full.json"))
        active = set(x["base"] for x in vr)
    except Exception:
        active = None
    multi = [t for t in targets if len(t["legs"]) >= 2]
    if active:
        multi = [t for t in multi if t["base"] in active]
    print(f"活跃多腿 base: {len(multi)} (窗口 08-16~08-30 UTC, MIN_DIV={MIN_DIV}%)", flush=True)

    all_legs = []
    for t in multi:
        for q in t["legs"]:
            if q in ("USDC", "USDT", "FDUSD", "USD1"):
                all_legs.append(t["base"] + q)
    all_legs = list(dict.fromkeys(all_legs))
    print(f"需拉 1m 腿数: {len(all_legs)}", flush=True)

    cache = {}
    errs = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(ensure, s): s for s in all_legs}
        done = 0
        for f in as_completed(futs):
            s = futs[f]
            try:
                cache[s] = f.result()
            except Exception as e:
                errs.append(f"{s}:{e}")
            done += 1
            if done % 50 == 0:
                print(f"  pull {done}/{len(all_legs)} cost={time.time()-t0:.0f}s", flush=True)

    cands = []
    for t in multi:
        base = t["base"]
        legs = [q for q in t["legs"] if q in ("USDC", "USDT", "FDUSD", "USD1")]
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
                    hu = float(bar[2]); lu = float(bar[3])
                    ho = float(other[tk][2]); lo = float(other[tk][3])
                    ref = min(hu, lu, ho, lo)
                    if ref > 0:
                        div = max(abs(hu - lo), abs(lu - ho),
                                  abs(hu - ho), abs(lu - ho)) / ref * 100
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
    for c in cands[:30]:
        print(f"  {c['base']}{c['leg']} div={c['div_pct']}% @{c['anchor_iso']}", flush=True)
    if errs:
        print("ERRORS sample:", errs[:5], flush=True)


if __name__ == "__main__":
    main()
