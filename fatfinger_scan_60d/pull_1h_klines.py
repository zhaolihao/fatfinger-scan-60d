#!/usr/bin/env python3
"""
pull_1h_klines.py —— 拉全部候选 base 的各稳定腿 1h K 线（过去 N 天，默认 60）
- 读取 targets.json（全市场跨腿 base）
- 每个 base 拉其 legs 里全部稳定腿（USDT/USDC/FDUSD/USD1）
- 分页拉满：60天=1440根 1h 棒 > limit=1000 上限，必须翻页（否则被截断到约42天）
- 断点续跑：已缓存且覆盖完整 [START, NOW] 窗口则跳过
用法：python3 pull_1h_klines.py [天数]   （默认 60）
"""
import json, os, time, subprocess, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
PROXY = os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:7897"
CACHE = "klines_1h"
os.makedirs(CACHE, exist_ok=True)

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 60
NOW = int(datetime.now(timezone.utc).timestamp() * 1000)
START = NOW - DAYS * 24 * 3600 * 1000
H1 = 3600 * 1000


def fetch_one(args):
    base, quote = args
    sym = base + quote
    fn = f"{CACHE}/{sym}.json"
    # 断点续跑：已缓存且覆盖完整窗口才跳过
    if os.path.exists(fn):
        try:
            d = json.load(open(fn))
            if isinstance(d, list) and d and d[0][0] <= START and d[-1][0] >= NOW - H1:
                return f"skip {sym} ({len(d)}bars)"
        except Exception:
            pass
    # 分页拉满 [START, NOW]
    allb = []
    cur = START
    pages = 0
    while cur < NOW:
        url = (f"https://api.binance.com/api/v3/klines?symbol={sym}"
               f"&interval=1h&startTime={cur}&endTime={NOW}&limit=1000")
        cmd = ["curl", "-s", "--max-time", "25", "-x", PROXY, url]
        try:
            raw = subprocess.check_output(cmd, timeout=30)
            data = json.loads(raw)
        except Exception as e:
            return f"err {sym}: {e}"
        if not data or isinstance(data, dict):
            break
        allb.extend(data)
        if len(data) < 1000:
            break
        cur = data[-1][0] + H1
        pages += 1
        if pages >= 40:   # 安全上限（60天仅约2页）
            break
    if not allb:
        return f"empty {sym}"
    # 去重按 open_time
    seen = {}
    for k in allb:
        seen[int(k[0])] = k
    allb = [seen[k] for k in sorted(seen)]
    json.dump(allb, open(fn, "w"))
    return f"ok {sym} {len(allb)}bars"


def main():
    targets = json.load(open("targets.json"))
    legs = []
    for t in targets:
        for q in t["legs"]:
            legs.append((t["base"], q))
    print(f"total leg requests: {len(legs)} ({DAYS}天, START={datetime.fromtimestamp(START/1000,timezone.utc).date()} NOW={datetime.fromtimestamp(NOW/1000,timezone.utc).date()})", flush=True)
    t0 = time.time()
    ok = err = skip = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(fetch_one, a) for a in legs]
        for i, f in enumerate(as_completed(futs), 1):
            r = f.result()
            if r.startswith("ok "): ok += 1
            elif r.startswith("skip "): skip += 1
            else: err += 1; print(r, flush=True)
            if i % 50 == 0:
                print(f"  progress {i}/{len(legs)} ok={ok} err={err} skip={skip} cost={time.time()-t0:.0f}s", flush=True)
    print(f"\nDONE ok={ok} skip={skip} err={err} cost={time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
