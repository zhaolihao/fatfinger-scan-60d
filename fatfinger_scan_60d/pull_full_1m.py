#!/usr/bin/env python3
"""pull_full_1m.py —— 为前兆分析拉取真乌龙指涉及币对的 10 天 1m 全量历史
缓存到 klines_full/{SYM}.json（多页拼接）；用于前兆窗口 + 对照基线。
"""
import json, os, subprocess, time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
PROXY = os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:7897"
OUT = "klines_full"
os.makedirs(OUT, exist_ok=True)

rows = json.load(open("verify_results_full.json"))
true = [r for r in rows if r["verdict"] == "真"]
syms = sorted(set(r["base"] + r["leg"] for r in true))
print(f"需拉取唯一币对: {len(syms)}", flush=True)

# 全局扫描窗口（与报告一致）
START = int(datetime(2026, 8, 19, 12, 46, tzinfo=timezone.utc).timestamp() * 1000)
END = int(datetime(2026, 8, 29, 12, 46, tzinfo=timezone.utc).timestamp() * 1000)


def pull_sym(sym):
    fn = os.path.join(OUT, f"{sym}.json")
    if os.path.exists(fn) and os.path.getsize(fn) > 1000:
        return f"skip {sym}"
    allbars = []
    cur = START
    pages = 0
    while cur < END and pages < 40:
        url = (f"https://api.binance.com/api/v3/klines?symbol={sym}"
               f"&interval=1m&startTime={cur}&endTime={END}&limit=1000")
        cmd = ["curl", "-s", "--max-time", "25", "-x", PROXY, url]
        data = None
        for _ in range(4):
            try:
                raw = subprocess.check_output(cmd, timeout=30)
                data = json.loads(raw)
                break
            except Exception:
                time.sleep(1.5)
        if not isinstance(data, list) or not data:
            break
        allbars.extend(data)
        if len(data) < 1000:
            break
        cur = data[-1][0] + 60000
        pages += 1
    if allbars:
        json.dump(allbars, open(fn, "w"))
        return f"ok {sym} {len(allbars)}bars"
    return f"empty {sym}"


results = []
with ThreadPoolExecutor(max_workers=6) as ex:
    futs = [ex.submit(pull_sym, s) for s in syms]
    for f in as_completed(futs):
        r = f.result()
        results.append(r)
        if r.startswith("ok") or r.startswith("empty"):
            print(r, flush=True)

ok = sum(1 for r in results if r.startswith("ok "))
skip = sum(1 for r in results if r.startswith("skip "))
empty = sum(1 for r in results if r.startswith("empty "))
print(f"\nDONE ok={ok} skip={skip} empty={empty} / {len(syms)}", flush=True)
