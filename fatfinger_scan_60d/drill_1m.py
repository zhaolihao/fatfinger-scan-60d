#!/usr/bin/env python3
"""
drill_1m.py —— 对单腿 wick 候选拉 1m K 线，定位精确针分钟（skill §3.5.1）
- 读取 candidates.json（scan_1h_wicks.py 输出，字段: base/leg/open_time/wick/...）
- 对每条候选拉 发散腿 ±80min 的 1m K 线，找该时段单根最大振幅分钟作为 anchor
- 输出 drill_full.json（带 anchor_ms / anchor_iso / max_wick_pct / single_needle）
"""
import json, os, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
CACHE = os.path.join(HERE, "klines_1m")
os.makedirs(CACHE, exist_ok=True)
PROXY = os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:7897"

candidates = json.load(open("candidates.json"))
print(f"候选: {len(candidates)} 条", flush=True)


def fetch_1m(c):
    base, leg = c["base"], c["leg"]
    sym = base + leg
    hour_start = c["open_time"]
    start = hour_start - 10 * 60 * 1000
    end = hour_start + 70 * 60 * 1000
    fn = os.path.join(CACHE, f"{sym}_{hour_start}.json")
    if os.path.exists(fn) and os.path.getsize(fn) > 50:
        return f"skip {sym}"
    url = (f"https://api.binance.com/api/v3/klines?symbol={sym}"
           f"&interval=1m&startTime={start}&endTime={end}&limit=1000")
    cmd = ["curl", "-s", "--max-time", "25", "-x", PROXY, url]
    try:
        raw = subprocess.check_output(cmd, timeout=30)
        data = json.loads(raw)
    except Exception as e:
        return f"err {sym}: {e}"
    if not data or isinstance(data, dict):
        return f"empty {sym}"
    json.dump(data, open(fn, "w"))
    return f"ok {sym} {len(data)}bars"


results = []
with ThreadPoolExecutor(max_workers=6) as ex:
    futs = [ex.submit(fetch_1m, c) for c in candidates]
    for f in as_completed(futs):
        results.append(f.result())
ok = sum(1 for r in results if r.startswith("ok "))
print(f"1m 拉取 ok={ok}/{len(candidates)}", flush=True)

out = []
for c in candidates:
    base, leg = c["base"], c["leg"]
    sym = base + leg
    hour_start = c["open_time"]
    fn = os.path.join(CACHE, f"{sym}_{hour_start}.json")
    rec = dict(c)
    if not os.path.exists(fn):
        rec.update(anchor_ms=0, anchor_iso=None, max_wick_pct=0, single_ratio=0, single_needle=False)
        out.append(rec)
        continue
    try:
        bars = json.load(open(fn))
    except Exception:
        rec.update(anchor_ms=0, anchor_iso=None, max_wick_pct=0, single_ratio=0, single_needle=False)
        out.append(rec)
        continue
    win = [k for k in bars if abs(int(k[0]) - hour_start) < 3600 * 1000]
    max_wick = -1
    max_min = 0
    sq = 0
    for k in win:
        h, l, cl = float(k[2]), float(k[3]), float(k[4])
        if cl <= 0:
            continue
        w = (h - l) / cl
        sq += w * w
        if w > max_wick:
            max_wick = w
            max_min = int(k[0])
    ratio = max_wick / (sq ** 0.5) if sq > 0 else 0
    is_single = (ratio >= 0.5 and max_wick >= 0.05)
    anchor_iso = datetime.fromtimestamp(max_min / 1000, timezone.utc).strftime("%m-%d %H:%M:%S") if max_min else None
    rec.update(anchor_ms=max_min, anchor_iso=anchor_iso,
               max_wick_pct=round(max_wick * 100, 2),
               single_ratio=round(ratio, 2), single_needle=is_single)
    out.append(rec)

out.sort(key=lambda x: -x["cross_leg_excess"])
json.dump(out, open("drill_full.json", "w"), ensure_ascii=False)
print(f"\nsingle_needle 候选: {sum(1 for x in out if x['single_needle'])}/{len(out)}")
print("saved drill_full.json")
