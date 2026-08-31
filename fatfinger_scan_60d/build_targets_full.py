#!/usr/bin/env python3
"""
build_targets_full.py —— 全市场跨腿目标清单（修正版）
- 拉 exchangeInfo，取所有 TRADING 现货对
- 候选 base = 同时有 USDT + (USDC / FDUSD / USD1 任一) 的稳定腿（>=2 条才能跨腿验证）
- 不做成交额门槛（之前的 $100k 门槛漏掉了 FF 等 FDUSD-only 薄币）
- 输出 targets_full.json（与旧 targets.json 同结构，额外带 USD1）
"""
import json, os, subprocess, sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
PROXY = "http://127.0.0.1:7897"

QSET = ("USDT", "USDC", "FDUSD", "USD1")


def main():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    cmd = ["curl", "-s", "--max-time", "30", "-x", PROXY, url]
    try:
        raw = subprocess.check_output(cmd, timeout=35)
        d = json.loads(raw)
    except Exception as e:
        print("FETCH FAIL", e); raise SystemExit
    syms = d.get("symbols", [])

    base_legs = defaultdict(dict)   # base -> {quote: True}
    for s in syms:
        if s.get("status") != "TRADING":
            continue
        b = s["baseAsset"]; q = s["quoteAsset"]
        if q in QSET:
            base_legs[b][q] = True

    out = []
    for b, legs in base_legs.items():
        if "USDT" not in legs:
            continue
        others = [q for q in QSET if q != "USDT" and q in legs]
        if not others:
            continue
        rec = {"base": b, "USDT": 1, "USDC": 1 if "USDC" in legs else 0,
               "FDUSD": 1 if "FDUSD" in legs else 0, "USD1": 1 if "USD1" in legs else 0,
               "n_legs": 1 + len(others), "legs": ["USDT"] + others}
        out.append(rec)

    out.sort(key=lambda x: x["base"])
    json.dump(out, open("targets_full.json", "w"), ensure_ascii=False)
    print(f"全市场跨腿候选 base 数: {len(out)}")
    from collections import Counter
    c = Counter(tuple(sorted(x["legs"])) for x in out)
    for k, v in c.most_common():
        print(k, v)


if __name__ == "__main__":
    main()
