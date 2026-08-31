#!/usr/bin/env python3
"""
build_targets.py —— 生成全市场扫描的"目标 base 清单" targets.json（修正版）
数据来源：/api/v3/exchangeInfo（走代理）
筛选规则（修正：去掉过严的成交额门槛，否则漏掉 FF 等 FDUSD-only 薄币）：
  - TRADING 现货对
  - 同时有 USDT + (USDC / FDUSD / USD1 任一) 的稳定币腿（>=2 条才能跨腿对齐）
输出：targets.json = [{"base","USDT","USDC","FDUSD","USD1","n_legs","legs"}]
"""
import json, os, subprocess
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
PROXY = os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:7897"
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

    base_legs = defaultdict(dict)
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
    json.dump(out, open("targets.json", "w"), ensure_ascii=False)
    print(f"全市场跨腿候选 base 数: {len(out)}（无成交额门槛，含 FDUSD-only 薄币）")
    from collections import Counter
    c = Counter(tuple(sorted(x["legs"])) for x in out)
    for k, v in c.most_common():
        print(k, v)


if __name__ == "__main__":
    main()
