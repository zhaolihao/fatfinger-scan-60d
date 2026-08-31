"""
全市场乌龙指预筛（修正版）：单腿 wick 法
=========================================
修复旧「跨腿偏离」法的致命盲区：SAND/ENSO/SNX 这类针是基资产瞬间拉/砸、
两腿同动，跨腿差值≈1% 被当噪声漏掉；但每条腿自身的 wick 高达 7-14%。

正确预筛：base 在任一 1h 时间点，任一条稳定腿(USDT/USDC/FDUSD/USD1)的
单腿 wick = max((H-C)/C, (C-L)/C) >= WICK_MIN(默认5%) 即入选候选。
这同时捕获：
  - CHIP 型：一条腿插针、对侧死盘
  - SAND/ENSO/SNX 型：两腿同动插针
  - FF 型：FDUSD 腿单独脱锚插针
下游仍用 1m 下钻 + aggTrades ±90s 跨腿对齐(max>=5%) 做终审判真/假，
以剔除「两腿真同涨同跌、无实际跨腿报价差」的假阳性。

输入：klines_1h/<SYM>.json（1h K线，由 pull_1h_klines.py 生成）
输出：candidates_singlewick.json
"""
import json, os, sys
from datetime import datetime, timezone

WICK_MIN = 0.05  # 单腿 wick 阈值 5%
KDIR = "fatfinger_scan_full/klines_1h"
OUT = "fatfinger_scan_full/candidates_singlewick.json"


def wick_of(kline):
    h, l, c = float(kline[2]), float(kline[3]), float(kline[4])
    if c <= 0:
        return 0.0, ""
    up = (h - c) / c
    dn = (c - l) / c
    return (max(up, dn), "UP" if up >= dn else "DN")


def main():
    files = [f for f in os.listdir(KDIR) if f.endswith(".json")]
    # base -> {leg: [(t,wick,dir)]}
    by_base = {}
    for f in files:
        sym = f[:-5]
        # 解析 base/quote：quote 为 USDT/USDC/FDUSD/USD1
        for q in ("USDC", "FDUSD", "USD1", "USDT"):
            if sym.endswith(q):
                base = sym[:-len(q)]
                leg = q
                break
        else:
            continue
        try:
            kl = json.load(open(f"{KDIR}/{f}"))
        except Exception:
            continue
        by_base.setdefault(base, {})[leg] = kl

    cands = []
    for base, legs in by_base.items():
        for leg, kl in legs.items():
            for x in kl:
                w, d = wick_of(x)
                if w >= WICK_MIN:
                    t = int(x[0])
                    iso = datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
                    cands.append({
                        "base": base, "leg": leg, "open_time": t, "iso": iso,
                        "wick": round(w * 100, 2), "direction": d,
                    })
    cands.sort(key=lambda c: (c["base"], c["open_time"]))
    json.dump(cands, open(OUT, "w"), indent=1)
    print(f"候选总数: {len(cands)}  (单腿 wick>={WICK_MIN*100:.0f}%)")
    # 统计涉及 base 数 与 关键币命中
    bases = sorted(set(c["base"] for c in cands))
    print(f"涉及 base 数: {len(bases)}")
    for b in ("CHIP", "SAND", "ENSO", "SNX", "FF", "ACE", "LUNC", "RED", "ONT"):
        hit = [c for c in cands if c["base"] == b]
        if hit:
            legs = ",".join(f"{c['leg']}:{c['wick']}%" for c in hit)
            print(f"  {b:5} 命中 {len(hit)} 条 -> {legs}")
        else:
            print(f"  {b:5} 未命中")


if __name__ == "__main__":
    main()
