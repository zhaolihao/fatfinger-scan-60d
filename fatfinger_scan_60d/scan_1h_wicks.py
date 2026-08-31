"""
全市场乌龙指预筛（单腿 wick 法，已验证修正版）
=============================================
【重要】不要用「跨腿偏离」做预筛！实测 SAND/ENSO/SNX 这类针是基资产瞬间
拉/砸、两腿同动，跨腿差值仅 1%（被当噪声漏掉）；但每条腿自身 wick 高达 7-14%。
所以预筛必须看「单腿 wick」而非「两腿差值」。

预筛规则（base 在任一 1h 时间点）：
  任一条稳定腿(USDT/USDC/FDUSD/USD1) 单腿 wick = max((H-C)/C,(C-L)/C) >= WICK_MIN(默认5%)
  -> 入选候选（同时捕获 CHIP 型/双薄同动型/FF 脱锚型）

为控制下游 aggTrades 数量，可加二级门：cross_leg_excess >= CROSS_MIN（默认1%，
保留 ENSO/SNX 这类跨腿差 1% 的真针，剔除两腿真同涨同跌的纯波动假阳性）。
设 CROSS_MIN=0 则不过滤（最全但候选多）。

输入：klines_1h/<SYM>.json
输出：candidates.json（含 wick/cross_leg_excess/legs 供下游 drill+verify）
"""
import json, os
from datetime import datetime, timezone

WICK_MIN = 0.05   # 单腿 wick 阈值
CROSS_MIN = 0.0    # 跨腿 excess 二级门（0=不过滤；1h级跨腿无法区分基础资产插针与全盘同跌，故不在此过滤，精度交给aggTrades终审）
KDIR = "klines_1h"
OUT = "candidates.json"


def wick_of(kline):
    h, l, c = float(kline[2]), float(kline[3]), float(kline[4])
    if c <= 0:
        return 0.0, ""
    up = (h - c) / c
    dn = (c - l) / c
    return (max(up, dn), "UP" if up >= dn else "DN")


def cross_excess(kA, kB):
    # 两腿同时间点 excess = max(|upA-upB|, |dnA-dnB|)
    hA, lA, cA = float(kA[2]), float(kA[3]), float(kA[4])
    hB, lB, cB = float(kB[2]), float(kB[3]), float(kB[4])
    if cA <= 0 or cB <= 0:
        return 0.0
    upA, dnA = (hA - cA) / cA, (cA - lA) / cA
    upB, dnB = (hB - cB) / cB, (cB - lB) / cB
    return max(abs(upA - upB), abs(dnA - dnB))


def main():
    files = [f for f in os.listdir(KDIR) if f.endswith(".json")]
    by_base = {}
    for f in files:
        sym = f[:-5]
        for q in ("USDC", "FDUSD", "USD1", "USDT"):
            if sym.endswith(q):
                by_base.setdefault(sym[:-len(q)], {})[q] = json.load(open(f"{KDIR}/{f}"))
                break

    cands = []
    for base, legs in by_base.items():
        # 建时间对齐
        times = {}
        for leg, kl in legs.items():
            for x in kl:
                times.setdefault(int(x[0]), {})[leg] = x
        for t, row in times.items():
            best_w = 0.0
            best_leg = None
            best_dir = ""
            for leg, x in row.items():
                w, d = wick_of(x)
                if w > best_w:
                    best_w, best_leg, best_dir = w, leg, d
            if best_w < WICK_MIN:
                continue
            # 跨腿 excess（取任意两腿）
            cx = 0.0
            legs_sorted = sorted(row.keys())
            for i in range(len(legs_sorted)):
                for j in range(i + 1, len(legs_sorted)):
                    cx = max(cx, cross_excess(row[legs_sorted[i]], row[legs_sorted[j]]))
            if cx < CROSS_MIN:
                continue
            iso = datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            cands.append({
                "base": base, "leg": best_leg, "open_time": t, "iso": iso,
                "wick": round(best_w * 100, 2), "direction": best_dir,
                "cross_leg_excess": round(cx * 100, 2),
                "legs": legs_sorted,
            })
    cands.sort(key=lambda c: (c["base"], c["open_time"]))
    json.dump(cands, open(OUT, "w"), indent=1)
    print(f"候选总数: {len(cands)}  (单腿wick>={WICK_MIN*100:.0f}% 且 跨腿excess>={CROSS_MIN*100:.1f}%)")
    print(f"涉及 base 数: {len(set(c['base'] for c in cands))}")


if __name__ == "__main__":
    main()
