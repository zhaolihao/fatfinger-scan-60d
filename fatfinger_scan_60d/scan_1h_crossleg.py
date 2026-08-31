#!/usr/bin/env python3
"""
scan_1h_crossleg.py —— 全市场 10 天乌龙指粗筛（跨腿偏离为主，修复双薄币漏检）
======== 关键修正（相对旧 scan_1h_wicks 的缺陷）========
旧法要求"对侧 USDT 腿 wick<5%"才算候选，把双薄小币真针（CHIP/SAND/ENSO/SNX：
出针时两腿都晃>=5%）误杀。
本法改用【跨腿偏离】作为唯一筛选：在 1h 同一根 K 线内，比较两腿的"瞬时偏离自身收盘"
的盈余（excess excursion）：
    e_up = (high - close)/close        e_dn = (close - low)/close
    up_excess = e_up(q1) - e_up(q2)    dn_excess = e_dn(q1) - e_dn(q2)
    cross = max(|up_excess|, |dn_excess|)
- cross >= 5% 即候选：捕获单腿针（一腿静、一腿突）+ 双薄币腿针（两腿都动但偏离差>=5%）
- 正常同涨同跌：两腿 e 近似相等 → cross≈0 → 排除（降假阳性）
- 最终真假由 aggTrades 跨腿对齐判定（verify_agg.py），本步只粗筛候选。
输出：candidates_full.json
"""
import json, os
from collections import Counter
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
CACHE = os.path.join(HERE, "klines_1h")

CROSS_MIN = 0.03  # 跨腿偏离预筛阈值 3%（宽松于真假线5%，避免漏掉 SAND 类亚5%候选；ENSO/SNX 类亚分钟瞬态针1h仍不可见，需秒级监控）


def load_legs():
    out = {}
    for fn in sorted(os.listdir(CACHE)):
        if not fn.endswith(".json"):
            continue
        try:
            d = json.load(open(os.path.join(CACHE, fn)))
        except Exception:
            continue
        if isinstance(d, list) and d:
            out[fn[:-5]] = d
    return out


def parse(arr):
    return {int(k[0]): {"o": float(k[1]), "h": float(k[2]), "l": float(k[3]),
                        "c": float(k[4]), "qav": float(k[7]) if len(k) > 7 else 0}
            for k in arr}


def is_fdusd_systemic(leg, iso):
    if leg != "FDUSD":
        return False
    if not iso.startswith("08-22 "):
        return False
    hh = int(iso.split(" ")[1].split(":")[0])
    return 3 <= hh <= 5


def main():
    legs = load_legs()
    print(f"loaded {len(legs)} legs")

    by_base = {}
    for sym, bars in legs.items():
        for q in ("USDT", "USDC", "FDUSD", "USD1"):
            if sym.endswith(q):
                by_base.setdefault(sym[:-len(q)], {})[q] = parse(bars)
                break
    print(f"bases: {len(by_base)}")

    candidates = []
    for base, qm in by_base.items():
        if len(qm) < 2:
            continue
        quotes = list(qm.keys())
        # 所有时间戳并集
        all_t = set()
        for bars in qm.values():
            all_t.update(bars.keys())
        for t in all_t:
            # 取该 base 在该 t 有数据的腿
            present = {q: qm[q][t] for q in quotes if t in qm[q]}
            if len(present) < 2:
                continue
            best = None
            for i in range(len(quotes)):
                for j in range(i + 1, len(quotes)):
                    q1, q2 = quotes[i], quotes[j]
                    if q1 not in present or q2 not in present:
                        continue
                    b1, b2 = present[q1], present[q2]
                    if b1["c"] <= 0 or b2["c"] <= 0:
                        continue
                    e1_up = (b1["h"] - b1["c"]) / b1["c"]
                    e1_dn = (b1["c"] - b1["l"]) / b1["c"]
                    e2_up = (b2["h"] - b2["c"]) / b2["c"]
                    e2_dn = (b2["c"] - b2["l"]) / b2["c"]
                    up_ex = e1_up - e2_up
                    dn_ex = e1_dn - e2_dn
                    cross = max(abs(up_ex), abs(dn_ex))
                    if cross < CROSS_MIN:
                        continue
                    # 偏离更大的腿 = 发散腿
                    if abs(up_ex) >= abs(dn_ex):
                        div_leg, ref_leg, direction = (q1 if e1_up >= e2_up else q2), (q2 if e1_up >= e2_up else q1), "up"
                        mag = up_ex
                    else:
                        div_leg, ref_leg, direction = (q1 if e1_dn >= e2_dn else q2), (q2 if e1_dn >= e2_dn else q1), "down"
                        mag = dn_ex
                    if best is None or cross > best["cross_pct"]:
                        iso = datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%m-%d %H:%M")
                        best = {
                            "base": base, "t": t, "iso": iso,
                            "cross_pct": round(cross * 100, 2),
                            "mag": round(mag * 100, 2),
                            "div_leg": div_leg, "ref_leg": ref_leg, "direction": direction,
                            "div_wick_up": round(e1_up * 100, 2) if div_leg == q1 else round(e2_up * 100, 2),
                            "div_wick_dn": round(e1_dn * 100, 2) if div_leg == q1 else round(e2_dn * 100, 2),
                            "ref_wick_up": round(e2_up * 100, 2) if div_leg == q1 else round(e1_up * 100, 2),
                            "ref_wick_dn": round(e2_dn * 100, 2) if div_leg == q1 else round(e1_dn * 100, 2),
                            "category": "systemic_fdusd_depeg" if is_fdusd_systemic(div_leg, iso) else "cross_leg",
                        }
            if best:
                candidates.append(best)

    candidates.sort(key=lambda x: -x["cross_pct"])
    print(f"\n跨腿偏离候选 (>=5%): {len(candidates)} 条")
    print("各发散腿:", dict(Counter(x["div_leg"] for x in candidates)))
    print("方向:", dict(Counter(x["direction"] for x in candidates)))
    print("systemic(FDUSD脱锚):", sum(1 for x in candidates if x["category"] == "systemic_fdusd_depeg"))

    # 旧法单腿孤立口径对比：div_leg in (USDC,FDUSD) 且 ref_leg==USDT 且 ref_wick<5%
    old_style = [x for x in candidates if x["div_leg"] in ("USDC", "FDUSD")
                 and x["ref_leg"] == "USDT"
                 and min(x["ref_wick_up"], x["ref_wick_dn"]) < 5.0]
    print(f"其中旧法单腿孤立口径能命中: {len(old_style)} 条")
    print(f"旧法会漏掉的双薄/USDT发散类: {len(candidates) - len(old_style)} 条")

    json.dump(candidates, open("candidates_full.json", "w"), ensure_ascii=False)
    print("saved candidates_full.json")


if __name__ == "__main__":
    main()
