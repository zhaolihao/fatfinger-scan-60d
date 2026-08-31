#!/usr/bin/env python3
"""precursor_analysis.py —— 乌龙指前兆统计（带对照基线）
对每条真乌龙指，看针前 2h(120根1m)是否出现小针(>=1% / 1-2% / 2-4% / >=5%)，
并与同一批币"任意时段2h窗"的经验基线对比，只有事件命中率>>基线才算真规律。
对重复出现币单独拆。
"""
import json, os
from datetime import datetime, timezone
from collections import defaultdict, Counter

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
rows = json.load(open("verify_results_full.json"))
true = [r for r in rows if r["verdict"] == "真"]

SYM2BARS = {}
miss_sym = []
for r in true:
    sym = r["base"] + r["leg"]
    if sym not in SYM2BARS:
        fn = os.path.join("klines_full", f"{sym}.json")
        if not os.path.exists(fn):
            miss_sym.append(sym)
            SYM2BARS[sym] = None
            continue
        SYM2BARS[sym] = json.load(open(fn))

PRE = 120  # 2h = 120 根 1m

def wick_of(b):
    h, l, c = float(b[2]), float(b[3]), float(b[4])
    if c <= 0:
        return 0.0
    return max((h - c) / c, (c - l) / c) * 100.0

# 每个币：open_time->idx 映射 + wick数组 + 前缀和
def build(sym):
    bars = SYM2BARS[sym]
    if bars is None:
        return None
    n = len(bars)
    t2i = {int(b[0]): i for i, b in enumerate(bars)}
    w = [wick_of(b) for b in bars]
    return {"n": n, "t2i": t2i, "w": w}

# 前缀和（统计某窗内是否有达标针）
def prefmask(w, lo, hi):
    # hi=None 表示无上限
    pre = [0] * (len(w) + 1)
    for i, x in enumerate(w):
        ok = (x >= lo) and (hi is None or x < hi)
        pre[i + 1] = pre[i] + (1 if ok else 0)
    return pre

# 事件侧
events = []  # dict: base,leg,anchor_ms,idx,found
for r in true:
    sym = r["base"] + r["leg"]
    bars = SYM2BARS[sym]
    if bars is None:
        continue
    t2i = {int(b[0]): i for i, b in enumerate(bars)}
    a = t2i.get(int(r["anchor_ms"]))
    if a is None:
        continue
    events.append({"base": r["base"], "leg": r["leg"], "sym": sym,
                   "anchor_ms": int(r["anchor_ms"]), "idx": a, "peak": r["peak_pct"]})

print(f"纳入分析事件: {len(events)} (缺历史跳过 {len(miss_sym)})", flush=True)

# 滑窗基线 + 事件命中：逐币处理
# 收集每币的 anchors
by_sym = defaultdict(list)
for e in events:
    by_sym[e["sym"]].append(e["idx"])

bands = [("any1", 1.0, None), ("b12", 1.0, 2.0), ("b24", 2.0, 4.0), ("big5", 5.0, None)]

event_hit = {k: 0 for k, _, _ in bands}
event_lead = []          # (lead_min, band) for events that have any1 precursor
event_imm1 = 0           # t-1 分钟 wick>=1
event_imm2 = 0           # t-2 分钟 wick>=1
event_imm1_b24 = 0       # t-1 分钟 wick in 2-4
baseline_rate = {k: [] for k, _, _ in bands}  # 每币一个基线率
base_events = defaultdict(list)  # base -> list of (has_any1, has_b24)

for sym, idxs in by_sym.items():
    bars = SYM2BARS[sym]
    if bars is None:
        continue
    w = [wick_of(b) for b in bars]
    n = len(w)
    # 锚点集合（用于屏蔽）
    anchors = set(idxs)
    # 无效起始：窗[s,s+PRE-1]若含锚点则无效。锚点a -> s in [a-PRE+1, a]
    invalid = [False] * (n - PRE + 1) if n >= PRE else []
    for a in anchors:
        lo = max(0, a - PRE + 1)
        hi = min(a, len(invalid) - 1)
        for s in range(lo, hi + 1):
            invalid[s] = True
    # 各 band 前缀和
    prefs = {k: prefmask(w, lo, hi) for k, lo, hi in bands}
    # 基线：滑窗统计（仅有效起始）
    base_cnt = {k: 0 for k, _, _ in bands}
    base_tot = 0
    if n >= PRE:
        for s in range(0, n - PRE + 1):
            if invalid[s]:
                continue
            base_tot += 1
            for k, _, _ in bands:
                if prefs[k][s + PRE] - prefs[k][s] > 0:
                    base_cnt[k] += 1
    if base_tot > 0:
        for k, _, _ in bands:
            baseline_rate[k].append(base_cnt[k] / base_tot)
    # 事件侧
    for a in idxs:
        s0 = max(0, a - PRE)
        e0 = a  # 不含锚点本身
        # 用锚点币的 base 名
        # 找该 event 的 base
        e_rec = next(e for e in events if e["sym"] == sym and e["idx"] == a)
        base = e_rec["base"]
        has = {}
        for k, lo, hi in bands:
            cnt = prefs[k][e0] - prefs[k][s0]
            has[k] = cnt > 0
        for k, _, _ in bands:
            if has[k]:
                event_hit[k] += 1
        # lead：找 pre 窗内最大 wick 的位置
        if has["any1"]:
            best = -1.0
            best_i = -1
            for i in range(s0, e0):
                if w[i] > best:
                    best = w[i]
                    best_i = i
            if best_i >= 0:
                event_lead.append((a - best_i, best))
        # immediate
        if a - 1 >= 0:
            if w[a - 1] >= 1.0:
                event_imm1 += 1
            if 2.0 <= w[a - 1] < 4.0:
                event_imm1_b24 += 1
        if a - 2 >= 0 and w[a - 2] >= 1.0:
            event_imm2 += 1
        base_events[base].append((has["any1"], has["b24"]))

NE = len(events)
def pct(x):
    return f"{x/NE*100:.1f}%"

# lead 直方图
lead_buckets = Counter()
for lm, _ in event_lead:
    if lm <= 1:
        lead_buckets["1min"] += 1
    elif lm <= 5:
        lead_buckets["2-5min"] += 1
    elif lm <= 15:
        lead_buckets["6-15min"] += 1
    elif lm <= 60:
        lead_buckets["16-60min"] += 1
    else:
        lead_buckets["61-120min"] += 1

# 基线均值
base_mean = {k: (sum(v) / len(v) if v else 0) for k, v in baseline_rate.items()}

# 重复币
multi = {b: v for b, v in base_events.items() if len(v) >= 2}

# ===== 输出 =====
L = []
L.append("# 乌龙指前兆统计（针前 2 小时，带对照基线）\n")
L.append(f"- 纳入事件：{NE} 条真乌龙指（缺 1m 历史跳过 {len(miss_sym)} 条）")
L.append(f"- 窗口：针前 2h = 120 根 1m 蜡烛；锚点分钟本身不计入（它是针）")
L.append(f"- 基线：同批币全量 10 天历史上滑 2h 窗（排除针±2h 邻域），统计'任意时段出现小针'的概率\n")
L.append("## 一、事件命中率 vs 对照基线（核心）\n")
L.append("| 前兆定义 | 针前2h命中率 | 任意时段基线 | 倍数(lift) | 解读 |")
L.append("|------|------|------|------|------|")
interp = {
    "any1": ">=1% 小针（极宽松）",
    "b12": "1-2% 小针",
    "b24": "2-4% 小针（用户重点）",
    "big5": ">=5% 另一根真针（簇/连续）",
}
for k, _, _ in bands:
    er = event_hit[k] / NE if NE else 0
    br = base_mean.get(k, 0)
    lift = (er / br) if br > 0 else float("inf")
    lift_s = f"{lift:.2f}x" if br > 0 else "∞"
    L.append(f"| {interp[k]} | {er*100:.1f}% ({event_hit[k]}/{NE}) | {br*100:.1f}% | {lift_s} | {'有信号' if lift>=1.3 else '与基线无差' if br>0 else '?'} |")
L.append("")
L.append("**判读**：若事件命中率≈基线，说明这些小针只是波动币的常态噪声，不构成可提前检测的'前兆'；")
L.append("只有 lift 明显>1（建议≥1.5）且 lead 集中在前几分钟，才有实战预警价值。\n")
L.append("## 二、前兆的'提前量'（仅统计有 >=1% 前兆的事件的领先分钟）\n")
L.append(f"- 有 >=1% 前兆的事件：{len(event_lead)}/{NE}（{len(event_lead)/NE*100:.1f}%）")
L.append(f"- 这些前兆的领先时间分布：")
for b in ["1min", "2-5min", "6-15min", "16-60min", "61-120min"]:
    L.append(f"  - {b}: {lead_buckets.get(b,0)}")
if event_lead:
    import statistics as st
    L.append(f"  - 中位领先：{st.median(x[0] for x in event_lead)} min；最大前兆 wick 中位：{st.median(x[1] for x in event_lead):.2f}%")
L.append("")
L.append("## 三、'前几秒'级即时前兆（针前 1-2 分钟）\n")
L.append(f"- 针前第1分钟(≈前1min) wick>=1%：{event_imm1}/{NE}（{event_imm1/NE*100:.1f}%）")
L.append(f"- 针前第2分钟(≈前2min) wick>=1%：{event_imm2}/{NE}（{event_imm2/NE*100:.1f}%）")
L.append(f"- 针前第1分钟 wick 在 2-4%：{event_imm1_b24}/{NE}（{event_imm1_b24/NE*100:.1f}%）")
L.append("")
L.append("## 四、重复出现币的前兆一致性\n")
L.append(f"重复真针(≥2次)的 base 共 {len(multi)} 个。逐币'针前2h含>=1%小针'与'含2-4%小针'命中率：\n")
L.append("| base | 真针次数 | 含>=1%前兆 | 含2-4%前兆 |")
L.append("|------|------|------|------|")
for b, v in sorted(multi.items(), key=lambda x: -len(x[1])):
    h1 = sum(1 for x in v if x[0])
    h24 = sum(1 for x in v if x[1])
    L.append(f"| {b} | {len(v)} | {h1}/{len(v)} ({h1/len(v)*100:.0f}%) | {h24}/{len(v)} ({h24/len(v)*100:.0f}%) |")
L.append("")
L.append("## 五、结论\n")
L.append("- 见上表 lift 与领先分布。若各 band lift≈1 且领先分散到 1-2h 均匀，则**无共有前兆规律**，")
L.append("  这类乌龙指本质是流动性瞬时空洞下的独立随机事件，难以用'前几分钟小针'提前预警；")
L.append("  例外是 big5 簇（同一币短时间内连发），那属于'已经发生的针之后 2h 内再来一根'，对首次预警无帮助。")
L.append("- 若某 band（尤其 2-4%）lift 显著且领先集中在前 1-5 min，则可在 live 监控加'同币 1m wick 突破 2% 即预警'规则。")
md = "\n".join(L)
open("precursor_analysis.md", "w").write(md)
print(md)
print("\nDONE -> precursor_analysis.md", flush=True)
