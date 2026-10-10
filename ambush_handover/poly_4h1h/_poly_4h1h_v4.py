# -*- coding: utf-8 -*-
"""
4h x 1h 嵌套回测 v4 —— 修正结算模型（真实二元结算 $1/$0），并与旧模型(最后市场价)对照。
入场逻辑完全不变（THR/GAP/方向相反/两侧便宜）。环境变量 SLIP/MIN_SIZE/THR_LIST 可调。
"""
import os, io, csv, gzip, json, math, statistics, bisect
from collections import defaultdict
from datetime import datetime, timedelta

OD = os.path.dirname(os.path.abspath(__file__))
SLIP = float(os.environ.get("SLIP", "0.01"))
MIN_SIZE = float(os.environ.get("MIN_SIZE", "5.0"))
GAS = float(os.environ.get("GAS", "0.0"))
THR_LIST = [float(x) for x in os.environ.get("THR_LIST", "0.30,0.25,0.20,0.15,0.10,0.05,0.02").split(",")]
GAP = float(os.environ.get("GAP", "0.00"))
TARGET = 2.0 * MIN_SIZE
OUT = os.environ.get("OUT", "_poly_4h1h_v4_out.txt")
logf = io.open(os.path.join(OD, OUT), "w", encoding="utf-8")
def log(s=""):
    print(s); logf.write(s + "\n"); logf.flush()

def et(ts):
    return (datetime.utcfromtimestamp(ts) - timedelta(hours=4)).strftime("%m/%d %H:%M ET")

def load_json(name):
    p = os.path.join(OD, name)
    if os.path.exists(p):
        return json.load(io.open(p, encoding="utf-8"))
    if os.path.exists(p + ".gz"):
        with gzip.open(p + ".gz", "rt", encoding="utf-8") as f:
            return json.load(f)
    raise FileNotFoundError(f"缺少数据文件: {name} (.json 或 .json.gz)")

fh_meta = load_json("poly_meta_30d_4h.json")
fh_series = defaultdict(list)
with gzip.open(os.path.join(OD, "poly_series_30d_4h.csv.gz"), "rt", encoding="utf-8") as f:
    rd = csv.reader(f); next(rd)
    for slug, k, up, dn, ua, da, mn, mx in rd:
        if "4h" not in slug: continue
        fh_series[slug].append((int(k), float(up), float(dn)))
h1_meta = load_json("poly_meta_1h_last_30d.json")
h1_trades = load_json("poly_trades_1h_last_30d.json")

# ---- 解析 1h 真实胜负 ----
def parse_outcome_prices(v):
    if v is None: return None
    if isinstance(v, str):
        try: v = json.loads(v)
        except Exception: return None
    if isinstance(v, list) and len(v) == 2:
        try: return [float(x) for x in v]
        except Exception: return None
    return None

h1_winner = {}
diag = defaultdict(int)
for h in h1_meta:
    op = parse_outcome_prices(h.get("outcomePrices"))
    if op and ({op[0], op[1]} == {0.0, 1.0}):
        h1_winner[h["1h_slug"]] = "Up" if op[0] >= 1.0 else "Down"
        diag["resolved_1h"] += 1
    else:
        diag["unresolved_1h"] += 1
log(f"1h 市场：可从 outcomePrices 判定胜负 {diag['resolved_1h']} 个；无法判定 {diag['unresolved_1h']} 个")

# 4h 真实胜负
fh_winner = {s: m.get("winner") for s, m in fh_meta.items()}
log(f"4h 市场：有 winner 字段 {sum(1 for v in fh_winner.values() if v in ('Up','Down'))} 个")

by_slug = defaultdict(list)
for tr in h1_trades: by_slug[tr["slug"]].append(tr)
def build_h1(trs):
    trs.sort(key=lambda x: int(x["timestamp"])); up={}; dn={}
    for tr in trs:
        ts=int(tr["timestamp"]); pr=float(tr["price"]); side=tr["outcome"]
        lu,ld = (pr,1.0-pr) if side=="Up" else (1.0-pr,pr)
        up[ts]=lu; dn[ts]=ld
    return up,dn
def build_fh(recs):
    recs.sort(key=lambda x:x[0]); return ({k:u for k,u,d in recs}, {k:d for k,u,d in recs})
h1_up={}; h1_dn={}; h1_sorted={}
for slug,trs in by_slug.items():
    u,d=build_h1(trs); h1_up[slug]=u; h1_dn[slug]=d; h1_sorted[slug]=sorted(u.keys())
fh_up={}; fh_dn={}; fh_sorted={}
for slug,recs in fh_series.items():
    u,d=build_fh(recs); fh_up[slug]=u; fh_dn[slug]=d; fh_sorted[slug]=sorted(u.keys())

pairs=[]
for slug,recs in fh_series.items():
    m=fh_meta.get(slug,{})
    if not m.get("closed"): continue
    if m.get("winner") not in ("Up","Down"): continue
    start4=int(m["start"]); end4=int(m["end"])
    h1=next((x for x in h1_meta if x["4h_slug"]==slug),None)
    if not h1 or h1["1h_slug"] not in h1_winner: continue
    pairs.append({"fh_slug":slug,"h1_slug":h1["1h_slug"],"fh_start":start4,"fh_end":end4,
                  "h1_start":int(h1["1h_start_utc"]),"h1_end":int(h1["1h_start_utc"])+3600,
                  "fh_win":m["winner"],"h1_win":h1_winner[h1["1h_slug"]]})
log(f"可判定双市场胜负的配对：{len(pairs)} 对")

def find_entry(p, thr, gap):
    res=[]
    for ts in h1_sorted[p["h1_slug"]]:
        if ts<p["h1_start"] or ts>p["h1_end"]: continue
        ks=fh_sorted[p["fh_slug"]]; i=bisect.bisect_right(ks,ts-p["fh_start"])-1
        if i<0: continue
        k=ks[i]; fu,fd=fh_up[p["fh_slug"]][k], fh_dn[p["fh_slug"]][k]
        hu,hd=h1_up[p["h1_slug"]][ts], h1_dn[p["h1_slug"]][ts]
        if fu<fd: fl,fb="up",fu
        elif fd<fu: fl,fb="dn",fd
        else: continue
        if hu<hd: hl,hb="up",hu
        elif hd<hu: hl,hb="dn",hd
        else: continue
        if fl==hl: continue
        if fb>=thr or hb>=thr: continue
        if abs(fu-0.5)<gap: continue
        return ts,fl,fb,hl,hb
    return None

def settle_price(leg, winner, last_px, model):
    """model='binary' 真实二元结算($1/$0)；model='last' 旧模型用到期前最后市场价"""
    if model=="binary":
        return 1.0 if (leg==winner) else 0.0
    return last_px

def simulate(p, thr, gap, model, detail=False):
    e=find_entry(p,thr,gap)
    if e is None: return None
    ts,fl,fb,hl,hb=e
    fl=fl.upper(); hl=hl.upper()
    fs=MIN_SIZE/(fb*(1+SLIP)); hs=MIN_SIZE/(hb*(1+SLIP))
    cost=2*MIN_SIZE
    lines=[]
    if detail:
        lines.append(f"  入场 {et(ts)} | 4h买{fl} @${fb:.4f}→{fs:.1f}份 | 1h买{hl} @${hb:.4f}→{hs:.1f}份 | 投${cost:.0f}")
    # 4h 侧
    fsold=False; fpx=None
    for k,u,d in fh_series[p["fh_slug"]]:
        t=p["fh_start"]+k
        if t<ts or t>p["fh_end"]: continue
        pr=u if fl=="UP" else d
        if fs*pr>=TARGET:
            fpx=pr*(1-SLIP); fsold=True
            if detail: lines.append(f"  → 4h {fl} 于 {et(t)} 市值${fs*pr:.2f}≥${TARGET:.0f} 全卖@${fpx:.4f} 收回${fs*fpx:.2f}")
            break
    if not fsold:
        ks=fh_sorted[p["fh_slug"]]; i=bisect.bisect_right(ks,p["fh_end"]-p["fh_start"])-1
        last=fh_up[p["fh_slug"]][ks[i]] if fl=="UP" else fh_dn[p["fh_slug"]][ks[i]]
        fpx=settle_price(fl,p["fh_win"],last,model)
        if detail:
            tag="赢" if p["fh_win"]==fl else "输"
            lines.append(f"  → 4h {fl} 未翻倍，到期结算({tag}) 单价=${fpx:.4f} 得${fs*fpx:.2f}" + ("" if model=="binary" else f"  [旧模型用最后价{last:.4f}]"))
    # 1h 侧
    hsold=False; hpx=None
    for t in h1_sorted[p["h1_slug"]]:
        if t<ts or t>p["h1_end"]: continue
        pr=h1_up[p["h1_slug"]][t] if hl=="UP" else h1_dn[p["h1_slug"]][t]
        if hs*pr>=TARGET:
            hpx=pr*(1-SLIP); hsold=True
            if detail: lines.append(f"  → 1h {hl} 于 {et(t)} 市值${hs*pr:.2f}≥${TARGET:.0f} 全卖@${hpx:.4f} 收回${hs*hpx:.2f}")
            break
    if not hsold:
        tl=h1_sorted[p["h1_slug"]]; i=bisect.bisect_right(tl,p["h1_end"])-1
        last=h1_up[p["h1_slug"]][tl[i]] if hl=="UP" else h1_dn[p["h1_slug"]][tl[i]]
        hpx=settle_price(hl,p["h1_win"],last,model)
        if detail:
            tag="赢" if p["h1_win"]==hl else "输"
            lines.append(f"  → 1h {hl} 未翻倍，到期结算({tag}) 单价=${hpx:.4f} 得${hs*hpx:.2f}" + ("" if model=="binary" else f"  [旧模型用最后价{last:.4f}]"))
    proceed=fs*fpx+hs*hpx
    pnl=proceed-cost
    if detail:
        lines.append(f"  回收${proceed:.2f} − 成本${cost:.0f} = 盈亏 ${pnl:+.2f} (ROI {pnl/cost*100:+.1f}%)")
    return pnl, lines

log("\n" + "="*76)
log("【结算模型对照】入场逻辑相同；last=旧模型(用到期前最后市场价) / binary=真实二元结算($1/$0)")
log(f"成本：每侧${MIN_SIZE} 价差{SLIP*100:.1f}% 固定费{GAS}")
log("="*76)
log(f"{'THR':>6} | {'旧模型 last':^34} | {'真实结算 binary':^34}")
log(f"{'':>6} | {'n':>3} {'ROI%':>8} {'t':>7} {'胜':>7} | {'n':>3} {'ROI%':>8} {'t':>7} {'胜':>7}")
for thr in THR_LIST:
    row=[]
    for model in ("last","binary"):
        ps=[]
        for p in pairs:
            r=simulate(p,thr,GAP,model)
            if r: ps.append(r[0])
        if ps:
            n=len(ps); mean=statistics.mean(ps); sd=statistics.stdev(ps) if n>1 else 0
            t=mean/(sd/math.sqrt(n)) if sd>0 else 0
            w=sum(1 for x in ps if x>0)
            row.append((n,mean/(2*MIN_SIZE)*100,t,w,mean))
        else:
            row.append((0,0,0,0,0))
    a,b=row
    log(f"{thr:>6.2f} | {a[0]:>3d} {a[1]:>+8.2f} {a[2]:>+7.2f} {a[3]:>3d}/{a[0]:<3d} | {b[0]:>3d} {b[1]:>+8.2f} {b[2]:>+7.2f} {b[3]:>3d}/{b[0]:<3d}")

# 真实例子：新旧模型下的同一局
log("\n" + "="*76)
log("【真实例子逐步还原】09/12 12:00-16:00 ET 4h 窗口")
log("="*76)
tgt=next((p for p in pairs if p["fh_slug"]=="btc-updown-4h-1789228800"), None)
if tgt:
    for model in ("last","binary"):
        r=simulate(tgt,0.20,0.00,model,detail=True)
        log(f"\n--- 模型={model}（4h实际结果={tgt['fh_win']}, 1h实际结果={tgt['h1_win']}）---")
        for l in r[1]: log(l)
else:
    log("未找到该窗口")
log("\n完成"); logf.close()
