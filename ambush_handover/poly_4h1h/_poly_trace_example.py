# -*- coding: utf-8 -*-
"""从 30 天真实数据里挑一个真实触发的 4h 窗口，逐步还原入场+出场全过程（THR=0.20,GAP=0.00,每侧5$,价差1%）。"""
import os, io, csv, gzip, json, math, bisect
from collections import defaultdict
from datetime import datetime, timedelta

OD = os.path.dirname(os.path.abspath(__file__))
THR, GAP, SLIP, MIN_SIZE = 0.20, 0.00, 0.01, 5.0
TARGET = 2.0 * MIN_SIZE  # 任一侧市值>=10$ 即全卖收回成本

def et(ts):  # UTC -> ET(10月=UTC-4)
    return (datetime.utcfromtimestamp(ts) - timedelta(hours=4)).strftime("%m/%d %H:%M ET")

fh_meta = json.load(io.open(os.path.join(OD, "poly_meta_30d_4h.json"), encoding="utf-8"))
fh_series = defaultdict(list)
with gzip.open(os.path.join(OD, "poly_series_30d_4h.csv.gz"), "rt", encoding="utf-8") as f:
    rd = csv.reader(f); next(rd)
    for slug, k, up, dn, ua, da, mn, mx in rd:
        if "4h" not in slug: continue
        fh_series[slug].append((int(k), float(up), float(dn)))
h1_meta = json.load(io.open(os.path.join(OD, "poly_meta_1h_last_30d.json"), encoding="utf-8"))
h1_trades = json.load(io.open(os.path.join(OD, "poly_trades_1h_last_30d.json"), encoding="utf-8"))

by_slug = defaultdict(list)
for tr in h1_trades: by_slug[tr["slug"]].append(tr)

def build_h1(trs):
    trs.sort(key=lambda x: int(x["timestamp"]))
    up={}; dn={}
    for tr in trs:
        ts=int(tr["timestamp"]); pr=float(tr["price"]); side=tr["outcome"]
        if side=="Up": lu,ld=pr,1.0-pr
        else: ld,lu=pr,1.0-pr
        up[ts]=lu; dn[ts]=ld
    return up,dn
def build_fh(recs):
    recs.sort(key=lambda x:x[0]); up={}; dn={}
    for k,u,d in recs: up[k]=u; dn[k]=d
    return up,dn

fh_up={}; fh_dn={}; fh_sorted={}
for slug,recs in fh_series.items():
    u,d=build_fh(recs); fh_up[slug]=u; fh_dn[slug]=d; fh_sorted[slug]=sorted(u.keys())
h1_up={}; h1_dn={}; h1_sorted={}
for slug,trs in by_slug.items():
    u,d=build_h1(trs); h1_up[slug]=u; h1_dn[slug]=d; h1_sorted[slug]=sorted(u.keys())

pairs=[]
for slug,recs in fh_series.items():
    m=fh_meta.get(slug,{})
    if not m.get("closed"): continue
    start4=m["start"]; end4=m["end"]
    h1=next((x for x in h1_meta if x["4h_slug"]==slug),None)
    if not h1: continue
    pairs.append({"fh_slug":slug,"h1_slug":h1["1h_slug"],"fh_start":start4,"fh_end":end4,
                  "h1_start":h1["1h_start_utc"],"h1_end":h1["1h_start_utc"]+3600,"entry":start4+10800})

def find_entry(p):
    for ts in h1_sorted[p["h1_slug"]]:
        if ts<p["h1_start"] or ts>p["h1_end"]: continue
        fh_k=ts-p["fh_start"]; i=bisect.bisect_right(fh_sorted[p["fh_slug"]],fh_k)-1
        if i<0: continue
        fu,fd=fh_up[p["fh_slug"]][fh_sorted[p["fh_slug"]][i]], fh_dn[p["fh_slug"]][fh_sorted[p["fh_slug"]][i]]
        hu,hd=h1_up[p["h1_slug"]][ts], h1_dn[p["h1_slug"]][ts]
        if fu<fd: fl,fb="up",fu
        elif fd<fu: fl,fb="dn",fd
        else: continue
        if hu<hd: hl,hb="up",hu
        elif hd<hu: hl,hb="dn",hd
        else: continue
        if fl==hl: continue
        if fb>=THR or hb>=THR: continue
        if abs(fu-0.5)<GAP: continue
        return ts,fl,fb,hl,hb
    return None

def simulate_full(p, entry_info):
    ts,fl,fb,hl,hb=entry_info
    fs=MIN_SIZE/(fb*(1+SLIP)); hs=MIN_SIZE/(hb*(1+SLIP))
    cost=2*MIN_SIZE
    log_lines=[]
    log_lines.append(f"  入场时刻 {et(ts)} | 4h买{fl.upper()} @${fb:.4f} 得{fs:.1f}份 | 1h买{hl.upper()} @${hb:.4f} 得{hs:.1f}份 | 共投${cost}")
    # 4h 侧出场
    fsold=False; fpx=None
    for k,u,d in fh_series[p["fh_slug"]]:
        t=p["fh_start"]+k
        if t<ts: continue
        if t>p["fh_end"]: break
        pr=u if fl=="up" else d
        if not fsold and fs*pr>=TARGET:
            fpx=pr*(1-SLIP); fsold=True
            log_lines.append(f"  → 4h {fl.upper()} 在 {et(t)} 市值=${fs*pr:.2f}≥${TARGET} → 全卖 @${fpx:.4f} 收回${fs*fpx:.2f}")
            break
    if not fsold:
        ks=fh_sorted[p["fh_slug"]]; i=bisect.bisect_right(ks,p["fh_end"]-p["fh_start"])-1
        fe=fh_series[p["fh_slug"]][i][1] if fl=="up" else fh_series[p["fh_slug"]][i][2]
        fpx=fe; log_lines.append(f"  → 4h {fl.upper()} 未翻倍，到期结算价=${fe:.4f}（{('赢' if fe>=0.5 else '输')}）")
    # 1h 侧出场
    hsold=False; hpx=None
    for t in h1_sorted[p["h1_slug"]]:
        if t<ts: continue
        if t>p["h1_end"]: break
        pr=h1_up[p["h1_slug"]][t] if hl=="up" else h1_dn[p["h1_slug"]][t]
        if not hsold and hs*pr>=TARGET:
            hpx=pr*(1-SLIP); hsold=True
            log_lines.append(f"  → 1h {hl.upper()} 在 {et(t)} 市值=${hs*pr:.2f}≥${TARGET} → 全卖 @${hpx:.4f} 收回${hs*hpx:.2f}")
            break
    if not hsold:
        ts_l=h1_sorted[p["h1_slug"]]; i=bisect.bisect_right(ts_l,p["h1_end"])-1
        he=h1_up[p["h1_slug"]][ts_l[i]] if hl=="up" else h1_dn[p["h1_slug"]][ts_l[i]]
        hpx=he; log_lines.append(f"  → 1h {hl.upper()} 未翻倍，到期结算价=${he:.4f}（{('赢' if he>=0.5 else '输')}）")
    proceed=fs*fpx+hs*hpx
    pnl=proceed-cost
    log_lines.append(f"  回收总额 ${proceed:.2f} − 成本 ${cost} = 盈亏 ${pnl:+.2f}  (ROI {pnl/cost*100:+.1f}%)")
    return log_lines,pnl

# 找真实触发的对，区分赢/输
wins=[]; losses=[]
for p in pairs:
    e=find_entry(p)
    if e:
        _,pnl=simulate_full(p,e)
        (wins if pnl>0 else losses).append((p,e,pnl))

print("="*70)
print(f"THR={THR} GAP={GAP} 每侧${MIN_SIZE} 价差{SLIP*100:.0f}% | 触发局: 赢{len(wins)} / 输{len(losses)}")
print("="*70)

# 选一个赢局：让一侧中途翻倍(最直观的'免费期权'结构)
def story_score(p_e_pnl):
    p,e,pnl=p_e_pnl
    ts,fl,fb,hl,hb=e
    # 检查是否有一侧中途翻倍
    for k,u,d in fh_series[p["fh_slug"]]:
        t=p["fh_start"]+k
        if t<ts or t>p["fh_end"]: continue
        pr=u if fl=="up" else d
        if MIN_SIZE/(fb*(1+SLIP))*pr>=TARGET: return 1
    for t in h1_sorted[p["h1_slug"]]:
        if t<ts or t>p["h1_end"]: continue
        pr=h1_up[p["h1_slug"]][t] if hl=="up" else h1_dn[p["h1_slug"]][t]
        if MIN_SIZE/(hb*(1+SLIP))*pr>=TARGET: return 1
    return 0

good_win=max(wins,key=story_score)
p,e,pnl=good_win
m=fh_meta[p["fh_slug"]]
print(f"\n【真实赢局示例】4h窗口 {et(p['fh_start'])}→{et(p['fh_end'])}  slug={p['fh_slug']}")
print(f"嵌套1h市场 {p['h1_slug']}  ({et(p['h1_start'])}→{et(p['h1_end'])})")
ll,_=simulate_full(p,e)
for l in ll: print(l)

print("\n"+"="*70)
# 选一个输局
if losses:
    p,e,pnl=losses[0]
    print(f"\n【真实输局示例】4h窗口 {et(p['fh_start'])}→{et(p['fh_end'])}  slug={p['fh_slug']}")
    print(f"嵌套1h市场 {p['h1_slug']}  ({et(p['h1_start'])}→{et(p['h1_end'])})")
    ll,_=simulate_full(p,e)
    for l in ll: print(l)
