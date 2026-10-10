# -*- coding: utf-8 -*-
"""
4h x 1h 嵌套回测 v5 —— CEO 两条修正：
  修正1) 下单单位是「份」不是「元」：orderMinSize=5 份；本轮按 每侧 1 USD 下注，
         1USD 买不满 5 份时按 5 份补齐（并同时给出"严格跳过"对照）。
  修正2) 1h 不再只用最后一小时 —— 第1/2/3/4 个小时都扫，符合条件的都算。
  沿用 v4 的真实二元结算（$1/$0）。
环境变量：SLIP / MIN_SIZE(=1.0) / MIN_SHARES(=5) / MODE(A|B) / THR_LIST / OUT
"""
import os, io, csv, gzip, json, math, statistics, bisect
from collections import defaultdict
from datetime import datetime, timedelta

OD = os.path.dirname(os.path.abspath(__file__))
SLIP = float(os.environ.get("SLIP", "0.01"))
MIN_SIZE = float(os.environ.get("MIN_SIZE", "1.0"))     # 每侧下注 USD
MIN_SHARES = float(os.environ.get("MIN_SHARES", "5.0")) # 交易所最小份数
MODE = os.environ.get("MODE", "A")   # A=每 4h 窗口只入最早一次; B=每个满足条件的小时各入一次
THR_LIST = [float(x) for x in os.environ.get("THR_LIST", "0.30,0.25,0.20,0.15,0.10,0.05,0.02").split(",")]
HOURS = [int(x) for x in os.environ.get("HOURS", "0,1,2,3").split(",")]
GAP = float(os.environ.get("GAP", "0.00"))
OUT = os.environ.get("OUT", "_poly_4h1h_v5_out.txt")
logf = io.open(os.path.join(OD, OUT), "w", encoding="utf-8")
def log(s=""):
    print(s); logf.write(s + "\n"); logf.flush()

def et(ts):
    return (datetime.utcfromtimestamp(ts) - timedelta(hours=4)).strftime("%m/%d %H:%M ET")

def load_json(name):
    """自动识别 name 或 name.gz（本目录数据以 .gz 存放以适配 git）"""
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
h1_meta = load_json("poly_meta_1h_all4_30d.json")
h1_trades = load_json("poly_trades_1h_all4_30d.json")

def parse_op(v):
    if v is None: return None
    if isinstance(v, str):
        try: v = json.loads(v)
        except Exception: return None
    if isinstance(v, list) and len(v) == 2:
        try: return [float(x) for x in v]
        except Exception: return None
    return None
h1_winner = {}
for h in h1_meta:
    op = parse_op(h.get("outcomePrices"))
    if op and {op[0], op[1]} == {0.0, 1.0}:
        h1_winner[h["1h_slug"]] = "Up" if op[0] >= 1.0 else "Down"
log(f"1h 市场可判定胜负 {len(h1_winner)} 个 / 共 {len(h1_meta)} 个")

by_slug = defaultdict(list)
for tr in h1_trades: by_slug[tr["slug"]].append(tr)
def build_h1(trs):
    trs.sort(key=lambda x: int(x["timestamp"])); up={}; dn={}
    for tr in trs:
        ts=int(tr["timestamp"]); pr=float(tr["price"])
        lu,ld = (pr,1.0-pr) if tr["outcome"]=="Up" else (1.0-pr,pr)
        up[ts]=lu; dn[ts]=ld
    return up,dn
h1_up={}; h1_dn={}; h1_sorted={}
for slug,trs in by_slug.items():
    u,d=build_h1(trs); h1_up[slug]=u; h1_dn[slug]=d; h1_sorted[slug]=sorted(u.keys())
fh_up={}; fh_dn={}; fh_sorted={}
for slug,recs in fh_series.items():
    recs.sort(key=lambda x:x[0])
    fh_up[slug]={k:u for k,u,d in recs}; fh_dn[slug]={k:d for k,u,d in recs}
    fh_sorted[slug]=sorted(fh_up[slug].keys())

# 配对：每个 4h 窗口 × 4 个小时
windows=[]
for slug in fh_series:
    m=fh_meta.get(slug,{})
    if not m.get("closed") or m.get("winner") not in ("Up","Down"): continue
    sub=[]
    for hi in HOURS:
        hs=int(m["start"])+hi*3600
        cand=next((x for x in h1_meta if x["4h_slug"]==slug and int(x["1h_start_utc"])==hs), None)
        # 必须同时有真实成交数据（部分市场元数据存在但盘口为空）
        if cand and cand["1h_slug"] in h1_winner and cand["1h_slug"] in h1_sorted:
            sub.append({"hi":hi,"slug":cand["1h_slug"],"s":hs,"e":hs+3600,"win":h1_winner[cand["1h_slug"]]})
    if sub:
        windows.append({"fh":slug,"fs":int(m["start"]),"fe":int(m["end"]),"fw":m["winner"],"hours":sub})
n_hours=sum(len(w["hours"]) for w in windows)
log(f"4h 窗口 {len(windows)} 个；可用 1h 子市场 {n_hours} 个（平均每窗口 {n_hours/max(1,len(windows)):.2f} 个）")

def check(ts, fh_slug, fs, h1_slug, thr, gap):
    ks=fh_sorted[fh_slug]; i=bisect.bisect_right(ks, ts-fs)-1
    if i<0: return None
    k=ks[i]; fu,fd=fh_up[fh_slug][k], fh_dn[fh_slug][k]
    hu,hd=h1_up[h1_slug][ts], h1_dn[h1_slug][ts]
    if fu<fd: fl,fb="UP",fu
    elif fd<fu: fl,fb="DN",fd
    else: return None
    if hu<hd: hl,hb="UP",hu
    elif hd<hu: hl,hb="DN",hd
    else: return None
    if fl==hl: return None
    if fb>=thr or hb>=thr: return None
    if abs(fu-0.5)<gap: return None
    return fl,fb,hl,hb

def size_leg(price, strict):
    """返回 (份数, 成本USD, 是否被最小份数约束)"""
    sh = MIN_SIZE/(price*(1+SLIP))
    if sh >= MIN_SHARES:
        return sh, MIN_SIZE, False
    if strict:
        return None, None, True
    return MIN_SHARES, MIN_SHARES*price*(1+SLIP), True

def run_trade(w, h, ts, fl, fb, hl, hb, strict=False):
    fs_,fc_,fclip = size_leg(fb, strict)
    hs_,hc_,hclip = size_leg(hb, strict)
    if fs_ is None or hs_ is None: return None
    total=fc_+hc_; target=total
    # 4h 腿
    fpx=None; fsold=False
    for k in fh_sorted[w["fh"]]:
        t=w["fs"]+k
        if t<ts or t>w["fe"]: continue
        pr=fh_up[w["fh"]][k] if fl=="UP" else fh_dn[w["fh"]][k]
        if fs_*pr>=target:
            fpx=pr*(1-SLIP); fsold=True; break
    if not fsold:
        fpx = 1.0 if fl==w["fw"] else 0.0
    # 1h 腿
    hpx=None; hsold=False
    for t in h1_sorted[h["slug"]]:
        if t<ts or t>h["e"]: continue
        pr=h1_up[h["slug"]][t] if hl=="UP" else h1_dn[h["slug"]][t]
        if hs_*pr>=target:
            hpx=pr*(1-SLIP); hsold=True; break
    if not hsold:
        hpx = 1.0 if hl==h["win"] else 0.0
    pnl = fs_*fpx + hs_*hpx - total
    return {"pnl":pnl,"cost":total,"hi":h["hi"],"ts":ts,"fb":fb,"hb":hb,"fl":fl,"hl":hl,
            "clip":fclip or hclip,"dbl":(fsold or hsold),"fdbl":fsold,"hdbl":hsold,"fpx":fpx,"hpx":hpx}

def scan(thr, gap, mode, strict=False):
    out=[]; clipped=0
    for w in windows:
        entered_this_window=0
        for h in w["hours"]:
            if h["slug"] not in h1_sorted: continue
            if mode=="A" and entered_this_window: break
            hit=None
            for ts in h1_sorted[h["slug"]]:
                if ts<h["s"] or ts>h["e"]: continue
                c=check(ts, w["fh"], w["fs"], h["slug"], thr, gap)
                if c: hit=(ts,)+c; break
            if not hit: continue
            r=run_trade(w,h,hit[0],hit[1],hit[2],hit[3],hit[4],strict)
            if r is None:
                clipped+=1; continue
            out.append(r); entered_this_window+=1
    return out, clipped

def stats(rows):
    if not rows: return None
    ps=[r["pnl"] for r in rows]; n=len(ps)
    mean=statistics.mean(ps); sd=statistics.stdev(ps) if n>1 else 0
    t=mean/(sd/math.sqrt(n)) if sd>0 else 0
    cost=statistics.mean([r["cost"] for r in rows])
    return n, mean, cost, t, sum(1 for x in ps if x>0), sum(ps)

log("\n"+"="*104)
log(f"【v5】每侧 {MIN_SIZE} USD · 最小 {MIN_SHARES} 份 · 价差 {SLIP*100:.1f}% · 真实二元结算($1/$0) · 模式 {MODE}")
log("="*104)
log(f"{'THR':>6} | {'n':>4} {'每局成本$':>9} {'每局盈亏$':>10} {'ROI%':>8} {'t':>7} {'胜率':>9} {'总成本$':>8} {'总盈亏$':>9} | {'被5份门槛挡掉':>11}")
best=None
for thr in THR_LIST:
    rows,_=scan(thr,GAP,MODE)
    rows_s,clip=scan(thr,GAP,MODE,strict=True)
    s=stats(rows)
    if not s: log(f"{thr:>6.2f} | 无入场"); continue
    n,mean,cost,t,w,tot=s
    roi=mean/cost*100
    log(f"{thr:>6.2f} | {n:>4d} {cost:>9.2f} {mean:>+10.3f} {roi:>+8.2f} {t:>+7.2f} {w:>4d}/{n:<4d} {cost*n:>8.0f} {tot:>+9.1f} | {clip:>11d}")
    if best is None or t>best[1]: best=(thr,t,n,mean,cost,roi,w,tot)

if best:
    thr,t,n,mean,cost,roi,w,tot=best
    log(f"\n最佳(按 t)：THR={thr:.2f}  n={n}  每局成本${cost:.2f}  每局盈亏${mean:+.3f}  ROI={roi:+.2f}%  t={t:+.2f}  胜{w}/{n}")
    log(f"30天累计：总成本 ${cost*n:.0f}，总盈亏 ${tot:+.1f}")

log("\n"+"="*104)
log(f"【按「第几个小时」拆解】模式{MODE} · 参与小时 {HOURS}")
for thr in THR_LIST:
    rows_b,_=scan(thr,GAP,MODE)
    if not rows_b: continue
    log(f"\n-- THR={thr:.2f} --")
    log(f"{'小时':>6} | {'n':>4} {'每局盈亏$':>10} {'ROI%':>8} {'t':>7} {'胜率':>9}")
    for hi in HOURS:
        sub=[r for r in rows_b if r["hi"]==hi]
        s=stats(sub)
        if not s: log(f"{'第'+str(hi+1)+'小时':>6} | 无入场"); continue
        n2,mean2,cost2,t2,w2,tot2=s
        afb=statistics.mean([r["fb"] for r in sub]); ahb=statistics.mean([r["hb"] for r in sub])
        dr=sum(1 for r in sub if r["dbl"])/n2*100
        fdr=sum(1 for r in sub if r["fdbl"])/n2*100
        hdr=sum(1 for r in sub if r["hdbl"])/n2*100
        log(f"{'第'+str(hi+1)+'小时':>6} | {n2:>4d} {mean2:>+10.3f} {mean2/cost2*100:>+8.2f} {t2:>+7.2f} {w2:>4d}/{n2:<4d} | 均价{afb:.3f}/{ahb:.3f} 保本{dr:5.1f}% (4h腿{fdr:4.1f}% 1h腿{hdr:4.1f}%)")

# 双模式对照
log("\n"+"="*104)
log("【模式 A vs B】A=每4h窗口只入最早一次；B=每个满足条件的小时各入一次")
log(f"{'THR':>6} | {'A: n':>6} {'A: ROI%':>9} {'A: t':>7} | {'B: n':>6} {'B: ROI%':>9} {'B: t':>7}")
for thr in THR_LIST:
    ra,_=scan(thr,GAP,"A"); rb,_=scan(thr,GAP,"B")
    sa=stats(ra); sb=stats(rb)
    a=f"{sa[0]:>6d} {sa[1]/sa[2]*100:>+9.2f} {sa[3]:>+7.2f}" if sa else f"{0:>6d} {'--':>9} {'--':>7}"
    b=f"{sb[0]:>6d} {sb[1]/sb[2]*100:>+9.2f} {sb[3]:>+7.2f}" if sb else f"{0:>6d} {'--':>9} {'--':>7}"
    log(f"{thr:>6.2f} | {a} | {b}")

log("\n完成"); logf.close()
