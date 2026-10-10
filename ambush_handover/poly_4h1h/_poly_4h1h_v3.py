# -*- coding: utf-8 -*-
"""
4h x 1h 嵌套周期条件入场回测 v3（30 天数据 + 真实成本）
交易规则同 v2（CEO 最终规则）。数据：poly_meta_30d_4h.json / poly_series_30d_4h.csv.gz / poly_meta_1h_last_30d.json / poly_trades_1h_last_30d.json
只配对已收盘(closed)的 4h 窗口，保证 4h 与 1h 价格路径完整到到期。
环境变量：SLIP（价差）, GAS（每笔费）, MIN_SIZE（每侧下单 USD）, OUT（输出文件）
"""
import os, io, csv, gzip, json, math, statistics, bisect
from collections import defaultdict

OD = os.path.dirname(os.path.abspath(__file__))
THR_LIST = [0.30, 0.25, 0.20, 0.15, 0.10, 0.05, 0.02]
GAP_LIST = [0.00, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35]
SLIP = float(os.environ.get("SLIP", "0.01"))
GAS = float(os.environ.get("GAS", "0.0"))
MIN_SIZE = float(os.environ.get("MIN_SIZE", "5.0"))
OUT = os.environ.get("OUT", "_poly_4h1h_30d_out.txt")

logf = io.open(os.path.join(OD, OUT), "w", encoding="utf-8")
def log(s=""):
    print(s); logf.write(s + "\n"); logf.flush()

log("=" * 70)
log("4h x 1h 嵌套周期条件入场回测 v3（30 天数据 · 真实成本）")
log(f"成本假设：每侧 MIN_SIZE={MIN_SIZE} USD，价差 SLIP={SLIP*100:.1f}%，每笔固定费 GAS={GAS}")

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
log(f"4h 市场（含未收盘） {len(fh_series)} 个")

h1_meta = load_json("poly_meta_1h_last_30d.json")
h1_trades = load_json("poly_trades_1h_last_30d.json")
log(f"1h 市场 {len(h1_meta)} 个，成交 {len(h1_trades)} 笔")

def build_h1_series(trades_for_slug):
    trades_for_slug.sort(key=lambda x: int(x["timestamp"]))
    up_path = {}; dn_path = {}; last_up = 0.5; last_dn = 0.5
    for tr in trades_for_slug:
        ts = int(tr["timestamp"]); pr = float(tr["price"]); side = tr["outcome"]
        if side == "Up": last_up = pr; last_dn = 1.0 - pr
        else: last_dn = pr; last_up = 1.0 - pr
        up_path[ts] = last_up; dn_path[ts] = last_dn
    return up_path, dn_path

by_slug = defaultdict(list)
for tr in h1_trades: by_slug[tr["slug"]].append(tr)
h1_up = {}; h1_dn = {}; h1_sorted = {}
for slug, trs in by_slug.items():
    up, dn = build_h1_series(trs); h1_up[slug] = up; h1_dn[slug] = dn; h1_sorted[slug] = sorted(up.keys())

def build_fh_series(recs):
    recs.sort(key=lambda x: x[0]); up_path = {}; dn_path = {}; lu = 0.5; ld = 0.5
    for k, up, dn in recs: up_path[k] = up; dn_path[k] = dn; lu = up; ld = dn
    return up_path, dn_path

fh_up = {}; fh_dn = {}; fh_sorted = {}
for slug, recs in fh_series.items():
    u, d = build_fh_series(recs); fh_up[slug] = u; fh_dn[slug] = d; fh_sorted[slug] = sorted(u.keys())

# ---- 配对：只取已收盘 4h 窗口 ----
pairs = []
skipped_open = 0
for slug, recs in fh_series.items():
    m = fh_meta.get(slug, {})
    if not m.get("closed"):
        skipped_open += 1; continue
    start4 = m["start"]; end4 = m["end"]
    h1 = next((x for x in h1_meta if x["4h_slug"] == slug), None)
    if not h1:
        continue
    pairs.append({
        "fh_slug": slug, "h1_slug": h1["1h_slug"],
        "fh_start": start4, "fh_end": end4,
        "h1_start": h1["1h_start_utc"], "h1_end": h1["1h_start_utc"] + 3600,
        "entry": start4 + 10800,
    })
log(f"跳过未收盘 4h 窗口 {skipped_open} 个；成功配对（已收盘） {len(pairs)} 对")

# ---- 预计算候选 ----
fh_path_sorted = {slug: [(k, fh_up[slug][k], fh_dn[slug][k]) for k in fh_sorted[slug]] for slug in fh_up}
h1_path_sorted = {slug: [(ts, h1_up[slug][ts], h1_dn[slug][ts]) for ts in h1_sorted[slug]] for slug in h1_up}

def build_cands(p):
    fh_ps = fh_path_sorted[p["fh_slug"]]; fh_ks = [x[0] for x in fh_ps]
    cands = []
    for ts in h1_sorted[p["h1_slug"]]:
        if ts < p["h1_start"] or ts > p["h1_end"]: continue
        fh_k = ts - p["fh_start"]; i = bisect.bisect_right(fh_ks, fh_k) - 1
        if i < 0: continue
        fh_u, fh_d = fh_ps[i][1], fh_ps[i][2]
        h1_u = h1_up[p["h1_slug"]][ts]; h1_d = h1_dn[p["h1_slug"]][ts]
        cands.append((ts, fh_u, fh_d, h1_u, h1_d))
    return cands

for p in pairs:
    p["cands"] = build_cands(p); p["fh_ps"] = fh_path_sorted[p["fh_slug"]]; p["h1_ps"] = h1_path_sorted[p["h1_slug"]]

def simulate(p, buy_threshold=0.3, gap_floor=0.0):
    cands = p["cands"]
    if not cands: return None, False, {"reason": "no_price"}
    entry = None; entry_info = None
    for ts, fh_u, fh_d, h1_u, h1_d in cands:
        if fh_u < fh_d: fh_leg, fh_buy = "up", fh_u
        elif fh_d < fh_u: fh_leg, fh_buy = "dn", fh_d
        else: continue
        if h1_u < h1_d: h1_leg, h1_buy = "up", h1_u
        elif h1_d < h1_u: h1_leg, h1_buy = "dn", h1_d
        else: continue
        if fh_leg == h1_leg: continue
        if fh_buy >= buy_threshold or h1_buy >= buy_threshold: continue
        gap = abs(fh_u - 0.5)
        if gap < gap_floor: continue
        entry = ts; entry_info = (fh_leg, fh_buy, h1_leg, h1_buy, gap); break
    if entry is None: return None, False, {"reason": "no_entry"}
    fh_leg, fh_buy, h1_leg, h1_buy, gap = entry_info
    fh_shares = MIN_SIZE / (fh_buy * (1 + SLIP)); h1_shares = MIN_SIZE / (h1_buy * (1 + SLIP))
    target = 2.0 * MIN_SIZE
    fh_sold = False; fh_sell_price = None; h1_sold = False; h1_sell_price = None
    for ts, up, dn in p["h1_ps"]:
        if ts < entry: continue
        if ts > p["h1_end"]: break
        h1_price = up if h1_leg == "up" else dn
        if not h1_sold and h1_shares * h1_price >= target:
            h1_sell_price = h1_price * (1 - SLIP); h1_sold = True
    for k, up, dn in p["fh_ps"]:
        t_abs = p["fh_start"] + k
        if t_abs < entry: continue
        if t_abs > p["fh_end"]: break
        fh_price = up if fh_leg == "up" else dn
        if not fh_sold and fh_shares * fh_price >= target:
            fh_sell_price = fh_price * (1 - SLIP); fh_sold = True
    if not fh_sold:
        fh_ps = p["fh_ps"]; ks = [x[0] for x in fh_ps]
        i = bisect.bisect_right(ks, p["fh_end"] - p["fh_start"]) - 1
        fe = fh_ps[i][1] if fh_leg == "up" else fh_ps[i][2]; fh_sell_price = fe if fe is not None else 0.5
    if not h1_sold:
        h1_ps = p["h1_ps"]; ts_list = [x[0] for x in h1_ps]
        i = bisect.bisect_right(ts_list, p["h1_end"]) - 1
        he = h1_ps[i][1] if h1_leg == "up" else h1_ps[i][2]; h1_sell_price = he if he is not None else 0.5
    fh_proceeds = fh_shares * fh_sell_price; h1_proceeds = h1_shares * h1_sell_price
    n_trades = 2 + int(fh_sold) + int(h1_sold)
    total = fh_proceeds + h1_proceeds - n_trades * GAS
    pnl = total - 2 * MIN_SIZE
    return pnl, True, {"entry": entry, "gap": gap, "pnl": pnl}

log("\n===== 阈值 x ref gap 扫描（30 天 · 真实成本） =====")
results = []
for thr in THR_LIST:
    for gf in GAP_LIST:
        entered = []
        for p in pairs:
            pnl, ok, info = simulate(p, thr, gf)
            if ok: entered.append(pnl)
        if not entered:
            log(f"THR={thr:.2f} GAP={gf:.2f}: 0 局入场"); continue
        n = len(entered); mean_pnl = statistics.mean(entered)
        std_pnl = statistics.stdev(entered) if n > 1 else 0
        t = mean_pnl / (std_pnl / math.sqrt(n)) if std_pnl > 0 else 0
        wins = sum(1 for x in entered if x > 0); roi = mean_pnl / (2 * MIN_SIZE)
        log(f"THR={thr:.2f} GAP={gf:.2f}: n={n:3d}  mean/pair(MIN_SIZE=1)={mean_pnl:+.4f}  ROI%={roi*100:+.2f}  t={t:+.2f}  win={wins}/{n}")
        results.append((thr, gf, n, mean_pnl, roi, t, wins))

if results:
    best = max(results, key=lambda x: x[5] if x[5] > 0 else -1e9)
    log(f"\n最佳（按 t 值）: THR={best[0]:.2f} GAP={best[1]:.2f} n={best[2]} ROI%={best[4]*100:.2f} t={best[5]:+.2f}")
    log(f"实盘每侧 5USD：该配置每对期望 ≈ {best[3]*5:+.2f} USD，{best[2]} 对期望 ≈ {best[3]*5*best[2]:+.2f} USD（投入 {best[2]*10} USD）")
log("\n完成"); logf.close()
