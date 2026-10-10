# -*- coding: utf-8 -*-
"""
仅抓取 Polymarket BTC Up/Down 4h 周期最近 N 小时数据，落「每秒价格序列」。
（4h×1h 回测只需要 4h 价格路径，不抓 5m/15m 以省资源）
输出：
  poly_meta_30d_4h.json      slug -> 元数据
  poly_series_30d_4h.csv.gz  slug,k,up,dn,ua,da,mn,mx
用法： POLY_HOURS=720 python _poly_fetch_4h30.py
"""
import time, json, csv, gzip, io, os, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

HOURS = float(os.environ.get("POLY_HOURS", "720"))
TAG = os.environ.get("POLY_TAG", "30d_4h")
OUTDIR = os.path.dirname(os.path.abspath(__file__))
LOG = io.open(os.path.join(OUTDIR, f"_poly_fetch_{TAG}_out.txt"), "w", encoding="utf-8")
_lk = threading.Lock()
def log(s=""):
    with _lk:
        print(s, flush=True); LOG.write(s + "\n"); LOG.flush()

_tl = threading.local()
def sess():
    s = getattr(_tl, "s", None)
    if s is None:
        s = requests.Session(); s.trust_env = False
        s.proxies = {"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"}
        rt = Retry(total=4, backoff_factor=1.2, status_forcelist=[429, 500, 502, 503, 504],
                   allowed_methods=["GET"], raise_on_status=False)
        ad = HTTPAdapter(max_retries=rt, pool_connections=8, pool_maxsize=8)
        s.mount("https://", ad); s.mount("http://", ad)
        _tl.s = s
    return s

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
def g(url, params=None, tries=3):
    for a in range(tries):
        try:
            r = sess().get(url, params=params, headers=UA, timeout=45)
            if r.status_code == 200:
                return r
        except Exception:
            pass
        time.sleep(1.5 + a * 2)
    return None

PERIODS = [("4h", 14400)]
NOW = int(time.time()); END = NOW; START = NOW - int(HOURS * 3600)

slugs = []
for per, sec in PERIODS:
    t = (START // sec) * sec
    while t <= END:
        slugs.append((per, sec, t, f"btc-updown-{per}-{t}")); t += sec
log(f"[{TAG}] 窗口 {HOURS}h | 4h 市场 {len(slugs)} 个")

meta = {}
def fetch_meta(item):
    per, sec, ts, slug = item
    r = g("https://gamma-api.polymarket.com/events", {"slug": slug})
    if r is None: return slug, None
    try: arr = r.json()
    except Exception: return slug, None
    if not arr: return slug, None
    ev = arr[0]; mks = ev.get("markets") or []
    if not mks: return slug, None
    mk = mks[0]
    oc, pr = mk.get("outcomes"), mk.get("outcomePrices")
    try: oc = json.loads(oc) if isinstance(oc, str) else oc
    except Exception: pass
    try: pr = json.loads(pr) if isinstance(pr, str) else pr
    except Exception: pass
    winner = None
    if mk.get("closed") and str(mk.get("umaResolutionStatus")) == "resolved" and oc and pr:
        for i, p in enumerate(pr):
            if str(p) in ("1", "1.0"): winner = oc[i] if i < len(oc) else str(i)
    return slug, {"period": per, "ts": ts, "sec": sec, "start": ts, "end": ts + sec,
                  "conditionId": mk.get("conditionId"), "closed": bool(mk.get("closed")),
                  "resolved": str(mk.get("umaResolutionStatus")),
                  "outcomes": oc, "outcomePrices": pr, "winner": winner,
                  "question": ev.get("title")}

t0 = time.time(); done = 0
with ThreadPoolExecutor(max_workers=16) as ex:
    for f in as_completed({ex.submit(fetch_meta, it): it for it in slugs}):
        slug, m = f.result(); done += 1
        if m: meta[slug] = m
        if done % 50 == 0: log(f"  元数据 {done}/{len(slugs)} 有效 {len(meta)} ({time.time()-t0:.0f}s)")
log(f"元数据完成 {len(meta)} ({time.time()-t0:.0f}s)")

def build(m, rows):
    st, en = m["start"], m["end"]
    i, n = 0, len(rows)
    lu = ld = tu = td = None
    arr = []
    for s in range(st, en):
        mn, mx = None, None
        while i < n and rows[i][0] <= s:
            tt, oi, p = rows[i]
            if oi == 0:
                lu, tu = p, tt
                mn = p if mn is None else min(mn, p)
                mx = p if mx is None else max(mx, p)
            else:
                ld, td = p, tt
            i += 1
        up = lu if lu is not None else (1 - ld if ld is not None else None)
        dn = ld if ld is not None else (1 - lu if lu is not None else None)
        if up is None:
            arr.append(None)
        else:
            arr.append((round(up, 4), round(dn, 4), (s - tu) if tu is not None else 999999,
                        (s - td) if td is not None else 999999,
                        mn if mn is not None else "", mx if mx is not None else ""))
    return arr

def fetch_series(item):
    slug, m = item
    cond = m["conditionId"]; rows = []
    for page in range(12):
        r = g("https://data-api.polymarket.com/trades",
              {"market": cond, "limit": 10000, "offset": page * 10000})
        if r is None: break
        try: d = r.json()
        except Exception: break
        if not d: break
        for x in d:
            try: rows.append((int(x["timestamp"]), int(x.get("outcomeIndex", 0)), float(x["price"])))
            except Exception: pass
        if len(d) < 10000: break
    rows.sort()
    return slug, build(m, rows)

fp = gzip.open(os.path.join(OUTDIR, f"poly_series_{TAG}.csv.gz"), "wt", newline="", encoding="utf-8")
w = csv.writer(fp); w.writerow(["slug", "k", "up", "dn", "ua", "da", "mn", "mx"])
t0 = time.time(); done = 0; npts = 0
todo = [(s, v) for s, v in meta.items() if v.get("conditionId")]
with ThreadPoolExecutor(max_workers=12) as ex:
    for f in as_completed({ex.submit(fetch_series, it): it for it in todo}):
        slug, arr = f.result(); done += 1
        for k, v in enumerate(arr):
            if v is None: continue
            w.writerow([slug, k, v[0], v[1], v[2], v[3], v[4], v[5]]); npts += 1
        if done % 50 == 0: log(f"  序列 {done}/{len(todo)}  点 {npts} ({time.time()-t0:.0f}s)")
fp.close()
log(f"序列完成 {npts} 点 ({time.time()-t0:.0f}s)")
json.dump(meta, io.open(os.path.join(OUTDIR, f"poly_meta_{TAG}.json"), "w", encoding="utf-8"),
          ensure_ascii=False)
res = {}
for s, v in meta.items():
    r0 = res.setdefault(v["period"], [0, 0, 0]); r0[0] += 1
    if v["winner"] == "Up": r0[1] += 1
    elif v["winner"] == "Down": r0[2] += 1
log("\n周期   总数   Up胜  Down胜  未结算")
for p, _ in PERIODS:
    a, b, c = res.get(p, [0, 0, 0]); log(f"  {p:<5}{a:>5}{b:>7}{c:>8}{a-b-c:>9}")
log("\nDONE"); LOG.close()
