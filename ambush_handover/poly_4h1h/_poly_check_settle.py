# -*- coding: utf-8 -*-
"""检查 4h 市场 meta 字段是否有真实结算结果(outcome/winner)，以及收盘前最后价格分布。"""
import os, io, json, gzip, csv, bisect
from collections import defaultdict
OD = os.path.dirname(os.path.abspath(__file__))
fh_meta = json.load(io.open(os.path.join(OD, "poly_meta_30d_4h.json"), encoding="utf-8"))
print("FH META 字段:", list(fh_meta[list(fh_meta.keys())[0]].keys()))
k0=list(fh_meta.keys())[0]
print("样本:", json.dumps(fh_meta[k0], ensure_ascii=False)[:600])

fh_series=defaultdict(list)
with gzip.open(os.path.join(OD,"poly_series_30d_4h.csv.gz"),"rt",encoding="utf-8") as f:
    rd=csv.reader(f); next(rd)
    for slug,k,up,dn,ua,da,mn,mx in rd:
        if "4h" not in slug: continue
        fh_series[slug].append((int(k),float(up),float(dn)))

# 统计已收盘 4h 市场的"最后 Up 价"分布
import collections
buckets=collections.Counter()
n_closed=0
for slug,recs in fh_series.items():
    m=fh_meta.get(slug,{})
    if not m.get("closed"): continue
    n_closed+=1
    recs.sort(key=lambda x:x[0])
    last_up=recs[-1][1]
    if last_up<0.01: buckets["<0.01"]+=1
    elif last_up<0.1: buckets["0.01~0.1"]+=1
    elif last_up<0.9: buckets["0.1~0.9(中间态)"]+=1
    elif last_up<0.99: buckets["0.9~0.99"]+=1
    else: buckets[">=0.99"]+=1
print(f"\n已收盘 4h 市场数={n_closed}")
print("最后 Up 价分布:", dict(buckets))
