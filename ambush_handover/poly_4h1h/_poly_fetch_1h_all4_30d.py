# -*- coding: utf-8 -*-
"""补抓每个 4h 窗口的 第1/2/3 个小时 1h 市场（第4个小时=最后一小时 已有数据），合并为全 4 小时数据集。
输出：poly_meta_1h_all4_30d.json / poly_trades_1h_all4_30d.json
1h_start = 4h_start + hour_index*3600, hour_index ∈ {0,1,2,3}
"""
import requests, json, time, os
from datetime import datetime, timedelta

proxies = {"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"}
base = "https://gamma-api.polymarket.com"
data_base = "https://data-api.polymarket.com"
UA = {"User-Agent": "Mozilla/5.0"}
OD = os.path.dirname(os.path.abspath(__file__))

def slug_time(h):
    if h == 0: return "12am"
    if h == 12: return "12pm"
    if h < 12: return f"{h}am"
    return f"{h-12}pm"

def slug_date(dt):
    months = ["january","february","march","april","may","june","july","august","september","october","november","december"]
    return f"{months[dt.month-1]}-{dt.day}-{dt.year}"

def make_slug_from_utc(ts_utc):
    dt_et = datetime.utcfromtimestamp(ts_utc) - timedelta(hours=4)
    return f"bitcoin-up-or-down-{slug_date(dt_et)}-{slug_time(dt_et.hour)}-et"

def fetch_event(slug):
    try:
        r = requests.get(base + "/events", params={"slug": slug}, proxies=proxies, timeout=25, headers=UA)
        if r.status_code != 200: return None
        arr = r.json()
        return arr[0] if arr else None
    except Exception:
        return None

def main():
    meta4h = json.load(open(os.path.join(OD, "poly_meta_30d_4h.json")))
    # 已有：最后一小时（hour_index=3）
    old_meta = json.load(open(os.path.join(OD, "poly_meta_1h_last_30d.json")))
    old_trades = json.load(open(os.path.join(OD, "poly_trades_1h_last_30d.json")))
    print(f"已有最后一小时：meta {len(old_meta)} 个，成交 {len(old_trades)} 笔")

    # 先用已有数据（hour_index=3）
    all_meta = []
    for m in old_meta:
        mm = dict(m); mm["hour_index"] = 3
        all_meta.append(mm)
    known_slugs = {m["1h_slug"] for m in all_meta}

    # 构造 第1/2/3 个小时的目标
    targets = []
    for s, m in sorted(meta4h.items(), key=lambda x: x[1]["start"]):
        if "4h" not in s or not m.get("closed"): continue
        fh_start = int(m["start"]); fh_end = int(m["end"])
        for hi in (0, 1, 2):
            hs = fh_start + hi * 3600
            sl = make_slug_from_utc(hs)
            if sl in known_slugs: continue
            targets.append({"4h_slug": s, "4h_start_utc": fh_start, "4h_end_utc": fh_end,
                            "1h_slug": sl, "1h_start_utc": hs, "hour_index": hi})
    print(f"待补抓目标（第1/2/3小时） {len(targets)} 个")

    found = []
    for i, t in enumerate(targets):
        ev = fetch_event(t["1h_slug"])
        if ev:
            mk = (ev.get("markets") or [None])[0]
            if mk:
                t.update({"title": ev.get("title"), "startDate": mk.get("startDate"),
                          "endDate": mk.get("endDate"), "outcomePrices": mk.get("outcomePrices"),
                          "question": mk.get("question"), "conditionId": mk.get("conditionId"),
                          "market_id": mk.get("id"), "volume": mk.get("volume"),
                          "resolved": mk.get("umaResolutionStatus")})
                found.append(t)
            else:
                print(f"  NO_MARKET {t['1h_slug']}")
        else:
            print(f"  MISSING {t['1h_slug']} (hi={t['hour_index']})")
        if (i + 1) % 50 == 0:
            print(f"  ...元数据进度 {i+1}/{len(targets)}，已找到 {len(found)}")
        time.sleep(0.12)

    print(f"\n补抓成功 {len(found)}/{len(targets)} 个")
    all_meta.extend(found)

    # 抓成交（只保留必要字段，省空间）
    all_trades = []
    for tr in old_trades:
        all_trades.append({"slug": tr["slug"], "timestamp": tr["timestamp"],
                           "price": tr["price"], "outcome": tr["outcome"]})
    print(f"已并入最后一小时成交 {len(all_trades)} 笔")

    for i, t in enumerate(found):
        cond = t.get("conditionId")
        if not cond: continue
        offset = 0; mt = []
        while True:
            try:
                r = requests.get(data_base + "/trades", params={"market": cond, "limit": 10000, "offset": offset},
                                 proxies=proxies, timeout=30, headers=UA)
                if r.status_code != 200: break
                data = r.json()
                if not isinstance(data, list) or not data: break
                mt.extend(data)
                if len(data) < 10000: break
                offset += len(data)
                if offset > 200000: break
            except Exception:
                break
        for tr in mt:
            all_trades.append({"slug": t["1h_slug"], "timestamp": tr["timestamp"],
                               "price": tr["price"], "outcome": tr["outcome"]})
        if (i + 1) % 50 == 0:
            print(f"  ...成交进度 {i+1}/{len(found)}，累计 {len(all_trades)} 笔")
        time.sleep(0.12)

    print(f"\n总 1h 市场 {len(all_meta)} 个（4h窗口 × 4小时）")
    print(f"总成交 {len(all_trades)} 笔")
    json.dump(all_meta, open(os.path.join(OD, "poly_meta_1h_all4_30d.json"), "w"), default=str)
    json.dump(all_trades, open(os.path.join(OD, "poly_trades_1h_all4_30d.json"), "w"), default=str)
    print("已保存 poly_meta_1h_all4_30d.json / poly_trades_1h_all4_30d.json")

if __name__ == "__main__":
    main()
