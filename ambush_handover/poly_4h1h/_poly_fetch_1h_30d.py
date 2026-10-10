# -*- coding: utf-8 -*-
"""抓取 30 天 4h×1h 嵌套：每个已收盘 4h 窗口最后一个 1h 市场的元数据 + 逐笔成交。
读取 poly_meta_30d_4h.json（4h 窗口），输出 poly_meta_1h_last_30d.json / poly_trades_1h_last_30d.json。
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
    meta = json.load(open(os.path.join(OD, "poly_meta_30d_4h.json")))
    fourh = [(s, m) for s, m in meta.items() if "4h" in s]
    print(f"已有 4h 市场 {len(fourh)} 个")

    targets = []
    for s, m in sorted(fourh, key=lambda x: x[1]["start"]):
        # 只配对已收盘的 4h 窗口（保证 4h 与 1h 均有完整价格路径到到期）
        if not m.get("closed"):
            continue
        fh_start = int(m["start"]); fh_end = int(m["end"])
        last_1h_start = fh_end - 3600
        targets.append({
            "4h_slug": s, "4h_title": m.get("question", ""),
            "4h_start_utc": fh_start, "4h_end_utc": fh_end,
            "1h_slug": make_slug_from_utc(last_1h_start), "1h_start_utc": last_1h_start,
        })
    print(f"目标 1h 市场数（已收盘 4h 窗口） {len(targets)}")

    found = []
    for t in targets:
        ev = fetch_event(t["1h_slug"])
        if ev:
            mk = (ev.get("markets") or [None])[0]
            if mk:
                t.update({
                    "title": ev.get("title"), "startDate": mk.get("startDate"),
                    "endDate": mk.get("endDate"), "outcomePrices": mk.get("outcomePrices"),
                    "question": mk.get("question"), "conditionId": mk.get("conditionId"),
                    "market_id": mk.get("id"), "volume": mk.get("volume"),
                    "resolved": mk.get("umaResolutionStatus"),
                })
                found.append(t)
                print(f"  FOUND {t['1h_slug']} | prices={mk.get('outcomePrices')} | status={mk.get('umaResolutionStatus')}")
            else:
                print(f"  NO_MARKET {t['1h_slug']}")
        else:
            print(f"  MISSING {t['1h_slug']}")
        time.sleep(0.15)

    print(f"\n成功找到 {len(found)}/{len(targets)} 个 1h 市场")
    json.dump(found, open(os.path.join(OD, "poly_meta_1h_last_30d.json"), "w"), indent=2, default=str)

    all_trades = []
    for t in found:
        cond = t["conditionId"]; offset = 0; mt = []
        while True:
            try:
                r = requests.get(data_base + "/trades", params={"market": cond, "limit": 10000, "offset": offset},
                                 proxies=proxies, timeout=30, headers=UA)
                if r.status_code != 200:
                    print(f"    {t['1h_slug']} trades status {r.status_code}"); break
                data = r.json()
                if not isinstance(data, list) or not data: break
                mt.extend(data)
                if len(data) < 10000: break
                offset += len(data)
                if offset > 200000:
                    print(f"    {t['1h_slug']} 超 20 万笔截断"); break
            except Exception as e:
                print(f"    {t['1h_slug']} trades err {e}"); break
        print(f"  {t['1h_slug']}: {len(mt)} 笔")
        for tr in mt:
            tr["slug"] = t["1h_slug"]; tr["conditionId"] = cond
        all_trades.extend(mt)
        time.sleep(0.15)

    print(f"\n总成交 {len(all_trades)} 笔")
    json.dump(all_trades, open(os.path.join(OD, "poly_trades_1h_last_30d.json"), "w"), default=str)
    print("已保存 poly_trades_1h_last_30d.json")

if __name__ == "__main__":
    main()
