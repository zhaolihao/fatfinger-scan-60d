# -*- coding: utf-8 -*-
"""全量清扫: 撤掉交易所上全部 open algo 条件单(所有实例已死, 162张均无主)。带重试与终检。"""
import re, time, urllib.request, urllib.parse, hmac, hashlib, json, ssl, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

src = open("_count_orders.py", encoding="utf-8").read()
KEY = re.search(r'KEY = "([^"]+)"', src).group(1)
SEC = re.search(r'SEC = "([^"]+)"', src).group(1)

ctx = ssl.create_default_context()
try:
    import os
    proxy = "http://127.0.0.1:7897"
    ph = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
    opener = urllib.request.build_opener(ph, urllib.request.HTTPSHandler(context=ctx))
except Exception:
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))

def signed(method, path, params):
    # 时间戳不信任本地时钟(沙箱偏移>1s会-1021): 每次签名前拉服务器时间
    try:
        srv = signed_nosign_ts()
        ts = srv + 200
    except Exception:
        ts = int(time.time()*1000) - 1500  # 兜底: 往回调1.5s避开-1021
    q = urllib.parse.urlencode(dict(params, timestamp=ts, recvWindow=10000))
    sig = hmac.new(SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request("https://papi.binance.com" + path + "?" + q + "&signature=" + sig,
                                 headers={"X-MBX-APIKEY": KEY}, method=method)
    with opener.open(req, timeout=20) as r:
        return json.loads(r.read())

def signed_nosign_ts():
    with opener.open("https://papi.binance.com/papi/v1/time", timeout=10) as r:
        return json.loads(r.read())["serverTime"]

def get_open():
    for i in range(6):
        try:
            d = signed("GET", "/papi/v1/um/algo/openAlgoOrders", {})
            return d.get("orders", d) if isinstance(d, dict) else d
        except Exception as e:
            time.sleep(2)
    return None

def cancel(sym, aid):
    for i in range(4):
        try:
            signed("DELETE", "/papi/v1/um/algo/order", {"algoId": aid, "symbol": sym})
            return "ok"
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            if "-2011" in body:
                return "gone"
            time.sleep(1.5)
        except Exception:
            time.sleep(1.5)
    return "fail"

orders = get_open()
if orders is None:
    print("拉取失败, 中止(不撤任何单)")
    raise SystemExit(1)

todo = [(str(o.get("symbol")), str(o.get("algoId"))) for o in orders]
print(f"待撤 {len(todo)} 张")
ok = gone = fail = 0
fail_list = []
for sym, aid in todo:
    r = cancel(sym, aid)
    if r == "ok": ok += 1
    elif r == "gone": gone += 1
    else:
        fail += 1; fail_list.append((sym, aid))
    time.sleep(0.15)

print(f"撤成功 {ok} | 已消失 {gone} | 失败 {fail}")
if fail_list:
    print("失败清单:", fail_list[:10])

# 终检
time.sleep(2)
left = get_open()
n = len(left) if left is not None else -1
print(f"终检: 交易所残留 {n} 张")
