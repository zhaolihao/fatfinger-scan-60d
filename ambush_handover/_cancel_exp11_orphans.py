# -*- coding: utf-8 -*-
"""清理 exp11 那 11 币的全部 algo 条件单(含强杀旧进程遗留的孤儿单)。
用法: python _cancel_exp11_orphans.py
仅针对 exp11 专属 11 币(与 resident20 零重叠), 不影响其他进程。"""
import hashlib, hmac, time, urllib.request, urllib.parse, json

KEY = "hCpyw7sGSEA8wIYgaTOUn4CUf6GgxbRQDlU43KiwsniLAtVy7RGHhLGIAI4CCJug"
SEC = "SRoIn7KTCp1oIu6nFR4CWo9jvlrO1QukgxmN9nYBMCbHgL8TpLgXolFHrb2ysvvu"
PROXY = urllib.request.ProxyHandler({"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"})
opener = urllib.request.build_opener(PROXY)
BASE = "https://papi.binance.com"

EXP11 = ["ACTUSDT","AKEUSDT","BARDUSDT","BULLAUSDT","HEMIUSDT","MARSCOINUSDT",
         "ROBOUSDT","SCRUSDT","SOLVUSDT","TOWNSUSDT","TUTUSDT"]
SYMS = set(EXP11)

def signed_req(method, path, params=None):
    params = params or {}
    params["timestamp"] = int(time.time()*1000); params["recvWindow"] = 10000
    q = urllib.parse.urlencode(params)
    sig = hmac.new(SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
    url = BASE + path + "?" + q + "&signature=" + sig
    req = urllib.request.Request(url, headers={"X-MBX-APIKEY": KEY}, method=method)
    with opener.open(req, timeout=15) as r:
        return json.loads(r.read())

# 1) 列出这 11 币的全部 live algo 单
all_orders = signed_req("GET", "/papi/v1/um/algo/openAlgoOrders")
target = [o for o in all_orders if o["symbol"] in SYMS]
print("待取消 %d 张 (exp11 11币):" % len(target))
for o in target:
    print("  %s %s algoId=%s trigger=%s" % (o["symbol"], o.get("side"), o["algoId"], o.get("triggerPrice")))

# 2) 逐张取消
ok = 0
for o in target:
    try:
        r = signed_req("DELETE", "/papi/v1/um/algo/order",
                       {"algoId": o["algoId"], "symbol": o["symbol"]})
        if r.get("code") == 0 or r.get("algoId") or "error" not in str(r).lower():
            ok += 1
            print("  ✓ 撤 %s algoId=%s" % (o["symbol"], o["algoId"]))
        else:
            print("  ✗ 撤失败 %s algoId=%s -> %s" % (o["symbol"], o["algoId"], str(r)[:80]))
    except Exception as e:
        print("  ✗ 异常 %s algoId=%s -> %s" % (o["symbol"], o["algoId"], str(e)[:80]))
print("已撤 %d/%d" % (ok, len(target)))
