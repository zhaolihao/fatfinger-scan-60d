# -*- coding: utf-8 -*-
"""只读查询币安当前挂单数（fapi + papi），用于验证挂单回补"""
import hashlib, hmac, time, urllib.request, urllib.parse, json, sys

KEY = "hCpyw7sGSEA8wIYgaTOUn4CUf6GgxbRQDlU43KiwsniLAtVy7RGHhLGIAI4CCJug"
SEC = "SRoIn7KTCp1oIu6nFR4CWo9jvlrO1QukgxmN9nYBMCbHgL8TpLgXolFHrb2ysvvu"
PROXY = urllib.request.ProxyHandler({"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"})
opener = urllib.request.build_opener(PROXY)

def signed_get(base, path, tries=5):
    last = None
    for i in range(tries):
        try:
            q = urllib.parse.urlencode({"timestamp": int(time.time()*1000), "recvWindow": 10000})
            sig = hmac.new(SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
            req = urllib.request.Request(base + path + "?" + q + "&signature=" + sig,
                                         headers={"X-MBX-APIKEY": KEY})
            with opener.open(req, timeout=15) as r:
                return json.loads(r.read())
        except Exception as e:
            last = e
            time.sleep(1.0 * (i + 1))
    raise last

for name, base, path in [
                          ("组合保证金UM Algo挂单(papi)", "https://papi.binance.com", "/papi/v1/um/algo/openAlgoOrders")]:
    try:
        orders = signed_get(base, path)
        syms = {}
        for o in orders:
            syms[o["symbol"]] = syms.get(o["symbol"], 0) + 1
        print("%s: 共 %d 单, 覆盖 %d 个币" % (name, len(orders), len(syms)))
        for s in sorted(syms):
            print("   %s x%d" % (s, syms[s]))
    except Exception as e:
        print("%s: 查询失败 %s" % (name, str(e)[:120]))
