# -*- coding: utf-8 -*-
"""精准撤单: python _cancel_algo.py SYMBOL ALGOID  (只撤指定一张, 不误伤)"""
import sys, hashlib, hmac, json, time, urllib.request, urllib.parse

KEY = "hCpyw7sGSEA8wIYgaTOUn4CUf6GgxbRQDlU43KiwsniLAtVy7RGHhLGIAI4CCJug"
SEC = "SRoIn7KTCp1oIu6nFR4CWo9jvlrO1QukgxmN9nYBMCbHgL8TpLgXolFHrb2ysvvu"
PROXY = "http://127.0.0.1:7897"
opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": PROXY, "https": PROXY}))

def signed(method, path, params):
    q = urllib.parse.urlencode(params)
    sig = hmac.new(SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request("https://papi.binance.com" + path + "?" + q + "&signature=" + sig,
                                 headers={"X-MBX-APIKEY": KEY}, method=method)
    with opener.open(req, timeout=15) as r:
        return json.loads(r.read())

def main():
    sym, aid = sys.argv[1], sys.argv[2]
    for i in range(4):
        try:
            r = signed("DELETE", "/papi/v1/um/algo/order",
                       {"algoId": aid, "symbol": sym, "timestamp": int(time.time() * 1000), "recvWindow": 10000})
            print(f"撤成功 {sym} {aid}: code={r.get('code')} msg={r.get('msg', 'OK')}")
            return
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            if "-2011" in body:
                print(f"{sym} {aid}: 本已消失(-2011), 视为成功")
                return
            print(f"HTTP {e.code}: {body[:200]} (重试{i+1})")
        except Exception as ex:
            print(f"网络异常 {ex} (重试{i+1})")
        time.sleep(2)
    print("失败: 4次重试均失败")

if __name__ == "__main__":
    main()
