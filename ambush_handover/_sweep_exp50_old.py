# -*- coding: utf-8 -*-
"""紧急清扫: 撤掉 exp50 币种上"新进程未登记"的旧代孤儿单。
白名单 = resident_exp50_out.tmp 日志中新进程挂单成功的 algoId(append-only, 不会被污染)。
用法: python _sweep_exp50_old.py [--dry]
"""
import sys, re, json, time, hashlib, hmac, urllib.request, urllib.parse, urllib.error

KEY = "hCpyw7sGSEA8wREPLACE_FROM_COUNT"  # 占位, 运行时从 _count_orders.py 同步
SEC = "REPLACE"

def load_creds():
    src = open("_count_orders.py", encoding="utf-8").read()
    k = re.search(r'KEY = "([^"]+)"', src).group(1)
    s = re.search(r'SEC = "([^"]+)"', src).group(1)
    return k, s

KEY, SEC = load_creds()
PROXY = "http://127.0.0.1:7897"
opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": PROXY, "https": PROXY}))

def signed(method, path, params, tries=4):
    for i in range(tries):
        try:
            q = urllib.parse.urlencode(params)
            sig = hmac.new(SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
            req = urllib.request.Request("https://papi.binance.com" + path + "?" + q + "&signature=" + sig,
                                         headers={"X-MBX-APIKEY": KEY}, method=method)
            with opener.open(req, timeout=15) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            if "-2011" in body:
                return {"code": -2011}
            print(f"  HTTP {e.code}: {body[:120]} (重试{i+1})")
        except Exception as ex:
            print(f"  网络 {str(ex)[:80]} (重试{i+1})")
        time.sleep(2)
    return None

def main():
    dry = "--dry" in sys.argv
    syms = set(json.load(open("experimental_50_extra.json", encoding="utf-8")))
    raw = open("resident_exp50_out.tmp", "rb").read()
    log = None
    for enc in ("utf-8", "gbk", "latin-1"):
        try:
            log = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if log is None:
        log = raw.decode("utf-8", errors="replace")
    # 新进程挂单成功行(✓ 在文件里是字面 \u2713 转义): "... SYM SIDE TAKE_PROFIT_MARKET ... algoId=..."
    keep = set()
    for m in re.finditer(r"(\w+USDT)\s+(BUY|SELL)\s+TAKE_PROFIT_MARKET[^\n]*algoId=(\d+)", log):
        keep.add((m.group(1), m.group(3)))
    print(f"exp50 币数={len(syms)} | 新进程白名单={len(keep)} 张")

    data = signed("GET", "/papi/v1/um/algo/openAlgoOrders",
                  {"timestamp": int(time.time() * 1000), "recvWindow": 10000})
    if data is None:
        print("拉取挂单失败, 放弃"); return
    orders = data.get("orders", data) if isinstance(data, dict) else data
    victims = []
    for o in orders:
        sym = o.get("symbol"); aid = str(o.get("algoId"))
        if sym in syms and (sym, aid) not in keep:
            victims.append((sym, aid, o.get("side"), o.get("triggerPrice")))
    print(f"待撤旧单 {len(victims)} 张" + (" (--dry 不执行)" if dry else ""))
    for sym, aid, side, tp in victims:
        print(f"  {sym} {side} {aid} tp={tp}")
    if dry:
        return
    ok = gone = fail = 0
    for sym, aid, side, tp in victims:
        r = signed("DELETE", "/papi/v1/um/algo/order",
                   {"algoId": aid, "symbol": sym, "timestamp": int(time.time() * 1000), "recvWindow": 10000})
        if r is None:
            fail += 1
        elif r.get("code") == -2011:
            gone += 1
        else:
            ok += 1
    print(f"结果: 撤成功{ok} 本已消失{gone} 失败{fail}")
    # 复核
    time.sleep(2)
    data = signed("GET", "/papi/v1/um/algo/openAlgoOrders",
                  {"timestamp": int(time.time() * 1000), "recvWindow": 10000})
    if data:
        orders = data.get("orders", data) if isinstance(data, dict) else data
        from collections import Counter
        c = Counter(o["symbol"] for o in orders if o["symbol"] in syms)
        bad = {s: n for s, n in c.items() if n != 2}
        print(f"复核: exp50币上共{sum(c.values())}张 | 异常分布={bad}")

if __name__ == "__main__":
    main()
