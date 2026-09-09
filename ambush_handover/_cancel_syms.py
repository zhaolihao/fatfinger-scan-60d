# -*- coding: utf-8 -*-
"""通用: 撤掉指定币种列表的全部 UM algo 条件单(含强杀进程遗留的孤儿单)。

用法:
    python _cancel_syms.py resident20_draft.json
    python _cancel_syms.py experimental_50_extra.json
    python _cancel_syms.py experimental_20_clean.json
    python _cancel_syms.py --syms ACTUSDT,AKEUSDT        (直接给币名)
    python _cancel_syms.py resident20_draft.json --dry   (只看不撤)

设计要点(踩过的坑):
  · 强杀(taskkill /F)后 bot 没机会跑 cleanup, 条件单会留在交易所 → 必须手工撤,
    否则新进程再挂 → 逼近 200/账户 硬上限 → -4045。
  · 网络抖(Clash SSL UNEXPECTED_EOF)时单张撤单会失败 → 每张重试 3 次,
    末尾复查一次剩余张数, 不达标就明确报警, 绝不假装成功。
"""
import hashlib, hmac, time, urllib.request, urllib.parse, json, sys, os

KEY = "hCpyw7sGSEA8wIYgaTOUn4CUf6GgxbRQDlU43KiwsniLAtVy7RGHhLGIAI4CCJug"
SEC = "SRoIn7KTCp1oIu6nFR4CWo9jvlrO1QukgxmN9nYBMCbHgL8TpLgXolFHrb2ysvvu"
PROXY = urllib.request.ProxyHandler({"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"})
opener = urllib.request.build_opener(PROXY)
BASE = "https://papi.binance.com"


def signed_req(method, path, params=None, tries=3):
    last = None
    for i in range(tries):
        try:
            p = dict(params or {})
            p["timestamp"] = int(time.time() * 1000)
            p["recvWindow"] = 10000
            q = urllib.parse.urlencode(p)
            sig = hmac.new(SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
            url = BASE + path + "?" + q + "&signature=" + sig
            req = urllib.request.Request(url, headers={"X-MBX-APIKEY": KEY}, method=method)
            with opener.open(req, timeout=20) as r:
                return json.loads(r.read())
        except Exception as e:
            last = e
            time.sleep(1.5 * (i + 1))
    raise last


def load_syms(argv):
    if "--syms" in argv:
        raw = argv[argv.index("--syms") + 1]
        return [s.strip().upper() for s in raw.split(",") if s.strip()]
    for a in argv[1:]:
        if a.startswith("--"):
            continue
        if os.path.exists(a):
            with open(a, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data = data.get("symbols") or list(data.keys())
            return [str(s).upper() for s in data]
    raise SystemExit("用法: python _cancel_syms.py <symbols.json> | --syms A,B  [--dry]")


def main():
    dry = "--dry" in sys.argv
    syms = set(load_syms(sys.argv))
    print("目标币种 %d 个: %s%s" % (len(syms), ",".join(sorted(syms))[:200],
                                 "..." if len(syms) > 12 else ""))

    all_orders = signed_req("GET", "/papi/v1/um/algo/openAlgoOrders")
    print("账户当前 algo 条件单总数 = %d" % len(all_orders))
    target = [o for o in all_orders if o["symbol"] in syms]
    print("其中命中目标币种 = %d 张" % len(target))
    for o in target:
        print("   %s %s algoId=%s trigger=%s" % (o["symbol"], o.get("side"),
                                                 o["algoId"], o.get("triggerPrice")))
    if dry:
        print("[--dry] 只看不撤, 退出")
        return
    if not target:
        print("无需撤单")
        return

    ok = 0
    for o in target:
        try:
            r = signed_req("DELETE", "/papi/v1/um/algo/order",
                           {"algoId": o["algoId"], "symbol": o["symbol"]})
            txt = str(r)
            if ("success" in txt.lower()) or r.get("code") == 0 or r.get("algoId"):
                ok += 1
                print("   OK  撤 %s %s algoId=%s" % (o["symbol"], o.get("side"), o["algoId"]))
            else:
                print("   ERR 撤失败 %s algoId=%s -> %s" % (o["symbol"], o["algoId"], txt[:100]))
        except Exception as e:
            print("   ERR 异常 %s algoId=%s -> %s" % (o["symbol"], o["algoId"], str(e)[:100]))
    print("已撤 %d/%d" % (ok, len(target)))

    time.sleep(2)
    try:
        again = signed_req("GET", "/papi/v1/um/algo/openAlgoOrders")
        left = [o for o in again if o["symbol"] in syms]
        print("复查: 账户总 %d 张 | 目标币种残留 %d 张" % (len(again), len(left)))
        if left:
            print("!! 仍有残留, 请再跑一次本脚本 (网络抖动导致部分撤单失败)")
            for o in left:
                print("   残留 %s %s algoId=%s" % (o["symbol"], o.get("side"), o["algoId"]))
    except Exception as e:
        print("复查失败(网络): %s" % str(e)[:100])


if __name__ == "__main__":
    main()
