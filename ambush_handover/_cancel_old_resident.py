# -*- coding: utf-8 -*-
"""精确撤销 resident20 的【旧一代】40 张条件单(21:11 重启前那一批)。

背景(必须留档的坑):
  重启时 _resident_load_cancel() 依赖 resident_state.json 撤旧单, 但那个文件在上一次
  失败启动(进程被沙箱回收前)已经被 os.remove 掉了 → 新进程 n0=0;
  _resident_sweep_logs() 的正则匹配 "[常驻✓]", 而旧 .tmp 日志里中文被写成 \u2713 转义
  → 也匹配不到 → n1=0。两道兜底同时失效, 新进程直接在旧单之上又挂了一批, 逼近 200 硬上限。

因此这里用 21:10 dry-run 抓到的 40 个旧 algoId 明确清单撤销(不靠阈值猜, 绝不误伤新单)。
新一代 algoId 从 1000002551836016 起, 与下表零交集。
"""
import hashlib, hmac, time, urllib.request, urllib.parse, json

KEY = "hCpyw7sGSEA8wIYgaTOUn4CUf6GgxbRQDlU43KiwsniLAtVy7RGHhLGIAI4CCJug"
SEC = "SRoIn7KTCp1oIu6nFR4CWo9jvlrO1QukgxmN9nYBMCbHgL8TpLgXolFHrb2ysvvu"
PROXY = urllib.request.ProxyHandler({"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"})
opener = urllib.request.build_opener(PROXY)
BASE = "https://papi.binance.com"

OLD = [
    ("USELESSUSDT", 1000002551810726), ("USELESSUSDT", 1000002551810720),
    ("MELANIAUSDT", 1000002551778997), ("MELANIAUSDT", 1000002551778988),
    ("PROMUSDT", 1000002551758895),    ("PROMUSDT", 1000002551758887),
    ("UAIUSDT", 1000002551757793),     ("UAIUSDT", 1000002551757785),
    ("MOVRUSDT", 1000002551753485),    ("MOVRUSDT", 1000002551753467),
    ("BBUSDT", 1000002551721875),      ("BBUSDT", 1000002551721871),
    ("GWEIUSDT", 1000002551720609),    ("GWEIUSDT", 1000002551720598),
    ("TRIAUSDT", 1000002551678831),    ("TRIAUSDT", 1000002551678824),
    ("GENIUSUSDT", 1000002551622554),  ("GENIUSUSDT", 1000002551622547),
    ("FOLKSUSDT", 1000002551516058),   ("FOLKSUSDT", 1000002551516050),
    ("BOMEUSDT", 1000002551516038),    ("BOMEUSDT", 1000002551516036),
    ("BERAUSDT", 1000002551516032),    ("BERAUSDT", 1000002551516020),
    ("TRUMPUSDT", 1000002551515997),   ("TRUMPUSDT", 1000002551515994),
    ("NEIROUSDT", 1000002551515979),   ("NEIROUSDT", 1000002551515976),
    ("GPSUSDT", 1000002551515943),     ("GPSUSDT", 1000002551515937),
    ("CATIUSDT", 1000002551515884),    ("CATIUSDT", 1000002551515875),
    ("AIOUSDT", 1000002551515859),     ("AIOUSDT", 1000002551515856),
    ("RIVERUSDT", 1000002551515823),   ("RIVERUSDT", 1000002551515822),
    ("ONEUSDT", 1000002551515817),     ("ONEUSDT", 1000002551515808),
    ("APRUSDT", 1000002551515748),     ("APRUSDT", 1000002551515745),
]


def signed_req(method, path, params=None, tries=4):
    last = None
    for i in range(tries):
        try:
            p = dict(params or {})
            p["timestamp"] = int(time.time() * 1000)
            p["recvWindow"] = 10000
            q = urllib.parse.urlencode(p)
            sig = hmac.new(SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
            req = urllib.request.Request(BASE + path + "?" + q + "&signature=" + sig,
                                         headers={"X-MBX-APIKEY": KEY}, method=method)
            with opener.open(req, timeout=20) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="ignore")
            if "-2011" in body or "Unknown order" in body:
                return {"code": -2011, "msg": "already gone"}
            last = Exception(body[:120])
        except Exception as e:
            last = e
        time.sleep(1.2 * (i + 1))
    raise last


ok = gone = fail = 0
for sym, aid in OLD:
    try:
        r = signed_req("DELETE", "/papi/v1/um/algo/order", {"algoId": aid, "symbol": sym})
        if r.get("code") == -2011:
            gone += 1
            print("   -    %s %s 已不存在(视为已撤)" % (sym, aid))
        else:
            ok += 1
            print("   OK   %s %s 已撤" % (sym, aid))
    except Exception as e:
        fail += 1
        print("   FAIL %s %s -> %s" % (sym, aid, str(e)[:90]))
print("撤成功 %d | 本已消失 %d | 失败 %d" % (ok, gone, fail))

time.sleep(2)
try:
    allo = signed_req("GET", "/papi/v1/um/algo/openAlgoOrders")
    print("复查: 账户 algo 单总数 = %d" % len(allo))
    left = [o for o in allo if int(o["algoId"]) in {a for _s, a in OLD}]
    print("旧一代残留 = %d 张" % len(left))
    for o in left:
        print("   残留 %s %s" % (o["symbol"], o["algoId"]))
except Exception as e:
    print("复查失败: %s" % str(e)[:110])
