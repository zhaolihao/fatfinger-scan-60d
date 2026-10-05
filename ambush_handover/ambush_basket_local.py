# -*- coding: utf-8 -*-
"""
ambush_basket.py — 58 惯犯币乌龙指埋伏（多币单进程版）
====================================================
架构（为什么不用 58 个 dual_loop 进程：REST 轮询会撞限流）：
  1 条组合 WS bookTicker  → 全部币的实时中间价（O 取值 / A/B 路线监控）
  1 条 listenKey 用户数据流 → 成交推送（零轮询，不撞限流）
  分钟边界 → 撤自己 track 的单（错峰）→ 按新 O 重挂双侧埋伏

安全设计：
  - 只撤自己记录的 orderId（不用 cancelAll——防误伤同账户其他策略）
  - 启动不自动 flatten 既有持仓（只告警）
  - REST 401(-2015) 自动关闭重开 httpx 连接换 Clash 出口节点重试
  - IP 守卫：每轮挂单前查出口 IP，不在白名单(币安强制IP限制→Clash轮换节点会跳到非白名单IP)
             则只撤不挂，杜绝「单边成交、对侧 -2015 平不掉」的单腿事故
  - 平仓无限重试：IP 漂移期间 -2015 时持续换连接重试，绕回白名单节点即平掉；平不掉则保留仓位待收尾
  - 撤单查不到终态时保留追踪下一轮再试，杜绝孤儿单
  - WS 掉线自动重连；每 60s REST 兜底核对挂单状态（防漏成交事件）
  - Ctrl+C / max-rounds 退出：撤自己的单 + 市价平自己开的仓

用法：
  python ambush_basket.py --smoke                                       # 公开数据自测(不用key, 30s)
  python ambush_basket.py --symbols VELVETUSDT,HUSDT --max-rounds 2     # 小样实盘
  python ambush_basket.py --ip 1.2.3.4                                 # 指定允许出口IP
  python ambush_basket.py                                              # 全58币无限轮
"""
import asyncio, json, time, hmac, hashlib, logging, os, sys, argparse, signal
import traceback
from collections import deque
from urllib.parse import urlencode, quote
from datetime import datetime
import httpx
import websockets

# ── 企业微信通知（沿用多稳定币预埋伏脚本的同一个机器人；非阻塞守护线程发送）──
# 导入失败不能拖垮交易主流程，故包 try/except，缺失时自动降级为只写日志。
try:
    import wecom_notify
except Exception as _e:                       # pragma: no cover
    wecom_notify = None
    print(f"[通知] wecom_notify 导入失败，通知已关闭: {_e}")

PROXY = os.environ.get("PROXY_URL") or ""  # 本地无需代理，IP已在白名单
# ── SNI绕过：DNS预解析为IP，之后所有URL用IP拼接，TLS握手SNI=IP不被过滤 ──
import socket as _sock
import ssl as _sslmod
_FAPI_IP = _sock.gethostbyname("fapi.binance.com")
_API_IP = _sock.gethostbyname("api.binance.com")
_FSTREAM_IP = _sock.gethostbyname("fstream.binance.com")
PAPI = f"https://{_FAPI_IP}"
FSTREAM_WS = f"wss://{_FSTREAM_IP}"
_FAPI_HOST = "fapi.binance.com"
_API_HOST = "api.binance.com"
_FSTREAM_HOST = "fstream.binance.com"
# WS专用SSL context：不检查hostname（因为用IP连接），不验证证书
_ws_ssl = _sslmod.SSLContext(_sslmod.PROTOCOL_TLS_CLIENT)
_ws_ssl.check_hostname = False
_ws_ssl.verify_mode = _sslmod.CERT_NONE

# ── 配置：环境变量优先，.env 兜底 ────────────────────────────
def load_env():
    d = {}
    envf = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(envf):
        for line in open(envf, encoding="utf-8"):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                d[k.strip()] = v.strip().strip('"').strip("'")
    return d

_ENV = load_env()
KEY = os.environ.get("BN_KEY") or _ENV.get("BN_KEY", "")
SECRET = os.environ.get("BN_SECRET") or _ENV.get("BN_SECRET", "")
# IP 守卫：白名单内的出口 IP 集合。Clash 轮换到非白名单节点时 -2015 → 禁止挂新单。
EXPECTED_IPS = {ip.strip() for ip in (os.environ.get("BN_IP") or _ENV.get("BN_IP", "")).split(",") if ip.strip()}
_ip_alert_throttle = {"ts": 0.0}   # 企业微信 IP 告警节流(同一次异常每300s最多1条)

# ── REST：401 自动换连接重试（Clash 多出口 IP 核心对策）────────
import threading
_cli = [None]
_cli_sync = [None]       # 同步客户端（utility 函数用）
_cli_lock = threading.Lock()

def client_sync():
    """同步 httpx 客户端（exit_ip/binance_ip_ok/clock_resync 等 utility 函数用）。"""
    with _cli_lock:
        if _cli_sync[0] is None:
            _cli_sync[0] = httpx.Client(proxy=PROXY or None, timeout=15, verify=False)
            _cli_sync[0].headers.update({"User-Agent": "ambush-basket-1", "X-MBX-APIKEY": KEY,
                                         "Host": _FAPI_HOST})
        return _cli_sync[0]

def reset_client_sync():
    with _cli_lock:
        try:
            if _cli_sync[0]:
                _cli_sync[0].close()
        except Exception:
            pass
        _cli_sync[0] = httpx.Client(proxy=PROXY or None, timeout=15, verify=False)
        _cli_sync[0].headers.update({"User-Agent": "ambush-basket-1", "X-MBX-APIKEY": KEY,
                                     "Host": _FAPI_HOST})

async def client():
    with _cli_lock:
        if _cli[0] is None:
            _cli[0] = httpx.AsyncClient(proxy=PROXY or None, timeout=15, verify=False)
            _cli[0].headers.update({"User-Agent": "ambush-basket-1", "X-MBX-APIKEY": KEY,
                                     "Host": _FAPI_HOST})
        return _cli[0]

async def reset_client():
    # 加锁：并发撤单(Semaphore6)多线失败时会同时调 reset，避免互相关掉对方连接→雪崩
    with _cli_lock:
        try:
            if _cli[0]:
                await _cli[0].aclose()
        except Exception:
            pass
        _cli[0] = httpx.AsyncClient(proxy=PROXY or None, timeout=15, verify=False)
        _cli[0].headers.update({"User-Agent": "ambush-basket-1", "X-MBX-APIKEY": KEY,
                                 "Host": _FAPI_HOST})

import re as _re

# 【2026-09-04 事故修复】原只有 ipify/ip.sb 两个探测站，两站同时不可达 → 返回 None，
# 而守卫把 None 当成"IP 不在白名单" → 误拦 29 分钟（其实 IP 一直是白名单内的 50.7.158.117）。
# 对策两层：① 探测站扩到 7 个冗余；② 万一全挂，不再猜，改由币安自己裁决（binance_ip_ok）。
# 【2026-09-08 二次修复】探测站混进了国内站点(myip.ipip.net/ip.3322.net/taobao)，
# Clash 规则对国内域名走 DIRECT → 返回的是本机直连 IP(218.201.184.224 山东联通)，
# 与币安实际看到的代理出口(45.149.92.90)不是同一个 → 守卫误判"不在白名单"而停挂。
# 对策：探测站只保留境外站（必定走代理），保证"探测所见 = 下单所用"。
_IP_PROBES = (
    "https://api.ipify.org",
    "https://api.ip.sb/ip",
    "https://icanhazip.com",
    "https://api.myip.com",
    "https://ifconfig.me/ip",
    "https://ipinfo.io/ip",
    "https://checkip.amazonaws.com",
)

def exit_ip():
    """当前 httpx 连接的 Clash 出口 IP（与下单同一连接，所见即所得）。
    多站冗余：任一站点返回 IPv4 即采用；全部不可达才返回 None。"""
    for url in _IP_PROBES:
        try:
            r = client_sync().get(url, timeout=8)
            m = _re.search(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", r.text or "")
            if m:
                return m.group(1)
        except Exception:
            reset_client_sync()   # 换连接=换出口节点，下一站可能就通
    return None

def binance_ip_ok():
    """【探测失败兜底】探测站全挂(ip=None)时，不再把"查不到"当成"IP 不对"，
    而是直接问币安：用同一连接发一个最轻量的签名查询，币安肯收=放行，-2015=真的没白名单。
    返回 (放行bool, 说明str)。"""
    try:
        srv = client_sync().get(f"{PAPI}/fapi/v1/time", timeout=10).json().get("serverTime")
        p = {"timestamp": int(srv), "recvWindow": 60000}
        qs = urlencode(p)
        sig = hmac.new(SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
        r = client_sync().get(f"{PAPI}/fapi/v2/openOrders?{qs}&signature={sig}", timeout=12)
        try:
            js = r.json() if r.content else {}
        except Exception:
            js = {}
        code = js.get("code") if isinstance(js, dict) else None
        if r.status_code == 200 and not code:
            return True, "币安放行(出口在白名单)"
        if code == -2015 or r.status_code == 401:
            return False, "币安拒收 -2015(出口确实未白名单)"
        return False, f"币安返回 code={code} {str(js)[:60]}"
    except Exception as e:
        # 连币安都不通 = 网络真断了，此时本来也下不了单，保守拦截
        return False, f"币安不可达 {type(e).__name__}(保守拦截)"

# ── 后台任务管理（成交处理 spawn 成独立任务，避免阻塞 WS 消息循环/分钟循环）──
_bg = set()
def spawn(coro):
    async def _wrap():
        try:
            await coro
        except Exception as e:
            log.error(f"[task✗] 后台任务异常: {e}")
    t = asyncio.create_task(_wrap())
    _bg.add(t); t.add_done_callback(_bg.discard)

OFF = 0  # 服务器时钟偏移(ms)

# 这些错误与出口 IP / 密钥 / 签名相关：换连接(换 Clash 节点)无效，必须立即返回真实错误码，
# 不要当网络异常盲重试——否则会把 -2015(IP未白名单) 误报成 retries_exhausted，误导排查。
_NON_RETRY_CODES = {-2015, -2014, -2013, -1022, -1021, -2011}

async def signed_request(method, path, params=None, tries=6):
    """签名请求。出口 IP 不在白名单 → 币安返回 -2015，换连接无效 → 立即返回真实错误码；
    仅在真正的网络异常(连接/超时/代理断开)时才换连接重试。"""
    params = dict(params or {})
    for k in range(tries):
        p = dict(params)
        p["timestamp"] = int(time.time() * 1000) + OFF
        p["recvWindow"] = 60000
        qs = urlencode(p)   # 【坑】中文 symbol 必须 urlencode 后再签名
        sig = hmac.new(SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
        try:
            cli = await client()
            r = await cli.request(method, f"{PAPI}{path}?{qs}&signature={sig}", timeout=15)
            try:
                js = r.json() if r.content else {}
            except Exception:
                js = {"raw": (r.text or "")[:200]}
            code = js.get("code") if isinstance(js, dict) else None
            if code == -1021 and k < tries - 1:
                # 【-1021自愈】时钟偏移→立即重校准OFF并重试(重签时用新OFF)。下单被拒无副作用, 重试安全。
                log.warning(f"[签名请求] {method} {path} → -1021 时钟偏移 → 立即重校准重试")
                _clock_resync()
                continue
            if r.status_code == 401 or (isinstance(code, int) and code in _NON_RETRY_CODES):
                if code == -2015:
                    log.warning(f"[签名请求] {method} {path} → -2015 出口IP未白名单(请求IP={exit_ip()})")
                return r.status_code, js          # 立即返回，不空转
            return r.status_code, js
        except Exception as e:
            if k == 0:
                log.warning(f"[签名请求] {method} {path} 网络异常 {type(e).__name__}: {e}（换连接重试）")
            await reset_client()
            await asyncio.sleep(0.5 * (k + 1))
    return 0, {"error": "retries_exhausted"}

async def public_get(path, params=None):
    cli = await client()
    r = await cli.get(f"{PAPI}{path}", params=params, timeout=20)
    return r.status_code, (r.json() if r.content else {})

def sync_signed_request(method, path, params=None, tries=6):
    """同步版签名请求（utility 函数用：place_stop_algo_sync/_resident_load_cancel/_open_algo_orders等）。"""
    params = dict(params or {})
    for k in range(tries):
        p = dict(params)
        p["timestamp"] = int(time.time() * 1000) + OFF
        p["recvWindow"] = 60000
        qs = urlencode(p)
        sig = hmac.new(SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
        try:
            cli = client_sync()
            r = cli.request(method, f"{PAPI}{path}?{qs}&signature={sig}", timeout=15)
            try:
                js = r.json() if r.content else {}
            except Exception:
                js = {"raw": (r.text or "")[:200]}
            code = js.get("code") if isinstance(js, dict) else None
            if code == -1021 and k < tries - 1:
                _clock_resync()
                continue
            if r.status_code == 401 or (isinstance(code, int) and code in _NON_RETRY_CODES):
                return r.status_code, js
            return r.status_code, js
        except Exception:
            reset_client_sync()
            time.sleep(0.5 * (k + 1))
    return 0, {"error": "retries_exhausted"}

# ── 日志 ─────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
tag = datetime.now().strftime("%Y%m%d_%H%M%S")
log = logging.getLogger("ambush_basket")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_sh = logging.StreamHandler(); _sh.setFormatter(_fmt); log.addHandler(_sh)
_fh = logging.FileHandler(f"logs/ambush_basket_{tag}.log", encoding="utf-8")
_fh.setFormatter(_fmt); log.addHandler(_fh)

# ── 全局状态 ─────────────────────────────────────────────────
mids = {}            # symbol -> {"bid","ask","ts"}   (WS bookTicker)
last_mid = {}        # symbol -> float  最后已知锚价（WS 过期/缺失时回退用，启动 prime）
orders = {}          # orderId -> {symbol, side, price, qty, O, round}
exits_in_progress = set()   # orderId 已触发退出流程
exit_in_flight = set()     # 平仓占坑锁: 同一仓(方向键)同时只允许一张市价平仓单在飞
                            # 防止 WS快路径 与 轮询路线A/B 同拍各发一张平仓单 → 双平/反向开仓(-2022根因之一)
positions_open = {}  # symbol -> {"pos_side","qty","entry","O","order_id"} 我们开的仓
last_ref = {}        # symbol -> 上次挂单锚价O；价格偏移>=REPRICE_THRESH才重挂(降撤挂比,防-4400)
reprice_cooldown = {} # symbol -> 轮次号；该轮之前不重挂(挂新失败-4400后进入冷却,保留旧单防洞)
REPRICE_COOLDOWN = 30 # 重挂失败后冷却轮数(≈30min),等量化风控窗口滚过再试
ANCHOR_SEC = 1       # 【秒级锚价】锚价刷新周期(秒)=接针判定窗口。1=秒级(与回测对齐); CEO 20260913 定
                     #  1=秒级(只接1秒内的真乌龙, 阴跌时锚价跟着降→不接)。
                     #  触发判定本身走WS(亚秒级), 变的只是"锚价O每隔多久重取一次"。
                     #  回测(2026-09-08 XAN逐笔43.9万笔): 60s锚价→XAN阴跌触发亏损;
                     #  ≤5s锚价→XAN不触发; FORM真插针(单秒-9%)仍能接到。

FREEZE_PULL_MARGIN = 0.05  # 【补丁2】冷却期爬单守护：旧单距真实价<5% 强制撤（防缓涨/缓跌爬进死单）
stats = {"rounds": 0, "fills": 0, "routeA": 0, "routeB": 0, "fallback": 0, "pnl": 0.0,
         "cov_min": 999, "cov_sum": 0, "cov_n": 0, "anchor_stale": 0, "ip_bad_rounds": 0,
         "cmp_M_fills": 0, "cmp_L_fills": 0}
RUN_SYMBOLS = []     # 当前运行币种（供 Ctrl+C/SIGTERM 兜底撤单用）
shutdown = asyncio.Event()
STARTUP_HELD = set()  # 启动即存在的持仓(非本策略开): 仅屏蔽触发, 不接管/不清仓

FILTERS = {}         # symbol -> {tick, step, minQty, minNotional}

# ── A/B 组 + 响应式入场（2026-09-05 改）──────────────────────
# A组：不提前挂单，WS 监测跌破锚价×(1-DEPTH) → 市价买（吃滑点但接得快）
# B组：同条件 → 限价买（价=近期 WS 最低，想接在尖针最低点）
# 两者都不挂"被动限价单"，成交后走 A/B 路线平仓。
tp_limits = {}        # (sym, pos_side) -> orderId  在册的止盈限价单
anchors = {}          # symbol -> 锚价O（实时盘口更新，用于平仓判断）
price_hist = {}       # symbol -> deque([(ts_ms, price), ...]) 最近300ms价格历史，触发判断用100ms窗口锚价
_AT_CNT = {0: 0}      # aggTrade处理计数器，用于心跳诊断
_RAW_CNT = {0: 0}     # aggTrade原始消息计数器，用于格式诊断
_MAIN_LOOP = None    # 主事件循环引用，供子线程提交on_fill用
PRICE_HIST_MS = 300   # 保留最近300ms，取80ms前的价格作触发锚价
dip_low = {}          # symbol -> 近期 WS 最低中间价（B组/CMP 多单限价入场价）
peak_high = {}        # symbol -> 近期 WS 最高中间价（空单限价卖空入场价，对称 dip_low）

# ── 动态阈值（滚动波动率，2026-09-06 加）──────────────────────
DEPTH = 0.06          # 固定阈值（【CEO定 20260918】统一6%）
L_TIMEOUT_S = 5.0     # 【旧L腿·已停用】保留常量供回滚；L挂单逻辑已被风暴单取代
L_DEPTH_EXTRA = 0.05  # 【旧L腿·已停用】保留常量供回滚
# 【CEO定 20260907·反弹确认关闭】DASH复盘：确认等待1.4s让空单入场差0.8%(少赚)，10%大针噪音已由阈值过滤 → M触发即市价。
# REBOUND_PCT/WAIT_S 保留常量供回滚(翻 REBOUND_ON=True 即恢复旧逻辑)。
REBOUND_ON = False
REBOUND_PCT = 0.008   # (回滚用) 反弹确认幅度 0.8%
REBOUND_WAIT_S = 15.0 # (回滚用) 反弹确认窗口 15s
pend_m = {}           # (sym,pos_side) -> {"tid","anc","depth","t0","ext"} 待反弹确认的 M 腿(REBOUND_ON=False 时不使用)
DEPTH = 0.04          # 固定阈值（临时改为4%验证修改效果，验证后改回6%）
# 风暴模式已删除(20260911 CEO定): 不再派生同向追单接第二刀, 所有成交腿统一走 on_fill 流水线
# (SL先挂/路线A/路线B自适应)。相关常量 STORM_* 一并移除。

# ── 常驻埋伏模式（CEO定 20260907，--mode resident）────────────
# A组=20个惯犯(杠杆≥50, 30天10%穿刺榜)双侧常驻条件单: TAKE_PROFIT_MARKET
#   BUY@锚×0.90(跌破触发市价接针) / SELL@锚×1.10(涨破触发市价开空)。
#   语法依据(官方文档): TP买单=价格≤stopPrice触发; TP卖单=价格≥stopPrice触发;
#   不带reduceOnly即可开仓。workingType=CONTRACT_PRICE(最新价,标记价平滑过可能不触发);
#   priceProtect保持关闭(插针恰是最新价/标记价价差最大时刻)。
#   vs 常驻限价单关键差异: 限价"穿过≠成交"(针跳空越过挂价无成交), 条件单"触发必成交"。
# 入场全平台托管(代理断/WS断/检测延迟免疫); 成交后仍复用 on_fill 流水线(SL先挂/路线A/路线B)。
# CEO三决策: ①杠杆门槛50(B方案名单) ②单边成交不撤另一侧 ③常驻阈值10%。
MODE = "react"        # react=响应式触发+市价(130币) | resident=常驻条件单(20惯犯)
RESIDENT_DEPTH = 0.06 # 常驻触发深度±6%（CEO 20260918 定: 统一到6%, 易改）
INSTANCE = ""         # 实例标签(空=默认): 隔离锁文件/state文件/日志前缀, 允许多个resident进程并行跑不同币种
RESIDENT_REPEG = 0.02 # 锚价漂移≥2%才重挂（复用 --reprice 语义）
RESIDENT_MIN_GAP = 0.08  # 冻结侧安全距离: 触发价距现价<8%→上移重挂(异常兜底)
# 说明: 正常路径不会触发——仓位成交后 TP 在+5%就平, 而冻结侧在+10%之外, 安全垫16.4%。
# 仅防 TP 挂单失败/跳空越过等异常场景: 仓位久挂不平, 价格一路涨到冻结侧触发价(趋势单非插针)。
res_cond = {}         # (sym, side) -> {"algoId","trigger","base"} 在册常驻条件单
res_base = {}         # sym -> 该币最近挂单锚价(R腿成交回填锚价用)
res_freeze = {}       # sym -> True: 单边成交后冻结(另一侧保留原触发价, 不漂移不补挂; 平仓后解冻双侧重挂)
res_fills = {}        # (sym, pos_side) -> ts: 成交去重(WS与REST兜底双通道, 120s窗口)
res_orphan = {}       # (sym, side, algoId) -> True: 重挂时撤旧失败的单, 下轮重试撤(防孤儿单堆积)


entry_cooldown = {}   # symbol -> 截止时间戳（响应式入场冷却，防同一次插针重复触发）
_entry_lock = {}     # (sym, side) -> bool，触发后置True，防止同币并发下单（协程堆积根因修复）
near_miss_ts = {}     # symbol -> 上次打"逼近"日志时间（节流，证明bot在盯盘且不空转）
GROUP_A = []          # 响应式市价入场组
GROUP_B = []          # 响应式限价入场组（限价=WS最低）
TP_PCT = 0.03         # 路线A止盈幅度（相对成交价3%）
ROUTE_B_MODE = "adaptive"  # 路线B: 'adaptive'=自适应退出(对齐回测新逻辑); 旧'wait'/'close'已弃用

# ── 路线A/B 退出逻辑（2026-09-27 修改：统一用成交价判断）────────────
# 路线A: 成交价×(1+TP_PCT) 盈利3%才平（T_A=1s窗口内）
# 路线B: 成交价×(1+B_PROFIT_PCT) 盈利1%即平（T_A窗口后）
# 止损: 成交价×(1-B_LOSS) 亏损3%止损
REB_ANCHOR = False    # 改用成交价判断（不再用锚价）
REB_PCT = 0.03        # 路线A盈利阈值（相对成交价3%）
B_PROFIT = 0.06       # (保留) 旧路线B利润带，已弃用
B_REVERSAL = 0.005    # (保留) 旧路线B反转撤退，已弃用
B_TIME = 10.0         # 路线B时间止损(秒, 自路线A窗口结束起算)
B_LOSS = 0.03         # 止损幅度（相对成交价3%）
B_BREAKEVEN = True    # 路线B盈利即平开关
B_PROFIT_PCT = 0.01   # 路线B盈利1%即平（相对成交价）
BRANCH_TR = 0.02      # (保留) 旧分岔移动止盈回撤
BRANCH_CUT = 0.02     # (保留) 旧分岔快砍
BRANCH_HOLD = 900.0   # (保留) 旧分岔最长持有

# ── 对比模式 CMP（2026-09-05 改）──────────────────────────────
# 取消按币分 A/B 组：每个币每次触发同时下 市价(M)+限价(L) 两单(各 NOTIONAL U)，
# 两单打同一 trigger_id、不同子仓键 sub(f"{tid}:M"/"{tid}:L")，各自独立挂 TP/SL，
# 退出逻辑完全一致(只比入场执行)。sub 同时用于 positions_open/tp_limits/algo_stops 键，
# 使同一币的两个多仓不互相覆盖（非 CMP 模式 sub="" 行为不变）。
CMP_MODE = False
cmp_seq = 0
cmp_events = {}      # trigger_id -> {sym,t,anchor,thr,ws_low,mkt_px,M_oid,L_oid,M_fill,L_fill}
def cmp_seq_incr():
    global cmp_seq
    cmp_seq += 1
    return cmp_seq

# ── 系统性事件熔断（P0 安全网）────────────────────────────────
# 为什么必须加：日常插针是单币零星事件；系统性事件（全市场闪崩）会让几十个币在
# 同一分钟内一起插针，而这类事件的特征是"不回弹"（实测回弹率 25.2% vs 日常 62.3%）。
# 此时 58 币埋伏单会被同时击穿 → 全部成交 → 全部不回弹 → 集体裸仓，且往往叠加 IP
# 漂移导致平仓 -2015 失败。熔断是这套策略唯一能防住"一次归零"的机制。
# 规则：SPIKE_WINDOW 秒内 ≥SPIKE_MIN_SYMBOLS 个不同标的插针 ≥SPIKE_PCT
#       → 撤掉自己全部挂单 + 停止挂新单 SPIKE_COOLDOWN 秒（已有持仓照常平）。
SPIKE_PCT = 0.05        # 单币插针幅度阈值
SPIKE_WINDOW = 300.0    # 统计窗口（秒）
SPIKE_MIN_SYMBOLS = 40  # 触发熔断的标的数量（357 币宇宙：40/357≈11% 同时插针≥5%=真实普跌信号）
SPIKE_COOLDOWN = 3600.0 # 熔断冷却（秒）
SPIKE_REF_TTL = 60.0    # "平静锚价"刷新间隔（秒）

spike_ref = {}       # symbol -> 平静期参考价（每 SPIKE_REF_TTL 秒刷新一次）
spike_ref_ts = {}    # symbol -> 参考价更新时间
spike_seen = {}      # symbol -> 上次记事件时间（防同一币重复计数）
spike_events = deque()   # [(ts, symbol)] 滚动窗口内的插针事件

halt_until = 0.0     # 熔断截止时间戳（>now 表示熔断中）
halt_reason = ""

# ── 杠杆与手续费 ────────────────────────────────────────────
# 【接飞刀策略的生命线】埋伏单是"接下落的刀"：插针的典型形态是超调
# （跌 8% 触发 → 继续跌到 12% → 才回弹）。开最高杠杆时保证金 = 名义/杠杆，
# 反向 4~5% 就被强平 —— 等不到回弹就出局了。低杠杆才能"扛住超调、等到回弹"。
LEVERAGE = 5         # 目标杠杆；0 = 用交易所最高杠杆（不推荐，见上）。子账户上限5x(-4421)
FEE_MAKER = 0.0002   # 埋伏单成交（LIMIT = maker）
FEE_TAKER = 0.0005   # 市价平仓（MARKET = taker）

# ── 交易所端止损（STOP_MARKET 条件单）────────────────────────
# 【为什么必须挂】ZEST 事故（2026-09-03）：失控拉盘 6s +22.7%，路线B 5s 市价硬平
# 吃 -2.25U。交易所端条件单的触发判定在币安服务器上——我们断网/死机/WS 断流它照常触发，
# 是唯一不依赖本机网络的保护。triggerPrice=成交价×(1±STOP_LOSS)，相对成交价而非锚价，
# 与埋伏阈值(6%/8%/10%)天然不重合。closePosition=true → 触发后全平且仓位归零自动失效。
# 只挂止损不挂止盈：止盈由路线A/B 管，挂了会打架出孤儿单。
STOP_LOSS = 0.10     # 止损幅度(相对成交价)；0=关闭
# 【-4130 修复】币安对同一(币, 方向)只允许存在一张 closePosition=true 的全平止损单。
# 风暴加仓(S腿)成交后若再挂第二张 → -4130 被拒，第二笔仓位实际没有自己的止损，
# "由哪张单保护"完全不可控。开启合并：加仓时撤旧单、按全部腿的加权均价重挂一张，
# 覆盖该方向全部仓位，触发价确定且唯一。
SL_MERGE = True
algo_stops = {}      # (sym, pos_side, sub) -> algoId  在册的条件止损单

# ── 工具 ─────────────────────────────────────────────────────
def rnd_price(sym, p):
    f = FILTERS[sym]
    return round(round(p / f["tick"]) * f["tick"], 8)

def calc_qty(sym, price, notional):
    f = FILTERS[sym]
    q = max(f["minQty"], int(notional / price / f["step"]) * f["step"])
    q = round(q, 8)
    # 币安要求 notional 严格大于 minNotional(5U), 等于即拒(-4164);
    # 且 qty 按 stepSize 向下取整后名义会缩水, 故留 5% 缓冲
    floor_notional = max(f["minNotional"] * 1.05, 5.25)
    while q * price < floor_notional:      # 低价腿保名义（minNotional 坑）
        q = round(q + f["step"], 8)
    return q

def mid(sym):
    m = mids.get(sym)
    if not m:
        return None
    return (m["bid"] + m["ask"]) / 2

def _record_spike(sym, m):
    """每次收到新中间价调用：对比"平静锚价"检测瞬时插针，记入滚动窗口。

    为什么用独立参考价而不用 last_mid：last_mid 每轮都会被刷新成当前价，插针时
    参考价跟着一起跑，反而检测不到偏离。这里用每 60s 刷新一次的"慢锚"。
    """
    now = time.time()
    ref = spike_ref.get(sym)
    if ref is None or now - spike_ref_ts.get(sym, 0) > SPIKE_REF_TTL:
        spike_ref[sym] = m
        spike_ref_ts[sym] = now
        return
    if ref <= 0:
        return
    if abs(m - ref) / ref >= SPIKE_PCT and now - spike_seen.get(sym, 0) > SPIKE_REF_TTL:
        spike_seen[sym] = now
        spike_events.append((now, sym))

def circuit_state():
    """返回 (窗口内插针标的数量, 触发阈值)。顺带清掉过期事件。"""
    now = time.time()
    while spike_events and now - spike_events[0][0] > SPIKE_WINDOW:
        spike_events.popleft()
    return len({s for _, s in spike_events}), SPIKE_MIN_SYMBOLS

# ── 启动准备：规格 + 杠杆 ────────────────────────────────────
def load_filters(symbols):
    c = 0; js = {}
    for url in (f"{PAPI}/fapi/v1/exchangeInfo",
                "https://fapi.binance.com/fapi/v1/exchangeInfo"):
        try:
            r = client_sync().get(url, timeout=30)
            if r.status_code == 200 and r.content[:1] in (b"[", b"{"):
                c, js = r.status_code, r.json()
                break
        except Exception:
            continue
    ok = []
    info = {}
    if c == 200 and isinstance(js.get("symbols"), list):
        for s in js["symbols"]:
            if s.get("status") == "TRADING":
                info[s["symbol"]] = s
    for sym in symbols:
        s = info.get(sym)
        if not s:
            log.warning(f"[规格] {sym} 不在交易列表(可能已下架) → 跳过")
            continue
        f = {}
        for fl in s.get("filters", []):
            if fl["filterType"] == "PRICE_FILTER":
                f["tick"] = float(fl["tickSize"])
            elif fl["filterType"] == "LOT_SIZE":
                f["step"] = float(fl["stepSize"]); f["minQty"] = float(fl["minQty"])
            elif fl["filterType"] == "MIN_NOTIONAL":
                f["minNotional"] = float(fl.get("notional", 5))
        f.setdefault("minNotional", 5.0)
        FILTERS[sym] = f
        ok.append(sym)
    return ok

lev_set = set()   # 已成功设置最高杠杆的币（IP 不在白名单时跳过，待恢复后自动补齐）

async def set_max_leverage(symbols):
    """每个币开到最高杠杆（取杠杆分层第一档 maxInitialLeverage）。已设过的跳过；
    出口 IP 未白名单(-2015)时整体跳过，待 IP 恢复后由分钟循环自动补齐。"""
    todo = [s for s in symbols if s not in lev_set]
    if not todo:
        return
    done = 0
    ip_blocked = False
    for sym in todo:
        if ip_blocked:
            continue
        c, br = await signed_request("GET", "/fapi/v1/leverageBracket", {"symbol": sym})
        if isinstance(br, dict) and br.get("code") == -2015:
            ip_blocked = True
            log.warning(f"[杠杆] 出口IP未白名单(-2015) → 跳过杠杆设置，待IP恢复后自动补齐")
            continue
        try:
            # papi/v1/um/leverageBracket 返回 list，bracket[0] 的 initialLeverage 即该币最高杠杆
            maxlev = int(br[0]["brackets"][0]["initialLeverage"])
        except Exception:
            log.warning(f"[杠杆] {sym} 取分层失败: {str(br)[:80]}")
            continue
        # 目标杠杆取 min(设定值, 交易所上限)。LEVERAGE=0 才回退到最高杠杆（不推荐）。
        target_lev = min(LEVERAGE, maxlev) if LEVERAGE and LEVERAGE > 0 else maxlev
        c, r = await signed_request("POST", "/fapi/v1/leverage", {"symbol": sym, "leverage": target_lev})
        if c == 200:
            lev_set.add(sym); done += 1
        else:
            log.warning(f"[杠杆] {sym} 设置 {target_lev}x 失败: {str(r)[:80]}")
        await asyncio.sleep(0.05)
    if done:
        log.info(f"[杠杆] 本轮新增最高杠杆 {done} 个（累计 {len(lev_set)}/{len(symbols)}）")

lev_locks = {}   # symbol -> asyncio.Lock（避免同币并发重复设杠杆）
args = None      # 模块级默认：main() 里 global args 重新赋值为真实参数。
                 # 【教训】ensure_leverage 引用了 main 内部的 args，而它不在模块级 ——
                 # 昨天没有这行时，首次真实触发即 NameError 崩溃（15:25/15:36 两次）。
async def ensure_leverage(sym):
    """触发时惰性设最高杠杆（一次/币，lev_set 缓存去重）。357 币启动时批量设=714 次签名请求会卡数分钟且易限频，
    故改为仅在实际触发交易的币上设一次；--no-lev 时整体跳过。"""
    if (args and args.no_lev) or sym in lev_set:
        return
    lock = lev_locks.get(sym) or lev_locks.setdefault(sym, asyncio.Lock())
    async with lock:
        if sym in lev_set:
            return
        c, br = await signed_request( "GET", "/fapi/v1/leverageBracket", {"symbol": sym})
        if isinstance(br, dict) and br.get("code") == -2015:
            log.warning(f"[杠杆] {sym} 出口IP未白名单(-2015) → 暂不设（IP恢复后下次触发重试）")
            return
        try:
            maxlev = int(br[0]["brackets"][0]["initialLeverage"])
        except Exception:
            return
        target_lev = min(LEVERAGE, maxlev) if LEVERAGE and LEVERAGE > 0 else maxlev
        c, r = await signed_request( "POST", "/fapi/v1/leverage",
                                       {"symbol": sym, "leverage": target_lev})
        if c == 200:
            lev_set.add(sym)
            log.info(f"[杠杆] {sym} 设为 {target_lev}x（触发惰性设置）")
        else:
            log.warning(f"[杠杆] {sym} 设 {target_lev}x 失败: {str(r)[:80]}")
            if isinstance(r, dict) and r.get("code") == -4421:
                lev_set.add(sym)
                log.info(f"[杠杆] {sym} -4421子账户限制 → 加入缓存不再重试")

async def prime_anchors(symbols):
    """启动时为每个币取一个回退锚价。
    【修复】改用批量 bookTicker API（不带 symbol 参数），1 个请求返回全市场，
    彻底消除逐个请求导致的并发失败和限流问题。"""
    done = 0
    try:
        r = client_sync().get(f"{PAPI}/fapi/v1/ticker/bookTicker", params=None, timeout=20)
        if r.status_code == 200 and r.content[:1] == b"[":
            rows = {x["symbol"]: x for x in r.json() if isinstance(x, dict)}
            for sym in symbols:
                x = rows.get(sym)
                if not x:
                    continue
                try:
                    b, a = float(x["bidPrice"]), float(x["askPrice"])
                except (KeyError, TypeError, ValueError):
                    continue
                if b > 0 and a > 0 and a >= b:
                    last_mid[sym] = (b + a) / 2
                    anchors[sym] = (b + a) / 2
                    done += 1
        else:
            log.warning(f"[锚价] 批量 bookTicker 请求失败: status={r.status_code}")
    except Exception as e:
        log.warning(f"[锚价] 批量 bookTicker 异常: {e}")
    log.info(f"[锚价] 已 prime 回退锚价 {done}/{len(symbols)} 个币 (批量API)")

def rest_refresh_anchors(stale_syms):
    """【补丁1】REST 批量拉最新 bookTicker，刷新 stale 币的锚价，返回刷到的 symbol 集合。

    为什么必须加：WS 断流时 last_mid 冻结在断流前的价格 → 锚价失真 → 重挂判定
    abs(O - last_ref) 恒≈0 永不触发 → 旧单距真实价可以无限拉开却不重挂
    （USELESS 事故：34 分钟 +10.6% 缓涨爬进旧 SELL 单，根因之一）。
    /fapi/v1/ticker/bookTicker 不带 symbol 一次返回全市场 → 1 个请求覆盖全部 stale 币，
    每轮最多调 1 次，不撞限流。public 端点无需签名，与 prime_anchors 同源(fapi)。"""
    try:
        r = client_sync().get(f"{PAPI}/fapi/v1/ticker/bookTicker",
                         params=None, timeout=15)
        if r.status_code != 200 or not r.content or r.content[:1] != b"[":
            return set()
        rows = {x["symbol"]: x for x in r.json() if isinstance(x, dict)}
        fixed = set()
        for sym in stale_syms:
            x = rows.get(sym)
            if not x:
                continue
            try:
                b, a = float(x["bidPrice"]), float(x["askPrice"])
            except (KeyError, TypeError, ValueError):
                continue
            if b > 0 and a > 0 and a >= b:
                last_mid[sym] = (b + a) / 2
                fixed.add(sym)
        return fixed
    except Exception:
        return set()

# ── WS：组合 bookTicker（公开行情）─────────────────────────────

async def _ws_instant_check(sym: str, px: float, src: str):
    """方案1：WS回调即时检测——收到价格时立刻判断偏离，不等50ms轮询。
    在bookTicker/aggTrade回调中调用，0延迟触发。
    src='BT'(bookTicker中间价) 或 'AT'(aggTrade成交价)。
    只做判断+触发，不阻塞WS消息循环（触发走spawn）。"""
    try:
        if not CMP_MODE and not GROUP_A and not GROUP_B:
            return
        if time.time() < halt_until:
            return
        if sym in STARTUP_HELD:
            return
        anc = anchors.get(sym)  # 平仓判断用实时锚价
        # 触发判断改用100ms窗口锚价（price_hist里最老的价格）
        _now_ms = int(time.time()*1000)
        _hist = price_hist.get(sym)
        anc_trig = None
        if _hist:
            for _hts, _hpx in _hist:
                if _now_ms - _hts >= 80:
                    anc_trig = _hpx
        if not anc_trig:
            anc_trig = anc  # 历史不够则回退到实时锚价
        if not anc_trig or anc_trig <= 0:
            return
        depth = DEPTH  # 固定阈值6%
        now = time.time()
        open_pos = {(k[0], k[1]) for k in positions_open.keys()}
        # 触发判断用100ms窗口锚价（anc_trig）
        thr    = anc_trig * (1 - depth)
        thr_up = anc_trig * (1 + depth)
        if px < thr:
            pos_long = (sym, "LONG") in open_pos
            if not pos_long and entry_cooldown.get((sym, "LONG", src), 0) <= now:
                if _entry_lock.get((sym, "LONG"), False):
                    return
                _entry_lock[(sym, "LONG")] = True
                await ensure_leverage(sym)
                for ksub in [k for k in list(tp_limits.keys()) if k[0] == sym and k[1] == "LONG"]:
                    await cancel_tp_limit(sym, "LONG", quiet=True, sub=ksub[2])
                for ksub in [k for k in list(algo_stops.keys()) if k[0] == sym and k[1] == "LONG"]:
                    await cancel_stop_algo(sym, "LONG", quiet=True, sub=ksub[2])
                if CMP_MODE:
                    tid = f"T{cmp_seq_incr():04d}"
                    p = {"tid": tid, "anc": anc_trig, "depth": depth, "t0": now, "ext": px}
                    log.info(f"[CMP触发✓][WS即时] {sym} tid={tid} src={src} 锚{anc_trig:.8f}(窗口100ms) -{depth:.0%} | "
                             f"M线程直发(0阻塞) | WS回调0延迟触发")
                    cmp_events[tid] = {"sym": sym, "pos_side": "LONG", "t": now, "anchor": anc_trig,
                                       "thr": thr, "mkt_px": px, "src": src,
                                       "M_oid": None, "L_oid": None}
                    asyncio.create_task(asyncio.to_thread(sync_fire_cmp_m, sym, "LONG", p, px))
            return
        if px > thr_up:
            pos_short = (sym, "SHORT") in open_pos
            if not pos_short and entry_cooldown.get((sym, "SHORT", src), 0) <= now:
                if _entry_lock.get((sym, "SHORT"), False):
                    return
                _entry_lock[(sym, "SHORT")] = True
                await ensure_leverage(sym)
                for ksub in [k for k in list(tp_limits.keys()) if k[0] == sym and k[1] == "SHORT"]:
                    await cancel_tp_limit(sym, "SHORT", quiet=True, sub=ksub[2])
                for ksub in [k for k in list(algo_stops.keys()) if k[0] == sym and k[1] == "SHORT"]:
                    await cancel_stop_algo(sym, "SHORT", quiet=True, sub=ksub[2])
                if CMP_MODE:
                    tid = f"T{cmp_seq_incr():04d}"
                    p = {"tid": tid, "anc": anc_trig, "depth": depth, "t0": now, "ext": px}
                    log.info(f"[CMP触发✓空][WS即时] {sym} tid={tid} src={src} 锚{anc_trig:.8f}(窗口100ms) +{depth:.0%} | "
                             f"M线程直发(0阻塞) | WS回调0延迟触发")
                    cmp_events[tid] = {"sym": sym, "pos_side": "SHORT", "t": now, "anchor": anc_trig,
                                       "thr": thr_up, "mkt_px": px, "src": src,
                                       "M_oid": None, "L_oid": None}
                    asyncio.create_task(asyncio.to_thread(sync_fire_cmp_m, sym, "SHORT", p, px))
    except Exception as e:
        log.warning(f"[WS即时] {sym} src={src} 检测异常: {e}")

async def ws_book_loop(symbols):
    """单连接订阅一批 symbol 的 bookTicker（实时买卖一）。357 币拆成多连接，每连接≤WS_CHUNK 个，
    避免单连接消息量过大导致 keepalive ping 超时断连（实测 50 币/连接≈314条/s 稳定不超时）。"""
    streams = "/".join(f"{s.lower()}@bookTicker" for s in symbols)
    uri = f"{FSTREAM_WS}/stream?streams={quote(streams, safe='')}"
    while not shutdown.is_set():
        try:
            async with websockets.connect(uri, proxy=PROXY or None, ping_interval=20,
                                          ping_timeout=30, open_timeout=15,
                                          ssl=_ws_ssl,
                                          additional_headers={"Host": _FSTREAM_HOST}) as ws:
                log.info(f"[WS] bookTicker 已连接 ({len(symbols)} 币/单连接)")
                async for msg in ws:
                    try:
                        d = json.loads(msg).get("data") or json.loads(msg)
                    except Exception:
                        continue
                    if not isinstance(d, dict) or d.get("e") != "bookTicker":
                        continue
                    # P3: WS数据校验 — bid/ask<=0 或 ask<bid 时丢弃
                    _bid_bt, _ask_bt = float(d["b"]), float(d["a"])
                    if _bid_bt <= 0 or _ask_bt <= 0 or _ask_bt < _bid_bt:
                        continue
                    mids[d["s"]] = {"bid": _bid_bt, "ask": _ask_bt,
                                    "ts": int(d.get("T") or time.time() * 1000)}
                    _record_spike(d["s"], (_bid_bt + _ask_bt) / 2)
                    # 【平仓快路径】该币有持仓 → 用刚收到的盘口中间价立即WS回调判定, 0轮询延迟。
                    # create_task 不阻塞行情流; exit_in_flight 占坑锁防与轮询双平。
                    _sym_bt, _mid_bt = d["s"], (_bid_bt + _ask_bt) / 2
                    anchors[_sym_bt] = _mid_bt  # 实时更新，用于平仓判断
                    # 【诊断日志】BT推送记录（只在anc存在且偏离>2%时打，避免日志爆炸）
                    if _sym_bt in RUN_SYMBOLS:
                        _anc_diag = anchors.get(_sym_bt)
                        if _anc_diag and _anc_diag > 0 and abs(_mid_bt/_anc_diag-1) > 0.02:
                            log.info(f"[BT推送] {_sym_bt} bid={_bid_bt} ask={_ask_bt} mid={_mid_bt:.8g} anc={_anc_diag:.8g} 偏离={abs(_mid_bt/_anc_diag-1)*100:.2f}%")
                    # BT不再写入price_hist：AT独立维护，防止BT mid价污染锚价
                    # BT平仓已移除：AT用真实成交价平仓，更准确无盘口噪声
                    # 【BT触发判断】用100ms前的价格作锚价，防止锚价跟价格同步导致触发窗口消失
                    if _sym_bt in RUN_SYMBOLS:
                        _hist_trig = price_hist.get(_sym_bt)
                        if _hist_trig and len(_hist_trig) >= 2:
                            # 取100ms前的价格：找最靠近(_ts_bt - 100ms)的历史价格
                            _anc_bt = None
                            for _hts, _hpx in _hist_trig:
                                if _ts_bt - _hts >= 80:  # >=80ms前的价格
                                    _anc_bt = _hpx
                            if _anc_bt and _anc_bt > 0:
                                _dev_bt = abs(_mid_bt / _anc_bt - 1)
                                if _dev_bt > DEPTH * 0.3:  # 偏离>30%阈值记录诊断
                                    _dir_bt = "↓" if _mid_bt < _anc_bt else "↑"
                                    log.info(f"[BT逼近] {_sym_bt} anc={_anc_bt:.8g} mid={_mid_bt:.8g} 偏离={_dev_bt*100:.2f}% {_dir_bt} hist={len(_hist_trig)}条")
                                # BT触发入场已移除：AT路径更快、用真实成交价，无盘口噪声
        except Exception as e:
            log.warning(f"[WS] bookTicker 断线({len(symbols)}币): {e} → 3s 后重连")
            await asyncio.sleep(3)

# ── WS：aggTrade（逐笔成交价，AB对照组B组数据源）──────────────────
trade_prices = {}   # sym -> {"price": float, "ts": int}

async def ws_aggtrade_loop(symbols, chunk_idx):
    """订阅一批 symbol 的 aggTrade（逐笔成交），更新 trade_prices。
    与 ws_book_loop 并行运行，提供逐笔成交价数据源。
    消息量比 bookTicker 大（热门币每秒几十条），但只更新字典不做计算，开销极小。"""
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)  # 用@trade替代@aggTrade（fstream的aggTrade流被阻断）
    uri = f"{FSTREAM_WS}/stream?streams={quote(streams, safe='')}"
    while not shutdown.is_set():
        try:
            async with websockets.connect(uri, proxy=PROXY or None, ping_interval=20,
                                          ping_timeout=30, open_timeout=15,
                                          ssl=_ws_ssl,
                                          additional_headers={"Host": _FSTREAM_HOST}) as ws:
                log.info(f"[WS] aggTrade#{chunk_idx} 已连接 ({len(symbols)} 币/单连接)")
                async for msg in ws:
                    try:
                        d = json.loads(msg).get("data") or json.loads(msg)
                    except Exception:
                        continue
                    _RAW_CNT[0] += 1
                    if _RAW_CNT[0] % 10000 == 0:
                        log.info(f"[AT原始] 第{_RAW_CNT[0]}条 e={d.get('e','?')} keys={list(d.keys())[:5]}")
                    if not isinstance(d, dict) or d.get("e") != "trade":
                        continue
                    # P3: aggTrade数据校验 — price<=0 时丢弃
                    _px_at = float(d["p"])
                    if _px_at <= 0:
                        continue
                    _AT_CNT[0] += 1
                    if _AT_CNT[0] % 1000 == 0:
                        log.info(f"[AT心跳] 已处理{_AT_CNT[0]}条aggTrade")
                    trade_prices[d["s"]] = {"price": _px_at,
                                            "ts": int(d.get("T") or time.time() * 1000)}
                    # 【平仓快路径】aggTrade同款: 有持仓就立即WS回调判定(逐笔成交价, 比盘口更精细)
                    _sym_at = d["s"]
                    _has_pos_at = any(k[0] == _sym_at for k in positions_open)
                    if _has_pos_at:
                        asyncio.create_task(_ws_exit_check(_sym_at, _px_at))
                    # 【AT写入price_hist】每条aggTrade都写，不依赖RUN_SYMBOLS，保证锚价可用
                    _ts_at_ms = int(d.get("T") or time.time()*1000)
                    _hist_at_w = price_hist.setdefault(_sym_at, deque())
                    if not _hist_at_w or _hist_at_w[-1][1] != _px_at:
                        _hist_at_w.append((_ts_at_ms, _px_at))
                    while _hist_at_w and _ts_at_ms - _hist_at_w[0][0] > PRICE_HIST_MS:
                        _hist_at_w.popleft()
                    # 【诊断日志】验证_sym_at in RUN_SYMBOLS判断（只在偏离>3%时打）
                    _in_run = _sym_at in RUN_SYMBOLS
                    _cmp_ok = CMP_MODE or GROUP_A or GROUP_B
                    _hist_at_diag = price_hist.get(_sym_at)
                    _anc_at_diag = None
                    if _hist_at_diag:
                        for _hts, _hpx in _hist_at_diag:
                            if _ts_at_ms - _hts >= 80:
                                _anc_at_diag = _hpx
                    if _anc_at_diag:
                        _dev_at_diag = abs(_px_at / _anc_at_diag - 1)
                        if _dev_at_diag > DEPTH * 0.5:  # 偏离>3%时打诊断
                            if not _in_run:
                                log.warning(f"[AT诊断] {_sym_at} 偏离{_dev_at_diag*100:.1f}%但不在RUN_SYMBOLS！RUN_SYMBOLS长度={len(RUN_SYMBOLS)}")
                            else:
                                log.info(f"[AT诊断] {_sym_at} 偏离{_dev_at_diag*100:.1f}% 在RUN_SYMBOLS={_in_run} CMP={_cmp_ok}")
                    # 【aggTrade内联入场·直接下单】
                    if _in_run and _cmp_ok:
                        # 取100ms前的价格作触发锚价
                        _hist_at = price_hist.get(_sym_at)
                        _anc_at = None
                        if _hist_at:
                            for _hts, _hpx in _hist_at:
                                if _ts_at_ms - _hts >= 80:
                                    _anc_at = _hpx
                        if not _anc_at:
                            continue  # 没有80ms前的锚价，跳过本次触发判断
                        if _anc_at and _anc_at > 0 and time.time() >= halt_until \
                                and _sym_at not in STARTUP_HELD:
                            _dev_at = abs(_px_at / _anc_at - 1)
                            if _dev_at > DEPTH * 0.5:  # 偏离>3%记录诊断
                                _dir_at = "↓" if _px_at < _anc_at else "↑"
                                _m_str = "主动卖" if bool(d.get("m")) else "主动买"
                                log.info(f"[AT逼近] {_sym_at} anc={_anc_at:.8g} px={_px_at:.8g} 偏离={_dev_at*100:.2f}% {_dir_at} {_m_str}")
                            if _dev_at > DEPTH:  # 超过6%阈值才处理
                                _now_at = time.time()
                                _op_at = {(k[0], k[1]) for k in positions_open.keys()}
                                _m_at = bool(d.get("m"))  # True=主动卖(价跌接多) False=主动买(价涨接空)
                                # 多单：跌破100ms前锚价*(1-DEPTH)
                                if _px_at < _anc_at * (1 - DEPTH):
                                    if not _m_at:  # 主动买推低不合理，过滤
                                        pass
                                    elif (_sym_at, "LONG") not in _op_at \
                                            and not _entry_lock.get((_sym_at, "LONG"), False) \
                                            and entry_cooldown.get((_sym_at, "LONG", "AT"), 0) <= _now_at:
                                        _entry_lock[(_sym_at, "LONG")] = True
                                        _tid_at = f"T{cmp_seq_incr():04d}"
                                        _p_at = {"tid": _tid_at, "anc": _anc_at, "depth": DEPTH,
                                                 "t0": _now_at, "ext": _px_at}
                                        cmp_events[_tid_at] = {"sym": _sym_at, "pos_side": "LONG",
                                            "t": _now_at, "anchor": _anc_at, "thr": _anc_at*(1-DEPTH),
                                            "mkt_px": _px_at, "src": "AT", "M_oid": None, "L_oid": None}
                                        log.info(f"[CMP触发✓][AT内联] {_sym_at} tid={_tid_at} src=AT "
                                                 f"锚{_anc_at:.8f}(窗口100ms) -{DEPTH:.0%} | aggTrade={_px_at:.8f} m=卖 → 线程直发")
                                        asyncio.create_task(asyncio.to_thread(sync_fire_cmp_m, _sym_at, "LONG", _p_at, _px_at))
                                # 空单：涨破100ms前锚价*(1+DEPTH)
                                elif _px_at > _anc_at * (1 + DEPTH):
                                    if _m_at:  # 主动卖推高不合理，过滤
                                        pass
                                    elif (_sym_at, "SHORT") not in _op_at \
                                            and not _entry_lock.get((_sym_at, "SHORT"), False) \
                                            and entry_cooldown.get((_sym_at, "SHORT", "AT"), 0) <= _now_at:
                                        _entry_lock[(_sym_at, "SHORT")] = True
                                        _tid_at = f"T{cmp_seq_incr():04d}"
                                        _p_at = {"tid": _tid_at, "anc": _anc_at, "depth": DEPTH,
                                                 "t0": _now_at, "ext": _px_at}
                                        cmp_events[_tid_at] = {"sym": _sym_at, "pos_side": "SHORT",
                                            "t": _now_at, "anchor": _anc_at, "thr": _anc_at*(1+DEPTH),
                                            "mkt_px": _px_at, "src": "AT", "M_oid": None, "L_oid": None}
                                        log.info(f"[CMP触发✓空][AT内联] {_sym_at} tid={_tid_at} src=AT "
                                                 f"锚{_anc_at:.8f}(窗口100ms) +{DEPTH:.0%} | aggTrade={_px_at:.8f} m=买 → 线程直发")
                                        asyncio.create_task(asyncio.to_thread(sync_fire_cmp_m, _sym_at, "SHORT", _p_at, _px_at))
        except Exception as e:
            log.warning(f"[WS] aggTrade#{chunk_idx} 断线({len(symbols)}币): {e} → 3s 后重连")
            await asyncio.sleep(3)

# ── WS：用户数据流（成交推送）─────────────────────────────────
LISTEN_KEY = [None]

async def keep_listen_key():
    while not shutdown.is_set():
        try:
            if LISTEN_KEY[0] is None:
                r = client_sync().post(f"{PAPI}/fapi/v1/listenKey", timeout=15)
                LISTEN_KEY[0] = r.json().get("listenKey")
                if LISTEN_KEY[0]:
                    log.info("[WS] listenKey 已获取")
            else:
                client_sync().put(f"{PAPI}/fapi/v1/listenKey", timeout=15)
        except Exception as e:
            log.warning(f"[listenKey] 保活失败: {e}")
            LISTEN_KEY[0] = None
        await asyncio.sleep(25 * 60)

async def ws_user_loop():
    last_lat_warn = 0.0   # 延迟告警节流(30s最多1条)
    while not shutdown.is_set():
        if LISTEN_KEY[0] is None:
            await asyncio.sleep(2)
            continue
        try:
            uri = f"{FSTREAM_WS}/ws/{LISTEN_KEY[0]}"
            async with websockets.connect(uri, proxy=PROXY or None, ping_interval=20,
                                          ping_timeout=30, open_timeout=15,
                                          ssl=_ws_ssl,
                                          additional_headers={"Host": _FSTREAM_HOST}) as ws:
                log.info("[WS] 用户数据流已连接")
                async for msg in ws:
                    d = json.loads(msg)
                    if d.get("e") == "ORDER_TRADE_UPDATE":
                        o = d["o"]
                        # 【监控】回报延迟 = 服务器校正时钟 - 交易所事件时间。Clash卡流(11.8s裸奔事故)
                        # 从这里当场现形，不再靠事后复盘。>2s告警, >10s强制重连换连接。
                        lat = (time.time() * 1000 + OFF) - int(o.get("T") or d.get("E") or 0)
                        if lat > 10000:
                            log.warning(f"[WS延迟⚠] 用户流回报延迟 {lat/1000:.1f}s >10s → 强制重连")
                            wecom_notify.send_async(f"⚠️埋伏合约 用户流延迟{lat/1000:.1f}s 已强制重连")
                            break   # 跳出 async for → with 退出连接 → 重连
                        elif lat > 2000 and time.time() - last_lat_warn > 30:
                            last_lat_warn = time.time()
                            log.warning(f"[WS延迟] 用户流回报延迟 {lat/1000:.1f}s >2s (Clash卡流?)")
                        if o.get("X") == "FILLED":
                            oid = int(o["i"])
                            if oid in orders:
                                spawn(on_fill(oid, float(o.get("ap") or o["p"]),
                                              float(o["z"]), o["s"]))
                            elif oid in tp_limits.values():
                                # 【修复·TP限价成交】在册限价止盈单成交 → 交易所已把仓平掉,
                                # 脚本必须立即清登记/撤止损/释放锁 + 发平仓通知 + 记pnl。
                                # 9/23实盘实锤: 缺这条路径 → 路线A/B对着已平的仓发市价单 →
                                # -2022无限重试370+次烧穿REST配额触发IP封禁。
                                _tpk = next((k for k, v in tp_limits.items() if v == oid), None)
                                if _tpk:
                                    _tsym, _tps, _tsub = _tpk
                                    tp_limits.pop(_tpk, None)
                                    _tp = positions_open.pop(_tpk, None)
                                    _entry_lock.pop((_tsym, _tps), None)
                                    spawn(cancel_stop_algo(_tsym, _tps, sub=_tsub, quiet=True))
                                    if _tp:
                                        _texit = float(o.get("ap") or o.get("p") or 0)
                                        _tentry = float(_tp.get("entry") or 0)
                                        _tqty = float(_tp.get("qty") or 0)
                                        _tgross = (_texit - _tentry) * _tqty if _tps == "LONG" else (_tentry - _texit) * _tqty
                                        _tfee = (_tentry * FEE_MAKER + _texit * FEE_TAKER) * _tqty
                                        _tpnl = _tgross - _tfee
                                        stats["pnl"] += _tpnl
                                        stats["routeA"] += 1
                                        log.info(f"[TP限价成交✓] {_tsym} {_tps} 交易所侧已平 @{_texit:.8g} "
                                                 f"pnl≈{_tpnl:+.4f}U | 累计{stats['fills']}次 {stats['pnl']:+.4f}U")
                                        if wecom_notify:
                                            wecom_notify.notify_close(_tsym, _tps, _tentry, _texit, _tpnl,
                                                                      route="A", tag="TP限价",
                                                                      total_pnl=stats["pnl"], fills=stats["fills"])
                                    else:
                                        # 登记已被兑底复核清掉 → 只补一条日志, 不重复通知
                                        log.info(f"[TP限价成交✓] {_tsym} {_tps} 交易所侧已平(登记已被兑底清理)")
                            elif MODE == "resident" and o["s"] in RUN_SYMBOLS:
                                # 常驻条件单成交: 条件单触发生成的市价单不在 orders 表,
                                # 用原始订单类型 ot=TAKE_PROFIT_MARKET 识别(react进程的
                                # 入场是MARKET/LIMIT, SL是STOP_MARKET, 不会误中此分支)
                                ot = o.get("ot") or o.get("o")
                                if ot == "TAKE_PROFIT_MARKET":
                                    spawn(on_fill_resident(o["s"], o["S"],
                                                           float(o.get("ap") or o["p"]),
                                                           float(o["z"])))
        except Exception as e:
            log.warning(f"[WS] 用户流断线: {e} → 3s 后重连")
            LISTEN_KEY[0] = None
            await asyncio.sleep(3)

# ── 成交处理 → A/B 路线退出 ──────────────────────────────
async def _ws_exit_check(sym, px):
    """【WS平仓快路径】行情WS( bookTicker/aggTrade )每收到一笔价, 该币有仓就立即判定:
    - A窗口内( t_fill + T_A 之前 ): 价回到路线A目标(锚价±REB_PCT冻结在仓位登记) → 市价平(route=A)
    - B阶段: 触及亏损上限B_LOSS(3%,冻结) 或 盈利即平(回成交价) → 市价平(route=B)
    0轮询延迟(原50/100ms轮询仍保留为WS断流兑底)。全部判定参数从仓位登记读取, 不重算。
    exit_in_flight 占坑锁: 同仓同时只允许一张市价平仓单在飞, 占不到坑直接返回(轮询在管)。"""
    for (s, ps, sub), p in [kv for kv in positions_open.items() if kv[0][0] == sym]:
        # 占坑失败 = 该仓已有平仓单在飞(轮询路径或另一路WS), 直接让位
        if (sym, ps) in exit_in_flight:
            continue
        is_long = ps == "LONG"
        target = p.get("target") or 0.0
        loss_cap = p.get("loss_cap") or 0.0
        t_fill = p.get("t_fill") or time.time()
        entry_px = float(p.get("entry") or 0.0)
        O = float(p.get("O") or 0.0)
        hit = None
        if time.time() - t_fill < T_A:
            # A窗口: 回到目标就平
            if (is_long and px >= target) or (not is_long and px <= target):
                hit = "A"
        else:
            # B阶段: 盈利1%即平 优先于止损判定
            if B_BREAKEVEN:
                target_b = entry_px * (1 + B_PROFIT_PCT) if is_long else entry_px * (1 - B_PROFIT_PCT)
                if (is_long and px >= target_b) or (not is_long and px <= target_b):
                    hit = f"B盈利{B_PROFIT_PCT:.0%}"
                elif loss_cap > 0 and ((is_long and px <= loss_cap) or (not is_long and px >= loss_cap)):
                    hit = "B亏损上限"
            elif loss_cap > 0 and ((is_long and px <= loss_cap) or (not is_long and px >= loss_cap)):
                hit = "B亏损上限"
        if not hit:
            continue
        exit_in_flight.add((sym, ps))
        try:
            # 占坑后复确认仓位还在(TP限价成交/兑底复核可能刚清掉)
            if (sym, ps, sub) not in positions_open:
                continue
            log.info(f"[WS快平] {sym} {ps} @{px:.8g} 命中{hit} → 市价平仓(0延迟)")
            ok = await market_close(sym, ps, float(p["qty"]), entry_px, O,
                                    hit, tag=f"CMP-WS" if p.get("cmp") else "WS快")
            if ok:
                positions_open.pop((sym, ps, sub), None)
                _entry_lock.pop((sym, ps), None)
                await cancel_stop_algo(sym, ps, sub=sub)
                await cancel_tp_limit(sym, ps, sub=sub)
        except Exception as e:
            log.warning(f"[WS快平✗] {sym} {ps} {type(e).__name__}: {e} → 轮询兑底仍在")
        finally:
            exit_in_flight.discard((sym, ps))

# ── 聚合仓位: 同币同方向多腿的合计数量与加权均价 ───────────────
def _agg_position(sym, pos_side):
    """【-4130 修复配套】同币同方向所有在册腿的合计数量与加权均价。
    例：M腿 0.016783×593 + 风暴腿 0.015677×627 → 均价 0.0162146（而非任一单腿价）。
    返回 (总数量, 加权均价)；无在册腿时返回 (0.0, 0.0)。"""
    tot_q, tot_v = 0.0, 0.0
    for (s, ps, _sb), p in positions_open.items():
        if s != sym or ps != pos_side:
            continue
        q = float(p.get("qty") or 0.0)
        if q <= 0:
            continue
        tot_q += q
        tot_v += q * float(p.get("entry") or 0.0)
    if tot_q <= 0:
        return 0.0, 0.0
    return tot_q, tot_v / tot_q

def place_stop_algo_sync(sym, pos_side, entry_px, stop_pct=None):
    """成交后立刻挂交易所端止损条件单（同步版，供 to_thread 调用）。
    stop_pct=None 用全局 STOP_LOSS；传 0.0 = 保本单(trigger=成交价)。"""
    sl = STOP_LOSS if stop_pct is None else stop_pct
    trig = entry_px * (1 + sl) if pos_side == "SHORT" else entry_px * (1 - sl)
    params = {
        "symbol": sym,
        "side": "BUY" if pos_side == "SHORT" else "SELL",   # 平仓方向
        "positionSide": pos_side,
        "type": "STOP_MARKET",
        "triggerPrice": f"{rnd_price(sym, trig):.8f}".rstrip("0").rstrip("."),
        "closePosition": "true",
        "workingType": "CONTRACT_PRICE",
    }
    return sync_signed_request("POST", "/fapi/v1/order", params, tries=3)

async def place_stop_algo(sym, pos_side, entry_px, sub="", stop_pct=None):
    """挂止损条件单并登记。失败只告警不抛——路线B(5s硬平)仍是兜底，
    且 -4400 可能拒单（风控优先级高于保护，接受）。sub=对比模式子仓键(trigger_id:腿)。
    stop_pct=None 用全局 STOP_LOSS；传 0.0 = 保本单(trigger=成交价)。"""
    sl = STOP_LOSS if stop_pct is None else stop_pct
    if sl <= 0 and stop_pct is None:
        return
    # 【-4130 修复】同(币,方向)已有其他腿在册(如风暴加仓)时，币安只允许一张全平单。
    # 做法：先撤掉该方向所有在册全平单 → 按"全部腿的加权均价"重挂唯一一张，覆盖全部仓位。
    same_keys = [k for k in positions_open if k[0] == sym and k[1] == pos_side]
    merge = SL_MERGE and len(same_keys) > 1
    if merge:
        for k in [k for k in algo_stops if k[0] == sym and k[1] == pos_side]:
            aid = algo_stops.pop(k, None)
            if not aid:
                continue
            try:
                c, r = await signed_request( "DELETE", "/fapi/v1/order",
                                               {"orderId": aid, "symbol": sym}, tries=2)
                log.info(f"[止损合并] {sym} {pos_side} 撤旧全平单 algoId={aid}: "
                         f"{'OK' if c in (200, 400) else str(r)[:50]}")
            except Exception as e:
                log.warning(f"[止损合并✗] {sym} {pos_side} 撤旧单异常 {type(e).__name__}: {e}")
        _tq, avg_px = _agg_position(sym, pos_side)
        if avg_px > 0:
            log.info(f"[止损合并] {sym} {pos_side} 共{len(same_keys)}腿 合计{_tq:g}张 "
                     f"加权均价{avg_px:.8g} → 改按均价×{sl:.0%}挂全平单(原单腿价{entry_px:.8g})")
            entry_px = avg_px
        else:
            merge = False
    try:
        c, r = await asyncio.to_thread(place_stop_algo_sync, sym, pos_side, entry_px, stop_pct)
        if c == 200 and r.get("orderId"):
            if merge:
                # 同一 algoId 被该方向所有腿共享：任意一腿平仓时都能找到并撤掉它
                for k in [k for k in algo_stops if k[0] == sym and k[1] == pos_side]:
                    algo_stops.pop(k, None)
                for k in same_keys:
                    algo_stops[k] = r["orderId"]
            else:
                algo_stops[(sym, pos_side, sub)] = r["orderId"]
            log.info(f"[止损✓] {sym} {pos_side} STOP_MARKET 挂出 触发@{r.get('triggerPrice', '?')} "
                     f"({'合并均价' if merge else '单腿成交价'}±{sl:.0%}, orderId={r['orderId']})")
        else:
            log.warning(f"[止损✗] {sym} {pos_side} 条件单被拒: {str(r)[:80]} "
                        f"(code=-4130 表示同向已有一张全平单, 检查 SL_MERGE) → 本机路线B硬平兜底")
    except Exception as e:
        log.warning(f"[止损✗] {sym} {pos_side} 挂单异常 {type(e).__name__}: {e} → 路线B兜底")

async def cancel_stop_algo(sym, pos_side, quiet=False, sub=""):
    """撤掉该仓位的止损条件单。仓位已平后 closePosition 单大概率已被币安自动失效
    (-2011)，视为成功。失败也只记录——残留条件单无持仓无法成交，且会被币安过期。"""
    aid = algo_stops.pop((sym, pos_side, sub), None)
    if not aid:
        return
    # 【-4130 修复配套】合并模式下同一 algoId 被该方向多腿共享。若还有其他腿仍在持仓，
    # 撤掉它等于让那些腿裸奔 → 保留不撤，只解除本腿的映射（由最后一腿平仓时撤）。
    still = [k for k in positions_open
             if k[0] == sym and k[1] == pos_side and k[2] != sub]
    if still:
        log.info(f"[止损保留] {sym} {pos_side} 同向仍有{len(still)}腿在册 → "
                 f"保留全平止损 algoId={aid} 继续保护")
        return
    try:
        c, r = await signed_request( "DELETE", "/fapi/v1/order",
                                       {"orderId": aid, "symbol": sym}, tries=3)
        ok = c == 200 or c == 400   # -2011(已触发/已失效)也算清
        if not quiet or not ok:
            log.info(f"[止损撤] {sym} {pos_side} algoId={aid}: "
                     f"{'OK' if ok else str(r)[:60]}")
    except Exception as e:
        log.warning(f"[止损撤✗] {sym} {pos_side} algoId={aid} {type(e).__name__}: {e}")

async def place_tp_limit(sym, pos_side, entry_px, sub="", tp_pct=None):
    """成交后挂交易所端止盈限价单（接管原路线B的5s市价硬平）。
    LONG→SELL LIMIT @entry×(1+TP_PCT)；SHORT→BUY LIMIT @entry×(1-TP_PCT)。sub=对比模式子仓键。
    tp_pct=风暴单等腿自定止盈(默认None用全局TP_PCT)。"""
    if TP_PCT <= 0 and not tp_pct:
        return
    try:
        p = positions_open.get((sym, pos_side, sub))
        if not p:
            return
        tpp = TP_PCT if tp_pct is None else tp_pct
        tp_px = entry_px * (1 + tpp) if pos_side == "LONG" else entry_px * (1 - tpp)
        params = {
            "symbol": sym,
            "side": "SELL" if pos_side == "LONG" else "BUY",   # 平仓方向
            "positionSide": pos_side,
            "type": "LIMIT", "timeInForce": "GTC",
            "price": f"{rnd_price(sym, tp_px):.8f}".rstrip("0").rstrip("."),
            "quantity": f"{p['qty']:.8f}".rstrip("0").rstrip("."),
        }
        c, r = await signed_request( "POST", "/fapi/v1/order", params, tries=3)
        if c == 200:
            tp_limits[(sym, pos_side, sub)] = int(r["orderId"])
            log.info(f"[止盈✓] {sym} {pos_side} LIMIT 挂出 @{r.get('price', '?')} "
                     f"(成交价+{tpp:.0%}, oid={r['orderId']})")
        else:
            log.warning(f"[止盈✗] {sym} {pos_side} 限价单被拒: {str(r)[:70]} → 仅靠路线A/SL")
    except Exception as e:
        log.warning(f"[止盈✗] {sym} {pos_side} 挂单异常 {type(e).__name__}: {e}")

async def cancel_tp_limit(sym, pos_side, quiet=False, sub=""):
    """撤掉该仓位的止盈限价单。"""
    oid = tp_limits.pop((sym, pos_side, sub), None)
    if not oid:
        return
    try:
        c, r = await signed_request(
            "DELETE", "/fapi/v1/order",
            {"symbol": sym, "orderId": oid}, tries=3)
        if not quiet or c != 200:
            log.info(f"[止盈撤] {sym} {pos_side} oid={oid}: {'OK' if c == 200 else str(r)[:60]}")
    except Exception as e:
        log.warning(f"[止盈撤✗] {sym} {pos_side} oid={oid} {type(e).__name__}: {e}")

async def place_limit_tp(sym, pos_side, qty, tp_px, sub=""):
    """限价止盈单（异步）：在route A目标价挂BUY/SELL LIMIT平仓。
    与市价路线A并行，先成交者胜出。用固定qty避免子账号-4136。
    SHORT→BUY LIMIT @tp_px；LONG→SELL LIMIT @tp_px。"""
    side = "BUY" if pos_side == "SHORT" else "SELL"
    params = {
        "symbol": sym, "side": side, "positionSide": pos_side,
        "type": "LIMIT", "timeInForce": "GTC",
        "price": f"{rnd_price(sym, tp_px):.8f}".rstrip("0").rstrip("."),
        "quantity": f"{qty:.8f}".rstrip("0").rstrip("."),
    }
    try:
        c, r = await signed_request("POST", "/fapi/v1/order", params, tries=3)
        if c == 200:
            tp_limits[(sym, pos_side, sub)] = int(r["orderId"])
            log.info(f"[限价止盈✓] {sym} {pos_side} LIMIT @{r.get('price','?')} "
                     f"(oid={r['orderId']}) → 异步等待成交, 与市价路线A并行")
        else:
            log.warning(f"[限价止盈✗] {sym} {pos_side} 挂单被拒: {str(r)[:70]} → 仅靠市价路线A")
    except Exception as e:
        log.warning(f"[限价止盈✗] {sym} {pos_side} 挂单异常 {type(e).__name__}: {e}")

FILL_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cmp_fills.csv")

def record_fill_csv(sym, pos_side, cmp_tag, tid, anchor, entry_px, qty, depth):
    """【验证工具】每笔成交落盘一行 → 事后用 aggTrades 逐笔对照"成交在何处、是否最优"。
    字段: 时间, 币, 方向, 腿M/L, 触发id, 锚价, 触发阈值, 成交价, 数量, 成交时中间价。
    只追加不阻塞：open/写/关共毫秒级，绝不延误挂TP/SL。"""
    try:
        header_needed = not os.path.exists(FILL_CSV)
        with open(FILL_CSV, "a", encoding="utf-8") as f:
            if header_needed:
                f.write("ts,sym,side,leg,tid,anchor,depth,entry_px,qty,mid_at_fill\n")
            m = mid(sym)
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]},{sym},{pos_side},"
                    f"{cmp_tag or ''},{tid or ''},{anchor or ''},{depth if depth is not None else ''},"
                    f"{entry_px},{qty},{m if m else ''}\n")
    except Exception as e:
        log.warning(f"[成交CSV] 写入失败(不影响交易): {e}")

async def on_fill(order_id, entry_px, qty, sym):
    if order_id in exits_in_progress:
        return
    exits_in_progress.add(order_id)
    o = orders.pop(order_id, None)
    if not o:
        return
    pos_side = "LONG" if o["side"] == "BUY" else "SHORT"
    O = o["O"]
    sub = o.get("sub") or ""          # 对比模式子仓键(同触发 M/L 不同，避免同币两仓互相覆盖)
    cmp_tag = o.get("cmp")            # "M"/"L" 仅对比模式有
    # 撤对面单（自己 track 的）
    # 【修复】原按 round 匹配：撤旧单失败而残留的旧单（round 不同）在本币成交时撤不掉，
    # 会一直挂在账户里 → 孤儿单累积（这正是说明书 §4.1 "200+ 单"乱象的另一条路径，
    # 单实例锁只防住了双实例，没防住这条）。同一币在本进程下的单都是我们下的，全撤才安全。
    if not (CMP_MODE or MODE == "resident"):
        for k, v in list(orders.items()):
            if v["symbol"] == sym:
                orders.pop(k)
                c, r = await signed_request( "DELETE", "/fapi/v1/order",
                                               {"symbol": sym, "orderId": k})
                log.info(f"[成交] {sym} {pos_side} @{entry_px} x{qty} | 撤同币单{k}: "
                         f"{'OK' if c == 200 else str(r)[:60]}")
    else:
        log.info(f"[成交] {sym} {pos_side} @{entry_px} x{qty} cmp={cmp_tag} tid={sub} "
                 f"(对比模式: 保留同币另一腿, 各自独立管理)")
    stats["fills"] += 1
    if cmp_tag == "M":
        stats["cmp_M_fills"] += 1
    elif cmp_tag == "L":
        stats["cmp_L_fills"] += 1
    elif cmp_tag == "R":
        stats["cmp_R_fills"] = stats.get("cmp_R_fills", 0) + 1
    record_fill_csv(sym, pos_side, cmp_tag, sub, O, entry_px, qty, o.get("depth"))
    # 【企业微信通知】成交通报。必须放在挂 TP/SL 之前（先通知再保护），
    # 但 notify 内部走守护线程非阻塞，不会延误下面毫秒必争的止损/止盈单。
    if wecom_notify:
        wecom_notify.notify_fill(sym, pos_side, entry_px, qty,
                                 cmp_tag=cmp_tag or "", tid=sub or "", anchor=O or 0.0)
    # 【修复】原 key=sym：同一币 LONG/SHORT 同时成交（插针来回扫）时后写覆盖前写，
    # 前一个仓位就此丢失追踪 → 裸仓无人管。改为 (symbol, pos_side) 双键各自独立追踪。
    # 【WS快路径配套】入场时就把退出参数冻结进仓位登记(t_fill/目标价/止损线),
    # WS回调快路径只读不重算 → 单一事实源, 防止快路径与路线A/B口径漂移。
    _tfill = time.time()
    # 路线A目标：成交价×(1+TP_PCT) 盈利3%
    _exit_target = entry_px * (1 + TP_PCT) if pos_side == "LONG" else entry_px * (1 - TP_PCT)
    # 止损线：成交价×(1-B_LOSS) 亏损3%
    _exit_loss_cap = entry_px * (1 - B_LOSS) if pos_side == "LONG" else entry_px * (1 + B_LOSS)
    positions_open[(sym, pos_side, sub)] = {"pos_side": pos_side, "qty": qty, "entry": entry_px,
                                            "O": O, "order_id": order_id, "cmp": cmp_tag,
                                            "t_fill": _tfill, "target": _exit_target,
                                            "loss_cap": _exit_loss_cap}
    # 【止损已禁用】子账号所有条件单类型都被拒(code -4120)，挂SL白白浪费~0.4s。
    # routeB本机轮询兜底即可。CEO 20260916 定。
    # spawn(place_stop_algo(sym, pos_side, entry_px, sub, stop_pct=B_LOSS))   # 8% 止损兜底(对齐回测B亏损上限)
    # 【路线A 目标】回弹到目标价就平仓。
    # 默认：相对成交价 (entry×(1±TP_PCT))。
    # --reb-anchor 开启：相对锚价 (O×(1∓REB_PCT))，与回测口径一致（CEO 20260907 新20币实验）。
    tpp_a = TP_PCT
    if REB_ANCHOR:
        target = O * (1 - REB_PCT) if pos_side == "LONG" else O * (1 + REB_PCT)
    else:
        target = entry_px * (1 + tpp_a) if pos_side == "LONG" else entry_px * (1 - tpp_a)
    # 限价止盈@route A目标价(已移除,日志显示全部失败,仅靠WS快路径平仓)
    last_ref.pop(sym, None)   # 该币单已成交消耗→清锚价，平完仓回来必重挂(防误判"未动"漏补单)
    # 入场即检查：如果入场价已满足路线A条件，直接市价平仓(限价止盈已在路上)
    if (pos_side == "SHORT" and entry_px <= target) or \
       (pos_side == "LONG"  and entry_px >= target):
        log.info(f"[路线A立即] {sym} {pos_side} entry={entry_px:.8g} 已在目标{target:.8g}内 → 直接市价平仓")
        exit_in_flight.add((sym, pos_side))   # 占坑锁: 防WS快路径同时发平仓单
        try:
            ok = await market_close(sym, pos_side, qty, entry_px, O, "A立即",
                                    tag=f"CMP-{cmp_tag}" if cmp_tag else "")
        finally:
            exit_in_flight.discard((sym, pos_side))
        if ok:
            positions_open.pop((sym, pos_side, sub), None)
            _entry_lock.pop((sym, pos_side), None)
            await cancel_stop_algo(sym, pos_side, sub=sub)
            await cancel_tp_limit(sym, pos_side, sub=sub)
        else:
            log.critical(f"[告警] {sym} {pos_side} 路线A立即平仓失败, 保留仓位+TP/SL 待收尾")
        return
    t0 = time.time()
    route = None
    # 路线A期间同步跟踪best，传入routeB让反转阈值有更合理的基准(CEO 20260916 定)
    best = entry_px
    while time.time() - t0 < T_A and not shutdown.is_set():
        if (sym, pos_side, sub) not in positions_open:
            break        # 限价止盈已成交,仓位已空,无需继续
        m = mid(sym) or entry_px
        if pos_side == "LONG":
            if m > best: best = m
        else:
            if m < best: best = m
        if ((pos_side == "LONG" and m >= target) or
                (pos_side == "SHORT" and m <= target)):
            route = "A"
            break
        await asyncio.sleep(0.05)
    if route == "A":
        if (sym, pos_side) in exit_in_flight:
            return   # WS快路径已接管平仓, 让位避免双平
        exit_in_flight.add((sym, pos_side))
        try:
            ok = await market_close(sym, pos_side, qty, entry_px, O, "A", tag=f"CMP-{cmp_tag}" if cmp_tag else "")
        finally:
            exit_in_flight.discard((sym, pos_side))
        if ok:
            positions_open.pop((sym, pos_side, sub), None)
            _entry_lock.pop((sym, pos_side), None)
            await cancel_stop_algo(sym, pos_side, sub=sub)
            await cancel_tp_limit(sym, pos_side, sub=sub)
        else:
            # 仓位还在→保留 TP/SL 条件单：万一 bot 崩了/断网，交易所端仍保护仓位
            log.critical(f"[告警] {sym} {pos_side} 路线A未平掉，保留仓位+TP/SL 待收尾/重试")
        return
    # 路线B：路线A(锚价∓6%/1s)未触发 → 自适应退出(盈利即平 + bl3%止损 + 10s超时)
    # 交易所端止损已注释(子账号不支持条件单)，本机轮询兜底。
    await route_b_adaptive(sym, pos_side, qty, entry_px, O, sub, cmp_tag, best_init=best)
    return

async def branch_exit(sym, pos_side, qty, entry_px, O, sub="", cmp_tag=""):
    """【分岔式退出 --route-branch】5s 未快速回弹后，看当前盈亏分岔：
    - 已盈利 → 止损上移保本(成交价) + 移动止盈(从本单最高/最低点回撤 BRANCH_TR 就市价平)
    - 仍亏损 → 快砍：止损收紧到 -BRANCH_CUT，触到就市价平
    - 无论哪种 → 超过 BRANCH_HOLD 秒强制市价平
    本机盯盘（移动止盈/快砍交易所托管单做不了固定价）；交易所端固定 STOP_LOSS 仍挂着兜底。
    """
    tag = f"branch-{cmp_tag}" if cmp_tag else "branch"
    is_long = pos_side == "LONG"
    # 极值锚：多单记录成交后最高价，空单记录最低价（移动止盈用）
    extreme = entry_px
    profit_branch = None   # True=盈利分支, False=亏损分支；首次判定后不再切换(保本优先)
    t0 = time.time()
    cancelled_stop = False
    while time.time() - t0 < BRANCH_HOLD and not shutdown.is_set():
        m = mid(sym)
        if m is None:
            await asyncio.sleep(0.1)
            continue
        # 更新极值
        if is_long:
            extreme = max(extreme, m)
            pnl_now = (m - entry_px) / entry_px
        else:
            extreme = min(extreme, m)
            pnl_now = (entry_px - m) / entry_px
        # 首次判定分岔方向
        if profit_branch is None:
            profit_branch = pnl_now > 0
            if profit_branch:
                # 盈利分支：撤掉原固定SL，改挂保本单(trigger=成交价)，让移动止盈接管
                if not cancelled_stop:
                    await cancel_stop_algo(sym, pos_side, sub=sub)
                    await place_stop_algo(sym, pos_side, entry_px, sub=sub, stop_pct=0.0)
                    cancelled_stop = True
                    log.info(f"[branch] {sym} {pos_side} 分岔→盈利分支: 止损上移保本@{entry_px:.8g} + 移动止盈(回撤{BRANCH_TR:.0%})")
            else:
                log.info(f"[branch] {sym} {pos_side} 分岔→亏损分支: 快砍止损-{BRANCH_CUT:.0%}")
        # 执行分支逻辑
        if profit_branch:
            # 移动止盈：从极值回撤 BRANCH_TR 就平
            trail_line = extreme * (1 - BRANCH_TR) if is_long else extreme * (1 + BRANCH_TR)
            if (is_long and m <= trail_line) or (not is_long and m >= trail_line):
                ok = await market_close(sym, pos_side, qty, entry_px, O, "B", tag=tag)
                if ok:
                    positions_open.pop((sym, pos_side, sub), None)
                    _entry_lock.pop((sym, pos_side), None)
                    await cancel_stop_algo(sym, pos_side, sub=sub)
                    await cancel_tp_limit(sym, pos_side, sub=sub)
                else:
                    log.critical(f"[告警] {sym} {pos_side} 分岔移动止盈未平掉，保留仓位+SL 待收尾/重试")
                return
        else:
            # 快砍：跌破 -cut% 就平
            cut_line = entry_px * (1 - BRANCH_CUT) if is_long else entry_px * (1 + BRANCH_CUT)
            if (is_long and m <= cut_line) or (not is_long and m >= cut_line):
                ok = await market_close(sym, pos_side, qty, entry_px, O, "B", tag=tag)
                if ok:
                    positions_open.pop((sym, pos_side, sub), None)
                    _entry_lock.pop((sym, pos_side), None)
                    await cancel_stop_algo(sym, pos_side, sub=sub)
                    await cancel_tp_limit(sym, pos_side, sub=sub)
                else:
                    log.critical(f"[告警] {sym} {pos_side} 分岔快砍未平掉，保留仓位+SL 待收尾/重试")
                return
        await asyncio.sleep(0.1)
    # 超时强制平
    ok = await market_close(sym, pos_side, qty, entry_px, O, "B", tag=tag)
    if ok:
        positions_open.pop((sym, pos_side, sub), None)
        _entry_lock.pop((sym, pos_side), None)
        await cancel_stop_algo(sym, pos_side, sub=sub)
        await cancel_tp_limit(sym, pos_side, sub=sub)
        log.info(f"[branch] {sym} {pos_side} 超时{BRANCH_HOLD:.0f}s强制平仓")
    else:
        log.critical(f"[告警] {sym} {pos_side} 分岔超时未平掉，保留仓位+SL 待收尾/重试")


async def route_b_adaptive(sym, pos_side, qty, entry_px, O, sub="", cmp_tag="", best_init=None):
    """【路线B·自适应退出(回测最优方案 2026-09-22)】路线A未触发后接管。
    三道出口(本机轮询):
      ① 盈利即平 B_BREAKEVEN(价格回到成交价就市价平)
      ② 亏损上限 B_LOSS(3%, 相对成交价) 硬砍
      ③ 时间止损 B_TIME(10s, 自路线A窗口结束起算) 强制市价平
    (利润带B_PROFIT和反转B_REVERSAL已弃用: 盈利即平总是在利润带之前触发; 反转在亏损位触发是bug)"""
    is_long = pos_side == "LONG"
    best = best_init if best_init is not None else entry_px
    t0 = time.time()
    tag = f"routeB-{cmp_tag}" if cmp_tag else "routeB"
    while time.time() - t0 < T_A + B_TIME and not shutdown.is_set():
        m = mid(sym)
        if m is None:
            await asyncio.sleep(0.1); continue
        # ① 盈利即平(价格回到成交价)
        if B_BREAKEVEN:
            if (is_long and m >= entry_px) or (not is_long and m <= entry_px):
                if (sym, pos_side) in exit_in_flight:
                    return   # WS快路径已接管平仓, 让位避免双平
                exit_in_flight.add((sym, pos_side))
                try:
                    return await _finish_routeB(await market_close(sym, pos_side, qty, entry_px, O, "B盈利即平", tag=tag), sym, pos_side, sub)
                finally:
                    exit_in_flight.discard((sym, pos_side))
        # ② 亏损上限(3%止损)
        loss_cap = entry_px * (1 - B_LOSS) if is_long else entry_px * (1 + B_LOSS)
        if (is_long and m <= loss_cap) or (not is_long and m >= loss_cap):
            if (sym, pos_side) in exit_in_flight:
                return   # WS快路径已接管平仓, 让位避免双平
            exit_in_flight.add((sym, pos_side))
            try:
                return await _finish_routeB(await market_close(sym, pos_side, qty, entry_px, O, "B亏损上限", tag=tag), sym, pos_side, sub)
            finally:
                exit_in_flight.discard((sym, pos_side))
        await asyncio.sleep(0.1)
    # ③ 时间止损
    if (sym, pos_side) in exit_in_flight:
        return   # WS快路径已接管平仓, 让位避免双平
    exit_in_flight.add((sym, pos_side))
    try:
        return await _finish_routeB(await market_close(sym, pos_side, qty, entry_px, O, "B超时", tag=tag), sym, pos_side, sub)
    finally:
        exit_in_flight.discard((sym, pos_side))


async def _finish_routeB(ok, sym, pos_side, sub):
    if ok:
        positions_open.pop((sym, pos_side, sub), None)
        _entry_lock.pop((sym, pos_side), None)
        await cancel_stop_algo(sym, pos_side, sub=sub)
        await cancel_tp_limit(sym, pos_side, sub=sub)
    else:
        log.critical(f"[告警] {sym} {pos_side} 路线B未平掉，保留仓位+SL 待收尾/重试")


async def market_close(sym, pos_side, qty, entry, O, route,
                       stop_on_shutdown=True, max_tries=0, tag=""):
    """市价平仓。失败就一直重试（IP 漂移 -2015 时，换连接重试可能绕回白名单节点）。
    max_tries=0 → 无限重试；stop_on_shutdown=True 时一旦 shutdown 置位立即放弃。
    【修复·-2022】ReduceOnly 被拒且持仓已为0 → 该仓已被交易所侧(TP限价/条件单)平掉,
    视为已平成功返回, 不再无限重试。这是 9/23 实盘实锤的倖尸重试循环根因
    (B2USDT 重试370+次烧穿REST配额触发IP封禁)。"""
    side = "SELL" if pos_side == "LONG" else "BUY"
    t0 = time.time(); attempt = 0
    while max_tries == 0 or attempt < max_tries:
        attempt += 1
        c, r = await signed_request(
            "POST", "/fapi/v1/order",
            {"symbol": sym, "side": side, "positionSide": pos_side,
             "type": "MARKET", "quantity": qty})
        if c == 200:
            exit_px = float(r.get("avgPrice") or 0) or mid(sym) or entry
            gross = (exit_px - entry) * qty if pos_side == "LONG" else (entry - exit_px) * qty
            # 【修复】原只算毛盈亏、未扣手续费 → 统计偏乐观。
            # 埋伏单是 LIMIT(maker) 成交、市价平仓是 taker，两边各计一次。
            fee = (entry * FEE_MAKER + exit_px * FEE_TAKER) * qty
            pnl = gross - fee
            stats["pnl"] += pnl
            stats[{"A": "routeA", "B": "routeB"}.get(route, "fallback")] += 1
            log.info(f"[平仓✓] {('['+tag+'] ') if tag else ''}{sym} {pos_side} 路线{route} 出场@{exit_px} "
                     f"pnl={pnl:+.4f}U (第{attempt}次, 耗时{time.time()-t0:.1f}s) | "
                     f"累计{stats['fills']}次 {stats['pnl']:+.4f}U")
            # 【企业微信通知】平仓通报（含入场→出场价、本笔与累计盈亏）
            if wecom_notify:
                wecom_notify.notify_close(sym, pos_side, entry, exit_px, pnl,
                                          route=route, tag=tag,
                                          total_pnl=stats["pnl"], fills=stats["fills"])
            return True
        # 【修复·-2022】ReduceOnly被拒 → 先查真实持仓; 已为0 = 交易所侧已平, 视为成功并终結
        if isinstance(r, dict) and r.get("code") == -2022:
            try:
                c2, pr = await signed_request("GET", "/fapi/v2/positionRisk", {"symbol": sym}, tries=3)
                amt = 0.0
                if c2 == 200 and isinstance(pr, list):
                    amt = next((float(x.get("positionAmt", 0) or 0) for x in pr
                                if x.get("positionSide") == pos_side), 0.0)
                if amt == 0:
                    log.warning(f"[平仓·已平] {sym} {pos_side} -2022但持仓=0 → 交易所侧已平(TP/SL), "
                                f"视为已平, 停止重试。需手工核对成交价后再计pnl")
                    if wecom_notify:
                        wecom_notify.send_async(
                            f"埋伏合约通知\nℹ️ {sym} {pos_side} 交易所侧已平仓(TP/SL先成交)\n"
                            f"脚本市价单被-2022拒绝(正常)\n"
                            f"脚本侧按已平处理, 盈亏以交易所流水为准\n时间：{wecom_notify._now()}")
                    return True
                # 持仓还在但ReduceOnly被拒 = 数量/状态异常, 继续重试但报错可见
                log.error(f"[平仓✗] {sym} {pos_side} -2022但持仓={amt} → 数量可能不匹配, 继续重试")
            except Exception as e:
                log.warning(f"[平仓✗] {sym} {pos_side} -2022后查持仓失败: {e} → 继续重试")
        if stop_on_shutdown and shutdown.is_set():
            break
        back = min(15, attempt * 2)
        log.error(f"[平仓✗] {sym} {pos_side} 第{attempt}次失败: {str(r)[:80]} → {back}s后重试")
        await asyncio.sleep(back)
    log.critical(f"[平仓✗] {sym} {pos_side} x{qty} 路线{route} 未能平掉 → 需手动处理！")
    return False

# ── 兜底：每 60s 核对挂单状态（防 WS 漏事件）──────────────────
async def fallback_poll():
    while not shutdown.is_set():
        await asyncio.sleep(60)
        for oid, o in list(orders.items()):
            c, r = await signed_request( "GET", "/fapi/v1/order",
                                           {"symbol": o["symbol"], "orderId": oid})
            if c == 200 and r.get("status") == "FILLED":
                log.warning(f"[兜底] {o['symbol']} 订单{oid} 已成交但未收到WS推送 → 立即退出")
                stats["fallback"] += 1
                spawn(on_fill(oid, float(r.get("avgPrice") or r.get("price")),
                              float(r.get("executedQty") or 0), o["symbol"]))
        # 【复核】本策略开的持仓：若已被交易所 TP/SL 平掉（路线B留仓场景），
        # 清 positions_open + 撤残留 TP/SL，释放该币重新埋伏。
        for (sym, ps, _sub), p in list(positions_open.items()):
            c, pr = await signed_request( "GET", "/fapi/v2/positionRisk",
                                           {"symbol": sym})
            if c == 200 and isinstance(pr, list):
                amt = next((float(x["positionAmt"]) for x in pr
                            if x.get("positionSide") == ps and float(x.get("positionAmt", 0)) != 0), 0.0)
                if amt == 0:
                    log.info(f"[复核] {sym} {ps} 持仓已被交易所平掉(TP/SL) → 清状态, 释放埋伏")
                    for k in [k for k in list(positions_open.keys()) if k[0] == sym]:
                        positions_open.pop(k, None)
                    for k in [k for k in list(positions_open.keys()) if k[0] == sym]:
                        _entry_lock.pop((k[0], k[1]), None)
                    for k in [k for k in list(tp_limits.keys()) if k[0] == sym]:
                        await cancel_tp_limit(sym, ps, sub=k[2], quiet=True)
                        tp_limits.pop(k, None)
                    for k in [k for k in list(algo_stops.keys()) if k[0] == sym]:
                        await cancel_stop_algo(sym, ps, sub=k[2], quiet=True)
                        algo_stops.pop(k, None)

async def l_leg_watch(oid, sym):
    """【新增】CMP限价腿监督：0.5s 间隔 REST 查单（不等用户流 WS——Clash 卡流时它会晚 11s+）。
    - 5s 内成交 → 立即走 on_fill 挂 TP/SL（把 L 腿裸奔窗口从 12s 压到 ≤5.5s）
    - 5s 未成交 → 撤单（CEO 规则：过期机会不赌，也杜绝"M 已平仓 L 才成交"的反向亏损单）
    - 撤单失败(常为 -2011 刚好成交) → 再查一次，已成交按成交处理
    与 WS 竞态安全：on_fill 先从 orders 弹出订单，重复调用自动 no-op。"""
    if oid not in orders:
        return
    t0 = time.time()
    while not shutdown.is_set() and time.time() - t0 < L_TIMEOUT_S:
        await asyncio.sleep(0.5)
        if oid not in orders:      # WS 已先确认成交
            return
        c, r = await signed_request( "GET", "/fapi/v1/order",
                                       {"symbol": sym, "orderId": oid})
        if c != 200:
            continue
        st = r.get("status")
        if st == "FILLED":
            log.info(f"[CMP-L成交✓] {sym} 订单{oid} REST确认 @{r.get('avgPrice', '?')} (不等WS推送)")
            await on_fill(oid, float(r.get("avgPrice") or r.get("price") or 0),
                          float(r.get("executedQty") or 0), sym)
            return
        # PARTIALLY_FILLED / NEW → 继续轮询到超时
    # ── 超时撤单 ──
    if oid not in orders:
        return
    c, r = await signed_request( "DELETE", "/fapi/v1/order",
                                   {"symbol": sym, "orderId": oid})
    if c == 200:
        orders.pop(oid, None)
        log.info(f"[CMP-L撤单✓] {sym} 订单{oid} {L_TIMEOUT_S:.0f}s未成交 → 已撤 (过期机会不赌)")
    else:
        c2, r2 = await signed_request( "GET", "/fapi/v1/order",
                                         {"symbol": sym, "orderId": oid})
        if c2 == 200 and r2.get("status") == "FILLED":
            log.info(f"[CMP-L成交✓] {sym} 订单{oid} 撤单竞态: 实际已成交 @{r2.get('avgPrice', '?')} → 按成交处理")
            await on_fill(oid, float(r2.get("avgPrice") or r2.get("price") or 0),
                          float(r2.get("executedQty") or 0), sym)
        else:
            log.warning(f"[CMP-L撤单✗] {sym} 订单{oid} 撤单失败: {str(r)[:60]} → 留给兜底轮询复核")

# ── 常驻埋伏模式（CEO定 20260907，--mode resident）────────────
def _resident_state_file():
    suf = f"_{INSTANCE}" if INSTANCE else ""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), f"resident_state{suf}.json")

def _resident_save():
    """条件单登记落盘: 崩溃/重启后能撤掉上次运行的残留条件单(防双单重复入场)。"""
    try:
        json.dump({f"{k[0]}|{k[1]}": v for k, v in res_cond.items()},
                  open(_resident_state_file(), "w"))
    except Exception as e:
        log.warning(f"[常驻] 状态保存失败(不影响交易): {e}")

def _resident_load_cancel():
    """启动时撤销上次运行遗留的条件单。只撤自己 state 文件里登记的 algoId,
    绝不批量撤账户条件单——react 进程的止损条件单也在同一账户, 不能误伤。"""
    f = _resident_state_file()
    if not os.path.exists(f):
        return 0
    n = 0
    try:
        d = json.load(open(f))
        for key, v in d.items():
            try:
                sym, side = key.split("|")
                sync_signed_request("DELETE", "/fapi/v1/order",
                               {"orderId": v["orderId"], "symbol": sym}, tries=2)
                n += 1
            except Exception:
                pass
        os.remove(f)
    except Exception:
        pass
    return n

def _resident_sweep_logs():
    """启动兜底清理: 扫描【常驻进程自己的历史日志】里出现过的所有 algoId 并撤掉。
    用途: 重挂时若撤旧失败/崩溃, 旧单会成为"孤儿单"永久留在交易所(越积越多, 且触发价
    随行情漂移后会把趋势单当插针打)——本函数按日志全量对账, 保证不留残余。
    安全边界: 只扫 resident 前缀的日志文件, 绝不扫 react 进程的日志(其止损条件单同账户)。"""
    d = os.path.dirname(os.path.abspath(__file__))
    prefix = f"resident{'_' + INSTANCE if INSTANCE else ''}"
    files = [os.path.join(d, f) for f in os.listdir(d)
             if f.startswith(prefix) and (f.endswith(".log") or f.endswith(".tmp"))]
    ld = os.path.join(d, "logs")
    if os.path.isdir(ld):
        files += [os.path.join(ld, f) for f in os.listdir(ld)
                  if prefix in f and f.endswith(".log")]
    aids = {}
    for f in files:
        try:
            txt = open(f, encoding="utf-8", errors="ignore").read()
        except Exception:
            continue
        for m in _re.finditer(r"\[常驻✓\] (\S+) (BUY|SELL) [^\n]*?algoId=(\d+)", txt):
            aids.setdefault((m.group(1), m.group(2)), set()).add(int(m.group(3)))
    n = 0
    # 【隔离补丁 20260908】主 resident 实例的 prefix="resident", startswith 会把
    # resident_exp11_out.tmp / resident_exp50_out.tmp 也扫进来 → 撤掉那两个实例**当前活跃**
    # 的条件单(它们随后只能缺单补挂, 白烧 API 且短暂裸奔)。三个实例币种列表零重叠,
    # 故按"本实例负责的币"过滤即可彻底隔离。
    _own = set(RUN_SYMBOLS)
    _skip = 0
    for (sym, side), s in aids.items():
        if _own and sym not in _own:
            _skip += 1
            continue
        for aid in s:
            try:
                c, _r = sync_signed_request("DELETE", "/fapi/v1/order",
                                       {"orderId": aid, "symbol": sym}, tries=2)
                if c == 200:
                    n += 1
            except Exception:
                pass
    if aids:
        log.info(f"[常驻] 日志对账: 扫描到 {sum(len(v) for v in aids.values())} 个历史algoId, "
                 f"成功撤除 {n} 张"
                 f"{f' | 跳过 {_skip} 个非本实例币种(隔离保护)' if _skip else ''}")
    return n

async def _place_conditional(sym, side, trigger, base, register=True):
    """挂常驻入场条件单: TAKE_PROFIT_MARKET (BUY@跌10%触发/SELL@涨10%触发)。
    papi 条件单走 /papi/v1/um/algo/order (algoType=CONDITIONAL, 与止损同端点, 生产已验证)。
    workingType=CONTRACT_PRICE(最新价) + 不传 priceProtect(插针=最新/标记价差最大时刻)。
    返回 (ok, algoId)。register=False: 不写 res_cond(重挂场景由调用方先记旧algoId再原子替换,
    避免"覆写后撤错单→旧单变孤儿单"——20260907 修复的致命bug)。"""
    pos_side = "LONG" if side == "BUY" else "SHORT"
    qty = calc_qty(sym, trigger, NOTIONAL)
    params = {
        "symbol": sym,
        "side": side, "positionSide": pos_side,
        "type": "TAKE_PROFIT_MARKET",
        "triggerPrice": f"{rnd_price(sym, trigger):.8f}".rstrip("0").rstrip("."),
        "quantity": f"{qty:.8f}".rstrip("0").rstrip("."),
        "workingType": "CONTRACT_PRICE",
    }
    c, r = await signed_request( "POST", "/fapi/v1/order", params, tries=3)
    if c == 200 and r.get("orderId"):
        aid = int(r["orderId"])
        if register:
            res_cond[(sym, side)] = {"orderId": aid, "trigger": trigger, "base": base}
            res_base[sym] = base   # 【P0修复 20260913】入场锚价回填: 成交后 on_fill_resident 取此作平仓锚O, 与回测口径一致(原漏赋值→永远回退entry_px)
            _resident_save()
            # last_ref 是 minute_cycle 的 LIMIT 重挂路径用的簿记; resident 的条件单跟随判定
            # 走 resident_loop 里的 cond["base"], 与此无关。此处仅登记备用, 不参与 resident 重挂。
            last_ref[sym] = base
        log.info(f"[常驻✓] {sym} {side} TAKE_PROFIT_MARKET 触发@{trigger:.8g} x{qty} "
                 f"(锚{base:.8g} ±{RESIDENT_DEPTH:.0%}, algoId={aid})")
        return True, aid
    log.warning(f"[常驻✗] {sym} {side} 条件单被拒: {str(r)[:90]}")
    return False, None

async def _cancel_algo_by_id(sym, side, algo_id, quiet=True):
    """按 algoId 精确撤单(重挂场景专用: 撤的是被替换掉的旧单, 不是 res_cond 里当前那张)。
    -2011(已触发/已失效)视为成功。返回 True=已撤/已失效, False=需重试(登记 res_orphan)。"""
    if not algo_id:
        return True
    try:
        c, r = await signed_request( "DELETE", "/fapi/v1/order",
                                       {"orderId": algo_id, "symbol": sym}, tries=3)
        ok = c == 200 or c == 400
        if not quiet or not ok:
            log.info(f"[常驻撤旧] {sym} {side} algoId={algo_id}: "
                     f"{'OK' if ok else str(r)[:60]}")
        return bool(ok)
    except Exception as e:
        log.warning(f"[常驻撤旧✗] {sym} {side} algoId={algo_id} {type(e).__name__}: {e}")
        return False

async def _cancel_conditional(sym, side, quiet=False):
    """撤常驻条件单。-2011(已触发/已失效)视为成功。"""
    v = res_cond.pop((sym, side), None)
    if not v:
        return
    _resident_save()
    try:
        c, r = await signed_request( "DELETE", "/fapi/v1/order",
                                       {"orderId": v["orderId"], "symbol": sym}, tries=3)
        ok = c == 200 or c == 400
        if not quiet or not ok:
            log.info(f"[常驻撤] {sym} {side} orderId={v['orderId']}: "
                     f"{'OK' if ok else str(r)[:60]}")
    except Exception as e:
        log.warning(f"[常驻撤✗] {sym} {side} {type(e).__name__}: {e}")

async def on_fill_resident(sym, side, entry_px, qty):
    """常驻条件单成交 → 合成 R 腿订单复用 on_fill 全套流水线
    (交易所端SL先挂 → TP限价 → 风暴单接力 → 3秒路线A → 路线B留仓)。
    CEO定: 不撤另一侧(冻结, 保留原触发价); 仓位平掉后自动解冻双侧重挂。"""
    pos_side = "LONG" if side == "BUY" else "SHORT"
    key = (sym, pos_side)
    now = time.time()
    if res_fills.get(key, 0) > now - 120:      # WS与REST双通道去重(同步判断, 无竞态)
        return
    res_fills[key] = now
    res_cond.pop((sym, side), None)            # 已成交的条件单从在册表移除
    _resident_save()
    res_freeze[sym] = True                     # 冻结该币: 另一侧保留原触发价, 平仓后解冻
    base = res_base.get(sym) or entry_px
    oid = -(int(time.time() * 1000) % 10**12) - 7   # 合成负数oid, 与真实orderId不冲突
    orders[oid] = {"symbol": sym, "side": side, "qty": qty, "O": base,
                   "round": -1, "cmp": "R", "sub": "R", "trigger_id": f"R:{sym}",
                   "depth": RESIDENT_DEPTH}
    log.info(f"[常驻成交✓] {sym} {side} @{entry_px:.8g} x{qty} (条件单触发, 锚{base:.8g}) "
             f"→ SL/TP/风暴 | 另一侧保留(冻结)")
    await on_fill(oid, entry_px, qty, sym)
    res_base.pop(sym, None)

def _open_algo_orders():
    """拉取交易所当前全部 open algo 条件单的**完整对象**列表。失败返回 None。
    反向对账(撤交易所有、登记表没有的孤儿单)需要 symbol/side/type, 只有 algoId 不够。"""
    try:
        c, data = sync_signed_request("GET", "/fapi/v1/openOrders", tries=2)
        if c == 200:
            if isinstance(data, dict):
                return list(data.get("orders", []))
            if isinstance(data, list):
                return data
    except Exception as e:
        log.warning(f"[常驻对账] 拉取openAlgoOrders失败: {e}")
    return None


def _open_algo_ids():
    """拉取交易所当前全部 open 条件单的 orderId 集合。
    GET /fapi/v1/openOrders。失败返回 None(调用方跳过本轮对账)。"""
    orders = _open_algo_orders()
    if orders is None:
        return None
    return {str(x.get("orderId")) for x in orders}

async def _reconcile_res_cond():
    """与交易所对账: res_cond 里交易所已不存在的单(过期/外撤/接口下线期损失)
    → 移除登记, 下一轮循环自动补挂。修复'登记表死单卡住补挂'问题。"""
    open_ids = await asyncio.to_thread(_open_algo_ids)
    if not open_ids:            # None=查询失败, 空=交易所无单(也算有效结果)
        if open_ids is None:
            return
    stale = [k for k, v in res_cond.items() if str(v.get("orderId")) not in open_ids]
    if stale:
        for k in stale:
            res_cond.pop(k, None)
        _resident_save()
        log.warning(f"[常驻对账] 清除交易所已消失的登记单 {len(stale)} 张: "
                    + ",".join(f"{k[0]}|{k[1]}" for k in stale[:10])
                    + ("…" if len(stale) > 10 else "") + " → 将自动补挂")


async def _resident_reverse_reconcile():
    """反向对账(孤儿单自愈合): 以交易所为准, 凡是本实例币种里、res_cond 未登记的条件单 → 撤除。

    修复两道兜底同时失效导致的孤儿堆积:
      (1) resident_state.json 在失败启动时被 os.remove → _resident_load_cancel 返回0;
      (2) _resident_sweep_logs 正则匹配中文被转义(\\u2713) → 也返回0。
    本函数不依赖任何本地文件, 直接拉交易所当前挂单, 凡 symbol∈RUN_SYMBOLS 且 algoId
    不在 res_cond 登记的, 一律视为孤儿撤掉(含 state 被删时 res_cond 为空→全撤, 循环随即补挂)。
    隔离: symbol 不在本实例 RUN_SYMBOLS 的单绝不碰(防误伤其他实例)。"""
    orders = await asyncio.to_thread(_open_algo_orders)
    if orders is None:
        log.warning("[常驻反向对账] 拉取交易所挂单失败, 跳过本轮(下个周期重试)")
        return
    managed = {str(v.get("algoId")) for v in res_cond.values()}
    orphan = []
    for o in orders:
        sym = o.get("symbol")
        if not sym or sym not in RUN_SYMBOLS:
            continue
        aid = str(o.get("algoId"))
        if aid not in managed:
            orphan.append((sym, o.get("side"), aid))
    if not orphan:
        return
    log.warning(f"[常驻反向对账] 发现 {len(orphan)} 张孤儿单(交易所有/本实例未登记) → 撤除自愈合")
    for sym, side, aid in orphan:
        if await _cancel_algo_by_id(sym, side, aid, quiet=False):
            log.info(f"[常驻反向对账✓] 撤孤儿 {sym} {side} algoId={aid}")
        else:
            res_orphan[(sym, side, aid)] = time.time()
            log.warning(f"[常驻反向对账✗] 撤孤儿失败, 转入孤儿重试队列 {sym} {side} algoId={aid}")


async def resident_loop():
    """常驻条件单循环(CEO定 20260907):
    - 每币双侧 TAKE_PROFIT_MARKET: BUY@锚×0.90 / SELL@锚×1.10, CONTRACT_PRICE
    - 单侧锚价漂移≥2% → 先挂新后撤旧(原子, 不留洞)
    - 单边成交 → 冻结该币(另一侧保留原触发价, CEO:不撤); 双侧平仓 → 解冻双侧重挂
    - REST positionRisk 每15s兜底(WS漏推时补处理成交)
    - 熔断: 撤全部条件单冷却(持仓+其TP/SL保留), 冷却结束自动重挂

    【秒级跟随改造 20260908】跟随周期 RES_TICK = ANCHOR_SEC(封顶15s):
      · 逐币维护是**纯本地比价**, 只有偏移≥2%才真发单 → 缩周期不增加静默期API
      · 阴跌(XAN型): 每几秒移动2~8% → 触发价一路跟随下移 → 永远打不到 → 过滤 ✅
      · 真乌龙(FORM型): 1~3秒内跳>JUMP_GATE(8%) → 判为针尖不跟随 → 旧单被打 → 捕获 ✅
      · 重API(positionRisk 全量持仓)仍按 15s 门控, 不随周期变密"""
    global halt_until, halt_reason
    RES_TICK = max(1, min(15, int(ANCHOR_SEC)))       # 跟随周期(秒), 复用 --anchor-sec
    _pr_t = 0.0                                       # positionRisk 兜底上次执行时刻
    log.info(f"[常驻] 循环启动: {len(RUN_SYMBOLS)}币×2侧 | 深度±{RESIDENT_DEPTH:.0%} "
             f"| 漂移{RESIDENT_REPEG:.0%}重挂 | 跟随周期{RES_TICK}s"

             f" | TP/SL/风暴由成交后流水线接管")
    await asyncio.sleep(5)
    n0 = await asyncio.to_thread(_resident_load_cancel)
    n1 = await asyncio.to_thread(_resident_sweep_logs)
    if n0 or n1:
        log.warning(f"[常驻] 启动清理: 尝试撤除上次运行遗留条件单 {n0}(state)+{n1}(日志对账) 张"
                    f"(已失效者返回-2011属正常) → 防孤儿单堆积/双单重复入场")
    await _reconcile_res_cond()      # 启动即对账: 清掉登记表中交易所已消失的死单
    await _resident_reverse_reconcile()  # 启动反向对账: 撤交易所有/本实例未登记的孤儿单(自愈合)
    rec_t = time.time()
    hb_t = time.time()
    while not shutdown.is_set():
        await asyncio.sleep(RES_TICK)
        # 熔断(与minute_cycle同一判据, 双保险): 撤条件单, 持仓交其TP/SL保护
        n_spike, thr = circuit_state()
        if n_spike >= thr and time.time() >= halt_until:
            halt_until = time.time() + SPIKE_COOLDOWN
            halt_reason = f"常驻:{SPIKE_WINDOW:.0f}s内{n_spike}标的插针≥{SPIKE_PCT:.0%}"
            log.critical(f"[熔断!] {halt_reason} → 撤全部常驻条件单, 冷却{SPIKE_COOLDOWN / 60:.0f}分钟")
            for (s2, sd2) in list(res_cond.keys()):
                await _cancel_conditional(s2, sd2, quiet=True)
        if time.time() < halt_until:
            if time.time() - hb_t >= 300:
                log.info(f"[常驻心跳] 熔断中(至{datetime.fromtimestamp(halt_until).strftime('%H:%M')})")
                hb_t = time.time()
            continue
        # 孤儿单清理: 上轮撤旧失败的单, 本轮重试(绝不让条件单在交易所堆积)
        if res_orphan:
            for _k in list(res_orphan.keys()):
                if await _cancel_algo_by_id(_k[0], _k[1], _k[2], quiet=False):
                    res_orphan.pop(_k, None)
        # REST兜底: WS漏推时按持仓反推成交(positionRisk一次全量拉)
        # 【秒级改造】固定15s门控: 跟随周期缩到1~3s后, 这条重API绝不能跟着变密(否则限频翻15倍)
        if time.time() - _pr_t >= 15:
            _pr_t = time.time()
            try:
                c, pr = await signed_request( "GET", "/fapi/v2/positionRisk", tries=3)
                if c == 200 and isinstance(pr, list):
                    for x in pr:
                        s = x.get("symbol", "")
                        if s not in RUN_SYMBOLS or s in STARTUP_HELD:
                            continue
                        amt = float(x.get("positionAmt", 0) or 0)
                        if amt == 0:
                            continue
                        ps = "LONG" if amt > 0 else "SHORT"
                        if (s, ps, "R") not in positions_open and res_fills.get((s, ps), 0) <= time.time() - 120:
                            ep = float(x.get("entryPrice") or 0) or mid(s) or 0
                            log.warning(f"[常驻兜底] {s} {ps} 发现未登记持仓 x{abs(amt)} @≈{ep:.8g} → 按R腿成交处理")
                            await on_fill_resident(s, "BUY" if ps == "LONG" else "SELL", ep, abs(amt))
            except Exception as e:
                log.warning(f"[常驻] 持仓兜底查询失败: {type(e).__name__}: {e}")
        # 逐币维护
        for sym in RUN_SYMBOLS:
            if shutdown.is_set():
                break
            cur = mid(sym)
            if not cur:
                continue
            pos_l = (sym, "LONG", "R") in positions_open
            pos_s = (sym, "SHORT", "R") in positions_open
            # 解冻: 双侧仓位都没了 → 撤冻结, 全部按当前价重建
            if not pos_l and not pos_s and res_freeze.get(sym):
                res_freeze.pop(sym)
                log.info(f"[常驻解冻] {sym} 仓位已平 → 恢复双侧常驻")
            if res_freeze.get(sym):
                # 冻结期(CEO定:不撤另一侧)。但加一道安全距离守卫:
                # 存活侧触发价若距现价<8%(太近)→上移重挂到±10%, 仍不撤单。
                # 正常路径用不到(仓位TP+5%即平, 冻结侧在+10%外, 安全垫16.4%);
                # 这是 TP挂单失败/跳空越过 等异常场景的兜底——防冻结单被趋势行情打掉。
                for sd in ("BUY", "SELL"):
                    if (sd == "BUY" and pos_l) or (sd == "SELL" and pos_s):
                        continue                      # 该侧有仓: 不碰
                    c2 = res_cond.get((sym, sd))
                    if not c2:
                        continue
                    tr2 = c2["trigger"]
                    dist = (cur - tr2) / cur if sd == "BUY" else (tr2 - cur) / cur
                    if dist >= RESIDENT_MIN_GAP:
                        continue
                    new_tr = cur * (1 - RESIDENT_DEPTH) if sd == "BUY" else cur * (1 + RESIDENT_DEPTH)
                    old_aid = c2.get("algoId")
                    # 【并发替换】同上: 挂新+撤旧同时发, 旧单存活窗口压到撤旧RTT
                    r_place2, cancel_ok2 = await asyncio.gather(
                        _place_conditional(sym, sd, new_tr, cur, register=False),
                        _cancel_algo_by_id(sym, sd, old_aid, quiet=False))
                    ok2, naid = r_place2
                    if ok2:
                        res_cond[(sym, sd)] = {"algoId": naid, "trigger": new_tr, "base": cur}
                        res_base[sym] = cur   # 【P0修复】冻结上移时也同步锚价, 保证成交后O=最新base
                        _resident_save()
                        stats["res_freeze_shift"] = stats.get("res_freeze_shift", 0) + 1
                        log.warning(f"[常驻冻结上移] {sym} {sd} 触发@{tr2:.8g} 距现价仅{dist:.1%}"
                                    f"(<{RESIDENT_MIN_GAP:.0%}) → 重挂@{new_tr:.8g} (异常兜底,防趋势单)")
                        if not cancel_ok2:
                            res_orphan[(sym, sd, old_aid)] = True
                    elif cancel_ok2:
                        log.warning(f"[常驻·并发替换] {sym} {sd} 冻结上移挂新失败但旧单已撤 → 下一拍补挂")
                    # 挂新失败且撤旧失败: 旧单保留原样, 下轮重试
                continue    # 冻结期: 不漂移重挂、不补挂成交侧(CEO:不撤另一侧)
            for side in ("BUY", "SELL"):
                pos = pos_l if side == "BUY" else pos_s
                cond = res_cond.get((sym, side))
                if pos:
                    if cond:
                        await _cancel_conditional(sym, side, quiet=True)   # 该侧有仓: 入场单多余, 撤
                    continue
                if cond:
                    # 漂移判定: 现价距该侧锚价≥2% → 先挂新(不登记) → 成功后原子替换 → 再按旧algoId撤旧
                    _mv = abs(cur - cond["base"]) / cond["base"]
                    if _mv >= RESIDENT_REPEG:
                        # 【跳变闸门·仅危险方向】单周期(RES_TICK秒)内朝**该侧触发价方向**移动≥JUMP_GATE
                        # → 判为针尖/乌龙, 不跟随: 保留旧触发价让它被打到(这正是我们要接的货)。
                        #   · 阴跌: 每周期只走2~8% → 逐次跟随并重置base → _mv永远累积不到闸门 → 过滤 ✅
                        #   · 乌龙: 单周期内直接跳>8% → 闸门拦住不跟随 → 旧单成交 → 捕获 ✅
                        # 方向判断必要性: 若不分方向, 单向大涨(BUY侧远离触发价)也会被拦 → base永不更新
                        # → 该币埋伏单永久钉死在旧位置失效。故只拦"价格朝触发价扑过来"的那一侧。
                        _danger = (cur < cond["base"]) if side == "BUY" else (cur > cond["base"])
                        tr = cur * (1 - RESIDENT_DEPTH) if side == "BUY" else cur * (1 + RESIDENT_DEPTH)
                        old_aid = cond.get("algoId")
                        # 【并发替换 20260909 CEO定】挂新+撤旧同时发出:
                        # 旧单存活窗口从顺序的 ~1-1.5s(挂新RTT+撤旧RTT) 压到 ~0.3s(仅撤旧RTT)。
                        # 代价: 若挂新失败而撤旧已成功 → 该侧 ≤1s 空窗, 下一拍补挂自愈
                        # (空窗=漏接一针, 比"旧单被阴跌打穿成交"便宜)。
                        # 撤旧返回 -2011(已触发)=True: 仓位由 on_fill_resident 接管,
                        # 新单下一拍被"该侧有仓→撤"逻辑收走, 不产生孤儿。
                        r_place, cancel_ok = await asyncio.gather(
                            _place_conditional(sym, side, tr, cur, register=False),
                            _cancel_algo_by_id(sym, side, old_aid, quiet=False))
                        ok, new_aid = r_place
                        if ok:
                            res_cond[(sym, side)] = {"algoId": new_aid, "trigger": tr, "base": cur}
                            res_base[sym] = cur   # 【P0修复】重挂(漂移≥2%)时也同步锚价, 保证成交后O=最新base(匹配回测"入场锚价")
                            _resident_save()
                            stats["res_replace"] = stats.get("res_replace", 0) + 1
                            if not cancel_ok:
                                res_orphan[(sym, side, old_aid)] = True   # 撤旧失败→登记, 下轮重试(绝不留孤儿单)
                        elif cancel_ok:
                            # 挂新失败但旧单已撤 → 短暂空窗, 下一拍走缺单补挂路径
                            log.warning(f"[常驻·并发替换] {sym} {side} 挂新失败但旧单已撤 → 短暂空窗, 下一拍补挂")
                        # 挂新失败且撤旧失败: 旧单原样保留(不留洞), 下轮重试
                    continue
                # 缺单(启动/成交消耗后): 补挂。-2021守卫: 现价已在触发价内侧→不挂(等下轮)
                tr = cur * (1 - RESIDENT_DEPTH) if side == "BUY" else cur * (1 + RESIDENT_DEPTH)
                if side == "BUY" and cur <= tr * 1.002:
                    continue
                if side == "SELL" and cur >= tr * 0.998:
                    continue
                if sym not in lev_set:
                    try:
                        await ensure_leverage(sym)
                    except Exception:
                        pass
                await _place_conditional(sym, side, tr, cur)
        if time.time() - rec_t >= 300:
            await _reconcile_res_cond()   # 每5分钟与交易所对账一次(防死单卡补挂)
            rec_t = time.time()
        if time.time() - hb_t >= 300:
            log.info(f"[常驻心跳] 条件单{len(res_cond)}张 | 重挂{stats.get('res_replace', 0)}次"
                     f"(撤旧{stats.get('res_replace', 0) - len(res_orphan)}/{stats.get('res_replace', 0)}成功)"
                     f" | 待撤孤儿{len(res_orphan)} | 冻结上移{stats.get('res_freeze_shift', 0)}"
                     f" | 持仓{len(positions_open)} | "
                     f"R腿成交{stats.get('cmp_R_fills', 0)}次 | 正常")
            hb_t = time.time()

# ── 分钟主循环 ───────────────────────────────────────────────
async def minute_cycle(symbols, notional):
    """【精简版心跳】CMP模式下只保留：
    1. 每秒对齐边界sleep
    2. R循环心跳日志（有效币数 + 累计成交）
    3. dip_low/peak_high按自然分钟重置（纯内存，供逼近日志用）
    删除：anchors更新/stale补刷/IP守卫/熔断/重挂/冷却守护（全部在CMP下无效或已被替代）
    目的：彻底消除R循环对事件循环的阻塞，使_fire_cmp_m触发→下单不再被R循环延迟。"""
    round_no = 0
    _last_min_no = None
    P = max(1, int(ANCHOR_SEC))
    while not shutdown.is_set():
        # 每秒对齐边界
        now_srv = time.time() + OFF / 1000
        P_local = max(1, int(ANCHOR_SEC))
        boundary = (int(now_srv // P_local) + 1) * P_local - OFF / 1000
        await asyncio.sleep(max(0.05, boundary - time.time()))

        round_no += 1
        stats["rounds"] = round_no

        # 【自然分钟重置】dip_low/peak_high供逼近日志用，仍按分钟滚动
        _min_no = int((time.time() + OFF / 1000) // 60)
        if (_last_min_no is None) or (_min_no != _last_min_no):
            _last_min_no = _min_no
            for sym in RUN_SYMBOLS:
                dip_low[sym] = None
                peak_high[sym] = None

        if MAX_ROUNDS and round_no > MAX_ROUNDS:
            log.info(f"[停止] 已达 max-rounds={MAX_ROUNDS}")
            shutdown.set(); return

        # 心跳日志
        _d = DEPTH
        n_valid = sum(1 for s in RUN_SYMBOLS if mids.get(s))
        log.info(f"[R{round_no}] 有效币 {n_valid}/{len(RUN_SYMBOLS)} | 重挂0 | "
                 f"实时{n_valid} REST0 回退0 | 对比CMP监控(响应式M+L双单) ±{_d:.0%}...")
        log.info(f"[R{round_no}] 挂单 {len(orders)} 笔 | 累计成交{stats['fills']}次 "
                 f"pnl={stats['pnl']:+.4f}U")

# ── 响应式入场（A/B 组）：不提前挂单，WS 跌破阈值才下单 ──────────
async def _supervised(fn, name, *args):
    """监督者包装：关键任务崩溃时记录完整 traceback 并 3 秒后自动重启。
    【修复】此前 reactive_entry_loop 等任务无 try/except + create_task 无回调，
    任何异常都会静默杀死任务(无日志)，导致 2026-09-06 13:33 后盯盘循环死亡、
    市场 35 次 ≥2% 穿刺机会全部错过而日志毫无痕迹。"""
    n = 0
    while not shutdown.is_set():
        try:
            await fn(*args)
            return   # 正常返回(如 shutdown 触发)则不再重启
        except asyncio.CancelledError:
            raise    # 主动取消(final_cleanup)不算崩溃
        except Exception:
            n += 1
            log.error(f"[{name}] 第{n}次崩溃! 3秒后自动重启。完整堆栈:\n{traceback.format_exc()}")
            await asyncio.sleep(3)


async def clock_sync_loop():
    """【修复·时钟漂移】启动即校准一次OFF，之后每10分钟重校准(代理路径/时钟漂移后渐偏,
    DASH复盘实测本地慢~1.5s且随Clash节点漂移; 2026-09-09实盘时钟+2s致-1021拒单数分钟)。
    只动全局OFF(已线程安全地被 signed_request/延迟监控引用)，失败保留旧值。"""
    while not shutdown.is_set():
        _clock_resync()
        await asyncio.sleep(600)


def _clock_resync():
    """同步版时钟校准: OFF = 服务器时间 - 本地时间(折半消除RTT)。clock_sync_loop 与
    signed_request 的 -1021 自愈共用。成功返回新OFF, 失败返回None(保留旧值)。"""
    global OFF
    try:
        t_send = int(time.time() * 1000)
        r = client_sync().get(f"https://{_API_IP}/api/v3/time",
                         headers={"Host": _API_HOST}, timeout=10)
        t_recv = int(time.time() * 1000)
        srv = int(r.json()["serverTime"])
        rtt = t_recv - t_send
        new_off = srv - (t_send + rtt // 2)     # 折半消除网络单程延迟
        if abs(new_off - OFF) > 200:
            log.info(f"[时钟重校准] offset {OFF}ms → {new_off}ms (RTT {rtt}ms)")
        OFF = new_off
        return new_off
    except Exception as e:
        log.warning(f"[时钟重校准失败] {type(e).__name__} → 保留旧 offset={OFF}ms")
        return None


async def _poll_m_fill(oid, sym, qty_hint, tag=""):
    """【修复·60s盲区】M市价单同步回报未确认成交时，REST轮询订单状态(0.1s×15s)。
    REST轮询为主（UserData WS子账户0推送）。币安市价单实际300ms内成交，
    缺的只是回报 → 轮询一击即中。首次查询不sleep，后续每0.1s。
    查到 FILLED → on_fill 挂TP/SL；15s仍未成交(极罕见) → 交给用户流/兜底轮询。"""
    for i in range(150):
        if shutdown.is_set():
            return
        if i > 0:                    # 首次不sleep，后续0.1s
            await asyncio.sleep(0.1)
        if oid not in orders:        # 已被其他路径成交处理
            return
        c, r = await asyncio.to_thread(
            sync_signed_request, "GET", "/fapi/v1/order",
            {"symbol": sym, "orderId": oid})
        if c == 200 and (r.get("status") == "FILLED") and float(r.get("avgPrice") or 0) > 0:
            log.info(f"[M成交轮询✓{tag}] {sym} 订单{oid} REST确认 @{r['avgPrice']} "
                     f"(第{i+1}次轮询, {i*0.1:.1f}s内) → 立即挂TP/SL")
            await on_fill(oid, float(r["avgPrice"]), float(r.get("executedQty") or qty_hint), sym)
            return
        if c == 200 and r.get("status") in ("CANCELED", "EXPIRED", "REJECTED"):
            log.warning(f"[M成交轮询✗{tag}] {sym} 订单{oid} 状态异常: {r.get('status')}")
            return
    log.warning(f"[M成交轮询超时{tag}] {sym} 订单{oid} 15s未查到成交 → 交给用户流/兜底")


def sync_fire_cmp_m(sym, side, p, px):
    """【独立线程版】用sync_signed_request执行，完全脱离asyncio事件循环。
    POST后同步轮询GET（最多3次×50ms），直到FILLED拿到真实avgPrice，
    省去兜底8秒延迟，成交价准确。触发→成交目标：~30~150ms。"""
    tid, anc = p["tid"], p["anc"]
    tag = "" if side == "LONG" else "空"
    bs  = "BUY" if side == "LONG" else "SELL"
    qty = calc_qty(sym, px, NOTIONAL)
    c, r = sync_signed_request(
        "POST", "/fapi/v1/order",
        {"symbol": sym, "side": bs, "positionSide": side,
         "type": "MARKET", "quantity": f"{qty:.8f}".rstrip("0").rstrip(".")})
    if c == 200:
        oid = int(r["orderId"])
        orders[oid] = {"symbol": sym, "side": bs, "price": px,
                       "qty": qty, "O": anc, "round": -1,
                       "cmp": "M", "trigger_id": tid, "sub": f"{tid}:M",
                       "depth": p.get("depth")}
        log.info(f"[CMP-M✓{tag}] {sym} tid={tid} 线程直发(0阻塞) → "
                 f"市价{'买' if side == 'LONG' else '卖空'} x{qty} @≈{px:.8g}")
        # 不轮询FILLED状态，直接用触发价作为成交价
        ap_m = px  # 用触发价作为成交价
        eq = qty   # 用计算的数量
        log.info(f"[CMP-M成交✓{tag}] {sym} 推测成交 @{ap_m:.8g} x{eq} → 立即挂TP/SL")
        # 更新orders里的真实成交数量
        orders[oid]["qty"] = eq
        ev = cmp_events.get(tid)
        if ev is not None:
            ev["M_oid"] = oid
        # on_fill需要在事件循环里执行，用启动时保存的主loop地址（子线程get_event_loop()不可靠）
        import asyncio as _aio
        try:
            loop = _MAIN_LOOP
            if loop and loop.is_running():
                _aio.run_coroutine_threadsafe(on_fill(oid, ap_m, eq, sym), loop)
            else:
                log.warning(f"[sync_fire] _MAIN_LOOP未就绪或已停止，on_fill无法提交")
        except Exception as _e:
            log.warning(f"[sync_fire] on_fill提交失败: {_e}")
    else:
        log.warning(f"[CMP-M✗{tag}] {sym} tid={tid} 市价下单失败: {str(r)[:70]}")

async def _fire_cmp_m(sym, side, p, px):
    """下 M 市价单（同步回报即挂TP/SL，不等WS）。p = 触发条目{"tid","anc","depth",...}。
    REBOUND_ON=True 时由反弹确认调用；False(现行)时触发即调用。"""
    tid, anc = p["tid"], p["anc"]
    tag = "" if side == "LONG" else "空"
    bs = "BUY" if side == "LONG" else "SELL"
    qty = calc_qty(sym, px, NOTIONAL)
    c, r = await signed_request(
        "POST", "/fapi/v1/order",
        {"symbol": sym, "side": bs, "positionSide": side,
         "type": "MARKET", "quantity": f"{qty:.8f}".rstrip("0").rstrip(".")})
    if c == 200:
        oid = int(r["orderId"])
        orders[oid] = {"symbol": sym, "side": bs, "price": px,
                       "qty": qty, "O": anc, "round": -1,
                       "cmp": "M", "trigger_id": tid, "sub": f"{tid}:M",
                       "depth": p.get("depth")}
        if REBOUND_ON:
            log.info(f"[CMP-M✓{tag}] {sym} tid={tid} 反弹确认(自极值{p['ext']:.8g} {REBOUND_PCT:.1%}) → "
                     f"市价{'买' if side == 'LONG' else '卖空'} x{qty} @≈{px:.8g}")
        else:
            log.info(f"[CMP-M✓{tag}] {sym} tid={tid} 立即市价开火(10%大针无确认) → "
                     f"市价{'买' if side == 'LONG' else '卖空'} x{qty} @≈{px:.8g}")
        # 不轮询FILLED状态，直接用触发价作为成交价
        ap_m = px  # 用触发价作为成交价
        log.info(f"[CMP-M成交✓{tag}] {sym} 推测成交 @{ap_m:.8g} → 立即挂TP/SL")
        spawn(on_fill(oid, ap_m, qty, sym))
        ev = cmp_events.get(tid)
        if ev is not None:
            ev["M_oid"] = oid
    else:
        log.warning(f"[CMP-M✗{tag}] {sym} tid={tid} 市价下单失败: {str(r)[:70]}")


async def reactive_entry_loop():
    """响应式入场：监控 WS 中间价，多空双向触发（与 8 币时代对称）。
    多单：跌穿锚价×(1-DEPTH) → 市价(M)+限价(L) 做多接下跌针；空单：涨破锚价×(1+DEPTH) → 市价(M)+限价(L) 做空接闪涨针。
    对比模式CMP：触发先挂 L 深位限价单；M 市价腿等"反弹确认"(自触发后极值≥REBOUND_PCT，REBOUND_WAIT_S 窗口)才下，
    超时放弃——把"接落刀"变"接确认弹起"(CEO规则)。两腿打同 trigger_id、不同子仓键，各自独立挂 TP/SL。
    非CMP：A组=市价；B组=限价@WS最低/最高。
    允许同币多空双开（(sym,pos_side) 独立冷却）；启动遗留仓(STARTUP_HELD)不接管；熔断期不入场。"""
    hb_t = time.time(); hb_n = 0   # 心跳：每5分钟证明盯盘循环活着(再也不会静默死亡)
    while not shutdown.is_set():
        await asyncio.sleep(0.05)
        hb_n += 1
        if time.time() - hb_t >= 300:
            log.info(f"[盯盘心跳] 存活{time.time()-hb_t:.0f}s | 已扫描{hb_n}轮 | "
                     + (f"熔断中(至{datetime.fromtimestamp(halt_until).strftime('%H:%M')})" if time.time() < halt_until else "正常"))
            hb_t = time.time(); hb_n = 0
        if not (CMP_MODE or GROUP_A or GROUP_B):
            continue
        if time.time() < halt_until:
            continue
        open_pos = {(k[0], k[1]) for k in positions_open.keys()}   # (sym,pos_side)，允许同币多空双开
        now = time.time()
        # ── 反弹确认处理：已触发的 M 腿等"从极值反弹≥REBOUND_PCT"才真正下单(CEO:不接落刀) ──
        # 多单：跟踪触发后最低价 ext，cur ≥ ext×(1+REBOUND_PCT) → 开火；空单对称(跟踪最高，回落确认)。
        # REBOUND_WAIT_S 内无反弹 → 放弃(不赌)，L 深位限价单由 l_leg_watch 自行 5s 撤收尾。
        for key in list(pend_m.keys()):
            p = pend_m[key]
            sym, side = key
            mm = mids.get(sym)
            if not mm:
                continue
            cur_p = (mm["bid"] + mm["ask"]) / 2
            if now - p["t0"] > REBOUND_WAIT_S:
                del pend_m[key]
                log.info(f"[CMP-M放弃] {sym} tid={p['tid']} {side} {REBOUND_WAIT_S:.0f}s内无反弹确认"
                         f"(极值{p['ext']:.8g} 现{cur_p:.8g}) → 不赌,仅L深位单留守(自行5s撤)")
                continue
            if side == "LONG":
                if cur_p < p["ext"]:
                    p["ext"] = cur_p      # 刀还在落，更新极值
                    continue
                fired = cur_p >= p["ext"] * (1 + REBOUND_PCT)
            else:
                if cur_p > p["ext"]:
                    p["ext"] = cur_p
                    continue
                fired = cur_p <= p["ext"] * (1 - REBOUND_PCT)
            if not fired:
                continue
            del pend_m[key]
            await _fire_cmp_m(sym, side, p, cur_p)   # 反弹确认成立 → 立即下 M
        active = RUN_SYMBOLS if CMP_MODE else (GROUP_A + GROUP_B)
        for sym in active:
            if sym in STARTUP_HELD:
                continue   # 启动即存在的遗留仓: 本策略不接管、不重复触发(尊重"先不用管")
            anc = anchors.get(sym)
            if not anc or anc <= 0:
                continue
            m = mids.get(sym)
            if not m:
                continue
            cur_bt = (m["bid"] + m["ask"]) / 2   # bookTicker中间价（A组）
            tp = trade_prices.get(sym)
            cur_at = tp["price"] if tp else None  # aggTrade成交价（B组）
            # 响应式组/B组/CMP：每轮记录近期 WS 最低(dip_low, 多单接底) / 最高(peak_high, 空单接顶)，
            # 每分钟被 minute_cycle 重置为 None，捕捉本分钟插针。
            if CMP_MODE or sym in GROUP_B:
                dl = dip_low.get(sym)
                if dl is None or cur_bt < dl:
                    dip_low[sym] = cur_bt
                ph = peak_high.get(sym)
                if ph is None or cur_bt > ph:
                    peak_high[sym] = cur_bt
            depth = DEPTH  # 固定阈值6%
            # 【逼近/触发阈值用100ms窗口锚价】与WS触发判断保持一致，避免实时锚价导致差值≈0
            _now_mc_ms = int(time.time() * 1000)
            _hist_mc = price_hist.get(sym)
            anc_trig = None
            if _hist_mc:
                for _hts, _hpx in _hist_mc:
                    if _now_mc_ms - _hts >= 80:
                        anc_trig = _hpx
            if not anc_trig:
                anc_trig = anc  # 历史不够则回退实时锚价
            thr    = anc_trig * (1 - depth)      # 多单触发：跌穿100ms前锚价 depth%
            thr_up = anc_trig * (1 + depth)      # 空单触发：涨破100ms前锚价 depth%
            # 【逼近日志】价格进入"阈值-2%带内但还没触发"时打一条，证明 bot 真在盯盘(非空转)。
            # 下跌带→[逼近](多单)；上涨带→[逼近↑](空单)。每币每5分钟最多1条。
            band_lo = max(depth * 0.5, 0.01)   # 带下限=阈值×0.5
            if band_lo > 0:
                drop_pct = (anc_trig - cur_bt) / anc_trig
                if band_lo <= drop_pct < depth:
                    nl = near_miss_ts.get(sym, 0)
                    if now - nl > 300:
                        near_miss_ts[sym] = now
                        log.info(f"[逼近] {sym} 距阈值 {drop_pct:.1%}(阈值{depth:.0%}) "
                                 f"| 锚(100ms){anc_trig:.8f} 现{cur_bt:.8f} — 未触发,继续盯")
                rise_pct = (cur_bt - anc_trig) / anc_trig
                if band_lo <= rise_pct < depth:
                    nl = near_miss_ts.get(sym + "↑", 0)
                    if now - nl > 300:
                        near_miss_ts[sym + "↑"] = now
                        log.info(f"[逼近↑] {sym} 距阈值 {rise_pct:.1%}(阈值{depth:.0%}) "
                                 f"| 锚(100ms){anc_trig:.8f} 现{cur_bt:.8f} — 未触发,继续盯")
            # 【aggTrade补盲】bookTicker逼近但没触发时，主动拉最近1笔aggTrade成交价
            # 解决WS不推送低流动性币aggTrade的问题（REUSD1/币安人生USD1等）
            if band_lo <= drop_pct < depth:
                try:
                    _st, _at = await signed_request("GET", "/fapi/v1/aggTrades", params={"symbol": sym, "limit": 1})
                    if _st == 200 and isinstance(_at, list) and len(_at) > 0:
                        _at_px = float(_at[0]["p"])
                        _at_age = int(time.time() * 1000) - int(_at[0].get("T", 0))
                        if _at_px < thr and _at_age < 2000:  # 2秒内的成交才算
                            if not pos_long and _entry_lock.get((sym, "LONG"), False) is False:
                                _entry_lock[(sym, "LONG")] = True
                                if CMP_MODE:
                                    tid = f"T{cmp_seq_incr():04d}"
                                    p = {"tid": tid, "anc": anc, "depth": depth, "t0": now, "ext": _at_px}
                                    log.info(f"[CMP触发✓][AT补盲] {sym} tid={tid} src=AT 锚{anc:.8f} -{depth:.0%} "
                                             f"| aggTrade={_at_px:.8f} age={_at_age}ms → M立即市价开火")
                                    cmp_events[tid] = {"sym": sym, "pos_side": "LONG", "t": now, "anchor": anc,
                                                       "thr": thr, "mkt_px": _at_px, "src": "AT",
                                                       "M_oid": None, "L_oid": None}
                                    await _fire_cmp_m(sym, "LONG", p, _at_px)
                except Exception:
                    pass  # 补盲失败不影响主流程
            pos_long = (sym, "LONG") in open_pos
            pos_short = (sym, "SHORT") in open_pos
            # ── A组：bookTicker检测 → 多单做空（接下跌针）──
            if cur_bt < thr and not pos_long and entry_cooldown.get((sym, "LONG", "BT"), 0) <= now:
                if _entry_lock.get((sym, "LONG"), False):
                    continue
                _entry_lock[(sym, "LONG")] = True
                await ensure_leverage(sym)
                for ksub in [k for k in list(tp_limits.keys()) if k[0] == sym and k[1] == "LONG"]:
                    await cancel_tp_limit(sym, "LONG", quiet=True, sub=ksub[2])
                for ksub in [k for k in list(algo_stops.keys()) if k[0] == sym and k[1] == "LONG"]:
                    await cancel_stop_algo(sym, "LONG", quiet=True, sub=ksub[2])
                if CMP_MODE:
                    tid = f"T{cmp_seq_incr():04d}"
                    p = {"tid": tid, "anc": anc, "depth": depth, "t0": now, "ext": cur_bt}
                    log.info(f"[CMP触发✓] {sym} tid={tid} src=BT 锚{anc:.8f} -{depth:.0%} | "
                             f"M立即市价开火 | 成交后走 on_fill 流水线(路线A/路线B)")
                    cmp_events[tid] = {"sym": sym, "pos_side": "LONG", "t": now, "anchor": anc,
                                       "thr": thr, "mkt_px": cur_bt, "src": "BT",
                                       "M_oid": None, "L_oid": None}
                    # entry_cooldown[(sym, "LONG", "BT")] = now + 60  # 暂时关闭cooldown测试
                    await _fire_cmp_m(sym, "LONG", p, cur_bt)
            # ── B组：aggTrade检测 → 多单（独立冷却，允许同币同方向重复下单做AB对照）──
            if cur_at is not None and cur_at < thr and not pos_long and entry_cooldown.get((sym, "LONG", "AT"), 0) <= now:
                if _entry_lock.get((sym, "LONG"), False):
                    continue
                _entry_lock[(sym, "LONG")] = True
                await ensure_leverage(sym)
                for ksub in [k for k in list(tp_limits.keys()) if k[0] == sym and k[1] == "LONG"]:
                    await cancel_tp_limit(sym, "LONG", quiet=True, sub=ksub[2])
                for ksub in [k for k in list(algo_stops.keys()) if k[0] == sym and k[1] == "LONG"]:
                    await cancel_stop_algo(sym, "LONG", quiet=True, sub=ksub[2])
                if CMP_MODE:
                    tid = f"T{cmp_seq_incr():04d}"
                    p = {"tid": tid, "anc": anc, "depth": depth, "t0": now, "ext": cur_at}
                    log.info(f"[CMP触发✓] {sym} tid={tid} src=AT 锚{anc:.8f} -{depth:.0%} | "
                             f"M立即市价开火 | 成交后走 on_fill 流水线(路线A/路线B)")
                    cmp_events[tid] = {"sym": sym, "pos_side": "LONG", "t": now, "anchor": anc,
                                       "thr": thr, "mkt_px": cur_at, "src": "AT",
                                       "M_oid": None, "L_oid": None}
                    # entry_cooldown[(sym, "LONG", "AT")] = now + 60  # 暂时关闭cooldown测试
                    await _fire_cmp_m(sym, "LONG", p, cur_at)
            # ── A组：bookTicker检测 → 空单（接闪涨针）──
            if cur_bt > thr_up and not pos_short and entry_cooldown.get((sym, "SHORT", "BT"), 0) <= now:
                if _entry_lock.get((sym, "SHORT"), False):
                    continue
                _entry_lock[(sym, "SHORT")] = True
                await ensure_leverage(sym)
                for ksub in [k for k in list(tp_limits.keys()) if k[0] == sym and k[1] == "SHORT"]:
                    await cancel_tp_limit(sym, "SHORT", quiet=True, sub=ksub[2])
                for ksub in [k for k in list(algo_stops.keys()) if k[0] == sym and k[1] == "SHORT"]:
                    await cancel_stop_algo(sym, "SHORT", quiet=True, sub=ksub[2])
                if CMP_MODE:
                    tid = f"T{cmp_seq_incr():04d}"
                    p = {"tid": tid, "anc": anc, "depth": depth, "t0": now, "ext": cur_bt}
                    log.info(f"[CMP触发✓空] {sym} tid={tid} src=BT 锚{anc:.8f} +{depth:.0%} | "
                             f"M立即市价开火 | 成交后走 on_fill 流水线(路线A/路线B)")
                    cmp_events[tid] = {"sym": sym, "pos_side": "SHORT", "t": now, "anchor": anc,
                                       "thr": thr_up, "mkt_px": cur_bt, "src": "BT",
                                       "M_oid": None, "L_oid": None}
                    # entry_cooldown[(sym, "SHORT", "BT")] = now + 60  # 暂时关闭cooldown测试
                    await _fire_cmp_m(sym, "SHORT", p, cur_bt)
            # ── B组：aggTrade检测 → 空单（独立冷却，AB对照）──
            if cur_at is not None and cur_at > thr_up and not pos_short and entry_cooldown.get((sym, "SHORT", "AT"), 0) <= now:
                if _entry_lock.get((sym, "SHORT"), False):
                    continue
                _entry_lock[(sym, "SHORT")] = True
                await ensure_leverage(sym)
                for ksub in [k for k in list(tp_limits.keys()) if k[0] == sym and k[1] == "SHORT"]:
                    await cancel_tp_limit(sym, "SHORT", quiet=True, sub=ksub[2])
                for ksub in [k for k in list(algo_stops.keys()) if k[0] == sym and k[1] == "SHORT"]:
                    await cancel_stop_algo(sym, "SHORT", quiet=True, sub=ksub[2])
                if CMP_MODE:
                    tid = f"T{cmp_seq_incr():04d}"
                    p = {"tid": tid, "anc": anc, "depth": depth, "t0": now, "ext": cur_at}
                    log.info(f"[CMP触发✓空] {sym} tid={tid} src=AT 锚{anc:.8f} +{depth:.0%} | "
                             f"M立即市价开火 | 成交后走 on_fill 流水线(路线A/路线B)")
                    cmp_events[tid] = {"sym": sym, "pos_side": "SHORT", "t": now, "anchor": anc,
                                       "thr": thr_up, "mkt_px": cur_at, "src": "AT",
                                       "M_oid": None, "L_oid": None}
                    # entry_cooldown[(sym, "SHORT", "AT")] = now + 60  # 暂时关闭cooldown测试
                    await _fire_cmp_m(sym, "SHORT", p, cur_at)

# ── 收尾 ─────────────────────────────────────────────────────
async def final_cleanup(symbols):
    log.info("[收尾] 撤销自己的挂单…")
    old = dict(orders); orders.clear()
    for oid, o in old.items():
        c, r = await signed_request( "DELETE", "/fapi/v1/order",
                                       {"symbol": o["symbol"], "orderId": oid})
        log.info(f"[收尾] 撤 {o['symbol']} {oid}: {'OK' if c == 200 else str(r)[:60]}")
    log.info("[收尾] 平自己开的仓…")
    for (sym, _ps, _sub), p in list(positions_open.items()):
        ok = await market_close(sym, p["pos_side"], p["qty"], p["entry"], p["O"], "收尾",
                                stop_on_shutdown=False, max_tries=10)
        if not ok:
            log.critical(f"[收尾✗] {sym} {p['pos_side']} {p['qty']} 未平掉 → 需手动处理！")
    # 撤掉所有残留的止损条件单（正常平仓路径已各自撤过，这里是兜底）
    for (sym, ps, _sub) in list(algo_stops.keys()):
        await cancel_stop_algo(sym, ps, sub=_sub, quiet=True)
    # 撤掉所有残留的止盈限价单（路线B留仓场景）
    for (sym, ps, _sub) in list(tp_limits.keys()):
        await cancel_tp_limit(sym, ps, sub=_sub, quiet=True)
    # 【常驻模式】撤掉全部在册条件单(bot 停机期间条件单若触发=裸仓无人管, 必须撤)
    if MODE == "resident":
        for (sym, side) in list(res_cond.keys()):
            await _cancel_conditional(sym, side, quiet=True)
        log.info(f"[收尾] 常驻条件单已全部撤除 (剩{len(res_cond)}张未撤→查残留)")
    cov_avg = stats["cov_sum"] / stats["cov_n"] if stats["cov_n"] else 0
    log.info(f"[结束] 共{stats['fills']}次成交 | pnl={stats['pnl']:+.4f}U | "
             f"A:{stats['routeA']} B:{stats['routeB']} 兜底:{stats['fallback']} | "
             f"覆盖均值{cov_avg:.1f}/{len(symbols)} 最低{stats['cov_min']} "
             f"锚回退总{stats['anchor_stale']}次 IP失守{stats['ip_bad_rounds']}轮 "
             f"熔断{stats.get('halt_count', 0)}次")
    # 【企业微信通知】停止播报（收尾平仓后同步发一次，阻塞也没关系了）
    if wecom_notify:
        wecom_notify._send_text(
            f"埋伏合约通知\n⏹ 合约埋伏已停止\n"
            f"成交：{stats['fills']} 次 | 总盈亏：{stats['pnl']:+.4f} USDT\n"
            f"A:{stats['routeA']} B:{stats['routeB']} 兜底:{stats['fallback']}\n"
            f"时间：{wecom_notify._now()}")
    release_lock()

# ── 单例锁：禁止双实例（双实例会重复挂单→孤儿单累积，正是 159 单乱象根因）──
LOCK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ambush_basket.lock")

def _pid_alive(pid):
    # 【修·20260907】Windows 下 os.kill(pid,0) 不是"探测"而是 TerminateProcess(会把目标进程杀掉)！
    # 改用 OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION) 真探测，失败即认为已死。
    try:
        import ctypes
        k = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))
        if k:
            ctypes.windll.kernel32.CloseHandle(k)
            return True
        return False
    except Exception:
        return False

def release_lock():
    try:
        if os.path.exists(LOCK_PATH) and open(LOCK_PATH).read().strip() == str(os.getpid()):
            os.remove(LOCK_PATH)
    except Exception:
        pass

# ── 主入口 ───────────────────────────────────────────────────
async def main():
    global DEPTH, TARGET, T_A, ROUTE_B_HOLD, MAX_ROUNDS, NOTIONAL, EXPECTED_IPS, REPRICE_THRESH
    global _MAIN_LOOP
    global LEVERAGE, FEE_MAKER, FEE_TAKER, FREEZE_PULL_MARGIN, STOP_LOSS
    global SPIKE_PCT, SPIKE_MIN_SYMBOLS, SPIKE_WINDOW, SPIKE_COOLDOWN
    global args, GROUP_A, GROUP_B, TP_PCT, ROUTE_B_MODE, CMP_MODE
    global DYN_THRESH, MIN_DEPTH, MAX_DEPTH, VOL_MULT, VOL_WINDOW
    global MODE, LOCK_PATH
    global REB_ANCHOR, REB_PCT, ROUTE_BRANCH, BRANCH_TR, BRANCH_CUT, BRANCH_HOLD
    global INSTANCE, RESIDENT_DEPTH
    global SL_MERGE
    global ANCHOR_SEC
    _MAIN_LOOP = asyncio.get_event_loop()
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols-file", default=None, help="币种清单json(默认 top58)")
    ap.add_argument("--symbols", default=None, help="逗号分隔覆盖(小样测试用)")
    ap.add_argument("--notional", type=float, default=10.0, help="每侧名义U(默认10)")
    ap.add_argument("--depth", type=float, default=0.10, help="埋伏深度(默认0.10=±10%%,CEO定20260907)；动态阈值下作为冷启动回退值")
    ap.add_argument("--dyn-thresh", action="store_true", help="开启动态阈值(10分钟EMA)：阈值=clamp(EMA振幅×4, 10%%, 不封顶)")
    ap.add_argument("--min-depth", type=float, default=0.10, help="动态阈值下限(默认10%%,CEO定20260907)")
    ap.add_argument("--max-depth", type=float, default=1.0, help="动态阈值上限(默认1.0=不封顶,CEO定)")
    ap.add_argument("--vol-mult", type=float, default=4.0, help="动态阈值倍数(默认4)")
    ap.add_argument("--vol-window", type=int, default=10, help="动态阈值窗口分钟(默认10, EMA加权)")
    ap.add_argument("--target", type=float, default=0.03, help="路线A回归目标(默认3%%)")
    ap.add_argument("--ta", type=float, default=1.0, help="路线A窗口秒(默认1)")
    ap.add_argument("--hold-b", type=float, default=5.0, help="路线B强制平仓秒(默认5)")
    ap.add_argument("--max-rounds", type=int, default=0, help="跑N轮停(0=无限)")
    ap.add_argument("--reprice", type=float, default=0.02, help="重挂阈值(默认0.02=2%%)：价格相对上次挂单价偏移>=此值才撤旧挂新，否则保留原单不撤(降撤挂比)")
    ap.add_argument("--anchor-sec", type=float, default=60, help="秒级锚价: 锚价刷新周期(秒)=接针窗口。默认60(分钟级,旧行为); 1=只接1秒内真乌龙,阴跌不接")
    ap.add_argument("--pull-margin", type=float, default=0.05,
                    help="冷却期爬单守护(默认0.05=5%%)：冷却/-4400期间旧单距真实价<此值强制撤，防缓涨缓跌爬进死单")
    ap.add_argument("--stop", type=float, default=0.10,
                    help="交易所端止损(默认0.10=10%%，相对成交价)：成交即挂STOP_MARKET条件单，触发判定在币安服务器(断网也生效)；0=关闭")
    ap.add_argument("--no-sl-merge", action="store_true",
                    help="关闭同向止损合并(回滚到旧行为：每腿各挂一张全平单，第二张会被币安-4130拒绝)")
    ap.add_argument("--no-lev", action="store_true", help="跳过设置杠杆")
    ap.add_argument("--lev", type=int, default=5,
                    help="目标杠杆(默认5, 子账户上限5x, 2026-09-22定)"
                         "；设 0 = 用交易所最高杠杆(不推荐)")
    ap.add_argument("--fee-maker", type=float, default=0.0002, help="maker 费率(默认0.02%%)")
    ap.add_argument("--fee-taker", type=float, default=0.0005, help="taker 费率(默认0.05%%)")
    ap.add_argument("--spike-pct", type=float, default=0.05, help="熔断：单币插针幅度(默认5%%)")
    ap.add_argument("--spike-syms", type=int, default=40, help="熔断：窗口内标的数阈值(默认40, 357币宇宙调高避免误熔断)")
    ap.add_argument("--spike-window", type=float, default=300.0, help="熔断：统计窗口秒(默认300)")
    ap.add_argument("--spike-cooldown", type=float, default=3600.0, help="熔断：冷却秒(默认3600)")
    ap.add_argument("--group-a", default=None, help="A组币(逗号分隔):不挂单,WS跌破阈值后市价买")
    ap.add_argument("--group-b", default=None, help="B组币(逗号分隔):不挂单,WS跌破阈值后限价买@WS最低")
    ap.add_argument("--tp", type=float, default=0.03, help="止盈幅度(相对成交价,默认3%%):路线A目标&TP限价")
    ap.add_argument("--route-b", default="wait", choices=["wait", "close"],
                    help="路线B: wait=挂TP+SL后留仓由交易所管理(默认); close=原5s市价硬平(回滚)")
    ap.add_argument("--cmp", action="store_true",
                    help="对比模式: 取消A/B分组, 每币每触发同时下 市价(M)+限价(L) 两单(各NOTIONAL U), "
                         "打同trigger_id各自独立TP/SL, 仅比入场执行")
    ap.add_argument("--smoke", action="store_true", help="公开数据自测30s(不用key)")
    ap.add_argument("--mode", default="react", choices=["react", "resident"],
                    help="react=响应式触发+市价(默认,136币); resident=常驻条件单模式(20惯犯, "
                         "TAKE_PROFIT_MARKET 双侧±10%%平台托管, 成交后SL/TP/风暴复用on_fill)")
    ap.add_argument("--ip", default=None, help="允许的出口IP,逗号分隔(默认取.env BN_IP)")
    ap.add_argument("--instance", default="", help="实例标签(空=默认): 隔离锁文件/state文件/日志前缀, 允许多resident进程并行")
    ap.add_argument("--resident-depth", type=float, default=0.10, help="常驻触发深度(默认10%%, 实验可设15%%)")
    ap.add_argument("--reb-anchor", action="store_true", help="回弹目标改相对锚价(实验,默认关=相对成交价)")
    ap.add_argument("--reb", type=float, default=0.06, help="回弹阈值(相对锚价,默认6%%=对齐回测最终档,配合--reb-anchor)")
    ap.add_argument("--route-branch", action="store_true", help="分岔式退出(实验,默认关=留仓等TP/SL)")
    ap.add_argument("--branch-tr", type=float, default=0.02, help="移动止盈回撤(默认2%%)")
    ap.add_argument("--branch-cut", type=float, default=0.02, help="快砍止损(默认2%%)")
    ap.add_argument("--branch-hold", type=float, default=900.0, help="最长持有秒(默认900=15分钟)")
    args = ap.parse_args()

    MODE = args.mode
    INSTANCE = args.instance
    # 锁文件按模式+实例分开: react/resident/实验多进程并行, 互不排斥(名单零重叠, 同币冲突不存在)
    lock_suf = f"_{INSTANCE}" if INSTANCE else ""
    LOCK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             f"ambush_basket_{MODE}{lock_suf}.lock")

    # 单例锁：已有活实例则拒绝启动，避免双实例重复挂单
    try:
        if os.path.exists(LOCK_PATH):
            old = open(LOCK_PATH).read().strip()
            if old.isdigit() and _pid_alive(int(old)):
                log.critical(f"[单例锁✗] 已有实例在运行 (PID {old}) → 拒绝启动，避免双实例重复挂单")
                return
            log.warning(f"[单例锁⚠] 发现陈旧锁(PID {old} 已死) → 接管")
        with open(LOCK_PATH, "w") as f:
            f.write(str(os.getpid()))
    except Exception as e:
        log.warning(f"[单例锁] 写入失败(忽略): {e}")

    if args.ip:
        EXPECTED_IPS = {s.strip() for s in args.ip.split(",") if s.strip()}

    DEPTH, TARGET, T_A, ROUTE_B_HOLD = args.depth, args.target, args.ta, args.hold_b
    DYN_THRESH = args.dyn_thresh
    MIN_DEPTH, MAX_DEPTH = args.min_depth, args.max_depth
    VOL_MULT, VOL_WINDOW = args.vol_mult, args.vol_window
    TP_PCT, ROUTE_B_MODE = args.tp, args.route_b
    REB_ANCHOR, REB_PCT = args.reb_anchor, args.reb
    ROUTE_BRANCH = args.route_branch
    BRANCH_TR, BRANCH_CUT, BRANCH_HOLD = args.branch_tr, args.branch_cut, args.branch_hold
    RESIDENT_DEPTH = args.resident_depth
    CMP_MODE = args.cmp
    MAX_ROUNDS, NOTIONAL = args.max_rounds, args.notional
    REPRICE_THRESH = args.reprice
    ANCHOR_SEC = max(1, int(args.anchor_sec))
    FREEZE_PULL_MARGIN = args.pull_margin
    STOP_LOSS = args.stop
    SL_MERGE = not args.no_sl_merge
    LEVERAGE = args.lev
    FEE_MAKER, FEE_TAKER = args.fee_maker, args.fee_taker
    SPIKE_PCT = args.spike_pct
    SPIKE_MIN_SYMBOLS = args.spike_syms
    SPIKE_WINDOW = args.spike_window
    SPIKE_COOLDOWN = args.spike_cooldown

    base = os.path.dirname(os.path.abspath(__file__))
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        sf = args.symbols_file or os.path.join(base, "contract_spike_scan", "data", "top58_offenders.json")
        symbols = [s.upper() for s in json.load(open(sf, encoding="utf-8"))]

    # A/B 组划分（响应式入场）：显式指定优先，否则默认前一半A、后一半B
    # 对比模式 CMP：取消分组，每币都走 M+L 双单逻辑
    if CMP_MODE:
        GROUP_A, GROUP_B = [], []
    elif args.group_a or args.group_b:
        GROUP_A = [s.strip().upper() for s in (args.group_a or "").split(",") if s.strip()]
        GROUP_B = [s.strip().upper() for s in (args.group_b or "").split(",") if s.strip()]
    else:
        half = (len(symbols) + 1) // 2
        GROUP_A = symbols[:half]
        GROUP_B = symbols[half:]
    # 去重 & 仅保留在 symbols 内的（避免误指定不存在的币）
    GROUP_A = [s for s in GROUP_A if s in symbols]
    GROUP_B = [s for s in GROUP_B if s in symbols and s not in GROUP_A]

    log.info("=" * 64)
    bmode = (f"挂{TP_PCT:.0%}TP/{STOP_LOSS:.0%}SL留仓" if ROUTE_B_MODE == "wait"
             else f"{ROUTE_B_HOLD}s强平")
    if CMP_MODE:
        log.info(f"乌龙指埋伏篮子 | {len(symbols)}币 ±{DEPTH:.0%} "
                 f"**对比CMP**:每触发 市价(M)+限价(L) 各{NOTIONAL}U(总{NOTIONAL*2}U), 打同trigger_id")
    elif MODE == "resident":
        log.info(f"乌龙指埋伏篮子·常驻模式 | {len(symbols)}惯犯×2侧 TAKE_PROFIT_MARKET 条件单 "
                 f"BUY@锚-{RESIDENT_DEPTH:.0%}/SELL@锚+{RESIDENT_DEPTH:.0%} | 平台托管触发(代理/WS/检测延迟免疫) | "
                 f"漂移{RESIDENT_REPEG:.0%}重挂 | 单边成交不撤另一侧(冻结) | "
                 f"成交后 SL{B_LOSS:.0%}/路线A(锚价{REB_PCT:.0%}/{T_A:.0f}s)/路线B自适应"
                 + (f" | 【实验】回弹锚价{REB_PCT:.0%}/{T_A:.0f}s + 分岔(tr{BRANCH_TR:.0%}/cut{BRANCH_CUT:.0%}/hold{BRANCH_HOLD:.0f}s)"
                    if REB_ANCHOR or ROUTE_BRANCH else ""))
    else:
        log.info(f"乌龙指埋伏篮子 | {len(symbols)}币 每侧{NOTIONAL}U ±{DEPTH:.0%} "
                 f"A:回归O∓{TARGET:.0%}({T_A}s) B:{bmode}")
    log.info(f"重挂阈值 REPRICE_THRESH={REPRICE_THRESH:.0%}（价格偏移>=此值才撤旧挂新，否则保留原单降撤挂比）")
    log.info(f"锚价刷新周期 ANCHOR_SEC={ANCHOR_SEC}s（=接针窗口：60=分钟级旧行为；1=秒级,只接1秒内真乌龙）")
    log.info(f"补丁1 WS断流→REST刷锚价 | 补丁2 冷却期旧单距真实价<{FREEZE_PULL_MARGIN:.0%}强制撤(防爬单)")
    log.info(f"止损补丁：成交即挂交易所端STOP_MARKET 触发价=成交价×(1±{STOP_LOSS:.0%})，判定在币安服务器(断网也生效)"
             if STOP_LOSS > 0 else "止损已关闭(--stop 0)：仅靠路线B硬平，失控拉盘风险自担")
    log.info(f"杠杆 {LEVERAGE if LEVERAGE else '最高(!)'}x | 手续费 maker{FEE_MAKER:.2%}/taker{FEE_TAKER:.2%}")
    log.info(f"熔断：{SPIKE_WINDOW:.0f}s内 ≥{SPIKE_MIN_SYMBOLS} 标的插针≥{SPIKE_PCT:.0%} "
             f"→ 撤单停手冷却 {SPIKE_COOLDOWN / 60:.0f} 分钟")
    if CMP_MODE:
        log.info(f"**对比模式CMP**: 每币每触发 M(市价,必成交)+L(限价@WS最低) 两单, 各自独立挂 {TP_PCT:.0%}TP/{STOP_LOSS:.0%}SL 留仓")
        if DYN_THRESH:
            _cap_txt = "不封顶" if MAX_DEPTH >= 1.0 else f"{MAX_DEPTH:.0%}"
            log.info(f"动态阈值ON: 阈值=clamp(EMA(α={EMA_ALPHA:g})×{VOL_MULT:g}, {MIN_DEPTH:.1%}~{_cap_txt}) "
                     f"窗口{VOL_WINDOW}分钟 | 冷启动回退{DEPTH:.0%} | "
                     + ("M腿反弹确认≥{:.1%}/{:.0f}s".format(REBOUND_PCT, REBOUND_WAIT_S) if REBOUND_ON
                        else f"M触发即市价(无确认) | 成交后走 on_fill 流水线(路线A锚价{REB_PCT:.0%}/{T_A:.0f}s + 路线B自适应)"))
        else:
            log.info(f"动态阈值OFF: 固定阈值DEPTH={DEPTH:.0%}")
        log.info(f"阈值DEPTH={DEPTH:.0%} | 退出逻辑两单一致(只比入场执行) | 同币两仓用子仓键 M/L 独立追踪")
    else:
        log.info(f"A组(响应式市价入场): {GROUP_A}")
        log.info(f"B组(响应式限价@WS最低): {GROUP_B}")
        log.info(f"路线B模式={ROUTE_B_MODE} | 止盈TP={TP_PCT:.0%} | 阈值DEPTH={DEPTH:.0%} "
                 f"| 非A/B组仍挂双侧埋伏")
    log.info("=" * 64)

    # 【修复】exchangeInfo 拉取失败(Clash抖动返回非200)时，原逻辑静默返回空表
    # → 58 币全被当"已下架"跳过 → IndexError 崩溃。改为重试直到拉到，拉不到明确退出。
    symbols_loaded = []
    for attempt in range(10):
        symbols_loaded = load_filters(list(symbols))
        if symbols_loaded:
            break
        log.warning(f"[规格] exchangeInfo 拉取失败(第{attempt + 1}次,网络/代理抖动) → 5s后重试")
        await reset_client()
        await asyncio.sleep(5)
    if not symbols_loaded:
        log.critical("[规格✗] 连续10次拉不到交易规格 → 退出(检查Clash代理后手动重启)")
        release_lock()
        return
    symbols = symbols_loaded
    RUN_SYMBOLS[:] = symbols
    log.info(f"[规格] 就绪 {len(symbols)}/{len(symbols)} | 样例: "
             f"{symbols[0]} tick={FILTERS[symbols[0]]['tick']} minNotional={FILTERS[symbols[0]]['minNotional']}")
    # 【企业微信通知】启动播报：币种数/阈值/模式/止盈止损，一眼确认参数没带错
    if wecom_notify:
        wecom_notify.notify_start(n_symbols=len(symbols), depth=DEPTH, notional=NOTIONAL,
                                  mode="cmp" if CMP_MODE else "响应式",
                                  lev=(LEVERAGE if LEVERAGE else "最高"),
                                  tp=TP_PCT, sl=STOP_LOSS)

    # 启动 prime 回退锚价（让低流动性币也能稳定接入埋伏，不被 10s WS 门槛踢出）
    await prime_anchors(symbols)

    # 优雅退出：Ctrl+C / SIGTERM → 触发 shutdown，main 的 await shutdown.wait() 会跑 final_cleanup
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: shutdown.set())
            except (NotImplementedError, RuntimeError, ValueError):
                pass
    except RuntimeError:
        pass

    # 出口 IP 自检（IP 守卫的启动闸门）
    ip0 = await asyncio.to_thread(exit_ip)
    log.info(f"[IP] 当前 Clash 出口 IP = {ip0}")
    if EXPECTED_IPS:
        if ip0 is None:
            # 探测站全挂：问币安，不猜
            ok, why = await asyncio.to_thread(binance_ip_ok)
            if ok:
                log.info(f"[IP守卫✓] 出口IP探测失败，但 {why} → 允许交易")
            else:
                log.warning(f"[IP守卫⚠] 出口IP探测失败且 {why} → 先启动，"
                            f"每轮守卫会拦住挂单，待出口回到白名单自动续挂")
        elif ip0 not in EXPECTED_IPS:
            log.warning(f"[IP守卫⚠] 启动出口 {ip0} 不在白名单 {sorted(EXPECTED_IPS)} → 先启动，"
                        f"每轮守卫会拦住挂单，待 IP 回到白名单自动续挂（建议把 {ip0} 加入币安白名单）")
            if wecom_notify is not None:
                wecom_notify.send_async(f"⚠️ ambush_{MODE} 启动即在坏IP! 出口={ip0} 不在白名单{sorted(EXPECTED_IPS)}\n"
                                        f"→ 挂单被锁, 陈旧限价单有被漂移价成交风险。请尽快把该IP加白名单或切Clash节点。")
        else:
            log.info(f"[IP守卫✓] 出口在白名单内，允许交易")

    # WS 行情：357 币拆成多连接，每连接≤WS_CHUNK 个 bookTicker（单连接过多会 keepalive 超时）
    WS_CHUNK = 50
    sym_chunks = [symbols[i:i + WS_CHUNK] for i in range(0, len(symbols), WS_CHUNK)]
    log.info(f"[WS] 行情连接数={len(sym_chunks)} (每连接≤{WS_CHUNK}币)")
    tasks = [asyncio.create_task(_supervised(ws_book_loop, f"WS行情#{i}({len(ch)}币)", ch))
             for i, ch in enumerate(sym_chunks)]
    # 对照组：aggTrade WS连接（与bookTicker同样拆分，错峰启动避免同时握手超时）
    for i, ch in enumerate(sym_chunks):
        tasks.append(asyncio.create_task(_supervised(ws_aggtrade_loop, f"WS逐笔#{i}({len(ch)}币)", ch, i)))
        await asyncio.sleep(1)
    if args.smoke:
        await asyncio.sleep(30)
        log.info(f"[SMOKE] 30s 收到 {len(mids)} 个币的盘口 | 样例: "
                 f"{symbols[0]} mid={mid(symbols[0])}")
        for t in tasks: t.cancel()
        release_lock()
        return

    # 时钟偏移（【修复】原无异常保护：代理抖动时这里抛异常会让整个 bot 启动即崩）
    global OFF
    try:
        r = client_sync().get(f"https://{_API_IP}/api/v3/time",
                         headers={"Host": _API_HOST}, timeout=10)
        OFF = int(r.json()["serverTime"]) - int(time.time() * 1000)
        log.info(f"时钟 offset={OFF}ms")
    except Exception as e:
        OFF = 0
        log.warning(f"[时钟] 校准失败({type(e).__name__}) → offset 置 0 继续启动")

    # 启动检测：一次拉取全部 U 本位持仓，找出非空仓的币 → 屏蔽触发（非本策略开的，不接管不清仓）
    # 357 币规模下单符号循环 357 次会被限频，改一次全量拉取（无 symbol 参数即返回全部仓位）。
    try:
        c, p = await signed_request( "GET", "/fapi/v2/positionRisk")
        if c == 200 and isinstance(p, list):
            _held_msgs = []
            for x in p:
                amt = float(x.get("positionAmt", 0) or 0)
                if amt != 0:
                    s = x["symbol"]
                    STARTUP_HELD.add(s)
                    pos_side = x.get('positionSide', '?')
                    entry_px = float(x.get('entryPrice', 0) or 0)
                    mark_px = float(x.get('markPrice', 0) or 0)
                    upnl = float(x.get('unRealizedProfit', 0) or 0)
                    log.warning(f"[告警] {s} 已有持仓 ({pos_side}, {amt}) "
                                f"（非本策略开的 → 屏蔽触发, 保留原仓由交易所管理）")
                    _held_msgs.append(
                        f"  {s} {pos_side} qty={amt}\n"
                        f"    入场价={entry_px:.8g} 标记价={mark_px:.8g}\n"
                        f"    浮盈亏={upnl:+.4f} USDT")
            if _held_msgs and wecom_notify:
                wecom_notify.send_async(
                    f"⚠️检测到遗留持仓（非本策略开）\n"
                    f"请判断是否手动平仓：\n\n" + "\n".join(_held_msgs)
                )
    except Exception as e:
        log.warning(f"[启动检测] 拉取全部持仓失败: {e}（不屏蔽, 后续触发自行处理）")

    # 杠杆改为"触发时惰性设置"（ensure_leverage）：357 币启动时批量设杠杆=714 次签名请求会卡数分钟且易限频。
    # 仅在实际触发交易的币上设一次最高杠杆（lev_set 缓存去重），启动不再阻塞。
    if args.no_lev:
        log.info("[杠杆] --no-lev：跳过杠杆设置（含触发时）")

    tasks.append(asyncio.create_task(_supervised(ws_user_loop, "WS用户流")))
    tasks.append(asyncio.create_task(_supervised(keep_listen_key, "listenKey保活")))
    tasks.append(asyncio.create_task(_supervised(fallback_poll, "REST兜底轮询")))
    cyc = asyncio.create_task(_supervised(minute_cycle, "分钟周期", symbols, NOTIONAL))

    async def _lev_presets():
        """【并发版】启动后后台并发预置杠杆(10 worker, ~30s铺完767币)。
        权重账：767币x2请求(查分层+设杠杆)=1534权重 < 2400/min 安全线。
        保留 lev_set 缓存和触发时 ensure_leverage 现场补设双保险。
        遇限频(-1003/429)自动降速(worker内重试等待)。"""
        await asyncio.sleep(10)   # 等启动收尾
        n0 = len(lev_set)
        todo = [s for s in symbols if s not in lev_set]
        if not todo:
            return
        sem = asyncio.Semaphore(10)   # 最多10个并发
        async def _worker(s):
            async with sem:
                for attempt in range(3):
                    try:
                        await ensure_leverage(s)
                        if s in lev_set:  # 成功则不再重试
                            return
                    except Exception:
                        pass
                    if shutdown.is_set():
                        return
                    await asyncio.sleep(1.0 * (attempt + 1))   # 限频/失败退避
        await asyncio.gather(*[_worker(s) for s in todo])
        log.info(f"[杠杆预置] 后台铺完: {len(lev_set)-n0} 币新增 (累计{len(lev_set)})")

    tasks.append(asyncio.create_task(_supervised(_lev_presets, "杠杆预置")))
    tasks.append(asyncio.create_task(_supervised(clock_sync_loop, "时钟重校准")))
    if MODE == "resident":
        tasks.append(asyncio.create_task(_supervised(resident_loop, "常驻条件单循环")))
    elif CMP_MODE or GROUP_A or GROUP_B:
        tasks.append(asyncio.create_task(_supervised(reactive_entry_loop, "盯盘循环")))

    await shutdown.wait()
    cyc.cancel()
    await final_cleanup(symbols)
    for t in tasks: t.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCtrl+C → 执行收尾（撤单+平仓）…")
        try:
            shutdown.set()
            asyncio.run(final_cleanup(RUN_SYMBOLS))
        except Exception as e:
            print(f"[收尾异常] {e} → 请手动检查币安账户挂单/持仓")
