# -*- coding: utf-8 -*-
"""
四逻辑统一回测: AKEUSDT / BULLAUSDT 7天逐笔(aggTrades) × 4进程逻辑 × 新旧锚价对比
【口径 = 线上最新代码 ambush_basket.py, 2026-09-09】
  - 新配置: 锚价/跟随 1s; resident系 无跳变闸门; react 保留 8% 闸门(未重启, 原参数)
  - 旧配置对照: 60s 锚价/跟随, 无闸门 (隔离"秒级精准度"这一个变量, 退出逻辑两版相同)
四套逻辑(均无风暴单, 20260911 CEO定删除):
  react   : CMP动态阈值(EMA分钟振幅x4, 地板10%) 市价M, TP3%/SL10%, 路线A 3s, 无分岔
  resident: 条件单±10%双侧, TP5%/SL10%, 路线A 3s(相对成交价), 无分岔, 冻结+8%守卫
  exp11   : 条件单±15%双侧, TP3%/SL10%, 路线A 5s(目标=锚x0.95/1.05 reb-anchor), 分岔900s(tr/cut 2%)
  exp50   : 同exp11(仅单笔金额不同)
悲观假设: 触发判定先用旧trigger再做跟随; 同秒TP与SL同时可达→按SL; 手续费 taker 0.05% / maker 0.02%。
"""
import os, csv, pickle, datetime, sys, re

TZ = datetime.timezone(datetime.timedelta(hours=8))
HERE = os.path.dirname(os.path.abspath(__file__))
TICKS = os.path.join(HERE, 'ticks')
FEE_TK, FEE_MK = 0.0005, 0.0002
COINS = ['AKEUSDT', 'BULLAUSDT']
DAYS = ['2026-08-08', '2026-08-09', '2026-08-10', '2026-08-11', '2026-08-12', '2026-08-13',
        '2026-08-14', '2026-08-15', '2026-08-16', '2026-08-17', '2026-08-18', '2026-08-19',
        '2026-08-20', '2026-08-21', '2026-08-22', '2026-08-23', '2026-08-24', '2026-08-25',
        '2026-08-26', '2026-08-27', '2026-08-28', '2026-08-29', '2026-08-30', '2026-08-31',
        '2026-09-01', '2026-09-02', '2026-09-03', '2026-09-04', '2026-09-05', '2026-09-06']
# 【20260912·优化】逐笔tick缓存改为"按天预解析一次": 原先每查1秒就重读整天tick文件,
# 导致 AKE/BULLA 单run耗时154s 且 _TICK_CACHE 无上限暴涨内存。
# 现: 首次查某(sym,day)即把整天文件解析成 {秒:[(ms,价)...]} 并缓存; _sec_ticks 仅做字典查找;
# 一个币跑完调用 clear_tick_cache(sym) 释放, 控制峰值内存。出场/触发逻辑未改动。
_DAY_TICK_CACHE = {}      # (sym, day_utc) -> {sec: [(ms_offset, price), ...]}  (已按ms排序)
# 【20260912·补强】30天样本下, 一个币可能横跨30天, 若整天缓存只在"该币跑完"才清,
# 单币即可累积 30天×数百MB ≈ 数GB, 内存会再次暴涨(7天样本时不明显)。
# 回测按时间序推进, 同时只需驻留最近几天, 故设容量上限做FIFO淘汰(按插入顺序)。
_MAX_DAY_CACHE = 3


def _evict_day_cache():
    """FIFO淘汰: 缓存达到上限时丢最早入驻的(币,天)。dict保持插入顺序, 首项即最早。"""
    while len(_DAY_TICK_CACHE) >= _MAX_DAY_CACHE:
        _DAY_TICK_CACHE.pop(next(iter(_DAY_TICK_CACHE)))


def hhmmss(s):
    return datetime.datetime.fromtimestamp(s, TZ).strftime('%m-%d %H:%M:%S')


BARS1S = os.path.join(HERE, 'bars1s')


def _load_bars1s(sym):
    """从 bars1s/ 读已聚合的秒级K线(7天), 补空秒后写pkl缓存。成功返回True。"""
    fps = [os.path.join(BARS1S, '%s_%s.csv' % (sym, d)) for d in DAYS]
    have = [p for p in fps if os.path.exists(p)]
    if not have:
        return False
    secs = {}
    for fp in have:
        with open(fp, newline='') as f:
            for r in csv.reader(f):
                if len(r) < 7:
                    continue
                try:
                    s = int(r[0])
                    o, h, l, c = float(r[1]), float(r[2]), float(r[3]), float(r[4])
                    hm, lm = int(r[5]), int(r[6])
                except ValueError:
                    continue
                secs[s] = [o, h, l, c, hm, lm]
    if not secs:
        return False
    bars, prev = [], None
    for s in range(min(secs), max(secs) + 1):
        e = secs.get(s)
        if e is None:
            if prev is None:
                continue
            bars.append((s, prev, prev, prev, prev, 0, 0))   # 空秒: 无成交, 禁止同秒平仓
        else:
            o, h, l, c, hm, lm = e
            bars.append((s, o, h, l, c, hm, lm))
            prev = c
    if not bars:
        return False
    with open(os.path.join(HERE, '_cache6_%s.pkl' % sym), 'wb') as f:
        pickle.dump(bars, f, protocol=4)
    return True


def load_bars(sym):
    """7天合并的秒级bars [(s,o,h,l,c,h_ms,l_ms)]; 带pkl缓存
    【20260912 扩样本】数据源优先级:
      1) bars1s/{sym}_{day}.csv  —— 流式下载器已聚合成秒(52个新币, 省磁盘)
      2) ticks/{sym}_{day}.csv   —— 原始逐笔(AKE/BULLA 专用, 现场聚合)
    两者产出完全相同的7元组结构, 下游逻辑无需区分。
    """
    cache = os.path.join(HERE, '_cache6_%s.pkl' % sym)
    if os.path.exists(cache):
        with open(cache, 'rb') as f:
            return pickle.load(f)
    if _load_bars1s(sym):
        with open(cache, 'rb') as f:
            return pickle.load(f)
    secs = {}
    for day in DAYS:
        fp = os.path.join(TICKS, '%s_%s.csv' % (sym, day))
        if not os.path.exists(fp):
            print('  !! 缺', fp); continue
        with open(fp, newline='') as f:
            rd = csv.reader(f)
            for r in rd:
                if len(r) < 7 or not r[1][0].isdigit():
                    continue
                try:
                    p = float(r[1]); ts = int(r[5])
                except ValueError:
                    continue
                s = ts // 1000
                ms = ts % 1000
                e = secs.get(s)
                if e is None:
                    secs[s] = [p, p, p, p, ms, ms]      # o,h,l,c,h_ms,l_ms
                else:
                    if p > e[1]:
                        e[1] = p; e[4] = ms              # 最高价首次出现时刻
                    if p < e[2]:
                        e[2] = p; e[5] = ms              # 最低价首次出现时刻
                    e[3] = p
    if not secs:
        print('  !! 无数据', sym); return []
    # 【补空秒 20260912·P0修复】原实现只聚合"有成交的秒" → bars的1根≠墙钟1秒。
    # 实测(7天=604800秒): AKE覆盖79.4%(平均间隔1.26s/最大31s); BULLA覆盖64.7%(平均1.54s/最大77s)
    # → 回测的 t_a=1 实际是1.26~1.54s, b_time=10 实际是12.6~15.4s, 比实盘宽松26%~54%(偏乐观)。
    # 无成交的秒: 沿用上一秒收盘价, 高=低=收, h_ms=l_ms=0(无时序信息→禁止同秒平仓)。
    bars, prev = [], None
    for s in range(min(secs), max(secs) + 1):
        e = secs.get(s)
        if e is None:
            if prev is None:
                continue
            bars.append((s, prev, prev, prev, prev, 0, 0))
        else:
            o, h, l, c, hm, lm = e
            bars.append((s, o, h, l, c, hm, lm))
            prev = c
    if not bars:
        print('  !! 无数据', sym); return []
    with open(cache, 'wb') as f:
        pickle.dump(bars, f, protocol=4)
    return bars


def _load_day_ticks(sym, day):
    """解析 ticks/{sym}_{day}.csv 一整天为 {秒:[(ms,价)...]} (按ms排序), 只解析一次并缓存。
    【20260912·优化】替代原"每查1秒重读整天文件", 把 O(整天行数) 的IO从"每触发秒一次"降到"每(币,天)一次"。"""
    dk = (sym, day)
    if dk in _DAY_TICK_CACHE:
        return _DAY_TICK_CACHE[dk]
    fp = os.path.join(TICKS, '%s_%s.csv' % (sym, day))
    if not os.path.exists(fp):
        _evict_day_cache()
        _DAY_TICK_CACHE[dk] = None
        return None
    secs = {}
    with open(fp, newline='') as f:
        for r in csv.reader(f):
            if len(r) < 7 or not r[0][0].isdigit():
                continue
            try:
                ts = int(r[5]); p = float(r[1])
            except ValueError:
                continue
            s = ts // 1000
            secs.setdefault(s, []).append((ts % 1000, p))
    for s in secs:
        secs[s].sort(key=lambda x: x[0])
    _DAY_TICK_CACHE[dk] = secs
    return secs


def clear_tick_cache(sym=None):
    """释放逐笔tick缓存(默认全部; 或仅某币)。一个币跑完即清, 控制峰值内存。"""
    global _DAY_TICK_CACHE
    if sym is None:
        _DAY_TICK_CACHE = {}
    else:
        for k in list(_DAY_TICK_CACHE):
            if k[0] == sym:
                del _DAY_TICK_CACHE[k]


def _sec_ticks(sym, s):
    """读取某秒(sym,s)的所有逐笔tick [(ms_offset, price)], 按时间顺序。
    【20260912·优化】改查按天预解析缓存(见 _load_day_ticks), 不再每查1秒重读整天文件。
    tick文件名按UTC日期命名(如 AKEUSDT_2026-09-02.csv 包含北京09-03 05:44的数据)。"""
    day = datetime.datetime.utcfromtimestamp(s).strftime('%Y-%m-%d')
    d = _load_day_ticks(sym, day)
    if d is None:
        return None
    return d.get(s)


# ── 穿刺事件扫描(真/假乌龙客观分类) ─────────────────────────────
def find_episodes(bars, drop_th=0.08):
    """滚动60s高点回撤>=8%的episode。返回
    [{hi_t, hi, lo_t, lo, drop, fall_s, max1s, reb, recover}]:
    fall_s=高点到谷底秒数; max1s=下跌途中单秒最大跌幅; reb=谷底后10min内最大反弹%;
    recover=反弹收回跌幅的比例(0~1)"""
    eps = []
    n = len(bars)
    i = 0
    hi_i = 0
    while i < n:
        s, o, h, l, c, _hm, _lm = bars[i]
        # 维护过去60s最高
        j = i
        hh, hi_i = -1.0, i
        while j >= 0 and bars[i][0] - bars[j][0] <= 60:
            if bars[j][2] > hh:
                hh, hi_i = bars[j][2], j
            j -= 1
        if hh > 0 and (hh - l) / hh >= drop_th and (not eps or bars[i][0] > eps[-1]['lo_t'] + 300 or l < eps[-1]['lo'] * 0.995):
            # 找谷底: 反弹3%或1800s
            lo, lo_i = l, i
            k = i
            while k < n and bars[k][0] - bars[i][0] <= 1800:
                if bars[k][3] < lo:
                    lo, lo_i = bars[k][3], k
                if lo > 0 and bars[k][2] >= lo * 1.03:
                    break
                k += 1
            max1s = 0.0
            for m in range(hi_i, lo_i + 1):
                if m + 1 <= lo_i and bars[m + 1][3] > 0:
                    d = (bars[m][2] - bars[m + 1][3]) / bars[m][2]
                    max1s = max(max1s, d)
            # 谷底后10min最大反弹
            rb, rb_i = lo, lo_i
            k2 = lo_i
            while k2 < n and bars[k2][0] - bars[lo_i][0] <= 600:
                if bars[k2][2] > rb:
                    rb = bars[k2][2]
                k2 += 1
            drop = (hh - lo) / hh
            eps.append({'hi_t': bars[hi_i][0], 'hi': hh, 'lo_t': bars[lo_i][0], 'lo': lo,
                        'drop': drop, 'fall_s': bars[lo_i][0] - bars[hi_i][0], 'max1s': max1s,
                        'reb': (rb - lo) / lo, 'recover': (rb - lo) / (hh - lo) if hh > lo else 0})
            i = max(lo_i + 1, i + 1)
            continue
        i += 1
    return eps


def ep_class(e):
    if e['fall_s'] <= 60 and e['recover'] >= 0.5:
        return '真乌龙(快跌+强反弹)'
    if e['fall_s'] > 300 or e['recover'] < 0.2:
        return '阴跌/假乌龙'
    return '中间型'


# ── 通用仓位生命周期 ─────────────────────────────────────────
def mk_pos(kind, side, entry, O, i0, tp_px, sl_px, ra_target, t_a, branch, hold_s, notional, sym=None, bp_base='O'):
    return {'kind': kind, 'side': side, 'entry': entry, 'O': O, 'i0': i0,
            'tp': tp_px, 'sl': sl_px, 'ra': ra_target, 'ta': t_a,
            'branch': branch, 'hold': hold_s, 'notional': notional,
            'extreme': entry, 'pbr': None, 'best': entry, 'open': True,
            'sym': sym, 'trig_ms': None, 'trig_px': entry, 'bp_base': bp_base}


def pos_fee_in(p):
    # 所有开仓腿均为市价成交(taker); 风暴限价腿已删除(20260911 CEO定)
    return FEE_TK


def close_pos(p, px, how, fee_out, i_exit=None):
    lng = p['side'] == 'LONG'
    mv = (px - p['entry']) / p['entry'] if lng else (p['entry'] - px) / p['entry']
    net = p['notional'] * (mv - pos_fee_in(p) - fee_out)
    p['open'] = False
    return {'kind': p['kind'], 'side': p['side'], 'entry': p['entry'], 'exit': px, 'how': how,
            'i0': p['i0'], 'i_exit': i_exit, 'mv': mv, 'net': net, 'notional': p['notional']}


def same_bar_exit(p, bar, exit_mode, b_profit, b_reversal, b_time, b_loss, recs):
    """同一根 1s bar 内（毫秒级）立即检查能否平仓。
    【20260912·P0修复·时序判据】原实现用整秒 h/l 判定, 假设"做多先跌后涨/做空先涨后跌",
    但真实逐笔 70% 是反的 → 把"开仓之前"出现的极值当成"开仓之后"的平仓依据(偷看未来),
    实测净利虚高 37.5%(+5.311U vs 真实 +3.864U)。
    【20260912·P0修复v2·触发后路径】进一步改为: 只检查"触发成交之后"的价格路径,
    触发后达到目标才允许同秒平; 无逐笔数据源时退回保守的"先高后低/先低后高"判据。
    【20260912·CEO纠正·路线A无止损】same_bar_exit 只在开仓那一根bar调用(age=0),
    必在路线A窗口(ta秒)内。新退出模式: 同秒只查路线A, 绝不查亏损上限——
    亏损上限是路线B出口, 仅在 age>ta 后由 step_positions 接管。旧退出模式:
    SL是全程硬止损, 同秒仍可查(逻辑不变)。
    """
    s, o, h, l, c, h_ms, l_ms = bar
    lng = p['side'] == 'LONG'
    ticks = None
    if p.get('sym') and p.get('trig_ms') is not None:
        ticks = _sec_ticks(p['sym'], s)
    if ticks:
        after = [(ms, px) for ms, px in ticks if ms >= p['trig_ms']]
        if not after:
            return False
        if exit_mode == 'new':
            # 【20260912·CEO纠正】同秒(age=0)属路线A窗口内, 路线A无止损;
            #   亏损上限是路线B出口, 不在A窗口生效 → 同秒只查路线A。
            if p['ra'] is not None:
                hit = any(px <= p['ra'] for _, px in after) if not lng else any(px >= p['ra'] for _, px in after)
                if hit:
                    recs.append(close_pos(p, p['ra'], '路线A(同秒)', FEE_TK, p['i0']))
                    return True
        else:
            if p['sl'] is not None:
                hit = any(px >= p['sl'] for _, px in after) if not lng else any(px <= p['sl'] for _, px in after)
                if hit:
                    recs.append(close_pos(p, p['sl'], 'SL(同秒)', FEE_TK, p['i0']))
                    return True
            if p['ra'] is not None:
                hit = any(px <= p['ra'] for _, px in after) if not lng else any(px >= p['ra'] for _, px in after)
                if hit:
                    recs.append(close_pos(p, p['ra'], '路线A(同秒)', FEE_TK, p['i0']))
                    return True
            if p['tp'] is not None:
                hit = any(px <= p['tp'] for _, px in after) if not lng else any(px >= p['tp'] for _, px in after)
                if hit:
                    recs.append(close_pos(p, p['tp'], 'TP限价(同秒)', FEE_MK, p['i0']))
                    return True
        return False
    # 无逐笔/无触发时刻 → 退回保守整秒判据
    if lng:
        if not (l_ms < h_ms):
            return False
    else:
        if not (h_ms < l_ms):
            return False
    if exit_mode == 'new':
        # 同秒属路线A窗口, 只查路线A(亏损上限属路线B, 不在A窗口生效)
        if p['ra'] is not None:
            hit = (h >= p['ra']) if lng else (l <= p['ra'])
            if hit:
                recs.append(close_pos(p, p['ra'], '路线A(同秒)', FEE_TK, p['i0']))
                return True
    else:
        if p['sl'] is not None:
            hit = (l <= p['sl']) if lng else (h >= p['sl'])
            if hit:
                recs.append(close_pos(p, p['sl'], 'SL(同秒)', FEE_TK, p['i0']))
                return True
        if p['ra'] is not None:
            hit = (h >= p['ra']) if lng else (l <= p['ra'])
            if hit:
                recs.append(close_pos(p, p['ra'], '路线A(同秒)', FEE_TK, p['i0']))
                return True
        if p['tp'] is not None:
            hit = (h >= p['tp']) if lng else (l <= p['tp'])
            if hit:
                recs.append(close_pos(p, p['tp'], 'TP限价(同秒)', FEE_MK, p['i0']))
                return True
    return False


def step_positions(bars, i, poss, recs, exit_mode='old',
                   b_profit=0.06, b_reversal=0.02, b_time=10, b_loss=0.05):
    """每秒推进所有持仓的退出判定。
    exit_mode='old': 固定SL10% → 路线A(成交价±tp_pct, ta秒) → 分岔(仅exp) → TP限价
    exit_mode='new': 路线A(锚价±reb_pct带, ta秒) → 路线B(锚价冻结:
                      ①亏损上限5% ②利润带6% ③反转撤退2% ④时间止损10s)"""
    s, o, h, l, c, _hm, _lm = bars[i]
    mid = (h + l) / 2.0
    for p in poss:
        if not p['open']:
            continue
        lng = p['side'] == 'LONG'
        age = i - p['i0']
        if exit_mode == 'new':
            # ── 路线A: ta秒内回到锚价±reb_pct带内 → 市价平 ──
            if age <= p['ta'] and p['ra'] is not None:
                hit = (h >= p['ra']) if lng else (l <= p['ra'])
                if hit and age > 0:
                    recs.append(close_pos(p, p['ra'], '路线A', FEE_TK, i)); continue
            # ── 路线B: A未触发后接管(锚价p['O']在开仓时已冻结) ──
            if age > p['ta']:
                # ① 亏损上限5%(入场价±5%)硬砍, 纯安全网
                loss_cap = p['entry'] * (1 - b_loss) if lng else p['entry'] * (1 + b_loss)
                if (l <= loss_cap) if lng else (h >= loss_cap):
                    recs.append(close_pos(p, loss_cap, 'B亏损上限', FEE_TK, i)); continue
                # ② 利润带(b_profit; 锚价或成交价基准由 p['bp_base'] 决定) → 平
                bp_base = p['entry'] if p.get('bp_base') == 'entry' else p['O']
                if p.get('bp_base') == 'entry':
                    # 成交价基准: 价格从成交价向盈利方向移动 b_profit(做空: 跌4%)
                    bp = bp_base * (1 + b_profit) if lng else bp_base * (1 - b_profit)
                else:
                    # 锚价基准: 锚价是反向参考点, 符号与盈利方向相反
                    bp = bp_base * (1 - b_profit) if lng else bp_base * (1 + b_profit)
                if (h >= bp) if lng else (l <= bp):
                    recs.append(close_pos(p, bp, 'B利润带', FEE_TK, i)); continue
                # ③ 反转撤退2%: 从最佳回归点反跑2% → 平
                if lng:
                    if mid > p['best']: p['best'] = mid
                    if mid <= p['best'] * (1 - b_reversal):
                        recs.append(close_pos(p, mid, 'B反转撤退', FEE_TK, i)); continue
                else:
                    if mid < p['best']: p['best'] = mid
                    if mid >= p['best'] * (1 + b_reversal):
                        recs.append(close_pos(p, mid, 'B反转撤退', FEE_TK, i)); continue
                # ④ 时间止损10s(自A窗口结束起算) → 市价平
                if age - p['ta'] > b_time:
                    recs.append(close_pos(p, c, 'B超时%ds' % b_time, FEE_TK, i)); continue
        else:
            # ── 旧逻辑(保留作对照) ──
            stop_px = p['sl']
            if p['branch'] and p['pbr'] is True:
                stop_px = max(p['entry'], p['extreme'] * (1 - 0.02)) if lng else \
                          min(p['entry'], p['extreme'] * (1 + 0.02))
            if stop_px is not None:
                hit = (l <= stop_px) if lng else (h >= stop_px)
                if hit and age > 0:
                    recs.append(close_pos(p, stop_px, 'SL/保本' if not p['branch'] else
                                          ('保本/移动止盈' if p['pbr'] else 'SL'), FEE_TK, i))
                    continue
            if age <= p['ta'] and p['ra'] is not None:
                hit = (h >= p['ra']) if lng else (l <= p['ra'])
                if hit and age > 0:
                    recs.append(close_pos(p, p['ra'], '路线A', FEE_TK, i)); continue
            if p['branch'] and age > p['ta']:
                if p['pbr'] is None:
                    pnl_now = (c - p['entry']) / p['entry'] if lng else (p['entry'] - c) / p['entry']
                    p['pbr'] = pnl_now > 0
                if age - p['ta'] > p['hold']:
                    recs.append(close_pos(p, c, '超时%.0fs' % p['hold'], FEE_TK, i)); continue
            if p['branch'] and p['pbr'] is True:
                p['extreme'] = max(p['extreme'], c) if lng else min(p['extreme'], c)
            if p['branch'] and p['pbr'] is False and age > p['ta']:
                cut = p['entry'] * (1 - 0.02) if lng else p['entry'] * (1 + 0.02)
                hit = (l <= cut) if lng else (h >= cut)
                if hit:
                    recs.append(close_pos(p, cut, '快砍-2%', FEE_TK, i)); continue
            hit = (h >= p['tp']) if lng else (l <= p['tp'])
            if hit and age > 0:
                recs.append(close_pos(p, p['tp'], 'TP限价', FEE_MK, i))


def run_conditional(bars, depth, tp_pct, t_a, reb_anchor, reb_pct, branch, hold_s,
                    notional, follow_sec, label, anchor_log=None,
                    exit_mode='old', follow_thresh=0.02, b_profit=0.06,
                    b_reversal=0.02, b_time=10, b_loss=0.05, sym=None,
                    exit_basis='anchor', entry_a_pct=0.06, entry_b_pct=0.04):
    """resident/exp 族: 双侧常驻条件单 + 1s/60s跟随 + 冻结(无风暴单, 20260911 CEO定删除)
    anchor_log: 若提供(列表), 每根 bar 结束时若 上/下墙 base 或 frozen 状态变化,
                则追加一条锚价变更事件 {s,up_b,up_t,dn_b,dn_t,fz}, 供 viewer 画"回测真实墙"。
                仅对代表配置(resident新 ±10%)传此参数即可, 体积很小。"""
    poss, recs = [], []
    # 成交价基准模式: 利润带百分比改用 entry_b_pct(用户定为4%)
    if exit_basis == 'entry':
        b_profit = entry_b_pct
    st = {'BUY': {'base': bars[0][4], 'trig': bars[0][4] * (1 - depth), 'last': bars[0][0]},
          'SELL': {'base': bars[0][4], 'trig': bars[0][4] * (1 + depth), 'last': bars[0][0]}}
    frozen = False
    n_rep = n_gate = 0
    last_a = {'up': None, 'dn': None, 'fz': None}
    def snap():
        nonlocal last_a
        up_b = st['SELL']['base']; up_t = st['SELL']['trig']
        dn_b = st['BUY']['base'];  dn_t = st['BUY']['trig']
        if up_b != last_a['up'] or dn_b != last_a['dn'] or frozen != last_a['fz']:
            last_a = {'up': up_b, 'dn': dn_b, 'fz': frozen}
            if anchor_log is not None:
                anchor_log.append({'s': s, 'up_b': up_b, 'up_t': up_t,
                                   'dn_b': dn_b, 'dn_t': dn_t, 'fz': frozen})
    for i, (s, o, h, l, c, _hm, _lm) in enumerate(bars):
        # --- 持仓推进 ---
        step_positions(bars, i, poss, recs, exit_mode, b_profit, b_reversal, b_time, b_loss)
        # --- 条件单触发判定(先于跟随, 悲观) ---
        for side, px_hit in (('BUY', l), ('SELL', h)):
            stt = st[side]
            if side == 'BUY' and px_hit > stt['trig']:
                continue
            if side == 'SELL' and px_hit < stt['trig']:
                continue
            # 该侧已有持仓则不重复(冻结保证)
            if any(p['open'] and p['side'] == side for p in poss):
                continue
            # 条件单触发必成交, 但市价腿成交在触发时点市价: wick收回≈触发价; 跳空穿越≈按c(悲观)
            entry = min(stt['trig'], c) if side == 'BUY' else max(stt['trig'], c)
            O = stt['base']
            lng = side == 'BUY'
            kind_side = 'LONG' if lng else 'SHORT'
            if exit_mode == 'new':
                if exit_basis == 'entry':
                    # 路线A = 成交价向盈利方向偏移 entry_a_pct(做空: 跌6% → entry*(1-6%))
                    ra = entry * (1 + entry_a_pct) if lng else entry * (1 - entry_a_pct)
                    bp_base = 'entry'
                else:
                    # 路线A = 冻结锚价±reb_pct(当前锚价模式4%)
                    ra = O * (1 - reb_pct) if lng else O * (1 + reb_pct)
                    bp_base = 'O'
                tp = ra
                sl = entry * (1 - b_loss) if lng else entry * (1 + b_loss)
            else:
                if reb_anchor:
                    ra = O * (1 - reb_pct) if lng else O * (1 + reb_pct)
                else:
                    ra = entry * (1 + tp_pct) if lng else entry * (1 - tp_pct)
                tp = entry * (1 + tp_pct) if lng else entry * (1 - tp_pct)
                sl = entry * (1 - 0.10) if lng else entry * (1 + 0.10)
            # 计算触发时刻(ms): 本秒首个满足触发条件的tick(用于同秒平仓"触发后路径"检查)
            trig_ms = None
            if sym:
                ticks = _sec_ticks(sym, s)
                if ticks:
                    for ms, px in ticks:
                        if (side == 'SELL' and px >= stt['trig']) or (side == 'BUY' and px <= stt['trig']):
                            trig_ms = ms
                            break
            p = mk_pos('R', kind_side, entry, O, i, tp, sl, ra, t_a, branch, hold_s, notional, sym=sym, bp_base=bp_base)
            if trig_ms is not None:
                p['trig_ms'] = trig_ms
            poss.append(p)
            frozen = True
            recs.append({'fill': True, 'side': kind_side, 'entry': entry, 'i': i, 'base': O,
                         'trig': stt['trig'], 'mv': 0, 'net': 0, 'notional': notional,
                         'kind': 'R', 'how': 'FILL'})
            # 毫秒级：条件单成交后立即检查同秒内能否平仓（路线A / SL / TP限价）
            same_bar_exit(p, (s, o, h, l, c, _hm, _lm), exit_mode, b_profit, b_reversal, b_time, b_loss, recs)
        # --- 冻结守卫(存活侧距现价<8%重挂) ---
        if frozen:
            for side in ('BUY', 'SELL'):
                if any(p['open'] and p['side'] == side for p in poss):
                    continue
                stt = st[side]
                if abs(c - stt['trig']) / c < 0.08:
                    stt['base'] = c
                    stt['trig'] = c * (1 - depth) if side == 'BUY' else c * (1 + depth)
                    n_rep += 1
        # --- 平仓完→解冻重建 ---
        if frozen and not any(p['open'] for p in poss):
            frozen = False
            for side in ('BUY', 'SELL'):
                st[side]['base'] = c
                st[side]['trig'] = c * (1 - depth) if side == 'BUY' else c * (1 + depth)
        # --- 跟随重挂(未冻结): mid=(高+低)/2, 阈值 follow_thresh(默认1%) ---
        if not frozen:
            mid = (h + l) / 2.0
            for side in ('BUY', 'SELL'):
                stt = st[side]
                if s - stt['last'] >= follow_sec:
                    stt['last'] = s
                    mv = abs(mid - stt['base']) / stt['base']
                    if mv >= follow_thresh:
                        stt['base'] = mid
                        stt['trig'] = mid * (1 - depth) if side == 'BUY' else mid * (1 + depth)
                        n_rep += 1
        # --- 锚价变更事件快照(供 viewer 真实墙) ---
        if anchor_log is not None:
            snap()
    # 收尾: 未平仓按最后close估值
    for p in poss:
        if p['open']:
            recs.append(close_pos(p, bars[-1][4], '数据结束未平(按收盘估值)', FEE_TK, len(bars) - 1))
    return summarize(recs, label, notional, n_rep, n_gate)


def run_react(bars, notional, follow_sec, label, dyn=True):
    """react: 动态阈值市价(无风暴单, 20260911 CEO定删除); follow_sec=锚价刷新周期(1或60)"""
    poss, recs = [], []
    hist = []            # 分钟振幅 deque(maxlen=10) 手工
    anchor = bars[0][4]
    last_anc = bars[0][0]
    cool = {'LONG': 0, 'SHORT': 0}
    minute = -1
    m_hi = m_lo = None
    n_gate = 0
    for i, (s, o, h, l, c, _hm, _lm) in enumerate(bars):
        m = s // 60
        if m != minute:
            if minute >= 0 and m_hi and m_lo and m_lo > 0:
                hist.append((m_hi - m_lo) / m_lo)
                if len(hist) > 10:
                    hist.pop(0)
            minute = m
            m_hi = m_lo = None
        if m_hi is None or h > m_hi: m_hi = h
        if m_lo is None or l < m_lo: m_lo = l
        # 动态阈值
        if dyn and len(hist) >= 3:
            e = hist[0]
            for x in hist[1:]:
                e = 0.3 * x + 0.7 * e
            depth = min(1.0, max(0.10, e * 4.0))
        else:
            depth = 0.10
        step_positions(bars, i, poss, recs)
        # 触发判定(用旧锚, 对应bot 0.05s扫描能抓秒内插针: 用l/h判触发, 触发价≈市价成交价)
        for kind_side in ('LONG', 'SHORT'):
            if cool[kind_side] > s:
                continue
            if any(p['open'] and p['side'] == kind_side for p in poss):
                continue
            lng = kind_side == 'LONG'
            thr = anchor * (1 - depth) if lng else anchor * (1 + depth)
            fire = (l < thr) if lng else (h > thr)
            if fire:
                # 成交价=触发时点市价: 秒内收回(wick)≈触发价; 跳空穿越(c越过thr)≈按收盘c成交(更差,悲观)
                entry = min(thr, c) if lng else max(thr, c)
                tp = entry * (1 + 0.03) if lng else entry * (1 - 0.03)
                sl = entry * (1 - 0.10) if lng else entry * (1 + 0.10)
                ra = tp
                poss.append(mk_pos('M', kind_side, entry, anchor, i, tp, sl, ra, 3.0, False, 0, notional))
                cool[kind_side] = s + 60
                recs.append({'fill': True, 'side': kind_side, 'entry': entry, 'i': i, 'base': anchor,
                             'trig': thr, 'mv': 0, 'net': 0, 'notional': notional, 'kind': 'M', 'how': 'FILL'})
        # 锚价刷新(每 follow_sec 秒一次; 闸门逻辑已删除)
        if anchor > 0 and s - last_anc >= follow_sec:
            last_anc = s
            anchor = c
    for p in poss:
        if p['open']:
            recs.append(close_pos(p, bars[-1][4], '数据结束未平(按收盘估值)', FEE_TK, len(bars) - 1))
    return summarize(recs, label, notional, 0, n_gate)


def summarize(recs, label, notional, n_rep, n_gate):
    fills = [r for r in recs if r.get('fill')]
    exits = [r for r in recs if not r.get('fill')]
    net = sum(r['net'] for r in exits)
    wins = [r for r in exits if r['net'] > 0]
    losses = [r for r in exits if r['net'] <= 0]
    return {'label': label, 'fills': fills, 'exits': exits, 'net': net,
            'n_win': len(wins), 'n_loss': len(losses),
            'win_usdt': sum(r['net'] for r in wins), 'loss_usdt': sum(r['net'] for r in losses),
            'n_rep': n_rep, 'n_gate': n_gate}


# ── 主流程 ──────────────────────────────────────────────────
def main():
    only = [a for a in sys.argv[1:] if not a.startswith('--')]
    if not only:
        only = COINS
    DETAIL_ROWS = []   # (脚本,版本,币种,腿,方向,类型,UTC毫秒,价格,净值U,出场方式,锚价base,墙位trig)
    ALL_ANCHOR = {}    # sym -> [锚价变更事件]  (resident新 ±10% 真实轨迹)
    for sym in only:
        print('=' * 110)
        print('【%s】加载7天逐笔...' % sym)
        bars = load_bars(sym)
        print('  bars=%d  覆盖 %s ~ %s' % (len(bars), hhmmss(bars[0][0]), hhmmss(bars[-1][0])))
        # 事件清单
        eps = find_episodes(bars)
        print('  穿刺episode(>=8%%): %d 个' % len(eps))
        for e in eps:
            print('    %s  跌%.1f%% | %ds内跌完(单秒最大%.1f%%) | 10min反弹%.1f%%(收回%.0f%%) → %s'
                  % (hhmmss(e['hi_t']), e['drop'] * 100, e['fall_s'], e['max1s'] * 100,
                     e['reb'] * 100, e['recover'] * 100, ep_class(e)))
        print()
        # 【20260912 扩样本】锚价轨迹只记录 COINS(AKE/BULLA)。
        # 54个币全记 = 3000万行CSV, 会撑爆磁盘; 新币只需成交明细用于统计。
        anchor_log = (ALL_ANCHOR.setdefault(sym, []) if sym in COINS else None)
        configs = [
            # 【20260912 扩样本·隔离变量】旧/新退出 × 跟随阈值1%/2% 四组合,
            # 把"退出逻辑"和"跟随灵敏度"两个变量拆开(此前混在一起, 结论不干净)。
            ('resident 旧退出(跟随2%)', lambda b, sym=sym: run_conditional(b, 0.10, 0.05, 3.0, False, 0, False, 0, 10, 1, 'resident旧退出', exit_mode='old', follow_thresh=0.02, sym=sym)),
            ('resident 旧退出(跟随1%)', lambda b, sym=sym: run_conditional(b, 0.10, 0.05, 3.0, False, 0, False, 0, 10, 1, 'resident旧退出', exit_mode='old', follow_thresh=0.01, sym=sym)),
            ('resident 新退出(跟随1%)', lambda b, sym=sym: run_conditional(b, 0.10, 0.05, 1.0, False, 0.04, False, 0, 10, 1, 'resident新退出',
                                                               exit_mode='new', follow_thresh=0.01,
                                                               b_profit=0.06, b_reversal=0.02, b_time=10, b_loss=0.05,
                                                               anchor_log=anchor_log, sym=sym)),
            ('resident 新退出(跟随2%)', lambda b, sym=sym: run_conditional(b, 0.10, 0.05, 1.0, False, 0.04, False, 0, 10, 1, 'resident新退出',
                                                               exit_mode='new', follow_thresh=0.02,
                                                               b_profit=0.06, b_reversal=0.02, b_time=10, b_loss=0.05,
                                                               anchor_log=anchor_log, sym=sym)),
            ('exp11    旧退出(1s)', lambda b, sym=sym: run_conditional(b, 0.15, 0.03, 5.0, True, 0.05, True, 900, 3, 1, 'exp11旧退出', exit_mode='old', sym=sym)),
            ('exp11    新退出(1s)', lambda b, sym=sym: run_conditional(b, 0.15, 0.03, 1.0, True, 0.04, True, 900, 3, 1, 'exp11新退出',
                                                               exit_mode='new', follow_thresh=0.01,
                                                               b_profit=0.06, b_reversal=0.02, b_time=10, b_loss=0.05, sym=sym)),
        ]
        print('  %-24s %6s %6s %8s %10s %10s %10s %12s' %
              ('配置', '成交', '胜', '负', '盈利U', '亏损U', '净利U', '净收益率%'))
        print('  ' + '-' * 100)
        results = []
        for label, fn in configs:
            r = fn(bars)
            results.append((label, r))
            tot = sum(x['notional'] for x in r['fills']) or 1
            print('  %-24s %6d %6d %8d %10.2f %10.2f %10.2f %11.2f%%' %
                  (label, len(r['fills']), r['n_win'], r['n_loss'],
                   r['win_usdt'], r['loss_usdt'], r['net'], r['net'] / tot * 100))
        print()
        # 每笔成交明细
        for label, r in results:
            # ── 导出成交点(开仓+平仓)给查看器标记 ──
            m_ver = re.search(r'(新|旧)退出', label)
            ver = (m_ver.group(1) if m_ver else '')
            m_ft = re.search(r'跟随([12])%', label)
            if m_ft:
                ver += m_ft.group(1) + '%'
            elif scr.startswith('exp11'):
                ver += '2%' if '旧' in ver else '1%'
            scr = label.split()[0]
            # 把每笔"成交(fill)"的真实锚价base/墙位trig, 按 (入场根i0, 方向) 建索引, 供平仓记录关联
            fills_by = {(fl['i'], fl['side']): fl for fl in r['fills']}
            for ex in r['exits']:
                # 把每笔"成交(fill)"的真实锚价base/墙位trig, 按 (入场根i0, 方向) 关联到平仓记录
                fl = fills_by.get((ex['i0'], ex['side']))
                a_base = fl['base'] if fl else None
                a_trig = fl['trig'] if fl else None
                DETAIL_ROWS.append((scr, ver, sym, ex['kind'], ex['side'], '开仓',
                                    bars[ex['i0']][0], ex['entry'], 0.0, '-', a_base, a_trig))
                ie = ex.get('i_exit')
                if ie is not None:
                    DETAIL_ROWS.append((scr, ver, sym, ex['kind'], ex['side'], '平仓',
                                        bars[ie][0], ex['exit'], ex['net'], ex['how'], a_base, a_trig))
            if not r['exits']:
                continue
            print('  --- %s 成交明细(%d笔) ---' % (label, len(r['exits'])))
            print('     %-16s %-5s %-5s %10s %10s %8s %s' %
                  ('入场时间', '腿', '方向', '入场', '出场', '净值U', '出场方式'))
            for ex in r['exits'][:80]:
                print('     %-16s %-5s %-5s %10.6g %10.6g %8.2f %s' %
                      (hhmmss(bars[ex['i0']][0]), ex['kind'], ex['side'],
                       ex['entry'], ex['exit'], ex['net'], ex['how']))
            if len(r['exits']) > 80:
                print('     ...(共%d笔)' % len(r['exits']))
            print()

    # ── 导出成交明细 CSV(供 kline_viewer 标记) ──
    import csv as _csv
    with open('成交明细_回测.csv', 'w', newline='', encoding='utf-8-sig') as f:
        w = _csv.writer(f)
        w.writerow(['脚本', '版本', '币种', '腿', '方向', '类型', '北京时间', 'UTC毫秒',
                    '价格', '净值U', '出场方式', '锚价base', '墙位trig'])
        for (scr, ver, sym, kind, side, typ, ts, px, net, how, ab, at) in DETAIL_ROWS:
            w.writerow([scr, ver, sym, kind, side, typ, hhmmss(ts), ts * 1000,
                        ('%.8g' % px), ('%.4f' % net), how,
                        ('%.8g' % ab) if ab is not None else '',
                        ('%.8g' % at) if at is not None else ''])
    # ── 导出 锚价轨迹(resident新 ±10% 回测真实) ──
    with open('锚价轨迹_回测.csv', 'w', newline='', encoding='utf-8-sig') as f:
        w = _csv.writer(f)
        w.writerow(['币种', '脚本', '版本', 'UTC毫秒', '北京时间',
                    '上墙锚base', '上墙trig', '下墙锚base', '下墙trig', '持仓冻结'])
        for sym in ALL_ANCHOR:
            for e in ALL_ANCHOR[sym]:
                w.writerow([sym, 'resident', '新', e['s'] * 1000, hhmmss(e['s']),
                            ('%.8g' % e['up_b']), ('%.8g' % e['up_t']),
                            ('%.8g' % e['dn_b']), ('%.8g' % e['dn_t']),
                            ('1' if e['fz'] else '0')])
    print('=' * 110)
    print('成交明细已导出: 成交明细_回测.csv (%d 行, 开仓+平仓)' % len(DETAIL_ROWS))
    n_tr = sum(len(v) for v in ALL_ANCHOR.values())
    print('锚价轨迹已导出: 锚价轨迹_回测.csv (%d 个变更事件, resident新 ±10%%)' % n_tr)


if __name__ == '__main__':
    main()
