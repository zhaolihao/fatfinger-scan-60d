# -*- coding: utf-8 -*-
"""
四逻辑统一回测: AKEUSDT / BULLAUSDT 7天逐笔(aggTrades) × 4进程逻辑 × 新旧锚价对比
【口径 = 线上最新代码 ambush_basket.py, 2026-09-09】
  - 新配置: 锚价/跟随 1s; resident系 无跳变闸门; react 保留 8% 闸门(未重启, 原参数)
  - 旧配置对照: 60s 锚价/跟随, 无闸门 (隔离"秒级精准度"这一个变量, 退出逻辑两版相同)
四套逻辑:
  react   : CMP动态阈值(EMA分钟振幅x4, 地板10%) 市价M + 风暴S, TP3%/SL10%, 路线A 3s, 无分岔
  resident: 条件单±10%双侧, TP5%/SL10%, 路线A 3s(相对成交价), 无分岔, 冻结+8%守卫, 风暴S
  exp11   : 条件单±15%双侧, TP3%/SL10%, 路线A 5s(目标=锚x0.95/1.05 reb-anchor), 分岔900s(tr/cut 2%), 风暴S
  exp50   : 同exp11(仅单笔金额不同)
悲观假设: 触发判定先用旧trigger再做跟随; 同秒TP与SL同时可达→按SL; 手续费 taker 0.05% / maker 0.02%。
"""
import os, csv, pickle, datetime, sys

TZ = datetime.timezone(datetime.timedelta(hours=8))
HERE = os.path.dirname(os.path.abspath(__file__))
TICKS = os.path.join(HERE, 'ticks')
FEE_TK, FEE_MK = 0.0005, 0.0002
COINS = ['AKEUSDT', 'BULLAUSDT']
DAYS = ['2026-08-31', '2026-09-01', '2026-09-02', '2026-09-03', '2026-09-04', '2026-09-05', '2026-09-06']


def hhmmss(s):
    return datetime.datetime.fromtimestamp(s, TZ).strftime('%m-%d %H:%M:%S')


def load_bars(sym):
    """7天合并的秒级bars [(s,o,h,l,c)]; 带pkl缓存"""
    cache = os.path.join(HERE, '_cache4_%s.pkl' % sym)
    if os.path.exists(cache):
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
                e = secs.get(s)
                if e is None:
                    secs[s] = [p, p, p, p]
                else:
                    if p > e[1]: e[1] = p
                    if p < e[2]: e[2] = p
                    e[3] = p
    bars, prev = [], None
    for s in sorted(secs):
        o, h, l, c = secs[s]
        bars.append((s, o, h, l, c)); prev = c
    if not bars:
        print('  !! 无数据', sym); return []
    with open(cache, 'wb') as f:
        pickle.dump(bars, f, protocol=4)
    return bars


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
        s, o, h, l, c = bars[i]
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
def mk_pos(kind, side, entry, O, i0, tp_px, sl_px, ra_target, t_a, branch, hold_s, notional):
    return {'kind': kind, 'side': side, 'entry': entry, 'O': O, 'i0': i0,
            'tp': tp_px, 'sl': sl_px, 'ra': ra_target, 'ta': t_a,
            'branch': branch, 'hold': hold_s, 'notional': notional,
            'extreme': entry, 'pbr': None, 'open': True}


def pos_fee_in(p):
    return FEE_MK if p['kind'] == 'S' else FEE_TK


def close_pos(p, px, how, fee_out, i_exit=None):
    lng = p['side'] == 'LONG'
    mv = (px - p['entry']) / p['entry'] if lng else (p['entry'] - px) / p['entry']
    net = p['notional'] * (mv - pos_fee_in(p) - fee_out)
    p['open'] = False
    return {'kind': p['kind'], 'side': p['side'], 'entry': p['entry'], 'exit': px, 'how': how,
            'i0': p['i0'], 'i_exit': i_exit, 'mv': mv, 'net': net, 'notional': p['notional']}


def step_positions(bars, i, poss, recs):
    """每秒推进所有持仓的退出判定(悲观: 止损优先→分岔/路线A→TP限价)"""
    s, o, h, l, c = bars[i]
    for p in poss:
        if not p['open']:
            continue
        lng = p['side'] == 'LONG'
        age = i - p['i0']
        # 1) 固定SL(交易所端) / 分岔保本单
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
        # 2) 路线A窗口: 回弹到目标市价平
        if age <= p['ta'] and p['ra'] is not None:
            hit = (h >= p['ra']) if lng else (l <= p['ra'])
            if hit and age > 0:
                recs.append(close_pos(p, p['ra'], '路线A', FEE_TK, i))
                continue
        # 3) 分岔分支逻辑(路线A窗口结束后)
        if p['branch'] and age > p['ta']:
            if p['pbr'] is None:
                pnl_now = (c - p['entry']) / p['entry'] if lng else (p['entry'] - c) / p['entry']
                p['pbr'] = pnl_now > 0
            if age - p['ta'] > p['hold']:
                recs.append(close_pos(p, c, '超时%.0fs' % p['hold'], FEE_TK, i))
                continue
        # 4) 更新极值(用c, 对应bot 0.1s轮询mid)
        if p['branch'] and p['pbr'] is True:
            p['extreme'] = max(p['extreme'], c) if lng else min(p['extreme'], c)
        # 5) 快砍(亏损分支) — 已并入step1的stop_px? 亏损分支stop=entry*(1-cut) 在下方单独判
        if p['branch'] and p['pbr'] is False and age > p['ta']:
            cut = p['entry'] * (1 - 0.02) if lng else p['entry'] * (1 + 0.02)
            hit = (l <= cut) if lng else (h >= cut)
            if hit:
                recs.append(close_pos(p, cut, '快砍-2%', FEE_TK, i))
                continue
        # 6) TP限价(maker)
        hit = (h >= p['tp']) if lng else (l <= p['tp'])
        if hit and age > 0:
            recs.append(close_pos(p, p['tp'], 'TP限价', FEE_MK, i))
    # 亏损分支的"快砍"其实等效固定-2%止损; 上面step1的stop_px未含 → 已单独处理(step5)


def run_conditional(bars, depth, tp_pct, t_a, reb_anchor, reb_pct, branch, hold_s,
                    notional, follow_sec, gate, label, storm_on=True, anchor_log=None):
    """resident/exp 族: 双侧常驻条件单 + 1s/60s跟随 + 冻结 + 风暴
    anchor_log: 若提供(列表), 每根 bar 结束时若 上/下墙 base 或 frozen 状态变化,
                则追加一条锚价变更事件 {s,up_b,up_t,dn_b,dn_t,fz}, 供 viewer 画"回测真实墙"。
                仅对代表配置(resident新 ±10%)传此参数即可, 体积很小。"""
    poss, recs, storms = [], [], []
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
    for i, (s, o, h, l, c) in enumerate(bars):
        # --- 持仓推进 ---
        step_positions(bars, i, poss, recs)
        # --- 风暴单成交判定 ---
        for so in storms:
            if so['done'] or i < so['i0']:
                continue
            if i - so['i0'] > 300:
                so['done'] = True
                continue
            lng = so['side'] == 'LONG'
            hit = (l <= so['lim']) if lng else (h >= so['lim'])
            if hit:
                so['done'] = True
                e = so['lim']
                tp = e * (1 + 0.05) if lng else e * (1 - 0.05)
                ra = tp  # 风暴腿路线A目标=STORM_TP 5%(相对风暴成交价)
                sl = e * (1 - 0.10) if lng else e * (1 + 0.10)
                p = mk_pos('S', so['side'], e, e, i, tp, sl, ra, t_a, branch, hold_s, notional)
                poss.append(p)
        storms = [x for x in storms if not x['done']]
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
            if reb_anchor:
                ra = O * (1 - reb_pct) if lng else O * (1 + reb_pct)
            else:
                ra = entry * (1 + tp_pct) if lng else entry * (1 - tp_pct)
            tp = entry * (1 + tp_pct) if lng else entry * (1 - tp_pct)
            sl = entry * (1 - 0.10) if lng else entry * (1 + 0.10)
            p = mk_pos('R', kind_side, entry, O, i, tp, sl, ra, t_a, branch, hold_s, notional)
            poss.append(p)
            frozen = True
            recs.append({'fill': True, 'side': kind_side, 'entry': entry, 'i': i, 'base': O,
                         'trig': stt['trig'], 'mv': 0, 'net': 0, 'notional': notional,
                         'kind': 'R', 'how': 'FILL'})
            if storm_on:
                lim = entry * (1 - 0.05) if lng else entry * (1 + 0.05)
                storms.append({'side': kind_side, 'lim': lim, 'i0': i, 'done': False})
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
        # --- 跟随重挂(未冻结) ---
        if not frozen:
            for side in ('BUY', 'SELL'):
                stt = st[side]
                if s - stt['last'] >= follow_sec:
                    stt['last'] = s
                    mv = abs(c - stt['base']) / stt['base']
                    if mv >= 0.02:
                        danger = (c < stt['base']) if side == 'BUY' else (c > stt['base'])
                        if gate > 0 and mv >= gate and danger:
                            n_gate += 1
                            continue
                        stt['base'] = c
                        stt['trig'] = c * (1 - depth) if side == 'BUY' else c * (1 + depth)
                        n_rep += 1
        # --- 锚价变更事件快照(供 viewer 真实墙) ---
        if anchor_log is not None:
            snap()
    # 收尾: 未平仓按最后close估值
    for p in poss:
        if p['open']:
            recs.append(close_pos(p, bars[-1][4], '数据结束未平(按收盘估值)', FEE_TK, len(bars) - 1))
    return summarize(recs, label, notional, n_rep, n_gate)


def run_react(bars, notional, follow_sec, gate, label, dyn=True):
    """react: 动态阈值市价 + 风暴; follow_sec=锚价刷新周期(1或60)"""
    poss, recs, storms = [], [], []
    hist = []            # 分钟振幅 deque(maxlen=10) 手工
    anchor = bars[0][4]
    last_anc = bars[0][0]
    cool = {'LONG': 0, 'SHORT': 0}
    minute = -1
    m_hi = m_lo = None
    n_gate = 0
    for i, (s, o, h, l, c) in enumerate(bars):
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
        # 风暴单
        for so in storms:
            if so['done'] or i < so['i0']:
                continue
            if i - so['i0'] > 300:
                so['done'] = True; continue
            lng = so['side'] == 'LONG'
            hit = (l <= so['lim']) if lng else (h >= so['lim'])
            if hit:
                so['done'] = True
                e2 = so['lim']
                tp = e2 * (1 + 0.05) if lng else e2 * (1 - 0.05)
                sl = e2 * (1 - 0.10) if lng else e2 * (1 + 0.10)
                poss.append(mk_pos('S', so['side'], e2, e2, i, tp, sl, tp, 3.0, False, 0, notional))
        storms = [x for x in storms if not x['done']]
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
                lim = entry * (1 - 0.05) if lng else entry * (1 + 0.05)
                storms.append({'side': kind_side, 'lim': lim, 'i0': i, 'done': False})
        # 锚价刷新(每 follow_sec 秒一次, 闸门拦截超8%跳变)
        if anchor > 0 and s - last_anc >= follow_sec:
            last_anc = s
            mv = abs(c - anchor) / anchor
            if gate > 0 and mv >= gate:
                n_gate += 1
            else:
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
        anchor_log = ALL_ANCHOR.setdefault(sym, [])
        configs = [
            # label, fn
            ('react    旧(60s锚,无闸)', lambda b: run_react(b, 10, 60, 0.0, 'react旧')),
            ('react    新(1s锚,无闸)',  lambda b: run_react(b, 10, 1, 0.0, 'react新')),
            ('resident 旧(60s,无闸)',   lambda b: run_conditional(b, 0.10, 0.05, 3.0, False, 0, False, 0, 10, 60, 0.0, 'resident旧')),
            ('resident 新(1s,无闸)',    lambda b: run_conditional(b, 0.10, 0.05, 3.0, False, 0, False, 0, 10, 1, 0.0, 'resident新', anchor_log=anchor_log)),
            ('exp11    旧(60s,无闸)',   lambda b: run_conditional(b, 0.15, 0.03, 5.0, True, 0.05, True, 900, 3, 60, 0.0, 'exp11旧')),
            ('exp11    新(1s,无闸)',    lambda b: run_conditional(b, 0.15, 0.03, 5.0, True, 0.05, True, 900, 3, 1, 0.0, 'exp11新')),
            ('exp50    旧(60s,无闸)',   lambda b: run_conditional(b, 0.15, 0.03, 5.0, True, 0.05, True, 900, 5, 60, 0.0, 'exp50旧')),
            ('exp50    新(1s,无闸)',    lambda b: run_conditional(b, 0.15, 0.03, 5.0, True, 0.05, True, 900, 5, 1, 0.0, 'exp50新')),
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
            ver = '新' if '新' in label else ('旧' if '旧' in label else '')
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
            print('  --- %s 成交明细(%d笔, 含风暴腿) ---' % (label, len(r['exits'])))
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
