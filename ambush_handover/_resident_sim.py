# -*- coding: utf-8 -*-
"""
resident_loop 秒级跟随 + 跳变闸门  真实tick回测

【与线上代码的严格对应】(ambush_basket.py resident_loop, 改造后)
  每 RES_TICK 秒执行一次逐币维护:
      cur = mid(sym)                                  # 实时中间价
      mv  = |cur - cond.base| / cond.base
      if mv >= RESIDENT_REPEG(2%):
          danger = (cur < base) if BUY else (cur > base)     # 价格是否朝触发价扑来
          if JUMP_GATE>0 and mv >= JUMP_GATE and danger:
              continue                                # 判为针尖 -> 不跟随, 留单接货
          trigger = cur*(1±DEPTH);  base = cur        # 撤旧挂新
  触发判定: 条件单在**币安服务器**上, 逐tick实时判(非本地采样!)
      BUY  成交 <=> 某tick价 <= trigger
      SELL 成交 <=> 某tick价 >= trigger

  同一秒内的顺序: 先判触发(用旧trigger) -> 再做跟随更新。
  这是悲观假设(重挂总有网络延迟), 不会高估策略。

用法: python _resident_sim.py
"""
import os, csv, datetime

TZ = datetime.timezone(datetime.timedelta(hours=8))
HERE = os.path.dirname(os.path.abspath(__file__))
TICKS = os.path.join(HERE, 'ticks_evt')
REPEG = 0.02            # RESIDENT_REPEG


def load_day(sym, day):
    """秒级 OHLC: [(sec,o,h,l,c,n)]；无成交秒用上一秒收盘平推(模拟 mid 冻结)"""
    fp = os.path.join(TICKS, '%s_%s.csv' % (sym, day.replace('-', '')))
    if not os.path.exists(fp):
        print('!! 缺数据', fp)
        return []
    secs = {}
    with open(fp, newline='') as f:
        for r in csv.reader(f):
            if len(r) < 2:
                continue
            try:
                ts = int(r[0]); p = float(r[1])
            except ValueError:
                continue
            s = ts // 1000
            e = secs.get(s)
            if e is None:
                secs[s] = [p, p, p, p, 1]
            else:
                if p > e[1]: e[1] = p
                if p < e[2]: e[2] = p
                e[3] = p; e[4] += 1
    if not secs:
        return []
    k0, k1 = min(secs), max(secs)
    rows, prev = [], None
    for s in range(k0, k1 + 1):
        if s in secs:
            o, h, l, c, n = secs[s]; prev = c
        else:
            o = h = l = c = prev if prev else 0.0; n = 0
        if c > 0:
            rows.append((s, o, h, l, c, n))
    return rows


def sim_resident(rows, tick, depth, jump_gate, side='BUY', win=None):
    """返回 {fills:[(t,price,trigger,base,drop%)], n_reprice, n_gate}"""
    if not rows:
        return {}
    if win:
        rows = [r for r in rows if win[0] <= r[0] <= win[1]]
    if not rows:
        return {}
    base = rows[0][4]
    trigger = base * (1 - depth) if side == 'BUY' else base * (1 + depth)
    last_t = rows[0][0]
    fills, n_rep, n_gate = [], 1, 0
    for (s, o, h, l, c, n) in rows:
        # ── 1) 触发判定(交易所侧, 逐tick实时) ──
        hit = (l <= trigger) if side == 'BUY' else (h >= trigger)
        if hit:
            px = trigger                                  # 条件单触发后市价成交, 近似取触发价
            fills.append((s, px, trigger, base,
                          (base - px) / base * 100 if side == 'BUY' else (px - base) / base * 100))
            # 成交后该侧冻结(CEO定: 不撤另一侧)。本仿真只看"是否被打到"→ 记一次后重建
            base = c
            trigger = base * (1 - depth) if side == 'BUY' else base * (1 + depth)
            last_t = s
            n_rep += 1
            continue
        # ── 2) 跟随更新(每 tick 秒一次) ──
        if s - last_t >= tick:
            last_t = s
            mv = abs(c - base) / base
            if mv >= REPEG:
                danger = (c < base) if side == 'BUY' else (c > base)
                if jump_gate > 0 and mv >= jump_gate and danger:
                    n_gate += 1
                    continue                              # 针尖: 不跟随, 留单接货
                base = c
                trigger = base * (1 - depth) if side == 'BUY' else base * (1 + depth)
                n_rep += 1
    return {'fills': fills, 'n_reprice': n_rep, 'n_gate': n_gate}


def hhmmss(s):
    return datetime.datetime.fromtimestamp(s, TZ).strftime('%H:%M:%S')


def worst_drop(rows, span):
    """全天扫描: 找出 span 秒滚动窗口内 最大跌幅(从窗口内最高到之后最低)。
    返回 (跌幅%, 起点秒, 谷底秒)。用于客观定位事件, 不靠人工指定窗口。"""
    best = (0.0, None, None)
    n = len(rows)
    j = 0
    for i in range(n):
        hi = rows[i][2]           # high
        if hi <= 0:
            continue
        j = max(j, i)
        lo, lo_t = hi, rows[i][0]
        k = i
        while k < n and rows[k][0] - rows[i][0] <= span:
            if rows[k][3] < lo:
                lo = rows[k][3]; lo_t = rows[k][0]
            k += 1
        dp = (hi - lo) / hi * 100
        if dp > best[0]:
            best = (dp, rows[i][0], lo_t)
    return best


def main():
    day = '2026-09-08'
    cases = [
        ('FORMUSDT', '真乌龙(12:37 极值-9.09%, 砸完弹回)', (12 * 3600 + 30 * 60, 12 * 3600 + 45 * 60)),
        ('XANUSDT',  '阴跌(14:42~43 连续下行不回弹=实盘亏损单)', (14 * 3600 + 35 * 60, 14 * 3600 + 55 * 60)),
    ]
    d0 = int(datetime.datetime.strptime(day, '%Y-%m-%d').replace(tzinfo=TZ).timestamp())

    # ── 客观定位: 各币全天最猛的 1s/3s/15s/60s 跌幅, 判断"针"到底有多陡 ──
    print('=' * 112)
    print('【零】事件陡度客观测量: 滚动窗口最大跌幅 (决定 跟随周期 会不会"跟丢"针)')
    print('=' * 112)
    print('  %-12s %-10s %12s %12s %12s %12s' % ('币', '数据秒数', '1秒内', '3秒内', '15秒内', '60秒内'))
    print('  ' + '-' * 104)
    steep = {}
    for sym, day2 in (('FORMUSDT', day), ('XANUSDT', day), ('BOMEUSDT', '2026-09-04')):
        rows = load_day(sym, day2)
        if not rows:
            continue
        vals = []
        for span in (1, 3, 15, 60):
            dp, t0_, tl = worst_drop(rows, span)
            vals.append(dp)
        steep[sym] = vals
        print('  %-12s %-10d %11.2f%% %11.2f%% %11.2f%% %11.2f%%' % (
            sym, len(rows), vals[0], vals[1], vals[2], vals[3]))
    print('  ' + '-' * 104)
    print('  读法: 若"3秒内最大跌幅" < 深度(10%), 则 3s 跟随周期不会漏针(针必然跨周期);')
    print('        若某针是"每秒跌6%连跌两秒"(1秒内6% / 3秒内12%), 则1s跟随会跟丢, 3s能接住。')
    print()

    for depth, tag in ((0.10, 'resident主(±10%)'), (0.15, 'exp11/exp50(±15%)')):
        print('=' * 112)
        print('【BUY侧埋伏单 是否被打到】深度 %s   数据: 币安真实逐笔成交' % tag)
        print('=' * 112)
        for sym, desc, w in cases:
            rows = load_day(sym, day)
            if not rows:
                continue
            win = (d0 + w[0], d0 + w[1])
            print('  [%s] %s' % (sym, desc))
            print('  窗口 %s~%s' % (hhmmss(win[0]), hhmmss(win[1])))
            print('  %-9s %-9s %8s %10s %10s %-20s %-12s' % (
                '跟随周期', '跳变闸门', '成交次数', '重挂次数', '闸门拦截', '首次成交时刻', '相对锚跌幅'))
            print('  ' + '-' * 104)
            for tick in (15, 5, 3, 2, 1):
                for gate in (0.0, 0.08):
                    r = sim_resident(rows, tick, depth, gate, 'BUY', win)
                    if not r:
                        continue
                    gtxt = '关闭' if gate == 0 else '%.0f%%' % (gate * 100)
                    if r['fills']:
                        t, px, tr, bs, dp = r['fills'][0]
                        print('  %-9s %-9s %8d %10d %10d %-20s %10.2f%%  %s' % (
                            '%ds' % tick, gtxt, len(r['fills']), r['n_reprice'], r['n_gate'],
                            hhmmss(t), dp, '<<< 成交' ))
                    else:
                        print('  %-9s %-9s %8d %10d %10d %-20s %10s' % (
                            '%ds' % tick, gtxt, 0, r['n_reprice'], r['n_gate'], '— 未被打到 —', '-'))
                print('  ' + '-' * 104)
            print()

    # ── 全天 API 压力(重挂次数) ──
    print('=' * 112)
    print('【全天 API 压力】重挂次数 = 撤旧2+挂新2 = 4次签名请求 (双侧各算一次)')
    print('=' * 112)
    print('  %-11s %-9s %-9s %12s %12s %14s %14s' % (
        '币', '跟随周期', '跳变闸门', 'BUY重挂', 'SELL重挂', '双侧合计/天', 'API请求/天'))
    print('  ' + '-' * 104)
    for sym, _, _ in cases:
        rows = load_day(sym, day)
        if not rows:
            continue
        span = (rows[-1][0] - rows[0][0]) / 86400.0
        for tick in (15, 5, 3, 1):
            for gate in (0.0, 0.08):
                b = sim_resident(rows, tick, 0.10, gate, 'BUY')
                s2 = sim_resident(rows, tick, 0.10, gate, 'SELL')
                tot = (b['n_reprice'] + s2['n_reprice']) / span if span else 0
                gtxt = '关闭' if gate == 0 else '%.0f%%' % (gate * 100)
                print('  %-11s %-9s %-9s %12d %12d %14.0f %14.0f' % (
                    sym, '%ds' % tick, gtxt, b['n_reprice'], s2['n_reprice'], tot, tot * 4))
        print('  ' + '-' * 104)
    print('  注: 单币数据。resident20=20币 / exp50=50币 需按币数乘。')


if __name__ == '__main__':
    main()
