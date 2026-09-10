# -*- coding: utf-8 -*-
"""从原始aggTrades聚合1秒K线(OHLCV), 供CEO去币安网站核对。
字段与币安1s K线一致: 开/高/低/收/成交量。时间给北京时间(币安网页默认时区)+UTC毫秒双列。
输出: K线1s_AKEUSDT.csv / K线1s_BULLAUSDT.csv
"""
import csv, os, datetime

TZ8 = datetime.timezone(datetime.timedelta(hours=8))
HERE = os.path.dirname(os.path.abspath(__file__))
TICKS = os.path.join(HERE, 'ticks')
DAYS = ['2026-08-31', '2026-09-01', '2026-09-02', '2026-09-03', '2026-09-04', '2026-09-05', '2026-09-06']
COINS = ['AKEUSDT', 'BULLAUSDT']

for sym in COINS:
    secs = {}  # s -> [o,h,l,c,vol,n]
    for day in DAYS:
        fp = os.path.join(TICKS, '%s_%s.csv' % (sym, day))
        if not os.path.exists(fp):
            print('!! 缺', fp); continue
        with open(fp, newline='') as f:
            rd = csv.reader(f)
            for r in rd:
                if len(r) < 7 or not r[1][0].isdigit():
                    continue
                try:
                    p = float(r[1]); q = float(r[2]); ts = int(r[5])
                except ValueError:
                    continue
                s = ts // 1000
                e = secs.get(s)
                if e is None:
                    secs[s] = [p, p, p, p, q, 1]
                else:
                    if p > e[1]: e[1] = p
                    if p < e[2]: e[2] = p
                    e[3] = p
                    e[4] += q
                    e[5] += 1
    out_fp = os.path.join(HERE, 'K线1s_%s.csv' % sym)
    n = 0
    with open(out_fp, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['北京时间(UTC+8)', 'UTC毫秒', '开', '高', '低', '收', '成交量(币)', '成交笔数'])
        for s in sorted(secs):
            o, h, l, c, v, n_tr = secs[s]
            t = datetime.datetime.fromtimestamp(s, TZ8)
            w.writerow([t.strftime('%Y-%m-%d %H:%M:%S'), s * 1000, o, h, l, c, '%.4f' % v, n_tr])
            n += 1
    sz = os.path.getsize(out_fp) / 1048576
    print('%s: %d 根1s K线 -> %s (%.1f MB)' % (sym, n, out_fp, sz))
    # 打印两个大针秒供直接核对
    probes = {'AKEUSDT': ['2026-09-03 05:44:11', '2026-09-05 10:50:45'],
              'BULLAUSDT': ['2026-09-05 10:59:14']}
    for pt in probes.get(sym, []):
        dt = datetime.datetime.strptime(pt, '%Y-%m-%d %H:%M:%S').replace(tzinfo=TZ8)
        e = secs.get(int(dt.timestamp()))
        if e:
            print('  核对锚点 %s: 开%.6g 高%.6g 低%.6g 收%.6g 量%.0f 笔数%d' % (pt, e[0], e[1], e[2], e[3], e[4], e[5]))
        else:
            print('  核对锚点 %s: 无数据' % pt)
