import os, time, json

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "verify_divergence_results.json")
LOG = os.path.join(HERE, "focus_run2.log")
CAN = os.path.join(HERE, "divergence_candidates.json")

def mtime(p):
    try:
        return os.path.getmtime(p)
    except OSError:
        return 0

old_res = mtime(RES)
old_can = mtime(CAN)
last_log = mtime(LOG)
last_progress = time.time()
timeout = 45 * 60  # 45分钟无进展报警

while True:
    # 结果更新 = 扫描+verify全完成
    if mtime(RES) > old_res:
        print("=== FOCUS DONE @", time.strftime("%H:%M:%S"), "===")
        try:
            r = json.load(open(RES))
            real = [x for x in r if x.get("verdict") == "真"]
            weak = [x for x in r if x.get("verdict") == "弱"]
            print("盲区aggTrades终审: 候选=%d 真=%d 弱=%d" % (len(r), len(real), len(weak)))
            for x in real[:25]:
                print("  真:", x.get("base"), x.get("leg"), x.get("anchor_iso"), round(x.get("peak_pct", 0), 2), "%")
            for x in weak[:15]:
                print("  弱:", x.get("base"), x.get("leg"), x.get("anchor_iso"), round(x.get("peak_pct", 0), 2), "%")
        except Exception as e:
            print("读取结果失败:", e)
        break
    # 候选文件更新 = scan阶段完成, 进入verify
    if mtime(CAN) > old_can:
        print("SCAN阶段完成, 进入aggTrades终审 @", time.strftime("%H:%M:%S"))
        old_can = mtime(CAN)
        last_progress = time.time()
    # 日志有进展 = 还在拉
    if mtime(LOG) > last_log:
        last_log = mtime(LOG)
        last_progress = time.time()
    if time.time() - last_progress > timeout:
        print("ALERT: 超过45分钟无进度, 疑似代理断/卡死 @", time.strftime("%H:%M:%S"))
        break
    time.sleep(20)
print("MONITOR_DONE")
