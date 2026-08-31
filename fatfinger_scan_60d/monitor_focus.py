import os, time, json, subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "verify_divergence_results.json")
SCAN_PID = 13652

def mtime(p):
    try:
        return os.path.getmtime(p)
    except OSError:
        return 0

old = mtime(RES)
start = time.time()
last_progress = mtime(os.path.join(HERE, "focus_run2.log"))
timeout = 40 * 60  # 40分钟无进展则报警退出

while True:
    # 扫描进程是否还在
    alive = False
    try:
        out = subprocess.run(["tasklist"], capture_output=True, text=True).stdout
        alive = str(SCAN_PID) in out
    except Exception:
        pass
    new = mtime(RES)
    if new > old:
        print("=== FOCUS DONE @", time.strftime("%H:%M:%S"), "===")
        try:
            r = json.load(open(RES))
            real = [x for x in r if x.get("verdict") == "真"]
            weak = [x for x in r if x.get("verdict") == "弱"]
            print("盲区aggTrades终审: 候选=%d 真=%d 弱=%d" % (len(r), len(real), len(weak)))
            for x in real[:20]:
                print("  真:", x.get("base"), x.get("leg"), x.get("anchor_iso"), round(x.get("peak_pct", 0), 2), "%")
            for x in weak[:15]:
                print("  弱:", x.get("base"), x.get("leg"), x.get("anchor_iso"), round(x.get("peak_pct", 0), 2), "%")
        except Exception as e:
            print("读取结果失败:", e)
        break
    if not alive:
        # 进程退出但结果没更新 -> 可能卡死或代理断
        print("扫描进程已退出但结果未更新(可能代理断/卡死) @", time.strftime("%H:%M:%S"))
        break
    lp = mtime(os.path.join(HERE, "focus_run2.log"))
    if lp > last_progress:
        last_progress = lp
        start = time.time()  # 有进展，重置超时
    if time.time() - start > timeout:
        print("ALERT: 超过40分钟无进度，疑似卡死 @", time.strftime("%H:%M:%S"))
        break
    time.sleep(15)
print("MONITOR_DONE")
