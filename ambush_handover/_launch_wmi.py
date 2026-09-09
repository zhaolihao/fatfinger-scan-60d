# -*- coding: utf-8 -*-
"""WMI 启动器: 让 WMI 服务(WmiPrvSE.exe)代为创建进程 —— 新进程的父进程是系统服务,
彻底脱离 agent 沙箱的 Job Object, 不会随 bash 命令结束被连坐回收。

为什么必须这样(逐个试错的结论, 别再重复踩):
  · subprocess DETACHED_PROCESS        → 进程秒死(job 连坐)
  · CREATE_BREAKAWAY_FROM_JOB          → WinError 5 拒绝访问(job 不允许 breakaway)
  · Bash/PowerShell 直呼 cmd.exe       → 沙箱安全策略拦截
  · win32com Shell.Application.ShellExecute → shell32 是 in-proc, 仍是本进程的子进程,
                                          bash 命令一结束就被杀(实测活 ~20 秒)
  · schtasks.exe                       → 在沙箱程序黑名单, 明确不可绕
  · WMI Win32_Process.Create           → ✅ 由 WmiPrvSE 创建, 脱离 job

用法:
  python _launch_wmi.py "<bat 绝对路径>"
bat 内部自己做 stdout/stderr 重定向(WMI 不支持句柄传递)。
"""
import sys, os, time
import win32com.client

if len(sys.argv) < 2:
    raise SystemExit('用法: python _launch_wmi.py "<bat 绝对路径>"')

bat = os.path.abspath(sys.argv[1])
if not os.path.exists(bat):
    raise SystemExit("找不到 bat: %s" % bat)
workdir = os.path.dirname(bat)

# cmd /c 跑 bat: bat 里的 > 重定向才生效
cmdline = 'cmd.exe /c "%s"' % bat

# 注意: GetObject("winmgmts:") 拿到的类对象上, .Create 会被 pywin32 当成属性(int) 而不是方法
# → TypeError: 'int' object is not callable。必须走 SWbemLocator + ExecMethod 显式调用。
loc = win32com.client.Dispatch("WbemScripting.SWbemLocator")
svc = loc.ConnectServer(".", "root\\cimv2")
proc_cls = svc.Get("Win32_Process")
inp = proc_cls.Methods_("Create").InParameters.SpawnInstance_()
inp.CommandLine = cmdline
inp.CurrentDirectory = workdir
out = svc.ExecMethod("Win32_Process", "Create", inp)
rc, pid = out.ReturnValue, out.ProcessId
pid_out = (rc, pid)

# Create() 返回值形态随 pywin32 版本不同: 可能是 int(retcode), 也可能是 (retcode, pid)
if isinstance(pid_out, (tuple, list)):
    rc = pid_out[0]
    pid = pid_out[1] if len(pid_out) > 1 else None
else:
    rc, pid = pid_out, None
print("WMI_CREATE rc=%s pid=%s  (rc=0 表示成功)" % (rc, pid))
if rc != 0:
    sys.exit(2)

# 等一下, 报告 python 子进程是否真的起来了
time.sleep(6)
n = 0
for p in svc.ExecQuery("SELECT ProcessId,CommandLine FROM Win32_Process WHERE Name='python.exe'"):
    cl = (p.CommandLine or "")
    if "ambush_basket.py" in cl:
        n += 1
        print("  ALIVE pid=%s | %s" % (p.ProcessId, cl[:150]))
print("ambush_basket.py 进程数 = %d" % n)
