@echo off
REM  单实例重启: resident 20币 (秒级跟随 3s + 跳变闸门 8%)
REM
REM  启动方式(踩过的坑, 务必照做):
REM    python _com_launch.py "<本bat绝对路径>"
REM    —— 经 explorer(Shell.Application) 代为启动才能脱离 agent 沙箱的 Job Object;
REM       DETACHED_PROCESS 不够(进程秒死), CREATE_BREAKAWAY_FROM_JOB 被拒(WinError 5),
REM       从 Bash/PowerShell 直呼 cmd.exe 被安全策略拦。
REM  本 bat 内**不要用 start /B**: 那样 cmd 立刻 exit, 子进程连一行日志都写不出来(0字节 tmp)。
REM  直接前台跑 python, cmd 隐藏窗口陪跑, 进程就稳。
REM
REM  旧日志归档请在杀进程**之前**手工做: cp resident_v4_out.tmp logs/resident_v4_out_prev.log
REM  (放本 bat 里会被上一次失败启动留下的 0 字节 tmp 覆盖成空, 丢掉孤儿单对账线索)
cd /d "%~dp0"

"C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe" ambush_basket.py --symbols-file resident20_draft.json --notional 10 --lev 0 --tp 0.05 --mode resident --anchor-sec 1 --anchor-jump-gate 0 > resident_v4_out.tmp 2>&1
