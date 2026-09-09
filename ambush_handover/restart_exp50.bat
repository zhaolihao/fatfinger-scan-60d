@echo off
REM  单实例重启: exp50 50币 (秒级跟随 3s + 跳变闸门 8%, depth 15%)
REM  注意: 这是 100 张条件单的大户, 重启期间订单数先降后升, 必须单独重启不与他人并行
cd /d "%~dp0"

if exist resident_exp50_out.tmp copy /Y resident_exp50_out.tmp logs\resident_exp50_out_prev.log >nul

REM 前台跑(经 _launch_wmi.py 由 WMI 脱离沙箱拉起, 进程稳; 不用 start /B 以免 reparenting 歧义)
REM 注意: 不要 set PY="..." 再 "%PY%" —— 双层引号展开成 ""C:..."" → cmd 报 '""' 不是命令
"C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe" ambush_basket.py --symbols-file experimental_50_extra.json --mode resident --instance exp50 --resident-depth 0.15 --reb-anchor --reb 0.05 --ta 5.0 --route-branch --branch-tr 0.02 --branch-cut 0.02 --branch-hold 900 --notional 5 --lev 0 --tp 0.03 --anchor-sec 1 --anchor-jump-gate 0 > resident_exp50_out.tmp 2>&1

exit
