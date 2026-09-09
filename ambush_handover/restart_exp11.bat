@echo off
REM  单实例重启: exp11 11币 (秒级跟随 3s + 跳变闸门 8%, depth 15%)
cd /d "%~dp0"

if exist resident_exp11_out.tmp copy /Y resident_exp11_out.tmp logs\resident_exp11_out_prev.log >nul

REM 前台跑(经 _launch_wmi.py 由 WMI 脱离沙箱拉起, 进程稳; 不用 start /B 以免 reparenting 歧义)
REM 注意: PY 不要再带引号包一层 —— set PY="..." 后再 "%PY%" 会展开成 ""C:..."" → cmd 报 '""' 不是命令
"C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe" ambush_basket.py --symbols-file experimental_20_clean.json --mode resident --instance exp11 --resident-depth 0.15 --reb-anchor --reb 0.05 --ta 5.0 --route-branch --branch-tr 0.02 --branch-cut 0.02 --branch-hold 900 --notional 3 --lev 0 --tp 0.03 --anchor-sec 1 --anchor-jump-gate 0 > resident_exp11_out.tmp 2>&1

exit
