@echo off
REM  单实例重启: exp11 11币 (秒级跟随 3s + 跳变闸门 8%, depth 15%)
cd /d "%~dp0"

if exist resident_exp11_out.tmp copy /Y resident_exp11_out.tmp logs\resident_exp11_out_prev.log >nul

REM 前台跑(经 _launch_wmi.py 由 WMI 脱离沙箱拉起, 进程稳; 不用 start /B 以免 reparenting 歧义)
REM 注意: PY 不要再带引号包一层 —— set PY="..." 后再 "%PY%" 会展开成 ""C:..."" → cmd 报 '""' 不是命令
"C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe" ambush_basket.py --symbols-file resident61_draft.json --mode resident --instance res61 --resident-depth 0.08 --notional 5 --lev 0 --tp 0.05 --reb-anchor --anchor-sec 1 --anchor-jump-gate 0 > resident61_out.tmp 2>&1

exit
