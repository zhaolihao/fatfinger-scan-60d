@echo off
cd /d "%~dp0"
set PY="C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe"

REM  react 136币 · 秒级锚价(只接1秒内真乌龙, 阴跌不接)
REM  ANCHOR_SEC=1 锚价每秒刷新; ANCHOR_JUMP_GATE=0.08 针尖不污染锚价
REM  用法: 任务计划程序新建任务指向此bat(与 AmbushLaunch3 解耦, 单独常驻)
start "ambush_react" /B %PY% ambush_basket.py --symbols-file planR_136.json --dyn-thresh --notional 10 --lev 0 --cmp --mode react --min-depth 0.08 --depth 0.08 --reb-anchor --anchor-sec 1 --anchor-jump-gate 0.08 > react_out.tmp 2>&1

exit
