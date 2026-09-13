@echo off
chcp 65001 >nul
REM ============================================================
REM  乌龙指埋伏系统 一键启动（三进程）
REM  用法：双击本 .bat，或在 CMD 里运行
REM  会自动开三个后台进程：react(136币) + resident(旧20币) + 实验(11币)
REM ============================================================
cd /d "%~dp0"

set PY="C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe"

echo [启动] react 136币 实盘...
start "ambush_react" /B %PY% ambush_basket.py --symbols-file planR_136.json --dyn-thresh --notional 10 --lev 0 --cmp --mode react --min-depth 0.08 --depth 0.08 --reb-anchor --anchor-sec 1 --anchor-jump-gate 0.08 > react_out.tmp 2>&1

echo [启动] resident 旧20币 实盘...
start "ambush_resident" /B %PY% ambush_basket.py --symbols-file resident20_draft.json --notional 10 --lev 0 --tp 0.05 --mode resident --resident-depth 0.08 --reb-anchor --anchor-sec 1 --anchor-jump-gate 0.08 > resident_v4_out.tmp 2>&1

echo [启动] 实验 11币 (15%%墙距+回弹锚价5%%+分岔)...
start "ambush_res61" /B %PY% ambush_basket.py --symbols-file resident61_draft.json --mode resident --instance res61 --resident-depth 0.08 --notional 5 --lev 0 --tp 0.05 --reb-anchor --anchor-sec 1 --anchor-jump-gate 0.08 > resident61_out.tmp 2>&1

echo.
echo 三个进程已启动。查看日志：
echo   react     -> logs\ambush_basket_*.log (最新)
echo   resident  -> logs\ambush_basket_*.log (最新)
echo   实验exp11 -> logs\ambush_basket_*.log (最新)
echo.
echo 提示：启动后请确认币安白名单IP与Clash出口IP一致，否则会报-2015挂不了单。
pause
