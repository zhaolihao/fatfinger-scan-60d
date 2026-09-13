@echo off
set LOG="C:\Users\Administrator\WorkBuddy\2026-08-13-19-52-29\ambush_handover\_debug_launch.txt"
echo step1_start %date% %time% >> %LOG%
cd /d "%~dp0"
echo step2_cd_ok >> %LOG%
set PY="C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe"
echo step3_py_set >> %LOG%
start "ambush_res61" /B %PY% ambush_basket.py --symbols-file resident61_draft.json --mode resident --instance res61 --resident-depth 0.08 --reb-anchor --notional 5 --lev 0 --tp 0.05 > resident61_out.tmp 2>&1
echo step4_start_done errlvl=%errorlevel% >> %LOG%
exit
