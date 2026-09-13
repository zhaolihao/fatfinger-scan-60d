@echo off
cd /d "%~dp0"
set PY="C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe"

start "ambush_res61" /B %PY% ambush_basket.py --symbols-file resident61_draft.json --mode resident --instance res61 --resident-depth 0.08 --notional 5 --lev 0 --tp 0.05 --reb-anchor --anchor-sec 1 --anchor-jump-gate 0.08 > resident61_out.tmp 2>&1

exit
