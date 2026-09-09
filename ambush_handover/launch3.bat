@echo off
cd /d "%~dp0"
set PY="C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe"

start "ambush_react" /B %PY% ambush_basket.py --symbols-file planR_136.json --dyn-thresh --notional 10 --lev 0 --cmp --mode react --anchor-sec 1 --anchor-jump-gate 0.08 > react_out.tmp 2>&1

start "ambush_resident" /B %PY% ambush_basket.py --symbols-file resident20_draft.json --notional 10 --lev 0 --tp 0.05 --mode resident --anchor-sec 3 --anchor-jump-gate 0.08 > resident_v4_out.tmp 2>&1

start "ambush_exp11" /B %PY% ambush_basket.py --symbols-file experimental_20_clean.json --mode resident --instance exp11 --resident-depth 0.15 --reb-anchor --reb 0.05 --ta 5.0 --route-branch --branch-tr 0.02 --branch-cut 0.02 --branch-hold 900 --notional 3 --lev 0 --tp 0.03 --anchor-sec 3 --anchor-jump-gate 0.08 > resident_exp11_out.tmp 2>&1

exit
