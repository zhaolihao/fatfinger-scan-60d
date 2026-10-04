#!/bin/bash
cd "$(dirname "$0")"

# A组 react 136币（固定阈值0.08，去掉动态阈值）
nohup python3 ambush_basket_fapi.py \
  --symbols-file planR_136.json --notional 10 --lev 0 \
  --cmp --mode react --min-depth 0.08 --depth 0.08 \
  --reb-anchor --anchor-sec 1 --anchor-jump-gate 0.08 \
  > react_out.log 2>&1 &
echo "react PID=$!"

# B组 resident 20币
nohup python3 ambush_basket_fapi.py \
  --symbols-file resident20_draft.json --notional 10 --lev 0 \
  --tp 0.05 --mode resident --resident-depth 0.08 \
  --reb-anchor --anchor-sec 1 --anchor-jump-gate 0.08 \
  > resident20_out.log 2>&1 &
echo "resident20 PID=$!"

# C组 resident 61币
nohup python3 ambush_basket_fapi.py \
  --symbols-file resident61_draft.json --mode resident \
  --instance res61 --resident-depth 0.08 \
  --notional 5 --lev 0 --tp 0.05 \
  --reb-anchor --anchor-sec 1 --anchor-jump-gate 0.08 \
  > resident61_out.log 2>&1 &
echo "resident61 PID=$!"
