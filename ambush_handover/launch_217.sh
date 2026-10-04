#!/bin/bash
cd "$(dirname "$0")"

# 217币合并版（react模式，固定阈值0.08，10U/侧）
nohup python3 ambush_basket_local.py \
  --symbols-file planR_217.json --notional 10 --lev 0 \
  --cmp --mode react --min-depth 0.08 --depth 0.08 \
  --reb-anchor --anchor-sec 1 --anchor-jump-gate 0.08 \
  > logs/react217_$(date +%Y%m%d_%H%M%S).log 2>&1 &
echo "react217 PID=$!"
