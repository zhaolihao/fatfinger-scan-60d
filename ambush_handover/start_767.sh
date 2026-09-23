#!/bin/bash
# 767币启动脚本（之前一直用的）
cd "$(dirname "$0")"
set -a && . ./.env && set +a

LOGDIR="logs"
mkdir -p "$LOGDIR"

LOG="$LOGDIR/ambush_basket_$(date +%Y%m%d_%H%M%S).log"

python3 -u ambush_basket_local.py \
    --symbols-file planR_all.json \
    --mode react \
    --notional 10 \
    --depth 0.06 \
    --cmp \
    --reb-anchor \
    --reb 0.02 \
    --anchor-sec 1 \
    > "$LOG" 2>&1 &

echo "767币脚本已启动 PID=$! LOG=$LOG"
