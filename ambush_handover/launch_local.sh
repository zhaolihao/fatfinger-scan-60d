#!/bin/bash
# 本地启动脚本（三套并行）
# 用法: bash launch_local.sh [react136|resident20|resident61|all]
# 默认 all = 启动三套

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
set -a && . ./.env && set +a

LOGDIR="logs"
mkdir -p "$LOGDIR"

start_react136() {
    LOG="$LOGDIR/react136_$(date +%Y%m%d_%H%M%S).log"
    python3 -u ambush_basket_local.py \
        --symbols-file planR_136.json \
        --mode react \
        --notional 10 \
        --depth 0.08 \
        --reb-anchor \
        --reb 0.06 \
        --anchor-sec 1 \
        --anchor-jump-gate 0 \
        > "$LOG" 2>&1 &
    echo "react136 started PID=$! LOG=$LOG"
}

start_resident20() {
    LOG="$LOGDIR/resident20_$(date +%Y%m%d_%H%M%S).log"
    python3 -u ambush_basket_local.py \
        --symbols-file resident20_draft.json \
        --mode resident \
        --notional 10 \
        --depth 0.08 \
        --reb-anchor \
        --reb 0.06 \
        --anchor-sec 1 \
        --anchor-jump-gate 0 \
        > "$LOG" 2>&1 &
    echo "resident20 started PID=$! LOG=$LOG"
}

start_resident61() {
    LOG="$LOGDIR/resident61_$(date +%Y%m%d_%H%M%S).log"
    python3 -u ambush_basket_local.py \
        --symbols-file resident61_draft.json \
        --mode resident \
        --notional 5 \
        --depth 0.08 \
        --reb-anchor \
        --reb 0.06 \
        --anchor-sec 1 \
        --anchor-jump-gate 0 \
        > "$LOG" 2>&1 &
    echo "resident61 started PID=$! LOG=$LOG"
}

case "${1:-all}" in
    react136)   start_react136 ;;
    resident20) start_resident20 ;;
    resident61) start_resident61 ;;
    all)
        start_react136
        start_resident20
        start_resident61
        ;;
    *) echo "用法: bash launch_local.sh [react136|resident20|resident61|all]" ;;
esac
