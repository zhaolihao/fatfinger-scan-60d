#!/bin/bash
cd "$(dirname "$0")"

# 发SIGTERM让脚本优雅退出
PIDS=$(pgrep -f "ambush_basket_fapi.py")
if [ -z "$PIDS" ]; then
    echo "无运行中的进程"
    exit 0
fi

for PID in $PIDS; do
    kill -TERM $PID 2>/dev/null
    echo "已发送SIGTERM到 PID=$PID"
done

# 等待退出（最多60s）
for i in $(seq 1 12); do
    sleep 5
    REMAIN=$(pgrep -f "ambush_basket_fapi.py" | head -1)
    if [ -z "$REMAIN" ]; then
        echo "所有进程已退出"
        exit 0
    fi
    echo "等待退出... PID=$REMAIN"
done

# 超时强制kill
echo "超时，强制kill"
for PID in $PIDS; do
    kill -9 $PID 2>/dev/null
done
sleep 2
echo "done"
