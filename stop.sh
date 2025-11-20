#!/bin/bash

# 资产同步系统停止脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="$SCRIPT_DIR/asset_sync.pid"

# 检查PID文件是否存在
if [ ! -f "$PID_FILE" ]; then
    echo "系统未在运行 (找不到PID文件)"
    exit 1
fi

PID=$(cat "$PID_FILE")

# 检查进程是否存在
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "系统未在运行 (进程不存在)"
    rm -f "$PID_FILE"
    exit 1
fi

# 停止进程
echo "停止资产同步系统 (PID: $PID)..."
kill "$PID"

# 等待进程结束
for i in {1..10}; do
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "系统已停止"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
done

# 如果还没停止,强制杀死
echo "强制停止系统..."
kill -9 "$PID"
rm -f "$PID_FILE"
echo "系统已强制停止"
