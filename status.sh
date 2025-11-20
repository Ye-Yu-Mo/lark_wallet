#!/bin/bash

# 资产同步系统状态查看脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="$SCRIPT_DIR/asset_sync.pid"
LOG_FILE="$SCRIPT_DIR/logs/system.log"

echo "=== 资产同步系统状态 ==="
echo ""

# 检查PID文件
if [ ! -f "$PID_FILE" ]; then
    echo "状态: 未运行 (找不到PID文件)"
    exit 0
fi

PID=$(cat "$PID_FILE")

# 检查进程是否存在
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "状态: 未运行 (进程不存在)"
    echo "PID文件: $PID_FILE (过期)"
    exit 0
fi

# 获取进程信息
echo "状态: 运行中"
echo "PID: $PID"
echo ""

# 显示进程详情
echo "进程信息:"
ps -p "$PID" -o pid,ppid,start,etime,rss,vsz,command

echo ""
echo "日志文件: $LOG_FILE"

# 显示最后几行日志
if [ -f "$LOG_FILE" ]; then
    echo ""
    echo "=== 最近日志 (最后20行) ==="
    tail -n 20 "$LOG_FILE"
fi
