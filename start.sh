#!/bin/bash

# 资产同步系统启动脚本
# 用于在后台静默运行系统

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="$SCRIPT_DIR/asset_sync.pid"
LOG_FILE="$SCRIPT_DIR/logs/system.log"

# 确保日志目录存在
mkdir -p logs

# 检查是否已经在运行
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "系统已在运行 (PID: $PID)"
        exit 1
    else
        echo "删除过期的PID文件"
        rm -f "$PID_FILE"
    fi
fi

# 启动系统
echo "启动资产同步系统..."
nohup uv run python main.py > "$LOG_FILE" 2>&1 &

# 保存PID
echo $! > "$PID_FILE"

echo "系统已启动 (PID: $(cat $PID_FILE))"
echo "日志文件: $LOG_FILE"
echo ""
echo "使用以下命令:"
echo "  ./stop.sh   - 停止系统"
echo "  ./status.sh - 查看状态"
echo "  tail -f $LOG_FILE - 查看日志"
