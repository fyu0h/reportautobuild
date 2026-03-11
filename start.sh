#!/bin/bash
# 生产模式启动脚本
# 用法: bash start.sh

cd "$(dirname "$0")"

# 激活虚拟环境
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# 安装依赖（首次）
pip install -r requirements.txt -q
pip install gunicorn -q

echo "=================================="
echo "  国际移民管理咨询工作流"
echo "  http://0.0.0.0:5100"
echo "=================================="

# 用 gunicorn 启动（4 worker，支持并发）
exec gunicorn app:app \
    --bind 0.0.0.0:5100 \
    --workers 4 \
    --timeout 300 \
    --access-logfile access.log \
    --error-logfile error.log \
    --log-level info
