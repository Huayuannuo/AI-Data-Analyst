#!/bin/bash

# AI Data Analyst 前端启动脚本
# 用法: ./start-frontend.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/frontend"

if [ ! -d node_modules ]; then
  echo "[INFO] 未检测到 node_modules，请先执行 npm install"
fi

npm run dev -- --host 0.0.0.0
