#!/bin/bash

# AI Data Analyst 后端启动脚本
# 用法: ./start-backend.sh
# 说明:
# 1) 自动拉起 AI Data Analyst 基础依赖服务（PostgreSQL / Redis / Milvus / ES / MinIO / etcd）
# 2) 在本机终端启动后端 FastAPI 服务

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_docker() {
  if ! docker info > /dev/null 2>&1; then
    log_error "Docker 未运行，请先启动 Docker Desktop / Docker Engine"
    exit 1
  fi
}

start_services() {
  log_info "正在启动 AI Data Analyst 基础依赖服务..."
  docker compose -f docker-compose.yml up -d ai-data-analyst-postgres ai-data-analyst-redis ai-data-analyst-etcd ai-data-analyst-minio ai-data-analyst-milvus ai-data-analyst-elasticsearch
  log_success "AI Data Analyst 基础依赖服务已启动"
}

wait_for_postgres() {
  log_info "等待 PostgreSQL 就绪（localhost:5433）..."
  local max_attempts=60
  local attempt=1

  while [ "$attempt" -le "$max_attempts" ]; do
    if python3 - <<'PY' >/dev/null 2>&1
import socket
import sys
sock = socket.socket()
sock.settimeout(1)
try:
    sock.connect(("127.0.0.1", 5433))
except Exception:
    sys.exit(1)
finally:
    sock.close()
PY
    then
      log_success "PostgreSQL 已就绪"
      return 0
    fi

    sleep 1
    attempt=$((attempt + 1))
  done

  log_error "PostgreSQL 启动超时，请先检查 ai-data-analyst-postgres 容器状态"
  exit 1
}

run_backend() {
  export POSTGRES_HOST=localhost
  export POSTGRES_PORT=5433
  export POSTGRES_USER=postgres
  export POSTGRES_PASSWORD=postgres123
  export POSTGRES_DB=industry_assistant
  export REDIS_HOST=localhost
  export REDIS_PORT=6380
  export REDIS_PASSWORD=
  export MILVUS_HOST=localhost
  export MILVUS_PORT=19531

  log_info "正在启动后端服务..."
  log_warning "提示: 关闭后端请直接在当前终端按 Ctrl + C"
  cd backend
  python3 app/app_main.py
}

case "${1:-start}" in
  start)
    check_docker
    start_services
    wait_for_postgres
    run_backend
    ;;
  *)
    echo "用法: $0 [start]"
    ;;
esac
