#!/bin/bash

# AI Data Analyst 一键启动脚本
# 用法: ./start.sh [start|stop|restart|status|logs]

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

start_stack() {
  log_info "正在启动 AI Data Analyst 独立版 Docker 栈..."
  docker compose -f docker-compose.yml up -d --build
  log_success "AI Data Analyst 独立版已启动"
  echo ""
  echo "访问地址："
  echo "  - 后端 API: http://localhost:8001"
  echo "  - PostgreSQL: localhost:5433"
  echo "  - Redis: localhost:6380"
  echo "  - Milvus: localhost:19531"
  echo "  - Elasticsearch: localhost:1201"
  echo "  - MinIO Console: http://localhost:9003"
  echo ""
  echo "前端建议本地启动："
  echo "  cd frontend && npm run dev"
}

stop_stack() {
  log_info "正在停止 AI Data Analyst 独立版 Docker 栈..."
  docker compose -f docker-compose.yml down
  log_success "AI Data Analyst 独立版已停止"
}

status_stack() {
  docker compose -f docker-compose.yml ps
}

logs_stack() {
  if [ -n "${2:-}" ]; then
    docker compose -f docker-compose.yml logs -f --tail=100 "$2"
  else
    docker compose -f docker-compose.yml logs -f --tail=100
  fi
}

case "${1:-help}" in
  start)
    check_docker
    start_stack
    ;;
  stop)
    check_docker
    stop_stack
    ;;
  restart)
    check_docker
    stop_stack
    start_stack
    ;;
  status)
    check_docker
    status_stack
    ;;
  logs)
    check_docker
    logs_stack "$@"
    ;;
  help|--help|-h)
    echo "用法: $0 [start|stop|restart|status|logs]"
    ;;
  *)
    echo "用法: $0 [start|stop|restart|status|logs]"
    ;;
esac
