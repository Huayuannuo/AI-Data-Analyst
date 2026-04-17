# AI Data Analyst - Backend

后端服务说明。

## 启动方式

### 方式 A：通过根目录脚本启动（推荐）

```bash
cd ./
./start-backend.sh
```

默认会先启动 Docker 依赖服务，再在当前终端启动 FastAPI 后端，监听 `http://127.0.0.1:8000`。

### 方式 B：通过 Docker 一键启动

```bash
cd ./
./start.sh start
```

后端将运行在 `http://localhost:8001`。

## 数据初始化

推荐在启动 Docker 数据库后执行：

```bash
cd ./backend/app
POSTGRES_HOST=localhost POSTGRES_PORT=5433 POSTGRES_USER=postgres POSTGRES_PASSWORD=postgres123 POSTGRES_DB=industry_assistant python3 scripts/seed_radar_demo_data.py
```

## 关闭服务

- 如果是方式 A：在启动后端的终端按 `Ctrl + C`
- 如果是方式 B：执行 `./start.sh stop`
