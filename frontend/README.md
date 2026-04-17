# AI Data Analyst - Frontend

## 快速开始

### 环境要求
- Node.js >= 18
- npm >= 9

### 安装与运行

```bash
# 1. 安装依赖
npm install

# 2. 启动开发服务器
npm run dev
```

启动成功后访问 http://localhost:5173/

### 推荐方式

如果你想按“前后端两个终端”启动，直接在仓库根目录执行：

```bash
cd /home/dev/projects/Agent/AI Data Analyst
./start-frontend.sh
```

后端请在另一个终端执行：

```bash
cd /home/dev/projects/Agent/AI Data Analyst
./start-backend.sh
```

### 常见问题

#### macOS/Linux 权限问题

如果遇到 `Permission denied` 错误，运行：

```bash
chmod +x node_modules/.bin/*
```

然后重新执行 `npm run dev`

#### Windows 用户

如遇权限问题，请以管理员身份运行终端。

---

## 技术栈

- React 19
- TypeScript
- Vite
- Ant Design 5
- React Router 6

## 目录说明

- `src/`：主线前端代码
- `public/`：静态资源

## 环境变量

前端环境变量示例见仓库根目录的 `frontend/.env.example`，通常包含：

- `VITE_TITLE`
- `VITE_API_BASE`
- `VITE_API_PROXY`

## 可用命令

| 命令 | 说明 |
|------|------|
| `npm run dev` | 启动开发服务器 |
| `npm run build` | 构建生产版本 |
| `npm run preview` | 预览生产版本 |
| `npm run lint` | 运行代码检查 |
