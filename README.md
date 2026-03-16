# MyStock Web Project

这是一个基于 Python FastAPI 和 MySQL 的股票数据分析 Web 项目。

## 功能特性

- **首页 / 大盘概览**: 展示上证、深证、创业板、恒生指数，热门个股涨跌排行。
- **个股详情页**: K 线图（日/周/月），基本信息。
- **板块行情**: 展示板块涨跌（模拟数据）。
- **行情总览**: 股票列表。

## 技术栈

- Backend: FastAPI
- Database: MySQL 5.7 (Docker)
- ORM: SQLAlchemy
- Data Source: Yahoo Finance JSON API
- Frontend: Jinja2 Templates, Bootstrap 5, ECharts

## 快速开始

### 1. 启动数据库

确保已安装 Docker Desktop。

```bash
docker-compose up -d
```

这将启动 MySQL 5.7 容器，端口映射为 3307。

### 2. 安装依赖

建议使用 Python 3.9+。

```bash
pip install -r requirements.txt
```

### 3. 运行应用

**推荐方式 1: 使用 run.py 脚本 (最简单)**

直接运行根目录下的 `run.py` 脚本：

```bash
python run.py
```

**推荐方式 2: 使用 uvicorn 命令**

```bash
uvicorn app.main:app --reload
```

访问浏览器: http://127.0.0.1:8000

## 项目结构

- `app/`: 应用源码
  - `models.py`: 数据库模型
  - `database.py`: 数据库连接
  - `services/`: yfinance 数据获取逻辑
  - `routers/`: 路由
  - `templates/`: HTML 模板
  - `static/`: 静态文件
- `docker-compose.yml`: MySQL 容器配置

## 注意事项

- 初次运行会自动创建数据库表。
- 首页加载时会尝试从 Yahoo Finance 获取数据，可能会有延迟。
- 若日志出现 `request blocked 403`，通常是网络出口被 Yahoo 拦截。
- 可在启动前配置代理环境变量：

```bash
set HTTPS_PROXY=http://127.0.0.1:7890
set HTTP_PROXY=http://127.0.0.1:7890
python run.py
```
