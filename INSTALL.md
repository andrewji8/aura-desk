# Aura Desk 安装说明 / Installation Guide

> AI 虚拟客服平台 · AI Virtual Customer Service Platform
> 
> 本项目为 **Poetry 2.1.3 monorepo**，包含 3 个 workspace 成员。安装前请仔细阅读【避坑指南 / Pitfalls】章节。
> This is a **Poetry 2.1.3 monorepo** with 3 workspace members. Read the 【Pitfalls】section carefully before installing.

---

## 目录 / Table of Contents

1. [项目结构 / Project Structure](#1-项目结构--project-structure)
2. [环境要求 / Requirements](#2-环境要求--requirements)
3. [安装步骤 / Installation Steps](#3-安装步骤--installation-steps)
4. [配置说明 / Configuration](#4-配置说明--configuration)
5. [数据库 / Database](#5-数据库--database)
6. [运行服务 / Running the Service](#6-运行服务--running-the-service)
7. [运行测试 / Running Tests](#7-运行测试--running-tests)
8. [前端使用 / Frontend Usage](#8-前端使用--frontend-usage)
9. [Docker 部署 / Docker Deployment](#9-docker-部署--docker-deployment)
10. [避坑指南 / Pitfalls](#10-避坑指南--pitfalls)
11. [常见问题 / FAQ](#11-常见问题--faq)

---

## 1. 项目结构 / Project Structure

```
aura-desk/
├── pyproject.toml              # 根聚合配置（Poetry 2.1.3，无 workspace 表）
├── poetry.lock                 # 单一 lock 文件，覆盖所有成员
├── .env.example                # 环境变量模板（复制为 .env 后填写真实值）
├── .gitignore                  # 已忽略 .env、.venv、coverage.xml 等
│
├── apps/
│   └── gateway/                # ⭐ 主服务：FastAPI + WebSocket + 流式管道
│       ├── pyproject.toml
│       ├── Dockerfile          # 3 阶段构建（deps → builder → runner）
│       ├── src/aura_gateway/
│       │   ├── main.py         # FastAPI 应用工厂 / app factory
│       │   ├── core/
│       │   │   ├── config.py   # 所有配置项（AURA_ 前缀）
│       │   │   ├── llm.py      # 进程级 httpx 客户端
│       │   │   ├── logging.py  # structlog 配置
│       │   │   └── database.py # SQLAlchemy async 引擎
│       │   ├── pipeline/
│       │   │   └── streaming.py # ASR → LLM → TTS 核心管道
│       │   └── api/
│       │       └── websocket.py # WebSocket 路由 + receiver/sender 任务
│       ├── static/
│       │   └── index.html      # 单文件前端（Vanilla JS + Tailwind CDN）
│       └── tests/              # pytest 测试套件
│
├── packages/
│   ├── common/                 # 公共基础：config、logging、db session、错误体系
│   │   └── src/aura_common/
│   └── schemas/                # 共享 Pydantic 模型：WebSocket 帧定义
│       └── src/aura_schemas/
│           └── ws.py           # ClientMessage / ServerMessage 等类型
│
└── scripts/                    # 手工脚本（如 test_ws_client.py）
```

### 核心数据流 / Core Data Flow

```
浏览器 Browser
    │  WebSocket JSON（text）+ binary（audio）
    ▼
FastAPI websocket.py
    │  _receiver（验证帧）→ input_queue
    │  _sender（ draining output_queue → 发回浏览器）
    ▼
StreamingPipeline.process_stream()
    │
    ├─ ASR（模拟）   → audio → text
    ├─ LLM（真实）   → text → text_delta 流
    └─ TTS（edge-tts）→ text → audio_chunk 流（MP3 分块）
    ▼
浏览器收到：text_delta + audio_chunk → Web Audio API 解码播放
```

---

## 2. 环境要求 / Requirements

| 依赖项 / Dependency | 最低版本 / Min Version | 说明 / Notes |
|---|---|---|
| Python | **3.11+** | `requires-python = ">=3.11,<4.0"` |
| Poetry | **2.1.3** | 项目严格使用 Poetry 2.1.3 的 schema；`editable = true` 会被拒绝 |
| PostgreSQL | 12+ | 生产环境必选；本地开发可改用 SQLite |
| Node.js | 无需 / Not needed | 前端使用 Tailwind CDN，无需构建步骤 |
| 浏览器 | 现代浏览器 / Modern | 需支持 Web Audio API + WebSocket |

### 验证安装 / Verify Installation

```bash
# 检查 Python 版本 / Check Python version
python --version   # 应显示 3.11.x 或更高

# 检查 Poetry 版本 / Check Poetry version
poetry --version   # 应显示 Poetry 2.1.3
```

---

## 3. 安装步骤 / Installation Steps

### Step 1: 克隆仓库 / Clone Repository

```bash
git clone <repository-url>
cd aura-desk
```

### Step 2: 复制环境变量模板 / Copy Environment Template

```bash
# Windows PowerShell
Copy-Item .env.example .env

# Linux / macOS
cp .env.example .env
```

### Step 3: 填写真实配置 / Fill Real Configuration

编辑 `.env` 文件，填入真实值（详见 [第 4 章](#4-配置说明--configuration)）：

```bash
# 必填：LLM API Key（否则无法对话）
AURA_LLM_API_KEY=sk-你的真实密钥

# 根据实际情况修改
AURA_DATABASE_URL=postgresql+asyncpg://用户名:密码@localhost:5432/aura_desk
```

### Step 4: 安装依赖 / Install Dependencies

```bash
# 方式一：使用 Poetry（推荐 / Recommended）
poetry install

# 方式二：如果 Poetry lock 失败，尝试
poetry lock --no-update
poetry install
```

> ⚠️ **注意 / Note**：`poetry install` 会为整个 monorepo 创建一个共享虚拟环境 `.venv/`，包含所有 workspace 成员的可编辑安装。

### Step 5: 验证安装 / Verify Installation

```bash
# 激活虚拟环境
poetry shell

# 运行测试（验证一切正常）
poetry run pytest apps/gateway/tests/ -v
```

如果看到 `34 passed`，说明安装成功。

---

## 4. 配置说明 / Configuration

所有配置通过 `.env` 文件设置，代码中使用 `AURA_` 前缀读取。

### 完整配置清单 / Full Configuration List

| 环境变量 / Env Variable | 默认值 / Default | 说明 / Description |
|---|---|---|
| `AURA_ENVIRONMENT` | `local` | 运行环境：`local` / `dev` / `staging` / `prod` |
| `AURA_DEBUG` | `false` | 是否开启 FastAPI debug 模式 |
| `AURA_LOG_LEVEL` | `INFO` | 日志级别：`CRITICAL` / `ERROR` / `WARNING` / `INFO` / `DEBUG` |
| `AURA_DATABASE_URL` | `postgresql+asyncpg://aura:aura@localhost:5432/aura_desk` | 数据库连接字符串 |
| `AURA_DB_CONNECT_ON_STARTUP` | `true` | 启动时是否验证数据库连接 |
| `AURA_DB_FAIL_FAST_ON_STARTUP` | `false` | 启动时数据库连接失败是否退出 |
| `AURA_LLM_API_KEY` | `""` | LLM 提供商 API 密钥（**必填**） |
| `AURA_LLM_BASE_URL` | `https://api.deepseek.com/v1` | OpenAI 兼容端点地址 |
| `AURA_LLM_MODEL` | `deepseek-chat` | 模型名称 |
| `AURA_LLM_SYSTEM_PROMPT` | `你是 Aura Desk 的虚拟客服助手...` | 系统提示词 |
| `AURA_LLM_TEMPERATURE` | `0.7` | 生成温度（0.0-2.0） |
| `AURA_LLM_MAX_TOKENS` | `512` | 单次最大生成 token 数 |
| `AURA_LLM_TIMEOUT_CONNECT` | `5.0` | 连接超时（秒） |
| `AURA_LLM_TIMEOUT_READ` | `60.0` | 读取超时（秒） |
| `AURA_LLM_MAX_RETRIES` | `2` | 流式重试次数（仅在首 token 前生效） |
| `AURA_TTS_VOICE` | `zh-CN-XiaoxiaoNeural` | Edge-TTS 语音角色 |
| `AURA_CORS_ORIGINS` | 空 | 允许的跨域来源，逗号分隔 |
| `AURA_PORT` | `8000` | HTTP 服务端口 |

### 切换 LLM 提供商 / Switching LLM Providers

```bash
# DeepSeek（默认）
AURA_LLM_BASE_URL=https://api.deepseek.com/v1
AURA_LLM_MODEL=deepseek-chat

# 智谱 GLM
AURA_LLM_BASE_URL=https://open.bigmodel.cn/api/paas/v4
AURA_LLM_MODEL=glm-4-plus

# 本地 vLLM（无需 API Key）
AURA_LLM_BASE_URL=http://localhost:8000/v1
AURA_LLM_MODEL=your-model-name
AURA_LLM_API_KEY=  # 留空即可
```

### 切换 TTS 语音 / Switching TTS Voice

```bash
# 查看可用语音列表（需先安装 edge-tts）
poetry run edge-tts --list-voices

# 设置语音
AURA_TTS_VOICE=zh-CN-YunxiNeural    # 男声
AURA_TTS_VOICE=zh-CN-XiaoyiNeural   # 女声
AURA_TTS_VOICE=en-US-AriaNeural     # 英文
```

---

## 5. 数据库 / Database

### 方式一：PostgreSQL（生产推荐 / Production Recommended）

```bash
# 创建数据库
createdb aura_desk

# 或在 psql 中执行
CREATE DATABASE aura_desk;

# 确保 .env 中配置正确
AURA_DATABASE_URL=postgresql+asyncpg://aura:aura@localhost:5432/aura_desk
```

### 方式二：SQLite（本地快速启动 / Quick Local Start）

```bash
# 修改 .env
AURA_DATABASE_URL=sqlite+aiosqlite:///aura_local.db
AURA_DB_CONNECT_ON_STARTUP=false
```

> 无需安装 PostgreSQL，直接运行即可。数据存储在 `aura_local.db` 文件中。

### 运行数据库迁移 / Run Migrations

```bash
# 进入虚拟环境
poetry shell

# 运行 Alembic 迁移（如有迁移文件）
alembic upgrade head
```

---

## 6. 运行服务 / Running the Service

### 开发模式 / Development Mode

```bash
# 进入虚拟环境
poetry shell

# 启动服务（热重载需额外配置，当前为直接运行）
python -m aura_gateway

# 或使用 Poetry
poetry run python -m aura_gateway
```

服务将在 `http://0.0.0.0:8000` 启动。

### 验证服务 / Verify Service

```bash
# 健康检查
curl http://localhost:8000/health

# 预期返回
{
  "status": "ok",
  "service": "aura-gateway",
  "environment": "local",
  "version": "0.1.0",
  ...
}
```

### 服务地址 / Service URLs

| 端点 / Endpoint | 说明 / Description |
|---|---|
| `http://localhost:8000/health` | 健康检查 / Health check |
| `http://localhost:8000/healthz` | Docker HEALTHCHECK 兼容端点 |
| `http://localhost:8000/api/v1/health` | 版本化健康检查 |
| `ws://localhost:8000/ws/virtual-human/{session_id}` | WebSocket 虚拟人会话 |

---

## 7. 运行测试 / Running Tests

```bash
# 运行所有测试
poetry run pytest apps/gateway/tests/ -v

# 运行特定测试文件
poetry run pytest apps/gateway/tests/test_pipeline.py -v

# 运行特定测试函数
poetry run pytest apps/gateway/tests/test_pipeline.py::test_streams_real_deltas -v

# 查看覆盖率
poetry run pytest apps/gateway/tests/ --cov=apps/gateway/src/aura_gateway

# 代码质量检查
poetry run ruff check .
poetry run ruff format .
```

### 测试架构说明 / Test Architecture

- **单元测试**：`test_pipeline.py` — 使用 `httpx.MockTransport` 模拟 LLM，`edge_tts` 在 `conftest.py` 中被自动 mock
- **集成测试**：`test_websocket.py` — 使用 FastAPI `TestClient` 进行 WebSocket 端到端测试
- **edge_tts mock**：`conftest.py` 无条件注入 `sys.modules["edge_tts"] = MagicMock()`，确保测试不发起真实网络请求

---

## 8. 前端使用 / Frontend Usage

### 直接打开 / Direct Open

```bash
# 方式一：直接用浏览器打开
start apps/gateway/static/index.html

# 方式二：使用任意 HTTP 服务器
python -m http.server 8080 --directory apps/gateway/static
# 然后访问 http://localhost:8080
```

### 前端功能 / Frontend Features

| 功能 / Feature | 说明 / Description |
|---|---|
| 文本对话 / Text Chat | 输入框输入文字，点击"发送"或按 Enter |
| 语音输入 / Voice Input | 按住"🎤 按住说话"按钮录音，松开自动发送 |
| 打断 / Barge-in | 按 `Esc` 键发送中断信号 |
| 调试日志 / Debug Log | 点击"调试日志"展开查看 WebSocket 通信详情 |
| 自动重连 / Auto-reconnect | 断开后指数退避重连（最长 30 秒） |

### WebSocket 协议 / WebSocket Protocol

```
客户端 → 服务端 Client → Server:
1. 文本帧: {"type": "text", "session_id": "...", "text": {"text": "你好", "final": true}}
2. 音频控制帧: {"type": "audio", "session_id": "...", "audio": {"sample_rate": 16000, "channels": 1}}
3. 音频二进制帧: <raw PCM/WebM bytes，最大 16KB>
4. 中断帧: {"type": "interrupt", "session_id": "..."}

服务端 → 客户端 Server → Client:
1. text_delta: {"type": "text_delta", "text": {"text": "您好", "final": false}}
2. status: {"type": "status", "status": {"state": "thinking"|"speaking"|"listening"|"interrupted"}}
3. audio_chunk: {"type": "audio_chunk", "audio": {"final": true}} + <binary MP3 bytes>
4. error: {"type": "error", "error": {"code": "...", "message": "..."}}
```

---

## 9. Docker 部署 / Docker Deployment

### 构建镜像 / Build Image

```bash
# 必须在仓库根目录执行 / Must run from repo root
docker build -f apps/gateway/Dockerfile -t aura-gateway:local .
```

### 运行容器 / Run Container

```bash
docker run -p 8000:8000 \
  --env-file .env \
  --name aura-gateway \
  aura-gateway:local
```

### Docker 多阶段构建说明 / Docker Multi-Stage Build

| 阶段 / Stage | 作用 / Purpose |
|---|---|
| `deps` | 从 `poetry.lock` 解析依赖到 `/src/.venv` |
| `builder` | 为每个 workspace 成员构建 wheel |
| `runner` | 复制 venv + wheels，`pip install --no-deps` 锁定版本 |

> ⚠️ 构建时自动运行 `python -c "import aura_gateway, ..."` 验证导入，如果 `edge-tts` 未在 `pyproject.toml` 声明，这里会报 `ImportError`。

---

## 10. 避坑指南 / Pitfalls

### 🔴 P0 级：必须解决 / Must Fix

#### 坑 1：Poetry 版本错误会导致 schema 校验失败

**现象 / Symptom**：
```
poetry lock
[ValueError]
Invalid value for `editable = true`: ...
```

**原因 / Cause**：
- 项目使用 Poetry **2.1.3**，其 schema 只接受 `develop = true`，不接受 `editable = true`
- Poetry 1.8 没有原生 `workspace` 表，需安装 `poetry-workspaces` 插件

**解决 / Fix**：
```bash
# 检查 Poetry 版本
poetry --version  # 应显示 2.1.3

# 如果版本不对，安装指定版本
pip install poetry==2.1.3
```

#### 坑 2：`.env` 必须手动创建，不会自动生成

**现象 / Symptom**：
```
ModuleNotFoundError: No module named 'edge_tts'
```

或

```
LLM 连接失败：401 Unauthorized
```

**原因 / Cause**：
- `.env` 被 `.gitignore` 忽略，不会随仓库一起提交
- 克隆后必须手动 `Copy-Item .env.example .env` 并填写真实值

**解决 / Fix**：
```bash
# Windows
Copy-Item .env.example .env

# 编辑 .env，填入 AURA_LLM_API_KEY 等真实值
notepad .env
```

#### 坑 3：必须激活 Poetry 虚拟环境才能运行

**现象 / Symptom**：
```
ModuleNotFoundError: No module named 'fastapi'
```

**原因 / Cause**：
- Poetry 创建了 `.venv/` 虚拟环境，所有依赖都装在里面
- 直接使用系统 `python` 会找不到包

**解决 / Fix**：
```bash
# 方式一：激活 shell
poetry shell
python -m aura_gateway

# 方式二：使用 poetry run
poetry run python -m aura_gateway
```

#### 坑 4：edge-tts 必须声明在 pyproject.toml，否则 Docker 构建失败

**现象 / Symptom**：
```
docker build ... 
ImportError: No module named 'edge_tts'
```

**原因 / Cause**：
- `streaming.py` 顶层 `import edge_tts`
- 如果 `edge-tts` 未在 `apps/gateway/pyproject.toml` 的 `dependencies` 中声明，`poetry install` 不会安装它

**解决 / Fix**：
- 已在 `apps/gateway/pyproject.toml` 中添加 `"edge-tts>=7.0,<8.0"`
- 如需重新 lock：`poetry lock && poetry install`

---

### 🟠 P1 级：容易踩坑 / Common Issues

#### 坑 5：测试套件挂死（已修复 / Fixed）

**现象 / Symptom**：
```
test_binary_audio_payload_is_sent ... 永久阻塞（>90s）
```

**原因 / Cause**：
- `conftest.py` 中 `if "edge_tts" not in sys.modules` 条件判断不生效
- 当 `edge-tts` 已安装时，真实 `edge_tts.Communicate(...).stream()` 会在测试中发起网络请求

**解决 / Fix**：
- 已改为无条件 mock：`sys.modules["edge_tts"] = MagicMock()`
- 并添加 autouse fixture 确保 `streaming.edge_tts` 也被 patch

#### 坑 6：数据库连接失败

**现象 / Symptom**：
```
ConnectionRefusedError: [Errno 111] Connection refused
```

**解决 / Fix**：
```bash
# 检查 PostgreSQL 是否运行
# Windows（使用 pgAdmin 或服务管理器）
# Linux/macOS
pg_isready

# 本地开发可改用 SQLite（无需 PostgreSQL）
# 修改 .env：
AURA_DATABASE_URL=sqlite+aiosqlite:///aura_local.db
AURA_DB_CONNECT_ON_STARTUP=false
```

#### 坑 7：LLM API Key 无效

**现象 / Symptom**：
```
LLM 返回 401 Unauthorized
```

**解决 / Fix**：
```bash
# 1. 检查 .env 中的 AURA_LLM_API_KEY 是否填写正确
# 2. 确保没有多余的空格或引号
# 3. 检查 API Key 是否过期

# 测试 LLM 连通性
poetry run python scripts/test_llm.py
```

#### 坑 8：WebSocket 连接失败

**现象 / Symptom**：
```
WebSocket connection to 'ws://127.0.0.1:8000/ws/virtual-human/xxx' failed
```

**解决 / Fix**：
```bash
# 1. 确认服务已启动
curl http://localhost:8000/health

# 2. 检查 session_id 格式（仅允许字母、数字、连字符、下划线）
# 3. 浏览器控制台查看具体错误信息
```

#### 坑 9：前端音频无法播放

**现象 / Symptom**：
- 能收到 `audio_chunk` 消息，但浏览器没有声音

**原因 / Cause**：
- `edge-tts` 输出极小的 MP3 碎片（几百字节）
- 前端必须累积所有碎片后合并解码，不能逐片播放

**解决 / Fix**：
- 已在 `index.html` 中实现 `playAccumulatedAudio()` 函数
- 收到 `audio.final = true` 的元数据后自动合并播放

#### 坑 10：ruff lint 报错（已修复 / Fixed）

**现象 / Symptom**：
```
Found 86 errors
```

**解决 / Fix**：
```bash
# 自动修复
poetry run ruff check . --fix

# 格式化
poetry run ruff format .
```

---

### 🟡 P2 级：其他注意事项 / Other Notes

#### 坑 11：Windows 路径问题

- 项目使用 `src/` 布局，Poetry 2.1.3 会自动检测
- 不要在 `PYTHONPATH` 中手动添加路径，让 Poetry 管理

#### 坑 12：coverage.xml 被 git 跟踪

- 已添加到 `.gitignore`
- 如需移除历史跟踪：`git rm --cached coverage.xml`

#### 坑 13：临时脚本被 lint 扫到

- `test_ws_client.py` 已移至 `scripts/` 目录
- `pyproject.toml` 中 `extend-exclude` 已排除 `scripts`

---

## 11. 常见问题 / FAQ

### Q1: 为什么不用 pip 而用 Poetry？

**A**: 项目是 **monorepo**，包含 3 个 workspace 成员（aura-common、aura-schemas、aura-gateway），彼此有本地路径依赖。Poetry 能正确处理这种结构，生成单一的 `poetry.lock` 和共享虚拟环境。

### Q2: 可以不装 PostgreSQL 吗？

**A**: 可以。本地开发时改用 SQLite：
```bash
AURA_DATABASE_URL=sqlite+aiosqlite:///aura_local.db
AURA_DB_CONNECT_ON_STARTUP=false
```

### Q3: 如何更换 AI 模型？

**A**: 修改 `.env` 中的 `AURA_LLM_BASE_URL` 和 `AURA_LLM_MODEL`。项目兼容所有 OpenAI API 格式的提供商。

### Q4: TTS 支持哪些语言？

**A**: `edge-tts` 支持 100+ 种语言和方言。查看可用语音：
```bash
poetry run edge-tts --list-voices
```

### Q5: 如何调试 WebSocket 通信？

**A**: 打开前端页面的"调试日志"面板，可以看到所有 WebSocket 帧的收发详情。后端日志会输出到控制台。

### Q6: 测试为什么这么慢？

**A**: 测试使用了 `pytest-asyncio` 的 `function` 作用域，每个测试函数都会创建新的事件循环。这是为了保证测试隔离，但会牺牲一些速度。

### Q7: 如何贡献代码？

**A**:
1. 创建功能分支
2. 运行 `poetry run ruff check .` 和 `poetry run pytest`
3. 提交 PR

### Q8: 生产环境部署注意事项？

**A**:
- 设置 `AURA_ENVIRONMENT=prod` 和 `AURA_DEBUG=false`
- 使用 PostgreSQL 而非 SQLite
- 配置真实的 `AURA_LLM_API_KEY`
- 通过 Docker 部署（见第 9 章）
- 配置反向代理（Nginx/Caddy）处理 HTTPS

---

## 快速命令速查 / Quick Command Reference

```bash
# 安装 / Install
poetry install

# 激活环境 / Activate env
poetry shell

# 运行服务 / Run server
python -m aura_gateway

# 运行测试 / Run tests
poetry run pytest apps/gateway/tests/ -v

# 代码检查 / Lint
poetry run ruff check .
poetry run ruff format .

# Docker 构建 / Build Docker
docker build -f apps/gateway/Dockerfile -t aura-gateway:local .

# Docker 运行 / Run Docker
docker run -p 8000:8000 --env-file .env aura-gateway:local
```

---

## 技术支持 / Support

- 项目仓库：`D:\aistudypython\aura-desk`
- 配置文件：`apps/gateway/src/aura_gateway/core/config.py`
- 核心管道：`apps/gateway/src/aura_gateway/pipeline/streaming.py`
- 前端代码：`apps/gateway/static/index.html`

---

*最后更新 / Last Updated: 2026-10-03*
