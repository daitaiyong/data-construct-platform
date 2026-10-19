# Data Construct Platform
# 架构总图
┌─────────────────────────────────────────────────────┐
│ 前端控制台（Web）：模板管理、编排画布、任务管理、监控报表 │
├─────────────────────────────────────────────────────┤
│ API网关层（Go）：鉴权、限流、路由、参数校验、接口转发     │
├───────────────┬───────────────┬─────────────────────┤
│ 数据模板模块   │ 依赖编排模块   │ 任务调度模块          │ ← 三层核心模型
│ (Python/Go)   │ (Python/Go)   │ (Go为主)             │
├───────────────┼───────────────┼─────────────────────┤
│ 生成器插件池   │ DAG解析引擎   │ 调度器 + Worker集群   │
│ 表达式引擎     │ 依赖校验器    │ 状态管理器            │
├───────────────┴───────────────┴─────────────────────┤
│ 消息队列：Kafka（任务事件、执行日志、进度上报）         │
│ 缓存：Redis（任务锁、临时上下文、模板缓存、限流）       │
│ 存储：MySQL（元数据：模板、编排DAG、任务记录、配置）    │
│ 外部数据源：MySQL/其他DB（目标库写入、参考数据读取）     │
└─────────────────────────────────────────────────────┘
### 技术选型说明

1. **Go**：API 网关、调度器、Worker 节点，高并发、轻量协程，适合任务分发、IO 密集任务；
2. **Python**：数据模板解析、表达式引擎、各类数据生成器（Faker、自定义规则），生态丰富；
3. MySQL：持久化平台元数据（模板、DAG 编排、任务历史、用户、项目）；
4. Redis：分布式锁、任务上下文缓存、模板缓存、任务计数器、限流；
5. Kafka：任务事件流、执行日志、异步消息，解耦调度器与 worker，支持多消费组；
6. 定时调度：自研基于 Redis 锁的分布式定时调度，也可对接 Quartz/xxl-job 思想。


数据构造平台的最小可运行骨架，包含 Go API / Scheduler / Worker、Python 模板引擎、Vue 前端和本地依赖服务。

## 目录

- `go-service/`：Go HTTP API、DAG 校验、调度器和 Worker 入口。
- `python-engine/`：模板引擎 gRPC 合约及基础实现。
- `web/`：Vue 3 + Vite 前端骨架。
- `scripts/`：MySQL 初始化脚本和本地依赖 Compose 文件。

## 本地启动

### Go API

```bash
cd go-service
go run ./cmd/api-server
```

服务默认监听 `:8080`。可通过 `API_ADDR` 修改地址。健康检查：`GET /healthz`。

### Python 模板引擎

使用 `python-engine/proto/template_engine.proto` 生成 Python gRPC 文件，然后启动服务：

```bash
cd python-engine
python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I proto \
  --python_out=service --grpc_python_out=service \
  proto/template_engine.proto
PYTHONPATH=service python -m server
```

服务默认监听 `:50051`，可通过 `GRPC_ADDR` 修改。生成文件不纳入版本控制。

### 前端

```bash
cd web
npm install
npm run dev
```

前端开发服务器默认监听 `http://localhost:5173`。`VITE_API_BASE_URL` 可设置 API 地址。

### 本地依赖

```bash
docker compose -f scripts/docker-compose.yml up -d
```

MySQL、Redis、Kafka 仅由本地依赖环境提供；当前 MySQL 初始化脚本只建立平台元数据表，不保存生成的业务/测试数据。服务中的适配器目前是接口骨架，尚未接入这些组件。

## API 骨架

- `GET /healthz`：健康检查。
- `GET /api/v1/templates`：当前进程内模板列表（初始为空）。
- `POST /api/v1/templates`：创建进程内模板，JSON 字段为 `name`、`desc`、`targetType`、`batchSize`、`fields`、`creator`。
- `POST /api/v1/dags/validate`：校验 DAG 节点依赖并返回拓扑顺序。
- `POST /api/v1/jobs`：创建进程内 DAG 运行实例，JSON 字段为 `dagId` 和可选的 `triggerType`（`manual`、`schedule`、`api`，默认 `api`）。
- `GET /api/v1/jobs/{id}`：获取进程内任务状态。

## 元数据与消息约定

- MySQL 元数据表：`data_template`、`data_dag`、`data_job`、`data_task`、`data_job_log`，定义见 [init.sql](./scripts/init.sql)。生成的业务数据应写入目标数据源，不写入这些平台表。
- Redis Key：`tpl:{id}`、`job:{job_id}:context`、`lock:job:{job_id}`。运行上下文仅用于 DAG 执行期间的上游输出传递。
- Kafka Topic：`data-construct-task` 用于待执行任务消息；`data-construct-event` 用于任务状态与进度事件。

目前服务记录仍存储在内存中，进程重启后会清空；Redis/Kafka 客户端为未配置适配器。持久化、调度消费与日志写入完成后才会实际使用上述元数据约定。
