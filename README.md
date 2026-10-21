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

服务默认监听 `:8080`。可通过 `API_ADDR` 修改地址，模板引擎地址通过 `TEMPLATE_ENGINE_ADDR` 设置。健康检查：`GET /healthz`。所有平台 ID 使用十进制字符串形式的 Snowflake ID；通过 `SNOWFLAKE_NODE_ID` 配置 0–1023 的节点号，各 Go/Python 实例必须使用互不重复的节点号。单实例本地默认值为 Go `1`、Python `2`。

### Python 模板引擎

使用 `python-engine/proto/template_engine.proto` 生成 Go/Python gRPC 文件，然后启动服务。Go Worker 通过生成的 protobuf client 调用 `GenerateData`，模板引擎也提供 `ValidateTemplate` RPC；模板预览复用批量生成能力。Worker 的 Kafka 消费执行循环仍待接入。

```bash
# 在仓库根目录生成 Go protobuf 与 gRPC 代码
PATH="$(go env GOPATH)/bin:$PATH" protoc -I python-engine/proto \
  --go_out=go-service/internal/pb --go_opt=paths=source_relative \
  --go-grpc_out=go-service/internal/pb --go-grpc_opt=paths=source_relative \
  python-engine/proto/template_engine.proto

cd python-engine
python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I proto \
  --python_out=service --grpc_python_out=service \
  proto/template_engine.proto
PYTHONPATH=service python -m server
```

服务默认监听 `:50051`，可通过 `GRPC_ADDR` 修改。Go 生成文件纳入版本控制；Python gRPC 文件由启动前的命令生成，不纳入版本控制。`python_generic_services` 是当前 protoc 不识别的旧选项，Python gRPC 插件会生成服务与客户端 stub，因此未放入 proto 文件。

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

## REST API (`/api/v1`)

- `GET /healthz`：健康检查。
- `POST /api/v1/template`：创建模板，返回 `data.template_id`。
- `PUT /api/v1/template/{template_id}`：修改模板。
- `GET /api/v1/template/{template_id}`：获取模板详情。
- `GET /api/v1/template?page=1&size=10&name=`：分页查询模板。
- `DELETE /api/v1/template/{template_id}`：删除模板。
- `POST /api/v1/template/preview`：通过 Python gRPC 服务预生成少量样例数据，返回 `data.rows`。
- `POST /api/v1/dags/validate`：校验画布格式的 DAG（`nodes`、`edges`、`dag_meta`），按 edges 返回依赖优先的拓扑顺序；`position` 只用于前端渲染。
- `POST /api/v1/jobs`：创建进程内 DAG 运行实例，JSON 字段为 `dag_id` 和可选的 `trigger_type`（`manual`、`schedule`、`api`，默认 `api`）。
- `GET /api/v1/jobs/{id}`：获取进程内任务状态。

模板请求字段采用 snake_case，包括 `target_type`、`target_table`、`batch_size`、`fields` 和 `sink_conf`。创建和修改均校验模板字段及目标表配置。

### DAG 画布数据

前端 DAG 画布支持节点拖动、节点配置、依赖连线和 JSON 序列化。画布输出的 JSON 对应 `data_dag.dag_content`；调用 `/api/v1/dags/validate` 可校验节点/边引用及循环依赖，并获取拓扑顺序。边的 `source_node_id` 是上游节点，`target_node_id` 是下游节点。节点 `config.fail_strategy` 支持 `abort_dag` 和 `continue`。当前 DAG JSON 可在画布下方查看，但 DAG 创建/更新持久化 API 尚未实现。

## 元数据与消息约定

- MySQL 元数据表：`data_template`、`data_dag`、`data_job`、`data_task`、`data_job_log`，定义见 [init.sql](./scripts/init.sql)。生成的业务数据应写入目标数据源，不写入这些平台表。
- Redis Key 统一以 `dc:` 开头，完整命名和 TTL 见下方 Redis 约定。运行上下文使用 Hash，供 DAG 节点间传递上游输出。
- Kafka Topic：`data-construct-task` 用于待执行任务消息；`data-construct-event` 用于任务状态与进度事件。

目前服务记录仍存储在内存中，进程重启后会清空；Redis/Kafka 客户端为未配置适配器。Worker 消费处理逻辑已实现，但未连接真实 Kafka consumer、Redis 和 sink，因此仍不能端到端运行。模板创建、预览和分页 API 已有最小实现。

### Kafka Task / Event 消息

消息均使用 JSON 编码，并以 `job_id` 作为 Kafka key，保证同一 job 的消息落入同一分区。消息结构与校验定义位于 [messages.go](./go-service/internal/kafka/messages.go)。

`data-construct-task` 的字段为 `msg_id`、`job_id`、`task_id`、`node_id`、`template_json`、`context_key`、`batch_count`、`sink_key`、`fail_strategy`、`retry_times`、`max_retry` 和 `create_at`。Worker 从 Redis 读取 `context_key`，调用模板引擎生成数据，再由 `sink_key` 选择目标写入器，并在开始、成功或失败时向 Event Topic 上报事件。

`data-construct-event` 的 `event_type` 支持 `task_start`、`task_success`、`task_fail`、`job_success`、`job_fail` 和 `job_cancel`。事件包含 `event_id`、`job_id`、可选的 `task_id`/`node_id`、`payload`（生成行数、写入耗时、错误与堆栈）及 `timestamp`。状态、指标和日志消费组常量分别为 `data-construct-state`、`data-construct-metrics` 和 `data-construct-logs`。

Worker 的 `Consume` 在处理失败时发布 `task_fail` 并返回错误，让 Kafka Consumer/调度适配器决定重试或死信处理；`retry_times` 不会由 Worker 自动增加或重新投递。`task_id` 会传入 sink 的批量写入接口，作为实现幂等写入的键；调度/消费适配器应据此实施持久化幂等保护，并控制最多 `max_retry` 次重试。当前仓库的 Kafka、Redis 和 sink 实现仍是适配器接口/占位实现，尚未形成可运行的端到端消费部署。

### Job / Task 状态机

状态规则实现于 [state_machine.go](./go-service/internal/job/state_machine.go)，事件入口由 [event_consumer.go](./go-service/internal/job/event_consumer.go) 定义。Job 状态为 `pending → running → success|failed|canceled`，pending Job 可直接转 canceled；Task 状态为 `pending → running → success|failed|canceled`，failed Task 在还有重试额度时回到 pending，等待同一 task 的重投；pending Task 可转 skipped/canceled。

只有所有上游 Task 为 success 时，下游 Task 才会出现在 ready 列表。`continue` 失败会跳过所有依赖该失败节点的 pending 后代，同时允许无关分支继续；当所有 Task 进入终态且没有 abort 失败时，Job 成功（continue 失败视为允许的终态）。`abort_dag` 重试耗尽后 Job 失败，取消未执行的 pending Task，不再调度后续节点。Job 取消事件会将 pending/running Task 标为 canceled；已经开始的底层生成/写入操作无法被强制终止，迟到的 Task 事件会被确认但不会覆盖 canceled 状态。

API `POST /api/v1/jobs/{id}/cancel` 只发布 `job_cancel` 事件，不直接写 Job/Task 状态；需要注入 Kafka Producer。Worker 仅发 Task 事件。Event Consumer 调用 `AtomicEventStore`，生产实现必须在单个数据库事务中去重 `event_id`、应用 Job/Task 状态变化，并持久化重试/ready 动作（建议 transactional outbox），避免数据库更新与 Kafka 重投之间丢失动作。数据库枚举及重试字段见 [init.sql](./scripts/init.sql)；新增的 `data_event_inbox` 用于持久事件去重。当前项目只有内存状态机参考实现，尚未提供 MySQL 原子投影、真实 event consumer、重试 outbox 或 DAG 调度器。

### Redis Key 与过期策略

所有 Redis Key 使用 `dc:` 前缀，常量、key helper 和带 TTL 的辅助操作定义在 [client.go](./go-service/internal/redis/client.go)。本地配置模板位于 [config.yaml](./go-service/configs/config.yaml)。

| Key | 类型 / 用途 | TTL / 清理 |
| --- | --- | --- |
| `dc:lock:job:{job_id}` | Job 全局调度锁，防止多实例重复触发 | 300 秒；调度器需通过锁 lease 定期续期（看门狗），结束时释放 |
| `dc:lock:task:{task_id}` | Worker 任务执行锁，阻止同一任务并发执行 | 600 秒；Worker 处理后主动释放，超时兜底 |
| `dc:job:{job_id}:context` | Hash，上游变量字段如 `node-001.user_id_list`，值为 JSON 编码 | 24 小时；Job 完成/取消时调用 `DeleteJobContext` 主动删除 |
| `dc:template:{template_id}` | String，模板完整 JSON | 1 小时；模板变更时调用 `InvalidateTemplate` 删除缓存 |
| `dc:template:list` | 保留名称，不缓存模板列表 | 不设置、不读取 |
| `dc:job:{job_id}:task_cnt` | Hash，`success_count` / `failure_count` 等计数 | 24 小时兜底 |
| `dc:limit:sink:{sink_key}` | 目标库并发写入 permit 计数 | 单 sink 默认最多 5 个并发；permit lease 以 600 秒为兜底并在完成时释放 |
| `dc:retry:queue` | 可选 Sorted Set，延迟重试队列 | 由重试调度策略清理完成项 |
| `dc:preview:{preview_req_id}` | String，模板预览样例 | 10 分钟 |

临时运行数据均设置 TTL；Job 终态主动清理上下文，TTL 用于异常退出后的兜底。Redis 接口已暴露 Hash、计数器、锁 lease（续期/释放）和并发 permit 操作契约，但 Redis 适配器仍为占位实现，需要在接入真实 Redis 时以原子操作实现锁续期/释放、计数和 permit。生产环境建议使用 Redis Cluster。超大列表不应写入单个 Hash value，可按 `dc:job:{job_id}:var:{var_name}` 分片存储。
