# Data Construct Platform

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

MySQL、Redis、Kafka 仅由本地依赖环境提供；服务中的适配器目前是接口骨架，尚未接入这些组件。

## API 骨架

- `GET /healthz`：健康检查。
- `GET /api/v1/templates`：当前进程内模板列表（初始为空）。
- `POST /api/v1/templates`：创建进程内模板，JSON 字段为 `name`、`content`。
- `POST /api/v1/dags/validate`：校验 DAG 节点依赖并返回拓扑顺序。
- `POST /api/v1/jobs`：基于已存在模板创建进程内任务记录。
- `GET /api/v1/jobs/{id}`：获取进程内任务状态。

数据目前存储在内存中，进程重启后会清空。生产环境接入 MySQL、Redis、Kafka 前，应补充持久化、鉴权、输入限制、重试和可观测性。
