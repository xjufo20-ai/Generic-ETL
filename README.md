# Generic-ETL

[![Java](https://img.shields.io/badge/Java-21-blue)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-green)](https://spring.io/projects/spring-boot)
[![Apache Camel](https://img.shields.io/badge/Camel-4.8.0-orange)](https://camel.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

JSON 或 YAML 配置驱动的 ETL 引擎，基于 **Spring Boot 3.3 + Apache Camel 4.8**。
一条 Pipeline 配置 → 一条 Camel Route，利用 Camel 原生 EIP（Filter、Aggregate、Split、Multicast）处理数据流。

---

## 模块

| 模块 | 职责 | 关键类 |
|------|------|--------|
| `etl-common` | 数据模型与 DTO | `PipelineConfig`, `TransformDef`, `DataSourceConfig`, `Row` |
| `etl-core` | 表达式、变换、编译 & 校验 | `ExpressionEvaluator`, `EtlAggregator`, `JsonToYamlCompiler`, `PipelineConfigValidator` |
| `etl-engine` | 分发、持久化、Consumer 管理 | `LoadRouter`, `PersistHandler`, `ConsumerRegistry`, `ResultCache` |
| `etl-api` | Web 层 (REST + Dashboard + Security) | `PipelineController`, `ConsumerController`, `DashboardController` |

## 快速开始

```bash
# 1. 启动（默认 H2 内存数据库）
./gradlew :etl-api:bootRun

# 2. 访问
# Hawtio 控制台: http://localhost:8080/hawtio
# Swagger:        http://localhost:8080/swagger-ui.html
```

---

## 5 分钟体验：CSV → CSV

> 所有文件在 `etl-api/examples/csv-etl/` 目录下。

### 1. 查看输入数据

```bash
cat etl-api/examples/csv-etl/input.csv
```
```
order_id,customer,amount,city
1,张三,150.00,北京
2,李四,80.00,上海
3,王五,200.00,北京
4,赵六,50.00,深圳
5,孙七,320.00,上海
6,周八,95.00,北京
```

### 2. 注册 Pipeline

筛选 `amount > 100`，重命名字段，输出到 `data/output.csv`。

```bash
curl -s -X POST http://localhost:8080/api/pipelines/register \
  -H "X-API-Key: dev-admin" \
  -H "Content-Type: application/json" \
  -d @etl-api/examples/csv-etl/pipeline.json
```

### 3. 查看 Route 是否生效

```bash
curl -s http://localhost:8080/api/pipelines/routes -H "X-API-Key: dev-viewer"
# → ["csv-etl-demo", "test-minimal"]
```

### 4. 注册 Consumer（PULL 模式）

```bash
curl -s -X POST http://localhost:8080/api/consumers/register \
  -H "X-API-Key: dev-admin" \
  -H "Content-Type: application/json" \
  -d @etl-api/examples/csv-etl/consumer.json
```

### 5. 拉取数据

```bash
curl -s "http://localhost:8080/api/consumers/data/csv-etl-demo?consumer=my-app" \
  -H "X-API-Key: dev-viewer"
```

响应（3 条 amount > 100 的记录，字段已重命名）：
```json
{
  "success": true,
  "data": {
    "pipeline": "csv-etl-demo",
    "consumer": "my-app",
    "totalRows": 3,
    "data": [
      {"id": "1", "customer": "张三", "total": "150.00", "city": "北京"},
      {"id": "3", "customer": "王五", "total": "200.00", "city": "北京"},
      {"id": "5", "customer": "孙七", "total": "320.00", "city": "上海"}
    ]
  }
}
```

### 6. 查看输出的 CSV 文件

```bash
cat etl-api/data/output.csv
```
```
id,customer,total,city
1,张三,150.00,北京
3,王五,200.00,北京
5,孙七,320.00,上海
```

---

## Consumer 模式

| 模式 | 说明 |
|------|------|
| **PUSH** | 数据到达后主动 POST 到 Consumer 的 `endpoint` |
| **PULL** | Consumer 通过 API 拉取 `GET /api/consumers/data/{pipeline}?consumer=name` |

---

## Pipeline 配置格式

```json
{
  "pipeline": {"name": "csv-etl-demo", "version": "1.0"},
  "datasource": {
    "type": "csv",
    "filePath": "etl-api/examples/csv-etl/input.csv",
    "delimiter": ",",
    "hasHeader": true
  },
  "inputSchema": {
    "fields": [
      {"name": "order_id", "type": "LONG"},
      {"name": "customer", "type": "STRING"},
      {"name": "amount", "type": "DOUBLE"},
      {"name": "city", "type": "STRING"}
    ]
  },
  "transforms": [
    {"type": "filter", "expression": "amount > 100"},
    {"type": "project", "mappings": [
      {"from": "order_id", "to": "id"},
      {"from": "customer", "to": "customer"},
      {"from": "amount", "to": "total"},
      {"from": "city", "to": "city"}
    ]}
  ],
  "output": {"storage": {"type": "csv", "table": "output.csv"}}
}
```

支持的 transform 类型：`filter`、`project`、`rename`、`aggregate`、`typeCast`、`join`、`split`。

---

## API 参考

### Pipeline 管理

| 方法 | 端点 | 权限 | 说明 |
|------|------|------|------|
| `POST` | `/api/pipelines/register` | ADMIN | 注册 Pipeline JSON → 编译为 YAML → 创建 Camel Route |
| `DELETE` | `/api/pipelines/routes/{routeId}` | ADMIN | 停止并移除 Camel Route |
| `GET` | `/api/pipelines` | AUTH | 列出所有已注册的 Pipeline |
| `GET` | `/api/pipelines/routes` | AUTH | 列出所有 Camel Route ID |
| `GET` | `/api/pipelines/{name}/audit` | AUTH | 审计日志 |
| `GET` | `/api/pipelines/lineage` | AUTH | 数据血缘 |
| `POST` | `/api/pipelines/yaml` | ADMIN | 直接加载 YAML DSL |

### Consumer 管理

| 方法 | 端点 | 权限 | 说明 |
|------|------|------|------|
| `POST` | `/api/consumers/register` | ADMIN | 注册 Consumer |
| `DELETE` | `/api/consumers/{name}` | ADMIN | 注销 Consumer |
| `GET` | `/api/consumers` | AUTH | 列出所有 Consumer |
| `GET` | `/api/consumers/data/{pipeline}` | AUTH | PULL 拉取数据 |

### 认证

所有请求需携带 `X-API-Key` Header：

| Key | 角色 |
|-----|------|
| `dev-admin` | ADMIN |
| `dev-operator` | OPERATOR |
| `dev-viewer` | VIEWER |

---

## 配置 Profile

| Profile | 数据库 | 日志 |
|---------|--------|------|
| `dev` (默认) | H2 内存 | DEBUG |
| `qa` | PostgreSQL | INFO |
| `prod` | PostgreSQL | WARN |

```bash
./gradlew :etl-api:bootRun --args='--spring.profiles.active=prod'
```

---

## 项目结构

```
Generic-ETL/
├── etl-api/                          Spring Boot 应用 + REST + Security
│   ├── src/main/java/.../
│   │   ├── config/                   Spring 配置、EtlYamlRouteLoader
│   │   ├── controller/               PipelineController, ConsumerController
│   │   ├── security/                 ApiKeyAuthFilter, SecurityConfig
│   │   ├── store/                    StateStore, LineageStore
│   │   └── metrics/                  EtlMetrics
│   ├── src/main/resources/           application.yml, templates
│   ├── config/
│   │   ├── routes/                   Camel YAML DSL（启动时自动加载）
│   │   └── samples/                  Pipeline JSON 示例
│   └── examples/
│       ├── csv-etl/                  开箱即用的 CSV ETL 示例
│       └── sql/                      DDL 参考（PostgreSQL）
├── etl-common/                       共享数据模型 & DTO
├── etl-core/                         ETL 核心：编译、变换、表达式、校验
│   ├── compile/                      JsonToYamlCompiler
│   ├── config/                       PipelineConfigValidator
│   ├── expression/                   ExpressionEvaluator (MVEL)
│   ├── transform/                    EtlAggregator, ProjectTransformer, TypeCaster
│   └── store/                        AuditLog
├── etl-engine/                       分发、持久化 & Consumer 管理
│   ├── dispatch/                     ConsumerDispatchService (PUSH)
│   ├── persist/                      PersistHandler (JDBC)
│   ├── LoadRouter.java
│   ├── ConsumerRegistry.java
│   └── ResultCache.java
├── build.gradle
└── settings.gradle
```

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Java | 21 | 运行环境 |
| Spring Boot | 3.3.5 | 应用框架 |
| Apache Camel | 4.8.0 | ETL 管线引擎 (EIP + YAML DSL) |
| MVEL | 2.5.2 | 表达式评估 |
| Hawtio | 4.2.0 | Camel 可视化监控 |
| PostgreSQL / H2 | — | 数据存储 |
| Gradle | 8.10 | 构建工具 |

## License

MIT
