# Generic-ETL

[![Java](https://img.shields.io/badge/Java-22-blue)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-green)](https://spring.io/projects/spring-boot)
[![Apache Camel](https://img.shields.io/badge/Camel-4.7.0-orange)](https://camel.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

JSON 或 YAML 配置驱动的 ETL 引擎，基于 **Spring Boot 3.3 + Apache Camel 4.7**。
一条 Pipeline 配置 → 一条 Camel Route，利用 Camel 原生 EIP（Filter、Aggregate、Split、Multicast）处理数据流。

---

## 架构

```
 config/samples/*.json        config/routes/*.yaml
        │                           │
        ▼                           ▼
  JsonToYamlCompiler          Camel YAML DSL (原生加载)
        │                           │
        ▼                           ▼
   Camel YAML 字符串 ───────── Camel Route (EIP native)
                               ┌─────────────────────────┐
                               │ from(jdbc:/kafka:/...)  │
                               │   .filter(simple)       │  ← Filter EIP
                               │   .process(project)     │  ← Rename / Project
                               │   .process(aggregate)   │  ← Aggregate
                               │   .enrich(jdbc:...)     │  ← Join EIP
                               │   .split()              │  ← Split EIP
                               │   .multicast()          │
                               │     .to(bean:loadRouter)│  ← 分发到 Consumer
                               │     .to(file:...)       │  ← 写入 CSV
                               │   .end()                │
                               └─────────────────────────┘
                                        │
                         ┌──────────────┼──────────────┐
                         ▼              ▼              ▼
                   PersistHandler  ResultCache    Hawtio Console
                     (JDBC写入)   (PULL缓存)    (Camel 监控)
```

**设计要点**:
- **JSON → YAML 编译**: `JsonToYamlCompiler` 将 PipelineConfig 编译为 Camel YAML DSL 字符串，再由 Camel 原生加载为 Route。
- **YAML 直接加载**: 也可以直接写 Camel YAML DSL，放入 `config/routes/`，启动时自动注册。
- **零 Java Parser 参与 Route 构建**: Camel 原生理解 YAML DSL，不需要 Java 代码构建 RouteBuilder。

## 模块

| 模块 | 职责 | 关键类 |
|------|------|--------|
| `etl-common` | 数据模型与 DTO | `PipelineConfig`, `TransformDef`, `DataSourceConfig`, `Row`, `DataResponse` |
| `etl-core` | 表达式引擎 & 错误处理 | `ExpressionEvaluator` (MVEL), `CamelDeadLetterHandler` |
| `etl-load` | 数据分发与持久化 | `LoadRouter`, `PersistHandler`, `ConsumerDispatchService`, `ConsumerRegistry`, `ResultCache` |
| `etl-api` | Web 层 (REST + Dashboard + Security) | `PipelineController`, `ConsumerController`, `DashboardController`, `JsonToYamlCompiler`, `EtlYamlRouteLoader` |

## 快速开始

### 前置条件

- Java 22+
- Docker (可选，用于 PostgreSQL)

### 启动

```bash
# 1. 启动 PostgreSQL（可选）
docker compose up -d

# 2. 启动应用（默认使用 H2 内存数据库）
./gradlew :etl-api:bootRun

# 3. 访问
# Hawtio 监控控制台:  http://localhost:8080/hawtio
# Swagger API 文档:   http://localhost:8080/swagger-ui.html
# Dashboard:          http://localhost:8080/dashboard
```

### 5 分钟体验

```bash
# 1. 注册 Pipeline（POST JSON → 自动编译为 Camel Route）
curl -X POST http://localhost:8080/api/pipelines/register \
  -H "X-API-Key: dev-admin" \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline": {"name": "hello-world"},
    "datasource": {"type": "mysql", "query": "SELECT 1 AS num"},
    "output": {"storage": {"type": "csv", "table": "hello.csv"}}
  }'

# 2. 查看已注册的 Pipeline
curl http://localhost:8080/api/pipelines \
  -H "X-API-Key: dev-admin"

# 3. 查看 Camel Routes
curl http://localhost:8080/api/pipelines/routes \
  -H "X-API-Key: dev-viewer"
```

---

## 配置详解

### 方式一：JSON Pipeline 配置

完整的 Pipeline JSON 结构：

```jsonc
{
  "pipeline": {
    "name": "salary-stats",
    "version": "1.0",
    "cron": "0 0 8 * * ?"           // 可选：Cron 定时调度
  },
  "datasource": { ... },             // 数据源
  "watermark": { ... },              // 增量水位（可选）
  "inputSchema": { ... },            // 输入 Schema
  "transforms": [ ... ],             // 转换步骤
  "outputSchema": { ... },           // 输出 Schema
  "output": { ... }                  // 输出配置
}
```

#### 数据源 (datasource)

**JDBC (MySQL / PostgreSQL / Oracle)**:

```jsonc
{
  "type": "mysql",
  "connection": {
    "url": "jdbc:mysql://{{env:DB_HOST:localhost}}:3306/{{env:DB_NAME:company}}",
    "username": "{{env:DB_USER:etl}}",
    "password": "{{env:DB_PASS}}"
  },
  "query": "SELECT id, name, dept, salary FROM employees",
  "cursor": { "column": "id", "pageSize": 5000 }    // 可选：分页游标
}
```

**Kafka**:

```jsonc
{
  "type": "kafka",
  "connection": {
    "bootstrapServers": "localhost:9092",
    "topic": "events",
    "groupId": "etl-group"
  }
}
```

**CSV**:

```jsonc
{
  "type": "csv",
  "filePath": "/data/input.csv",
  "delimiter": ",",
  "hasHeader": true
}
```

**SFTP**:

```jsonc
{
  "type": "sftp",
  "connection": {
    "host": "sftp.example.com",
    "port": 22,
    "username": "{{env:SFTP_USER}}",
    "password": "{{env:SFTP_PASS}}",
    "directory": "/uploads"
  },
  "fileName": "*.csv"
}
```

> `{{env:VAR}}` 和 `{{env:VAR:default}}` 在运行时从环境变量注入，避免密码明文。

#### 增量抽取 (watermark)

```jsonc
{
  "column": "updated_at",
  "initial": "2024-01-01"
}
```

编译后在 SQL 末尾追加 `AND updated_at >= '2024-01-01'`。

#### 转换步骤 (transforms)

管道中的每一步对应一种 `type`：

| type | 说明 | 配置字段 |
|------|------|----------|
| `project` | 字段映射（SELECT a AS b） | `mappings: [{from, to}]` |
| `filter` | 行过滤（MVEL 表达式） | `expression: "salary > 5000"` |
| `rename` | 字段重命名 | `mappings: [{from, to}]` |
| `typeCast` | 类型转换 | `mappings: [{field, toType}]` |
| `aggregate` | 分组聚合 | `groupBy`, `aggregations: [{field, function, alias}]` |
| `join` | 关联查询 (enrich) | `query`, `leftKey`, `rightKey` |
| `split` | 数据拆分 | `field`, `delimiter` |

**聚合函数**: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`

**完整示例** — MySQL → Project → Filter → Aggregate → CSV：

```jsonc
{
  "pipeline": {"name": "salary-stats", "version": "1.0"},
  "datasource": {
    "type": "mysql",
    "connection": {
      "url": "jdbc:mysql://{{env:DB_HOST}}:3306/company",
      "username": "{{env:DB_USER}}",
      "password": "{{env:DB_PASS}}"
    },
    "query": "SELECT emp_id, emp_name, dept_code, salary_amt FROM employees"
  },
  "watermark": {"column": "salary_amt", "initial": "5000"},
  "inputSchema": {
    "fields": [
      {"name": "emp_id", "type": "LONG"},
      {"name": "emp_name", "type": "STRING"},
      {"name": "dept_code", "type": "STRING"},
      {"name": "salary_amt", "type": "DECIMAL"}
    ]
  },
  "transforms": [
    {
      "type": "project",
      "mappings": [
        {"from": "emp_id", "to": "id"},
        {"from": "emp_name", "to": "name"},
        {"from": "dept_code", "to": "dept"},
        {"from": "salary_amt", "to": "salary"}
      ]
    },
    {"type": "filter", "expression": "salary > 5000"},
    {
      "type": "aggregate",
      "groupBy": ["dept"],
      "aggregations": [
        {"field": "salary", "function": "SUM", "alias": "total_salary"},
        {"field": "id", "function": "COUNT", "alias": "employee_count"}
      ]
    }
  ],
  "outputSchema": {
    "fields": [
      {"name": "dept", "type": "STRING"},
      {"name": "total_salary", "type": "DECIMAL"},
      {"name": "employee_count", "type": "LONG"}
    ]
  },
  "output": {
    "enabled": true,
    "storage": {"type": "csv", "table": "salary_stats.csv"}
  }
}
```

#### 数据输出 (output)

```jsonc
{
  "enabled": true,
  "threshold": 1000,
  "storage": {
    "type": "postgresql",     // postgresql | mysql | csv
    "table": "etl.salary_stats",
    "primaryKeys": ["dept"],  // upsert 主键
    "mode": "upsert"          // insert | upsert
  }
}
```

| mode | SQL 行为 | 适用场景 |
|------|----------|----------|
| `insert` | `INSERT INTO ... VALUES ...` | 全量抽取、日志追加 |
| `upsert` | `INSERT ... ON CONFLICT DO UPDATE` | 增量抽取、聚合结果更新 |

### 方式二：Camel YAML DSL（原生）

如果你熟悉 Camel，可以直接用 YAML DSL，放在 `config/routes/` 下，启动时自动加载：

```yaml
- route:
    id: my-pipeline
    from:
      uri: jdbc:etlDataSource
      parameters:
        query: SELECT * FROM employees
    steps:
      - bean:
          ref: projectTransformer
          parameters:
            mappings: {emp_id: id, emp_name: name}
      - filter:
          simple: "${body[salary]} > 5000"
      - bean:
          ref: etlAggregator
          parameters:
            groupBy: dept
            aggregations:
              - {field: salary, function: SUM, alias: total}
      - multicast:
          steps:
            - to: bean:loadRouter
            - to:
                uri: file:data
                parameters:
                  fileName: output.csv
```

参见 `config/routes/salary-stats.yaml` 完整示例。

---

## Consumer（数据消费方）

Consumer 订阅 Pipeline 的输出数据，支持 **PULL**（拉取）和 **PUSH**（推送）两种模式。

### 注册 Consumer

```bash
curl -X POST http://localhost:8080/api/consumers/register \
  -H "X-API-Key: dev-admin" \
  -H "Content-Type: application/json" \
  -d '{
    "consumer": {"name": "my-dashboard"},
    "subscriptions": [{
      "pipeline": "salary-stats",
      "fields": ["dept", "total_salary"],
      "filter": "total_salary > 100000",
      "delivery": {"mode": "PULL", "batchSize": 500}
    }]
  }'
```

### 拉取数据

```bash
curl "http://localhost:8080/api/consumers/data/salary-stats?consumer=my-dashboard&page=0&pageSize=100" \
  -H "X-API-Key: dev-viewer"
```

响应：

```json
{
  "code": 200,
  "data": {
    "pipeline": "salary-stats",
    "consumer": "my-dashboard",
    "totalRows": 25,
    "page": 0,
    "pageSize": 100,
    "totalPages": 1,
    "data": [
      {"dept": "Engineering", "total_salary": 1250000},
      {"dept": "Marketing", "total_salary": 480000}
    ],
    "hasMore": false
  }
}
```

### PUSH 模式

设置 `"delivery": {"mode": "PUSH"}` 并提供 `consumer.endpoint`，Pipeline 执行后自动 POST 结果到目标 URL。

---

## API 参考

### Pipeline 管理

| 方法 | 端点 | 权限 | 说明 |
|------|------|------|------|
| `POST` | `/api/pipelines/register` | ADMIN | 注册 JSON Pipeline → 编译为 YAML → 创建 Camel Route |
| `DELETE` | `/api/pipelines/routes/{routeId}` | ADMIN | 停止并移除 Camel Route |
| `GET` | `/api/pipelines` | AUTH | 列出所有已注册的 Pipeline |
| `GET` | `/api/pipelines/routes` | AUTH | 列出所有 Camel Route ID |
| `GET` | `/api/pipelines/{name}/audit` | AUTH | 查看 Pipeline 变更审计日志 |
| `GET` | `/api/pipelines/lineage` | AUTH | 查看数据血缘（支持 `?pipeline=` 过滤） |
| `POST` | `/api/pipelines/yaml` | ADMIN | 直接加载 YAML DSL 字符串 |

### Consumer 管理

| 方法 | 端点 | 权限 | 说明 |
|------|------|------|------|
| `POST` | `/api/consumers/register` | ADMIN | 注册 Consumer |
| `DELETE` | `/api/consumers/{name}` | ADMIN | 注销 Consumer |
| `GET` | `/api/consumers` | AUTH | 列出所有 Consumer |
| `GET` | `/api/consumers/data/{pipeline}` | AUTH | 拉取数据（PULL 模式），支持分页 |

### 认证

所有 API 请求需携带 `X-API-Key` Header。API Key 配置在 `application.yml`：

```yaml
etl:
  api-keys: sk-admin:ADMIN,sk-operator:OPERATOR,sk-viewer:VIEWER
```

| Key | 角色 | 权限 |
|-----|------|------|
| `sk-admin` | ADMIN | 注册/删除 Pipeline 和 Consumer |
| `sk-operator` | OPERATOR | 查看 Pipeline 和 Consumer，拉取数据 |
| `sk-viewer` | VIEWER | 查看 Pipeline 和 Consumer |

---

## 监控

### Hawtio 控制台

`http://localhost:8080/hawtio` — Camel Routes 可视化、JMX 指标、Route 调试追踪。

### Dashboard

`http://localhost:8080/dashboard` — Pipeline 列表、最近活动、数据血缘。

### Actuator 指标

| 端点 | 内容 |
|------|------|
| `/actuator/health` | 健康检查 |
| `/actuator/metrics` | `etl.pipelines.executed`, `etl.pipelines.failed`, `etl.rows.extracted`, `etl.pipeline.duration` |

---

## 配置 Profile

| Profile | 数据库 | 日志 | 说明 |
|---------|--------|------|------|
| `dev` (默认) | H2 内存 | DEBUG | 开发环境，H2 Console: `/h2-console` |
| `qa` | PostgreSQL | INFO | 测试环境，Eureka 启用 |
| `prod` | PostgreSQL | WARN | 生产环境，Eureka 启用，优雅关闭 |

切换 Profile：

```bash
./gradlew :etl-api:bootRun --args='--spring.profiles.active=prod'
```

---

## 项目结构

```
Generic-ETL/
├── etl-api/                     Spring Boot 应用 + REST + Dashboard + Security
│   ├── src/main/java/.../api/
│   │   ├── config/              JsonToYamlCompiler, EtlYamlRouteLoader, 各类 Processor
│   │   ├── controller/          PipelineController, ConsumerController, DashboardController
│   │   ├── security/            ApiKeyAuthFilter, SecurityConfig
│   │   ├── store/               StateStore, AuditLog, LineageStore
│   │   └── metrics/             EtlMetrics (Micrometer)
│   └── src/main/resources/
│       ├── application.yml      主配置
│       ├── application-{dev,qa,prod}.yml
│       └── templates/           Dashboard Thymeleaf 模板
├── etl-common/                  共享数据模型
│   └── src/main/java/.../common/
│       ├── model/               PipelineConfig, TransformDef, DataSourceConfig, Row...
│       └── dto/                 ApiResponse, DataResponse
├── etl-core/                    表达式引擎 & 错误处理
│   └── src/main/java/.../core/
│       ├── expression/          ExpressionEvaluator (MVEL)
│       └── transform/           CamelDeadLetterHandler
├── etl-load/                    数据分发与持久化
│   └── src/main/java/.../load/
│       ├── persist/             PersistHandler (JDBC)
│       ├── dispatch/            ConsumerDispatchService (PUSH)
│       ├── LoadRouter.java      Camel 入口 → 分发 + 缓存
│       ├── ConsumerRegistry.java
│       └── ResultCache.java     PULL 模式内存缓存 (TTL)
├── config/
│   ├── samples/                 Pipeline JSON 示例 × 6 个
│   ├── routes/                  Camel YAML DSL 示例
│   └── sql/                     PostgreSQL 初始化 DDL
└── docker-compose.yml           PostgreSQL 开发环境
```

---

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Java | 22 | 运行环境 |
| Spring Boot | 3.3.5 | 应用框架 |
| Apache Camel | 4.7.0 | ETL 管线引擎 (EIP + YAML DSL) |
| MVEL | 2.5.2 | 表达式评估（Filter） |
| Jackson | 2.17 | JSON 序列化 / 多态反序列化 |
| Hawtio | 4.2.0 | Camel 可视化监控 |
| Lombok | 1.18.34 | 样板代码 |
| PostgreSQL / H2 | — | 数据存储 |
| Gradle | 8.10 | 构建工具 |

## License

MIT
