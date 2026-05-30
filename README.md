# Generic-ETL

[![Java](https://img.shields.io/badge/Java-22-blue)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-green)](https://spring.io/projects/spring-boot)
[![Apache Camel](https://img.shields.io/badge/Apache%20Camel-4.7-orange)](https://camel.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

JSON 配置驱动的轻量 ETL 引擎，基于 **Spring Boot 3.3 + Apache Camel**，支持多数据源抽取、可组合 Transform 链、混合落盘策略和下游 Schema 注册式消费。

---

## 架构

```
 config/samples/*.json          PipelineController         ConsumerController
        │                              │                         │
        ▼                              ▼                         ▼
 PipelineConfigParser          PipelineExecutionService    ConsumerDispatchService
        │                              │                         │
        ▼                              ▼                         ▼
   PipelineConfig ──────► Extract (JDBC/CSV) ──► TransformChain ──► LoadRouter
                                                      │                  │
                                              ┌───────┴──────┐    ┌─────┴─────┐
                                              │ filter, rename│    │ Persist   │
                                              │ typeCast      │    │ (threshold│
                                              │ aggregate     │    │  -based)  │
                                              │ join          │    │ PUSH/PULL │
                                              └──────────────┘    └───────────┘
```

## 模块

| 模块 | 职责 |
|------|------|
| `etl-common` | 数据模型：PipelineConfig、Row、Schema、DTO |
| `etl-core` | 核心引擎：ConfigParser、ExpressionEvaluator、TransformChain |
| `etl-extract` | 抽取层：JDBC (Oracle/MySQL/PostgreSQL)、CSV，游标分页 |
| `etl-transform` | 转换层：Filter、Rename、TypeCast、Aggregate、Join |
| `etl-load` | 加载层：混合落盘、InMemoryStore、PUSH/PULL 分发 |
| `etl-api` | Web 层：REST API、Cron 调度、重试、执行日志 |

## 快速开始

### 环境要求

- Java 21+
- Gradle 8+（或使用 `./gradlew`）

### 启动

```bash
# 1. 启动 PostgreSQL（可选，默认用 H2 内存库）
docker-compose up -d

# 2. 构建并启动
./gradlew :etl-api:bootRun

# 应用运行在 http://localhost:8080
```

### 一分钟示例

```bash
# 注册 Pipeline（带 cron 定时调度）
curl -X POST http://localhost:8080/api/pipelines/register \
  -H "Content-Type: application/json" \
  -d @config/samples/etl_csv_sample.json

# 立即执行
curl -X POST http://localhost:8080/api/pipelines/csv-sales-etl/execute

# 注册下游消费者
curl -X POST http://localhost:8080/api/consumers/register \
  -H "Content-Type: application/json" \
  -d @config/samples/consumer_register.json

# 下游拉取数据
curl "http://localhost:8080/api/consumers/data/csv-sales-etl?consumer=payroll-service"
```

---

## Pipeline JSON 配置

一条 Pipeline 由 **数据源 → Transform 链 → 输出策略** 三部分组成。

### 完整结构

```jsonc
{
  "pipeline": {
    "name": "my-etl",           // 唯一名称
    "version": "1.0",
    "cron": "0 */10 * * * ?"    // 可选，注册后自动按此表达式调度
  },
  "datasource": { ... },         // 数据源定义
  "inputSchema": { ... },        // 输入 Schema
  "transforms": [ ... ],         // Transform 链
  "output": { ... }              // 输出策略
}
```

### 数据源

**JDBC（Oracle / MySQL / PostgreSQL）**:

```jsonc
{
  "type": "oracle",                      // oracle | mysql | postgresql
  "connection": {
    "url": "jdbc:oracle:thin:@host:1521:db",
    "username": "{{env:ORA_USER}}",      // {{env:VAR}} 运行时从环境变量注入
    "password": "{{env:ORA_PASS}}"
  },
  "query": "SELECT id, name, dept, salary FROM employees",
  "cursor": { "column": "id", "pageSize": 5000 }   // 游标分页，可选
}
```

**CSV**:

```jsonc
{
  "type": "csv",
  "filePath": "/data/sales.csv",
  "delimiter": ",",
  "hasHeader": true
}
```

### Transform 类型

| type | 说明 | 关键参数 |
|------|------|---------|
| `filter` | 行级过滤 | `expression`: MVEL 表达式，如 `"salary > 0 && dept == 'Eng'"` |
| `rename` | 字段重命名 | `mappings`: `[{"from": "old", "to": "new"}]` |
| `typeCast` | 类型转换 | `mappings`: `[{"field": "price", "toType": "DECIMAL"}]` |
| `aggregate` | 分组聚合 | `groupBy`, `aggregations`: `[{"field", "function", "alias"}]` |
| `join` | 同库 JOIN | `query`, `on`, `joinType`: `INNER` / `LEFT` |

支持的聚合函数: `SUM`、`AVG`、`COUNT`、`MIN`、`MAX`

### 输出策略

```jsonc
{
  "persist": {
    "enabled": true,
    "threshold": 10000,                  // 行数 > 阈值才落盘
    "storage": {
      "type": "postgresql",              // postgresql | clickhouse
      "table": "etl_output.my_result"
    }
  }
}
```

- 行数 ≤ threshold → 仅走 API 分发（PUSH/PULL）
- 行数 > threshold → API 分发 + 写入中间表

---

## 下游消费

下游通过注册 **Output Schema** 订阅 Pipeline 输出：

```jsonc
POST /api/consumers/register
{
  "consumer": { "name": "my-service", "endpoint": "http://..." },
  "subscriptions": [{
    "pipeline": "my-etl",
    "outputSchema": {                     // 只取需要字段
      "fields": [
        {"name": "dept", "type": "STRING"},
        {"name": "total_salary", "type": "DECIMAL"}
      ]
    },
    "filter": "total_salary > 50000",      // 下游侧额外过滤
    "delivery": { "mode": "PULL", "batchSize": 200 }   // PULL | PUSH
  }]
}
```

- **PULL**: 下游主动调 `GET /api/consumers/data/{pipeline}?consumer=xxx`
- **PUSH**: ETL 完成后自动 POST 到 consumer endpoint

---

## API 参考

### Pipeline 管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/pipelines/register` | 注册 Pipeline（含 cron 则自动调度） |
| `DELETE` | `/api/pipelines/{name}` | 注销 Pipeline |
| `GET` | `/api/pipelines` | 列出所有已注册 Pipeline |
| `GET` | `/api/pipelines/{name}` | 查看 Pipeline JSON |

### Pipeline 执行

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/pipelines/execute` | 直接传入 JSON 执行（一次性） |
| `POST` | `/api/pipelines/{name}/execute` | 按名执行已注册 Pipeline |
| `POST` | `/api/pipelines/{name}/retry?runId=xxx` | 重跑指定失败任务 |
| `GET` | `/api/pipelines/runs` | 全部执行历史 |
| `GET` | `/api/pipelines/runs?pipeline=xxx` | 按 Pipeline 过滤历史 |
| `GET` | `/api/pipelines/runs/{runId}` | 单次执行详情 |

### 消费者管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/consumers/register` | 注册消费者及订阅 |
| `GET` | `/api/consumers` | 列出所有消费者 |
| `DELETE` | `/api/consumers/{name}` | 注销消费者 |
| `GET` | `/api/consumers/data/{pipeline}?consumer=xxx` | PULL 模式拉数据 |

---

## 灾备 & 容错

| 能力 | 实现 |
|------|------|
| **失败重试** | 自动重试 3 次，指数退避 1s → 2s → 4s |
| **人工恢复** | `POST /api/pipelines/{name}/retry?runId=xxx` |
| **执行日志** | 每次执行记录状态、行数、耗时、错误堆栈 |
| **配置持久化** | Pipeline 注册后存于内存仓库，重启后需重新注册 |
| **连接池** | HikariCP 连接池，支持故障转移配置 |

---

## 大表抽取

`JdbcExtractor` 使用游标分页：

```sql
SELECT * FROM (原始查询) _cursor WHERE cursor_col > ? ORDER BY cursor_col FETCH FIRST ? ROWS ONLY
```

每批 `pageSize` 行，内存友好，支持断点续跑。需上游表有单调递增列。

---

## 项目结构

```
Generic-ETL/
├── etl-api/          Spring Boot 入口 + REST 控制器 + 调度
├── etl-common/       数据模型 (PipelineConfig, Row, DTO...)
├── etl-core/         解析器 + TransformChain + 表达式引擎
├── etl-extract/      JDBC/CSV 抽取器 + 游标分页
├── etl-transform/    Filter / Rename / TypeCast / Aggregate / Join
├── etl-load/         混合落盘 + ConsumerDispatch + InMemoryStore
├── config/samples/   4 个 Pipeline + Consumer 注册示例
├── config/sql/       PostgreSQL 初始化 DDL
└── docker-compose.yml
```

## License

MIT
