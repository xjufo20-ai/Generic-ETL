# Generic-ETL

[![Java](https://img.shields.io/badge/Java-22-blue)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-green)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

JSON 配置驱动的轻量 ETL 引擎，基于 **Spring Boot 3.3 + JdbcTemplate + HikariCP**，支持全量/增量抽取、6 种 Transform、混合落盘、下游注册式消费、RBAC 安全、Dashboard 控制台。

---

## 架构

```
 config/samples/*.json       Dashboard (/dashboard)        ConsumerController
        │                           │                            │
        ▼                           ▼                            ▼
 PipelineConfigParser ──► PipelineExecutionService ──► ConsumerDispatchService
        │                           │                            │
        ▼                           ▼                            ▼
   PipelineConfig ────► Extract (JDBC/CSV) ──► TransformChain ──► LoadRouter
      │                        │                       │                │
      ├─ WatermarkConfig       ├─ JdbcExtractor        ├─ Filter       ├─ Persist
      ├─ ParallelConfig        │  (JdbcTemplate)       ├─ Rename       │  (threshold)
      ├─ dependsOn             │                       ├─ TypeCast     ├─ PUSH
      └─ tenant                ├─ CsvExtractor         ├─ Aggregate    └─ PULL
                               └─ WatermarkStore       ├─ Join
                                                       └─ Split
                                                       DeadLetterQueue
```

## 模块

| 模块 | 职责 |
|------|------|
| `etl-common` | 数据模型：PipelineConfig, TransformDef, Schema, DTOs |
| `etl-core` | 核心引擎：ConfigParser, ExpressionEvaluator, TransformPipeline, DeadLetterQueue |
| `etl-extract` | 抽取层：JdbcExtractor(JdbcTemplate), CsvExtractor, WatermarkStore |
| `etl-transform` | 转换层：Filter, Rename, TypeCast, Aggregate, Join, **Split** |
| `etl-load` | 加载层：PersistHandler, ConsumerDispatch, InMemoryDataStore, ConsumerRegistry |
| `etl-api` | Web 层：REST API, Dashboard, Security, Metrics, StateStore, LineageStore |

## 快速开始

```bash
# 启动（H2 内存库，无需外部依赖）
./gradlew :etl-api:bootRun

# 打开 Dashboard
open http://localhost:8080/dashboard

# Swagger
open http://localhost:8080/swagger-ui.html
```

## Pipeline JSON 配置

```jsonc
{
  "pipeline": {
    "name": "my-etl",
    "version": "1.0",
    "cron": "0 */10 * * * ?",       // 可选：定时调度
    "dependsOn": ["upstream-pipeline"], // 可选：依赖
    "tenant": "team-a"                // 可选：多租户
  },
  "datasource": { ... },
  "watermark": {                      // 可选：增量抽取
    "column": "updated_at",
    "initial": "2024-01-01"
  },
  "parallel": {                       // 可选：并行处理
    "extractPartitions": 4,
    "transformThreads": 2
  },
  "inputSchema": { ... },
  "transforms": [ ... ],
  "output": { ... }
}
```

### 数据源

**JDBC（Oracle / MySQL / PostgreSQL）** — 基于 Spring `JdbcTemplate` + `HikariCP` 连接池：

```jsonc
{
  "type": "mysql",
  "connection": {
    "url": "jdbc:mysql://{{env:DB_HOST}}:3306/db",
    "username": "{{env:DB_USER}}",
    "password": "{{env:DB_PASS}}"
  },
  "query": "SELECT id, name, salary FROM employees",
  "cursor": { "column": "id", "pageSize": 5000 }
}
```

`{{env:VAR}}` 运行时从环境变量注入，密码不存明文。

**Kafka** — 通过 Apache Camel Kafka 组件消费消息：

```jsonc
{
  "type": "kafka",
  "connection": {
    "bootstrapServers": "localhost:9092",
    "topic": "input-topic",
    "groupId": "etl-group"
  }
}
```

**SFTP** — 通过 Apache Camel FTP 组件拉取远程文件：

```jsonc
{
  "type": "sftp",
  "connection": {
    "host": "sftp.example.com",
    "port": 22,
    "username": "{{env:SFTP_USER}}",
    "password": "{{env:SFTP_PASS}}",
    "directory": "/incoming"
  },
  "fileName": "*.csv"
}
```

**CSV** — 纯 Java NIO，流式读取：

```jsonc
{
  "type": "csv",
  "filePath": "/data/sales.csv",
  "delimiter": ",",
  "hasHeader": true
}
```

### Transform 类型

| type | 说明 | JSON 示例 |
|------|------|-----------|
| `filter` | 行级过滤 | `{"type":"filter", "expression":"salary > 0 && dept == 'Eng'"}` |
| `rename` | 字段重命名 | `{"type":"rename", "mappings":[{"from":"name","to":"employee_name"}]}` |
| `typeCast` | 类型转换 | `{"type":"typeCast", "mappings":[{"field":"price","toType":"DECIMAL"}]}` |
| `aggregate` | 分组聚合 | `{"type":"aggregate", "groupBy":["dept"], "aggregations":[{"field":"salary", "function":"SUM", "alias":"total"}]}` |
| `join` | 同库 JOIN | `{"type":"join", "query":"SELECT name FROM dept WHERE id = :dept_id", "leftKey":"dept_id", "rightKey":"1", "joinType":"INNER"}` |
| `split` | 拆分行 | `{"type":"split", "field":"tags", "delimiter":","}` |

聚合函数: `SUM`, `AVG`, `COUNT`, `MIN`, `MAX`

### 增量抽取

配置 `watermark` 字段后，每次抽取记录水位值。下次执行自动追加 `WHERE watermark_col > 'last_value'`，水位持久化在 `data/watermarks.json`。

### 输出策略

```jsonc
{
  "output": {
    "enabled": true,
    "threshold": 10000,           // 行数 > 阈值才落盘
    "storage": {
      "type": "postgresql",
      "table": "etl_output.result"
    }
  }
}
```

## 数据血缘

每次 Pipeline 执行成功后自动记录血缘关系：

```
Pipeline → Output Table → Consumer → Rows → Timestamp
```

查询接口：

```bash
GET /api/pipelines/lineage                    # 全部血缘
GET /api/pipelines/lineage?pipeline=my-etl    # 按 Pipeline 过滤
```

血缘数据持久化在 `data/lineage.json`，重启不丢失。

## 数据清洗 & 验证

**执行前校验** — `PipelineConfig.validate()` 在解析 JSON 后、执行前自动运行：

| 校验项 | 说明 |
|--------|------|
| 必填字段 | pipeline.name, datasource, inputSchema.fields |
| 游标列存在性 | cursor.column 必须在 inputSchema 中声明 |
| Transform 字段引用 | rename/typeCast/aggregate 引用的字段必须在 schema 中 |

**执行中清洗** — Transform 链提供 6 种数据清洗能力：

| Transform | 清洗场景 |
|-----------|---------|
| `filter` | 剔除无效行（salary > 0, status == 'active'） |
| `typeCast` | 类型规范化（STRING→DECIMAL, 日期格式统一） |
| `rename` | 字段名标准化（source_name → target_name） |
| `split` | 拆分行（逗号分隔的 tags → 每行一个 tag） |
| `aggregate` | 去重聚合（按维度 SUM/COUNT） |
| `join` | 维度补全（事实表 JOIN 维度表） |

**错误行隔离** — DeadLetterQueue 机制：单行 Transform 失败不会阻塞全量，失败行记录日志，成功行继续流转。

## 下游消费

```jsonc
POST /api/consumers/register
{
  "consumer": { "name": "dashboard", "endpoint": "http://..." },
  "subscriptions": [{
    "pipeline": "my-etl",
    "outputSchema": {
      "fields": [{"name":"dept","type":"STRING"}, {"name":"total","type":"DECIMAL"}]
    },
    "filter": "total > 50000",
    "delivery": { "mode": "PULL", "batchSize": 200 }
  }]
}
```

- **PULL**: `GET /api/consumers/data/{pipeline}?consumer=dashboard&page=0&pageSize=500`
- **PUSH**: ETL 完成后 POST 到 consumer endpoint

## API 参考

### Pipeline

| 方法 | 路径 | 角色 |
|------|------|------|
| `POST` | `/api/pipelines/register` | ADMIN |
| `DELETE` | `/api/pipelines/{name}` | ADMIN |
| `POST` | `/api/pipelines/execute` | ADMIN, OPERATOR |
| `POST` | `/api/pipelines/{name}/execute` | ADMIN, OPERATOR |
| `POST` | `/api/pipelines/{name}/retry?runId=xxx` | ADMIN, OPERATOR |
| `GET` | `/api/pipelines` | ALL |
| `GET` | `/api/pipelines/{name}` | ALL |
| `GET` | `/api/pipelines/runs` | ALL |
| `GET` | `/api/pipelines/runs/{runId}` | ALL |
| `GET` | `/api/pipelines/lineage` | ALL |

### 消费者

| 方法 | 路径 | 角色 |
|------|------|------|
| `POST` | `/api/consumers/register` | ADMIN |
| `DELETE` | `/api/consumers/{name}` | ADMIN |
| `GET` | `/api/consumers` | ALL |
| `GET` | `/api/consumers/data/{pipeline}?consumer=xxx&page=0&pageSize=500` | ALL |

### 认证

所有 API 需 `X-API-Key` Header，配置在 `application.yml`:

```yaml
etl:
  api-keys: sk-admin:ADMIN,sk-operator:OPERATOR,sk-viewer:VIEWER
```

Dashboard (`/dashboard`) 和 Swagger 无需认证。

## 灾备 & 容错

| 能力 | 实现 |
|------|------|
| **失败重试** | RetryHandler: 3 次指数退避 (1s → 2s → 4s) |
| **错误行隔离** | DeadLetterQueue: 单行失败不阻塞全量 |
| **人工恢复** | `POST /api/pipelines/{name}/retry?runId=xxx` |
| **持久化** | StateStore: 重启不丢 Pipeline/Consumer/Watermark |
| **连接池** | HikariCP, 10 连接上限 |

## 可观测性

| 端点 | 说明 |
|------|------|
| `/dashboard` | 控制台：Pipeline 列表、最近执行、数据血缘 |
| `/actuator/metrics` | `etl.pipelines.executed`, `etl.rows.extracted`, `etl.pipeline.duration` |
| `/actuator/health` | 健康检查 |
| `/swagger-ui.html` | OpenAPI 文档 |

## 项目结构

```
Generic-ETL/
├── etl-api/          Spring Boot 入口 + REST + Dashboard + Security + Store + Metrics
├── etl-common/       PipelineConfig, TransformDef, Schema, Row, DTOs
├── etl-core/         ConfigParser, ExpressionEvaluator, TransformPipeline, DeadLetterQueue
├── etl-extract/      JdbcExtractor(JdbcTemplate), CsvExtractor, WatermarkStore
├── etl-transform/    Filter, Rename, TypeCast, Aggregate, Join, Split 实现
├── etl-load/         PersistHandler, ConsumerDispatch, InMemoryDataStore, ConsumerRegistry
├── config/samples/   5 个 Pipeline + 2 个 Consumer 示例
├── config/sql/       PostgreSQL DDL（含日/周/月聚合视图）
├── data/             运行时持久化（pipelines.json, consumers.json, watermarks.json, lineage.json）
└── docker-compose.yml
```

## License

MIT
