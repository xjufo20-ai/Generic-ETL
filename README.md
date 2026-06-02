# Generic-ETL

[![Java](https://img.shields.io/badge/Java-22-blue)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-green)](https://spring.io/projects/spring-boot)
[![Apache Camel](https://img.shields.io/badge/Camel-4.7.0-orange)](https://camel.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

JSON 配置驱动的 ETL 引擎，基于 **Spring Boot 3.3 + Apache Camel 4.7**。一条 JSON → 一条 Camel Route，原生 EIP 模式（Filter、Aggregate、Split、Multicast）。

---

## 架构

```
 config/samples/*.json
        │
        ▼
 PipelineConfigParser ──► CamelRouteFactory
        │                      │
        ▼                      ▼
  PipelineConfig          Camel Route (EIP native)
                          ┌────────────────────────┐
                          │ from(jdbc:/kafka:/...)  │
                          │   .filter(expr)         │  ← Filter EIP
                          │   .process(rename)      │  ← Rename
                          │   .aggregate(strategy)  │  ← Aggregate EIP
                          │   .enrich(jdbc:...)     │  ← Join EIP
                          │   .split(body())        │  ← Split EIP
                          │   .multicast()          │  ← Multicast EIP
                          │     .to(persist)        │
                          │     .to(dispatch)       │
                          │   .end()                 │
                          └────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
              PersistHandler  ConsumerDispatch  Hawtio Console
                 (JDBC)      (PUSH/PULL REST)   (/hawtio)
```

**关键设计**: Pipeline JSON 直接映射为 Camel Route，利用 Camel 原生 EIP 替代自写 TransformChain。
Camel 提供声明式错误处理 (`onException`)、自动重试、JMX 监控。

## 模块

| 模块 | 职责 |
|------|------|
| `etl-common` | 数据模型：PipelineConfig, TransformDef, Schema, DTOs |
| `etl-core` | 核心：ConfigParser, ExpressionEvaluator, CamelDeadLetterHandler |
| `etl-extract` | 抽取层：Jdbc/Csv/Kafka/Sftp Extractor + ExtractorRegistry |
| `etl-transform` | 转换层：Filter, Rename, TypeCast, Aggregate, Join, Split |
| `etl-load` | 加载层：PersistHandler, ConsumerDispatch, ConsumerRegistry |
| `etl-api` | Web 层：REST API, Dashboard, Security, Metrics, CamelRouteFactory |

## 快速开始

```bash
# 启动
./gradlew :etl-api:bootRun

# Hawtio 监控控制台 (Camel Routes 可视化)
open http://localhost:8080/hawtio

# Swagger
open http://localhost:8080/swagger-ui.html
```

## Pipeline JSON 配置

```jsonc
{
  "pipeline": {
    "name": "my-etl",
    "version": "1.0",
    "cron": "0 */10 * * * ?",
    "dependsOn": ["upstream-pipeline"],
    "tenant": "team-a"
  },
  "datasource": { ... },
  "watermark": {
    "column": "updated_at",
    "initial": "2024-01-01"
  },
  "inputSchema": { ... },
  "transforms": [ ... ],
  "output": { ... }
}
```

### 数据源

**JDBC (Oracle/MySQL/PostgreSQL)**:

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

**Kafka / CSV / SFTP** 同样支持，详见 `config/samples/`。

`{{env:VAR}}` 运行时从环境变量注入。

### Transform 类型

| type | 说明 | Camel EIP |
|------|------|-----------|
| `filter` | 行级过滤 | `.filter(predicate)` |
| `rename` | 字段重命名 | `.process(rename)` |
| `typeCast` | 类型转换 | `.process(cast)` |
| `aggregate` | 分组聚合 | `.aggregate(strategy)` |
| `join` | 关联查询 | `.enrich(jdbc:...)` |
| `split` | 数据拆分 | `.split(body())` |

## API 端点

| 端点 | 说明 |
|------|------|
| `POST /api/pipelines/register` | 注册 Pipeline（自动创建 Camel Route） |
| `DELETE /api/pipelines/{name}` | 注销 Pipeline（停止并移除 Route） |
| `POST /api/pipelines/execute` | 直接执行 Pipeline JSON |
| `POST /api/pipelines/{name}/execute` | 按名称触发执行 |
| `GET /api/pipelines` | 列出所有 Pipeline |
| `GET /api/pipelines/routes` | 列出所有 Camel Routes |
| `GET /api/pipelines/{name}/audit` | 审计日志 |
| `GET /api/pipelines/lineage` | 数据血缘 |

## 监控

### Hawtio 控制台

`http://localhost:8080/hawtio` — Camel Routes 可视化、JMX 指标、实时调试。

### Actuator

| 端点 | 指标 |
|------|------|
| `/actuator/health` | 健康检查 |
| `/actuator/metrics` | `etl.pipelines.executed`, `etl.rows.extracted` |
| `/actuator/jolokia` | JMX (Hawtio 后端) |

## 项目结构

```
Generic-ETL/
├── etl-api/          Spring Boot + CamelRouteFactory + REST + Hawtio
├── etl-common/       PipelineConfig, TransformDef, Schema, Row, DTOs
├── etl-core/         ConfigParser, ExpressionEvaluator, CamelDeadLetterHandler
├── etl-extract/      Jdbc/Csv/Kafka/Sftp Extractor
├── etl-transform/    Filter, Rename, TypeCast, Aggregate, Join, Split
├── etl-load/         PersistHandler, ConsumerDispatch, ConsumerRegistry
├── config/samples/   Pipeline + Consumer 示例
└── docker-compose.yml
```

## License

MIT
