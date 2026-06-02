# Apache Camel 集成深度评估与重构建议

> 资深工程师视角的代码审查 | 2026-05-30

## 一、总体评价

| 维度 | 评分 | 说明 |
|------|------|------|
| 模块划分 | ⭐⭐⭐⭐ | etl-common/core/extract/transform/load/api 六模块清晰 |
| JSON 配置模型 | ⭐⭐⭐⭐ | 多态 TransformDef、环境变量注入设计合理 |
| Camel 集成深度 | ⭐ | 名存实亡，仅用 ProducerTemplate 替代 JdbcTemplate |
| 代码一致性 | ⭐⭐⭐ | 存在双引擎、命名风格不一致 |
| 可扩展性 | ⭐⭐⭐ | 插件式 Extractor 好，但 Camel 组件未用 |

## 二、Camel 集成现状 —— 问题拆解

### 2.1 双引擎架构：增加复杂度，未带来收益

```
当前架构：
                    PipelineExecutionService
                    /                      \
         engine=="java"              engine=="camel"
              /                              \
   ExtractorRegistry                   CamelPipelineEngine
   → TransformPipeline                 → ProducerTemplate (仅JDBC!)
   → LoadRouter                        → 手动Java Stream Transform
                                       → 无Camel Route
```

**问题**：两条路径本质相同——都是 Java Stream transform。Camel 路径反而更弱（不支持 parallel、不支持 DLQ、不支持 cursor 分页）。

### 2.2 CamelPipelineEngine 的四个致命缺陷

```java
// 问题 1: 用 ProducerTemplate 查 JDBC，本质是换皮的 JdbcTemplate
List<Map<String, Object>> result = template.requestBody(sourceUri, query, List.class);

// 问题 2: Transforms 仍然用 Java Stream，完全没用 Camel Processor
for (Row row : rows) {
    for (TransformDef t : config.getTransforms()) {
        if (t instanceof TransformDef.FilterDef f) {
            if (ExpressionEvaluator.evaluate(row, f.getExpression())) trxCount++;
        }
    }
}

// 问题 3: 不支持 cursor 分页、不支持 parallel、不支持 watermark
// 问题 4: 不支持 Kafka、SFTP 作为 source URI（有 buildSourceUri 但路由未注册）
```

### 2.3 未使用 Camel 的核心能力

| Camel 能力 | 当前状态 | 应该怎么用 |
|------------|----------|-----------|
| **Route DSL** | 未使用 | Pipeline JSON → RouteBuilder 动态注册路由 |
| **EIP 模式** | 全部自实现 | 用 Camel 的 split/aggregate/filter/contentBasedRouter |
| **200+ Component** | 仅 camel-jdbc | 直接用 camel-kafka、camel-ftp、camel-csv 等 |
| **Error Handler** | 自写 RetryHandler | 用 Camel 的 onException/redeliveryPolicy/deadLetterChannel |
| **背压 & 限流** | 不支持 | Camel throttler + circuitBreaker |
| **Hawtio 监控** | 自写 Dashboard | 利用 Camel 的 JMX + Hawtio 开箱即用 |

## 三、重构方案：Camel-Native 架构

### 3.1 目标架构

```
config/samples/etl_trade_stats.json
            │
            ▼
    CamelRouteBuilder (NEW)
            │
            ▼
    CamelContext
    ┌──────────────────────────────────────────────────┐
    │                                                    │
    │  from("jdbc:datasource?...")                       │
    │    .routeId("pipeline-name")                       │
    │    .split(body())                                  │  ← Camel EIP
    │    .filter(exchange -> ...)                        │
    │    .process(new RenameProcessor())                 │
    │    .aggregate(header("group"), new GroupStrategy())│
    │    .marshal().json()                                │
    │    .multicast()                                    │  ← 同时分发
    │      .to("jdbc:output?…")                          │
    │      .to("direct:consumerDispatch")                 │
    │    .end()                                           │
    │                                                    │
    └──────────────────────────────────────────────────┘
```

### 3.2 核心思想：PipelineConfig → Camel Route

```java
@Component
public class CamelRouteFactory {
    
    @Autowired private CamelContext camelContext;
    
    /**
     * 将 PipelineConfig 动态注册为一条 Camel Route。
     * 每次 register/update 都先移除旧 Route，再添加新 Route。
     */
    public void registerRoute(PipelineConfig config) throws Exception {
        String routeId = "etl-" + config.getPipeline().getName();
        
        // 1. 先移除已有的同名 Route
        camelContext.getRouteController().stopRoute(routeId);
        camelContext.removeRoute(routeId);
        
        // 2. 动态构建新的 Route
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // 2a. 全局错误处理
                onException(Exception.class)
                    .handled(true)
                    .maximumRedeliveries(3)
                    .redeliveryDelay(5000)
                    .to("bean:deadLetterHandler");
                
                // 2b. 数据源端点
                from(buildSourceUri(config))
                    .routeId(routeId)
                    
                    // 2c. 转换链（每个 transform 是一个 .process() 或 Camel EIP）
                    .process(buildTransformChain(config))
                    
                    // 2d. 输出（multicast: 落盘 + 分发消费者）
                    .multicast().parallelProcessing()
                        .to(buildOutputUri(config))
                        .to("bean:consumerDispatchService")
                    .end()
                    
                    // 2e. 指标 & 审计
                    .to("bean:etlMetrics?method=recordSuccess")
                    .to("bean:auditLog?method=recordExecution");
            }
        });
    }
    
    private String buildSourceUri(PipelineConfig config) {
        return switch (config.getDatasource()) {
            case DataSourceConfig.JdbcDataSource j -> 
                String.format("jdbc:etlDataSource?outputType=StreamList&query=#%s", j.getQuery());
            case DataSourceConfig.KafkaDataSource k ->
                String.format("kafka:%s?brokers=%s&groupId=%s", 
                    k.getConnection().getTopic(), 
                    k.getConnection().getBootstrapServers(),
                    k.getConnection().getGroupId());
            case DataSourceConfig.CsvDataSource c ->
                String.format("file:%s?noop=true&charset=UTF-8", c.getFilePath());
            default -> "direct:noop";
        };
    }
}
```

### 3.3 Transform 映射：自写 Processor → Camel EIP

| 当前实现 | 映射到 Camel | 优势 |
|----------|-------------|------|
| `FilterProcessor.processStream()` | `.filter(exchange -> ...)` 或 `.filter().method(ExpressionEvaluator.class)` | Camel 原生 filter，支持 predicate |
| `AggregateProcessor.processSet()` | `.aggregate(header("group"), new AggregationStrategy())` | Camel 聚合器自带超时/完成策略 |
| `SplitProcessor` | `.split(body())` | Camel splitter 支持并行 |
| `JoinProcessor` | `.enrich("jdbc:...")` | Camel content enricher |
| `RenameProcessor` | `.process(new RenameProcessor())` | 保持 processor，但用 Camel Exchange |
| `TypeCastProcessor` | `.process(new TypeCastProcessor())` | 同上 |

### 3.4 删除冗余代码

重构后可删除/合并的文件：

```
删除:
  etl-core/src/main/java/.../TransformPipeline.java     → Camel Route 替代
  etl-core/src/main/java/.../TransformChain.java        → Camel Route 替代
  etl-api/src/main/java/.../CamelPipelineEngine.java    → CamelRouteFactory 替代
  etl-api/src/main/java/.../PipelineExecutionService.java → CamelRouteFactory + Scheduler 替代
  etl-api/src/main/java/.../RetryHandler.java           → Camel onException 替代

保留但改造:
  etl-extract/src/main/java/.../Extractor.java          → 改为 Camel endpoint 工厂
  etl-transform/src/main/java/.../TransformProcessor.java → 改为 Camel Processor 接口
  etl-load/src/main/java/.../LoadRouter.java            → 改为 Camel multicast endpoint
```

## 四、迁移路径（分阶段）

### Phase 1: 统一到单引擎（低风险，1-2天）

1. 删除 `engine` 字段和双引擎判断逻辑
2. 所有执行统一走 `CamelRouteFactory.registerRoute()`
3. 保留现有 `TransformProcessor` 实现，包装为 Camel `Processor`
4. 保留现有 `Extractor` 实现，改为生成 Camel endpoint URI

### Phase 2: 迁移 Transform 到 Camel EIP（中风险，2-3天）

1. `FilterProcessor` → `.filter()`
2. `AggregateProcessor` → `.aggregate()`
3. `SplitProcessor` → `.split()`
4. `JoinProcessor` → `.enrich()`
5. 引入 Camel CSV 组件处理 CSV 解析

### Phase 3: 利用 Camel 运维能力（低风险，1-2天）

1. 配置 Camel Hawtio 管理控制台（替代自建 Dashboard）
2. 使用 Camel 的 JMX MBeans 暴露 Pipeline 指标
3. 利用 Camel Tracer + NotifyBuilder 做 Pipeline 监控

### Phase 4: 高级特性（按需）

1. **Circuit Breaker**: `circuitBreaker()` 防止下游故障传导
2. **Throttler**: `throttle(100).timePeriodMillis(1000)` 限流
3. **Saga**: 分布式事务支持
4. **Wire Tap**: 实时调试，旁路拷贝数据到日志

## 五、代码规范建议

### 5.1 命名一致性

| 当前 | 建议 | 原因 |
|------|------|------|
| `PipelineExecutionService` | `PipelineExecutor` | 去掉 Service 后缀，更直接 |
| `CamelPipelineEngine` | 删除，合并到 `CamelRouteFactory` | 消除双引擎混淆 |
| `TransformDef` (etl-common) | `TransformStep` | Def 不是 Java 惯用名 |
| `PersistConfig` / `LoadRouter` | `SinkConfig` / `SinkRouter` | ETL 标准术语 |
| `extract/transform/load` 包名 | 保留 | 清晰 |

### 5.2 包结构优化

```
com.generic.etl
├── common/          # 不变
├── core/
│   ├── route/       # NEW: CamelRouteFactory, RouteValidator
│   ├── expression/  # ExpressionEvaluator
│   └── dq/          # DeadLetterQueue
├── extract/         # → 改为 CamelEndpointFactory
├── transform/       # → 改为 Camel Processor 实现
├── load/
│   ├── persist/
│   └── dispatch/
└── api/
    ├── controller/
    ├── store/       # AuditLog, LineageStore, StateStore
    ├── security/
    └── metrics/
```

## 六、结论

**当前代码的基础架构是好的**（模块划分、JSON 配置模型、审计/血缘），但 **Camel 集成的深度严重不足**。引入 Camel 但不用其 Route DSL、EIP、Component 生态，相当于"买了一辆跑车却用手推着走"。

**核心建议**：
1. **立即**: 将 PipelineConfig 映射为 Camel Route，消除双引擎
2. **短期**: 用 Camel EIP 替代自写的 TransformChain
3. **中期**: 利用 Camel 组件（kafka、ftp、csv）替代自写的 Extractor/Loader
4. **长期**: 引入 Hawtio 替代自建 Dashboard

这样改完后，新增一个 Pipeline 类型（比如 Kafka→MongoDB）**只需要写一个 JSON 配置 + 引入对应的 Camel Starter 依赖**，不需要再写任何 Java 代码。这才是 Camel 的真正价值。
