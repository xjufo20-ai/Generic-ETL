# OpenSpec: Camel-Native 重构

> 目标：消除双引擎，将 PipelineConfig 映射为 Camel Route，最大化发挥 Camel 框架优势

## 总体原则

- 每一步独立可验证：编译通过 + 现有测试不回归
- 每个 Step 完成后 commit，方便回滚
- 先改核心，再改周边

---

## Step 0: 建立任务文档 ✅

- **输入**: 当前代码库
- **输出**: 本文档
- **验收**: 任务拆分清晰，可独立执行

---

## Step 1: 删除双引擎

- **输入**: PipelineExecutionService.java, CamelPipelineEngine.java, PipelineConfig.java, EtlConfig.java
- **输出**: 
  - 删除 `CamelPipelineEngine.java`
  - 删除 `PipelineConfig.engine` 字段
  - `PipelineExecutionService` 移除 `engine=="camel"` 分支
  - `EtlConfig` 移除 `CamelPipelineEngine` Bean
- **验收**: `./gradlew compileJava` 通过

---

## Step 2: 创建 CamelRouteFactory

- **输入**: PipelineConfig, CamelContext
- **输出**: `CamelRouteFactory.java` — 将 PipelineConfig 转为 Camel Route
  - `registerRoute(PipelineConfig)` — 动态添加 Route
  - `removeRoute(String name)` — 移除 Route
  - `buildSourceUri(PipelineConfig)` — 生成 source endpoint
  - `buildTransformSteps(PipelineConfig)` — 生成 transform processor 链
  - `buildOutputUri(PipelineConfig)` — 生成 sink endpoint
- **验收**: 单元测试覆盖 Route 注册/移除

---

## Step 3: TransformProcessor 适配 Camel

- **输入**: TransformProcessor 接口 + 6 个实现类
- **输出**: 
  - `TransformProcessor extends org.apache.camel.Processor`
  - 每个 Processor 的 `process(Exchange)` 方法
  - Filter → `.filter()` (Camel predicate)
  - Aggregate → `.aggregate()` (Camel AggregationStrategy)
  - Rename/TypeCast → `.process(new XxxProcessor())`
  - Join → `.enrich()` (Camel content enricher)
  - Split → `.split()` (Camel splitter)
- **验收**: 现有 Transform 测试全部通过

---

## Step 4: 改造 Extractor 层

- **输入**: Extractor 接口 + Jdbc/Csv/Kafka/SftpExtractor
- **输出**:
  - 每个 Extractor 提供 `buildEndpointUri()` 方法
  - `JdbcExtractor` → `jdbc:dataSource?...`
  - `CsvExtractor` → `file:path?...`
  - `CamelKafkaExtractor` → `kafka:topic?...`
  - `CamelSftpExtractor` → `ftp:host/path?...`
  - Watermark 逻辑移到 Route 的 `.setHeader()` 步骤
- **验收**: Extract 测试通过

---

## Step 5: 改造 Load 层

- **输入**: LoadRouter, PersistHandler, ConsumerDispatchService
- **输出**:
  - `PersistHandler` 作为 Camel bean: `"bean:persistHandler?method=persistIfNeeded"`
  - `ConsumerDispatchService` 作为 Camel bean
  - Load 逻辑通过 `.multicast().to("bean:persist", "bean:dispatch")` 实现
- **验收**: Load 测试通过

---

## Step 6: 错误处理 Camel 化

- **输入**: RetryHandler.java
- **输出**:
  - 删除 `RetryHandler.java`
  - Route 中添加 `onException(Exception.class).maximumRedeliveries(3).redeliveryDelay(5000)`
  - DeadLetterQueue 改为 Camel deadLetterChannel
- **验收**: 错误场景测试通过

---

## Step 7: 装配 + 测试 + Push

- **输入**: EtlConfig.java, PipelineController.java, PipelineScheduler.java
- **输出**:
  - 更新 EtlConfig，注入 CamelRouteFactory
  - PipelineController 通过 CamelRouteFactory 注册/执行
  - PipelineScheduler 通过 CamelRouteFactory 触发
  - `./gradlew test` 全部通过
  - Push to GitHub
- **验收**: 全量测试通过，代码推送到远端
