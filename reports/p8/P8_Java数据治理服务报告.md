# P8 Java 数据治理服务报告

## 模块定位

该模块是现有数仓的数据治理服务层。Python 与 Spark SQL 负责采集、批量转换和质量计算；MySQL 保存数据资产、表级血缘、质量规则及运行结果；Java 服务只读访问这些治理元数据并向平台或前端提供接口。

技术栈：Java 17、Spring Boot 4.1.1、Spring JDBC、MySQL Connector/J、HikariCP、Maven 3.9.16。

## 已实现接口

- `GET /api/v1/overview`：治理概览与最新质量运行。
- `GET /api/v1/assets`：按关键词、数仓层级分页检索资产。
- `GET /api/v1/assets/{assetKey}/lineage`：查询资产的上下游表级血缘。
- `GET /api/v1/quality-runs`：按运行状态分页查询质量批次。
- `GET /api/v1/quality-runs/{runId}/results`：查询一次运行的逐项规则结果。

Repository、Service、Controller 分层实现。Repository 使用参数绑定执行固定 SQL；Service 负责分页上限、状态白名单和资源存在性校验；Controller 输出类型明确的 JSON；统一异常处理分别返回 HTTP 400 和 404。连接池配置为只读，不提供写入或任意 SQL 执行接口。

## 验证结果

- Maven 编译及打包成功，生成可执行 JAR。
- 3 项单元测试全部通过，覆盖分页限制、过滤条件与状态白名单。
- 连接项目 MySQL 的实际接口联调通过：返回 8 项资产、7 条血缘、10 项规则，最新质量运行状态为 `PASSED`，检查数据量为 3,655,040 行。
- 明细资产筛选返回 1 项；`dwd_ticket` 返回 4 条上下游血缘；最新质量运行返回 10 条规则结果。
- 分页大小超过 100 返回 HTTP 400，不存在的资产返回 HTTP 404。

机器可复查结果保存在 `reports/p8/java_service_integration.json`，服务启动日志保存在同目录。

## 实现边界

该服务用于证明 Java 数据访问和接口开发能力，不承担 Spark 作业调度，也不修改治理元数据。当前未加入登录鉴权、缓存和容器部署，因此简历表述为“开发只读治理查询接口”，不描述为生产级数据治理平台。
