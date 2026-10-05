# Java 数据治理服务

该模块位于离线数仓的治理服务层。Python/PySpark 负责采集、批量加工和规则计算，MySQL 保存资产、血缘、质量规则、运行结果和任务状态；Java 17、Spring Boot 与 Spring JDBC 提供 REST API，负责治理元数据查询以及质量任务控制。

## 查询接口

- `GET /api/v1/overview`：资产、血缘、规则及最新质量运行概览。
- `GET /api/v1/assets`：按关键词和数仓层级分页检索资产。
- `GET /api/v1/assets/{assetKey}/lineage`：查询资产上下游表级血缘。
- `GET /api/v1/quality-runs`：按状态分页查询质量运行。
- `GET /api/v1/quality-runs/{runId}/results`：查询逐项质量规则结果。
- `GET /api/v1/quality-rules`：查询质量规则。

## 规则与任务接口

- `POST /api/v1/quality-rules`：新增质量规则，重复编号返回 HTTP 409。
- `PATCH /api/v1/quality-rules/{ruleKey}/status`：启用或停用规则。
- `POST /api/v1/quality-jobs`：按 `jobKey` 幂等提交质量任务。
- `GET /api/v1/quality-jobs/{jobId}`：查询任务状态和执行次数。
- `POST /api/v1/quality-jobs/{jobId}/start`：以乐观锁将排队任务转为运行中。
- `POST /api/v1/quality-jobs/{jobId}/fail`：登记失败；剩余重试次数充足时自动回到排队状态，否则终止失败。
- `POST /api/v1/quality-jobs/{jobId}/complete`：登记成功并关联 `dq_run` 质量运行结果。

所有 SQL 固定在 Repository 并使用参数绑定；Service 层通过事务维护规则和任务状态，`version_no` 防止并发请求覆盖状态。Bean Validation 处理请求字段校验，统一异常处理返回 HTTP 400、404 或 409。分页大小上限为 100，不提供任意 SQL 执行接口。
