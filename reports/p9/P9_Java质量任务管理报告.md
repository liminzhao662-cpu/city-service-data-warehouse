# P9 Java 质量任务管理报告

## 模块定位

本阶段没有改变项目的数据开发主线。Python 与 Spark SQL 继续负责百万级采集、批量加工和质量计算；Java 服务负责质量规则配置、任务状态管理以及治理结果查询，使批处理链路具备可被调度平台调用的控制接口。

## 数据库设计

新增 `dq_job` 表，保存唯一任务编号、发布版本、目标资产、任务状态、执行次数、最大重试次数、错误原因、关联质量运行和版本号。`job_key` 唯一约束保证重复提交不产生第二个任务；`version_no` 与带版本条件的更新语句实现乐观锁，防止两个请求同时覆盖状态。

任务状态为 `QUEUED`、`RUNNING`、`SUCCEEDED`、`FAILED`。任务启动时增加执行次数；运行失败且尚有重试额度时重新回到 `QUEUED`，超过额度后进入 `FAILED`；成功任务可关联 `dq_run`，从任务控制记录追溯到逐项质量结果。

## Java 实现

- 使用 Controller、Service、Repository、Model 分层。
- 使用 Bean Validation 校验规则编号、严重级别、比较方式、重试次数和错误信息长度。
- Service 写操作使用 Spring 事务；Repository 使用 Spring JDBC 参数化 SQL。
- 任务提交按 `jobKey` 幂等；相同编号但参数不同返回 HTTP 409。
- 状态更新同时校验当前状态和 `version_no`，更新行数不为 1 时返回并发冲突。
- 失败处理根据 `attempt_count` 与 `max_retries` 自动决定回队或终止。

## 验证结果

- Java 17 编译成功，9 项单元测试全部通过。
- MySQL 实际建表成功。
- 规则新增、停用接口验证通过，测试规则已清理，正式规则仍为 10 项。
- Spark质量作业改为从MySQL读取已启用规则；停用DQ010并重新注册配置后仍只读取9项，验证接口配置会影响实际执行。
- 相同 `jobKey` 重复提交返回同一 `jobId`。
- 重试链路按 `QUEUED → RUNNING → QUEUED → RUNNING → FAILED` 流转。
- 对终止任务再次启动返回 HTTP 409。
- 成功任务能够关联已有 `dq_run` 质量运行结果。
- 集成测试任务和规则均已清理，机器结果位于 `reports/p9/java_quality_job_integration.json`。

## 边界

Java 服务管理任务控制面，不在 HTTP 请求线程中直接执行 Spark 全量扫描。实际调度器或批处理执行器领取排队任务后运行质量作业，再调用完成或失败接口更新状态。项目未实现登录鉴权、消息队列、分布式调度或高并发压测，因此不描述为生产级微服务平台。
