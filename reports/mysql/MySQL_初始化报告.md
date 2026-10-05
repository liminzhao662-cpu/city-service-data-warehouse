# MySQL 初始化报告

状态：MySQL 已安装、启动并完成核心 DDL。

- 版本：MySQL Community Server 8.4.11 LTS。
- 程序与数据目录：`E:\nyc311_runtime`，使用纯英文路径避免 Windows 便携版路径兼容问题。
- 网络：只监听 `127.0.0.1:3307`。
- 数据库：`nyc311`，字符集 `utf8mb4`。
- 账户：项目账户 `nyc311_app@127.0.0.1`，密码随机生成并仅保存在被 Git 忽略的 `.env`。
- 核心表：`pipeline_run`、`staging_ticket`、`ticket_current`、`ticket_observation`、`release_registry`。
- DDL：`sql/ddl/001_core.sql`。
- 启停脚本：`scripts/mysql/start_mysql.ps1`、`scripts/mysql/stop_mysql.ps1`。

已验证项目账户可连接 `nyc311`，服务器返回版本 8.4.11；数据库共有 5 张核心表和 15 项约束/键定义。

当前还没有将 365 万条数据导入 MySQL。下一步先完成 Spark/转换环境和 5 万条纵向导入，再运行全年转换与加载。

