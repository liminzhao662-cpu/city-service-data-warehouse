# NYC 311 选题验证实验

这是数据源检查与实验方案阶段，不是已完成的数据平台。

- [正式大样本项目方案](../NYC311_大样本数仓项目方案_V1.md)
- `probe_source.py`：通过官方 API 有界取样、查询元数据及总数，保存原始 JSON 和请求清单。
- `profile_sample.py`：离线读取样本，输出质量概况，不修正原始字段。
- `sample_profile.json`：本次样本分析结果。
- `evidence/<UTC时间>/`：保存 API 响应、请求 URL、时间、HTTP 状态、字节数与 SHA256。

Python 3.11 标准库即可，无需安装数据库或 Docker。

```powershell
python nyc311_experiment/probe_source.py
python nyc311_experiment/probe_source.py --followup
python nyc311_experiment/profile_sample.py
```

前两个命令需要联网；日期固定用于重访同一批数据，未来查询结果可能变化。第三个命令离线运行，选取各类最新证据文件。不要把不同时点的文件自动认定为一致性快照。

`recent_sample` 是 2026-09-14 起按创建时间排序的前 5000 条，实际只覆盖至当天 12:23，不能当作一整周样本或随机样本。`historical_sample` 同样是按时间截取的探索样本。

`cohort_rows` 为 BRONX、2026-09-14 创建工单的有界查询，本次返回 2056 条，与独立 count 查询相等。它们不是同一事务内的服务端快照；本次数量相等且主键唯一是有限对账证据，不是上游永不遗漏的证明。该响应上限是 5000，未来若返回条数达到上限或与 count 不同，需实现分页后再称完整范围。

处理时长使用源字段的无时区日期差，尚未处理纽约夏令时歧义；仅用于本次不跨时钟切换的探索。生产口径需单独确定。状态/关闭时间不一致被标记为待解释语义组合，不直接宣布源记录错误。

本次没有运行 Spark、Airflow、Docker、AI 或增量生产链路，没有跨日观察到同一工单状态迁移，也没有模拟数据冒充真实更新。
