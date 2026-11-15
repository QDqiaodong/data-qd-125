# SQLite 增量管道 v2

在不改变 v1 全量快照 CLI（`--source/--output`）的前提下，新增基于 SQLite 的增量访问日志管道。
所有业务规则（UTC 规范化、gap 会话、lookback 闭区间归因、同秒按 ID 排序、direct 回退、
unattributed 保留渠道）与 [数据契约](contract.md) 完全一致；增量结果在字段与顺序上等价于
把累计输入交给 `model.analyze` 的全量结果。

## 命令

```sh
python -m visit_attribution.cli apply  --db run/store.db --source batches/b1.json
python -m visit_attribution.cli export --db run/store.db     --output run/report.json
# 旧 CLI 原样保留
python -m visit_attribution.cli --source fixtures/input.json --output run/legacy.json
```

批次文件沿用现有输入契约，仅在顶层增加 `batch_id`（非空白、≤128 字符、无控制字符）：

```json
{
  "batch_id": "B1",
  "as_of": "2026-01-01T01:00:00Z",
  "session_gap_seconds": 1800,
  "lookback_seconds": 3600,
  "events": [ ... ]
}
```

仍受单文件 8MB、严格 JSON（拒绝重复键与非有限数值）与既有事件校验约束。

## 批次与水位规则

- **首批固定参数**：首批的 `session_gap_seconds` / `lookback_seconds` 写入库级元数据，
  后续批次必须一致，否则拒绝；`as_of` 允许持平但永不回退，回退即拒绝。
- **批次凭据**：每个成功批次在 `batches` 表保留 `batch_id`、源字节 SHA256、字节长度、
  提交时水位与批内重复数。
- **重放**：同一 `batch_id` 再次提交时比对源字节哈希与长度；字节一致则直接返回
  `replayed=true`，不写任何表、不推进水位、不累计重复数；字节不一致则拒绝。
- **跨批去重**：事件按 v1 规则规范化为 UTC 后以 `id` 主键去重；规范化内容一致即跳过
  （跨批重复不重复计数），任何字段冲突则整批拒绝。批内重复仍按 v1 计数，且重复数
  只在该批次首次成功提交时累计；重放不会二次累计。
- **未来事件**：`at > as_of` 的事件通过结构校验后以 `active=0` 保留，不进入物化结果；
  后续批次把 `as_of` 推进到覆盖其时刻时激活。空批次可以仅推进水位并触发激活。
- **乱序补入**：迟到事件按时间位置并入对应用户的既有会话流；若补入点使相邻间隔不再
  大于 gap，旧会话被合并，会话 ID 取最早事件 ID，历史 purchase 的 `session_id` 与渠道
  归因在同一事务中回改。

## 增量物化与内存边界

- 每次 `apply` 只重算受影响用户：新事件所属用户，以及因水位推进而新激活事件的所属用户；
  其他用户的物化行原样保留，禁止全库重跑。
- 单用户重算全部在 SQLite 内完成：把该用户的激活事件写入临时表（落盘、不在 Python
  列表中），用窗口函数按整数 epoch 秒做 gap 会话划分，再以窗口函数聚合会话与成员；
  每个转化的四个触点槽位（非 direct 首/末、direct 回退首/末）都是临时表索引上的
  `LIMIT 1` lookback 区间探测，成本只与窗口密度相关、与用户历史长度无关。
- 临时表与连接级缓存均受预算约束（`cache_size` 上限、关闭 mmap），重算结果走
  `INSERT ... SELECT` 直接落物化表，Python 端不持有整用户历史。
- 物化表 `m_sessions` / `m_session_events` / `m_conversions` 按 `(user, seq)` 组织，
  重算用户时先按用户删除再整体写回。
- `export` 用流式 JSON 写入器逐游标输出到临时文件（用户与会话均为嵌套游标，
  不构造用户列表或用户级字典），聚合全部走 SQL `GROUP BY`；
  成功后 `os.replace` 原子替换，任何失败都保留旧输出文件，拒绝符号链接输出。
- 实测（Python 3.11.2 / SQLite 3.40.1，100 个 1 万事件批次）：100 万事件且 70%
  集中在单一用户时，`apply` 峰值 RSS **54.07 MiB**、`export` 峰值 RSS **33.95 MiB**，
  均低于 128 MiB 预算，且 apply 峰值在约第 40 批后不再随库增大。复现与数据见
  `experiments/`，报告写入 `run/experiments/experiment-report.json`；完整导出 JSON
  的解析校验在独立进程进行，其内存不计入导出预算。
- 倾斜用户每批都被重算，其会话划分是 O(该用户历史) 的窗口计算（百万规模实测末批
  约 15 秒）；这是为保证迟到合并与历史回改正确性的有意取舍，内存不随历史增长。

## 事务与中断

一次 `apply` 在单条 `BEGIN IMMEDIATE` 事务内完成：事件插入/激活、批次凭据写入、
水位与重复数元数据更新、受影响用户物化替换，全部一起提交；任何异常（含注入故障）
都回滚，库内不会出现半生效状态。SQLite 默认 rollback journal 保证崩溃原子性，
单操作者模型下不提供并发写入。

## 导出等价性

导出报告为 `{"schema_version": 2, "result": {...}}`，其中 `result` 的字段、
嵌套键顺序与 `model.analyze` 完全一致：`as_of`、`duplicate_events`、`future_event_ids`、
`sessions`、`conversions`、`session_count`、`conversion_count`、`total_revenue_cents`、
`channel_totals`。会话按用户（假名排序）再按会话序号、转化按用户再按购买序号输出，
与全量模型的排序结果相同。注意跨批重复事件在累计模型中同样被去重，但重复计数语义
以“首次成功批次”为准（见上）。

## 表结构摘要

| 表 | 用途 |
| --- | --- |
| `meta` | schema 版本、固定 gap/lookback、当前 `as_of`、累计批内重复数 |
| `batches` | 批次凭据（ID、源哈希、字节长度、水位、批内重复数） |
| `events` | 规范化事件主表，`active` 标记是否已被水位激活，`first_batch_id` 溯源 |
| `m_sessions` / `m_session_events` | 每用户会话与成员事件的物化结果 |
| `m_conversions` | 每用户转化及其两个模型的渠道/触点物化结果 |
