# 离线佣金清分与付款预留 v1（commission-* / payout-*）

在 SQLite 增量管道（[incremental.md](incremental.md)）之上新增渠道合作方的离线
佣金账簿：按已物化的 `last_channel` 与 `revenue_cents` 计算整数分佣金，维护不可
改写的权利分录，并以内部预留/确认记录模拟付款计划。**不执行任何外部支付**，
旧全量 CLI、`apply`/`export`/`erase`、`privacy-*` 的行为完全不变，佣金账簿是
独立 SQLite 文件。

## 命令

```sh
python -m visit_attribution.cli commission-init   --ledger run/commission.db --rates rates.json
python -m visit_attribution.cli commission-sync   --source run/store.db --ledger run/commission.db --sync-id S1
python -m visit_attribution.cli payout-plan       --ledger run/commission.db --request plan.json
python -m visit_attribution.cli payout-confirm    --ledger run/commission.db --plan-id PL-1
python -m visit_attribution.cli payout-cancel     --ledger run/commission.db --plan-id PL-1
python -m visit_attribution.cli commission-report --ledger run/commission.db --output run/commission.json
```

`commission.db` 是独立文件；源访问库始终以 `mode=ro` 只读打开，佣金命令不改写
源库、匿名账本或既有物化结果。所有命令成功时向标准输出打印一行 JSON 摘要，
契约/校验失败退出码 2（不改动已提交账簿）。

## 费率（commission-init）

请求文件沿用受限 JSON（≤8MB、拒绝重复键与非有限数值），字段严格为：

```json
{"rates": {"email": 1200, "search": 2500, "direct": 0}}
```

- 渠道为与事件 `channel` 同规范的非空白标识（≤128 字符、无控制字符），不得重复。
- 每个费率为 `0..10000` 的**整数基点**（bps）；不接受字符串、浮点或布尔。
- `direct`、`unattributed` 的费率固定为 0，显式配置为 0 也接受，配置其他值即拒绝。
- 未出现在映射中的渠道费率为 0；显式空映射 `{"rates": {}}` 合法（所有渠道
  费率为 0，佣金恒为 0）。金额为任意整数分，佣金按
  `revenue_cents * bps // 10000` 的**整数截断**计算（不使用浮点）。
- 费率在初始化事务内一次性写入，之后**不可修改**：`rates` 上的触发器拒绝任何
  `INSERT`/`UPDATE`/`DELETE`，`commission-init` 对已初始化账簿拒绝重建。
  费率的规范摘要存为 `meta.rates_sha256`。
- 建表、费率与元数据在同一事务内提交；中断后重试可正常初始化（零表文件视为
  未初始化）。

## 同步（commission-sync）

一次 `commission-sync --sync-id S`：

1. 以只读单事务从源库同一已提交快照读取 `meta.as_of`、`meta.write_generation`
   与全部 `m_conversions` 的 `purchase_id`、`last_channel`、`revenue_cents`。
   并发 `apply`/`erase` 不会产生混合快照。
2. 以全局 `purchase_id` 为权利身份；**不重新计算归因**，不使用 user、触点、
   会话或 first 模型。源内 `purchase_id` 重复直接拒绝。
3. 源内容摘要 = 对按 `purchase_id` 规范排序的 `[purchase_id, last_channel,
   revenue_cents]` 列表做 SHA256；摘要与 `as_of` 各自独立，绝不因 `as_of` 相同
   而推断内容相同。
4. 在账簿的 `BEGIN IMMEDIATE` 事务内应用差异并提交（见下）。

### 首次绑定与水位规则

- 首次成功 sync 把源库**绝对路径**写入 `source_state` 并永久绑定；之后指向其他
  源库的同步拒绝（即使内容相同）。
- 拒绝回退：`as_of` 早于最近一次成功 sync 的 `as_of`，或 `write_generation`
  小于账簿记录值，均拒绝且不写任何行。
- 同代际不同内容：代际与最近成功 sync 相同但内容摘要不同，拒绝（源库应在每次
  提交时推进代际，此检查兜底损坏/伪造的源）。
- 同代际同内容、更深水位但同代际的合法快照照常接受。

### 幂等

`sync_id` 的主键为账簿凭据，其规范摘要覆盖 `sync_id`、源绝对路径、`as_of`、
代际与内容摘要：

- 同 `sync_id` 且**同一快照**：返回 `replayed=true` 与原摘要，不写分录、不失效
  计划、不推进任何状态；
- 同 `sync_id` 但快照不同（哪怕只是水位前进）：拒绝，整笔不生效。

用一个新 `sync_id` 重放同一快照也是零分录幂等（差异集为空）。

## 权利与分录

账簿保存**当前权利**与**不可改写的正负分录**（append-only，触发器拒绝
`UPDATE`/`DELETE`）：

| 源差异 | 分录 |
| --- | --- |
| 新增 `purchase_id` | 记 +佣金（零佣金权利只建 `rights` 行、不记零分录） |
| 渠道变化或金额变化 | 先记 −旧佣金（冲销旧渠道权利），再记 +新佣金 |
| `purchase_id` 消失 | 记 −旧佣金（逐 ID 冲销，不是只追新增 ID） |
| 权利未变 | 不记任何分录 |

因此每渠道 `SUM(entries.amount_cents) == SUM(rights.commission_cents)`，重复
sync 不增记；`commission-report` 的 `checks` 逐渠道核对这一守恒。

账簿只含 `purchase_id`、渠道、金额与账簿内部序号；**不存 user、触点、会话标识**。

## 付款计划

请求文件字段严格为 `plan_id` 与 `amounts`（渠道 → 正整数分，非空对象）：

```json
{"plan_id": "PL-2026-01", "amounts": {"email": 800, "search": 400}}
```

- 规范集合按渠道字典序的映射计算摘要：键序不同、同金额视为同一请求；同
  `plan_id` 同请求返回原计划，异参拒绝。
- 渠道 `净余额 = 累计分录 − 已确认付款`；`可预留 = max(0, 净余额 − 其他有效
  计划已预留)`。
- **各渠道全部足额才创建计划**：任一渠道不足即整单拒绝，不产生部分预留。
  预留与计划行在同一 `BEGIN IMMEDIATE` 事务内提交；并发计划因写事务串行化
  互斥，后到者看到前者预留后重算可预留额。
- `payout-confirm` 一次性把有效计划的预留全部转为永久 `payouts`/
  `payout_allocations`（触发器拒绝改写/删除）；同 `plan_id` 再次确认幂等返回
  原已付记录。已确认计划不可取消；已取消/失效计划不可确认。
- `payout-cancel` 释放该计划的全部预留（`status='cancelled'`），再次取消幂等；
  对已被同步整单失效的计划取消同样幂等无副作用。
- 账簿没有任何成功 sync 时拒绝创建计划。

## 同步失效与欠款（并发正确性）

- sync 若使某渠道的当前权利总额（该渠道 `rights.commission_cents` 合计）较
  本次同步开始时减少，则在**同一账簿事务**内把**所有包含该渠道的未确认
  （active）计划整单失效**（`status='expired'`，
  `resolution='sync_invalidated'`），释放其全部渠道预留——即便只是其中一个
  购买改归仍会整单失效。总额未减少（含持平）时不失效。同步分录、权利替换、
  计划失效、`syncs` 凭据与 `source_state` 共同提交：崩溃/中断后要么全部生效，
  要么全部不生效。
- 若相关付款已确认，则**不撤销已付**：净余额变为负数即为**欠款**
  （`debt_cents > 0`），可预留额为 0；后续正佣金先抵欠款，抵清后才重新可预留。
- 并发 `commission-sync` 与 `payout-confirm` 都走账簿 `BEGIN IMMEDIATE`，被
  SQLite 写者锁串行化，只可能得到两种完整顺序之一：
  - **sync 先提交**：计划在 sync 事务中失效，随后 confirm 看到 expired 而拒绝
    ——不会"已失效仍付"；
  - **confirm 先提交**：预留转为永久已付，sync 随后照常冲销并形成欠款——
    不会重复付，也不会出现释放一半预留的中间态。
- 只按**最近一次成功 sync** 的账簿状态结算；不承诺源库实时最新。

## 报告（commission-report）

输出独立 JSON（`os.replace` 原子替换，拒绝符号链接，且不得与账簿同路径）：

- `channels[]` 覆盖所有已配置渠道、有分录/已付/有效预留/当前权利的渠道：
  每渠道 `rate_bps`、`accrued_cents`（累计分录）、`net_cents`
  （累计 − 已付）、`paid_cents`、`reserved_cents`（有效计划预留）、
  `available_cents`、`debt_cents` 以及 `plans[]`（该渠道涉及的每个计划的
  `plan_id`/`status`/`amount_cents`）；
- `plans[]`：全部计划的 `status`（`active`/`confirmed`/`cancelled`/`expired`）、
  `resolution`、金额与各渠道分配；
- `source`/`last_sync`：绑定的源绝对路径、最近快照的 `as_of`/代际/内容摘要；
- `totals` 与 `checks`（逐渠道分录=权利、已付总额=已确认计划总额、逐渠道已付=
  已确认计划分配、计划分配守恒、无超预留、可预留=净−预留），`conservation_ok`
  为全部检查的合取。

报告在单一读事务内构建，并发 sync/confirm 不会得到撕裂视图。报告失败只影响
输出文件，不回滚已提交账簿；重试不会重复入账。

## 表结构摘要（佣金账簿）

| 表 | 用途 |
| --- | --- |
| `meta` | schema 版本、费率摘要、最近成功 `sync_id`、费率冻结标记 |
| `rates` | 初始化时固定、之后不可改写的渠道基点费率 |
| `source_state` | 单行：绑定的源绝对路径、最近快照 `as_of`/代际/内容摘要 |
| `syncs` | 同步凭据：`sync_id`、规范摘要、快照三元组、各类分录计数 |
| `rights` | 当前权利：`purchase_id` → 渠道、金额、佣金 |
| `entries` | 不可改写分录：`seq`、`purchase_id`、渠道、正负分、grant/reversal、`sync_id` |
| `plans` / `plan_allocations` | 付款计划及按渠道预留；状态 active/confirmed/cancelled/expired |
| `payouts` / `payout_allocations` | 永久已付记录（触发器禁止改写/删除） |

## 测试

`tests/test_commission.py` 覆盖（含真实跨进程 CLI 与进程屏障交错）：

- 模块级：费率校验与冻结、整数截断、未配置/direct/unattributed 零费率、
  首次绑定源路径、水位与代际回退、同代际异内容、同 ID 同/异快照、重复 sync
  不增记、渠道改归、金额变化、purchase 消失的完整冲正链与守恒；
- 付款：多渠道不足整单拒绝、竞争预留、重复确认、取消与失效语义、永久已付；
- 进程级交错（文件屏障实际控制，不用函数模拟并发）：sync 先提交则计划失效且
  确认被拒；confirm 先提交则已付不变并形成欠款，后续正佣金先抵欠款且不可预留；
  两个进程竞争同一渠道余额时只有一个计划成功、另一个拒绝且不部分预留；
- 单快照：sync 读取期间另一进程未提交的源写入不会泄漏进快照；
- 事务中 `SIGKILL`：sync 中断后分录、权利、计划、凭据、源状态全部回滚，重试
  只入账一次；confirm 中断后计划仍 active、无付款记录，重试确认恰好一次。
