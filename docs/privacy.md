# 差分隐私发布 v1（privacy-init / privacy-release）

在 SQLite 增量管道（[incremental.md](incremental.md)）之上新增用户级差分隐私发布：
按 UTC 日 × 公开渠道统计**独立用户数**，每桶加入独立 Laplace 噪声后发布。
预算由独立 SQLite 账本精确管理；旧全量 CLI 与增量 `apply`/`export` 行为不变。

## 命令

```sh
python -m visit_attribution.cli privacy-init    --ledger run/privacy.db --budget 10
python -m visit_attribution.cli privacy-release --source run/store.db \
    --ledger run/privacy.db --request request.json --output run/release.json
```

`privacy-init` 创建账本并写入固定总预算（正有限十进制字符串，规则同下）；
已初始化的账本拒绝重建，防止重置预算。建表与元数据在单事务内提交，
中断后重试可正常初始化（零表文件视为未初始化）。

## 请求契约

请求文件沿用受限 JSON（≤8MB、拒绝重复键与非有限数值），字段严格为：

```json
{
  "release_id": "R2026-01",
  "start_date": "2026-01-01",
  "end_date": "2026-01-08",
  "channels": ["email", "search"],
  "L": 4,
  "epsilon": "0.5"
}
```

- `release_id`：非空白标识（≤128 字符、无控制字符），账本主键。
- `start_date` / `end_date`：UTC 日历日 `YYYY-MM-DD`，须为真实日期；
  统计区间为**半开** `[start_date, end_date)`，长度 1..366 天。
- `channels`：公开渠道集，数组 ≤64 项，每项为非空白标识；不允许重复，
  不允许保留名 `other`（回退桶由系统自动追加）。空数组合法：所有渠道归入 `other`。
- `L`：正整数（≤10^6），每用户贡献桶数上限。
- `epsilon`：正有限**十进制字符串**（1–12 位整数，可选 1–18 位小数，无指数记号），
  本次发布消耗的预算。数值规范化后参与比较：`"0.5"` 与 `"0.50"` 视为同一规范。

## 统计语义与隐私机制

- 从源库**单一快照**读取 `active=1` 的 touch 事件（未来事件不计），按 UTC 日聚合；
  渠道不在公开集中的触点归入 `other`；发布域为 `sorted(channels) + ["other"]` 的
  全部 日×渠道 组合，无数据的桶补零后同样加噪发布。
- **邻接关系**：增/删整用户。每用户在其非空桶（日、渠道字典序）中只保留前 `L` 个，
  每桶至多贡献 1，因此计数向量的 L1 敏感度为 `L`。
- 每桶加独立 `Laplace(L/epsilon)` 噪声（逆 CDF 采样，输出实数计数值）。
  生产路径使用 `secrets.SystemRandom`；`privacy.release(..., rng=...)` 可注入
  确定性随机源，测试据此逐桶重放核验噪声尺度与独立采样顺序。CLI 级跨进程测试
  通过 `VA_PRIVACY_RNG_ZERO=1` 令全部噪声退化为确定性零值（仅限测试用途）。
- 输出只含公开参数（release_id、日期区间、渠道域、L、epsilon、noise_scale）、
  源库水位 `source_as_of` 与噪声计数；不含真实计数、用户标识或单事件信息。

## 账本规则

- epsilon 以 10^18 缩放的整数十进制串精确累计（`0.1+0.1+0.1` 严格等于 `0.3`），
  不存在浮点超支/少收；`spent + epsilon > budget` 即拒绝，不扣费、不写文件。
- 扣预算与存载荷在**同一 `BEGIN IMMEDIATE` 事务**内提交；SQLite 写者串行化
  保证并发请求不可超支（争用时等待 `busy_timeout`，后到者看到已扣余额）。
- **幂等重放**：`release_id` 已存在且规范哈希一致时，直接返回账本中的原载荷字节，
  不再读取源库、不再采样噪声、不再扣费；同 ID 异参（含不同 epsilon/L/区间/渠道）
  一律拒绝且不扣费。
- 输出文件在账本提交**之后**以临时文件 + `os.replace` 原子替换：提交后写文件失败
  （如目标是目录、注入故障 `VA_PRIVACY_FAULT_AFTER_COMMIT`）保留旧文件，
  重试同一请求走幂等重放免费补发。输出不得为符号链接，且不得与源库/账本/请求同路径。

## 源库一致性与擦除栅栏

源库以 `mode=ro` 只读打开，水位与聚合在**同一读事务**内完成：并发 `apply` 批次
（含未提交写入）不会造成混合快照，发布路径也不修改源库任何表或归因结果
（发布前后源库文件字节一致）。源库不存在或无已应用批次时拒绝发布且不扣费。

源库每次提交成功的 `apply`/`erase` 都会推进 `meta.write_generation` 代际计数
（见 [擦除文档](erasure.md)）。发布流程为：只读快照记录代际 → 账本
`BEGIN IMMEDIATE` → 在保持打开的只读源事务内复检代际 → 代际一致才扣费提交。
若快照之后有擦除或批次提交，发布以 `source changed since snapshot` 中止，
**不扣费、不写载荷、不产文件**，同一请求重试即读到新快照。复检读事务的共享锁
保持到账本提交，并发写者在 `busy_timeout` 内有界等待而非被拒绝，因此只存在
"发布先于擦除" 或 "擦除先于发布" 两种干净顺序。

擦除不修改账本：历史发布载荷字节与预算永久不变，已提交 `release_id` 重放始终
返回原字节、不二次扣费；擦除之后的**新**发布只可能统计擦除后的用户集合。

## 输出格式

```json
{
  "schema_version": 1,
  "release_id": "R2026-01",
  "start_date": "2026-01-01",
  "end_date": "2026-01-08",
  "channels": ["email", "search", "other"],
  "L": 4,
  "epsilon": "0.5",
  "noise_scale": 8.0,
  "source_as_of": "2026-01-08T00:00:00Z",
  "counts": [
    {"date": "2026-01-01", "channel": "email", "noisy_count": 3.14}
  ]
}
```

`counts` 按（日期, 渠道域序）全覆盖输出，桶数 = 天数 × (公开渠道数+1)，
上限 366×65。CLI 标准输出另打印本次摘要（是否重放、累计花费、总预算等）。

## 表结构摘要（账本）

| 表 | 用途 |
| --- | --- |
| `meta` | schema 版本、总预算与已花费（均为 10^18 缩放整数十进制串） |
| `releases` | 每次发布的规范哈希、epsilon、原载荷字节、快照源代际 `source_generation` 与创建时间。旧账本打开时自动 `ALTER TABLE` 补列，历史行代际记 0 |

## 测试

`tests/test_privacy.py` 覆盖：请求/预算校验、跨日裁剪与同日渠道序裁剪、零桶补零、
注入随机源核验尺度与独立采样、精确十进制累计、幂等重放与异参拒绝、并发争预算不超支、
提交后写文件失败免费重试、源写入交错下的一致性快照、源库字节不变，以及 CLI 端到端。
