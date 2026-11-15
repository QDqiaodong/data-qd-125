# 可复现实验

运行（仅用标准库，离线；以 `-I` 隔离模式启动子进程，避免工作目录植入模块）：

```sh
PYTHONPATH=src python3 -I experiments/run_experiments.py --out run/experiments
# 可选参数：--scale-events 1000000 --skew-share 0.7 --rss-budget-mib 128
```

`run_experiments.py` 只负责编排，实际负载在独立 `worker.py` 子进程中执行，
峰值 RSS 用 `resource.getrusage(RUSAGE_SELF).ru_maxrss` 按进程测量。
所有数据均为脚本生成的虚构合成事件，无网络、无外部账号。

## 场景

`functional` 模式（小数据，逐条断言）：

| 场景 | 断言内容 |
| --- | --- |
| `late_bridge` | 迟到触点落入 gap 内时两个会话合并为一，会话 ID 提前到最早事件 ID，历史 purchase 的会话与渠道归因被回改 |
| `same_second` | 跨批次同秒触点按事件 ID 字典序决定 first/last |
| `empty_advance` | 空批次仅推进 `as_of`，激活保留的未来事件，仅重算其所属用户 |
| `replay` / `replay_conflict` | 同 ID 同字节重放无副作用；同 ID 异字节被拒绝且无部分写入 |
| `crash_recovery` | 注入故障后整事务回滚（事件/批次/元数据均无残留），重试成功 |
| `small_data_equivalence` | 多批累计结果与 `model.analyze` 全量模型逐字段相等 |

`scale` 模式（内存与规模）：

- 生成 `--scale-events` 个事件，按契约上限切成每批 10000 事件（单文件 ≤8MB）；
- `--skew-share` 比例的事件属于同一个 `skew-user`（密集交替 touch/purchase），
  其余为大量互不重复的小用户（各一个 touch + 一个 purchase），所有事件都在
  首批 `as_of` 之前，保证无意外未来事件；
- 记录每批 apply 耗时与峰值 RSS；随后在**独立进程**中执行 export 并测量其峰值 RSS；
- 再启动第三个 `verify` 进程完整解析导出 JSON，与 SQL 物化结果逐字段比对并核对
  字段顺序，该进程内存不计入 128 MiB 预算。

## 报告

`run/experiments/experiment-report.json`（`run/` 已被 git 忽略）包含：

- `scenarios.functional`：每个功能场景的输入摘要与 `passed`；
- `scenarios.scale_memory`：事件/物化行数、`apply_peak_rss_mib`、
  `export_peak_rss_mib`、每批 RSS 与耗时、导出字节数、`full_export_verification`；
- `within_budget`：两个峰值均不超过 `--rss-budget-mib` 时为 true，否则进程退出码为 1。

参考实测（100 万事件、skew-share 0.7、Python 3.11.2、SQLite 3.40.1）：
apply 峰值 54.07 MiB、export 峰值 33.95 MiB，100 批 apply 约 687 秒、export 约 42 秒，
导出约 275 MB JSON，`within_budget=true` 且全量校验 `matches_materialization=true`。
具体数字随机器变化；预算断言可复现。
