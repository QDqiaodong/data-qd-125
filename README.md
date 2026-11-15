# data-qd-125 · 访问会话与转化归因

Deterministic sessionization and lookback conversion attribution。业务输入、计算和当前边界详见 [数据契约](docs/contract.md)。

## 安装与演示

Python >=3.11，运行和测试仅用标准库；先确认 `python3 --version`。
每仓建立独立环境，构建工具锁版本。首次安装无公共缓存时可能联网；业务运行不访问网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m pip check
.venv/bin/visit-attribution --source fixtures/input.json --output run/report.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。单操作者批处理，不承诺并发发布或断电恢复。
同输入重跑不累加。测试使用临时目录，运行产物在忽略的run/。

src/visit_attribution/model.py 为业务模型，相关算法模块独立组织；common.py 提供受限JSON及报告发布，
cli.py 衔接数据链路。fixtures 为明确自制样例，代码与样例采用MIT许可。

## SQLite 增量管道

旧的全量快照 CLI 原样保留；另增基于 SQLite 的增量管道，业务规则与全量模型一致，
导出 `result` 的字段与顺序等价于把累计输入交给全量模型。设计与表结构见
[增量管道文档](docs/incremental.md)。

```sh
python -m visit_attribution.cli apply  --db run/store.db --source batches/b1.json
python -m visit_attribution.cli export --db run/store.db --output run/report.json
```

批次文件沿用现有输入契约并在顶层增加 `batch_id`；首批固定 gap/lookback，`as_of`
只进不退。同批次 ID 且源字节一致时重放无副作用，否则拒绝；跨批事件按 UTC 规范化
去重、冲突整批拒绝，批内重复数仅在首次成功提交时累计。未来事件保留为非激活，
水位推进时激活；迟到事件合并旧会话、更换会话 ID 并回改历史归因。事件、批次凭据与
物化结果在单事务内提交，中断不产生半生效状态。仅重算受新激活事件影响的用户，
重算与导出全部走流式 SQL，不把整库或整用户历史装入 Python 内存。

可复现实验（含百万事件单用户倾斜下峰值 RSS ≤128 MiB 的实测）：

```sh
PYTHONPATH=src python3 -I experiments/run_experiments.py --out run/experiments
```

## 差分隐私发布

另增用户级差分隐私发布：独立 SQLite 账本固定总预算，按 UTC 日×公开渠道发布
加噪独立用户计数（未知渠道归 other、公开域补零、每用户限前 L 桶、每桶独立
Laplace(L/epsilon)）。账本精确累计 epsilon、并发不超支、同规范请求幂等重放，
源库只读单快照。契约与规则详见 [差分隐私发布文档](docs/privacy.md)。

```sh
python -m visit_attribution.cli privacy-init    --ledger run/privacy.db --budget 10
python -m visit_attribution.cli privacy-release --source run/store.db \
    --ledger run/privacy.db --request request.json --output run/release.json
```

## 业务与样例结果

6事件中1未来排除，形成3会话、3转化/1600分；email归因1500分、unattributed100分。
events.py规范化UTC与去重，attribution.py划分会话并按时间索引选触点；不作因果效果判断。
