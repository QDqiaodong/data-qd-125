# Session and conversion attribution v1

为离线网站分析者把完整的匿名访问事件快照归并成会话和可复核转化归因事实。
输入as_of、session_gap_seconds=1..86400、lookback_seconds=1..31天，以及最多10000事件。
每行id/user/at/kind/channel/amount_cents；id全局唯一、不透明user为假名，不推断跨设备身份。
时间为严格ISO秒加Z或±HH:MM，归一UTC；无小数秒，不允许无时区。offset小时<=23、分钟<=59。
touch含非空channel、amount=null；purchase含channel=null、amount=0..10^9整数分。
币种隐含为单一合成币种，不跨币种相加，不做支付退款对账；unattributed为保留channel。
同ID完整规范化内容一致可去重（等价offset视为一致），冲突拒绝。未来事件仍验证结构，单列但不纳入。

按user、UTC时刻、ID排序；相邻任意事件间隔严格大于gap才开新会话，等于gap仍同一会话。
会话ID取首事件ID，起止为首末观测。purchase也形成会话。没有把缺失访问时段填成活跃。
每笔purchase仅使用同user在[转化-lookback,转化]闭区间内touch，允许跨会话归因。
同秒touch视为已发生且可用；同秒多个touch按ID字典序。此为显式批处理约定，不推测真实毫秒顺序。
first_non_direct/last_non_direct优先选择窗口内首/末非direct；若全direct则首/末direct；
完全无touch则unattributed。direct不覆盖仍在窗口内的非direct来源。

每笔转化全额归给每个模型各自一个channel，不把两个模型汇总后再次相加。输出选中touch_id、
所属session_id、金额及每模型渠道总数/金额；两个模型各自的金额守恒=总转化金额。
归因是规则分配，不是因果贡献或广告增量评估；无权重拆分、身份拼接或外部广告接口。
样例6事件，1未来排除；3会话/3转化/1600分，email获1500分、unattributed获100分。
P2因相邻间隔1801秒开新会话，但仍归给上会话窗口内email触点。
当前全量静态快照，无迟到水位、实时流处理、跨设备合并、退款修订或持久增量状态。

增量场景（批次水位、迟到事件合并、未来事件激活与持久化状态）由 SQLite 增量管道
承担，业务规则与本契约一致，详见 [incremental.md](incremental.md)。

渠道合作方佣金清分与付款预留由独立 SQLite 账簿承担，只消费增量管道已物化的
`last_channel`/`revenue_cents`，不改变本契约的归因规则，也不执行外部支付；
详见 [commission.md](commission.md)。
