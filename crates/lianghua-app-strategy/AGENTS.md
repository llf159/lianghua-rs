# 组件取舍

- 本 crate 聚合策略编辑、选股、排名、相似度、统计和回测应用用例。
- 这是应用层最大的编译单元；先与 market、data、chart 隔离，避免一次重构同时打散高耦合策略内部类型。
- 允许依赖 shared 与后端 backtest；不得依赖 facade 或 market。后续只有出现稳定的单向边界时才继续拆小。
- data/download/scoring/backtest 均直接依赖能力所有者，禁止重新引入聚合导出层。
- 评分共享类型从 `lianghua-model` 导入，结果库操作从 `lianghua-data::data::scoring_store` 导入；不得借 scoring 建兼容路径。
- 只运行 `cargo test -p lianghua-app-strategy`，不要因此触发全 workspace 测试。
- 全市场触发近邻精排用触发次数推导的严格时序上界做剪枝，并用 101 个分数桶代替全候选排序；这是为了在保持精确 Top-K 语义的同时，线性地优先填满堆并提高后续 DP 剪枝率。
- 走势相似精排权重为触发 35%、市场环境 35%、量价 15%、指标 15%，以同等强调策略触发与市场状态，并降低个股数值通道的合计影响。
