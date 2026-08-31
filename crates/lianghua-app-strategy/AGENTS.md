# 组件取舍

- 本 crate 聚合策略编辑、选股、排名、相似度、统计和回测应用用例。
- 这是应用层最大的编译单元；先与 market、data、chart 隔离，避免一次重构同时打散高耦合策略内部类型。
- 允许依赖 shared 与后端 backtest；不得依赖 facade 或 market。后续只有出现稳定的单向边界时才继续拆小。
- data/download/scoring/backtest 均直接依赖能力所有者，禁止重新引入聚合导出层。
- 只运行 `cargo test -p lianghua-app-strategy`，不要因此触发全 workspace 测试。
