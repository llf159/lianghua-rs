# 组件取舍

- 本 crate 负责排名、规则、场景回测与统计模拟，属于 data/scoring 之上的业务能力层。
- 直接依赖 `lianghua-data` 与 `lianghua-scoring`，禁止重新引入后端聚合门面。
- 回测结果可以被应用层消费，但 data、scoring 和 engine 不得反向依赖本 crate。
- 只运行 `cargo test -p lianghua-backtest`，不要因此触发全 workspace 测试。
