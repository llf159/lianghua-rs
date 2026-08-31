# 组件取舍

- 本 crate 负责持久化、数据库结构、领域配置和数据到表达式运行时的转换。
- 评分结果库的建表、批量写入、数据库排名与补排名统一位于 `data::scoring_store`。
- 可以依赖 `lianghua-model` 的共享类型，禁止依赖 scoring、download、backtest 或应用层 crate，确保依赖无环。
- 不在 data 中实现评分规则计算或把评分序列构造成结果行。
- 只运行 `cargo test -p lianghua-data`，不要因此触发全 workspace 测试。
