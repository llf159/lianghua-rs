# 工程约束

- 允许 `cargo fmt` 带来的其他无关文件格式改动。
- 不要编写只使用一次的工具函数。
- 不运行全量测试，只验证实际改动的组件。
- 工程取舍记录在各组件自己的 `AGENTS.md`。

# 根包取舍

- 根包只承载 `src/main.rs` 和 `src/bin` 下的仓库内二进制，不提供 `src/lib.rs` 聚合接口。
- 根目录二进制必须直接依赖 data、scoring、backtest 等能力所有者，禁止恢复 `lianghua_rs::*` 兼容导出。
- 只运行 `cargo check -p lianghua-rs` 以及实际改动二进制的测试。
