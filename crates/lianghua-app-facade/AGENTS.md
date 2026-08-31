# 组件取舍

- 本 crate 是应用层最上层，只承载同时组合 chart、market、strategy 等能力的页面用例。
- 其他应用 crate 不得依赖本 crate；该单向约束用于阻止应用层循环依赖。
- 股票详情和筹码策略留在这里，是因为它们同时组合多个能力，不适合塞回任一底层业务域。
- 只运行 `cargo test -p lianghua-app-facade`，不要因此触发全 workspace 测试。
