# 组件取舍

- 本 crate 只负责图表指标定义、编译、执行和指标配置，不承载股票详情页面聚合。
- 页面级详情放在 `lianghua-app-facade`，以免图表能力反向依赖市场和策略能力。
- 直接依赖表达式与数据后端是执行图表指标的必要成本；不得经由聚合门面，也不得依赖其他应用层业务 crate。
- 只运行 `cargo test -p lianghua-app-chart`，不要因此触发全 workspace 测试。
