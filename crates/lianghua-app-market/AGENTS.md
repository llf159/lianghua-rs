# 组件取舍

- 本 crate 负责实时行情、盘中监控、龙虎榜和观察列表等市场用例。
- 允许依赖 shared 与 expression 应用 crate；不得依赖 strategy 或 facade，避免页面组合反向进入市场层。
- 后端能力直接依赖 data/download/scoring/provider 等所有者，禁止重新引入聚合门面。
- `RowData` 到表达式运行时的转换属于 data，市场用例不得经 scoring 间接导入。
- `scene_stage` 是市场域私有支持模块，不为跨域复用而扩大公开 API。
- 只运行 `cargo test -p lianghua-app-market`，不要因此触发全 workspace 测试。
