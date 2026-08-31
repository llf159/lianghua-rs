# 组件取舍

- 本 crate 只保存无页面归属的应用层日期、证券代码和股票元数据能力。
- 直接依赖后端数据层，不得经由聚合门面，也不得依赖其他 `lianghua-app-*` crate。
- `canonical_ts_code` 对 crate 外公开，是为了让其他应用层 crate 复用同一兼容格式化语义，不在各组件重复实现。
- 只运行 `cargo test -p lianghua-app-shared`，不要因此触发全 workspace 测试。
