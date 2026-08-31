# 组件取舍

- 本 crate 是兼容聚合层，不再承载应用业务实现。
- 旧的 `lianghua_app::ui_tools::*` 与根级后端重导出必须保持兼容；新代码优先直接依赖最窄的 `lianghua-app-*` crate。
- 为减少增量编译，禁止把实现重新放回本 crate。
- 只运行 `cargo check -p lianghua-app` 验证兼容导出；业务测试在对应组件 crate 运行。
