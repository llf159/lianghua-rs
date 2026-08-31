# 组件取舍

- Tauri 适配层直接依赖各 `lianghua-app-*` UI 能力 crate，并通过 data/download/provider 等后端 crate 获取底层契约。
- 禁止重新引入 `lianghua-app`、`lianghua-engine` 或类似聚合门面隐藏真实依赖。
- Rust 侧改动只检查 `cargo check -p app`；前端改动按实际涉及范围验证，不运行无关全量测试。
