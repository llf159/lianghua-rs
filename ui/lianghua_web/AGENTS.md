# 组件取舍

- Tauri 适配层直接依赖各 `lianghua-app-*` UI 能力 crate，并通过 data/download/provider 等后端 crate 获取底层契约。
- 禁止重新引入 `lianghua-app`、`lianghua-engine` 或类似聚合门面隐藏真实依赖。
- Rust 侧改动只检查 `cargo check -p app`；前端改动按实际涉及范围验证，不运行无关全量测试。
- Linux 桌面端启动时仅在检测到 Snap 宿主环境后清理 Snap 注入的 GTK/GIO 模块变量，避免系统 WebKit 误加载 Snap 内的 glibc，同时不影响普通终端中用户显式设置的 GTK 环境。
- 宽表需要同时支持表内横向滚动和随页面纵向悬浮表头时，将悬浮表头放在横向滚动容器外并同步 `scrollLeft`；不要让横向 `overflow` 成为原始 sticky 表头的滚动祖先。
- 总榜、场景榜和相似榜的冻结识别区统一使用 50px 排名列与 120px 股票列，股票列内名称在上、代码在下，以较小横向占用保留滚动时的个股上下文。
