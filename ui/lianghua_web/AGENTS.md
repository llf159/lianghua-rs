# 组件取舍

- Tauri 适配层直接依赖各 `lianghua-app-*` UI 能力 crate，并通过 data/download/provider 等后端 crate 获取底层契约。
- 禁止重新引入 `lianghua-app`、`lianghua-engine` 或类似聚合门面隐藏真实依赖。
- Rust 侧改动只检查 `cargo check -p app`；前端改动按实际涉及范围验证，不运行无关全量测试。
- Linux 桌面端启动时仅在检测到 Snap 宿主环境后清理 Snap 注入的 GTK/GIO 模块变量，避免系统 WebKit 误加载 Snap 内的 glibc，同时不影响普通终端中用户显式设置的 GTK 环境。
- Tauri 是最终应用边界，必须在构造 `tauri::Builder`、启动异步任务或创建任何 HTTP/TLS 客户端前安装进程级 rustls `CryptoProvider`。拆分网络能力 crate 时不得把这一初始化遗漏或分散到各库；当依赖图同时启用 AWS-LC 与 ring 时，依赖 rustls 自动选择会在首个建连线程 panic。
- Android 网络初始化包含两层且缺一不可：`MainActivity` 通过 JNI 初始化 `rustls-platform-verifier` 以使用系统证书验证，Rust 应用入口安装 `CryptoProvider` 以提供加密实现。排查证书类故障时必须同时核对两条链路。
