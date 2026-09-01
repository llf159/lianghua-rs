# Tauri crate 取舍

- Android 的 `MainActivity` 必须先调用 `super.onCreate`，再通过自定义 JNI 方法初始化 `rustls-platform-verifier`。Wry 0.54.4 会在加载 `WryActivity` 类时加载 Tauri 原生库，曾掩盖 JNI 调用过早的问题；Wry 0.55.1 改为在 `WryActivity.onCreate` 首次访问惰性的 `Rust` 对象时才执行 `System.loadLibrary`，因此禁止依赖类加载副作用，也禁止吞掉 `UnsatisfiedLinkError` 后继续启动，否则 verifier 实际未初始化，后续 HTTPS 请求会表现为证书验证失败。
- `tauri = "2.10.3"` 是兼容版本约束，不会固定在 2.10.3；合并或重建 workspace 锁文件可能升级 Tauri/Wry。涉及 Android 启动代码时，必须结合 `Cargo.lock` 中实际解析的 Wry 版本检查原生库加载时序。
