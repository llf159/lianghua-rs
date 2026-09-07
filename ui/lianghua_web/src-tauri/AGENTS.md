# Tauri crate 取舍

后续 agents 修改本组件时请继续维护本文件。

- 筹码策略导入与数据管理导入统一通过 `FilePath` 和 `app.fs().open` 读取选择器结果，并在 `spawn_blocking` 内执行；Android 返回的是 URI，不能直接交给 `std::fs`，否则会报路径不存在。百分号解码仅用于展示文件名，不改写读取 URI；业务层接收文本后继续完成 TOML 校验和备份。

- Android 的 `MainActivity` 必须先调用 `super.onCreate`，再通过自定义 JNI 方法初始化 `rustls-platform-verifier`。Wry 0.54.4 会在加载 `WryActivity` 类时加载 Tauri 原生库，曾掩盖 JNI 调用过早的问题；Wry 0.55.1 改为在 `WryActivity.onCreate` 首次访问惰性的 `Rust` 对象时才执行 `System.loadLibrary`，因此禁止依赖类加载副作用，也禁止吞掉 `UnsatisfiedLinkError` 后继续启动，否则 verifier 实际未初始化，后续 HTTPS 请求会表现为证书验证失败。
- `tauri = "2.10.3"` 是兼容版本约束，不会固定在 2.10.3；合并或重建 workspace 锁文件可能升级 Tauri/Wry。涉及 Android 启动代码时，必须结合 `Cargo.lock` 中实际解析的 Wry 版本检查原生库加载时序。
- Linux 相似度批量计算期间通过 `systemd-inhibit --what=idle:sleep` 临时阻止空闲息屏和睡眠，计算结束由 RAII 释放；问题是长时间同步计算会被桌面电源策略误认为空闲，选择 systemd-logind inhibitor 是为了不改永久电源配置，且命令不可用时只记录警告、不阻断计算。
- 问题：进程启动成功不代表防休眠申请成功，且同步回收子进程可能阻塞异步执行器；解决方案选择：异步等待退出或 RAII 释放通知，保留 stderr 并记录提前退出状态；解释：让申请失败可见，释放时异步终止并回收进程，不等待防休眠申请成功才开始计算。
- Android/移动 WebView 的相似度计算使用页面级 Screen Wake Lock，并在页面重新可见时重新申请；问题是移动系统会因页面隐藏或电量策略主动释放锁，选择可见页面标准接口是为了不申请常驻电源权限，且计算结束自动释放。

## Android Gradle 9 兼容性记录

### 2026-09-01 基线

- 最初故障由 Android Studio/Snap 自动更新到 JBR 25.0.2 触发：旧的 Gradle 8.14.3、AGP 8.11.0 不支持该运行组合，本机还实际出现过 JBR 25 的 HotSpot SIGSEGV。不要把这类故障误判成业务代码错误。
- 当前 Android 工程使用 Gradle 9.2.0、AGP 9.0.1、KGP 2.2.10。Android Studio/Gradle launcher 可以运行在 Java 25，但 `gen/android/gradle/gradle-daemon-jvm.properties` 将真正执行构建的 daemon 固定为任意厂商的 Java 21。
- Gradle daemon 使用的 JDK 21 路径只配置在用户级 `~/.gradle/gradle.properties`；禁止把机器路径写入仓库，也不得固定到会被 Snap 升级清理的 Android Studio revision 路径。GitHub Action 使用 Temurin 21。
- 锁文件实际解析为 Tauri 2.11.5、tauri-build 2.6.3、dialog 2.7.2、fs 2.5.1、log 2.9.0；前端锁文件为 API 2.11.1、CLI 2.11.4、dialog 2.7.0、fs 2.5.0。检查问题时以锁文件和 `cargo tree` 为准，不以 `Cargo.toml` 中的兼容下限为准。
- 当前 Release 基线是 `aarch64 + APK`；产物为 `gen/android/app/build/outputs/apk/universal/release/app-universal-release-unsigned.apk`。未配置签名时出现 `unsigned` 是预期行为。

### 当前临时兼容项

1. `gradle.properties` 中的 `android.builtInKotlin=false` 和 `android.newDsl=false` 必须保留。Tauri 2.11.5 及其插件仍应用 `org.jetbrains.kotlin.android`；AGP 9 默认的内置 Kotlin 会与它冲突。Tauri 已合并 Gradle 9 模板到 2.12 里程碑，但截至本记录日期尚未发布，而且上游 `dev` 模板仍保留这两个开关并注明计划到 v3 才移除。
2. `buildSrc` 的 `BuildTask` 使用注入的 `ExecOperations`，这是 Gradle 9 对已移除 `project.exec` 的必要适配。更新或重建 Android 模板时，不得恢复旧实现。
3. 根 `build.gradle.kts` 会从 Android library 的 consumer ProGuard 列表中仅移除不存在的文件。原因是锁定的 dialog 2.7.2、fs 2.5.1 发布包声明了 `consumer-rules.pro` 却没有携带它，AGP 9 的 Release 构建会在 `mergeReleaseConsumerProguardFiles` 失败。
4. app 的 `lint.checkReleaseBuilds=false` 是临时绕过。AGP 9 的 `lintVital` 曾在分析通过字符串形式应用的 Tauri Kotlin Gradle 脚本时内部崩溃，错误包含 `findFirCompiledSymbol only works on compiled declarations`。这不是一次正常的 lint 代码告警；关闭它不影响 R8/ProGuard，但会失去 Release 打包前的 lint 阻断。
5. `gen/android` 是 Tauri 生成目录但包含人工迁移：Gradle wrapper、根/app Gradle 脚本、`buildSrc`、daemon JVM 条件和 rustls verifier Maven 解析都不可被 `tauri android init` 直接覆盖。升级模板时应在临时 worktree/副本生成新工程后逐文件比较。

### 二次检查入口

- Tauri Gradle 9 模板：<https://github.com/tauri-apps/tauri/pull/15828>。该变更已合并到 `dev`、目标版本为 2.12；稳定版是否发布以 <https://github.com/tauri-apps/tauri/releases> 为准。
- Tauri 后续完整内置 Kotlin 迁移讨论：<https://github.com/tauri-apps/tauri/pull/15335>。不要仅因 Gradle 9 模板发布就提前删除 legacy Kotlin 开关。
- 插件缺失 consumer ProGuard 文件的修复：<https://github.com/tauri-apps/plugins-workspace/pull/3531>。dialog 2.7.3 和 fs 2.5.2 已包含 `consumer-rules.pro`，可以单独安排回收第 3 项绕过。
- 每次检查先运行以下窄范围命令确认实际版本和 daemon，不运行全量测试：

```bash
cargo tree --manifest-path ui/lianghua_web/src-tauri/Cargo.toml \
  -p tauri -p tauri-build -p tauri-plugin-dialog -p tauri-plugin-fs -p tauri-plugin-log --depth 0
cd ui/lianghua_web/src-tauri/gen/android
./gradlew --version
./gradlew help --no-daemon
```

### 回收与恢复顺序

一次只回收一个兼容项，失败时恢复该项，禁止同时删除全部绕过后再猜测故障来源。

1. 先回收缺失 ProGuard 文件绕过：将 Rust dialog 更新到 2.7.3、fs 更新到 2.5.2，并同步对应 npm 插件；确认生成的 `tauri.settings.gradle` 指向新 crate 后，删除根 `build.gradle.kts` 的 `LibraryExtension` import 和过滤缺失 consumer ProGuard 文件的 `subprojects` 块。只跑一次 aarch64 Release APK 验证；若仍报缺文件，恢复该块并记录具体插件路径。
2. 再尝试恢复 Release lint：将 app 末尾改为 `apply(from = file("tauri.build.gradle.kts"))`，删除 `lint { checkReleaseBuilds = false }`，只跑 aarch64 Release APK。上游确认字符串形式的 `apply` 会触发相关分析问题；若 `findFirCompiledSymbol` 仍出现，恢复 lint 绕过并等待新版 lint/Tauri 模板。
3. Tauri 2.12 发布后，在临时 worktree 生成官方 Android 工程，与当前 `gen/android` 比较后逐项吸收 Gradle、AGP、wrapper、`buildSrc` 和 `compilerOptions` 变化。保留本项目的 rustls verifier、applicationId、min/target SDK、Release 规则和 daemon Java 21 取舍，完成 Debug 与 aarch64 Release 验证后才能替换现有模板。
4. 不要在 Tauri 2.12 阶段删除 `android.builtInKotlin=false`/`android.newDsl=false`；只有官方插件全部停止应用旧 Kotlin 插件且上游模板删除这些开关后，才单独迁移并验证。当前上游计划点是 v3。
5. Java 25 daemon 只在新版 JBR/Gradle/AGP 下连续通过 Debug 和 Release 且不再生成 `hs_err_pid*.log` 后再考虑。测试失败就保留 Java 21 daemon；CI 的 Temurin 21 无需随 Android Studio JBR 升级。

定向构建命令：

```bash
cd ui/lianghua_web/src-tauri
cargo tauri android build --ci --debug --target aarch64 --apk
cargo tauri android build --ci --target aarch64 --apk
```
