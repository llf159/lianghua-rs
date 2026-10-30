# Tauri crate 取舍

后续 agents 修改本组件时请继续维护本文件，只记录难以从代码理解的工程取舍；首次设计和普通 bug 修复不记，没有合适记录时不要硬写。

- 问题：Android release 的 LLVM 优化在 Rust 1.95.0 下会因编译线程栈不足触发 SIGSEGV；解决方案选择：`rust-toolchain.toml`、Android CI 和本机 `cargo-tauri` 入口统一使用 stable，并保留 `RUST_MIN_STACK=134217728` 与 131072 KiB 的 shell 栈上限；解释：按用户要求只保留最新稳定工具链，同时保留针对旧版 LLVM 栈溢出的保护；新版是否仍需要该保护及 Android 构建兼容性尚未验证。

- 问题：东财人工验证后外部浏览器恢复，但 Rust 直连仍在 TLS 握手阶段断开，复制 Cookie 不能保证复用验证状态；解决方案选择：用户在独立 WebView 手动验证，分时通过同一窗口的官网 EventSource 接口取得首个完整快照后立即关闭并复用 Rust 严格解析，只在单股探测成功后接入补齐，不向远程页面授予应用 IPC 权限；解释：保留浏览器会话与网络栈，代价是补齐期间必须保留验证 WebView；桌面关闭按钮仅隐藏窗口以保留会话，再次打开恢复显示；每 200ms 读取一次异步结果、请求 12 秒超时，平衡 WebView 调度开销与补齐延迟，仍沿用后台源限速和监控暂停行为，不自动处理验证码。Android 验证页放在独立 singleInstance task，返回按钮和系统返回键只切回监控，避免 MainActivity 的 singleTask 回栈清掉验证页；Tao 的 set_focus 在 Android 未实现，重新显示验证页须走原生活动 Intent。

- 问题：长时间相似度计算会被桌面电源策略视为空闲，移动页面又可能被系统自动释放唤醒锁；解决方案选择：Linux 计算期间使用 systemd-inhibit --what=idle:sleep，结束由 RAII 释放，申请失败只记录警告；移动 WebView 使用页面级 Screen Wake Lock，重新可见时再申请、计算结束释放；解释：以临时、可释放的申请避免改永久电源配置或申请常驻移动电源权限，申请失败不阻断计算。

- 问题：Snap 的 Android Studio/JBR 自动升级可能改变实际构建 JVM，旧组合曾出现 Java 25 HotSpot 崩溃；解决方案选择：gen/android/gradle/gradle-daemon-jvm.properties 将 daemon 固定为 Java 21，JDK 路径只放用户级 ~/.gradle/gradle.properties，CI 使用 Temurin 21；解释：让构建 JVM 独立于 IDE 更新，不把机器路径或会被 Snap 清理的 revision 路径写进仓库。下次先在 gen/android 执行 ./gradlew --version 和 ./gradlew help --no-daemon；若找不到 Java 21，安装 JDK 21 并更新用户级 Gradle 的 JDK 路径。只有候选 JVM 连续通过 Debug/Release 且不再产生 hs_err_pid*.log 时才考虑解除固定。

- 问题：AGP 9 默认内置 Kotlin 与当前工程及插件使用的 org.jetbrains.kotlin.android 冲突；解决方案选择：保留 android.builtInKotlin=false 与 android.newDsl=false；解释：以继续使用旧插件/DSL 为代价兼容现有 Tauri Android 工程，只有生成模板和全部插件都完成迁移后才单独撤销并验证，不能仅因 Tauri 版本升级就删除。

- 问题：锁定插件的 Android 发布包曾声明未携带的 consumer-rules.pro，Release 合并因此失败；解决方案选择：根 build.gradle.kts 仅过滤不存在的 library consumer ProGuard 文件；解释：缩小绕过范围，保留已有规则与 R8。升级插件并确认实际解析包已携带所声明文件后，删除 LibraryExtension import 与过滤块，只验证一次 aarch64 Release APK；若仍缺文件，恢复该项并定位具体插件。

- 问题：Release lint 曾在分析字符串形式应用的 Tauri Gradle 脚本时内部崩溃，报 findFirCompiledSymbol only works on compiled declarations；解决方案选择：暂时保留 lint.checkReleaseBuilds=false；解释：绕过分析器故障以允许打包，代价是失去 Release lint 阻断，R8/ProGuard 仍开启。回收时先改为 apply(from = file("tauri.build.gradle.kts"))，再删除 lint 绕过并验证一次 aarch64 Release APK；同类内部崩溃仍存在时恢复该项。

- 问题：Tauri 生成的 gen/android 已包含 Gradle、daemon JVM 与 rustls verifier 等人工迁移，直接重新初始化会覆盖这些配置；解决方案选择：升级模板时在临时 worktree/副本生成后逐文件比较，一次只撤销一个兼容项；解释：增加迁移时的人工比较成本，换取已有取舍不丢失及失败来源可定位。BuildTask 的 ExecOperations 已是 Gradle 9 API 适配，不作为临时绕过撤销。

- 问题：外接数据仓库不在 $APPDATA 作用域内，符号链接也会经 canonicalize 落到作用域外；解决方案选择：启动时根据实际仓库根目录调用 fs_scope().allow_directory，前端文件操作传绝对路径，前后端共用 warehouse_root 入口；解释：以运行时限定实际目录替代扩大静态全局文件权限，避免页面与后台读写不同仓库；仅修改 capabilities 或创建符号链接不能解决 PathForbidden。

- 问题：移动数据仓库可能跨文件系统，rename 不适用，而复制时数据库连接仍可能写入；解决方案选择：复制后校验文件数与总字节再切根目录，目标 source 非空时拒绝，不删除源目录；解释：保留源数据以便校验失败时恢复，代价是占用双份磁盘空间，且该校验不保证数据库内容的一致快照；复制期间须停止其他页面的数据操作。

兼容项的窄范围验证命令（按本次撤销项只选所需构建）：

```bash
cd ui/lianghua_web/src-tauri
cargo tauri android build --ci --debug --target aarch64 --apk
cargo tauri android build --ci --target aarch64 --apk
```

- 问题：云同步截取榜单或恢复数据库快照时，普通 IPC 下载、计算及策略保存可能同时访问同一文件；解决方案选择：普通本地 IPC 任务持 SOURCE_ACCESS 共享锁，普通同步只在快照与应用下载时尝试独占，网络传输阶段释放独占锁；手动恢复从读取待保留的本地内容到提交恢复持续独占；解释：保留原有任务之间的并行，让同步在已有任务运行时提示稍后重试，避免普通同步长期网络等待阻塞本地编辑；明确恢复时则优先确保本地未同步内容不会在保留和替换之间被修改。新增访问仓库或持久配置的 IPC 入口需要继续参与此门禁。


- 问题：Tauri/Wry 仍依赖 JNI 0.21，而平台证书验证器 0.7 依赖不兼容的 JNI 0.22；解决方案选择：平台证书验证器固定 0.6.2 并与 Reqwest 共用，JNI 使用 0.21，jni-sys 固定 0.3.0；解释：保留 Android 系统证书验证和现有 WebView 调用，避免两代 JNI 同时进入应用；jni-sys 0.3.1 通过兼容层再依赖 0.4，固定 0.3.0 才能消除重复；Reqwest 对 0.6 与 0.7 的约束均允许，全量更新可能重新引入 0.7；发现后执行 `cargo update -p rustls-platform-verifier@0.7.1 --precise 0.6.2`。待 Tauri/Wry 升级后可连同验证器一起更新。

- 问题：跨页面启动多个研究和模拟任务会叠加两套评分及后验内存，页面离开或进程退出又可能丢失尝试记录；解决方案选择：三类研究入口共用单任务门禁，计算前创建运行日志，成功最后提交完成标记，异常由租约或下次进程识别为中断；解释：以研究任务并行吞吐换取平板内存余量，旧运行保持不可覆盖；内容校验期间仍参与仓库独占门禁，未开启时明确不认证输入一致性。导入导出用文件插件流而非普通路径，兼容Android系统文件选择器的content URI。
