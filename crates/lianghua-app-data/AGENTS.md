# 组件取舍

- 本 crate 负责托管数据源的下载编排、导入和查看，不放置底层数据实现。
- 下载实现继续属于后端 `lianghua-download`；这里仅保留面向适配层的准备、进度和结果 DTO。
- 直接依赖 data/download 等能力所有者，禁止重新引入聚合导出层。
- 依赖 `lianghua-app-shared` 统一日期语义，不得依赖 market、strategy 或 facade 应用 crate。
- 只运行 `cargo test -p lianghua-app-data`，不要因此触发全 workspace 测试。
