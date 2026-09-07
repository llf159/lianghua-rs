# 组件取舍

后续 agents 修改本组件时请继续维护本文件。

- 本 crate 负责托管数据源的下载编排、导入和查看，不放置底层数据实现。
- 下载实现继续属于后端 `lianghua-download`；这里仅保留面向适配层的准备、进度和结果 DTO。
- 直接依赖 data/download 等能力所有者，禁止重新引入聚合导出层。
- 依赖 `lianghua-app-shared` 统一日期语义，不得依赖 market、strategy 或 facade 应用 crate。
- 只运行 `cargo test -p lianghua-app-data`，不要因此触发全 workspace 测试。

- 目录导出与适配层 ZIP 导出共享临时路径过滤，跳过 `.tmp` 文件/目录、`.tmp.wal` 及新筹码重建锁文件；不能笼统排除 `.wal`，正式数据库 WAL 可能包含已提交数据。过滤只影响导出，不删除源目录的重建残留或活动临时文件。
