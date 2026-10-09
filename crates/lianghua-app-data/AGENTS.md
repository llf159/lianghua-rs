# 组件取舍

后续 agents 修改本组件时请继续维护本文件。

- 本 crate 负责托管数据源的下载编排、导入和查看，不放置底层数据实现。
- 下载实现继续属于后端 `lianghua-download`；这里仅保留面向适配层的准备、进度和结果 DTO。
- 直接依赖 data/download 等能力所有者，禁止重新引入聚合导出层。
- 依赖 `lianghua-app-shared` 统一日期语义，不得依赖 market、strategy 或 facade 应用 crate。
- 只运行 `cargo test -p lianghua-app-data`，不要因此触发全 workspace 测试。

- 目录导出与适配层 ZIP 导出共享临时路径过滤，跳过 `.tmp` 文件/目录、`.tmp.wal` 及新筹码重建锁文件；不能笼统排除 `.wal`，正式数据库 WAL 可能包含已提交数据。过滤只影响导出，不删除源目录的重建残留或活动临时文件。
- 问题：指标列补算把全历史数据堆进临时表并用一次 UPDATE 合并，长周期历史使数据库查询和事务内存峰值过高；解决方案选择：每累计约 131,072 行就合并提交并清空临时表；解释：用户允许补算短时新增约 1GB 内存，采用较大批次减少反复合并的开销，但不把该行数视为硬内存上限；失败时已提交批次可由重跑覆盖。
- 问题：单股补救行情已提交后若在筹码维护前闪退，仅依赖本轮下载结果会遗漏该股历史筹码重算；解决方案选择：增量维护读取原始库的 `pending_chip_repair`，成功完成筹码维护后再清除，策略变更而跳过维护时保留；解释：重复修复可覆盖相同股票，而提前清除会永久失去崩溃恢复线索。下次排查可在原始库执行 `SELECT * FROM pending_chip_repair`，查看待修复股票。

- 问题：全历史指标补算的批次读取若独立 `Connection::open`，会为同一文件创建多个 DuckDB 实例，写入实例提交后其他实例的元数据和块缓存仍可能过期；解决方案选择：所有批次从写入连接 `try_clone`，由 `DataReader::new_with_connection` 接收，写入连接保持存活至全部计算线程退出；解释：并发连接必须共享同一数据库实例，才能共享 MVCC、目录和块管理，不能把克隆连接替换成重新打开文件。下次可运行 `cargo test -p lianghua-app-data download::tests::indicator_rebuild_uses_persisted_moneyflow_columns -- --exact`，并检查 `rg -n "try_clone|new_with_connection" crates/lianghua-app-data/src/download.rs crates/lianghua-data/src/data/mod.rs`。
