# 组件取舍

后续 agents 修改本组件时请继续维护本文件。

- 本 crate 负责评分规则执行、场景计算、评分结果构造和规则缓存。
- 跨组件评分类型从 `lianghua-model` 导入；结果数据库操作从 `lianghua-data::data::scoring_store` 导入。
- 不得重新创建混合类型、持久化和计算职责的 `scoring_data` 模块或旧路径兼容导出。
- scoring 可以依赖 data 与 model，但不得被 data 或 model 反向依赖。
- 只运行 `cargo test -p lianghua-scoring`，不要因此触发全 workspace 测试。

- 评分输入批次统一为 32 股，写库队列只缓存 2 批：保留 Rayon 并行和批量读取，但避免每个工作线程同时持有 128 股历史明细，写库落后时及时反压。内存返回接口仍按调用契约保留完整结果。
