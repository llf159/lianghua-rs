# 组件取舍

- 本 crate 只保存跨组件共享的领域类型和契约，不实现数据库、网络或评分算法。
- 评分结果行、评分批次、场景阶段、写入消息和性能记录统一位于 `scoring` 模块。
- 类型只有在至少两个上层组件共享时才进入 model；单组件内部状态留在所属 crate。
- 禁止为缩短导入路径反向依赖 data、scoring 或应用层 crate。
- 只运行 `cargo test -p lianghua-model`，不要因此触发全 workspace 测试。
