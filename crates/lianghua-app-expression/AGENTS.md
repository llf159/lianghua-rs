# 组件取舍

- 本 crate 只把底层表达式能力转换成适配层需要的描述数据，不执行市场或策略业务。
- 保持依赖最小化，只依赖 `lianghua-core` 和序列化库，以获得最快的增量编译。
- 只运行 `cargo test -p lianghua-app-expression`，不要因此触发全 workspace 测试。
