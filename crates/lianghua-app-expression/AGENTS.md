# 组件取舍

- 本 crate 只把底层表达式能力转换成适配层需要的描述数据，不执行市场或策略业务。
- 保持依赖最小化，只依赖 `lianghua-core` 和序列化库，以获得最快的增量编译。
- 只运行 `cargo test -p lianghua-app-expression`，不要因此触发全 workspace 测试。
- 新增或改名 `INTRADAY_REALTIME_FIELDS` 时必须同步前端 `ui/lianghua_web/src/shared/expressionValidation.ts` 的 `KNOWN_EXPRESSION_IDENTIFIERS`：表达式回测“参数研究”的自动识别只能靠这份白名单区分“可用字段”与“待扫描参数”，漏同步的实时字段会被替换成常数。
