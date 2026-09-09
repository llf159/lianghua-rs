# 组件取舍

- 本 crate 负责排名、规则、场景回测与统计模拟，属于 data/scoring 之上的业务能力层。
- 直接依赖 `lianghua-data` 与 `lianghua-model`；回测只消费评分结果契约，不依赖 `lianghua-scoring` 的计算实现。
- 回测结果可以被应用层消费，但 data、model 和 scoring 不得反向依赖本 crate。
- 全策略规则回测的触发明细必须与 `parallel_batch_size` 同批加载并在批次结束后释放；该参数既约束计算并发，也约束触发明细的常驻内存。
- 构建规则回测公共残差缓存时，个股原始涨跌幅按残差批次读取和释放，禁止让全市场原始序列与全量残差结果同时常驻。
- 规则回测公共缓存用压缩位图记录评分样本范围，残差按股票批次直接写入最终 `day_groups`；禁止在 `universe_rows` 与最终样本之间恢复全量双层 `residual_map_cache`。
- 只运行 `cargo test -p lianghua-backtest`，不要因此触发全 workspace 测试。
