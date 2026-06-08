# market_analyze

本目录是独立 Cargo 分析工具，不属于根 crate/workspace。它只读本机应用数据目录，用于生成主板强势股、策略触发、起点前 K 线特征对比报告。

默认数据目录写在源码里的 `SOURCE` 常量：

```text
/home/lmingyuanl/.local/share/com.mingyuan.lianghua/source
```

## 运行

从本目录运行：

```bash
LIBRARY_PATH=/usr/local/lib LD_LIBRARY_PATH=/usr/local/lib cargo run --quiet
```

生成：

- `/tmp/lh_market_analyze/report_expanded.md`
- `/tmp/lh_market_analyze/out/winners_expanded.csv`

起点前 K 线特征对比：

```bash
LIBRARY_PATH=/usr/local/lib LD_LIBRARY_PATH=/usr/local/lib cargo run --quiet --bin prestart
```

生成：

- `/tmp/lh_market_analyze/prestart_feature_report.md`
- `/tmp/lh_market_analyze/out/prestart_strong_samples.csv`

## 口径

- 只看主板、非 ST。
- 默认参考最近 60 个交易日。
- `main.rs`：每个参考日取当日、20 日区间、3 日子区间涨幅 Top20，统计策略触发和总榜捕捉。
- `src/bin/prestart.rs`：以 20 日区间强势榜的第一天为“起点”，比较强势票起点前 T-1/T-5/T-10 与同日普通主板股票的 K 线特征差异。

