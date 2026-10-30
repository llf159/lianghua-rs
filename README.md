# lianghua-rs

股票行情、策略评分、盘中监控与回测应用，使用 Rust 工作区和 React + Tauri 界面。

工程代码位于 `crates/`，前端及 Tauri 应用位于 `ui/lianghua_web/`。

## 使用说明

- [跟踪策略：配置、观察与续算](docs/tracking-strategies.md)
- [坚果云 WebDAV 同步：配置、备份与故障排查](docs/cloud-sync.md)
- [自定义指标：逐股、市场与横截面](docs/indicator-system.md)
- [市场环境分析：原始行情、覆盖口径与十个维度](docs/market-environment-analysis.md)
- [筹码单股测试：生成机制对照与未知成本](docs/chip-mechanism-comparison.md)

- [研究回测：实验存档、条件对照、排名增量与交易模拟](docs/research-backtest.md)

## 工程维护

- [项目工程约束](AGENTS.md)
- [组件取舍目录](crates/README.md)
- [前端工程取舍](ui/lianghua_web/AGENTS.md)
- [Tauri 工程取舍](ui/lianghua_web/src-tauri/AGENTS.md)
