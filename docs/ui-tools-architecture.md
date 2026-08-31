# UI 工具应用层架构

原来的 `lianghua-app::ui_tools` 已按业务能力拆成独立 crate，聚合 crate 随后被完全移除。
Tauri 直接依赖各能力 crate，从而缩小 Rust 增量编译单元，并让依赖关系在 Cargo 清单中可见。

```text
lianghua-app-shared       日期、证券代码和股票元数据
lianghua-app-expression   面向适配层的表达式能力描述
lianghua-app-chart        图表指标及指标配置
lianghua-app-data         托管数据源下载、导入和查看
lianghua-app-market       实时行情和盘中市场观察
lianghua-app-strategy     策略编辑、排名、分析和验证
lianghua-app-facade       组合多个能力的页面级用例
```

## 实际代码结构

每个业务 crate 根目录都包含 `Cargo.toml` 和 `AGENTS.md`：前者声明最小依赖，后者记录该
组件的工程取舍与修改约束。源码结构如下：

```text
crates/
├── lianghua-app-shared/src/
│   ├── lib.rs                    公开中立应用能力
│   ├── date.rs                   交易日期规范化与解析
│   ├── symbol.rs                 证券代码规范化
│   └── stock_metadata.rs         名称、行业、概念和市值等元数据查询
├── lianghua-app-expression/src/
│   ├── lib.rs                    表达式应用层入口
│   └── capabilities.rs           适配层可用字段与函数描述
├── lianghua-app-chart/src/
│   ├── lib.rs                    图表域入口
│   ├── indicator.rs              指标配置、编译与执行
│   └── indicator_settings.rs     指标配置管理用例
├── lianghua-app-data/src/
│   ├── lib.rs                    数据管理域入口
│   ├── download.rs               下载编排、状态与进度 DTO
│   ├── import.rs                 托管数据源导入
│   └── viewer.rs                 数据表查看与预览
├── lianghua-app-market/src/
│   ├── lib.rs                    市场观察域入口
│   ├── realtime.rs               实时行情获取与规范化
│   ├── intraday_monitor.rs       单组盘中监控模板
│   ├── all_market_monitor.rs     全市场监控
│   ├── watch_observe.rs          观察列表
│   ├── dragon_tiger.rs           龙虎榜用例
│   └── scene_stage.rs            市场域私有场景阶段辅助逻辑
├── lianghua-app-strategy/src/
│   ├── lib.rs                    策略域入口
│   ├── manage.rs                 策略配置管理
│   ├── stock_pick.rs             表达式与概念选股
│   ├── overview.rs               场景策略概览
│   ├── overview_classic.rs       经典策略概览
│   ├── convolution_rank.rs       卷积排名
│   ├── ranking_compute.rs        排名计算编排
│   ├── statistics.rs             策略统计与验证分析
│   ├── paper_validation.rs       纸面交易验证
│   ├── stock_similarity.rs       个股相似度
│   ├── trigger_similarity.rs     触发条件相似度
│   └── trigger_similarity/
│       └── ranking.rs            触发相似度排名实现
└── lianghua-app-facade/src/
│   ├── lib.rs                    页面组合入口
│   ├── details.rs                股票详情页组合用例
│   └── cyq_chen.rs               陈氏筹码页面组合用例
```

## 依赖方向

```text
shared ──────> data
   ├─────────> market <──── expression
   ├─────────> strategy <── backtest
   └─────────> facade <──── chart / market / strategy

Tauri 适配层 ──> chart / data / expression / facade / market / strategy
```

图中只表达应用层的主要依赖约束；各 crate 直接依赖自己需要的后端能力所有者，不经过
聚合门面。

- `shared` 不得依赖其他应用层 crate。
- `market` 只允许依赖 `shared` 与 `expression`，不得依赖 `strategy` 或 `facade`。
- `strategy` 允许依赖 `shared` 与后端回测能力，不得依赖 `market` 或 `facade`。
- 跨 chart、market、strategy 的页面组合统一放在 `facade`。
- Tauri 必须直接依赖实际使用的应用能力 crate，不得重新创建 `ui_tools` 聚合层。
- 通用表达式、持久化、数据提供方、评分和回测逻辑继续属于对应后端 crate。

## 调用方式

历史 `lianghua_app::ui_tools::*` 路径已经删除。调用方从实际所有者导入，例如：

- 图表配置：`lianghua_app_chart::indicator_settings`；
- 数据管理：`lianghua_app_data::download`；
- 实时市场：`lianghua_app_market::intraday_monitor`；
- 策略管理：`lianghua_app_strategy::manage`；
- 股票详情组合：`lianghua_app_facade::details`。

新增调用也必须直接依赖拥有该能力的最窄 crate，不得为了缩短路径恢复兼容别名或聚合 crate。

## 编译取舍

- 应用层 crate 使用普通 dev 优化级别，避免日常开发为页面编排代码支付 `opt-level = 3` 的编译成本。
- 计算密集的 data、download、scoring、backtest 等后端 crate 仍保持 dev 优化。
- `strategy` 暂时是最大的应用层编译单元。它内部类型和测试共享较多，本次先隔离业务域，
  不为了追求 crate 数量继续制造不稳定边界。
- 每个 crate 的具体边界与后续约束记录在其 `AGENTS.md`。

## 文件增长规则

每个公开模块应代表一个应用用例，或一组紧密相关的应用用例。若某个模块需要复用其他页面
的辅助能力，应将能力下沉到 `shared`、业务域私有模块或后端 crate，不得仅为了复用 DTO、
解析函数或查询辅助函数而让页面模块彼此依赖。
