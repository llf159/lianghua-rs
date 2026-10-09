# 项目文档总目录

这里汇总当前工作区的工程约束、筹码指标研究和市场行为策略研究。研究报告保留原始口径、结果和复跑说明；本目录只提供阅读入口，不替代各报告。

## 从哪里开始

| 想了解 | 先读 | 接着读 |
|---|---|---|
| 周末及节假日前下跌是否能改善现有洗盘策略 | [周末与节前下跌报告](research/preholiday_washout/weekend_report.md) | [协议](research/preholiday_washout/protocol.md)、[研究目录](research/preholiday_washout/README.md)、[复跑脚本](scripts/preholiday_washout_research.py) |
| 用指数、成交量与大小盘风格判断当前市场状态 | [线性市场状态与行业分化](research/market_state_linear/report.md) | [研究目录](research/market_state_linear/README.md)、[协议](research/market_state_linear/protocol.md)、[复跑脚本](scripts/linear_state_research.py) |
| 此前全部市场行为与筹码研究在2011—2026长历史下哪些有答案 | [长历史总报告](research/long_history_all_prior/report.md) | [逐篇覆盖表](research/long_history_all_prior/coverage.md)、[研究协议](research/long_history_all_prior/protocol.md) |
| 小涨后为何掉榜、是否该保留 | [完整历史掉榜研究](research/chip_duration/rank_confirmation/long_history/dropout_restart_report.md) | [确认策略交接](STRATEGY_CONFIRMATION_HANDOFF.md)、[确认规则分支复核](research/chip_duration/rank_confirmation/long_history/restart_report.md) |
| 两个新筹码字段是什么、能不能写加分策略 | [解套后的累计换手研究](research/chip_duration/rebound_full_history/turnover_report.md) | [长周期下跌反弹研究](research/chip_duration/rebound_full_history/report.md)、[完整数据库策略研究](research/chip_duration/full_db_v2/report.md) |
| 新字段的计算、落盘和旧库重建 | [生产接入](research/chip_duration/production_integration.md) | [指标定义与公式](research/chip_duration/metrics.md)、[实现交接](research/chip_duration/implementation_handoff.md) |
| 主板、宽幅板及正式评分策略的历史研究 | [市场行为研究包目录](market_behavior_handoff/README.md) | [01—25号报告的阅读顺序](market_behavior_handoff/README.md#阅读顺序) |
| 研究数据、复跑命令和历史筹码实验 | [筹码研究目录](research/chip_duration/README.md) | [研究协议](research/chip_duration/field_behavior_v2/protocol.md)、[市值分层协议](research/chip_duration/field_behavior_v2/size_protocol.md) |
| 修改代码时遵守的约束 | [项目工程约束](AGENTS.md) | 对应组件的 `AGENTS.md` |

## 新筹码指标：当前资料

当前代码的 `CYQ_TRAP_COEF` 是版本2，按筹码份额、亏损深度和连续套牢时间逐批计算；`CYQ_REAL_LOSS20` 是最近20日模拟卖出亏损的日均。版本1研究的套牢阈值不能套到版本2。

| 顺序 | 文档 | 内容与状态 |
|---|---|---|
| 长周期追加 | [下跌反弹有效性与配合因子](research/chip_duration/rebound_full_history/report.md) | 2022—2026年的完整可用信号期；检查新字段与量能、模型成本、主力净流量能否共同判断反弹有效性。 |
| 事件顺序追加 | [解套后的累计换手](research/chip_duration/rebound_full_history/turnover_report.md) | 压力释放后才累计换手；固定最低换手量并未稳定提高反弹有效性。 |
| 0 | [完整数据库基础研究、样本分析与条件优化](research/chip_duration/full_db_v2/report.md) | 正式schema 5版本2摘要全库；三条表达式和一个低换手优化均未形成稳定上涨加分证据。 |
| 1 | [字段行为、简单表达式与触发样本](research/chip_duration/field_behavior_v2/report.md) | 640股原生重放；研究字段数值与走势，形成三条简单表达式，再分析优势与劣势样本。尚未得到稳定的独立上涨加分证据。 |
| 2 | [固定市值档位与动态三分位](research/chip_duration/field_behavior_v2/size/report.md) | 区分“涨3%导致K降61%”的数学算例与真实样本；保留三条原表达式，不根据分组结果改权重。 |
| 3 | [生产接入](research/chip_duration/production_integration.md) | 字段口径、数据库列与版本、历史重建和排查方式；全库研究已读取完成重建的实际筹码库。 |
| 4 | [指标定义](research/chip_duration/metrics.md) | 浮亏、浮盈、套牢时长、模拟实现亏损的公式和边界。 |
| 5 | [实现交接](research/chip_duration/implementation_handoff.md) | 从原模型到字段落盘与调用链的设计参考。 |

详细的[版本2研究协议](research/chip_duration/field_behavior_v2/protocol.md)、[市值分层协议](research/chip_duration/field_behavior_v2/size_protocol.md)、[研究表达式配置](research/chip_duration/field_behavior_v2/candidates.toml)与逐项结果，见[筹码研究目录](research/chip_duration/README.md)。

## 历史筹码研究

下列报告保留历史实验和复跑依据。其中简化研究与六条候选策略使用版本1的套牢系数；报告内旧阈值不适用于当前版本2。

| 文档 | 用途 |
|---|---|
| [初始完整报告](research/chip_duration/report.md)、[初始研究协议](research/chip_duration/protocol.md) | 原始套牢时长、浮动盈亏和近期交换盈亏研究。 |
| [单系数与窗口简化](research/chip_duration/simplification_report.md)、[简化协议](research/chip_duration/simplification_protocol.md) | 历史版本1的份额×时间系数和20日窗口选择。 |
| [市值分组追加研究](research/chip_duration/size_report.md)、[追加协议](research/chip_duration/size_protocol.md) | 历史因子按全市场动态市值分位验证。与上方版本2市值分层是两次不同研究。 |
| [六条候选策略测试](research/chip_duration/strategy_report.md) | 版本1阈值及当时的原生触发核验，不能直接加载为版本2策略。 |
| [已放弃的复杂形态方案](research/chip_duration/behavior_v2/protocol.md) | 曾先组合量价形态和状态记忆，后改为先研究字段行为；仅作过程记录。 |

## 市场行为策略研究

[市场行为研究包](market_behavior_handoff/README.md)已包含01—25号报告的完整目录、对应策略文件、阶段状态、开放问题及复跑入口。本总目录只指向该包；请以每篇报告记录的策略版本、日期和结果库状态解释历史结论，避免把旧快照当成当前评分结果。

## 工程与应用文档

| 范围 | 文档 |
|---|---|
| 全工程 | [项目工程约束](AGENTS.md) |
| 数据与评分 | [数据组件约束](crates/lianghua-data/AGENTS.md)、[评分组件约束](crates/lianghua-scoring/AGENTS.md)、[回测组件约束](crates/lianghua-backtest/AGENTS.md) |
| 应用 | [策略应用约束](crates/lianghua-app-strategy/AGENTS.md)、[表达式应用约束](crates/lianghua-app-expression/AGENTS.md)、[数据应用约束](crates/lianghua-app-data/AGENTS.md)、[前端约束](ui/lianghua_web/AGENTS.md)、[Tauri约束](ui/lianghua_web/src-tauri/AGENTS.md) |
| 其他组件 | [组件索引](crates/README.md) |

`research/`与`market_behavior_handoff/`目前被项目的 `.gitignore` 排除；上述研究链接对应当前工作区的本地资料。分享或迁移这些报告时，需要连同对应目录一起保存。工程代码中的正式策略与研究目录里的候选配置分别管理。

新增研究文档时，先更新所属研究目录的 README，再更新本总目录的入口和版本状态；保留旧报告的原始口径与结果。
