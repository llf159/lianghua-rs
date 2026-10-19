# 自定义指标：逐股、市场与横截面

在“指标管理”中维护名称、计算范围、表达式和精度，保存后执行“指标列补算”。配置统一保存在 `ind.toml`；省略 `scope` 时仍按原来的逐股指标计算，已有 KDJ 等配置无需改动。

| scope / 界面范围 | 计算结果 | 使用场景 |
| --- | --- | --- |
| `stock` / 逐股 | 每只股票自己的报价历史序列 | KDJ、均线、量比等原有指标；下载时随该股更新 |
| `market` / 市场 | 每个市场交易日一个共享值 | 涨跌家数、市场波动、成交集中、用户筛选样本的统计 |
| `cross_section` / 横截面 | 每个市场交易日每只股票一个值 | 全市场或组内排名、标准化、相对市场强弱、组均值 |

市场及横截面范围在本轮前复权行情、指数行情及逐股指标完成后计算。它们使用现有表达式引擎的 `MA`、`SUM`、`REF`、`STD`、`IF`、`EMA` 等函数；新增的 `CS_*` 专门沿同一天的股票维度运算。也可用前面声明的指标名称作为输入；不支持前向引用、循环依赖。逐股范围依赖跨股票结果时，请把范围改为横截面。

## 跨股票函数

下面的 `filter` 和 `group` 可省略。`filter` 非零且非空才纳入；`group` 是用户表达式产生的数值分组键，按相等的值分组。指定 `group` 时，统计结果只返回给该组通过筛选的股票，因此输出属于逐股结果，范围应选横截面。分组键为空的股票排除。

| 函数 | 含义 |
| --- | --- |
| `CS_MEAN(x[, filter[, group]])` | 当日截面均值 |
| `CS_SUM(x[, filter[, group]])` | 当日截面求和 |
| `CS_COUNT(x[, filter[, group]])` | 符合筛选且 x 有限的股票数；统计上涨家数可写 `CS_COUNT(1, PCT_CHG>0)` |
| `CS_MIN` / `CS_MAX` | 参数同上，截面最小／最大值 |
| `CS_MEDIAN` | 参数同上，截面中位数 |
| `CS_STD` / `CS_VAR` | 参数同上，截面样本标准差／方差，分母为 n−1；不足两股为空 |
| `CS_QUANTILE(x, q[, filter[, group]])` | 线性插值分位数，q 为 [0,1] 常数 |
| `CS_RANK(x[, filter[, group]])` | 从小到大的平均并列名次除以有效样本数，最大值接近1；返回各股值 |
| `CS_ZSCORE(x[, filter[, group]])` | `(x−组均值)/组样本标准差`，零方差为空；返回各股值 |
| `CS_CORR(x, y[, filter[, group]])` | 成对有效样本的 Pearson 相关 |
| `CS_SPEARMAN(x, y[, filter[, group]])` | 成对有效样本的平均并列秩相关 |
| `CS_WMEAN(x, weight[, filter[, group]])` | 非负权重加权均值，权重和为0或存在负权重时为空 |
| `CS_TOPSHARE(x, q[, filter[, group]])` | 非负 x 中最大 ceil(q×样本数) 个的合计占比；总量为0或存在负值时为空 |

所有函数忽略缺失、非有限的数据，不把它们当成0。无有效样本时返回空值；无分组的 `CS_COUNT` 在当日有行情但无人符合筛选时为0。并列秩使用固定起点的1e−12容差；不能保证“前20%”恰好包含20%的股票。相关性不足或分母为0时为空。

这些函数需要全市场上下文，只能出现在市场／横截面指标配置里。策略、图表和选股表达式使用补算后的指标名称，不在单股运行时重新扫描全市场。

## 新增通用数学函数

`LN(x)`、`EXP(x)`、`SQRT(x)` 可用于所有表达式。定义域错误或溢出返回空值。`ISVALID(x)` 对有限数值返回1、空值或非有限值返回0；`NULL()` 显式产生空值，例如 `IF(条件, 数值, NULL())`。

原有 `STD(x,n)` 是总体标准差，分母为 n；截面 `CS_STD` 则使用样本标准差，分母为 n−1。需统一口径时由公式显式换算。

## 可直接使用的示例

以下是普通用户配置，名称、周期、样本池和精度均可修改，不需要修改 Rust 或前端代码。

```toml
version = 1

[[ind]]
name = "MY_MARKET"
scope = "market"
expr = "MA(CS_MEAN(PCT_CHG, LISTED_DAYS >= 60), 5)"
prec = 6

[[ind]]
name = "MY_STRENGTH"
scope = "cross_section"
expr = "CS_RANK(SUM(PCT_CHG, 10), LISTED_DAYS >= 60)"
prec = 6

[[ind]]
name = "MY_GROUP_RANK"
scope = "cross_section"
expr = "CS_RANK(PCT_CHG, LISTED_DAYS >= 60, IF(TOTAL_MV_YI < 100, 1, IF(TOTAL_MV_YI < 500, 2, 3)))"
prec = 6

[[ind]]
name = "MY_RELATIVE"
scope = "cross_section"
expr = "PCT_CHG - MY_MARKET"
prec = 6

[[ind]]
name = "MY_CONCENTRATION"
scope = "market"
expr = "MA(CS_TOPSHARE(AMOUNT, 0.1, LISTED_DAYS >= 60), 5)"
prec = 6

[[ind]]
name = "MY_FOLLOW"
scope = "market"
expr = """
P := (EXP(SUM(LN(MAX(0.000001, 1 + REF(PCT_CHG,1)/100)),5))-1)*100;
Q := CS_RANK(P, LISTED_DAYS >= 60);
MA(CS_MEAN((C/O-1)*100, Q>0.8 AND O>0) - CS_MEAN((C/O-1)*100, Q<=0.2 AND O>0), 5);
"""
prec = 6
```

补算后可以在策略中直接使用：

```text
J < 20 AND MY_MARKET > 0 AND MY_STRENGTH > 0.8
```

阈值仅说明语法；公式有效性需自行回测。

已有10项市场环境指标也已改为[普通配置示例](indicator-market-examples.toml)，不再内置在 Rust 中。追加示例条目时保留自己文件开头的 `version`，不要重复复制。

## 时间、样本与空值

市场／横截面范围统一按 `trade_calendar.csv` 的市场交易日对齐。停牌、缺报价的位置保留空值，不压缩时间、不沿用前日。这里的 `REF(C,5)` 表示5个市场交易日前；原有逐股范围仍按该股报价行工作。需要排除缺报价窗口时可写 `COUNT(ISVALID(C),21)=21`。

可使用原始行情字段、已完成的逐股指标，以及两个附加字段：`SESSION` 是从交易日历起点计数的交易日编号；`LISTED_DAYS` 是按股票上市日计的市场交易日数。上市日或日历本身缺失、错误会影响样本，系统不替用户推断。`TOTAL_MV_YI` 沿用成交额／换手率反推流通市值的原口径。

市场范围最终必须得到共享序列；例如单独的 `CS_RANK(C)` 仍是逐股结果，应选择横截面。局部赋值仅在该指标内有效，跨指标使用发布名称。`LAST` 把区间末值广播到历史，市场／横截面落库禁止使用它，历史引用请用 `REF`。

## 更新、存储与补算

日常下载后统一更新。普通有限窗口按公式依赖估算足够预热，替换新增或待修复日期；包含 `EMA`、`SMA`、`BARSLAST` 的跨股票配置保留完整原始历史重放，以保证初值一致，因此这种配置的日常更新也可能较慢。读取原始输入按64股分批，截面中间结果仍随“股票数×历史长度×同时保留的序列数”增长，不是固定内存上限。

原始库使用通用长表 `indicator_panel_values` 保存名称、股票代码、交易日和数值；市场共享值使用空股票代码，横截面值使用真实股票代码。`indicator_panel_sessions` 记录已完成日期，`indicator_panel_state` 记录配置和待修复范围。用户新增名称无需改表结构、字段注册或图表代码。

配置修改后旧结果不再按新配置读取，需重新补算。历史行情或逐股指标修复时，受影响日期到补算完成前读为空。分批写入中断也保持该范围不可见，重新补算后发布完整结果。实时尚未保存的日期为空；`REF` 在策略中仍遵循该策略所在股票序列的位置。

可通过“指标列补算”完整重建，也可在项目根目录执行：

```bash
cargo run -p lianghua-data --bin indicators -- /数据目录/source
cargo run -p lianghua-data --bin indicators -- /数据目录/source --rebuild
```

该命令只重算市场／横截面阶段；引用的逐股指标应先通过正常下载或界面补算生成。原始库查看器及图表设置会按配置发现用户字段。“指标列删除”也会删除这些跨股票结果。
