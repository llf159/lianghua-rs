# 自定义筹码计算算法后端计划

## 背景

当前项目已有 `src/data/cyq.rs` 的筹码计算逻辑，使用单一筹码分布和统一换手衰减模型。新算法先作为独立后端能力设计，承载位置为 `src/data/cyq_chen.rs`，不直接改动现有 `cyq.rs` 的行为。

核心目标是把筹码归属拆成两类：

- 主力筹码
- 散户筹码

通过 K 线行为、普通指标列、筹码成本相对当日 OHLC 的盈利百分比，结合可配置策略，判断主力/散户筹码的增加和衰减。所有筹码以百分比记录，主力筹码和散户筹码在所有价位上的总和保持为 `100`。

## 第一阶段范围

第一阶段只做后端核心和单股测试：

- 实现策略文件解析。
- 实现单股 `RowData` 输入下的筹码快照计算。
- 添加 Rust 单元测试验证算法关键行为。

第一阶段不做：

- 不写入 `cyq.db`。
- 不新增数据库表。
- 不接 `ui_tools_feat`。
- 不接 Tauri command。
- 不改前端页面。

## 策略文件

新增运行时策略文件：

```text
source_path/chip_change_rule.toml
```

该文件跟随用户选择的数据目录 `source_path`，不要依赖仓库根目录配置。

建议结构：

```toml
version = 1

[[strategy]]
name = "主力低位承接"
holder = "main"
direction = "buy"
when = "RATEL < -0.08 AND C > O"
bias = 1.5

[[strategy]]
name = "散户追高买入"
holder = "retail"
direction = "buy"
when = "RATEC > 0.05 AND C >= H * 0.98"
bias = 1.2

[[strategy]]
name = "散户获利卖出"
holder = "retail"
direction = "sell"
when = "RATEH > 0.12"
bias = 1.0

[[strategy]]
name = "主力高位派发"
holder = "main"
direction = "sell"
when = "RATEC > 0.2 AND C < O"
bias = -0.6
```

字段说明：

- `version`: 策略文件版本，第一阶段只接受 `1`。
- `name`: 策略名称，不能为空。
- `holder`: 筹码持有人，取值为 `"main"` 或 `"retail"`。
- `direction`: 买卖方向，取值为 `"buy"` 或 `"sell"`。
- `when`: 表达式，复用项目现有表达式语法。
- `bias`: 倾向系数。买入策略必须是有限正数；卖出策略允许有限负数，用于抵消或降低该价格分桶的卖出倾向。

加载策略文件时必须完成结构校验和表达式解析。`version != 1`、字段非法、表达式解析失败都返回明确错误，不进入计算阶段。

## 表达式运行时注入

复用项目当前表达式引擎。输入数据复用 `RowData`。

策略表达式的求值结果按排名得分聚合的口径处理：每条策略表达式计算得到一个布尔序列，再按交易日 index 取当天是否触发。系数聚合也按“策略触发 -> 当日累加 bias”的方式处理，和排名得分里多条规则逐日聚合的逻辑保持一致；只是本算法不依赖 rank 等需要排名结果库的数据。

策略表达式在加载时应先解析成 AST。计算时可以复用完整 `RowData` 序列运行时，并为当前价格分桶注入同长度的 `RATE*` / `MAIN_CHIP_RATIO` 序列，表达式返回值必须能转换成与 `trade_dates` 对齐的布尔序列。正式计算阶段可能动态扩出新桶，新桶创建后再按该桶边界/中点动态构造或缓存对应的注入序列。

分桶不再使用固定桶数。计算时先根据首日前推窗口内的最高价/最低价确定价格区间，再按可配置的价格百分比步长划分动态分桶。针对每个筹码成本价位分桶，在计算策略时额外注入该桶中点价相对当前 OHLC 的盈利百分比：

```text
cost_price = (bucket.price_low + bucket.price_high) / 2
RATEO = (O - cost_price) / cost_price
RATEH = (H - cost_price) / cost_price
RATEL = (L - cost_price) / cost_price
RATEC = (C - cost_price) / cost_price
MAIN_CHIP_RATIO[i] = prev_day_main_chip_at_bucket / prev_day_total_chip_at_bucket
```

这些变量用于策略表达式区分不同成本区间的筹码行为。
`MAIN_CHIP_RATIO` 表示昨日计算结束后，当前成本分桶内主力筹码占该分桶总筹码的比例。表达式在第 `i` 个交易日读取的 `MAIN_CHIP_RATIO[i]` 来自第 `i - 1` 个交易日收盘后的筹码状态，整体向前推一格，避免当天策略读取当天尚未完成的计算结果。如果当前分桶没有昨日状态、没有筹码或总筹码小于误差阈值，则注入 `0`。

## 计算逻辑

每日换手筹码分为四类：

- 主力买入
- 主力卖出
- 散户买入
- 散户卖出

每日计算顺序：

- 先根据当日 `H/L` 扩展价格桶，使当前桶区间覆盖当日成交价格区间。
- 再计算卖出衰减。卖出只发生在计算开始时已有筹码的桶上；当日新扩出来的空桶初始筹码为 `0`，不会参与卖出。
- 最后计算买入增加。买入可以落入当日新扩出来的桶。
- 每日结束后做数值安全处理和归一化。

买入逻辑：

- 买入策略也按价格分桶求值。对当前目标买入分桶，使用该桶中点价计算 `RATE*` 并执行 `direction = "buy"` 策略。
- 对当日、当前买入分桶触发的 `direction = "buy"` 策略按 `holder` 汇总 `bias`。
- 主力和散户买入占比按各自买入倾向系数占全部买入倾向系数的比例分配。
- 例如散户买入倾向为 `1.2`、主力买入倾向为 `1.5`，则散户买入占 `1.2 / 2.7`，主力买入占 `1.5 / 2.7`。
- 买入侧兜底规则：如果主力/散户只有一方存在有效买入倾向，则当日该分桶买入量全部归属有倾向的一方；如果两方都没有有效买入倾向，则该分桶买入量全部归属散户。
- 当日新增筹码量由当日换手率决定，例如当日换手率 `5%`，则新增买入筹码总量为 `5`。买入侧独立计算“这 `5%` 是谁买的”，再按成交价格分布模型分摊到对应成本价位分桶，并计入对应持有人。
- 成交价格分布模型复用现有 `cyq.rs` 的 OHLC 三角分布口径：以 `(O + H + L + C) / 4` 作为成交重心，价格桶越靠近重心权重越高，向 `L/H` 两侧递减；`H == L` 时全部成交筹码落入覆盖该价格的桶。

起始预热逻辑：

- 新增可配置参数 `warmup_days`，表示首个输出日期向前取多少个交易日做预热，默认建议为 `120`。
- 计算某段输出区间时，必须从首个输出日期向前取满 `warmup_days` 根历史 K 线作为预热窗口。
- 如果停牌缺数据或数据源不足，导致首个输出日期前无法取满 `warmup_days` 根有效 K 线，则跳过该股票/该输出段，不做降级初始化，也不输出快照。
- 如果 `output_start_date` 之前无法取满 `warmup_days` 根有效历史 K 线，按预热不足处理并返回空快照。
- `output_start_date` 不在 `row_data.trade_dates` 中时返回明确错误。
- `output_start_date` 及之后存在 OHLC 或换手率异常时返回明确错误。
- 用预热窗口内的最高价和最低价确定初始价格区间。
- 分桶不再固定为 `factor` 个桶，而是按价格百分比步长动态划分：新增参数 `bucket_pct`，表示每个价格桶相对前一个桶的价格跨度比例，例如 `1` 表示约每 `1%` 一个桶。
- 正式计算阶段如果后续价格突破当前分桶区间，则按同一个 `bucket_pct` 动态新增空桶。扩桶时使用当前边界价为基数乘百分比跨度：向上扩展时，新桶上边界为当前最高边界 `* (1 + bucket_pct / 100)`；向下扩展时，新桶下边界为当前最低边界 `/ (1 + bucket_pct / 100)`。每次只新增一个跨度，重复直到覆盖当日最高价/最低价。新增桶初始主力/散户筹码均为 `0`，原有桶内筹码不做重采样。
- 初始化时在预热窗口确定的全部价格桶内平均分散总筹码 `100`，作为最原始筹码状态。
- 初始化平均分散时，主力/散户默认各占 `50`；随后用预热窗口内每一日的正常买卖策略和换手率连续滚动计算一遍，得到首个输出日期前的初始筹码分布。
- 预热滚动结束后，再从首个输出日期开始产出正式快照。
- 预热阶段只用于形成初始筹码分布，不输出快照。

卖出逻辑：

- 卖出策略按成本价位分桶分别计算，因为不同成本的筹码对应不同盈利/亏损状态。
- 对每个成本分桶注入 `RATEO`、`RATEH`、`RATEL`、`RATEC` 后执行 `direction = "sell"` 策略。
- 每个成本分桶都独立计算自己的总倾向系数，不能用全局统一倾向系数套到所有价格。
- 主力和散户分别在当前成本分桶内汇总各自触发策略的 `bias`，形成该分桶、该持有人的卖出倾向。
- 卖出策略的负数 `bias` 按代数和参与当前分桶总倾向计算；若某分桶某持有人的总卖出倾向小于等于 `0`，该分桶该持有人不发生卖出衰减。
- 同一持有人内部不再拆分更多子类，只按该价格分桶自己的总倾向系数参与分配。
- 卖出占比计算以所有持有人、所有成本分桶的汇总想卖量为分母。每个分桶先计算 `有效卖出倾向 = max(0, 触发卖出策略 bias 的代数和)`，再用 `分桶想卖量 = 当前分桶该持有人的可卖筹码 * 有效卖出倾向`；当前分桶衰减占比为 `分桶想卖量 / 全部分桶想卖量之和`。
- 卖出侧兜底规则：如果主力/散户只有一方存在有效卖出倾向，则当日卖出量全部从有倾向的一方筹码中按分桶想卖量分摊；如果两方都没有有效卖出倾向，则当日卖出量全部从散户筹码中按散户各分桶现有筹码占比分摊。
- 如果按规则选中的卖出方筹码不足，则不足部分从另一方筹码中补充卖出；如果两方可卖筹码仍不足以覆盖当日卖出量，则剩余可卖筹码在全部非空桶内按现有筹码占比分摊衰减到可卖上限。所有补充和占比分摊衰减都要做浮点误差保护，不能让任何桶出现负筹码。
- 卖出衰减量由当日换手率决定，例如当日换手率 `5%`，则卖出筹码总量为 `5`。卖出侧独立计算“这 `5%` 是谁卖的”，并按上述占比分摊到主力/散户各成本分桶。
- 分桶筹码不允许衰减到负数。

每日结束后需要归一化：

- 主力筹码 + 散户筹码合计保持 `100`。
- 所有分桶合计保持 `100`。
- 没有筹码的价格桶，占比必须为 `0`，不能因为分母或浮点误差产生非零占比。
- 每次卖出衰减、买入增加、归一化之后都要做数值安全处理：小于误差阈值的负数夹到 `0`，微小绝对值归零，任何分桶的主力/散户/总筹码都不能为负数。
- 如果某一步汇总分母为 `0`，对应占比直接按 `0` 处理，不允许产生 `NaN` 或 `Infinity`。
- 买入/卖出倾向分母为 `0` 时，不走普通占比归零逻辑，必须走上文定义的兜底归属规则。
- 非有限数值应返回明确错误，不静默吞掉。

输入数据校验：

- `RowData.trade_dates` 必须非空、升序且不可重复。
- `O/H/L/C` 和 `TURNOVER_RATE/TOR` 必须存在，长度必须与 `trade_dates` 一致。
- `O/H/L/C` 必须是有限正数，且 `H >= L`。
- 换手率必须是有限数值，且范围为 `[0, 100]`；非有限、负数或超过 `100` 都返回明确错误。

## 后端类型和接口建议

新增配置类型：

```rust
pub struct ChipChangeConfig {
    pub version: u32,
    pub strategy: Vec<ChipChangeStrategy>,
}

pub struct ChipChangeStrategy {
    pub name: String,
    pub holder: ChipHolder,
    pub direction: ChipDirection,
    pub when: String,
    pub bias: f64,
}

pub enum ChipHolder {
    Main,
    Retail,
}

pub enum ChipDirection {
    Buy,
    Sell,
}
```

新增计算配置和输出类型：

```rust
pub struct ChenChipConfig {
    pub warmup_days: usize,
    pub bucket_pct: f64,
}

pub struct ChenChipBin {
    pub index: usize,
    pub price: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub main_chip: f64,
    pub retail_chip: f64,
    pub total_chip: f64,
}

pub struct ChenChipSnapshot {
    pub trade_date: Option<String>,
    pub close: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub main_total: f64,
    pub retail_total: f64,
    pub total_chips: f64,
    pub bins: Vec<ChenChipBin>,
}
```

`ChenChipBin.price` 为该桶中点价，即 `(price_low + price_high) / 2`。

新增核心函数：

```rust
pub fn load_chip_change_config(source_dir: &str) -> Result<ChipChangeConfig, String>;

pub fn compute_chen_chip_snapshots_from_row_data(
    row_data: &RowData,
    output_start_date: &str,
    config: ChenChipConfig,
    strategies: &ChipChangeConfig,
) -> Result<Vec<ChenChipSnapshot>, String>;
```

`output_start_date` 是首个正式输出交易日。`row_data` 必须包含该日期之前至少 `warmup_days` 根有效 K 线；如果无法满足预热要求，则返回空快照表示该股票/该输出段被跳过。`output_start_date` 不在 `row_data.trade_dates` 中时返回明确错误。

## 测试计划

单元测试放在 `src/data/cyq_chen.rs`。

需要覆盖：

- 正常解析 `chip_change_rule.toml` 风格文本。
- `version != 1` 报错。
- 空 `name` 报错。
- 非法 `holder` 报错。
- 非法 `direction` 报错。
- 空 `when` 报错。
- `when` 表达式解析失败报错。
- 买入策略非正数或非有限 `bias` 报错。
- 卖出策略允许负数 `bias`，但非有限 `bias` 报错。
- `warmup_days` 为 `0` 时报错。
- `bucket_pct` 非有限或小于等于 `0` 时报错。
- 缺少 `O/H/L/C` 返回明确错误。
- 缺少 `TURNOVER_RATE/TOR` 返回明确错误。
- `trade_dates` 非升序或重复时报错。
- `O/H/L/C` 非有限、非正数或 `H < L` 时报错。
- 换手率非有限、负数或超过 `100` 时报错。
- 起始计算会从首个输出日期前推 `warmup_days` 读取预热数据，并使用预热窗口高低点和 `bucket_pct` 动态建桶。
- `output_start_date` 不在 `trade_dates` 中时报错。
- `output_start_date` 之前无法取满 `warmup_days` 根有效历史 K 线时返回空快照，不做降级预热。
- `output_start_date` 及之后存在 OHLC 或换手率异常时返回明确错误。
- 预热初始状态会在所有价格桶内平均分散总筹码 `100`，主力/散户默认各 `50`。
- 预热窗口会完整跑一遍正常买卖策略，首个输出快照使用预热后的筹码分布。
- 策略表达式能读取昨日状态的 `MAIN_CHIP_RATIO`，序列整体向前推一格，空筹码桶或无昨日状态时注入值为 `0`。
- 动态新增桶能在创建后构造或缓存自己的 `RATE*` / `MAIN_CHIP_RATIO` 注入序列。
- 策略表达式按布尔序列求值，并能按交易日 index 正确聚合 `bias`。
- `RATEO/RATEH/RATEL/RATEC` 使用当前价格桶中点价计算。
- 多日数据下，卖出策略能按不同成本价位的 `RATE*` 条件触发差异化衰减，并且每个价格分桶独立汇总自己的总倾向系数。
- 后续 K 线高低价突破初始分桶区间时，会按 `bucket_pct` 在对应方向新增空桶，并保持原筹码不重采样。
- 扩桶使用当前边界价乘百分比跨度，向上乘以 `1 + bucket_pct / 100`，向下除以 `1 + bucket_pct / 100`。
- 新增买入筹码按现有 `cyq.rs` 的 OHLC 三角分布模型落入价格桶，`H == L` 时落入覆盖该价格的桶。
- 快照不输出固定桶宽 `accuracy`，每个桶通过 `price_low`/`price_high` 表达自己的价格边界。
- 当日新增桶初始筹码为 `0`，先卖后买，因此新增桶只参与当日买入，不参与当日卖出。
- 当日 `5%` 换手率会独立形成 `5` 的卖出筹码量和 `5` 的买入筹码量，买卖两侧分别按各自倾向分配。
- 买入/卖出任一侧只有一方有有效倾向时全部归属该方，两方都没有有效倾向时全部归属散户方。
- 卖出方筹码不足时会从另一方补充，两方都不足时按全部非空桶现有筹码占比衰减到可卖上限，且不会出现负筹码。
- `ChenChipBin.price` 输出为 `(price_low + price_high) / 2`。
- 卖出负数 `bias` 能抵消当前价格分桶的卖出倾向，分桶总倾向小于等于 `0` 时不衰减。
- 空筹码桶的占比为 `0`，浮点误差不会导致主力/散户/总筹码出现负数。
- 换手率为 `0` 时，不新增也不衰减。
- 每日计算结果中，主力筹码、散户筹码、总筹码合计保持 `100`。

验证命令：

```bash
cargo test cyq_chen
cargo test
```

## 当前决策

- 策略文件名确定为 `chip_change_rule.toml`。
- 策略文件位置确定为运行时 `source_path` 下。
- 第一阶段结果不落库。
- 第一阶段只实现核心计算和单股测试。
- 第一阶段需要在 `src/data/mod.rs` 添加 `pub mod cyq_chen;`，保证 `cargo test cyq_chen` 能发现模块测试。
- 后续如果要接全量计算、落库、Tauri、前端，再在本文件追加设计。
