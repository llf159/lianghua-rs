# 自定义图表指标规划

## 背景

个股详情页当前已经能绘制主 K、指标、量能和砖型图，但指标字段、面板结构、颜色和部分画法仍然由代码固定定义。

当前关键位置：

- 后端详情数据结构：`src/ui_tools_feat/details.rs`
- 前端详情接口类型：`ui/lianghua_web_feat/src/apis/details.ts`
- 前端 SVG 图表渲染：`ui/lianghua_web_feat/src/pages/desktop/DetailsPage.tsx`
- 表达式解析与求值：`src/expr/*`
- 指标计算缓存：`src/download/ind_calc.rs`
- 数据源配置文件目录：用户选择的 `source_path`

现有表达式引擎已经支持常用指标计算能力，例如 `MA`、`EMA`、`SMA`、`REF`、`HHV`、`LLV`、`CROSS`、`IF`、`BARSLAST` 等。后续不应该重复实现一套独立计算语言，而应该复用这套表达式引擎。

## 目标

实现一套可配置的指标图表系统，让主图和副图的内容从代码写死逐步过渡到用户配置。

目标能力：

- 自定义面板：主图叠加、副图指标、量能面板、MACD 面板等。
- 自定义序列：画哪条数据、表达式是什么、显示名是什么。
- 自定义画法：线、柱、正负柱、面积、区间带、标记、文字。
- 自定义样式：颜色、线宽、透明度、柱宽、虚线等。
- 条件样式：满足不同条件时使用不同颜色或形状。
- 条件标记：买点、卖点、突破、风险、观察点等。
- 与实时行情兼容：实时拼接行也能计算配置中需要的指标。
- 可校验：保存配置前能够解析表达式并返回明确错误。
- 可编辑：后续可以做成图形化模板管理器。

非目标：

- 第一阶段不完整复刻通达信、同花顺、大智慧等股票软件 DSL。
- 第一阶段不支持所有复杂绘图函数，例如任意多边形、复杂文字排版、未来函数类行为。
- 第一阶段不改变现有评分、选股、模拟盘表达式语义。

## 总体方案

采用“两层配置”：

1. 指标计算层：继续使用现有表达式引擎，负责计算数值序列或布尔序列。
2. 图形描述层：新增结构化 TOML 配置，负责描述面板、序列、颜色、画法、标记。

这样做的原因：

- 计算逻辑可以复用现有 `src/expr` 和 `src/download/ind_calc.rs`。
- TOML 结构化配置更容易校验、迁移和做 UI 编辑器。
- 前后端协议清楚，后端负责算数据，前端负责按配置画图。
- 未来仍然可以增加“股票软件语法糖”解析器，将类似 `MA20:MA(C,20),COLORRED;` 转换成内部 TOML 结构。

## 配置文件

建议新增文件：

```text
<source_path>/chart_indicators.toml
```

如果用户数据源目录没有该文件，则使用代码内置默认配置，保持现有详情页行为不变。

也可以提供一个仓库内置模板：

```text
examples/chart_indicators.toml
```

## TOML 草案

```toml
version = 1

[[panel]]
key = "price"
label = "主K"
kind = "candles"
row_weight = 46

[[panel.series]]
key = "MA20"
label = "MA20"
expr = "MA(C, 20)"
kind = "line"
color = "#e13a1f"
line_width = 1.4

[[panel.series]]
key = "MA60"
label = "MA60"
expr = "MA(C, 60)"
kind = "line"
color = "#0057ff"
line_width = 1.2

[[panel.marker]]
key = "cross_ma20"
label = "上穿MA20"
when = "CROSS(C, MA(C, 20))"
y = "L"
position = "below"
shape = "triangle_up"
color = "#d9485f"
text = "B"

[[panel]]
key = "volume"
label = "量能"
row_weight = 16

[[panel.series]]
key = "VOL"
label = "成交量"
expr = "V"
kind = "bar"
color_when = [
  { when = "C >= REF(C, 1)", color = "#d9485f" },
  { when = "C < REF(C, 1)", color = "#178f68" },
]

[[panel.series]]
key = "VOL_MA5"
label = "量均5"
expr = "MA(V, 5)"
kind = "line"
color = "#7dd3fc"

[[panel]]
key = "macd"
label = "MACD"
row_weight = 18

[[panel.series]]
key = "DIF"
label = "DIF"
expr = "EMA(C, 12) - EMA(C, 26)"
kind = "line"
color = "#0057ff"

[[panel.series]]
key = "DEA"
label = "DEA"
expr = "EMA(EMA(C, 12) - EMA(C, 26), 9)"
kind = "line"
color = "#e13a1f"

[[panel.series]]
key = "MACD"
label = "MACD"
expr = "(DIF - DEA) * 2"
kind = "histogram"
base_value = 0
color_when = [
  { when = "MACD >= 0", color = "#d9485f" },
  { when = "MACD < 0", color = "#178f68" },
]
```

注意：上面的 `MACD` 表达式引用了 `DIF` 和 `DEA`。实现时需要保证同一个配置内的 series 按顺序计算，并把已计算序列注入 runtime。

## 配置模型

后端建议新增结构：

```rust
struct ChartIndicatorConfig {
    version: u32,
    panel: Vec<ChartPanelConfig>,
}

struct ChartPanelConfig {
    key: String,
    label: String,
    kind: Option<String>,
    row_weight: Option<u32>,
    series: Vec<ChartSeriesConfig>,
    marker: Vec<ChartMarkerConfig>,
}

struct ChartSeriesConfig {
    key: String,
    label: Option<String>,
    expr: String,
    kind: String,
    color: Option<String>,
    color_when: Vec<ChartColorRule>,
    line_width: Option<f64>,
    opacity: Option<f64>,
    base_value: Option<f64>,
}

struct ChartMarkerConfig {
    key: String,
    label: Option<String>,
    when: String,
    y: Option<String>,
    position: Option<String>,
    shape: Option<String>,
    color: Option<String>,
    text: Option<String>,
}

struct ChartColorRule {
    when: String,
    color: String,
}
```

字段规范：

- `key`：稳定唯一标识，建议只允许 `[A-Za-z_][A-Za-z0-9_]*`。
- `label`：展示名，可为空，默认使用 `key`。
- `expr`：数值表达式，结果必须能转成数值序列。
- `when`：布尔表达式，结果必须能转成布尔序列。
- `kind`：第一阶段支持 `line`、`bar`、`histogram`、`area`、`band`、`candles`、`brick`、`marker`。
- `color`：建议只支持十六进制颜色，避免任意 CSS 注入。
- `row_weight`：面板高度权重。

## 后端数据协议

当前 `DetailKlineRow` 是固定字段结构。为了支持任意指标，建议逐步过渡到动态字段。

第一阶段可以保留兼容：

```rust
pub struct DetailKlineRow {
    pub trade_date: String,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub vol: Option<f64>,
    pub amount: Option<f64>,
    pub tor: Option<f64>,
    // 兼容旧字段
    pub brick: Option<f64>,
    pub j: Option<f64>,
    pub duokong_short: Option<f64>,
    pub duokong_long: Option<f64>,
    pub bupiao_short: Option<f64>,
    pub bupiao_long: Option<f64>,
    pub vol_sigma: Option<f64>,
    // 新增动态指标
    #[serde(flatten)]
    pub indicators: HashMap<String, serde_json::Value>,
}
```

后续可以完全替换为：

```rust
pub struct DetailChartRow {
    pub trade_date: String,
    #[serde(flatten)]
    pub values: HashMap<String, serde_json::Value>,
}
```

前端 `DetailKlineRow` 已经有 `[key: string]: DetailPrimitive`，短期可以兼容动态字段。

面板协议建议从当前：

```ts
type DetailKlinePanel = {
  key: string
  label: string
  kind?: "candles" | "line" | "bar" | "brick"
  series_keys?: string[]
  row_weight?: number
}
```

升级为：

```ts
type DetailChartPanel = {
  key: string
  label: string
  kind?: "candles" | "custom" | "brick"
  row_weight?: number
  series?: DetailChartSeries[]
  markers?: DetailChartMarker[]
  series_keys?: string[]
}

type DetailChartSeries = {
  key: string
  label?: string
  kind: "line" | "bar" | "histogram" | "area" | "band"
  color?: string
  color_when?: DetailChartColorRule[]
  line_width?: number
  opacity?: number
  base_value?: number
}

type DetailChartMarker = {
  key: string
  label?: string
  when_key: string
  y_key?: string
  position?: "above" | "below" | "value"
  shape?: "dot" | "triangle_up" | "triangle_down" | "flag"
  color?: string
  text?: string
}

type DetailChartColorRule = {
  when_key: string
  color: string
}
```

后端不建议把原始表达式全部下发给前端执行。后端应将 `expr`、`when` 计算完成，并下发结果 key：

- series 的数值结果写入 `row[series.key]`
- marker 的条件结果写入 `row[marker.when_key]`
- color rule 的条件结果写入 `row[colorRule.when_key]`

这样前端只负责读取值并渲染，不需要实现表达式引擎。

## 后端实现步骤

### 1. 配置解析

新增模块建议：

```text
src/ui_tools_feat/chart_indicator_config.rs
```

职责：

- 找到 `<source_path>/chart_indicators.toml`
- 没有文件时返回默认配置
- 使用 `toml::from_str` 解析
- 标准化 `key`、`kind`、颜色等字段
- 校验重复 key、空表达式、非法颜色、非法 kind
- 返回结构化 `ChartIndicatorConfig`

### 2. 表达式编译

新增编译结构：

```rust
struct CompiledChartSeries {
    key: String,
    expr: Stmts,
    render: ChartSeriesRenderConfig,
}

struct CompiledChartMarker {
    key: String,
    when_key: String,
    when_expr: Stmts,
    y_key: Option<String>,
    render: ChartMarkerRenderConfig,
}
```

编译阶段使用现有：

- `lex_all`
- `Parser::parse_main`

编译错误要带上 panel key、series key、marker key，方便 UI 展示。

### 3. 构建 Runtime 数据

从 `stock_data` 查询基础列，构造成 `RowData`。

基础变量保持与现有表达式体系一致：

- `O`
- `H`
- `L`
- `C`
- `V`
- `AMOUNT`
- `PRE_CLOSE`
- `CHANGE`
- `PCT_CHG`
- `TURNOVER_RATE`

同时为了兼容旧字段，可以注入：

- `OPEN`
- `HIGH`
- `LOW`
- `CLOSE`
- `VOL`
- `TOR`

### 4. 计算 series

按配置顺序计算 `panel.series`：

1. 调用 `Runtime::eval_program`
2. 转成数值序列
3. 写入输出 rows
4. 将结果注入 runtime，允许后续表达式引用前序 series

例如 `MACD` 引用 `DIF`、`DEA`。

### 5. 计算 marker 和 color_when

每个 `when` 表达式计算成布尔序列，生成内部 key，例如：

```text
__marker_cross_ma20
__color_volume_0
```

这些 key 可以下发给前端，但不在 tooltip 默认展示。

### 6. 实时行情兼容

当前详情页实时逻辑会对最后一行实时数据补算部分指标。新方案需要改成：

- 实时拼接后重新构建 `RowData`
- 使用同一套 chart config 计算所需 series / marker / color_when
- 只更新最后一行或直接返回完整窗口结果

短期可以优先返回完整窗口结果，逻辑更简单。

### 7. Tauri 命令

新增或扩展命令：

- `get_stock_detail_page`：返回配置化图表结果
- `get_stock_detail_realtime`：返回配置化实时图表结果
- `validate_chart_indicator_config`：校验 TOML
- `read_chart_indicator_config`：读取当前配置
- `save_chart_indicator_config`：保存当前配置

命令注册位置：

```text
ui/lianghua_web_feat/src-tauri/src/lib.rs
```

## 前端实现步骤

### 1. 类型升级

更新：

```text
ui/lianghua_web_feat/src/apis/details.ts
```

新增 chart series、marker、color rule 类型。保留旧字段兼容。

### 2. 渲染器改造

当前 `renderChartPanel` 按 `panel.kind` 整体分支：

- `candles`
- `line`
- `bar`
- `brick`

改造方向：

- `candles` 和 `brick` 保持特殊面板。
- 普通面板改成按 `panel.series` 逐条绘制。
- 每条 series 自己决定画法。

建议拆函数：

```ts
renderLineSeries(...)
renderBarSeries(...)
renderHistogramSeries(...)
renderAreaSeries(...)
renderBandSeries(...)
renderMarkers(...)
resolveSeriesColor(...)
resolvePointColor(...)
```

### 3. domain 计算

当前 domain 按面板 kind 收集所有 series key。新方案需要：

- 收集所有可见 series 的数值。
- `histogram` / `bar` 可以声明 `base_value`，默认包含 0。
- `band` 要包含上下边界。
- marker 默认不参与 domain，除非 `y_key` 超出当前 domain 且配置要求包含。

### 4. tooltip

tooltip 应从 `panel.series` 生成，而不是从 `series_keys` 和硬编码 label 生成。

规则：

- 默认展示 `series.label ?? series.key`
- 隐藏内部 key，例如 `__marker_*`、`__color_*`
- 主 K 面板仍保留 OHLC、涨幅、换手、排名等特殊信息

### 5. 图例

图例颜色从 series 配置读取：

- 有固定 `color` 时使用固定色。
- 无固定色时使用默认调色板。
- 条件变色 series 图例可以显示默认色，或显示多色小块。

### 6. 配置管理 UI

后续可新增：

```text
ui/lianghua_web_feat/src/pages/desktop/components/ChartIndicatorTemplateManagerModal.tsx
```

第一版可以是文本编辑器：

- 左侧模板列表
- 右侧 TOML 文本
- 校验按钮
- 保存按钮
- 恢复默认按钮

第二版再做结构化表单：

- 面板增删改
- 序列增删改
- 颜色选择器
- 表达式输入
- 标记配置

## 默认配置迁移

现有默认面板应迁移成内置默认 TOML：

- 主 K：K 线 + `duokong_short` / `duokong_long` 叠加线
- 指标：`j` / `bupiao_long` / `bupiao_short`
- 量能：`vol` 柱 + `VOL_SIGMA` 线
- 砖型图：`brick`

为了降低风险，第一阶段可以保留旧 `default_kline_panels()`，当没有 `chart_indicators.toml` 时仍走旧逻辑；新配置稳定后再统一迁移。

## 与现有 ind.toml 的关系

`ind.toml` 当前用于下载、增量计算、落库指标。`chart_indicators.toml` 用于图表展示。

两者关系：

- `ind.toml`：偏“数据加工”，计算结果可能写入 `stock_data`。
- `chart_indicators.toml`：偏“显示层”，计算结果可以临时生成，不一定落库。

后续可以支持 chart series 直接引用已落库指标列，也可以在 chart config 中写临时表达式。

建议规则：

- 常用、昂贵、多个功能共用的指标放进 `ind.toml`。
- 只为了图表显示的指标放进 `chart_indicators.toml`。

## 表达式能力缺口

第一阶段可以直接复用现有表达式函数。后续可能需要补充：

- `STD(x, n)`：标准差
- `SUM(x, n)`：滚动求和
- `AVEDEV(x, n)`：平均绝对偏差
- `VALUEWHEN(cond, x)`：最近一次条件成立时的值
- `FILTER(cond, n)`：条件成立后 n 根内过滤重复信号
- `EXIST(cond, n)`：最近 n 根是否存在条件
- `EVERY(cond, n)`：最近 n 根是否全部满足
- `LONGCROSS(a, b, n)`：持续低于后上穿

这些应该加在 `src/expr/eval.rs`，并同步更新 `StrategySyntaxGuideModal.tsx`。

## 股票软件 DSL 兼容路线

不建议第一阶段直接实现完整 DSL。但可以预留转换路线。

未来可以支持这种输入：

```text
MA20:MA(C,20),COLORRED;
MA60:MA(C,60),COLORBLUE;
DIF:EMA(C,12)-EMA(C,26);
DEA:EMA(DIF,9);
MACD:(DIF-DEA)*2,COLORSTICK;
DRAWICON(CROSS(C,MA20),L,1);
DRAWTEXT(CROSS(MA20,MA60),L,'金叉');
```

转换为内部结构：

- `NAME:EXPR,COLORRED` -> `series`
- `COLORSTICK` -> `kind = "histogram"`
- `DRAWICON(cond, y, icon)` -> `marker`
- `DRAWTEXT(cond, y, text)` -> `marker` with text

内部渲染协议仍然使用结构化 TOML，不让股票软件 DSL 直接驱动前端。

## 分阶段计划

### Phase 0：设计冻结

- 确认 `chart_indicators.toml` 文件名。
- 确认第一阶段支持的 `kind` 列表。
- 确认是否保留旧字段兼容。
- 写一份 `examples/chart_indicators.toml`。

验收标准：

- 文档和样例能覆盖现有详情页默认图。
- 新配置能表达 MA、MACD、量能、买卖标记。

### Phase 1：后端配置解析和校验

- 新增配置结构和解析模块。
- 增加配置校验函数。
- 增加表达式编译校验。
- 增加 Tauri 校验命令。

验收标准：

- TOML 格式错误能返回明确错误。
- 表达式错误能指出 panel / series / marker。
- 重复 key、非法 kind、非法颜色能被拦截。

### Phase 2：后端动态计算

- 在详情页查询中构造 Runtime。
- 计算配置中的 series。
- 计算 marker / color_when。
- 将动态字段合并进 `items`。
- 返回扩展后的 panels 协议。

验收标准：

- 能用配置画 MA20 / MA60。
- 能用配置画 MACD。
- 能用配置产生买点 marker 条件。
- 无配置文件时旧图表不变。

### Phase 3：前端渲染改造

- 升级 `details.ts` 类型。
- 改造 `renderChartPanel`，支持按 series kind 绘制。
- 增加 marker 渲染。
- 改造 tooltip 和图例。
- 保留主 K、砖型图旧特殊行为。

验收标准：

- 旧默认图表仍能正常显示。
- 配置中的 line、bar、histogram、marker 能显示。
- tooltip 显示配置 label。
- 图表缩放、拖拽、十字光标不回退。

### Phase 4：实时兼容

- 实时 K 线拼接后走同一套配置计算。
- 实时行颜色保持现有涨跌逻辑。
- marker 和 color_when 在实时行上生效。

验收标准：

- 实时刷新后 MA / MACD 最后一根会更新。
- 条件标记能在实时行出现或消失。
- 无实时数据时历史图不受影响。

### Phase 5：配置管理 UI

- 增加图表指标配置管理入口。
- 第一版使用 TOML 文本编辑器。
- 支持读取、校验、保存、恢复默认。
- 保存后详情页刷新应用新配置。

验收标准：

- 用户可以在 UI 中编辑配置。
- 配置错误不会覆盖原有效配置。
- 保存成功后图表按新配置显示。

### Phase 6：语法糖和高级画法

- 支持 `band`、`area`、`stick`。
- 支持更多 marker shape。
- 支持股票软件风格 DSL 转换器。
- 补充更多表达式函数。

验收标准：

- 能表达常见通达信风格指标。
- 能将一部分股票软件公式转换为内部配置。

## 测试计划

Rust 单元测试：

- 配置 TOML 解析成功。
- 配置 TOML 格式错误。
- 重复 key 错误。
- 非法颜色错误。
- 表达式编译错误。
- series 之间顺序引用。
- marker 布尔序列计算。

Rust 集成式测试：

- 构造临时 DuckDB K 线数据。
- 使用 chart config 计算 MA / MACD。
- 校验返回 items 中动态字段。

前端验证：

- `npm run lint`
- `npm run build`
- 手工检查详情页图表：
  - 默认配置
  - 自定义 MA
  - MACD
  - 条件变色柱
  - marker
  - 实时刷新

Rust 验证：

```bash
cargo test
cargo check --manifest-path ui/lianghua_web_feat/src-tauri/Cargo.toml
```

## 风险与注意事项

- 不要让前端执行表达式。表达式执行应集中在 Rust，避免前后端语义不一致。
- 不要第一阶段大范围重写详情页交互，图表拖拽、缩放、区间统计、筹码显示都应保持。
- 不要把所有指标都写进 `DetailKlineRow` 固定字段，否则每加一个指标都要改 Rust struct。
- 配置保存前必须校验，避免用户保存坏配置导致详情页打不开。
- 条件表达式可能很多，要注意缓存编译结果，避免每次刷新重复 parse。
- 大窗口、多指标时计算量会上升，需要控制默认窗口和表达式复杂度。
- 颜色和文本字段要限制格式，避免任意 CSS 或不受控内容进入渲染层。

## 建议优先级

优先做：

1. `line`
2. `bar`
3. `histogram`
4. `marker`
5. `color_when`

暂缓做：

1. 股票软件 DSL 完整兼容
2. 任意文字排版
3. 任意形状绘制
4. 复杂未来函数或回填函数
5. 结构化可视化编辑器

## 最小可交付版本

最小版本只需要做到：

- 支持 `chart_indicators.toml`
- 支持 `panel.series`
- 支持 `line` 和 `bar`
- 后端计算表达式并下发动态字段
- 前端按配置画线和柱
- 无配置时旧逻辑不变

这个版本已经能解决“指标画法从代码写死变成可配置”的核心问题。

