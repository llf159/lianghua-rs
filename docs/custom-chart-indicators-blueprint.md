# 自定义图表指标设计蓝图

## 目标

这一版蓝图不追求一步到位的万能绘图库，先解决两个问题：

1. 用 `chart_indicators.toml` 接管当前详情页已经存在的 4 个面板。
2. 在不继续增加前端硬编码分支的前提下，为后续扩展 MACD、BOLL、区间带、信号标记等图法留出协议空间。

第一阶段必须完整覆盖当前 4 个面板：

- 主 K：K 线 + `duokong_short` + `duokong_long`
- 指标：`j` + `bupiao_long` + `bupiao_short`
- 量能：`vol` + `VOL_SIGMA`
- 砖型图：`brick`

后续扩展图法时，仍然遵守同一条原则：

- 表达式只在 Rust 执行
- 前端只消费结构化渲染协议和结果数据
- 主图特殊交互能力也走协议，不再靠 `panel.key == "price"` 之类的写死判断

## 总体结构

图表指标系统拆成两层：

1. 计算层：复用现有 `src/expr/*` 表达式引擎计算数值序列和布尔序列。
2. 描述层：使用结构化 TOML 描述面板、序列、标记和样式。

后端负责读取配置、校验配置、编译表达式、执行计算，并把结果连同图表描述协议一起返回。渲染端不执行表达式，只消费后端结果。

## 配置文件

运行时配置文件：

```text
<source_path>/chart_indicators.toml
```

仓库内可提供默认模板：

```text
examples/chart_indicators.toml
```

当运行目录下不存在 `chart_indicators.toml` 时，后端返回内置默认配置。

## 第一阶段能力边界

第一阶段只要求支持以下图法组合：

- `candles` 面板：K 线主体 + 同轴叠加 `line`
- `line` 面板：多条 `line`
- `bar` 面板：主柱体 + 同轴叠加 `line`
- `brick` 面板：单序列砖型图
- `marker`：基于布尔条件的买卖点/提示标记
- `color_when`：按条件动态着色
- `IF(...)` / `CROSS(...)` / `REF(...)` / `MA(...)` / `EMA(...)` 等现有表达式函数

第一阶段暂不做：

- 双 Y 轴
- 面板内多坐标系
- 堆叠柱
- 图层透明混合规则的复杂 DSL
- 前端执行表达式

## TOML 结构

推荐 TOML 结构如下：

```toml
version = 1

[[panel]]
key = "price"
label = "主K"
role = "main"
kind = "candles"
row_weight = 46
features = ["ohlc_tooltip", "interval_select", "rank_marker", "cyq_overlay"]

[[panel.series]]
key = "duokong_short"
label = "多空短"
expr = "DUOKONG_SHORT"
kind = "line"
draw_order = 10
color = "#e13a1f"
line_width = 1.4

[[panel.series]]
key = "duokong_long"
label = "多空长"
expr = "DUOKONG_LONG"
kind = "line"
draw_order = 11
color = "#0057ff"
line_width = 1.4

[[panel]]
key = "indicator"
label = "指标"
role = "sub"
kind = "line"
row_weight = 18

[[panel.series]]
key = "j"
label = "J"
expr = "J"
kind = "line"
draw_order = 10
color = "#0057ff"

[[panel.series]]
key = "bupiao_long"
label = "布票长"
expr = "BUPIAO_LONG"
kind = "line"
draw_order = 11
color = "#e13a1f"

[[panel.series]]
key = "bupiao_short"
label = "布票短"
expr = "BUPIAO_SHORT"
kind = "line"
draw_order = 12
color = "#00843d"

[[panel]]
key = "volume"
label = "量能"
role = "sub"
kind = "bar"
row_weight = 18

[[panel.series]]
key = "vol"
label = "成交量"
expr = "VOL"
kind = "bar"
draw_order = 10
base_value = 0
color_when = [
  { when = "C > REF(C, 1)", color = "#d9485f" },
  { when = "C < REF(C, 1)", color = "#178f68" },
  { when = "C = REF(C, 1)", color = "#536273" },
]

[[panel.series]]
key = "VOL_SIGMA"
label = "量波动"
expr = "VOL_SIGMA"
kind = "line"
draw_order = 20
color = "#7dd3fc"

[[panel]]
key = "brick"
label = "砖型图"
role = "sub"
kind = "brick"
row_weight = 18

[[panel.series]]
key = "brick"
label = "砖型"
expr = "BRICK"
kind = "brick"
draw_order = 10
color_when = [
  { when = "BRICK > REF(BRICK, 1)", color = "#d9485f" },
  { when = "BRICK < REF(BRICK, 1)", color = "#178f68" },
]

[[panel.marker]]
key = "cross_duokong_short"
label = "上穿多空短"
when = "CROSS(C, duokong_short)"
y = "L"
position = "below"
shape = "triangle_up"
color = "#d9485f"
text = "B"
```

说明：

- `panel.role` 显式区分主图和副图，不再依赖 `key` 猜测。
- `panel.kind` 表示面板主绘制模式。
- `panel.series.kind` 表示序列图元类型，可与 `panel.kind` 组合，但必须满足兼容约束。
- `draw_order` 控制同一面板内的绘制顺序，数字越小越先画。
- 第一阶段只允许同一面板共用一个 Y 轴域。
- 同一 `panel.series` 按声明顺序计算；后续表达式可引用前面已经产出的序列。

## 主图 / 副图模型

必须显式区分主图与副图：

- `role = "main"`：主图
- `role = "sub"`：副图

约束：

- 配置中必须且只能有一个 `main` 面板
- `main` 面板第一阶段必须是 `kind = "candles"`
- `sub` 面板第一阶段可为 `line`、`bar`、`brick`

主图与副图除了高度不同，更重要的是交互和附属能力不同。建议增加 `features`：

- `ohlc_tooltip`
- `interval_select`
- `rank_marker`
- `cyq_overlay`
- `watch_observe`

这样前端可以依据协议启用能力，而不是继续写：

- `panel.key === "price"`
- `panel.kind === "candles"`

## 指标重叠模型

“重叠”不是额外概念，而是同一面板中多个 `series` 共同绘制。

第一阶段支持的重叠组合：

- `candles` 面板：K 线主体 + 多条 `line`
- `bar` 面板：一条主 `bar` + 多条 `line`
- `line` 面板：多条 `line`
- `brick` 面板：单条 `brick`

第一阶段不支持的组合：

- 一个面板中多条主 `bar`
- `brick` 与其他图元混绘
- 一个面板里同时出现多种主坐标域

建议补充的渲染字段：

- `draw_order`: 控制先后覆盖顺序
- `base_value`: 柱体/直方图基线
- `opacity`: 透明度
- `line_width`: 线宽

如果后续需要更复杂的叠加，再考虑新增：

- `axis = "left" | "right"`
- `domain_mode = "shared" | "independent"`

但第一阶段不要引入，避免协议和前端实现一起失控。

## 分支判断逻辑

计算层允许表达式做条件分支，直接复用现有表达式函数。

例如：

```toml
expr = "IF(C > MA(C, 20), C, MA(C, 20))"
```

可用于：

- 动态数值序列
- `marker.when`
- `color_when`

约束：

- `expr` 最终必须产出数值序列
- `when` 最终必须产出布尔序列
- `color_when` 按声明顺序匹配，命中第一条后停止

推荐把分支逻辑放在表达式层，不在前端增加特殊判断分支。

## 配置模型

建议新增模块：

```text
src/ui_tools_feat/chart_indicator.rs
```

该文件内部按职责分段组织

- 配置结构与默认配置
- 配置校验与依赖收集
- 表达式编译与缓存
- 图表运行时执行与输出组装

建议结构：

```rust
struct ChartIndicatorConfig {
    version: u32,
    panel: Vec<ChartPanelConfig>,
}

struct ChartPanelConfig {
    key: String,
    label: String,
    role: ChartPanelRole,
    kind: ChartPanelKind,
    row_weight: Option<u32>,
    features: Vec<ChartPanelFeature>,
    series: Vec<ChartSeriesConfig>,
    marker: Vec<ChartMarkerConfig>,
}

struct ChartSeriesConfig {
    key: String,
    label: Option<String>,
    expr: String,
    kind: ChartSeriesKind,
    draw_order: Option<i32>,
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

建议枚举：

```rust
enum ChartPanelRole {
    Main,
    Sub,
}

enum ChartPanelKind {
    Candles,
    Line,
    Bar,
    Brick,
}

enum ChartSeriesKind {
    Line,
    Bar,
    Histogram,
    Area,
    Band,
    Brick,
}
```

## 命名与引用规则

这一部分必须显式定义，否则运行时和结果字段会冲突。

建议规则：

- `panel.key` 全局唯一
- `series.key` 全局唯一
- `marker.key` 在各自面板内唯一
- `key` 只允许 `[A-Za-z_][A-Za-z0-9_]*`

这样做的原因：

- `series.key` 会回注到 runtime
- `series.key` 会下发到前端作为动态字段
- 如果只要求“面板内唯一”，两个面板都写 `MA5` 会互相覆盖

表达式引用规则：

- 基础行情字段可直接引用，如 `O/H/L/C/V`
- 已落库指标列可直接引用，如 `J`、`VOL_SIGMA`
- 当前配置里前序 `series.key` 可直接引用，如 `duokong_short`

内部辅助 key 建议命名：

- `__marker_<panel>_<marker>`
- `__color_<panel>_<series>_<index>`

辅助 key 只参与运行时，不暴露为业务序列名。

## 编译模型

配置解析之后进入编译态，避免每次请求重复 parse：

```rust
struct CompiledChartIndicatorConfig {
    panels: Vec<CompiledChartPanel>,
}

struct CompiledChartPanel {
    key: String,
    label: String,
    role: ChartPanelRole,
    kind: ChartPanelKind,
    row_weight: Option<u32>,
    features: Vec<ChartPanelFeature>,
    series: Vec<CompiledChartSeries>,
    markers: Vec<CompiledChartMarker>,
}

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

struct CompiledChartColorRule {
    when_key: String,
    when_expr: Stmts,
    color: String,
}
```

编译时使用现有表达式能力：

- `lex_all`
- `Parser::parse_main`

编译错误需要携带完整路径信息：

- `panel.key`
- `series.key`
- `marker.key`
- `color_when` 下标

## Runtime 输入

运行时先把详情页基础行情构造成 `RowData`，基础变量沿用现有表达式体系：

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

兼容别名同时注入：

- `OPEN`
- `HIGH`
- `LOW`
- `CLOSE`
- `VOL`
- `TOR`

## 已落库指标列的加载策略

蓝图不能只说“允许引用落库指标列”，还要说明这些列怎么进 runtime。

建议策略：

1. 先收集配置中所有表达式引用到的标识符。
2. 剔除基础变量名、函数名、前序 `series.key`。
3. 剩余标识符视为数据库指标列依赖。
4. 查询 K 线时，把这些列追加到 SELECT 中。

约束：

- 只允许引用白名单列名，避免任意拼接 SQL
- 找不到列时在编译/预运行阶段报错
- 数据库列名与 runtime 变量名大小写归一化处理

这样才能真正支持：

- 图表表达式引用落库指标
- 第一阶段已有 `J`、`VOL_SIGMA`、`BRICK` 等列的平滑迁移

## 计算流程

单次详情页计算流程：

1. 读取或加载默认 `ChartIndicatorConfig`
2. 解析并编译为 `CompiledChartIndicatorConfig`
3. 收集配置依赖的数据库列
4. 查询基础 K 线数据并构造 `RowData`
5. 按 `panel.series` 顺序执行表达式
6. 将 `series` 结果写入输出行，并回注 runtime
7. 计算 `marker.when`
8. 计算 `series.color_when`
9. 返回图表协议和动态字段数据

序列回注规则：

- `series.key` 的结果写入运行时变量表
- 后续 `expr` 可直接引用前序 `series.key`
- `marker.when` 与 `color_when` 只产出布尔结果，不回注为业务指标名

## 空值与预热期规则

图表类指标一定会遇到前 N 根为空的问题，这部分需要提前定义。

建议规则：

- 表达式计算失败或预热不足时，结果为 `null`
- 前端折线遇到 `null` 自动断开，不强行补点
- 柱体遇到 `null` 不绘制
- 标记条件为 `null` 时按 `false` 处理
- `color_when` 条件为 `null` 时视为未命中

这样可以统一处理：

- `MA/EMA` 预热期
- `REF` 越界
- 实时行缺少部分衍生值

## 输出数据协议

详情数据行需要从固定字段结构过渡为固定基础字段加动态指标字段：

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
    #[serde(flatten)]
    pub indicators: HashMap<String, serde_json::Value>,
}
```

动态字段写入规则：

- `row[series.key]`：数值序列结果
- `row[marker.when_key]`：布尔序列结果
- `row[color_rule.when_key]`：布尔序列结果

推荐的图表协议：

```ts
type DetailChartPanel = {
  key: string
  label: string
  role: "main" | "sub"
  kind: "candles" | "line" | "bar" | "brick"
  row_weight?: number
  features?: Array<
    | "ohlc_tooltip"
    | "interval_select"
    | "rank_marker"
    | "cyq_overlay"
    | "watch_observe"
  >
  series?: DetailChartSeries[]
  markers?: DetailChartMarker[]
}

type DetailChartSeries = {
  key: string
  label?: string
  kind: "line" | "bar" | "histogram" | "area" | "band" | "brick"
  draw_order?: number
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

协议下发时不包含原始 `expr`、`when` 字符串，只下发已经计算好的结果 key 和渲染描述。

## 校验规则

配置校验分三层：

1. 结构校验
2. 编译校验
3. 运行前约束校验

结构校验负责：

- TOML 格式是否合法
- 必填字段是否存在
- `main` 面板数量是否为 1
- key 是否重复
- role 是否合法
- kind 是否合法
- color 是否合法

编译校验负责：

- `expr` 是否可解析
- `when` 是否可解析
- 是否引用未定义的前序 `series`
- 是否引用不存在的数据库列

运行前约束校验负责：

- `expr` 结果是否可转数值序列
- `when` 结果是否可转布尔序列
- `y` / `y_key` 是否引用可取值字段
- 面板内图元组合是否合法

面板组合约束建议明确写死：

- `candles` 面板允许 `line` marker，不允许 `bar/brick`
- `line` 面板只允许 `line/area/band`
- `bar` 面板允许一个主 `bar`，允许叠加 `line`
- `brick` 面板只允许一个 `brick`

## 默认配置

无配置文件时返回内置默认配置，维持现有图表行为。

默认配置第一阶段必须覆盖当前既有面板：

- 主 K：K 线 + `duokong_short` + `duokong_long`
- 指标：`j` + `bupiao_long` + `bupiao_short`
- 量能：`vol` + `VOL_SIGMA`
- 砖型图：`brick`

实现约束：

- 基础行情字段继续作为固定字段保留
- 图表指标、标记条件、颜色条件统一进入动态字段
- 返回协议以 `series[]`、`markers[]`、`features[]` 为准，不再以 `series_keys` 为主协议
- 内置默认配置与 `examples/chart_indicators.toml` 的语义必须一致

## 工程落点

这一版的实现应直接落到完整的新链路上，不保留“半配置化、半硬编码”的中间设计。具体落点如下。

后端核心：

- `src/ui_tools_feat/details.rs`
  负责详情图查询入口，改为“加载配置 -> 编译配置 -> 收集依赖列 -> 构造 `RowData` -> 执行图表运行时 -> 返回协议化 payload”。
- `src/ui_tools_feat/mod.rs`
  导出新增模块。
- `src/ui_tools_feat/chart_indicator.rs`
  新增。统一承载配置结构、默认配置、校验、表达式编译、依赖列收集、缓存，以及基于 `RowData` 执行 `series`、`marker`、`color_when` 并组装详情图表输出。

前端协议层：

- `ui/lianghua_web_feat/src/apis/details.ts`
  升级 `DetailKlinePayload`、`DetailKlinePanel`、`DetailKlineRow` 类型定义，允许固定基础字段与动态图表字段并存。
- `ui/lianghua_web_feat/src/pages/desktop/DetailsPage.tsx`
  改为基于 `role`、`kind`、`features`、`series`、`markers` 渲染，不再依赖 `series_keys` 和固定 `panel.key` 语义。

配置与样例：

- `docs/custom-chart-indicators-blueprint.md`
  记录协议、约束和默认配置语义。
- `examples/chart_indicators.toml`
  提供与内置默认配置一致的样例文件。

接口层：

- `ui/lianghua_web_feat/src-tauri/src/lib.rs`
  Tauri command 入口保持不变，只承接更丰富的返回结构。

## 实施拆分建议

这一版改造不适合按“新增一个图表功能”来拆，而应按“先打底协议与运行时，再替换调用方”来拆。原因是当前实现里，后端和前端都还深度依赖旧协议：

- 后端在 `src/ui_tools_feat/details.rs` 中直接固定查询列、固定 `DetailKlineRow` 字段，并通过 `default_kline_panels()` 写死面板结构
- 前端在 `ui/lianghua_web_feat/src/pages/desktop/DetailsPage.tsx` 中大量依赖 `panel.key`、`series_keys`、`formatSeriesLabel()`、`getSeriesColor()` 推断语义

如果不先把协议和运行时边界立住，后续每一个图法扩展都会继续把硬编码扩散到前后端。

### 阶段一：后端配置模型打底

目标：先把 `chart_indicators.toml` 的配置模型、默认配置和基础校验独立出来，不接入详情页主流程。

建议步骤：

1. 新增 `src/ui_tools_feat/chart_indicator.rs`
2. 定义配置结构、枚举、默认配置构造函数
3. 实现 `chart_indicators.toml` 的读取逻辑，规则为“外部文件优先，缺失时回落到内置默认配置”
4. 实现第一批结构校验：
   - `main` 面板数量必须且只能为 1
   - `panel.key` / `series.key` 唯一
   - `role` / `kind` 合法
   - `color` 合法
   - 面板组合约束合法
5. 为配置解析和结构校验补单元测试

这一阶段完成标准：

- 不改 `details.rs`
- 能稳定得到 `ChartIndicatorConfig`
- 默认配置与蓝图示例语义一致

### 阶段二：编译态与依赖分析

目标：把配置从“可读”推进到“可执行”，形成稳定的编译态和依赖列收集能力。

建议步骤：

1. 定义 `CompiledChartIndicatorConfig`、`CompiledChartPanel`、`CompiledChartSeries`、`CompiledChartMarker`
2. 接入现有表达式能力，完成 `expr`、`marker.when`、`color_when.when` 的编译
3. 实现标识符收集，区分：
   - 基础行情字段
   - 表达式函数名
   - 当前配置前序 `series.key`
   - 数据库指标列依赖
4. 做第二批校验：
   - 表达式是否可编译
   - 是否引用未定义的前序序列
   - 是否引用不存在的数据库列
5. 为编译错误增加路径信息：
   - `panel.key`
   - `series.key`
   - `marker.key`
   - `color_when` 下标
6. 为编译与依赖分析补单元测试

这一阶段完成标准：

- 输入配置后可得到编译态对象
- 可稳定收集详情图查询所需的数据库列
- 编译错误能够定位到具体配置项

### 阶段三：历史详情后端切换到新链路

目标：先只改历史详情，让详情页主数据从固定字段模型切到“基础字段 + 动态指标字段”模型。

建议步骤：

1. 改造 `DetailKlineRow`，从固定指标字段迁移到固定基础字段加动态 `indicators`
2. 改造详情图查询逻辑，不再固定 SELECT `brick`、`j`、`duokong_short` 等列，而是根据依赖列动态追加查询
3. 在 `src/ui_tools_feat/details.rs` 中接入完整新链路：
   - 加载配置
   - 编译配置
   - 收集依赖列
   - 查询 K 线
   - 构造 `RowData`
   - 执行 `series`
   - 执行 `marker.when`
   - 执行 `color_when`
   - 组装协议化 payload
4. 返回新的 `panels/series/markers/features` 协议
5. 处理 `null`、预热期、`REF` 越界等规则
6. 为默认配置路径和外部配置路径补测试

这一阶段完成标准：

- 历史详情页的 4 个面板都由新配置生成
- 后端不再依赖 `default_kline_panels()` 和固定指标字段拼装结果
- 历史详情请求可完整跑通新运行时

### 阶段四：实时详情并轨

目标：让实时详情复用同一份配置、同一份编译态、同一份运行时，不再保留单独指标补算分支。

建议步骤：

1. 复查当前实时详情链路中依赖 `ind.toml` 和固定指标字段的部分
2. 在实时行拼接后重建 `RowData`
3. 复用同一份 `CompiledChartIndicatorConfig` 重新执行 `series`、`marker.when`、`color_when`
4. 统一实时与历史的空值、预热期和条件判断行为
5. 验证“覆盖最后一根”和“追加新一根”两种实时路径

这一阶段完成标准：

- 实时详情与历史详情走同一套图表运行时
- 不再为实时图维护独立的详情指标补算逻辑

### 阶段五：前端协议类型适配

目标：先让前端类型系统接受新协议，再逐步替换渲染逻辑。

建议步骤：

1. 升级 `ui/lianghua_web_feat/src/apis/details.ts` 中的类型定义
2. 增加新的 `DetailChartPanel`、`DetailChartSeries`、`DetailChartMarker`、`DetailChartColorRule` 类型
3. 保证固定基础字段与动态指标字段能同时表达
4. 为前端增加必要的协议兼容辅助函数

这一阶段完成标准：

- TS 类型层面可以完整表达新协议
- 不要求这一阶段完成所有渲染切换

### 阶段六：前端渲染主流程切换

目标：把 `DetailsPage` 从 `series_keys + panel.key` 驱动改成 `role + kind + features + series + markers` 驱动。

建议步骤：

1. 抽离 `DetailsPage` 内当前基于 key 猜语义的辅助逻辑
2. 用 `panel.series[].label` 替代 `formatSeriesLabel()` 猜测
3. 用 `panel.series[].color` / `color_when` 替代 `getSeriesColor()` 的硬编码映射
4. 让 `candles`、`line`、`bar`、`brick` 四类面板都直接消费 `panel.series`
5. 让 tooltip、表头标签、叠加线、柱体、砖型图、marker 都从协议读配置
6. 把主图特殊交互从 `panel.key === "price"` 逐步切到 `role` / `features`
7. 仅在过渡期保留最小兼容 fallback，最终移除 `series_keys`

这一阶段完成标准：

- 前端主渲染流程不再依赖 `series_keys`
- 主图能力不再依赖 `panel.key == "price"`
- `formatSeriesLabel()`、`getSeriesColor()` 不再承担协议解释职责

### 建议交付粒度

如果按任务卡或 PR 拆分，建议粒度如下：

1. PR1：配置模型、默认配置、结构校验、样例文件、单元测试
2. PR2：编译态、表达式编译、依赖列收集、编译校验、单元测试
3. PR3：历史详情后端切换到新链路
4. PR4：实时详情并轨
5. PR5：前端协议类型适配
6. PR6：前端渲染主流程切换并移除旧硬编码

### 拆分原则

实施时建议遵守两个原则：

1. 不要在第一步同时重写后端与前端。应先稳定后端协议与运行时，再切前端渲染。
2. 不要在第一步同时重写历史详情与实时详情。应先让历史链路稳定，再让实时链路并轨。

## 验收标准

这一版不以“支持任意新图法”为验收，而以“现有详情图完全走新链路且视觉行为不退化”为验收。具体标准：

- 当前 4 个面板全部由新配置模型生成
- 历史详情与实时详情共用同一套图表编译与运行时逻辑
- 主图能力通过 `role/features` 协议启用，不再依赖 `panel.key == "price"`
- 前端渲染主流程不再依赖硬编码的 `formatSeriesLabel()`、`getSeriesColor()` 来猜测协议语义
- 在默认配置和外部 `chart_indicators.toml` 两种输入下，页面协议行为一致

## 与 `ind.toml` 的边界

- `ind.toml`：面向下载、增量计算、落库指标
- `chart_indicators.toml`：面向详情图表展示

建议边界：

- 高成本、复用度高的指标适合进 `ind.toml`
- 临时展示、局部叠加型指标适合进 `chart_indicators.toml`
- 图表表达式可以直接引用已经落库的指标列

判断标准：

- 是否需要全市场批量计算
- 是否需要长期落库复用
- 是否只在详情图临时展示

## 实时计算一致性

实时详情数据应复用与历史图相同的配置和计算链路：

1. 拼接实时行
2. 重新构造 `RowData`
3. 重新执行同一份 `CompiledChartIndicatorConfig`
4. 返回包含实时行的结果集

实时场景不应维护单独的指标计算分支。

## 后续扩展点

在第一阶段稳定后，再向下扩展：

- 新增 `histogram`、`area`、`band`
- 增加 MACD、BOLL 等内置模板
- 补充更丰富的 marker 图元
- 增加股票软件 DSL 到 TOML 的转换器
- 为复杂图元补充新的 render config

扩展方向不改变核心约束：

- 表达式在 Rust 执行
- 渲染端只消费结构化协议和结果数据
- 主图特殊能力走协议，不走前端硬编码分支
