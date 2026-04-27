# 自定义图表指标配置设置页设计

## 背景

本页用于在前端编辑运行时图表配置文件：

```text
<source_path>/chart_indicators.toml
```

它不是 `ind.toml`。`ind.toml` 用于下载/计算阶段生成落库指标；`chart_indicators.toml` 用于详情页 K 线区域“怎么画”。图表表达式可以引用 `stock_data.db` 里已经存在的指标列，也可以引用本配置中前面声明过的 `series.key`。

现有后端协议已支持：

- 面板：`candles`、`line`、`bar`、`brick`
- 序列：`line`、`bar`、`histogram`、`area`、`band`、`brick`，其中当前渲染实际支持 `line`、`bar`、`brick`
- 条件颜色：`color_when`
- 条件标记：`marker`
- 表达式：复用 `src/expr/*`，在 Rust 侧解析和执行

本设计目标是把编辑入口放进 `SettingsPage` 的设置列表，让用户可以用结构化表单修改配置，同时保留 TOML 源码编辑兜底能力。

## 入口位置

在 `ui/lianghua_web_feat/src/pages/desktop/SettingsPage.tsx` 的设置列表新增一项：

- 标题：`自定义图表指标`
- 描述：`编辑详情页 K 线、指标、量能、砖型图等图表面板。`
- 右侧值：
  - 已存在配置：`N 个面板`
  - 不存在配置：`使用默认`
  - 读取失败：`需修复`

点击后打开全屏或宽弹窗编辑器。推荐使用宽弹窗，沿用现有设置页弹窗层级：

- `settings-modal-backdrop`
- `settings-modal`
- 新增修饰类：`settings-modal-wide`

窗口顶部操作：

- `关闭`
- `重置为默认`
- `源码`
- `校验`
- `保存`

保存成功提示：`已保存。重新进入详情页或刷新当前详情页后生效。`

## 页面布局

编辑器采用三栏结构：

1. 左侧：面板列表
2. 中间：当前面板编辑
3. 右侧：当前序列/标记编辑 + 预览摘要

推荐宽度：

- 左侧面板列表：220px
- 中间面板编辑：minmax(360px, 1fr)
- 右侧细节编辑：minmax(360px, 1fr)

移动端或窄屏时改为上下布局：

- 面板列表横向 tabs
- 面板表单
- 序列/标记表单

## 编辑模式

提供两个模式，用 tabs 切换：

- `结构化编辑`
- `TOML 源码`

结构化编辑适合日常使用。源码模式用于批量粘贴、复杂表达式调整、问题排查。

切换规则：

- 从结构化到源码：把当前 draft 序列化成 TOML。
- 从源码到结构化：先调用校验/解析；通过后替换结构化 draft。
- 源码模式存在未通过校验内容时，允许停留源码模式，但禁用结构化切换和保存。

## 数据获取与保存 API

建议新增前端 API 文件：

```text
ui/lianghua_web_feat/src/apis/chartIndicatorSettings.ts
```

建议新增 Tauri commands：

```rust
get_chart_indicator_settings(source_path: String) -> Result<ChartIndicatorSettingsPayload, String>
validate_chart_indicator_settings(source_path: String, text: String) -> Result<ChartIndicatorValidationResult, String>
save_chart_indicator_settings(source_path: String, text: String) -> Result<ChartIndicatorSettingsPayload, String>
reset_chart_indicator_settings(source_path: String) -> Result<ChartIndicatorSettingsPayload, String>
```

`source_path` 通过 `ensureManagedSourcePath()` 获取。

### 返回结构

```ts
type ChartIndicatorSettingsPayload = {
  sourcePath: string
  filePath: string
  exists: boolean
  text: string
  config: ChartIndicatorConfigDraft
  summary: {
    panelCount: number
    seriesCount: number
    markerCount: number
    databaseIndicatorColumns: string[]
  }
}

type ChartIndicatorValidationResult = {
  ok: boolean
  error?: string
  config?: ChartIndicatorConfigDraft
  summary?: ChartIndicatorSettingsPayload['summary']
}
```

保存时后端应执行：

1. `toml::from_str`
2. `validate_chart_indicator_config`
3. 如果能拿到 `stock_data.db` 列名，则调用 `compile_chart_indicator_config(..., Some(columns))`
4. 原子写入 `<source_path>/chart_indicators.toml`

原子写入策略：先写 `chart_indicators.toml.tmp`，成功后 rename 覆盖。

## 顶层配置

### `version`

- 类型：整数
- 当前固定：`1`
- 控件：只读数字展示
- 默认：`1`
- 校验：必须为 `1`
- UI 文案：`配置协议版本`

不建议在 UI 暴露可编辑版本号，避免用户保存出当前后端不支持的协议。

### `panel`

- 类型：数组
- 控件：左侧可排序列表
- 操作：新增、复制、删除、上移、下移
- 校验：
  - 必须有且只有一个 `role = "main"` 面板
  - `panel.key` 全局唯一
  - 至少保留一个面板

新增面板时提供模板：

- `主K面板`
- `指标线面板`
- `量能柱面板`
- `砖型图面板`

如果已经存在主图，新增 `主K面板` 按钮置灰。

## 面板参数设计

### `panel.key`

- 类型：字符串
- 控件：文本输入
- 示例：`price`、`indicator`、`volume`、`brick`
- 校验：`[A-Za-z_][A-Za-z0-9_]*`
- 唯一性：全局唯一
- 是否可空：否
- UI 文案：`面板标识`
- 帮助文案：`用于内部引用和渲染 key，保存后不建议频繁修改。`

### `panel.label`

- 类型：字符串
- 控件：文本输入
- 示例：`主K`、`指标`、`量能`
- 校验：去除首尾空格后不可为空
- UI 文案：`显示名称`

### `panel.role`

- 枚举：`main`、`sub`
- 控件：分段控制
- UI 选项：
  - `主图` -> `main`
  - `副图` -> `sub`
- 校验：
  - 必须且只能有一个 `main`
  - `main` 面板必须使用 `kind = "candles"`
- 联动：
  - 选择 `main` 时，自动把 `kind` 改为 `candles`
  - 已有其他主图时，切换为 `main` 需要确认，并把旧主图改为 `sub`

### `panel.kind`

- 枚举：`candles`、`line`、`bar`、`brick`
- 控件：图法下拉或图标分段控制
- UI 选项：
  - `K线` -> `candles`
  - `折线` -> `line`
  - `柱状` -> `bar`
  - `砖型` -> `brick`
- 校验和联动见“不同面板图法”。

### `panel.row_weight`

- 类型：正整数或空
- 控件：数字输入/步进器
- 推荐范围：`6` 到 `80`
- 默认：
  - 主图：`46`
  - 副图：`18`
- 允许为空：是，空值表示交给前端默认比例
- UI 文案：`高度权重`
- 帮助文案：`同一图表中各面板按权重分配高度。`

### `panel.series`

- 类型：数组
- 控件：当前面板内的“序列”列表
- 操作：新增、复制、删除、排序
- 校验：
  - `series.key` 全局唯一
  - 不同 `panel.kind` 对 `series.kind` 有不同限制
  - 同一面板按声明顺序计算，后面的表达式才能引用前面的序列

### `panel.marker`

- 类型：数组
- 控件：当前面板内的“标记”列表
- 操作：新增、复制、删除、排序
- 校验：
  - `marker.key` 在当前面板内唯一
  - `marker.when` 必须返回布尔序列
  - `marker.y` 如果填写，必须是基础字段、已声明序列或已落库指标列

## 序列参数设计

### `series.key`

- 类型：字符串
- 控件：文本输入
- 示例：`ma20`、`vol`、`brick`
- 校验：`[A-Za-z_][A-Za-z0-9_]*`
- 唯一性：全局唯一
- 是否可空：否
- UI 文案：`序列标识`
- 帮助文案：`会成为表达式可引用变量，也会成为详情页数据字段。`

### `series.label`

- 类型：字符串或空
- 控件：文本输入
- 示例：`MA20`、`成交量`
- 允许为空：是，空时前端显示 `series.key`
- UI 文案：`显示名称`

### `series.expr`

- 类型：字符串
- 控件：多行表达式输入，建议 3-6 行高度
- 示例：
  - `C`
  - `MA(C, 20)`
  - `IF(C > MA(C, 20), C, MA(C, 20))`
  - `VOL_SIGMA`
- 校验：
  - 解析必须通过
  - 最终结果必须是数值序列
  - 可引用基础字段、已落库指标列、前序 `series.key`
- UI 文案：`计算表达式`
- 辅助入口：
  - `插入字段`
  - `插入函数`
  - `语法说明书`

基础字段建议在插入菜单中列出：

- `O / OPEN`
- `H / HIGH`
- `L / LOW`
- `C / CLOSE`
- `V / VOL`
- `AMOUNT`
- `PRE_CLOSE`
- `CHANGE`
- `PCT_CHG`
- `TOR / TURNOVER_RATE`

函数菜单建议列出现有白名单：

```text
ABS, MAX, MIN, DIV, HHV, LLV, COUNT, MA, REF, LAST, SUM, STD, IF,
CROSS, EMA, SMA, BARSLAST, RSV, GRANK, GTOPCOUNT, LTOPCOUNT, LRANK, GET
```

### `series.kind`

- 枚举：`line`、`bar`、`brick`
- 未来保留：`histogram`、`area`、`band`
- 控件：图法下拉
- 当前 UI 默认只开放当前渲染支持项：
  - `折线` -> `line`
  - `柱体` -> `bar`
  - `砖体` -> `brick`
- `histogram / area / band` 可在源码模式中提示“协议保留，当前前端暂不渲染”，结构化模式先不开放。

### `series.draw_order`

- 类型：整数或空
- 控件：数字输入
- 推荐范围：`-100` 到 `100`
- 默认：空，按声明顺序
- UI 文案：`绘制顺序`
- 规则：数字越小越先绘制，越大越盖在上层
- 典型值：
  - 柱体：`10`
  - 折线叠加：`20`
  - 强提示线：`30`

### `series.color`

- 类型：颜色字符串或空
- 控件：颜色选择器 + hex 输入
- 示例：`#d9485f`
- 校验：当前后端接受 `#RGB` 或 `#RRGGBB`
- 默认：空，前端使用内置调色板
- UI 文案：`默认颜色`

### `series.color_when`

- 类型：条件颜色规则数组
- 控件：可排序规则列表
- 每条规则字段：
  - `when`：布尔表达式
  - `color`：颜色
- UI 文案：`条件颜色`
- 校验：
  - `when` 必须返回布尔序列
  - `color` 必须为合法颜色
- 规则：按声明顺序匹配，前端使用第一条命中规则；都不命中时使用 `series.color` 或默认色
- 示例：

```toml
color_when = [
  { when = "C > REF(C, 1)", color = "#d9485f" },
  { when = "C < REF(C, 1)", color = "#178f68" },
]
```

### `series.line_width`

- 类型：数字或空
- 控件：数字输入/滑杆
- 推荐范围：`0.5` 到 `6`
- 默认：空，前端使用 `1.6`
- 适用图法：`line`
- UI 文案：`线宽`

### `series.opacity`

- 类型：数字或空
- 控件：滑杆 + 数字输入
- 范围：`0` 到 `1`
- 步长：`0.05`
- 默认：空，前端按 `1`
- 适用图法：`line`、`bar`、`brick`
- UI 文案：`透明度`

### `series.base_value`

- 类型：数字或空
- 控件：数字输入
- 默认：
  - `bar`：`0`
  - 其他：空
- 适用图法：`bar`
- UI 文案：`基线值`
- 帮助文案：`柱体从该值向上或向下绘制。成交量通常为 0。`

## 标记参数设计

### `marker.key`

- 类型：字符串
- 控件：文本输入
- 示例：`buy_signal`、`cross_ma20`
- 校验：`[A-Za-z_][A-Za-z0-9_]*`
- 唯一性：当前面板内唯一
- UI 文案：`标记标识`

### `marker.label`

- 类型：字符串或空
- 控件：文本输入
- UI 文案：`显示名称`

### `marker.when`

- 类型：字符串
- 控件：多行表达式输入
- 示例：
  - `CROSS(C, ma20)`
  - `C > MA(C, 20)`
  - `J > 80`
- 校验：
  - 解析必须通过
  - 最终结果必须是布尔序列
- UI 文案：`出现条件`

### `marker.y`

- 类型：字符串或空
- 控件：字段选择 + 可手输
- 示例：`L`、`H`、`C`、`ma20`
- 默认：空
- UI 文案：`定位字段`
- 规则：
  - 空值：按 `position` 使用面板上下边缘或当前值策略
  - 填写：标记锚定到该数值序列对应的 Y 坐标

### `marker.position`

- 枚举：`above`、`below`、`value`
- 控件：分段控制
- UI 选项：
  - `上方` -> `above`
  - `下方` -> `below`
  - `数值处` -> `value`
- 默认：`value`
- UI 文案：`位置`

### `marker.shape`

- 枚举：`dot`、`triangle_up`、`triangle_down`、`flag`
- 控件：图标分段控制
- UI 选项：
  - `圆点` -> `dot`
  - `上三角` -> `triangle_up`
  - `下三角` -> `triangle_down`
  - `旗标` -> `flag`
- 默认：`dot`
- UI 文案：`形状`

### `marker.color`

- 类型：颜色字符串或空
- 控件：颜色选择器 + hex 输入
- 默认：空，前端使用上涨色
- UI 文案：`颜色`

### `marker.text`

- 类型：字符串或空
- 控件：短文本输入
- 示例：`B`、`S`
- 建议长度：1-4 个字符
- UI 文案：`文本`
- 联动：
  - `shape = "flag"` 时推荐填写
  - 填写 `text` 时，即使 `shape` 不是 `flag`，当前前端也会按旗标文本方式渲染

## 不同面板图法

### `candles`：K 线面板

用途：主图，绘制 OHLC K 线，可叠加均线、趋势线、买卖点。

面板约束：

- `role` 必须为 `main`
- 当前只允许一个 `main` 面板
- `series.kind` 只能是 `line`
- K 线主体不需要用户配置 `series`，由基础字段 `O/H/L/C` 自动绘制

可配置序列参数：

- 必填：`key`、`expr`、`kind = "line"`
- 常用：`label`、`color`、`line_width`、`draw_order`
- 可选：`opacity`、`color_when`
- 不适用：`base_value`

推荐模板：

```toml
[[panel]]
key = "price"
label = "主K"
role = "main"
kind = "candles"
row_weight = 46

[[panel.series]]
key = "ma20"
label = "MA20"
expr = "MA(C, 20)"
kind = "line"
draw_order = 10
color = "#2563eb"
line_width = 1.4
```

### `line`：折线指标面板

用途：J、KDJ、强弱指标、任意数值曲线。

面板约束：

- `role = "sub"`
- `series.kind` 当前只能是 `line`
- 支持多条线共用一个 Y 轴域

可配置序列参数：

- 必填：`key`、`expr`、`kind = "line"`
- 常用：`label`、`color`、`line_width`
- 可选：`draw_order`、`opacity`、`color_when`
- 不适用：`base_value`

推荐模板：

```toml
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
color = "#2563eb"
```

### `bar`：柱状面板

用途：成交量、资金流、正负柱，并可叠加一条或多条折线。

面板约束：

- `role = "sub"`
- 最多一条 `series.kind = "bar"`
- 允许多条 `series.kind = "line"` 作为叠加线
- 不支持 `brick / area / band / histogram` 结构化编辑

柱体序列参数：

- 必填：`key`、`expr`、`kind = "bar"`
- 常用：`label`、`base_value`、`color_when`
- 可选：`color`、`opacity`、`draw_order`
- 不适用：`line_width`

叠加线序列参数：

- 必填：`key`、`expr`、`kind = "line"`
- 常用：`label`、`color`、`line_width`
- 可选：`opacity`、`draw_order`

推荐模板：

```toml
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
base_value = 0
color_when = [
  { when = "C > REF(C, 1)", color = "#d9485f" },
  { when = "C < REF(C, 1)", color = "#178f68" },
]

[[panel.series]]
key = "vol_ma5"
label = "量MA5"
expr = "MA(VOL, 5)"
kind = "line"
draw_order = 20
color = "#7c3aed"
```

### `brick`：砖型图面板

用途：根据某个离散或阶梯指标绘制砖块变化。

面板约束：

- `role = "sub"`
- 必须且只能有一条 `series`
- 该序列必须是 `kind = "brick"`
- 不支持和其他图元混绘

可配置序列参数：

- 必填：`key`、`expr`、`kind = "brick"`
- 常用：`label`、`color_when`
- 可选：`color`、`opacity`
- 不适用：`line_width`、`base_value`

推荐模板：

```toml
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
color_when = [
  { when = "BRICK > REF(BRICK, 1)", color = "#d9485f" },
  { when = "BRICK < REF(BRICK, 1)", color = "#178f68" },
]
```

## 未来保留图法

后端枚举已有 `histogram`、`area`、`band`，但当前 `DetailsPage` 尚未实际渲染这些类型。结构化编辑第一版不要开放；源码模式如果读到这些值：

- 保留原值
- 在序列卡片上显示“暂不支持结构化编辑”
- 禁用该序列的图法切换
- 保存前仍以后端校验为准

后续扩展建议：

- `histogram`：类似 `bar`，但默认以 `base_value = 0` 为中心，适合 MACD 柱
- `area`：在折线下方填充到 `base_value` 或面板底部
- `band`：需要两个表达式或一个返回上下轨的协议，目前单 `expr` 不够，建议未来新增 `upper_expr`、`lower_expr`

## 校验规则

前端即时校验：

- 必填字段不能为空
- `key` 正则
- 颜色格式
- 枚举合法性
- `row_weight`、`line_width`、`opacity`、`base_value` 数值范围
- 面板/序列数量约束
- 主图唯一性
- 图法组合约束

后端权威校验：

- TOML 解析
- 配置版本
- 颜色格式
- 全局唯一性
- 表达式解析
- 函数白名单
- 数据库指标列是否存在
- 前序序列引用规则
- 表达式结果类型

错误展示：

- 顶部显示第一条阻断错误
- 对应字段下方显示字段错误
- 源码模式显示后端错误原文
- 错误文案尽量保留路径，例如 `panel.volume.series.vol.expr`

## 表达式引用规则

用户可引用：

- 基础行情字段：`O/H/L/C/V`、`OPEN/HIGH/LOW/CLOSE/VOL`
- 基础扩展字段：`AMOUNT`、`PRE_CLOSE`、`CHANGE`、`PCT_CHG`、`TOR/TURNOVER_RATE`
- 已落库指标列：如 `J`、`VOL_SIGMA`、`BRICK`
- 当前配置中前面声明过的 `series.key`

不能引用：

- 后面才声明的 `series.key`
- 不存在的数据库列
- 未在白名单中的函数

UI 需要在表达式输入旁边提供“可引用字段”抽屉：

- 基础字段固定列出
- 已落库指标列从后端 summary 返回
- 前序序列根据当前声明顺序动态列出

## TOML 序列化约定

保存时建议使用稳定顺序，减少 diff 噪音：

```toml
version = 1

[[panel]]
key = "..."
label = "..."
role = "..."
kind = "..."
row_weight = ...

[[panel.series]]
key = "..."
label = "..."
expr = "..."
kind = "..."
draw_order = ...
color = "..."
line_width = ...
opacity = ...
base_value = ...
color_when = [
  { when = "...", color = "..." },
]

[[panel.marker]]
key = "..."
label = "..."
when = "..."
y = "..."
position = "..."
shape = "..."
color = "..."
text = "..."
```

空值字段不写入 TOML。

## 默认配置建议

如果文件不存在，设置页展示后端默认配置，并在顶部提示：

`当前数据源未创建 chart_indicators.toml，保存后会写入新文件。`

推荐“一键写入完整模板”，覆盖现有详情页四面板：

- 主K：K 线 + `DUOKONG_SHORT` + `DUOKONG_LONG`
- 指标：`J` + `BUPIAO_LONG` + `BUPIAO_SHORT`
- 量能：`VOL` + `VOL_SIGMA`
- 砖型图：`BRICK`

如果某些指标列不存在，模板仍可展示，但保存校验需要给出明确错误并提示用户先在 `ind.toml` 中计算落库，或删除对应序列。

## 操作细节

### 新增序列

根据当前面板图法给默认值：

- `candles`：`kind = "line"`、`expr = "MA(C, 20)"`
- `line`：`kind = "line"`、`expr = "C"`
- `bar`：
  - 如果没有柱体：`kind = "bar"`、`expr = "VOL"`、`base_value = 0`
  - 如果已有柱体：`kind = "line"`、`expr = "MA(VOL, 5)"`
- `brick`：如果已有序列，禁用新增；否则 `kind = "brick"`、`expr = "BRICK"`

### 删除面板

- 删除主图时必须阻止，除非同时选择另一个面板设为主图。
- 删除最后一个面板时阻止。
- 删除面板会同时删除其下序列和标记。

### 拖拽排序

- 面板排序影响显示顺序。
- 序列排序影响计算引用顺序和默认绘制顺序。
- 如果某个表达式引用了被拖到后面的序列，前端提示“引用顺序可能失效”，最终以后端校验为准。

### 复制

复制面板或序列时自动生成新 key：

- `ma20` -> `ma20_copy`
- 冲突继续追加数字：`ma20_copy2`

复制后 label 后追加 `副本`。

## 实现拆分建议

新增组件：

```text
ui/lianghua_web_feat/src/pages/desktop/components/ChartIndicatorSettingsModal.tsx
ui/lianghua_web_feat/src/pages/desktop/css/ChartIndicatorSettingsModal.css
ui/lianghua_web_feat/src/apis/chartIndicatorSettings.ts
```

`SettingsPage.tsx` 只负责：

- 新增设置列表项
- 控制弹窗开关
- 传入/刷新 `sourcePath`

弹窗组件内部负责：

- 加载配置
- draft 状态
- 结构化/源码模式
- 校验和保存

## 第一版验收标准

- 设置页出现“自定义图表指标”入口。
- 文件不存在时能展示默认配置，并可保存为 `<source_path>/chart_indicators.toml`。
- 能新增/编辑/删除/排序面板、序列、标记。
- 能编辑本文列出的所有当前协议字段。
- 能按 `candles / line / bar / brick` 显示不同可用参数和限制。
- 保存前走后端校验，错误能显示到用户可理解的位置。
- 源码模式能查看、粘贴、校验并保存 TOML。
- 保存后的配置在详情页图表中生效。
