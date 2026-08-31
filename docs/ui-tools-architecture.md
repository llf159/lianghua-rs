# UI 工具应用层架构

原来的 `lianghua-app::ui_tools` 已按业务能力拆成独立 crate。拆分目标不是改变 Tauri API，
而是缩小 Rust 增量编译单元：修改图表、数据管理或实时市场能力时，不再重新编译全部策略代码。

```text
lianghua-app-shared       日期、证券代码和股票元数据
lianghua-app-expression   面向适配层的表达式能力描述
lianghua-app-chart        图表指标及指标配置
lianghua-app-data         托管数据源下载、导入和查看
lianghua-app-market       实时行情和盘中市场观察
lianghua-app-strategy     策略编辑、排名、分析和验证
lianghua-app-facade       组合多个能力的页面级用例
lianghua-app              仅保留兼容重导出
```

## 依赖方向

```text
shared       expression       chart       data
   │              │             │          │
   ├──────────────┴──────┐      │          │
   v                     v      │          │
 market               strategy │          │
   │                     │      │          │
   └──────────┬──────────┴──────┘          │
              v                            │
            facade                         │
              └──────────────┬─────────────┘
                             v
                       lianghua-app
                             v
                       Tauri 适配层
```

图中只表达应用层的主要依赖约束；各 crate 还可以依赖自己需要的后端 crate。

- `shared` 不得依赖其他应用层 crate。
- `market` 只允许依赖 `shared` 与 `expression`，不得依赖 `strategy` 或 `facade`。
- `strategy` 允许依赖 `shared` 与后端回测能力，不得依赖 `market` 或 `facade`。
- 跨 chart、market、strategy 的页面组合统一放在 `facade`。
- `lianghua-app` 不承载实现，只聚合上述 crate 并保持历史 API。
- 通用表达式、持久化、数据提供方、评分和回测逻辑继续属于对应后端 crate。

## 兼容策略

Tauri 当前仍可使用 `lianghua_app::ui_tools::*`。`lianghua-app` 通过重导出保留两类路径：

- 规范嵌套路径，例如 `ui_tools::facade::details`、`ui_tools::strategy::manage`；
- 历史扁平别名，例如 `ui_tools::details`、`ui_tools::strategy_manage`。

新 Rust 调用方应直接依赖拥有该能力的最窄 crate。只有明确进行 API 破坏性升级时，才能删除
兼容别名和聚合 crate。

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
