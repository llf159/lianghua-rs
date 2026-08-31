# 后端工作空间架构

Rust 后端采用 Cargo 工作空间，并明确规定单向依赖层级。仓库不保留后端或 UI 聚合门面；
每个调用方直接依赖实际拥有所需能力的最窄 crate。

## 依赖层级

```text
lianghua-core          表达式 DSL 与通用工具
lianghua-provider      外部市场数据协议
lianghua-model         共享领域类型与进度契约
        |                    |                    |
        v                    v                    v
lianghua-data    lianghua-download    lianghua-scoring
  存储与领域配置          下载与指标计算          评分计算与规则缓存
       |                    |                    |
       └──────────────┬─────┴──────────────┬─────┘
                      v                    v
             lianghua-backtest    lianghua-app-* 能力 crate
                      |                    |
                      └───────────┬────────┘
                                  v
                             Tauri 适配层

lianghua-rs 根目录二进制 ──> model / data / backtest
```

最后一行表示仓库根目录中的演示与分析二进制同样直接依赖能力 crate，不提供
`lianghua_rs::*` 库级兼容路径。

依赖图必须保持无环，具体约束如下：

- `lianghua-core` 不得依赖存储、网络、Tauri 或 DuckDB。
- `lianghua-provider` 负责实现外部协议，不得打开本地数据库。
- `lianghua-model` 只保存契约和跨组件领域类型，不依赖基础设施。评分行、评分批次、场景阶段和排名策略等共享类型归它所有。
- `lianghua-data` 负责持久化和领域配置，不得依赖 download 或 scoring。评分结果库的建表、批量写入、数据库排名和补排名都位于 `data::scoring_store`。
- `lianghua-download` 和 `lianghua-scoring` 均可依赖 data，但二者不得互相依赖。
- `lianghua-scoring` 负责规则评分、场景计算、评分结果构造和规则缓存，不拥有结果数据库。
- `lianghua-backtest` 直接依赖 data 与 model，不依赖 scoring 的计算实现；底层 crate 不得反向依赖 backtest。
- `lianghua-app-*` crate 各自拥有应用层能力，调用方应依赖最窄的 crate。
- Tauri 直接依赖所需的 `lianghua-app-*`、data、download、provider 等契约所有者。
- Tauri 专属命令、文件系统插件和对话框保留在 `ui/lianghua_web/src-tauri`。

应用层内部的模块边界和兼容策略详见
[`ui-tools-architecture.md`](ui-tools-architecture.md)。

## 直接依赖策略

`lianghua-engine`、`lianghua-app` 和根包 `src/lib.rs` 兼容入口均已移除。后端代码、Tauri
适配层和根目录二进制必须在 `Cargo.toml` 中显式声明真实依赖，并从对应 crate 导入类型。

不得重新创建跨业务域的聚合 crate 来缩短导入路径。实现应放入实际拥有该能力的工作空间
crate；跨域页面组合只允许存在于明确的应用层 facade crate。

## 评分模块边界

原来的 `lianghua-scoring::scoring::scoring_data` 同时包含共享类型、结果构造、DuckDB
持久化和规则缓存，导致调用方为了一个类型或运行时转换也必须依赖 scoring。该混合模块已删除，职责拆分为：

- `lianghua-model::scoring`：评分结果、场景回测行、写入消息与性能记录等共享类型。
- `lianghua-data::data::scoring_store`：评分结果库结构、批量写入、场景排名和同分补排名。
- `lianghua-scoring::scoring::result_build`：把评分序列转换成共享结果类型。
- `lianghua-scoring::scoring::rule_cache`：加载并编译评分规则缓存。
- `lianghua-data::data::runtime`：`RowData` 到表达式运行时的转换。

调用方应直接导入职责所有者，不提供旧路径的兼容导出。

## 验证

在仓库根目录运行：

```bash
cargo fmt \
  --package lianghua-rs \
  --package lianghua-core \
  --package lianghua-provider \
  --package lianghua-model \
  --package lianghua-data \
  --package lianghua-download \
  --package lianghua-scoring \
  --package lianghua-backtest \
  --package lianghua-app-shared \
  --package lianghua-app-chart \
  --package lianghua-app-expression \
  --package lianghua-app-data \
  --package lianghua-app-market \
  --package lianghua-app-strategy \
  --package lianghua-app-facade \
  -- --check
cargo check --all-targets
cargo test --package lianghua-core --package lianghua-provider --package lianghua-model
```

默认工作空间成员覆盖所有后端层和应用能力 crate，不包含具有平台差异的 Tauri 包。项目在 Linux
上有意链接系统 DuckDB 库，因此 Linux CI 对依赖 DuckDB 的 crate 运行 `cargo check`。
不依赖基础设施的层会在 CI 中运行测试二进制；Tauri 工作流继续验证使用内置依赖的 Windows
和 Android 构建。
