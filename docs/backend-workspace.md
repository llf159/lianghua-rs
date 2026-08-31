# Backend workspace architecture

The Rust backend is a Cargo workspace with explicit, one-way dependency layers.
The root `lianghua-rs` package is a compatibility facade for existing binaries
and external callers; new code should depend on the narrowest crate that owns
the required capability.

## Dependency layers

```text
lianghua-core          expression DSL and shared utilities
lianghua-provider      external market-data protocols
lianghua-model         shared market DTOs and progress contracts
        |                    |                    |
        v                    v                    v
lianghua-data    lianghua-download    lianghua-scoring
 storage/domain      ingestion/indicators      rules/ranking
        \___________________|___________________/
                            v
                    lianghua-engine
                   compatibility facade
                            |
                            v
                   lianghua-backtest
                            |
                            v
                      lianghua-app
                            |
                            v
                       Tauri adapter
```

The dependency graph must remain acyclic. In particular:

- `lianghua-core` must not depend on storage, network, Tauri, or DuckDB.
- `lianghua-provider` implements external protocols and must not open local databases.
- `lianghua-model` contains contracts only and has no infrastructure dependency.
- `lianghua-data` owns persistence and domain configuration; it must not depend on
  download or scoring.
- `lianghua-download` may depend on data, while `lianghua-scoring` may depend on
  data; they must not depend on one another.
- `lianghua-engine` is a compatibility facade and contains no implementation.
- `lianghua-backtest` may read engine contracts but the engine must not call it.
- `lianghua-app` is the only backend crate intended for UI adapters.
- Tauri-specific commands, filesystem plugins, and dialogs stay in
  `ui/lianghua_web/src-tauri`.

应用层内部的模块边界和兼容策略详见
[`ui-tools-architecture.md`](ui-tools-architecture.md)。

## Compatibility policy

`src/lib.rs` re-exports the workspace crates under their historical module
paths. This keeps existing CLI binaries and third-party callers compiling while
allowing the Tauri adapter to depend directly on `lianghua-app`.

Do not add new implementation modules to the facade. Add them to the owning
workspace crate and re-export only when compatibility requires it.

## Verification

Run from the repository root:

```bash
cargo fmt \
  --package lianghua-rs \
  --package lianghua-core \
  --package lianghua-provider \
  --package lianghua-model \
  --package lianghua-data \
  --package lianghua-download \
  --package lianghua-scoring \
  --package lianghua-engine \
  --package lianghua-backtest \
  --package lianghua-app \
  -- --check
cargo check --all-targets
cargo test --package lianghua-core --package lianghua-provider --package lianghua-model
```

The default workspace members cover the facade and all backend layers without
requiring the platform-specific Tauri package. Linux CI uses `cargo check` for
DuckDB-dependent crates because the project intentionally links the system
DuckDB library on Linux. Dependency-free layers run their test binaries in CI;
the Tauri workflows continue to verify bundled Windows and Android builds.
