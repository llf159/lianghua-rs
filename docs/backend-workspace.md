# Backend workspace architecture

The Rust backend is a Cargo workspace with explicit, one-way dependency layers.
The root `lianghua-rs` package is a compatibility facade for existing binaries
and external callers; new code should depend on the narrowest crate that owns
the required capability.

## Dependency layers

```text
lianghua-core       expression DSL and shared utilities
       |
       +--------------------+
                            |
lianghua-provider           | external market-data protocols
       |                    |
       +--------+-----------+
                v
         lianghua-engine     storage, ingestion, indicators, scoring
                |
                v
       lianghua-backtest     simulations and statistical backtests
                |
                v
          lianghua-app       application-facing use cases
                |
                v
           Tauri adapter
```

The dependency graph must remain acyclic. In particular:

- `lianghua-core` must not depend on storage, network, Tauri, or DuckDB.
- `lianghua-provider` returns provider DTOs and must not open local databases.
- `lianghua-engine` owns the tightly coupled synchronous data/scoring pipeline.
- `lianghua-backtest` may read engine contracts but the engine must not call it.
- `lianghua-app` is the only backend crate intended for UI adapters.
- Tauri-specific commands, filesystem plugins, and dialogs stay in
  `ui/lianghua_web/src-tauri`.

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
  --package lianghua-engine \
  --package lianghua-backtest \
  --package lianghua-app \
  -- --check
cargo check --all-targets
cargo test --package lianghua-core --package lianghua-provider
```

The default workspace members cover the facade and all backend layers without
requiring the platform-specific Tauri package. Linux CI uses `cargo check` for
DuckDB-dependent crates because the project intentionally links the system
DuckDB library on Linux. Dependency-free layers run their test binaries in CI;
the Tauri workflows continue to verify bundled Windows and Android builds.
