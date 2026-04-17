# AGENTS.md

## Repo Shape
- `Cargo.toml` at repo root defines a single crate, not a workspace. `src/main.rs` is a stub (`fn main() {}`), so `cargo run` from repo root is not the real app.
- The real app is the nested Tauri project in `ui/lianghua_web_feat/`: frontend in `ui/lianghua_web_feat/src`, Tauri bridge/backend in `ui/lianghua_web_feat/src-tauri/src`.
- Tauri commands are registered in `ui/lianghua_web_feat/src-tauri/src/lib.rs`. Most command bodies delegate into the root crate's `src/ui_tools_feat/*` modules.
- Core Rust boundaries: `src/data` owns the source-directory file contract and DuckDB/CSV access, `src/expr` parses/evaluates rule expressions, `src/scoring` applies score rules, `src/simulate` contains backtest/metric logic, and `src/ui_tools_feat` is the page-oriented facade the Tauri layer calls.
- Frontend routes are wired in `ui/lianghua_web_feat/src/App.tsx`. Tauri calls are wrapped in `ui/lianghua_web_feat/src/apis/*`; pages/components generally do not call `invoke(...)` directly.

## Commands
- Root Rust checks: `cargo check`, `cargo test`
- Run one Rust test: `cargo test <test_name> -- --exact`
- Check the Tauri Rust crate specifically: `cargo check --manifest-path ui/lianghua_web_feat/src-tauri/Cargo.toml`
- Frontend commands run in `ui/lianghua_web_feat/`, not repo root. The root only has a stub `package-lock.json`; there is no root `package.json`.
- Frontend/Tauri commands: `npm run dev`, `npm run lint`, `npm run build`, `npm run tauri dev`
- `ui/lianghua_web_feat/src-tauri/tauri.conf.json` wires Tauri dev/build to Vite: `beforeDevCommand` is `npm run dev`, `beforeBuildCommand` is `npm run build`, and the dev server URL is `http://localhost:5173`.
- No CI workflows, pre-commit config, or Makefile/Justfile were found. Use the commands above directly.

## Data Contract
- Most app features require a selected `source_path` directory. The file contract is defined by `src/data/mod.rs`, `src/ui_tools_feat/data_import.rs`, and `ui/lianghua_web_feat/src/apis/managedSource.ts`.
- Expected files under that directory: `stock_data.db`, `stock_list.csv`, `trade_calendar.csv`, `scoring_result.db`, `concept_performance.db`, `score_rule.toml`, `ind.toml`, `stock_concepts.csv`.
- The Tauri app imports/exports these files under `BaseDirectory::AppData`, usually under the relative dir `source/`.
- The selected absolute source path is persisted in browser local storage key `lh_source_path` (`ui/lianghua_web_feat/src/shared/storage.ts`).
- Do not assume repo-root `score_rule.toml` is the live runtime config. Runtime helpers resolve `score_rule.toml` relative to the chosen `source_path`.

## Testing Notes
- Rust tests are inline unit tests inside module files such as `src/simulate/rule.rs`, `src/ui_tools_feat/statistics.rs`, and `src/data/concept_performance_data.rs`.
- Those tests usually build temporary DuckDB/CSV fixtures themselves, so they do not require a checked-in dataset.
- No frontend test runner is configured; frontend verification is `npm run lint` and `npm run build`.
