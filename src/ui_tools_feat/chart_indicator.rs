use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::UNIX_EPOCH,
};

use serde::{Deserialize, Serialize};

use crate::{
    data::RowData,
    expr::{
        eval::{Runtime, Value},
        parser::{Expr, Parser, Stmt, Stmts, lex_all},
    },
};

pub const CHART_INDICATORS_FILE_NAME: &str = "chart_indicators.toml";

static CHART_INDICATOR_COMPILE_CACHE: OnceLock<
    Mutex<HashMap<ChartIndicatorCacheKey, CompiledChartIndicatorConfig>>,
> = OnceLock::new();

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChartIndicatorConfig {
    pub version: u32,
    #[serde(rename = "panel")]
    pub panels: Vec<ChartPanelConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChartPanelConfig {
    pub key: String,
    pub label: String,
    pub role: ChartPanelRole,
    pub kind: ChartPanelKind,
    pub row_weight: Option<u32>,
    #[serde(default, rename = "series")]
    pub series: Vec<ChartSeriesConfig>,
    #[serde(default, rename = "marker")]
    pub markers: Vec<ChartMarkerConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChartSeriesConfig {
    pub key: String,
    pub label: Option<String>,
    pub expr: String,
    pub kind: ChartSeriesKind,
    pub draw_order: Option<i32>,
    pub color: Option<String>,
    #[serde(default)]
    pub color_when: Vec<ChartColorRule>,
    pub line_width: Option<f64>,
    pub opacity: Option<f64>,
    pub base_value: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChartMarkerConfig {
    pub key: String,
    pub label: Option<String>,
    pub when: String,
    pub y: Option<String>,
    pub position: Option<ChartMarkerPosition>,
    pub shape: Option<ChartMarkerShape>,
    pub color: Option<String>,
    pub text: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChartColorRule {
    pub when: String,
    pub color: String,
}

#[derive(Debug, Clone)]
pub struct CompiledChartIndicatorConfig {
    pub panels: Vec<CompiledChartPanel>,
    pub database_indicator_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CompiledChartPanel {
    pub key: String,
    pub label: String,
    pub role: ChartPanelRole,
    pub kind: ChartPanelKind,
    pub row_weight: Option<u32>,
    pub series: Vec<CompiledChartSeries>,
    pub markers: Vec<CompiledChartMarker>,
}

#[derive(Debug, Clone)]
pub struct CompiledChartSeries {
    pub key: String,
    pub expr: Stmts,
    pub render: ChartSeriesRenderConfig,
    pub color_rules: Vec<CompiledChartColorRule>,
}

#[derive(Debug, Clone)]
pub struct CompiledChartMarker {
    pub key: String,
    pub when_key: String,
    pub when_expr: Stmts,
    pub y_key: Option<String>,
    pub render: ChartMarkerRenderConfig,
}

#[derive(Debug, Clone)]
pub struct CompiledChartColorRule {
    pub when_key: String,
    pub when_expr: Stmts,
    pub color: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChartSeriesRenderConfig {
    pub label: Option<String>,
    pub kind: ChartSeriesKind,
    pub draw_order: Option<i32>,
    pub color: Option<String>,
    pub line_width: Option<f64>,
    pub opacity: Option<f64>,
    pub base_value: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChartMarkerRenderConfig {
    pub label: Option<String>,
    pub position: Option<ChartMarkerPosition>,
    pub shape: Option<ChartMarkerShape>,
    pub color: Option<String>,
    pub text: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ChartIndicatorExecution {
    pub values: HashMap<String, Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChartIndicatorCacheKey {
    config_path: PathBuf,
    file_stamp: ChartIndicatorFileStamp,
    db_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ChartIndicatorFileStamp {
    Missing,
    File { len: u64, modified_millis: u128 },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChartPanelRole {
    Main,
    Sub,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChartPanelKind {
    Candles,
    Line,
    Bar,
    Brick,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChartSeriesKind {
    Line,
    Bar,
    Histogram,
    Area,
    Band,
    Brick,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChartMarkerPosition {
    Above,
    Below,
    Value,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChartMarkerShape {
    Dot,
    TriangleUp,
    TriangleDown,
    Flag,
}

pub fn chart_indicator_config_path(source_path: impl AsRef<Path>) -> PathBuf {
    source_path.as_ref().join(CHART_INDICATORS_FILE_NAME)
}

pub fn default_chart_indicator_config() -> ChartIndicatorConfig {
    ChartIndicatorConfig {
        version: 1,
        panels: vec![ChartPanelConfig {
            key: "price".to_string(),
            label: "主K".to_string(),
            role: ChartPanelRole::Main,
            kind: ChartPanelKind::Candles,
            row_weight: Some(46),
            series: Vec::new(),
            markers: Vec::new(),
        }],
    }
}

pub fn parse_chart_indicator_config(text: &str) -> Result<ChartIndicatorConfig, String> {
    let config: ChartIndicatorConfig =
        toml::from_str(text).map_err(|error| format!("parse chart indicators failed: {error}"))?;
    validate_chart_indicator_config(&config)?;
    Ok(config)
}

pub fn load_chart_indicator_config(
    source_path: impl AsRef<Path>,
) -> Result<ChartIndicatorConfig, String> {
    let path = chart_indicator_config_path(source_path);
    if !path.exists() {
        let config = default_chart_indicator_config();
        validate_chart_indicator_config(&config)?;
        return Ok(config);
    }
    if !path.is_file() {
        return Err(format!(
            "chart indicator config path is not a file: {}",
            path.display()
        ));
    }

    let text = fs::read_to_string(&path).map_err(|error| {
        format!(
            "read chart indicator config failed: {}, {error}",
            path.display()
        )
    })?;
    parse_chart_indicator_config(&text)
}

pub fn load_compiled_chart_indicator_config(
    source_path: impl AsRef<Path>,
    available_db_columns: Option<&HashSet<String>>,
) -> Result<CompiledChartIndicatorConfig, String> {
    let source_path = source_path.as_ref();
    let config_path = chart_indicator_config_path(source_path);
    let cache_key = ChartIndicatorCacheKey {
        file_stamp: chart_indicator_file_stamp(&config_path)?,
        config_path,
        db_columns: available_db_columns.map(sorted_db_columns_cache_key),
    };

    let cache = CHART_INDICATOR_COMPILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(compiled) = cache
        .lock()
        .map_err(|_| "chart indicator compile cache poisoned".to_string())?
        .get(&cache_key)
        .cloned()
    {
        return Ok(compiled);
    }

    let config = load_chart_indicator_config(source_path)?;
    let compiled = compile_chart_indicator_config(&config, available_db_columns)?;
    cache
        .lock()
        .map_err(|_| "chart indicator compile cache poisoned".to_string())?
        .insert(cache_key, compiled.clone());

    Ok(compiled)
}

pub fn validate_chart_indicator_config(config: &ChartIndicatorConfig) -> Result<(), String> {
    if config.version != 1 {
        return Err(format!(
            "unsupported chart indicator config version: {}",
            config.version
        ));
    }

    let mut panel_keys = HashSet::new();
    let mut series_keys = HashSet::new();
    let mut main_count = 0usize;

    for panel in &config.panels {
        validate_key("panel.key", &panel.key)?;
        if !panel_keys.insert(panel.key.as_str()) {
            return Err(format!("duplicate panel.key: {}", panel.key));
        }
        if panel.role == ChartPanelRole::Main {
            main_count += 1;
            if panel.kind != ChartPanelKind::Candles {
                return Err(format!("main panel must use candles kind: {}", panel.key));
            }
        }

        let mut panel_marker_keys = HashSet::new();
        for series in &panel.series {
            validate_key("series.key", &series.key)?;
            if !series_keys.insert(series.key.as_str()) {
                return Err(format!("duplicate series.key: {}", series.key));
            }
            if let Some(color) = series.color.as_deref() {
                validate_color(
                    &format!("panel.{}.series.{}.color", panel.key, series.key),
                    color,
                )?;
            }
            for (index, rule) in series.color_when.iter().enumerate() {
                validate_color(
                    &format!(
                        "panel.{}.series.{}.color_when[{}].color",
                        panel.key, series.key, index
                    ),
                    &rule.color,
                )?;
            }
        }

        for marker in &panel.markers {
            validate_key("marker.key", &marker.key)?;
            if !panel_marker_keys.insert(marker.key.as_str()) {
                return Err(format!(
                    "duplicate marker.key in panel {}: {}",
                    panel.key, marker.key
                ));
            }
            if let Some(color) = marker.color.as_deref() {
                validate_color(
                    &format!("panel.{}.marker.{}.color", panel.key, marker.key),
                    color,
                )?;
            }
        }

        validate_panel_series_combination(panel)?;
    }

    if main_count != 1 {
        return Err(format!(
            "chart indicator config must contain exactly one main panel, got {main_count}"
        ));
    }

    Ok(())
}

pub fn compile_chart_indicator_config(
    config: &ChartIndicatorConfig,
    available_db_columns: Option<&HashSet<String>>,
) -> Result<CompiledChartIndicatorConfig, String> {
    validate_chart_indicator_config(config)?;

    let db_column_lookup = available_db_columns.map(build_db_column_lookup);
    let all_series_keys = collect_all_series_keys(config);
    let mut available_series_keys = HashSet::new();
    let mut database_indicator_columns = HashSet::new();
    let mut compiled_panels = Vec::with_capacity(config.panels.len());

    for panel in &config.panels {
        let mut compiled_series = Vec::with_capacity(panel.series.len());
        for series in &panel.series {
            let expr = compile_expression(
                &series.expr,
                &format!("panel.{}.series.{}.expr", panel.key, series.key),
            )?;
            collect_database_dependencies(
                &expr,
                &format!("panel.{}.series.{}.expr", panel.key, series.key),
                &all_series_keys,
                &available_series_keys,
                db_column_lookup.as_ref(),
                &mut database_indicator_columns,
            )?;

            let series_key = normalize_identifier(&series.key);
            available_series_keys.insert(series_key);

            let mut compiled_color_rules = Vec::with_capacity(series.color_when.len());
            for (index, rule) in series.color_when.iter().enumerate() {
                let when_expr = compile_expression(
                    &rule.when,
                    &format!(
                        "panel.{}.series.{}.color_when[{}].when",
                        panel.key, series.key, index
                    ),
                )?;
                collect_database_dependencies(
                    &when_expr,
                    &format!(
                        "panel.{}.series.{}.color_when[{}].when",
                        panel.key, series.key, index
                    ),
                    &all_series_keys,
                    &available_series_keys,
                    db_column_lookup.as_ref(),
                    &mut database_indicator_columns,
                )?;
                compiled_color_rules.push(CompiledChartColorRule {
                    when_key: format!("__color_{}_{}_{}", panel.key, series.key, index),
                    when_expr,
                    color: rule.color.clone(),
                });
            }

            compiled_series.push(CompiledChartSeries {
                key: series.key.clone(),
                expr,
                render: ChartSeriesRenderConfig {
                    label: series.label.clone(),
                    kind: series.kind,
                    draw_order: series.draw_order,
                    color: series.color.clone(),
                    line_width: series.line_width,
                    opacity: series.opacity,
                    base_value: series.base_value,
                },
                color_rules: compiled_color_rules,
            });
        }

        let mut compiled_markers = Vec::with_capacity(panel.markers.len());
        for marker in &panel.markers {
            let when_expr = compile_expression(
                &marker.when,
                &format!("panel.{}.marker.{}.when", panel.key, marker.key),
            )?;
            collect_database_dependencies(
                &when_expr,
                &format!("panel.{}.marker.{}.when", panel.key, marker.key),
                &all_series_keys,
                &available_series_keys,
                db_column_lookup.as_ref(),
                &mut database_indicator_columns,
            )?;
            if let Some(y_key) = marker.y.as_deref() {
                collect_marker_y_dependency(
                    y_key,
                    &format!("panel.{}.marker.{}.y", panel.key, marker.key),
                    &all_series_keys,
                    &available_series_keys,
                    db_column_lookup.as_ref(),
                    &mut database_indicator_columns,
                )?;
            }
            compiled_markers.push(CompiledChartMarker {
                key: marker.key.clone(),
                when_key: format!("__marker_{}_{}", panel.key, marker.key),
                when_expr,
                y_key: marker.y.clone(),
                render: ChartMarkerRenderConfig {
                    label: marker.label.clone(),
                    position: marker.position,
                    shape: marker.shape,
                    color: marker.color.clone(),
                    text: marker.text.clone(),
                },
            });
        }

        compiled_panels.push(CompiledChartPanel {
            key: panel.key.clone(),
            label: panel.label.clone(),
            role: panel.role,
            kind: panel.kind,
            row_weight: panel.row_weight,
            series: compiled_series,
            markers: compiled_markers,
        });
    }

    let mut database_indicator_columns = database_indicator_columns.into_iter().collect::<Vec<_>>();
    database_indicator_columns.sort();

    Ok(CompiledChartIndicatorConfig {
        panels: compiled_panels,
        database_indicator_columns,
    })
}

pub fn execute_chart_indicator_config(
    compiled: &CompiledChartIndicatorConfig,
    row_data: RowData,
) -> Result<ChartIndicatorExecution, String> {
    let series_len = row_data.trade_dates.len();
    let mut runtime = row_data_into_chart_runtime(row_data)?;
    let mut values = HashMap::new();

    for panel in &compiled.panels {
        for series in &panel.series {
            let value = runtime.eval_program(&series.expr).map_err(|error| {
                format!("chart series {} compute failed: {}", series.key, error.msg)
            })?;
            let num_series = Value::as_num_series(&value, series_len).map_err(|error| {
                format!(
                    "chart series {} result is not numeric series: {}",
                    series.key, error.msg
                )
            })?;
            values.insert(series.key.clone(), num_series_to_json_values(&num_series));
            insert_runtime_series_aliases(&mut runtime, &series.key, num_series);

            for rule in &series.color_rules {
                let value = runtime.eval_program(&rule.when_expr).map_err(|error| {
                    format!(
                        "chart color rule {} compute failed: {}",
                        rule.when_key, error.msg
                    )
                })?;
                let bool_series = Value::as_bool_series(&value, series_len).map_err(|error| {
                    format!(
                        "chart color rule {} result is not bool series: {}",
                        rule.when_key, error.msg
                    )
                })?;
                values.insert(
                    rule.when_key.clone(),
                    bool_series_to_json_values(&bool_series),
                );
            }
        }

        for marker in &panel.markers {
            let value = runtime.eval_program(&marker.when_expr).map_err(|error| {
                format!(
                    "chart marker {} compute failed: {}",
                    marker.when_key, error.msg
                )
            })?;
            let bool_series = Value::as_bool_series(&value, series_len).map_err(|error| {
                format!(
                    "chart marker {} result is not bool series: {}",
                    marker.when_key, error.msg
                )
            })?;
            values.insert(
                marker.when_key.clone(),
                bool_series_to_json_values(&bool_series),
            );

            if let Some(y_key) = marker.y_key.as_deref() {
                expose_marker_y_values(y_key, &runtime, series_len, &mut values)?;
            }
        }
    }

    Ok(ChartIndicatorExecution { values })
}

fn row_data_into_chart_runtime(row_data: RowData) -> Result<Runtime, String> {
    row_data.validate()?;
    let mut runtime = Runtime::default();

    for (name, series) in row_data.cols {
        insert_runtime_series_aliases(&mut runtime, &name, series);
    }

    insert_existing_runtime_alias(&mut runtime, "O", "OPEN");
    insert_existing_runtime_alias(&mut runtime, "H", "HIGH");
    insert_existing_runtime_alias(&mut runtime, "L", "LOW");
    insert_existing_runtime_alias(&mut runtime, "C", "CLOSE");
    insert_existing_runtime_alias(&mut runtime, "V", "VOL");
    insert_existing_runtime_alias(&mut runtime, "TOR", "TURNOVER_RATE");

    Ok(runtime)
}

fn insert_runtime_series_aliases(runtime: &mut Runtime, key: &str, series: Vec<Option<f64>>) {
    runtime
        .vars
        .insert(key.to_string(), Value::NumSeries(series.clone()));

    let normalized = normalize_identifier(key);
    runtime.vars.insert(normalized, Value::NumSeries(series));
}

fn insert_existing_runtime_alias(runtime: &mut Runtime, from: &str, to: &str) {
    if let Some(value) = runtime.vars.get(from).cloned() {
        runtime.vars.entry(to.to_string()).or_insert(value);
    }
    if let Some(value) = runtime.vars.get(to).cloned() {
        runtime.vars.entry(from.to_string()).or_insert(value);
    }
}

fn num_series_to_json_values(series: &[Option<f64>]) -> Vec<serde_json::Value> {
    series
        .iter()
        .map(|value| match value {
            Some(value) if value.is_finite() => serde_json::json!(value),
            _ => serde_json::Value::Null,
        })
        .collect()
}

fn bool_series_to_json_values(series: &[bool]) -> Vec<serde_json::Value> {
    series
        .iter()
        .map(|value| serde_json::json!(value))
        .collect()
}

fn compile_expression(expr: &str, path: &str) -> Result<Stmts, String> {
    let tokens = lex_all(expr);
    let mut parser = Parser::new(tokens);
    let stmts = parser
        .parse_main()
        .map_err(|error| format!("{path} parse failed at {}: {}", error.idx, error.msg))?;
    validate_expression_functions(&stmts, path)?;
    Ok(stmts)
}

fn build_db_column_lookup(columns: &HashSet<String>) -> HashMap<String, String> {
    columns
        .iter()
        .map(|column| (normalize_identifier(column), column.clone()))
        .collect()
}

fn chart_indicator_file_stamp(path: &Path) -> Result<ChartIndicatorFileStamp, String> {
    if !path.exists() {
        return Ok(ChartIndicatorFileStamp::Missing);
    }
    if !path.is_file() {
        return Err(format!(
            "chart indicator config path is not a file: {}",
            path.display()
        ));
    }

    let metadata = fs::metadata(path).map_err(|error| {
        format!(
            "read chart indicator config metadata failed: {}, {error}",
            path.display()
        )
    })?;
    let modified_millis = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis())
        .unwrap_or(0);

    Ok(ChartIndicatorFileStamp::File {
        len: metadata.len(),
        modified_millis,
    })
}

fn sorted_db_columns_cache_key(columns: &HashSet<String>) -> Vec<String> {
    let mut columns = columns
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>();
    columns.sort();
    columns
}

fn collect_all_series_keys(config: &ChartIndicatorConfig) -> HashSet<String> {
    config
        .panels
        .iter()
        .flat_map(|panel| panel.series.iter())
        .map(|series| normalize_identifier(&series.key))
        .collect()
}

fn collect_database_dependencies(
    stmts: &Stmts,
    path: &str,
    all_series_keys: &HashSet<String>,
    available_series_keys: &HashSet<String>,
    db_column_lookup: Option<&HashMap<String, String>>,
    database_indicator_columns: &mut HashSet<String>,
) -> Result<(), String> {
    let identifiers = collect_program_identifiers(stmts);
    for identifier in identifiers {
        let normalized = normalize_identifier(&identifier);
        if is_base_runtime_key(&normalized) {
            continue;
        }
        if available_series_keys.contains(&normalized) {
            continue;
        }
        if let Some(db_column_lookup) = db_column_lookup {
            if let Some(column) = db_column_lookup.get(&normalized) {
                database_indicator_columns.insert(column.clone());
                continue;
            }
            if all_series_keys.contains(&normalized) {
                return Err(format!(
                    "{path} references series `{identifier}` before it is declared"
                ));
            }
            return Err(format!(
                "{path} references unknown database indicator column `{identifier}`"
            ));
        }
        database_indicator_columns.insert(identifier);
    }

    Ok(())
}

fn collect_marker_y_dependency(
    y_key: &str,
    path: &str,
    all_series_keys: &HashSet<String>,
    available_series_keys: &HashSet<String>,
    db_column_lookup: Option<&HashMap<String, String>>,
    database_indicator_columns: &mut HashSet<String>,
) -> Result<(), String> {
    validate_key(path, y_key)?;
    let normalized = normalize_identifier(y_key);
    if is_base_runtime_key(&normalized) || available_series_keys.contains(&normalized) {
        return Ok(());
    }
    if let Some(db_column_lookup) = db_column_lookup {
        if let Some(column) = db_column_lookup.get(&normalized) {
            database_indicator_columns.insert(column.clone());
            return Ok(());
        }
        if all_series_keys.contains(&normalized) {
            return Err(format!(
                "{path} references series `{y_key}` before it is declared"
            ));
        }
        return Err(format!(
            "{path} references unknown database indicator column `{y_key}`"
        ));
    }

    database_indicator_columns.insert(y_key.to_string());
    Ok(())
}

fn expose_marker_y_values(
    y_key: &str,
    runtime: &Runtime,
    series_len: usize,
    values: &mut HashMap<String, Vec<serde_json::Value>>,
) -> Result<(), String> {
    if is_base_runtime_key(&normalize_identifier(y_key)) || values.contains_key(y_key) {
        return Ok(());
    }

    let value = runtime
        .vars
        .get(y_key)
        .or_else(|| runtime.vars.get(&normalize_identifier(y_key)))
        .cloned();
    let Some(value) = value else {
        return Ok(());
    };
    let num_series = Value::as_num_series(&value, series_len).map_err(|error| {
        format!(
            "chart marker y `{y_key}` is not numeric series: {}",
            error.msg
        )
    })?;
    values.insert(y_key.to_string(), num_series_to_json_values(&num_series));
    Ok(())
}

fn collect_program_identifiers(stmts: &Stmts) -> HashSet<String> {
    let mut locals = HashSet::new();
    let mut identifiers = HashSet::new();

    for stmt in &stmts.item {
        match stmt {
            Stmt::Assign { name, value } => {
                collect_expr_identifiers(value, &locals, &mut identifiers);
                locals.insert(name.clone());
            }
            Stmt::Expr(expr) => collect_expr_identifiers(expr, &locals, &mut identifiers),
        }
    }

    identifiers
}

fn collect_expr_identifiers(
    expr: &Expr,
    locals: &HashSet<String>,
    identifiers: &mut HashSet<String>,
) {
    match expr {
        Expr::Number(_) => {}
        Expr::Ident(name) => {
            if !locals.contains(name) {
                identifiers.insert(name.clone());
            }
        }
        Expr::Call { args, .. } => {
            for arg in args {
                collect_expr_identifiers(arg, locals, identifiers);
            }
        }
        Expr::Unary { rhs, .. } => collect_expr_identifiers(rhs, locals, identifiers),
        Expr::Binary { lhs, rhs, .. } => {
            collect_expr_identifiers(lhs, locals, identifiers);
            collect_expr_identifiers(rhs, locals, identifiers);
        }
    }
}

fn validate_expression_functions(stmts: &Stmts, path: &str) -> Result<(), String> {
    for stmt in &stmts.item {
        match stmt {
            Stmt::Assign { value, .. } => validate_expr_functions(value, path)?,
            Stmt::Expr(expr) => validate_expr_functions(expr, path)?,
        }
    }
    Ok(())
}

fn validate_expr_functions(expr: &Expr, path: &str) -> Result<(), String> {
    match expr {
        Expr::Number(_) | Expr::Ident(_) => Ok(()),
        Expr::Call { name, args } => {
            let normalized = normalize_identifier(name);
            if !is_expression_function(&normalized) {
                return Err(format!("{path} references unknown function `{name}`"));
            }
            for arg in args {
                validate_expr_functions(arg, path)?;
            }
            Ok(())
        }
        Expr::Unary { rhs, .. } => validate_expr_functions(rhs, path),
        Expr::Binary { lhs, rhs, .. } => {
            validate_expr_functions(lhs, path)?;
            validate_expr_functions(rhs, path)
        }
    }
}

fn normalize_identifier(identifier: &str) -> String {
    identifier.trim().to_ascii_uppercase()
}

fn is_expression_function(name: &str) -> bool {
    matches!(
        name,
        "ABS"
            | "MAX"
            | "MIN"
            | "DIV"
            | "HHV"
            | "LLV"
            | "COUNT"
            | "MA"
            | "REF"
            | "LAST"
            | "SUM"
            | "STD"
            | "IF"
            | "CROSS"
            | "EMA"
            | "SMA"
            | "BARSLAST"
            | "RSV"
            | "GRANK"
            | "GTOPCOUNT"
            | "LTOPCOUNT"
            | "LRANK"
            | "GET"
    )
}

fn is_base_runtime_key(key: &str) -> bool {
    matches!(
        key,
        "O" | "H"
            | "L"
            | "C"
            | "V"
            | "OPEN"
            | "HIGH"
            | "LOW"
            | "CLOSE"
            | "VOL"
            | "AMOUNT"
            | "PRE_CLOSE"
            | "CHANGE"
            | "PCT_CHG"
            | "TOR"
            | "TURNOVER_RATE"
    )
}

fn validate_panel_series_combination(panel: &ChartPanelConfig) -> Result<(), String> {
    match panel.kind {
        ChartPanelKind::Candles => {
            for series in &panel.series {
                if series.kind != ChartSeriesKind::Line {
                    return Err(format!(
                        "candles panel {} only supports line series in phase one: {}",
                        panel.key, series.key
                    ));
                }
            }
        }
        ChartPanelKind::Line => {
            for series in &panel.series {
                if series.kind != ChartSeriesKind::Line {
                    return Err(format!(
                        "line panel {} only supports line series in phase one: {:?} {}",
                        panel.key, series.kind, series.key
                    ));
                }
            }
        }
        ChartPanelKind::Bar => {
            let bar_count = panel
                .series
                .iter()
                .filter(|series| series.kind == ChartSeriesKind::Bar)
                .count();
            if bar_count > 1 {
                return Err(format!(
                    "bar panel {} supports at most one bar series in phase one",
                    panel.key
                ));
            }
            for series in &panel.series {
                if !matches!(series.kind, ChartSeriesKind::Bar | ChartSeriesKind::Line) {
                    return Err(format!(
                        "bar panel {} does not support {:?} series: {}",
                        panel.key, series.kind, series.key
                    ));
                }
            }
        }
        ChartPanelKind::Brick => {
            if panel.series.len() != 1 {
                return Err(format!(
                    "brick panel {} must contain exactly one brick series",
                    panel.key
                ));
            }
            for series in &panel.series {
                if series.kind != ChartSeriesKind::Brick {
                    return Err(format!(
                        "brick panel {} only supports brick series: {}",
                        panel.key, series.key
                    ));
                }
            }
        }
    }

    Ok(())
}

fn validate_key(path: &str, key: &str) -> Result<(), String> {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return Err(format!("{path} cannot be empty"));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(format!("{path} must match [A-Za-z_][A-Za-z0-9_]*: {key}"));
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(format!("{path} must match [A-Za-z_][A-Za-z0-9_]*: {key}"));
    }
    Ok(())
}

fn validate_color(path: &str, color: &str) -> Result<(), String> {
    let bytes = color.as_bytes();
    if bytes.len() != 7 || bytes.first() != Some(&b'#') {
        return Err(format!("{path} must be a #RRGGBB color: {color}"));
    }
    if !bytes[1..].iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("{path} must be a #RRGGBB color: {color}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[test]
    fn default_config_validates_and_covers_existing_panels() {
        let config = default_chart_indicator_config();

        validate_chart_indicator_config(&config).expect("default config should validate");

        assert_eq!(config.panels.len(), 1);
        assert_eq!(config.panels[0].key, "price");
        assert_eq!(config.panels[0].role, ChartPanelRole::Main);
        assert_eq!(config.panels[0].kind, ChartPanelKind::Candles);
        assert!(config.panels[0].series.is_empty());
    }

    #[test]
    fn example_config_matches_builtin_default_config() {
        let example = r##"
version = 1

[[panel]]
key = "price"
label = "主K"
role = "main"
kind = "candles"
"##;

        let config = parse_chart_indicator_config(example).expect("example config should parse");

        assert_eq!(config, default_chart_indicator_config());
    }

    #[test]
    fn default_config_compiles_and_collects_database_dependencies() {
        let compiled = compile_chart_indicator_config(
            &default_chart_indicator_config(),
            Some(&HashSet::new()),
        )
        .expect("default config should compile");

        assert!(compiled.database_indicator_columns.is_empty());
        assert_eq!(compiled.panels.len(), 1);
        assert!(compiled.panels[0].series.is_empty());
    }

    #[test]
    fn later_series_can_reference_previous_series_without_database_dependency() {
        let config = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "ma5"
expr = "MA(C, 5)"
kind = "line"

[[panel.series]]
key = "ma5_delta"
expr = "C - ma5"
kind = "line"
"##,
        )
        .expect("config should parse");

        let compiled = compile_chart_indicator_config(&config, Some(&HashSet::new()))
            .expect("previous series reference should compile");

        assert!(compiled.database_indicator_columns.is_empty());
    }

    #[test]
    fn future_series_reference_is_rejected_when_not_a_database_column() {
        let config = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "ma10_delta"
expr = "C - ma10"
kind = "line"

[[panel.series]]
key = "ma10"
expr = "MA(C, 10)"
kind = "line"
"##,
        )
        .expect("config should parse");

        let error = compile_chart_indicator_config(&config, Some(&HashSet::new()))
            .expect_err("future series reference should fail");

        assert!(error.contains("panel.price.series.ma10_delta.expr"));
        assert!(error.contains("before it is declared"));
    }

    #[test]
    fn unknown_database_dependency_is_rejected_with_path() {
        let config = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "mystery"
expr = "MYSTERY_COL"
kind = "line"
"##,
        )
        .expect("config should parse");

        let error = compile_chart_indicator_config(&config, Some(&HashSet::new()))
            .expect_err("unknown db column should fail");

        assert!(error.contains("panel.price.series.mystery.expr"));
        assert!(error.contains("MYSTERY_COL"));
    }

    #[test]
    fn marker_y_collects_and_exposes_database_dependency() {
        let config = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.marker]]
key = "flag_j"
when = "CROSS(C, J)"
y = "j"
shape = "flag"
"##,
        )
        .expect("config should parse");
        let compiled = compile_chart_indicator_config(&config, Some(&hash_set(["J"])))
            .expect("marker y dependency should compile");

        assert_eq!(compiled.database_indicator_columns, vec!["J".to_string()]);

        let execution = execute_chart_indicator_config(
            &compiled,
            RowData {
                trade_dates: vec!["20240101".to_string(), "20240102".to_string()],
                cols: HashMap::from([
                    ("C".to_string(), vec![Some(9.0), Some(11.0)]),
                    ("J".to_string(), vec![Some(10.0), Some(10.0)]),
                ]),
            },
        )
        .expect("marker should execute");

        assert_eq!(
            execution.values.get("j"),
            Some(&vec![serde_json::json!(10.0), serde_json::json!(10.0)])
        );
        assert_eq!(
            execution.values.get("__marker_price_flag_j"),
            Some(&vec![serde_json::json!(false), serde_json::json!(true)])
        );
    }

    #[test]
    fn parse_errors_include_config_path() {
        let config = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "bad"
expr = "MA(C,"
kind = "line"
"##,
        )
        .expect("config structure should parse");

        let error = compile_chart_indicator_config(&config, Some(&HashSet::new()))
            .expect_err("bad expression should fail");

        assert!(error.contains("panel.price.series.bad.expr"));
        assert!(error.contains("parse failed"));
    }

    #[test]
    fn unknown_expression_function_is_rejected() {
        let config = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "bad"
expr = "NOT_A_FUNCTION(C)"
kind = "line"
"##,
        )
        .expect("config structure should parse");

        let error = compile_chart_indicator_config(&config, Some(&HashSet::new()))
            .expect_err("unknown function should fail");

        assert!(error.contains("panel.price.series.bad.expr"));
        assert!(error.contains("unknown function"));
    }

    #[test]
    fn missing_external_file_falls_back_to_default_config() {
        let temp_dir = unique_temp_dir("chart_indicator_missing");
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");

        let config = load_chart_indicator_config(&temp_dir).expect("default should load");

        assert_eq!(config, default_chart_indicator_config());
        fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn external_file_takes_priority_over_default_config() {
        let temp_dir = unique_temp_dir("chart_indicator_external");
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        fs::write(
            temp_dir.join(CHART_INDICATORS_FILE_NAME),
            r##"
version = 1

[[panel]]
key = "main"
label = "Main"
role = "main"
kind = "candles"

[[panel.series]]
key = "ma5"
expr = "MA(C, 5)"
kind = "line"
color = "#123abc"
"##,
        )
        .expect("config should be written");

        let config = load_chart_indicator_config(&temp_dir).expect("external config should load");

        assert_eq!(config.panels.len(), 1);
        assert_eq!(config.panels[0].key, "main");
        assert_eq!(config.panels[0].series[0].key, "ma5");
        fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn duplicate_panel_keys_are_rejected() {
        let error = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel]]
key = "price"
label = "Other"
role = "sub"
kind = "line"
"##,
        )
        .expect_err("duplicate panel key should fail");

        assert!(error.contains("duplicate panel.key"));
    }

    #[test]
    fn duplicate_series_keys_are_rejected_globally() {
        let error = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "ma"
expr = "MA(C, 5)"
kind = "line"

[[panel]]
key = "indicator"
label = "Indicator"
role = "sub"
kind = "line"

[[panel.series]]
key = "ma"
expr = "MA(C, 10)"
kind = "line"
"##,
        )
        .expect_err("duplicate series key should fail");

        assert!(error.contains("duplicate series.key"));
    }

    #[test]
    fn exactly_one_main_panel_is_required() {
        let error = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "a"
label = "A"
role = "sub"
kind = "line"
"##,
        )
        .expect_err("missing main panel should fail");

        assert!(error.contains("exactly one main panel"));
    }

    #[test]
    fn invalid_color_is_rejected() {
        let error = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "ma"
expr = "MA(C, 5)"
kind = "line"
color = "red"
"##,
        )
        .expect_err("invalid color should fail");

        assert!(error.contains("#RRGGBB"));
    }

    #[test]
    fn panel_series_combination_is_checked() {
        let error = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "vol"
expr = "VOL"
kind = "bar"
"##,
        )
        .expect_err("invalid candles series should fail");

        assert!(error.contains("candles panel"));

        let error = parse_chart_indicator_config(
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel]]
key = "indicator"
label = "Indicator"
role = "sub"
kind = "line"

[[panel.series]]
key = "area"
expr = "C"
kind = "area"
"##,
        )
        .expect_err("area series should be rejected in phase one");

        assert!(error.contains("line panel"));
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{nanos}"))
    }

    fn hash_set<const N: usize>(items: [&str; N]) -> HashSet<String> {
        items.into_iter().map(str::to_string).collect()
    }
}
