use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use duckdb::Connection;
use serde::{Deserialize, Serialize};

use crate::{
    data::source_db_path,
    ui_tools_feat::chart_indicator::{
        ChartIndicatorConfig, ChartPanelConfig, ChartPanelKind, ChartPanelRole, ChartSeriesKind,
        chart_indicator_config_path, compile_chart_indicator_config,
        default_chart_indicator_config, normalize_chart_indicator_config,
        parse_chart_indicator_config,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartIndicatorSettingsSummary {
    pub panel_count: usize,
    pub series_count: usize,
    pub marker_count: usize,
    pub database_indicator_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartIndicatorSettingsPayload {
    pub source_path: String,
    pub file_path: String,
    pub exists: bool,
    pub text: String,
    pub config: ChartIndicatorConfig,
    pub summary: ChartIndicatorSettingsSummary,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartIndicatorValidationResult {
    pub ok: bool,
    pub error: Option<String>,
    pub config: Option<ChartIndicatorConfig>,
    pub summary: Option<ChartIndicatorSettingsSummary>,
}

pub fn get_chart_indicator_settings(
    source_path: &str,
) -> Result<ChartIndicatorSettingsPayload, String> {
    let source_path = normalize_source_path(source_path)?;
    let config_path = chart_indicator_config_path(&source_path);
    let exists = config_path.exists();
    let text = if exists {
        fs::read_to_string(&config_path).map_err(|error| {
            format!(
                "读取图表指标配置失败: path={}, err={error}",
                config_path.display()
            )
        })?
    } else {
        serialize_chart_indicator_config(&default_chart_indicator_config())?
    };

    match parse_and_compile_chart_indicator_settings(&source_path, &text) {
        Ok((config, summary)) => Ok(ChartIndicatorSettingsPayload {
            source_path,
            file_path: config_path.display().to_string(),
            exists,
            text,
            config,
            summary,
            error: None,
        }),
        Err(error) => {
            let fallback_config = default_chart_indicator_config();
            let summary = summarize_chart_indicator_config(&source_path, &fallback_config)?;
            Ok(ChartIndicatorSettingsPayload {
                source_path,
                file_path: config_path.display().to_string(),
                exists,
                text,
                config: fallback_config,
                summary,
                error: Some(error),
            })
        }
    }
}

pub fn validate_chart_indicator_settings(
    source_path: &str,
    text: &str,
) -> Result<ChartIndicatorValidationResult, String> {
    let source_path = normalize_source_path(source_path)?;
    match parse_and_compile_chart_indicator_settings(&source_path, text) {
        Ok((config, summary)) => Ok(ChartIndicatorValidationResult {
            ok: true,
            error: None,
            config: Some(config),
            summary: Some(summary),
        }),
        Err(error) => Ok(ChartIndicatorValidationResult {
            ok: false,
            error: Some(error),
            config: None,
            summary: None,
        }),
    }
}

pub fn save_chart_indicator_settings(
    source_path: &str,
    text: &str,
) -> Result<ChartIndicatorSettingsPayload, String> {
    let source_path = normalize_source_path(source_path)?;
    parse_and_compile_chart_indicator_settings(&source_path, text)?;

    let config_path = chart_indicator_config_path(&source_path);
    atomic_write_text(&config_path, text)?;
    get_chart_indicator_settings(&source_path)
}

pub fn reset_chart_indicator_settings(
    source_path: &str,
) -> Result<ChartIndicatorSettingsPayload, String> {
    let source_path = normalize_source_path(source_path)?;
    let text = serialize_chart_indicator_config(&default_chart_indicator_config())?;
    let config_path = chart_indicator_config_path(&source_path);
    atomic_write_text(&config_path, &text)?;
    get_chart_indicator_settings(&source_path)
}

fn parse_and_compile_chart_indicator_settings(
    source_path: &str,
    text: &str,
) -> Result<(ChartIndicatorConfig, ChartIndicatorSettingsSummary), String> {
    let config = parse_chart_indicator_config(text)?;
    let db_columns = load_stock_data_columns_if_available(source_path)?;
    compile_chart_indicator_config(&config, db_columns.as_ref())?;
    let summary = summarize_chart_indicator_config_with_columns(source_path, &config, db_columns)?;
    Ok((config, summary))
}

fn summarize_chart_indicator_config(
    source_path: &str,
    config: &ChartIndicatorConfig,
) -> Result<ChartIndicatorSettingsSummary, String> {
    summarize_chart_indicator_config_with_columns(
        source_path,
        config,
        load_stock_data_columns_if_available(source_path)?,
    )
}

fn summarize_chart_indicator_config_with_columns(
    source_path: &str,
    config: &ChartIndicatorConfig,
    db_columns: Option<HashSet<String>>,
) -> Result<ChartIndicatorSettingsSummary, String> {
    let panel_count = config.panels.len();
    let series_count = config
        .panels
        .iter()
        .map(|panel| panel.series.len())
        .sum::<usize>();
    let marker_count = config
        .panels
        .iter()
        .map(|panel| panel.markers.len())
        .sum::<usize>();
    let database_indicator_columns = match db_columns {
        Some(columns) => stock_data_indicator_columns_from_all(columns),
        None => {
            let db_path = source_db_path(source_path);
            if db_path.exists() {
                return Err(format!("读取 stock_data 指标列失败: {}", db_path.display()));
            }
            Vec::new()
        }
    };

    Ok(ChartIndicatorSettingsSummary {
        panel_count,
        series_count,
        marker_count,
        database_indicator_columns,
    })
}

fn serialize_chart_indicator_config(config: &ChartIndicatorConfig) -> Result<String, String> {
    let config = normalize_chart_indicator_config(config);
    let mut lines = vec!["version = 1".to_string(), String::new()];

    for panel in &config.panels {
        lines.push("[[panel]]".to_string());
        lines.push(format!("key = {}", toml_string(&panel.key)?));
        lines.push(format!("label = {}", toml_string(&panel.label)?));
        lines.push(format!(
            "role = {}",
            toml_string(panel_role_name(panel.role))?
        ));
        lines.push(format!(
            "kind = {}",
            toml_string(panel_kind_name(infer_panel_kind(panel)))?
        ));
        lines.push(String::new());

        for series in &panel.series {
            lines.push("[[panel.series]]".to_string());
            lines.push(format!("key = {}", toml_string(&series.key)?));
            if let Some(label) = series
                .label
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                lines.push(format!("label = {}", toml_string(label)?));
            }
            lines.push(format!("expr = {}", toml_string(&series.expr)?));
            lines.push(format!(
                "kind = {}",
                toml_string(series_kind_name(series.kind))?
            ));
            if let Some(color) = series
                .color
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                lines.push(format!("color = {}", toml_string(color)?));
            }
            if let Some(line_width) = series.line_width {
                lines.push(format!("line_width = {}", format_number(line_width)));
            }
            if let Some(opacity) = series.opacity {
                lines.push(format!("opacity = {}", format_number(opacity)));
            }
            if let Some(base_value) = series.base_value {
                lines.push(format!("base_value = {}", format_number(base_value)));
            }
            if !series.color_when.is_empty() {
                lines.push("color_when = [".to_string());
                for rule in &series.color_when {
                    lines.push(format!(
                        "  {{ when = {}, color = {} }},",
                        toml_string(&rule.when)?,
                        toml_string(&rule.color)?
                    ));
                }
                lines.push("]".to_string());
            }
            lines.push(String::new());
        }

        for marker in &panel.markers {
            lines.push("[[panel.marker]]".to_string());
            lines.push(format!("key = {}", toml_string(&marker.key)?));
            if let Some(label) = marker
                .label
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                lines.push(format!("label = {}", toml_string(label)?));
            }
            lines.push(format!("when = {}", toml_string(&marker.when)?));
            if let Some(y) = marker.y.as_deref().filter(|value| !value.trim().is_empty()) {
                lines.push(format!("y = {}", toml_string(y)?));
            }
            if let Some(position) = marker.position {
                lines.push(format!(
                    "position = {}",
                    toml_string(match position {
                        crate::ui_tools_feat::chart_indicator::ChartMarkerPosition::Above =>
                            "above",
                        crate::ui_tools_feat::chart_indicator::ChartMarkerPosition::Below =>
                            "below",
                        crate::ui_tools_feat::chart_indicator::ChartMarkerPosition::Value =>
                            "value",
                    })?
                ));
            }
            if let Some(shape) = marker.shape {
                lines.push(format!(
                    "shape = {}",
                    toml_string(match shape {
                        crate::ui_tools_feat::chart_indicator::ChartMarkerShape::Dot => "dot",
                        crate::ui_tools_feat::chart_indicator::ChartMarkerShape::TriangleUp =>
                            "triangle_up",
                        crate::ui_tools_feat::chart_indicator::ChartMarkerShape::TriangleDown =>
                            "triangle_down",
                        crate::ui_tools_feat::chart_indicator::ChartMarkerShape::Flag => "flag",
                    })?
                ));
            }
            if let Some(color) = marker
                .color
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                lines.push(format!("color = {}", toml_string(color)?));
            }
            if let Some(text) = marker
                .text
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                lines.push(format!("text = {}", toml_string(text)?));
            }
            lines.push(String::new());
        }
    }

    Ok(format!("{}\n", lines.join("\n").trim_end()))
}

fn toml_string(value: &str) -> Result<String, String> {
    serde_json::to_string(value).map_err(|error| format!("序列化图表指标配置失败: {error}"))
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn panel_role_name(role: ChartPanelRole) -> &'static str {
    match role {
        ChartPanelRole::Main => "main",
        ChartPanelRole::Sub => "sub",
    }
}

fn panel_kind_name(kind: ChartPanelKind) -> &'static str {
    match kind {
        ChartPanelKind::Candles => "candles",
        ChartPanelKind::Line => "line",
        ChartPanelKind::Bar => "bar",
        ChartPanelKind::Brick => "brick",
    }
}

fn series_kind_name(kind: ChartSeriesKind) -> &'static str {
    match kind {
        ChartSeriesKind::Line => "line",
        ChartSeriesKind::Bar => "bar",
        ChartSeriesKind::Histogram => "histogram",
        ChartSeriesKind::Area => "area",
        ChartSeriesKind::Band => "band",
        ChartSeriesKind::Brick => "brick",
    }
}

fn infer_panel_kind(panel: &ChartPanelConfig) -> ChartPanelKind {
    if panel.role == ChartPanelRole::Main {
        return ChartPanelKind::Candles;
    }
    if panel
        .series
        .iter()
        .any(|series| series.kind == ChartSeriesKind::Brick)
    {
        return ChartPanelKind::Brick;
    }
    if panel
        .series
        .iter()
        .any(|series| series.kind == ChartSeriesKind::Bar)
    {
        return ChartPanelKind::Bar;
    }
    ChartPanelKind::Line
}

fn normalize_source_path(source_path: &str) -> Result<String, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err("数据源路径不能为空".to_string());
    }
    Ok(trimmed.to_string())
}

fn atomic_write_text(path: &Path, text: &str) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("图表指标配置路径缺少父目录: {}", path.display()))?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("创建图表指标配置目录失败: {}, {error}", parent.display()))?;

    let tmp_path = tmp_path_for(path);
    fs::write(&tmp_path, text).map_err(|error| {
        format!(
            "写入图表指标配置临时文件失败: {}, {error}",
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, path)
        .map_err(|error| format!("保存图表指标配置失败: {}, {error}", path.display()))
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut tmp_path = path.to_path_buf();
    tmp_path.set_extension("toml.tmp");
    tmp_path
}

fn load_stock_data_columns_if_available(
    source_path: &str,
) -> Result<Option<HashSet<String>>, String> {
    let db_path = source_db_path(source_path);
    if !db_path.exists() {
        return Ok(None);
    }

    let conn = Connection::open(&db_path).map_err(|error| {
        format!(
            "打开 stock_data.db 失败: path={}, err={error}",
            db_path.display()
        )
    })?;
    let mut stmt = conn
        .prepare("DESCRIBE stock_data")
        .map_err(|error| format!("预编译 stock_data 列查询失败: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("查询 stock_data 列失败: {error}"))?;

    let mut columns = HashSet::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取 stock_data 列失败: {error}"))?
    {
        let name: String = row
            .get(0)
            .map_err(|error| format!("读取 stock_data 列名失败: {error}"))?;
        columns.insert(name);
    }

    Ok(Some(columns))
}

fn stock_data_indicator_columns_from_all(columns: HashSet<String>) -> Vec<String> {
    const BASE_COLUMNS: &[&str] = &[
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
        "tor",
        "turnover_rate",
        "adj_type",
    ];

    let mut indicator_columns = columns
        .into_iter()
        .filter(|name| {
            !BASE_COLUMNS
                .iter()
                .any(|base| name.eq_ignore_ascii_case(base))
        })
        .collect::<Vec<_>>();
    indicator_columns.sort();
    indicator_columns
}
