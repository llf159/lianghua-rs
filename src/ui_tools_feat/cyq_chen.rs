use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Local;
use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::{
    data::{
        DataReader, RowData, chip_change_rule_path,
        cyq_chen::{
            ChenChipConfig, ChenChipSnapshot, ChipChangeConfig, ChipChangeStrategy,
            collect_chen_chip_runtime_keys, compute_chen_chip_snapshots_with_compiled_config,
            round_chen_chip_snapshot,
        },
        load_trade_date_list, source_db_path,
    },
    scoring::tools::{inject_stock_extra_fields, load_st_list, load_total_share_map},
    ui_tools_feat::{details::DetailKlinePayload, watch_observe::normalize_ts_code},
};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const DEFAULT_VISIBLE_KLINE_WINDOW_DAYS: usize = 90;
const CHIP_CHANGE_BACKUP_DIR_NAME: &str = "chip_change_rule_backups";
const CHIP_CHANGE_META_SUFFIX: &str = ".meta.json";
const EMPTY_CHIP_CHANGE_TEMPLATE: &str = "version = 1\n";

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenSingleStockRequest {
    pub source_path: String,
    pub ts_code: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub warmup_days: usize,
    pub bucket_pct: f64,
    pub strategies: Vec<ChipChangeStrategy>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenKlineRow {
    pub trade_date: String,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub turnover_rate: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenSingleStockData {
    pub resolved_ts_code: String,
    pub start_date: String,
    pub end_date: String,
    pub output_start_date: Option<String>,
    pub kline: Vec<CyqChenKlineRow>,
    pub kline_payload: Option<DetailKlinePayload>,
    pub snapshots: Vec<ChenChipSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenStrategyPageData {
    pub file_path: String,
    pub exists: bool,
    pub strategies: Vec<ChipChangeStrategy>,
    pub backups: Vec<CyqChenStrategyBackupItem>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenStrategyBackupItem {
    pub backup_id: String,
    pub file_name: String,
    pub file_path: String,
    pub created_at: Option<String>,
    pub modified_at: Option<String>,
    pub size_bytes: u64,
    pub source_kind: String,
    pub source_file_name: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenStrategyFileExportResult {
    pub exported_path: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenStrategyBackupDiffLine {
    pub kind: String,
    pub backup_line: Option<usize>,
    pub active_line: Option<usize>,
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenStrategyBackupDiff {
    pub backup_id: String,
    pub backup_label: String,
    pub active_label: String,
    pub changed_line_count: usize,
    pub lines: Vec<CyqChenStrategyBackupDiffLine>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenStrategyFileDraft {
    pub strategies: Vec<ChipChangeStrategy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CyqChenStrategyBackupMeta {
    version: u32,
    created_at: String,
    source_kind: String,
    source_file_name: Option<String>,
    description: Option<String>,
}

fn default_chip_change_strategies() -> Vec<ChipChangeStrategy> {
    toml::from_str::<ChipChangeConfig>(
        r#"
version = 1

[[strategy]]
name = "主力低位承接"
holder = "main"
direction = "buy"
when = "RATEL < -8 AND C > O"
bias = 1.5

[[strategy]]
name = "散户追高买入"
holder = "retail"
direction = "buy"
when = "RATEC > 5 AND C >= H * 0.98"
bias = 1.2

[[strategy]]
name = "散户获利卖出"
holder = "retail"
direction = "sell"
when = "RATEH > 12"
bias = 1.0

[[strategy]]
name = "主力高位派发"
holder = "main"
direction = "sell"
when = "RATEC > 20 AND C < O"
bias = -0.6
"#,
    )
    .map(|config| config.strategy)
    .unwrap_or_default()
}

fn chip_change_backup_dir(source_path: &str) -> PathBuf {
    Path::new(source_path).join(CHIP_CHANGE_BACKUP_DIR_NAME)
}

fn validate_chip_change_draft(draft: CyqChenStrategyFileDraft) -> Result<ChipChangeConfig, String> {
    let config = ChipChangeConfig {
        version: 1,
        strategy: draft.strategies,
    };
    config.compile()?;
    Ok(config)
}

fn backup_file_name() -> String {
    format!(
        "chip_change_rule_{}.toml",
        Local::now().format("%Y%m%d_%H%M%S_%3f")
    )
}

fn validate_chip_change_backup_id(backup_id: &str) -> Result<&str, String> {
    let backup_id = backup_id.trim();
    if backup_id.is_empty()
        || backup_id.contains('/')
        || backup_id.contains('\\')
        || backup_id.contains("..")
        || !backup_id.ends_with(".toml")
    {
        return Err("备份文件名不合法".to_string());
    }
    Ok(backup_id)
}

fn chip_change_backup_path(source_path: &str, backup_id: &str) -> Result<PathBuf, String> {
    let backup_id = validate_chip_change_backup_id(backup_id)?;
    Ok(chip_change_backup_dir(source_path).join(backup_id))
}

fn chip_change_backup_meta_path(source_path: &str, backup_id: &str) -> Result<PathBuf, String> {
    let backup_id = validate_chip_change_backup_id(backup_id)?;
    Ok(chip_change_backup_dir(source_path).join(format!("{backup_id}{CHIP_CHANGE_META_SUFFIX}")))
}

fn now_millis_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn write_chip_change_backup_meta(
    source_path: &str,
    backup_id: &str,
    meta: &CyqChenStrategyBackupMeta,
) -> Result<(), String> {
    let meta_path = chip_change_backup_meta_path(source_path, backup_id)?;
    if let Some(parent) = meta_path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("创建筹码策略元数据目录失败: {e}"))?;
    }
    let payload =
        serde_json::to_string_pretty(meta).map_err(|e| format!("序列化筹码策略元数据失败: {e}"))?;
    let temp_path = meta_path.with_file_name(format!(
        ".{}.{}.tmp",
        meta_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("chip_change_rule.meta.json"),
        now_millis_suffix()
    ));
    fs::write(&temp_path, payload).map_err(|e| {
        format!(
            "写入筹码策略元数据临时文件失败: path={}, err={e}",
            temp_path.display()
        )
    })?;
    fs::rename(&temp_path, &meta_path).map_err(|e| {
        let _ = fs::remove_file(&temp_path);
        format!(
            "写入筹码策略元数据失败: path={}, err={e}",
            meta_path.display()
        )
    })
}

fn read_chip_change_backup_meta(
    source_path: &str,
    backup_id: &str,
) -> Option<CyqChenStrategyBackupMeta> {
    let meta_path = chip_change_backup_meta_path(source_path, backup_id).ok()?;
    let raw = fs::read_to_string(meta_path).ok()?;
    serde_json::from_str(&raw).ok()
}

fn files_have_same_content(left: &Path, right: &Path) -> Result<bool, String> {
    let left_metadata = fs::metadata(left).map_err(|e| {
        format!(
            "读取筹码策略文件元数据失败: path={}, err={e}",
            left.display()
        )
    })?;
    let right_metadata = fs::metadata(right).map_err(|e| {
        format!(
            "读取筹码策略备份元数据失败: path={}, err={e}",
            right.display()
        )
    })?;
    if left_metadata.len() != right_metadata.len() {
        return Ok(false);
    }
    let left_bytes = fs::read(left)
        .map_err(|e| format!("读取筹码策略文件失败: path={}, err={e}", left.display()))?;
    let right_bytes = fs::read(right)
        .map_err(|e| format!("读取筹码策略备份失败: path={}, err={e}", right.display()))?;
    Ok(left_bytes == right_bytes)
}

fn has_equivalent_chip_change_backup(
    source_path: &str,
    active_path: &Path,
) -> Result<bool, String> {
    let backup_dir = chip_change_backup_dir(source_path);
    if !backup_dir.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(&backup_dir).map_err(|e| {
        format!(
            "读取筹码策略备份目录失败: path={}, err={e}",
            backup_dir.display()
        )
    })? {
        let entry = entry.map_err(|e| format!("读取筹码策略备份项失败: {e}"))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !file_name.ends_with(".toml") {
            continue;
        }
        match files_have_same_content(active_path, &path) {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(_) => {}
        }
    }
    Ok(false)
}

fn copy_file_to_destination(
    source: &Path,
    destination_file: &str,
) -> Result<CyqChenStrategyFileExportResult, String> {
    let destination_file = destination_file.trim();
    if destination_file.is_empty() {
        return Err("导出目标文件为空".to_string());
    }
    if !source.exists() || !source.is_file() {
        return Err(format!("待导出的筹码策略文件不存在: {}", source.display()));
    }
    let destination_path = Path::new(destination_file);
    if let Some(parent) = destination_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "创建筹码策略导出目录失败: path={}, err={e}",
                    parent.display()
                )
            })?;
        }
    }
    fs::copy(source, destination_path).map_err(|e| {
        format!(
            "导出筹码策略文件失败: from={}, to={}, err={e}",
            source.display(),
            destination_path.display()
        )
    })?;
    Ok(CyqChenStrategyFileExportResult {
        exported_path: destination_path.display().to_string(),
    })
}

fn list_chip_change_backups(source_path: &str) -> Result<Vec<CyqChenStrategyBackupItem>, String> {
    let backup_dir = chip_change_backup_dir(source_path);
    if !backup_dir.exists() {
        return Ok(Vec::new());
    }

    let mut backups = Vec::new();
    for entry in fs::read_dir(&backup_dir).map_err(|e| {
        format!(
            "读取筹码策略备份目录失败: path={}, err={e}",
            backup_dir.display()
        )
    })? {
        let entry = entry.map_err(|e| format!("读取筹码策略备份项失败: {e}"))?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(|e| {
            format!(
                "读取筹码策略备份元信息失败: path={}, err={e}",
                path.display()
            )
        })?;
        if !metadata.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !file_name.ends_with(".toml") {
            continue;
        }
        let modified_at = metadata.modified().ok().map(|time| {
            chrono::DateTime::<Local>::from(time)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        });
        let meta = read_chip_change_backup_meta(source_path, file_name);
        backups.push(CyqChenStrategyBackupItem {
            backup_id: file_name.to_string(),
            file_name: file_name.to_string(),
            file_path: path.display().to_string(),
            created_at: meta.as_ref().map(|item| item.created_at.clone()),
            modified_at,
            size_bytes: metadata.len(),
            source_kind: meta
                .as_ref()
                .map(|item| item.source_kind.clone())
                .unwrap_or_else(|| "backup".to_string()),
            source_file_name: meta.as_ref().and_then(|item| item.source_file_name.clone()),
            description: meta.and_then(|item| item.description),
        });
    }

    backups.sort_by(|left, right| right.file_name.cmp(&left.file_name));
    Ok(backups)
}

fn build_strategy_page_data(source_path: &str) -> Result<CyqChenStrategyPageData, String> {
    let path = chip_change_rule_path(source_path);
    let exists = path.exists();
    let strategies = if exists {
        ChipChangeConfig::load(source_path)?.strategy
    } else {
        default_chip_change_strategies()
    };
    Ok(CyqChenStrategyPageData {
        file_path: path.display().to_string(),
        exists,
        strategies,
        backups: list_chip_change_backups(source_path)?,
    })
}

fn lcs_value(values: &[usize], width: usize, row: usize, col: usize) -> usize {
    values[row * width + col]
}

fn is_chip_strategy_entry_header(line: &str) -> bool {
    line.trim() == "[[strategy]]"
}

fn chip_strategy_entry_range_for_line(
    lines: &[&str],
    line_number: usize,
) -> Option<(usize, usize)> {
    if line_number == 0 || line_number > lines.len() {
        return None;
    }

    let index = line_number - 1;
    let start = (0..=index)
        .rev()
        .find(|&candidate| is_chip_strategy_entry_header(lines[candidate]))
        .unwrap_or(index);
    let end = ((index + 1)..lines.len())
        .find(|&candidate| is_chip_strategy_entry_header(lines[candidate]))
        .unwrap_or(lines.len());
    Some((start, end))
}

fn mark_chip_strategy_entry_range(keep: &mut [bool], lines: &[&str], line_number: usize) {
    let Some((start, end)) = chip_strategy_entry_range_for_line(lines, line_number) else {
        return;
    };
    keep[start..end].fill(true);
}

fn compact_chip_strategy_diff_lines_by_entry(
    lines: Vec<CyqChenStrategyBackupDiffLine>,
    backup_lines: &[&str],
    active_lines: &[&str],
) -> Vec<CyqChenStrategyBackupDiffLine> {
    if lines.is_empty() {
        return vec![CyqChenStrategyBackupDiffLine {
            kind: "omitted".to_string(),
            backup_line: None,
            active_line: None,
            text: "没有可显示的内容".to_string(),
        }];
    }

    if lines.iter().all(|line| line.kind == "context") {
        return vec![CyqChenStrategyBackupDiffLine {
            kind: "omitted".to_string(),
            backup_line: None,
            active_line: None,
            text: "没有差异".to_string(),
        }];
    }

    let mut keep_backup = vec![false; backup_lines.len()];
    let mut keep_active = vec![false; active_lines.len()];
    for line in &lines {
        if line.kind == "context" {
            continue;
        }
        if let Some(line_number) = line.backup_line {
            mark_chip_strategy_entry_range(&mut keep_backup, backup_lines, line_number);
        }
        if let Some(line_number) = line.active_line {
            mark_chip_strategy_entry_range(&mut keep_active, active_lines, line_number);
        }
    }

    let mut compacted = Vec::new();
    let mut omitted_count = 0usize;
    for line in lines {
        let keep_line = line
            .backup_line
            .and_then(|line_number| keep_backup.get(line_number.saturating_sub(1)))
            .copied()
            .unwrap_or(false)
            || line
                .active_line
                .and_then(|line_number| keep_active.get(line_number.saturating_sub(1)))
                .copied()
                .unwrap_or(false);

        if keep_line {
            if omitted_count > 0 {
                compacted.push(CyqChenStrategyBackupDiffLine {
                    kind: "omitted".to_string(),
                    backup_line: None,
                    active_line: None,
                    text: format!("省略 {omitted_count} 行未变化策略"),
                });
                omitted_count = 0;
            }
            compacted.push(line);
        } else {
            omitted_count += 1;
        }
    }

    if omitted_count > 0 {
        compacted.push(CyqChenStrategyBackupDiffLine {
            kind: "omitted".to_string(),
            backup_line: None,
            active_line: None,
            text: format!("省略 {omitted_count} 行未变化策略"),
        });
    }

    compacted
}

fn build_chip_strategy_backup_diff_lines(
    backup_text: &str,
    active_text: &str,
) -> (Vec<CyqChenStrategyBackupDiffLine>, usize) {
    let backup_lines: Vec<&str> = backup_text.lines().collect();
    let active_lines: Vec<&str> = active_text.lines().collect();
    let rows = backup_lines.len();
    let cols = active_lines.len();
    let width = cols + 1;
    let mut lcs = vec![0usize; (rows + 1) * (cols + 1)];

    for row in (0..rows).rev() {
        for col in (0..cols).rev() {
            lcs[row * width + col] = if backup_lines[row] == active_lines[col] {
                1 + lcs_value(&lcs, width, row + 1, col + 1)
            } else {
                lcs_value(&lcs, width, row + 1, col).max(lcs_value(&lcs, width, row, col + 1))
            };
        }
    }

    let mut diff_lines = Vec::new();
    let mut changed_line_count = 0usize;
    let mut backup_index = 0usize;
    let mut active_index = 0usize;

    while backup_index < rows && active_index < cols {
        if backup_lines[backup_index] == active_lines[active_index] {
            diff_lines.push(CyqChenStrategyBackupDiffLine {
                kind: "context".to_string(),
                backup_line: Some(backup_index + 1),
                active_line: Some(active_index + 1),
                text: backup_lines[backup_index].to_string(),
            });
            backup_index += 1;
            active_index += 1;
        } else if lcs_value(&lcs, width, backup_index + 1, active_index)
            >= lcs_value(&lcs, width, backup_index, active_index + 1)
        {
            diff_lines.push(CyqChenStrategyBackupDiffLine {
                kind: "backup".to_string(),
                backup_line: Some(backup_index + 1),
                active_line: None,
                text: backup_lines[backup_index].to_string(),
            });
            changed_line_count += 1;
            backup_index += 1;
        } else {
            diff_lines.push(CyqChenStrategyBackupDiffLine {
                kind: "active".to_string(),
                backup_line: None,
                active_line: Some(active_index + 1),
                text: active_lines[active_index].to_string(),
            });
            changed_line_count += 1;
            active_index += 1;
        }
    }

    while backup_index < rows {
        diff_lines.push(CyqChenStrategyBackupDiffLine {
            kind: "backup".to_string(),
            backup_line: Some(backup_index + 1),
            active_line: None,
            text: backup_lines[backup_index].to_string(),
        });
        changed_line_count += 1;
        backup_index += 1;
    }

    while active_index < cols {
        diff_lines.push(CyqChenStrategyBackupDiffLine {
            kind: "active".to_string(),
            backup_line: None,
            active_line: Some(active_index + 1),
            text: active_lines[active_index].to_string(),
        });
        changed_line_count += 1;
        active_index += 1;
    }

    (
        compact_chip_strategy_diff_lines_by_entry(diff_lines, &backup_lines, &active_lines),
        changed_line_count,
    )
}

pub fn get_cyq_chen_strategy_page(source_path: &str) -> Result<CyqChenStrategyPageData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    build_strategy_page_data(source_path)
}

pub fn save_cyq_chen_strategy_file(
    source_path: &str,
    draft: CyqChenStrategyFileDraft,
) -> Result<CyqChenStrategyPageData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    let config = validate_chip_change_draft(draft)?;
    let path = chip_change_rule_path(source_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("创建筹码策略目录失败: {e}"))?;
    }
    let text =
        toml::to_string_pretty(&config).map_err(|e| format!("序列化筹码变化策略文件失败: {e}"))?;
    fs::write(&path, text)
        .map_err(|e| format!("写入筹码变化策略文件失败: path={}, err={e}", path.display()))?;
    build_strategy_page_data(source_path)
}

pub fn check_cyq_chen_strategy_file_draft(
    draft: CyqChenStrategyFileDraft,
) -> Result<String, String> {
    validate_chip_change_draft(draft)?;
    Ok("筹码策略草稿检查通过".to_string())
}

pub fn backup_cyq_chen_strategy_file_with_meta(
    source_path: &str,
    source_kind: &str,
    description: &str,
) -> Result<CyqChenStrategyPageData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    let active_path = chip_change_rule_path(source_path);
    if !active_path.exists() {
        return Err("当前没有 chip_change_rule.toml 可备份".to_string());
    }
    let backup_dir = chip_change_backup_dir(source_path);
    fs::create_dir_all(&backup_dir).map_err(|e| {
        format!(
            "创建筹码策略备份目录失败: path={}, err={e}",
            backup_dir.display()
        )
    })?;
    let backup_path = backup_dir.join(backup_file_name());
    fs::copy(&active_path, &backup_path).map_err(|e| {
        format!(
            "备份筹码变化策略文件失败: from={}, to={}, err={e}",
            active_path.display(),
            backup_path.display()
        )
    })?;
    let backup_id = backup_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "无法识别筹码策略备份文件名".to_string())?;
    write_chip_change_backup_meta(
        source_path,
        backup_id,
        &CyqChenStrategyBackupMeta {
            version: 1,
            created_at: chrono::Utc::now().to_rfc3339(),
            source_kind: source_kind.to_string(),
            source_file_name: Some("chip_change_rule.toml".to_string()),
            description: Some(description.to_string()),
        },
    )?;
    build_strategy_page_data(source_path)
}

pub fn backup_cyq_chen_strategy_file(source_path: &str) -> Result<CyqChenStrategyPageData, String> {
    backup_cyq_chen_strategy_file_with_meta(source_path, "backup", "手动备份当前筹码策略")
}

pub fn auto_backup_cyq_chen_strategy_file_on_entry(
    source_path: &str,
) -> Result<Option<CyqChenStrategyPageData>, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    let active_path = chip_change_rule_path(source_path);
    if !active_path.exists() || !active_path.is_file() {
        return Ok(None);
    }
    if has_equivalent_chip_change_backup(source_path, &active_path)? {
        return Ok(None);
    }
    backup_cyq_chen_strategy_file_with_meta(source_path, "auto_entry", "自动备份：进入筹码策略页")
        .map(Some)
}

pub fn create_empty_cyq_chen_strategy_backup(
    source_path: &str,
) -> Result<CyqChenStrategyPageData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    ChipChangeConfig::from_toml_str(EMPTY_CHIP_CHANGE_TEMPLATE)?;
    let backup_dir = chip_change_backup_dir(source_path);
    fs::create_dir_all(&backup_dir).map_err(|e| {
        format!(
            "创建筹码策略备份目录失败: path={}, err={e}",
            backup_dir.display()
        )
    })?;
    let backup_path = backup_dir.join(backup_file_name());
    fs::write(&backup_path, EMPTY_CHIP_CHANGE_TEMPLATE).map_err(|e| {
        format!(
            "写入空白筹码策略失败: path={}, err={e}",
            backup_path.display()
        )
    })?;
    let backup_id = backup_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "无法识别筹码策略备份文件名".to_string())?;
    write_chip_change_backup_meta(
        source_path,
        backup_id,
        &CyqChenStrategyBackupMeta {
            version: 1,
            created_at: chrono::Utc::now().to_rfc3339(),
            source_kind: "empty".to_string(),
            source_file_name: Some("chip_change_rule.toml".to_string()),
            description: Some("空白筹码策略模板".to_string()),
        },
    )?;
    build_strategy_page_data(source_path)
}

pub fn import_cyq_chen_strategy_backup(
    source_path: &str,
    source_file: &str,
) -> Result<CyqChenStrategyPageData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    let source_file = source_file.trim();
    if source_file.is_empty() {
        return Err("导入文件为空".to_string());
    }
    let source_file_path = Path::new(source_file);
    let text = fs::read_to_string(source_file_path).map_err(|e| {
        format!(
            "读取筹码策略导入文件失败: path={}, err={e}",
            source_file_path.display()
        )
    })?;
    ChipChangeConfig::from_toml_str(&text)?;
    let backup_dir = chip_change_backup_dir(source_path);
    fs::create_dir_all(&backup_dir).map_err(|e| {
        format!(
            "创建筹码策略备份目录失败: path={}, err={e}",
            backup_dir.display()
        )
    })?;
    let backup_path = backup_dir.join(backup_file_name());
    fs::write(&backup_path, text).map_err(|e| {
        format!(
            "导入筹码策略备份失败: to={}, err={e}",
            backup_path.display()
        )
    })?;
    let backup_id = backup_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "无法识别筹码策略备份文件名".to_string())?;
    let source_file_name = source_file_path
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::to_string);
    write_chip_change_backup_meta(
        source_path,
        backup_id,
        &CyqChenStrategyBackupMeta {
            version: 1,
            created_at: chrono::Utc::now().to_rfc3339(),
            source_kind: "imported".to_string(),
            source_file_name,
            description: Some("外部导入筹码策略".to_string()),
        },
    )?;
    build_strategy_page_data(source_path)
}

pub fn delete_cyq_chen_strategy_backup(
    source_path: &str,
    backup_id: &str,
) -> Result<CyqChenStrategyPageData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    let backup_path = chip_change_backup_path(source_path, backup_id)?;
    if !backup_path.exists() {
        return Err(format!("筹码策略备份不存在: {}", backup_path.display()));
    }
    fs::remove_file(&backup_path).map_err(|e| {
        format!(
            "删除筹码策略备份失败: path={}, err={e}",
            backup_path.display()
        )
    })?;
    if let Ok(meta_path) = chip_change_backup_meta_path(source_path, backup_id) {
        if meta_path.exists() {
            fs::remove_file(&meta_path).map_err(|e| {
                format!(
                    "删除筹码策略备份元数据失败: path={}, err={e}",
                    meta_path.display()
                )
            })?;
        }
    }
    build_strategy_page_data(source_path)
}

pub fn export_cyq_chen_active_strategy_file(
    source_path: &str,
    destination_file: &str,
) -> Result<CyqChenStrategyFileExportResult, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    copy_file_to_destination(&chip_change_rule_path(source_path), destination_file)
}

pub fn export_cyq_chen_strategy_backup_file(
    source_path: &str,
    backup_id: &str,
    destination_file: &str,
) -> Result<CyqChenStrategyFileExportResult, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    let backup_path = chip_change_backup_path(source_path, backup_id)?;
    copy_file_to_destination(&backup_path, destination_file)
}

pub fn get_cyq_chen_strategy_backup_diff(
    source_path: &str,
    backup_id: &str,
) -> Result<CyqChenStrategyBackupDiff, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    let backup_id = validate_chip_change_backup_id(backup_id)?.to_string();
    let backup_path = chip_change_backup_path(source_path, &backup_id)?;
    let active_path = chip_change_rule_path(source_path);
    if !backup_path.exists() {
        return Err(format!("筹码策略备份不存在: {}", backup_path.display()));
    }
    if !active_path.exists() {
        return Err(format!("当前筹码策略文件不存在: {}", active_path.display()));
    }
    let backup_text = fs::read_to_string(&backup_path).map_err(|e| {
        format!(
            "读取筹码策略备份失败: path={}, err={e}",
            backup_path.display()
        )
    })?;
    let active_text = fs::read_to_string(&active_path).map_err(|e| {
        format!(
            "读取当前筹码策略失败: path={}, err={e}",
            active_path.display()
        )
    })?;
    let (lines, changed_line_count) =
        build_chip_strategy_backup_diff_lines(&backup_text, &active_text);
    Ok(CyqChenStrategyBackupDiff {
        backup_id: backup_id.clone(),
        backup_label: backup_id,
        active_label: "chip_change_rule.toml".to_string(),
        changed_line_count,
        lines,
    })
}

pub fn activate_cyq_chen_strategy_backup(
    source_path: &str,
    backup_id: &str,
) -> Result<CyqChenStrategyPageData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    let backup_path = chip_change_backup_path(source_path, backup_id)?;
    if !backup_path.exists() {
        return Err(format!("筹码策略备份不存在: {backup_id}"));
    }
    let text = fs::read_to_string(&backup_path).map_err(|e| {
        format!(
            "读取筹码策略备份失败: path={}, err={e}",
            backup_path.display()
        )
    })?;
    ChipChangeConfig::from_toml_str(&text)?;
    let active_path = chip_change_rule_path(source_path);
    fs::write(&active_path, text).map_err(|e| {
        format!(
            "恢复筹码变化策略文件失败: path={}, err={e}",
            active_path.display()
        )
    })?;
    build_strategy_page_data(source_path)
}

pub fn run_cyq_chen_single_stock_test(
    request: CyqChenSingleStockRequest,
) -> Result<CyqChenSingleStockData, String> {
    let source_path = request.source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    let ts_code = normalize_ts_code(&request.ts_code)
        .ok_or_else(|| "股票代码格式不正确，请输入 000001 或 000001.SZ".to_string())?;

    let config = ChenChipConfig {
        warmup_days: request.warmup_days,
        bucket_pct: request.bucket_pct,
    };
    let chip_config = ChipChangeConfig {
        version: 1,
        strategy: request.strategies,
    };
    let compiled_chip_config = chip_config.compile()?;

    let (start_date, end_date) =
        resolve_requested_range(source_path, request.start_date, request.end_date)?;
    let load_start_date =
        resolve_load_start_date(source_path, &start_date, &end_date, config.warmup_days)?
            .unwrap_or_else(|| start_date.clone());

    let required_runtime_keys = collect_chen_chip_runtime_keys(&compiled_chip_config);
    let reader = DataReader::new_with_runtime_keys(source_path, &required_runtime_keys)?;
    let mut row_data =
        match reader.load_one(&ts_code, DEFAULT_ADJ_TYPE, &load_start_date, &end_date) {
            Ok(data) => data,
            Err(err) if err.contains("trade_dates为空") => {
                return Ok(CyqChenSingleStockData {
                    resolved_ts_code: ts_code,
                    start_date,
                    end_date,
                    output_start_date: None,
                    kline: Vec::new(),
                    kline_payload: None,
                    snapshots: Vec::new(),
                });
            }
            Err(err) => return Err(err),
        };

    if row_data.trade_dates.is_empty() {
        return Ok(CyqChenSingleStockData {
            resolved_ts_code: ts_code,
            start_date,
            end_date,
            output_start_date: None,
            kline: Vec::new(),
            kline_payload: None,
            snapshots: Vec::new(),
        });
    }

    if resolve_output_start_date(&row_data, &start_date, &end_date, config.warmup_days).is_none() {
        let output_rows = count_visible_rows(&row_data, &start_date, &end_date);
        let need_rows = config.warmup_days.saturating_add(output_rows);
        if need_rows > 0 {
            let tail_row_data =
                reader.load_one_tail_rows(&ts_code, DEFAULT_ADJ_TYPE, &end_date, need_rows)?;
            if !tail_row_data.trade_dates.is_empty() {
                row_data = tail_row_data;
            }
        }
    }

    let st_list = load_st_list(source_path).unwrap_or_default();
    let total_share_map = load_total_share_map(source_path).unwrap_or_default();
    inject_stock_extra_fields(
        &mut row_data,
        &ts_code,
        st_list.contains(&ts_code),
        total_share_map.get(&ts_code).copied(),
    )?;

    let kline = build_kline_rows(&row_data, &start_date, &end_date)?;
    let kline_payload = build_detail_kline_payload(source_path, &ts_code, &start_date, &end_date)?;
    let output_start_date =
        resolve_output_start_date(&row_data, &start_date, &end_date, config.warmup_days);
    let mut snapshots: Vec<ChenChipSnapshot> =
        if let Some(output_start_date) = output_start_date.as_deref() {
            compute_chen_chip_snapshots_with_compiled_config(
                &row_data,
                output_start_date,
                &compiled_chip_config,
                config,
            )?
            .into_iter()
            .filter(|snapshot| {
                snapshot
                    .trade_date
                    .as_deref()
                    .map(|trade_date| {
                        trade_date >= start_date.as_str() && trade_date <= end_date.as_str()
                    })
                    .unwrap_or(false)
            })
            .collect()
        } else {
            Vec::new()
        };
    for snapshot in &mut snapshots {
        round_chen_chip_snapshot(snapshot);
    }

    Ok(CyqChenSingleStockData {
        resolved_ts_code: ts_code,
        start_date,
        end_date,
        output_start_date,
        kline,
        kline_payload,
        snapshots,
    })
}

fn build_detail_kline_payload(
    source_path: &str,
    ts_code: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Option<DetailKlinePayload>, String> {
    let source_db = source_db_path(source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;
    let name_map = crate::ui_tools_feat::build_name_map(source_path).unwrap_or_default();
    let watermark_name = name_map.get(ts_code).cloned();
    let mut payload = crate::ui_tools_feat::details::query_kline(
        &conn,
        source_path,
        ts_code,
        DEFAULT_VISIBLE_KLINE_WINDOW_DAYS,
        watermark_name,
    )?;

    if let Some(items) = payload.items.as_mut() {
        items.retain(|item| {
            item.trade_date.as_str() >= start_date && item.trade_date.as_str() <= end_date
        });
    }

    Ok(Some(payload))
}

fn resolve_requested_range(
    source_path: &str,
    start_date: Option<String>,
    end_date: Option<String>,
) -> Result<(String, String), String> {
    let source_db = source_db_path(source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;

    let mut stmt = conn
        .prepare("SELECT MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type = ?")
        .map_err(|e| format!("预编译交易日范围查询失败:{e}"))?;
    let mut rows = stmt
        .query(params![DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询交易日范围失败:{e}"))?;
    let Some(row) = rows.next().map_err(|e| format!("读取交易日范围失败:{e}"))? else {
        return Err("stock_data没有可用交易日".to_string());
    };
    let source_start: Option<String> = row.get(0).map_err(|e| format!("读取最早交易日失败:{e}"))?;
    let source_end: Option<String> = row.get(1).map_err(|e| format!("读取最晚交易日失败:{e}"))?;
    let source_start = source_start.ok_or_else(|| "stock_data没有可用交易日".to_string())?;
    let source_end = source_end.ok_or_else(|| "stock_data没有可用交易日".to_string())?;

    let start_date = normalize_trade_date_input(start_date)
        .map(|value| {
            if value.as_str() > source_start.as_str() {
                value
            } else {
                source_start.clone()
            }
        })
        .unwrap_or_else(|| source_start.clone());
    let end_date = normalize_trade_date_input(end_date)
        .map(|value| {
            if value.as_str() < source_end.as_str() {
                value
            } else {
                source_end.clone()
            }
        })
        .unwrap_or_else(|| source_end.clone());

    if start_date > end_date {
        return Err(format!("计算区间无效: {start_date} > {end_date}"));
    }

    Ok((start_date, end_date))
}

fn normalize_trade_date_input(value: Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .chars()
                .filter(|ch| ch.is_ascii_digit())
                .collect::<String>()
        })
        .filter(|value| value.len() == 8)
}

fn resolve_load_start_date(
    source_path: &str,
    output_start_date: &str,
    output_end_date: &str,
    warmup_days: usize,
) -> Result<Option<String>, String> {
    let trade_dates = load_trade_date_list(source_path)?;
    if trade_dates.is_empty() {
        return Ok(None);
    }

    let Some(first_output_index) = trade_dates.iter().position(|trade_date| {
        let trade_date = trade_date.as_str();
        trade_date >= output_start_date && trade_date <= output_end_date
    }) else {
        return Ok(None);
    };

    let load_start_index = first_output_index.saturating_sub(warmup_days);
    Ok(Some(trade_dates[load_start_index].clone()))
}

fn resolve_output_start_date(
    row_data: &RowData,
    start_date: &str,
    end_date: &str,
    warmup_days: usize,
) -> Option<String> {
    row_data
        .trade_dates
        .iter()
        .enumerate()
        .find_map(|(index, trade_date)| {
            let trade_date_str = trade_date.as_str();
            if trade_date_str < start_date || trade_date_str > end_date {
                return None;
            }
            if index < warmup_days {
                return None;
            }
            Some(trade_date.clone())
        })
}

fn count_visible_rows(row_data: &RowData, start_date: &str, end_date: &str) -> usize {
    row_data
        .trade_dates
        .iter()
        .filter(|trade_date| {
            let trade_date = trade_date.as_str();
            trade_date >= start_date && trade_date <= end_date
        })
        .count()
}

fn build_kline_rows(
    row_data: &RowData,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<CyqChenKlineRow>, String> {
    let open = required_series(row_data, "O")?;
    let high = required_series(row_data, "H")?;
    let low = required_series(row_data, "L")?;
    let close = required_series(row_data, "C")?;
    let turnover_rate = row_data
        .cols
        .get("TURNOVER_RATE")
        .or_else(|| row_data.cols.get("TOR"))
        .map(Vec::as_slice)
        .ok_or_else(|| "RowData 缺少 TURNOVER_RATE/TOR 列".to_string())?;

    let mut rows = Vec::new();
    for (index, trade_date) in row_data.trade_dates.iter().enumerate() {
        let trade_date_str = trade_date.as_str();
        if trade_date_str < start_date || trade_date_str > end_date {
            continue;
        }
        rows.push(CyqChenKlineRow {
            trade_date: trade_date.clone(),
            open: open[index],
            high: high[index],
            low: low[index],
            close: close[index],
            turnover_rate: turnover_rate[index],
        });
    }
    Ok(rows)
}

fn required_series<'a>(row_data: &'a RowData, key: &str) -> Result<&'a [Option<f64>], String> {
    row_data
        .cols
        .get(key)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("RowData 缺少 {key} 列"))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    use super::{CyqChenSingleStockRequest, run_cyq_chen_single_stock_test};
    use crate::data::{
        cyq_chen::{ChipChangeStrategy, ChipDirection, ChipHolder},
        source_db_path, stock_list_path, trade_calendar_path,
    };

    fn unique_temp_source_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua-cyq-chen-ui-test-{nanos}"))
    }

    fn prepare_source_db(source_dir: &Path) {
        fs::create_dir_all(source_dir).expect("create temp dir");
        fs::write(
            trade_calendar_path(source_dir.to_str().expect("utf8 path")),
            "cal_date\n20260401\n20260402\n20260403\n20260407\n",
        )
        .expect("write trade calendar");
        fs::write(
            stock_list_path(source_dir.to_str().expect("utf8 path")),
            "ts_code,symbol,name,area,industry,list_date,market,total_share\n000001.SZ,000001,平安银行,深圳,银行,19910403,主板,20000\n",
        )
        .expect("write stock list");

        let source_db = source_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(&source_db).expect("open source db");
        conn.execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                pre_close DOUBLE,
                change DOUBLE,
                pct_chg DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                tor DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");

        let rows = [
            ("20260401", 10.0, 10.3, 9.8, 10.1, 5.0),
            ("20260402", 10.1, 10.4, 10.0, 10.3, 5.0),
            ("20260403", 10.3, 10.8, 10.2, 10.6, 5.0),
            ("20260407", 10.6, 11.6, 10.5, 11.4, 5.0),
        ];
        for (trade_date, open, high, low, close, tor) in rows {
            conn.execute(
                r#"
                INSERT INTO stock_data (
                    ts_code, trade_date, adj_type, open, high, low, close,
                    pre_close, change, pct_chg, vol, amount, tor
                ) VALUES ('000001.SZ', ?, 'qfq', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                params![
                    trade_date, open, high, low, close, close, 0.0_f64, 0.0_f64, 1.0_f64, 1.0_f64,
                    tor
                ],
            )
            .expect("insert source row");
        }
    }

    #[test]
    fn single_stock_test_returns_kline_and_temporary_snapshots() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);

        let data = run_cyq_chen_single_stock_test(CyqChenSingleStockRequest {
            source_path: source_dir.to_str().expect("utf8 path").to_string(),
            ts_code: "000001".to_string(),
            start_date: Some("20260401".to_string()),
            end_date: Some("20260407".to_string()),
            warmup_days: 2,
            bucket_pct: 5.0,
            strategies: vec![ChipChangeStrategy {
                name: "主力买入".to_string(),
                holder: ChipHolder::Main,
                direction: ChipDirection::Buy,
                when: "C > O AND ZHANG > 0 AND TOTAL_MV_YI > 0".to_string(),
                bias: 1.0,
            }],
        })
        .expect("run single stock test");

        assert_eq!(data.resolved_ts_code, "000001.SZ");
        assert_eq!(data.kline.len(), 4);
        assert_eq!(data.output_start_date.as_deref(), Some("20260403"));
        assert_eq!(data.snapshots.len(), 2);

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn single_stock_test_returns_empty_result_for_unknown_stock() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);

        let data = run_cyq_chen_single_stock_test(CyqChenSingleStockRequest {
            source_path: source_dir.to_str().expect("utf8 path").to_string(),
            ts_code: "000999".to_string(),
            start_date: Some("20260401".to_string()),
            end_date: Some("20260407".to_string()),
            warmup_days: 2,
            bucket_pct: 5.0,
            strategies: vec![ChipChangeStrategy {
                name: "主力买入".to_string(),
                holder: ChipHolder::Main,
                direction: ChipDirection::Buy,
                when: "C > O AND ZHANG > 0 AND TOTAL_MV_YI > 0".to_string(),
                bias: 1.0,
            }],
        })
        .expect("unknown stock should return an empty single-stock test result");

        assert_eq!(data.resolved_ts_code, "000999.SZ");
        assert_eq!(data.output_start_date, None);
        assert!(data.kline.is_empty());
        assert!(data.snapshots.is_empty());

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }
}
