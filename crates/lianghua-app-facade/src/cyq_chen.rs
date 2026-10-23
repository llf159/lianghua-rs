use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Local;
use duckdb::params;
use serde::{Deserialize, Serialize};

use crate::details::DetailKlinePayload;
use crate::{
    data::{
        DataReader, RowData, chip_change_rule_path,
        cyq_chen::{
            ChenChipBasis, ChenChipConfig, ChenChipMechanismComparison, ChenChipSnapshot,
            ChipChangeConfig, ChipChangeStrategy, collect_chen_chip_runtime_keys,
            compute_chen_chip_basis, compute_chen_chip_mechanism_comparison,
            compute_chen_chip_snapshots_with_compiled_config, round_chen_chip_snapshot,
        },
        load_trade_date_list, source_db_path,
    },
    scoring::tools::{inject_stock_extra_fields, load_st_list, load_total_share_map},
};
use lianghua_app_shared::normalize_ts_code;

use crate::utils::utils::board_category;

const DEFAULT_ADJ_TYPE: &str = "qfq";
const CHIP_CHANGE_BACKUP_DIR_NAME: &str = "chip_change_rule_backups";
const EMPTY_CHIP_CHANGE_TEMPLATE: &str = "version = 1\n";

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenSingleStockRequest {
    #[serde(default)]
    pub compare_mechanisms: bool,
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
    pub mechanism_comparison: Option<ChenChipMechanismComparison>,
    pub basis: Option<ChenChipBasis>,
    pub l2_days: Vec<CyqChenL2Day>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenL2Day {
    pub trade_date: String,
    pub board: String,
    pub status: String,
    pub volume: Option<f64>,
    pub net_mf_volume: Option<f64>,
    pub net_mf_ratio: Option<f64>,
    pub net_mf_ratio_3d: Option<f64>,
    pub large_buy_volume: Option<f64>,
    pub large_sell_volume: Option<f64>,
    pub large_net_ratio: Option<f64>,
    pub large_net_ratio_3d: Option<f64>,
    pub large_buy_denominator: Option<f64>,
    pub large_buy_share: Option<f64>,
    pub large_buy_share_3d: Option<f64>,
    pub issues: Vec<String>,
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
    Ok(chip_change_backup_dir(source_path).join(format!("{backup_id}.meta.json")))
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
        (|| -> u128 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis())
                .unwrap_or_default()
        })()
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

fn build_strategy_page_data(source_path: &str) -> Result<CyqChenStrategyPageData, String> {
    let path = chip_change_rule_path(source_path);
    let exists = path.exists();
    let strategies = if exists {
        ChipChangeConfig::load(source_path)?.strategy
    } else {
        (|| -> Vec<ChipChangeStrategy> {
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
        })()
    };
    Ok(CyqChenStrategyPageData {
        file_path: path.display().to_string(),
        exists,
        strategies,
        backups: (|source_path: &str| -> Result<Vec<CyqChenStrategyBackupItem>, String> {
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
                let meta = (|source_path: &str,
                             backup_id: &str|
                 -> Option<CyqChenStrategyBackupMeta> {
                    let meta_path = chip_change_backup_meta_path(source_path, backup_id).ok()?;
                    let raw = fs::read_to_string(meta_path).ok()?;
                    serde_json::from_str(&raw).ok()
                })(source_path, file_name);
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
        })(source_path)?,
    })
}

fn lcs_value(values: &[usize], width: usize, row: usize, col: usize) -> usize {
    values[row * width + col]
}

fn is_chip_strategy_entry_header(line: &str) -> bool {
    line.trim() == "[[strategy]]"
}

fn mark_chip_strategy_entry_range(keep: &mut [bool], lines: &[&str], line_number: usize) {
    let Some((start, end)) = (|lines: &[&str], line_number: usize| -> Option<(usize, usize)> {
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
    })(lines, line_number) else {
        return;
    };
    keep[start..end].fill(true);
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
    if (|source_path: &str, active_path: &Path| -> Result<bool, String> {
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
    })(source_path, &active_path)?
    {
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
    import_cyq_chen_strategy_backup_from_text(
        source_path,
        source_file_path
            .file_name()
            .and_then(|value| value.to_str()),
        &text,
    )
}

pub fn import_cyq_chen_strategy_backup_from_text(
    source_path: &str,
    source_file_name: Option<&str>,
    text: &str,
) -> Result<CyqChenStrategyPageData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先确认当前数据源".to_string());
    }
    ChipChangeConfig::from_toml_str(text)?;
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
    write_chip_change_backup_meta(
        source_path,
        backup_id,
        &CyqChenStrategyBackupMeta {
            version: 1,
            created_at: chrono::Utc::now().to_rfc3339(),
            source_kind: "imported".to_string(),
            source_file_name: source_file_name.map(str::to_string),
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
    let (lines, changed_line_count) = (|backup_text: &str,
                                        active_text: &str|
     -> (Vec<CyqChenStrategyBackupDiffLine>, usize) {
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
            (|lines: Vec<CyqChenStrategyBackupDiffLine>,
              backup_lines: &[&str],
              active_lines: &[&str]|
             -> Vec<CyqChenStrategyBackupDiffLine> {
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
            })(diff_lines, &backup_lines, &active_lines),
            changed_line_count,
        )
    })(&backup_text, &active_text);
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

    let (start_date, end_date) = (|source_path: &str,
                                   start_date: Option<String>,
                                   end_date: Option<String>|
     -> Result<(String, String), String> {
        let source_db = source_db_path(source_path);
        let source_db_str = source_db
            .to_str()
            .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
        let conn = crate::data::open_db_readonly(source_db_str)
            .map_err(|e| format!("打开原始库失败:{e}"))?;

        let mut stmt = conn
            .prepare("SELECT MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type = ?")
            .map_err(|e| format!("预编译交易日范围查询失败:{e}"))?;
        let mut rows = stmt
            .query(params![DEFAULT_ADJ_TYPE])
            .map_err(|e| format!("查询交易日范围失败:{e}"))?;
        let Some(row) = rows.next().map_err(|e| format!("读取交易日范围失败:{e}"))? else {
            return Err("stock_data没有可用交易日".to_string());
        };
        let source_start: Option<String> =
            row.get(0).map_err(|e| format!("读取最早交易日失败:{e}"))?;
        let source_end: Option<String> =
            row.get(1).map_err(|e| format!("读取最晚交易日失败:{e}"))?;
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
    })(source_path, request.start_date, request.end_date)?;
    let load_start_date = (|source_path: &str,
                            output_start_date: &str,
                            output_end_date: &str,
                            warmup_days: usize|
     -> Result<Option<String>, String> {
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
    })(source_path, &start_date, &end_date, config.warmup_days)?
    .unwrap_or_else(|| start_date.clone());

    let mut required_runtime_keys = collect_chen_chip_runtime_keys(&compiled_chip_config);
    required_runtime_keys.insert("V".into());
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
                    mechanism_comparison: None,
                    basis: None,
                    l2_days: Vec::new(),
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
            mechanism_comparison: None,
            basis: None,
            l2_days: Vec::new(),
        });
    }

    if resolve_output_start_date(&row_data, &start_date, &end_date, config.warmup_days).is_none() {
        let output_rows = (|row_data: &RowData, start_date: &str, end_date: &str| -> usize {
            row_data
                .trade_dates
                .iter()
                .filter(|trade_date| {
                    let trade_date = trade_date.as_str();
                    trade_date >= start_date && trade_date <= end_date
                })
                .count()
        })(&row_data, &start_date, &end_date);
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

    let kline = (|row_data: &RowData,
                  start_date: &str,
                  end_date: &str|
     -> Result<Vec<CyqChenKlineRow>, String> {
        let open = required_series(row_data, "O")?;
        let high = required_series(row_data, "H")?;
        let low = required_series(row_data, "L")?;
        let close = required_series(row_data, "C")?;
        let turnover_rate = row_data
            .cols
            .get("TOR")
            .map(Vec::as_slice)
            .ok_or_else(|| "RowData 缺少 TOR 列".to_string())?;

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
    })(&row_data, &start_date, &end_date)?;
    let kline_payload = (|source_path: &str,
                          ts_code: &str,
                          start_date: &str,
                          end_date: &str|
     -> Result<Option<DetailKlinePayload>, String> {
        let source_db = source_db_path(source_path);
        let source_db_str = source_db
            .to_str()
            .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
        let conn = crate::data::open_db_readonly(source_db_str)
            .map_err(|e| format!("打开原始库失败:{e}"))?;
        let name_map = lianghua_app_shared::build_name_map(source_path).unwrap_or_default();
        let watermark_name = name_map.get(ts_code).cloned();
        let mut payload =
            crate::details::query_kline(&conn, source_path, ts_code, 90, watermark_name)?;

        if let Some(items) = payload.items.as_mut() {
            items.retain(|item| {
                item.trade_date.as_str() >= start_date && item.trade_date.as_str() <= end_date
            });
        }

        Ok(Some(payload))
    })(source_path, &ts_code, &start_date, &end_date)?;
    let output_start_date =
        resolve_output_start_date(&row_data, &start_date, &end_date, config.warmup_days);
    let (mut snapshots, mechanism_comparison) =
        if let Some(output_start_date) = output_start_date.as_deref() {
            if request.compare_mechanisms {
                let (snapshots, comparison) = compute_chen_chip_mechanism_comparison(
                    &row_data,
                    output_start_date,
                    &compiled_chip_config,
                    config,
                )?;
                (snapshots, Some(comparison))
            } else {
                (
                    compute_chen_chip_snapshots_with_compiled_config(
                        &row_data,
                        output_start_date,
                        &compiled_chip_config,
                        config,
                    )?,
                    None,
                )
            }
        } else {
            (Vec::new(), None)
        };
    let basis = output_start_date
        .as_deref()
        .map(|date| compute_chen_chip_basis(&row_data, date, config))
        .transpose()?;
    let l2_days = load_cyq_chen_l2_days(source_path, &ts_code, &start_date, &end_date)?;
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
        mechanism_comparison,
        basis,
        l2_days,
    })
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

#[derive(Clone, Copy, Default)]
struct CyqChenL2Raw {
    volume: Option<f64>,
    net_mf_volume: Option<f64>,
    buy_small: Option<f64>,
    buy_medium: Option<f64>,
    buy_large: Option<f64>,
    sell_large: Option<f64>,
    buy_extra_large: Option<f64>,
    sell_extra_large: Option<f64>,
}

fn load_cyq_chen_l2_days(
    source_path: &str,
    ts_code: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<CyqChenL2Day>, String> {
    let trade_calendar = load_trade_date_list(source_path)?;
    let query_start = trade_calendar
        .iter()
        .position(|date| date == start_date)
        .and_then(|index| trade_calendar.get(index.saturating_sub(2)))
        .map_or(start_date, String::as_str);
    let source_db = source_db_path(source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn = crate::data::open_db_readonly(source_db_str)
        .map_err(|error| format!("打开L2证据原始库失败:{error}"))?;
    let columns = (|conn: &duckdb::Connection| -> Result<HashSet<String>, String> {
        let mut stmt = conn
            .prepare("SELECT name FROM pragma_table_info('stock_data')")
            .map_err(|error| format!("读取L2字段结构失败:{error}"))?;
        stmt.query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| format!("查询L2字段结构失败:{error}"))?
            .collect::<Result<HashSet<_>, _>>()
            .map_err(|error| format!("读取L2字段结构失败:{error}"))
    })(&conn)?;
    let expression = |column: &str| {
        columns
            .iter()
            .any(|name| name.eq_ignore_ascii_case(column))
            .then(|| format!("TRY_CAST(\"{column}\" AS DOUBLE)"))
            .unwrap_or_else(|| "NULL::DOUBLE".to_string())
    };
    let sql = format!(
        "SELECT trade_date, {}, {}, {}, {}, {}, {}, {}, {} \
         FROM stock_data WHERE ts_code=? AND adj_type=? AND trade_date>=? AND trade_date<=? \
         ORDER BY trade_date",
        expression("vol"),
        expression("net_mf_v"),
        expression("b_sm_v"),
        expression("b_md_v"),
        expression("b_lg_v"),
        expression("s_lg_v"),
        expression("b_elg_v"),
        expression("s_elg_v"),
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|error| format!("预编译L2证据查询失败:{error}"))?;
    let mut rows = stmt
        .query(params![ts_code, DEFAULT_ADJ_TYPE, query_start, end_date])
        .map_err(|error| format!("查询L2证据失败:{error}"))?;
    let mut raw_by_date = BTreeMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取L2证据失败:{error}"))?
    {
        raw_by_date.insert(
            row.get::<_, String>(0)
                .map_err(|error| format!("读取L2证据日期失败:{error}"))?,
            CyqChenL2Raw {
                volume: row.get(1).map_err(|error| format!("读取V失败:{error}"))?,
                net_mf_volume: row
                    .get(2)
                    .map_err(|error| format!("读取NET_MF_V失败:{error}"))?,
                buy_small: row
                    .get(3)
                    .map_err(|error| format!("读取B_SM_V失败:{error}"))?,
                buy_medium: row
                    .get(4)
                    .map_err(|error| format!("读取B_MD_V失败:{error}"))?,
                buy_large: row
                    .get(5)
                    .map_err(|error| format!("读取B_LG_V失败:{error}"))?,
                sell_large: row
                    .get(6)
                    .map_err(|error| format!("读取S_LG_V失败:{error}"))?,
                buy_extra_large: row
                    .get(7)
                    .map_err(|error| format!("读取B_ELG_V失败:{error}"))?,
                sell_extra_large: row
                    .get(8)
                    .map_err(|error| format!("读取S_ELG_V失败:{error}"))?,
            },
        );
    }

    let ratio = |numerator: Option<f64>, denominator: Option<f64>| {
        numerator
            .filter(|value| value.is_finite())
            .zip(denominator.filter(|value| value.is_finite() && *value > 0.0))
            .map(|(numerator, denominator)| numerator / denominator)
    };
    let three_day_mean = |values: &[Option<f64>]| {
        (values.len() == 3)
            .then(|| {
                values
                    .iter()
                    .copied()
                    .collect::<Option<Vec<_>>>()
                    .filter(|values| values.iter().all(|value| value.is_finite()))
                    .map(|values| values.iter().sum::<f64>() / 3.0)
            })
            .flatten()
    };
    let board = board_category(ts_code, None);
    let mut days = Vec::new();
    for (trade_date, raw) in raw_by_date.iter().filter(|(trade_date, _)| {
        trade_date.as_str() >= start_date && trade_date.as_str() <= end_date
    }) {
        let large_buy = raw
            .buy_large
            .zip(raw.buy_extra_large)
            .map(|(large, extra_large)| large + extra_large)
            .filter(|value| value.is_finite());
        let large_sell = raw
            .sell_large
            .zip(raw.sell_extra_large)
            .map(|(large, extra_large)| large + extra_large)
            .filter(|value| value.is_finite());
        let large_buy_denominator = [
            raw.buy_small,
            raw.buy_medium,
            raw.buy_large,
            raw.buy_extra_large,
        ]
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .map(|values| values.into_iter().sum::<f64>())
        .filter(|value| value.is_finite() && *value > 0.0);
        let net_mf_ratio = ratio(raw.net_mf_volume, raw.volume);
        let large_net_ratio = large_buy
            .zip(large_sell)
            .and_then(|(buy, sell)| ratio(Some(buy - sell), raw.volume));
        let large_buy_share = large_buy.and_then(|buy| ratio(Some(buy), large_buy_denominator));
        let window = trade_calendar
            .iter()
            .position(|date| date == trade_date)
            .and_then(|index| {
                index
                    .checked_sub(2)
                    .map(|start| &trade_calendar[start..=index])
            })
            .filter(|window| window.len() == 3)
            .unwrap_or_default();
        let window_raw = window
            .iter()
            .map(|date| raw_by_date.get(date).copied().unwrap_or_default())
            .collect::<Vec<_>>();
        let net_mf_ratio_3d = three_day_mean(
            &window_raw
                .iter()
                .map(|raw| ratio(raw.net_mf_volume, raw.volume))
                .collect::<Vec<_>>(),
        );
        let large_net_ratio_3d = three_day_mean(
            &window_raw
                .iter()
                .map(|raw| {
                    raw.buy_large
                        .zip(raw.buy_extra_large)
                        .zip(raw.sell_large.zip(raw.sell_extra_large))
                        .and_then(
                            |((buy_large, buy_extra_large), (sell_large, sell_extra_large))| {
                                ratio(
                                    Some(
                                        buy_large + buy_extra_large - sell_large - sell_extra_large,
                                    ),
                                    raw.volume,
                                )
                            },
                        )
                })
                .collect::<Vec<_>>(),
        );
        let large_buy_share_3d = three_day_mean(
            &window_raw
                .iter()
                .map(|raw| {
                    let buy_large = raw
                        .buy_large
                        .zip(raw.buy_extra_large)
                        .map(|(large, extra_large)| large + extra_large);
                    let denominator = [
                        raw.buy_small,
                        raw.buy_medium,
                        raw.buy_large,
                        raw.buy_extra_large,
                    ]
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .map(|values| values.into_iter().sum::<f64>());
                    buy_large.and_then(|buy| ratio(Some(buy), denominator))
                })
                .collect::<Vec<_>>(),
        );

        let mut issues = Vec::new();
        if board == "北交所" {
            issues.push("北交所主动净量口径未核实，零值不等同于无方向".to_string());
        } else if !matches!(board, "主板" | "创业板" | "科创板") {
            issues.push("市场板块无法识别，L2口径未核实".to_string());
        }
        if raw.volume.is_none() {
            issues.push("成交量缺失，无法形成L2分母".to_string());
        } else if !raw
            .volume
            .is_some_and(|value| value.is_finite() && value > 0.0)
        {
            issues.push("成交量为零或非有限值，L2分母无效".to_string());
        }
        if raw.net_mf_volume.is_none() {
            issues.push("主动净量缺失".to_string());
        }
        if [
            raw.buy_large,
            raw.sell_large,
            raw.buy_extra_large,
            raw.sell_extra_large,
        ]
        .into_iter()
        .any(|value| value.is_none())
        {
            issues.push("大小单买卖量不完整".to_string());
        }
        if large_buy_denominator.is_none() {
            issues.push("买方分档合计缺失或无效".to_string());
        }
        if [
            raw.volume,
            raw.net_mf_volume,
            raw.buy_small,
            raw.buy_medium,
            raw.buy_large,
            raw.sell_large,
            raw.buy_extra_large,
            raw.sell_extra_large,
        ]
        .into_iter()
        .flatten()
        .any(|value| !value.is_finite())
        {
            issues.push("L2存在非有限数值".to_string());
        }
        let status = if matches!(board, "北交所" | "其他") {
            "unverified"
        } else if issues
            .iter()
            .any(|issue| issue.contains("无效") || issue.contains("非有限"))
        {
            "invalid"
        } else if issues.is_empty() {
            "observable"
        } else {
            "missing"
        };
        days.push(CyqChenL2Day {
            trade_date: trade_date.clone(),
            board: board.to_string(),
            status: status.to_string(),
            volume: raw.volume,
            net_mf_volume: raw.net_mf_volume,
            net_mf_ratio,
            net_mf_ratio_3d,
            large_buy_volume: large_buy,
            large_sell_volume: large_sell,
            large_net_ratio,
            large_net_ratio_3d,
            large_buy_denominator,
            large_buy_share,
            large_buy_share_3d,
            issues,
        });
    }
    Ok(days)
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
                tor DOUBLE,
                b_sm_v DOUBLE,
                b_md_v DOUBLE,
                b_lg_v DOUBLE,
                s_lg_v DOUBLE,
                b_elg_v DOUBLE,
                s_elg_v DOUBLE,
                net_mf_v DOUBLE
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
                    pre_close, change, pct_chg, vol, amount, tor,
                    b_sm_v, b_md_v, b_lg_v, s_lg_v, b_elg_v, s_elg_v, net_mf_v
                ) VALUES (
                    '000001.SZ', ?, 'qfq', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    40, 20, 20, 15, 10, 5, 5
                )
                "#,
                params![
                    trade_date, open, high, low, close, close, 0.0_f64, 0.0_f64, 100.0_f64,
                    1000.0_f64, tor
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
            compare_mechanisms: true,
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
                confirm_after: 0,
            }],
        })
        .expect("run single stock test");

        assert_eq!(data.resolved_ts_code, "000001.SZ");
        assert_eq!(data.kline.len(), 4);
        assert_eq!(data.output_start_date.as_deref(), Some("20260403"));
        assert_eq!(data.snapshots.len(), 2);
        let basis = data.basis.as_ref().unwrap();
        assert_eq!(basis.days.len(), 2);
        assert!(basis.days.iter().all(|day| day.values.is_some()));
        let comparison = data.mechanism_comparison.as_ref().unwrap();
        assert_eq!(
            serde_json::to_value(comparison).unwrap()["universeVerified"],
            false
        );
        assert_eq!(comparison.days.len(), data.snapshots.len());
        assert!(comparison.days[0].reference_unknown_ratio > 0.0);
        assert!(comparison.days[0].without_posterior_origin.is_some());
        assert!(data.snapshots[0].origin.is_some());
        assert!(data.snapshots[0].flow.is_some());
        let l2 = data
            .l2_days
            .iter()
            .find(|day| day.trade_date == "20260403")
            .expect("L2 evidence for output day");
        assert_eq!(l2.status, "observable");
        assert!((l2.net_mf_ratio.unwrap() - 0.05).abs() < 1e-9);
        assert!((l2.large_net_ratio.unwrap() - 0.1).abs() < 1e-9);
        assert!((l2.large_buy_share.unwrap() - 30.0 / 90.0).abs() < 1e-9);
        assert!((l2.net_mf_ratio_3d.unwrap() - 0.05).abs() < 1e-9);
        assert!(!source_dir.join("cyq_chen.db").exists());

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn single_stock_test_returns_empty_result_for_unknown_stock() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);

        let data = run_cyq_chen_single_stock_test(CyqChenSingleStockRequest {
            compare_mechanisms: true,
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
                confirm_after: 0,
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
