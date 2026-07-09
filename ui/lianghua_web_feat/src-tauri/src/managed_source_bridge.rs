use std::{
    io::{Read, Seek, Write},
    path::{Component, Path, PathBuf},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use lianghua_rs::ui_tools_feat::{
    data_import::{
        copy_directory_recursive, managed_source_file_name, resolve_managed_source_file_path,
        resolve_source_root, validate_target_relative_path,
    },
    data_viewer::{
        preview_managed_source_dataset as core_preview_managed_source_dataset,
        preview_managed_source_stock_data as core_preview_managed_source_stock_data,
        ManagedSourceDatasetPreviewResult, ManagedSourceDbPreviewResult,
    },
};
use serde::{Deserialize, Serialize};
use tauri::{Emitter, Manager};

fn decode_percent_encoded_path(raw: &str) -> String {
    if !raw.contains('%') {
        return raw.to_string();
    }

    let input = raw.as_bytes();
    let mut bytes = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'%' && i + 2 < input.len() {
            let hi = hex_val(input[i + 1]);
            let lo = hex_val(input[i + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                bytes.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        bytes.push(input[i]);
        i += 1;
    }

    String::from_utf8(bytes).unwrap_or_else(|_| raw.to_string())
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}
use tauri_plugin_fs::{FilePath, FsExt};
use zip::{write::FileOptions, CompressionMethod, ZipArchive, ZipWriter};

const MANAGED_SOURCE_IMPORT_EVENT: &str = "managed-source-import";
const IMPORT_BUFFER_SIZE: usize = 1024 * 1024;
const IMPORT_PROGRESS_STEP_BYTES: u64 = 32 * 1024 * 1024;
const STRATEGY_BACKUP_DIR_NAME: &str = "strategy_backups";
const STRATEGY_SNAPSHOT_DIR_NAME: &str = "strategy_snapshots";
const RANK_COMPUTE_SNAPSHOT_DIR_NAME: &str = "rank_compute";
const STRATEGY_RULE_FILE_NAME: &str = "score_rule.toml";
const STRATEGY_META_FILE_NAME: &str = "meta.json";
const STRATEGY_META_READ_RETRY_COUNT: usize = 3;
const STRATEGY_META_READ_RETRY_DELAY: Duration = Duration::from_millis(25);
const EMPTY_STRATEGY_TEMPLATE: &str = r#"version = 1

[[scene]]
name = "empty_scene"
direction = "long"
observe_threshold = 1.0
trigger_threshold = 2.0
confirm_threshold = 3.0
fail_threshold = 1.0

[[rule]]
name = "empty_rule"
scene = "empty_scene"
stage = "base"
scope_windows = 1
scope_way = "LAST"
when = "C > 99999999"
points = 0.0
explain = "空白模板占位规则，可直接编辑替换"
"#;

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedSourceImportEventPayload {
    import_id: String,
    target_relative_path: String,
    phase: String,
    bytes_copied: u64,
    total_bytes: Option<u64>,
    error: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSourceExportResult {
    source_path: String,
    exported_path: String,
    file_count: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSourceFileExportResult {
    file_id: String,
    file_name: String,
    source_path: String,
    exported_path: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSourceZipImportResult {
    source_path: String,
    imported_path: String,
    extracted_file_count: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedStrategyActiveFile {
    file_name: String,
    relative_path: String,
    absolute_path: String,
    exists: bool,
    modified_at: Option<String>,
    size_bytes: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedStrategyBackupItem {
    backup_id: String,
    folder_name: String,
    relative_path: String,
    absolute_path: String,
    created_at: String,
    modified_at: Option<String>,
    size_bytes: u64,
    source_kind: String,
    source_file_name: Option<String>,
    description: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedStrategyAssetsStatus {
    source_path: String,
    backup_root_path: String,
    active: ManagedStrategyActiveFile,
    backups: Vec<ManagedStrategyBackupItem>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedStrategyBundleExportResult {
    exported_path: String,
    backup_count: usize,
    includes_active_strategy: bool,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedStrategyBackupDiffLine {
    kind: String,
    backup_line: Option<usize>,
    active_line: Option<usize>,
    text: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedStrategyBackupDiff {
    backup_id: String,
    backup_label: String,
    active_label: String,
    changed_line_count: usize,
    lines: Vec<ManagedStrategyBackupDiffLine>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrategyBackupMeta {
    version: u32,
    created_at: String,
    source_kind: String,
    source_file_name: Option<String>,
    description: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StrategyAssetLocation {
    Backup,
    RankComputeSnapshot,
}

fn normalize_strategy_backup_description(description: &str) -> Result<Option<String>, String> {
    let trimmed = description.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if trimmed.chars().count() > 120 {
        return Err("策略说明不能超过 120 个字符".into());
    }

    Ok(Some(trimmed.to_string()))
}

fn emit_import_event(
    app: &tauri::AppHandle,
    import_id: &Option<String>,
    target_relative_path: &str,
    phase: &str,
    bytes_copied: u64,
    total_bytes: Option<u64>,
    error: Option<String>,
) {
    let Some(import_id) = import_id.as_ref() else {
        return;
    };

    let payload = ManagedSourceImportEventPayload {
        import_id: import_id.clone(),
        target_relative_path: target_relative_path.to_string(),
        phase: phase.to_string(),
        bytes_copied,
        total_bytes,
        error,
    };

    if let Err(emit_error) = app.emit(MANAGED_SOURCE_IMPORT_EVENT, payload) {
        log::warn!("failed to emit import event: {}", emit_error);
    }
}

fn append_directory_to_zip<W: Write + Seek>(
    zip_writer: &mut ZipWriter<W>,
    source_root: &Path,
    current_dir: &Path,
    archive_root: &str,
) -> Result<u64, String> {
    let file_options = zip_file_options();
    let mut file_count = 0u64;

    for entry in std::fs::read_dir(current_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let entry_path = entry.path();
        let entry_type = entry.file_type().map_err(|error| error.to_string())?;
        let relative_path = entry_path
            .strip_prefix(source_root)
            .map_err(|error| error.to_string())?;
        let archive_path = Path::new(archive_root).join(relative_path);
        let mut archive_name = archive_path.to_string_lossy().replace('\\', "/");

        if entry_type.is_dir() {
            if !archive_name.ends_with('/') {
                archive_name.push('/');
            }
            zip_writer
                .add_directory(archive_name, file_options)
                .map_err(|error| error.to_string())?;
            file_count +=
                append_directory_to_zip(zip_writer, source_root, &entry_path, archive_root)?;
            continue;
        }

        if entry_type.is_file() {
            zip_writer
                .start_file(archive_name, file_options)
                .map_err(|error| error.to_string())?;
            let mut source_file =
                std::fs::File::open(&entry_path).map_err(|error| error.to_string())?;
            std::io::copy(&mut source_file, zip_writer).map_err(|error| error.to_string())?;
            file_count += 1;
        }
    }

    Ok(file_count)
}

fn zip_file_options() -> FileOptions {
    FileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .large_file(true)
}

fn normalize_archive_root(source_dir: &str) -> String {
    let normalized = source_dir.trim().replace('\\', "/");
    let trimmed = normalized.trim_matches('/');
    if trimmed.is_empty() {
        "source".to_string()
    } else {
        trimmed.to_string()
    }
}

fn path_to_normalized_segments(path: &Path) -> Vec<String> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().to_string()),
            _ => None,
        })
        .collect()
}

fn strip_archive_root(entry_path: &Path, archive_root: &str) -> PathBuf {
    let entry_segments = path_to_normalized_segments(entry_path);
    let root_segments = path_to_normalized_segments(Path::new(archive_root));

    if !root_segments.is_empty()
        && entry_segments.len() >= root_segments.len()
        && entry_segments
            .iter()
            .take(root_segments.len())
            .zip(root_segments.iter())
            .all(|(left, right)| left == right)
    {
        return entry_segments.iter().skip(root_segments.len()).fold(
            PathBuf::new(),
            |mut current, segment| {
                current.push(segment);
                current
            },
        );
    }

    entry_path.to_path_buf()
}

fn import_managed_source_zip_inner(
    app: tauri::AppHandle,
    source_dir: String,
    source_path: String,
) -> Result<ManagedSourceZipImportResult, String> {
    let trimmed_source_path = source_path.trim();
    if trimmed_source_path.is_empty() {
        return Err("empty import source path".into());
    }

    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    let source_root = resolve_source_root(&app_data_root, &source_dir)?;
    std::fs::create_dir_all(&source_root).map_err(|error| error.to_string())?;

    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.read(true);
    let source_file_path =
        FilePath::from_str(trimmed_source_path).map_err(|error| error.to_string())?;
    let source_label = source_file_path.to_string();
    let source_file = app
        .fs()
        .open(source_file_path, open_options)
        .map_err(|error| error.to_string())?;
    let mut archive =
        ZipArchive::new(source_file).map_err(|error| format!("解析 ZIP 文件失败: {error}"))?;
    let archive_root = normalize_archive_root(&source_dir);

    let mut extracted_file_count = 0u64;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| format!("读取 ZIP 条目失败: {error}"))?;
        let entry_name = entry.name().replace('\\', "/");
        if entry_name.trim().is_empty()
            || entry_name == "__MACOSX"
            || entry_name.starts_with("__MACOSX/")
        {
            continue;
        }

        let enclosed_name = entry
            .enclosed_name()
            .ok_or_else(|| format!("ZIP 包含非法路径: {}", entry.name()))?;
        let relative_path = strip_archive_root(enclosed_name, &archive_root);
        if relative_path.as_os_str().is_empty() {
            continue;
        }

        let target_path = source_root.join(&relative_path);
        if entry.is_dir() {
            std::fs::create_dir_all(&target_path).map_err(|error| {
                format!(
                    "创建导入目录失败: path={}, err={error}",
                    target_path.display()
                )
            })?;
            continue;
        }

        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!("创建导入目录失败: path={}, err={error}", parent.display())
            })?;
        }

        let mut target = std::fs::File::create(&target_path).map_err(|error| {
            format!(
                "创建导入文件失败: path={}, err={error}",
                target_path.display()
            )
        })?;
        std::io::copy(&mut entry, &mut target).map_err(|error| {
            format!(
                "写入导入文件失败: path={}, err={error}",
                target_path.display()
            )
        })?;
        target.flush().map_err(|error| error.to_string())?;
        extracted_file_count += 1;
    }

    Ok(ManagedSourceZipImportResult {
        source_path: source_root.display().to_string(),
        imported_path: source_label,
        extracted_file_count,
    })
}

fn format_system_time(value: std::time::SystemTime) -> String {
    DateTime::<Utc>::from(value).to_rfc3339()
}

fn current_strategy_backup_id() -> String {
    Utc::now().format("%Y%m%d-%H%M%S-%3f").to_string()
}

fn managed_strategy_backup_root(source_root: &Path) -> PathBuf {
    source_root.join(STRATEGY_BACKUP_DIR_NAME)
}

fn managed_strategy_snapshot_root(source_root: &Path) -> PathBuf {
    source_root.join(STRATEGY_SNAPSHOT_DIR_NAME)
}

fn managed_rank_compute_snapshot_root(source_root: &Path) -> PathBuf {
    managed_strategy_snapshot_root(source_root).join(RANK_COMPUTE_SNAPSHOT_DIR_NAME)
}

fn managed_strategy_backup_dir(source_root: &Path, backup_id: &str) -> PathBuf {
    managed_strategy_backup_root(source_root).join(backup_id)
}

fn managed_rank_compute_snapshot_dir(source_root: &Path, backup_id: &str) -> PathBuf {
    managed_rank_compute_snapshot_root(source_root).join(backup_id)
}

fn managed_strategy_backup_file_path(source_root: &Path, backup_id: &str) -> PathBuf {
    managed_strategy_backup_dir(source_root, backup_id).join(STRATEGY_RULE_FILE_NAME)
}

fn managed_rank_compute_snapshot_file_path(source_root: &Path, backup_id: &str) -> PathBuf {
    managed_rank_compute_snapshot_dir(source_root, backup_id).join(STRATEGY_RULE_FILE_NAME)
}

fn managed_strategy_backup_meta_path(source_root: &Path, backup_id: &str) -> PathBuf {
    managed_strategy_backup_dir(source_root, backup_id).join(STRATEGY_META_FILE_NAME)
}

fn managed_rank_compute_snapshot_meta_path(source_root: &Path, backup_id: &str) -> PathBuf {
    managed_rank_compute_snapshot_dir(source_root, backup_id).join(STRATEGY_META_FILE_NAME)
}

fn managed_strategy_asset_dir(
    source_root: &Path,
    location: StrategyAssetLocation,
    backup_id: &str,
) -> PathBuf {
    match location {
        StrategyAssetLocation::Backup => managed_strategy_backup_dir(source_root, backup_id),
        StrategyAssetLocation::RankComputeSnapshot => {
            managed_rank_compute_snapshot_dir(source_root, backup_id)
        }
    }
}

fn managed_strategy_asset_file_path(
    source_root: &Path,
    location: StrategyAssetLocation,
    backup_id: &str,
) -> PathBuf {
    match location {
        StrategyAssetLocation::Backup => managed_strategy_backup_file_path(source_root, backup_id),
        StrategyAssetLocation::RankComputeSnapshot => {
            managed_rank_compute_snapshot_file_path(source_root, backup_id)
        }
    }
}

fn managed_strategy_asset_meta_path(
    source_root: &Path,
    location: StrategyAssetLocation,
    backup_id: &str,
) -> PathBuf {
    match location {
        StrategyAssetLocation::Backup => managed_strategy_backup_meta_path(source_root, backup_id),
        StrategyAssetLocation::RankComputeSnapshot => {
            managed_rank_compute_snapshot_meta_path(source_root, backup_id)
        }
    }
}

fn strategy_asset_relative_path(
    source_dir: &str,
    location: StrategyAssetLocation,
    backup_id: &str,
) -> String {
    let dir_name = match location {
        StrategyAssetLocation::Backup => STRATEGY_BACKUP_DIR_NAME,
        StrategyAssetLocation::RankComputeSnapshot => STRATEGY_SNAPSHOT_DIR_NAME,
    };
    let maybe_segment = match location {
        StrategyAssetLocation::Backup => None,
        StrategyAssetLocation::RankComputeSnapshot => Some(RANK_COMPUTE_SNAPSHOT_DIR_NAME),
    };
    let mut path = format!("{}/{}", source_dir.trim().trim_matches('/'), dir_name);
    if let Some(segment) = maybe_segment {
        path.push('/');
        path.push_str(segment);
    }
    path.push('/');
    path.push_str(backup_id);
    path.push('/');
    path.push_str(STRATEGY_RULE_FILE_NAME);
    path.trim_start_matches('/').to_string()
}

fn locate_strategy_asset(
    source_root: &Path,
    backup_id: &str,
) -> Option<(StrategyAssetLocation, PathBuf)> {
    let normalized_backup_id = validate_strategy_backup_id(backup_id).ok()?;
    for location in [
        StrategyAssetLocation::Backup,
        StrategyAssetLocation::RankComputeSnapshot,
    ] {
        let asset_dir = managed_strategy_asset_dir(source_root, location, normalized_backup_id);
        let file_path =
            managed_strategy_asset_file_path(source_root, location, normalized_backup_id);
        let meta_path =
            managed_strategy_asset_meta_path(source_root, location, normalized_backup_id);
        if asset_dir.is_dir() && file_path.is_file() && meta_path.is_file() {
            return Some((location, asset_dir));
        }
    }
    None
}

fn validate_strategy_backup_id(backup_id: &str) -> Result<&str, String> {
    let trimmed = backup_id.trim();
    if trimmed.is_empty() {
        return Err("策略备份 ID 不能为空".into());
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains("..") {
        return Err("策略备份 ID 非法".into());
    }
    Ok(trimmed)
}

fn read_strategy_backup_meta(
    source_root: &Path,
    backup_id: &str,
) -> Result<StrategyBackupMeta, String> {
    let meta_path = locate_strategy_asset(source_root, backup_id)
        .map(|(_, asset_dir)| asset_dir.join(STRATEGY_META_FILE_NAME))
        .unwrap_or_else(|| managed_strategy_backup_meta_path(source_root, backup_id));
    for attempt in 0..=STRATEGY_META_READ_RETRY_COUNT {
        let raw = std::fs::read_to_string(&meta_path).map_err(|error| {
            format!(
                "读取策略备份元数据失败: path={}, err={error}",
                meta_path.display()
            )
        })?;
        match serde_json::from_str(&raw) {
            Ok(meta) => return Ok(meta),
            Err(error) if error.is_eof() && attempt < STRATEGY_META_READ_RETRY_COUNT => {
                std::thread::sleep(STRATEGY_META_READ_RETRY_DELAY);
            }
            Err(error) => {
                return Err(format!(
                    "解析策略备份元数据失败: path={}, err={error}",
                    meta_path.display()
                ));
            }
        }
    }
    unreachable!("strategy meta read retry loop always returns")
}

fn write_strategy_backup_meta_atomically(meta_path: &Path, payload: &str) -> Result<(), String> {
    if let Some(parent) = meta_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let unique_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let temp_path = meta_path.with_file_name(format!(
        ".{}.{}.tmp",
        STRATEGY_META_FILE_NAME, unique_suffix
    ));
    let write_result = (|| {
        let mut temp_file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|error| error.to_string())?;
        temp_file
            .write_all(payload.as_bytes())
            .map_err(|error| error.to_string())?;
        temp_file.sync_all().map_err(|error| error.to_string())?;
        drop(temp_file);
        std::fs::rename(&temp_path, meta_path).map_err(|error| error.to_string())
    })();

    if write_result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    write_result
}

fn write_strategy_backup_meta(
    source_root: &Path,
    backup_id: &str,
    meta: &StrategyBackupMeta,
) -> Result<(), String> {
    let meta_path = locate_strategy_asset(source_root, backup_id)
        .map(|(_, asset_dir)| asset_dir.join(STRATEGY_META_FILE_NAME))
        .unwrap_or_else(|| managed_strategy_backup_meta_path(source_root, backup_id));
    let payload = serde_json::to_string_pretty(meta).map_err(|error| error.to_string())?;
    write_strategy_backup_meta_atomically(&meta_path, &payload).map_err(|error| {
        format!(
            "写入策略备份元数据失败: path={}, err={error}",
            meta_path.display()
        )
    })
}

fn files_have_same_content(left: &Path, right: &Path) -> Result<bool, String> {
    let left_metadata = std::fs::metadata(left).map_err(|error| {
        format!(
            "读取策略文件元数据失败: path={}, err={error}",
            left.display()
        )
    })?;
    let right_metadata = std::fs::metadata(right).map_err(|error| {
        format!(
            "读取策略备份元数据失败: path={}, err={error}",
            right.display()
        )
    })?;
    if left_metadata.len() != right_metadata.len() {
        return Ok(false);
    }

    let mut left_file = std::fs::File::open(left)
        .map_err(|error| format!("读取策略文件失败: path={}, err={error}", left.display()))?;
    let mut right_file = std::fs::File::open(right).map_err(|error| {
        format!(
            "读取策略备份文件失败: path={}, err={error}",
            right.display()
        )
    })?;
    let mut left_buffer = [0u8; 8192];
    let mut right_buffer = [0u8; 8192];

    loop {
        let left_len = left_file
            .read(&mut left_buffer)
            .map_err(|error| error.to_string())?;
        let right_len = right_file
            .read(&mut right_buffer)
            .map_err(|error| error.to_string())?;
        if left_len != right_len {
            return Ok(false);
        }
        if left_len == 0 {
            return Ok(true);
        }
        if left_buffer[..left_len] != right_buffer[..right_len] {
            return Ok(false);
        }
    }
}

fn has_equivalent_strategy_backup(
    source_root: &Path,
    active_file_path: &Path,
) -> Result<bool, String> {
    let backup_root = managed_strategy_backup_root(source_root);
    if !backup_root.exists() {
        return Ok(false);
    }

    for entry in std::fs::read_dir(&backup_root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if !entry
            .file_type()
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            continue;
        }
        let backup_file_path = entry.path().join(STRATEGY_RULE_FILE_NAME);
        if !backup_file_path.is_file() {
            continue;
        }
        match files_have_same_content(active_file_path, &backup_file_path) {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(error) => log::warn!("failed to compare strategy backup content: {error}"),
        }
    }

    Ok(false)
}

fn lcs_value(table: &[usize], width: usize, row: usize, col: usize) -> usize {
    table[row * width + col]
}

fn is_strategy_entry_header(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed == "[[rule]]" || trimmed == "[[scene]]"
}

fn strategy_entry_range_for_line(lines: &[&str], line_number: usize) -> Option<(usize, usize)> {
    if line_number == 0 || line_number > lines.len() {
        return None;
    }

    let index = line_number - 1;
    let start = (0..=index)
        .rev()
        .find(|&candidate| is_strategy_entry_header(lines[candidate]))
        .unwrap_or(index);
    let end = ((index + 1)..lines.len())
        .find(|&candidate| is_strategy_entry_header(lines[candidate]))
        .unwrap_or(lines.len());
    Some((start, end))
}

fn mark_strategy_entry_range(keep: &mut [bool], lines: &[&str], line_number: usize) {
    let Some((start, end)) = strategy_entry_range_for_line(lines, line_number) else {
        return;
    };
    keep[start..end].fill(true);
}

fn compact_strategy_backup_diff_lines_by_entry(
    lines: Vec<ManagedStrategyBackupDiffLine>,
    backup_lines: &[&str],
    active_lines: &[&str],
) -> Vec<ManagedStrategyBackupDiffLine> {
    if lines.is_empty() {
        return vec![ManagedStrategyBackupDiffLine {
            kind: "omitted".to_string(),
            backup_line: None,
            active_line: None,
            text: "没有可显示的内容".to_string(),
        }];
    }

    if lines.iter().all(|line| line.kind == "context") {
        return vec![ManagedStrategyBackupDiffLine {
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
            mark_strategy_entry_range(&mut keep_backup, backup_lines, line_number);
        }
        if let Some(line_number) = line.active_line {
            mark_strategy_entry_range(&mut keep_active, active_lines, line_number);
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
                compacted.push(ManagedStrategyBackupDiffLine {
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
        compacted.push(ManagedStrategyBackupDiffLine {
            kind: "omitted".to_string(),
            backup_line: None,
            active_line: None,
            text: format!("省略 {omitted_count} 行未变化策略"),
        });
    }

    compacted
}

fn build_strategy_backup_diff_lines(
    backup_text: &str,
    active_text: &str,
) -> (Vec<ManagedStrategyBackupDiffLine>, usize) {
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
            diff_lines.push(ManagedStrategyBackupDiffLine {
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
            diff_lines.push(ManagedStrategyBackupDiffLine {
                kind: "backup".to_string(),
                backup_line: Some(backup_index + 1),
                active_line: None,
                text: backup_lines[backup_index].to_string(),
            });
            changed_line_count += 1;
            backup_index += 1;
        } else {
            diff_lines.push(ManagedStrategyBackupDiffLine {
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
        diff_lines.push(ManagedStrategyBackupDiffLine {
            kind: "backup".to_string(),
            backup_line: Some(backup_index + 1),
            active_line: None,
            text: backup_lines[backup_index].to_string(),
        });
        changed_line_count += 1;
        backup_index += 1;
    }

    while active_index < cols {
        diff_lines.push(ManagedStrategyBackupDiffLine {
            kind: "active".to_string(),
            backup_line: None,
            active_line: Some(active_index + 1),
            text: active_lines[active_index].to_string(),
        });
        changed_line_count += 1;
        active_index += 1;
    }

    (
        compact_strategy_backup_diff_lines_by_entry(diff_lines, &backup_lines, &active_lines),
        changed_line_count,
    )
}

fn build_managed_strategy_backup_item(
    source_root: &Path,
    source_dir: &str,
    location: StrategyAssetLocation,
    backup_id: &str,
) -> Result<ManagedStrategyBackupItem, String> {
    let normalized_backup_id = validate_strategy_backup_id(backup_id)?;
    let file_path = managed_strategy_asset_file_path(source_root, location, normalized_backup_id);
    let metadata = std::fs::metadata(&file_path).map_err(|error| {
        format!(
            "读取策略备份文件失败: path={}, err={error}",
            file_path.display()
        )
    })?;
    let meta = read_strategy_backup_meta(source_root, normalized_backup_id)?;
    let modified_at = metadata.modified().ok().map(format_system_time);
    let relative_path = strategy_asset_relative_path(source_dir, location, normalized_backup_id);

    Ok(ManagedStrategyBackupItem {
        backup_id: normalized_backup_id.to_string(),
        folder_name: normalized_backup_id.to_string(),
        relative_path,
        absolute_path: file_path.display().to_string(),
        created_at: meta.created_at,
        modified_at,
        size_bytes: metadata.len(),
        source_kind: meta.source_kind,
        source_file_name: meta.source_file_name,
        description: meta.description,
    })
}

fn cleanup_rank_compute_strategy_snapshots(
    source_root: &Path,
    keep_backup_id: &str,
) -> Result<(), String> {
    let backup_root = managed_rank_compute_snapshot_root(source_root);
    if !backup_root.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(&backup_root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if !entry
            .file_type()
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            continue;
        }

        let backup_id = entry.file_name().to_string_lossy().to_string();
        if backup_id == keep_backup_id {
            continue;
        }

        let meta = match read_strategy_backup_meta(source_root, &backup_id) {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        if meta.source_kind != "rank_compute" {
            continue;
        }

        std::fs::remove_dir_all(entry.path()).map_err(|error| {
            format!(
                "清理旧排名计算快照失败: path={}, err={error}",
                entry.path().display()
            )
        })?;
    }

    Ok(())
}

pub(crate) fn snapshot_rank_compute_strategy(
    app_data_root: &Path,
    source_dir: &str,
    strategy_file_path: &Path,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<PathBuf, String> {
    if !strategy_file_path.exists() || !strategy_file_path.is_file() {
        return Err(format!(
            "用于计算的策略文件不存在: {}",
            strategy_file_path.display()
        ));
    }

    let source_root = resolve_source_root(app_data_root, source_dir)?;
    let backup_id = current_strategy_backup_id();
    let backup_dir = managed_rank_compute_snapshot_dir(&source_root, &backup_id);
    std::fs::create_dir_all(&backup_dir).map_err(|error| error.to_string())?;
    let backup_file_path = managed_rank_compute_snapshot_file_path(&source_root, &backup_id);
    std::fs::copy(strategy_file_path, &backup_file_path).map_err(|error| {
        format!(
            "复制排名计算策略快照失败: from={}, to={}, err={error}",
            strategy_file_path.display(),
            backup_file_path.display()
        )
    })?;

    let file_name = strategy_file_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(STRATEGY_RULE_FILE_NAME)
        .to_string();
    let range_text = match (start_date, end_date) {
        (Some(start), Some(end)) => format!("{start} 至 {end}"),
        _ => "未记录区间".to_string(),
    };
    let payload = serde_json::to_string_pretty(&StrategyBackupMeta {
        version: 1,
        created_at: Utc::now().to_rfc3339(),
        source_kind: "rank_compute".to_string(),
        source_file_name: Some(file_name),
        description: Some(format!("排名计算快照：{range_text}")),
    })
    .map_err(|error| error.to_string())?;
    let meta_path = managed_rank_compute_snapshot_meta_path(&source_root, &backup_id);
    write_strategy_backup_meta_atomically(&meta_path, &payload).map_err(|error| {
        format!(
            "写入策略备份元数据失败: path={}, err={error}",
            meta_path.display()
        )
    })?;
    cleanup_rank_compute_strategy_snapshots(&source_root, &backup_id)?;

    Ok(backup_file_path)
}

fn build_managed_strategy_active_file(
    source_root: &Path,
    source_dir: &str,
) -> ManagedStrategyActiveFile {
    let file_path = source_root.join(STRATEGY_RULE_FILE_NAME);
    let metadata = std::fs::metadata(&file_path).ok();
    let modified_at = metadata
        .as_ref()
        .and_then(|item| item.modified().ok())
        .map(format_system_time);
    let size_bytes = metadata.as_ref().map(std::fs::Metadata::len).unwrap_or(0);
    let relative_path = format!(
        "{}/{}",
        source_dir.trim().trim_matches('/'),
        STRATEGY_RULE_FILE_NAME
    )
    .trim_start_matches('/')
    .to_string();

    ManagedStrategyActiveFile {
        file_name: STRATEGY_RULE_FILE_NAME.to_string(),
        relative_path,
        absolute_path: file_path.display().to_string(),
        exists: metadata.is_some(),
        modified_at,
        size_bytes,
    }
}

fn get_managed_strategy_assets_status_inner(
    app_data_root: &Path,
    source_dir: &str,
) -> Result<ManagedStrategyAssetsStatus, String> {
    let source_root = resolve_source_root(app_data_root, source_dir)?;
    let backup_root = managed_strategy_backup_root(&source_root);
    let mut backups = Vec::new();

    if backup_root.exists() {
        for entry in std::fs::read_dir(&backup_root).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let entry_type = entry.file_type().map_err(|error| error.to_string())?;
            if !entry_type.is_dir() {
                continue;
            }

            let backup_id = entry.file_name().to_string_lossy().to_string();
            let file_path = managed_strategy_backup_file_path(&source_root, &backup_id);
            let meta_path = managed_strategy_backup_meta_path(&source_root, &backup_id);
            if !file_path.exists() || !meta_path.exists() {
                continue;
            }

            backups.push(build_managed_strategy_backup_item(
                &source_root,
                source_dir,
                StrategyAssetLocation::Backup,
                &backup_id,
            )?);
        }
    }

    let snapshot_root = managed_rank_compute_snapshot_root(&source_root);
    if snapshot_root.exists() {
        for entry in std::fs::read_dir(&snapshot_root).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let entry_type = entry.file_type().map_err(|error| error.to_string())?;
            if !entry_type.is_dir() {
                continue;
            }

            let backup_id = entry.file_name().to_string_lossy().to_string();
            let file_path = managed_rank_compute_snapshot_file_path(&source_root, &backup_id);
            let meta_path = managed_rank_compute_snapshot_meta_path(&source_root, &backup_id);
            if !file_path.exists() || !meta_path.exists() {
                continue;
            }

            backups.push(build_managed_strategy_backup_item(
                &source_root,
                source_dir,
                StrategyAssetLocation::RankComputeSnapshot,
                &backup_id,
            )?);
        }
    }

    backups.sort_by(|left, right| right.backup_id.cmp(&left.backup_id));

    Ok(ManagedStrategyAssetsStatus {
        source_path: source_root.display().to_string(),
        backup_root_path: backup_root.display().to_string(),
        active: build_managed_strategy_active_file(&source_root, source_dir),
        backups,
    })
}

fn copy_import_file_to_appdata_inner(
    app: tauri::AppHandle,
    source_path: String,
    target_relative_path: String,
    import_id: Option<String>,
) -> Result<(), String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("empty import source path".into());
    }
    validate_target_relative_path(&target_relative_path)?;

    let target_path = app
        .path()
        .resolve(&target_relative_path, tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;

    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.read(true);
    let source_file_path = FilePath::from_str(source_path).map_err(|error| error.to_string())?;
    let total_bytes = source_file_path
        .clone()
        .into_path()
        .ok()
        .and_then(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len());
    let mut source = app
        .fs()
        .open(source_file_path, open_options)
        .map_err(|error| error.to_string())?;
    let mut target = std::fs::File::create(&target_path).map_err(|error| error.to_string())?;
    let mut buffer = vec![0u8; IMPORT_BUFFER_SIZE];
    let mut bytes_copied = 0u64;
    let mut next_progress_threshold = IMPORT_PROGRESS_STEP_BYTES;

    emit_import_event(
        &app,
        &import_id,
        &target_relative_path,
        "started",
        0,
        total_bytes,
        None,
    );

    loop {
        let read_bytes = source
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read_bytes == 0 {
            break;
        }

        target
            .write_all(&buffer[..read_bytes])
            .map_err(|error| error.to_string())?;
        bytes_copied += read_bytes as u64;

        if bytes_copied >= next_progress_threshold {
            emit_import_event(
                &app,
                &import_id,
                &target_relative_path,
                "progress",
                bytes_copied,
                total_bytes,
                None,
            );
            next_progress_threshold = bytes_copied.saturating_add(IMPORT_PROGRESS_STEP_BYTES);
        }
    }

    target.flush().map_err(|error| error.to_string())?;
    target.sync_all().map_err(|error| error.to_string())?;

    emit_import_event(
        &app,
        &import_id,
        &target_relative_path,
        "completed",
        bytes_copied,
        total_bytes,
        None,
    );

    Ok(())
}

fn export_managed_source_file_inner(
    app: tauri::AppHandle,
    source_dir: String,
    file_id: String,
    destination_file: String,
) -> Result<ManagedSourceFileExportResult, String> {
    let destination_file = destination_file.trim();
    if destination_file.is_empty() {
        return Err("empty export destination file".into());
    }

    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    let (_, source_path) = resolve_managed_source_file_path(&app_data_root, &source_dir, &file_id)?;
    let normalized_file_id = file_id.trim();
    let file_name = managed_source_file_name(normalized_file_id)
        .ok_or_else(|| format!("未知文件项: {normalized_file_id}"))?;

    if !source_path.exists() {
        return Err(format!(
            "当前应用数据目录缺少文件: {}",
            source_path.display()
        ));
    }

    if !source_path.is_file() {
        return Err(format!(
            "当前应用数据目录目标不是文件: {}",
            source_path.display()
        ));
    }

    let mut source = std::fs::File::open(&source_path).map_err(|error| error.to_string())?;
    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.write(true).truncate(true).create(true);
    let destination_file =
        FilePath::from_str(destination_file).map_err(|error| error.to_string())?;
    let destination_label = decode_percent_encoded_path(&destination_file.to_string());
    let mut target = app
        .fs()
        .open(destination_file, open_options)
        .map_err(|error| error.to_string())?;

    let mut buffer = vec![0u8; IMPORT_BUFFER_SIZE];
    loop {
        let read_bytes = source
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read_bytes == 0 {
            break;
        }
        target
            .write_all(&buffer[..read_bytes])
            .map_err(|error| error.to_string())?;
    }

    target.flush().map_err(|error| error.to_string())?;

    Ok(ManagedSourceFileExportResult {
        file_id: normalized_file_id.to_string(),
        file_name: file_name.to_string(),
        source_path: source_path.display().to_string(),
        exported_path: destination_label,
    })
}

#[tauri::command]
pub fn allow_import_path(
    app: tauri::AppHandle,
    path: String,
    directory: bool,
    recursive: bool,
) -> Result<(), String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("empty import path".into());
    }

    let scope = app.fs_scope();
    if directory {
        scope
            .allow_directory(trimmed, recursive)
            .map_err(|error| error.to_string())?;
    } else {
        scope
            .allow_file(trimmed)
            .map_err(|error| error.to_string())?;
    }

    Ok(())
}

#[tauri::command]
pub async fn copy_import_file_to_appdata(
    app: tauri::AppHandle,
    source_path: String,
    target_relative_path: String,
    import_id: Option<String>,
) -> Result<(), String> {
    log::info!("starting import copy to {}", target_relative_path);
    let target_for_log = target_relative_path.clone();
    let import_id_for_error = import_id.clone();
    let app_for_copy = app.clone();
    let result = tauri::async_runtime::spawn_blocking(move || {
        copy_import_file_to_appdata_inner(
            app_for_copy,
            source_path,
            target_relative_path,
            import_id,
        )
    })
    .await
    .map_err(|error| error.to_string())?;

    if let Err(error) = &result {
        log::error!("import copy failed for {}: {}", target_for_log, error);
        emit_import_event(
            &app,
            &import_id_for_error,
            &target_for_log,
            "failed",
            0,
            None,
            Some(error.clone()),
        );
    } else {
        log::info!("finished import copy to {}", target_for_log);
    }

    result
}

#[tauri::command]
pub async fn preview_managed_source_stock_data(
    app: tauri::AppHandle,
    source_dir: String,
    trade_date: Option<String>,
    ts_code: Option<String>,
    limit: Option<usize>,
) -> Result<ManagedSourceDbPreviewResult, String> {
    let limit = limit.unwrap_or(100).clamp(20, 500);
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || {
        core_preview_managed_source_stock_data(
            &app_data_root,
            source_dir,
            trade_date,
            ts_code,
            limit,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn preview_managed_source_dataset(
    app: tauri::AppHandle,
    source_dir: String,
    dataset_id: String,
    trade_date: Option<String>,
    ts_code: Option<String>,
    limit: Option<usize>,
) -> Result<ManagedSourceDatasetPreviewResult, String> {
    let limit = limit.unwrap_or(100).clamp(20, 500);
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || {
        core_preview_managed_source_dataset(
            &app_data_root,
            source_dir,
            dataset_id,
            trade_date,
            ts_code,
            limit,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub fn export_managed_source_directory(
    app: tauri::AppHandle,
    source_dir: String,
    destination_dir: String,
) -> Result<ManagedSourceExportResult, String> {
    let destination_dir = destination_dir.trim();
    if destination_dir.is_empty() {
        return Err("empty export destination".into());
    }

    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    let source_path = resolve_source_root(&app_data_root, &source_dir)?;

    if !source_path.exists() {
        return Err(format!("当前应用数据目录不存在: {}", source_path.display()));
    }

    if !source_path.is_dir() {
        return Err(format!(
            "当前应用数据目录不是文件夹: {}",
            source_path.display()
        ));
    }

    let normalized_source_dir = source_dir.trim().replace('\\', "/");
    let export_root = normalized_source_dir
        .split('/')
        .filter(|segment| !segment.trim().is_empty())
        .fold(
            std::path::PathBuf::from(destination_dir),
            |current, segment| current.join(segment),
        );

    if export_root == source_path || export_root.starts_with(&source_path) {
        return Err("导出目录不能选在当前应用数据目录内部".into());
    }

    let file_count = copy_directory_recursive(&source_path, &export_root)?;

    Ok(ManagedSourceExportResult {
        source_path: source_path.display().to_string(),
        exported_path: export_root.display().to_string(),
        file_count,
    })
}

#[tauri::command]
pub fn export_managed_source_directory_mobile(
    app: tauri::AppHandle,
    source_dir: String,
    destination_file: String,
) -> Result<ManagedSourceExportResult, String> {
    let destination_file = destination_file.trim();
    if destination_file.is_empty() {
        return Err("empty export destination file".into());
    }

    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    let source_path = resolve_source_root(&app_data_root, &source_dir)?;

    if !source_path.exists() {
        return Err(format!("当前应用数据目录不存在: {}", source_path.display()));
    }

    if !source_path.is_dir() {
        return Err(format!(
            "当前应用数据目录不是文件夹: {}",
            source_path.display()
        ));
    }

    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.write(true).truncate(true).create(true);
    let destination_file =
        FilePath::from_str(destination_file).map_err(|error| error.to_string())?;
    let destination_label = decode_percent_encoded_path(&destination_file.to_string());
    let target_file = app
        .fs()
        .open(destination_file, open_options)
        .map_err(|error| error.to_string())?;
    let mut zip_writer = ZipWriter::new(target_file);
    let archive_root = source_dir.trim().replace('\\', "/");
    let archive_root = archive_root.trim_matches('/');
    let file_count = append_directory_to_zip(
        &mut zip_writer,
        &source_path,
        &source_path,
        if archive_root.is_empty() {
            "source"
        } else {
            archive_root
        },
    )?;
    zip_writer.finish().map_err(|error| error.to_string())?;

    Ok(ManagedSourceExportResult {
        source_path: source_path.display().to_string(),
        exported_path: destination_label,
        file_count,
    })
}

#[tauri::command]
pub async fn export_managed_source_file(
    app: tauri::AppHandle,
    source_dir: String,
    file_id: String,
    destination_file: String,
) -> Result<ManagedSourceFileExportResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        export_managed_source_file_inner(app, source_dir, file_id, destination_file)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn import_managed_source_zip(
    app: tauri::AppHandle,
    source_dir: String,
    source_path: String,
) -> Result<ManagedSourceZipImportResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        import_managed_source_zip_inner(app, source_dir, source_path)
    })
    .await
    .map_err(|error| error.to_string())?
}

fn import_strategy_backup_inner(
    app: tauri::AppHandle,
    source_dir: String,
    source_path: String,
) -> Result<ManagedStrategyBackupItem, String> {
    let trimmed_source_path = source_path.trim();
    if trimmed_source_path.is_empty() {
        return Err("empty import source path".into());
    }
    let source_file_name = Path::new(trimmed_source_path)
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "无法识别导入文件名".to_string())?
        .to_string();
    if !source_file_name.to_ascii_lowercase().ends_with(".toml") {
        return Err("仅支持导入 toml 策略文件".into());
    }

    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    let source_root = resolve_source_root(&app_data_root, &source_dir)?;
    let backup_id = current_strategy_backup_id();
    let target_relative_path = format!(
        "{}/{}/{}/{}",
        source_dir.trim().trim_matches('/'),
        STRATEGY_BACKUP_DIR_NAME,
        backup_id,
        STRATEGY_RULE_FILE_NAME
    )
    .trim_start_matches('/')
    .to_string();

    copy_import_file_to_appdata_inner(app, source_path, target_relative_path, None)?;
    write_strategy_backup_meta(
        &source_root,
        &backup_id,
        &StrategyBackupMeta {
            version: 1,
            created_at: Utc::now().to_rfc3339(),
            source_kind: "imported".to_string(),
            source_file_name: Some(source_file_name),
            description: Some("外部导入策略".to_string()),
        },
    )?;

    build_managed_strategy_backup_item(
        &source_root,
        &source_dir,
        StrategyAssetLocation::Backup,
        &backup_id,
    )
}

fn backup_active_strategy_with_meta(
    app_data_root: &Path,
    source_dir: String,
    source_kind: &str,
    description: &str,
) -> Result<ManagedStrategyBackupItem, String> {
    let source_root = resolve_source_root(app_data_root, &source_dir)?;
    let active_file_path = source_root.join(STRATEGY_RULE_FILE_NAME);
    if !active_file_path.exists() || !active_file_path.is_file() {
        return Err("当前没有可备份的生效策略文件".into());
    }

    let backup_id = current_strategy_backup_id();
    let backup_dir = managed_strategy_backup_dir(&source_root, &backup_id);
    std::fs::create_dir_all(&backup_dir).map_err(|error| error.to_string())?;
    let backup_file_path = backup_dir.join(STRATEGY_RULE_FILE_NAME);
    std::fs::copy(&active_file_path, &backup_file_path).map_err(|error| {
        format!(
            "复制当前策略到备份目录失败: from={}, to={}, err={error}",
            active_file_path.display(),
            backup_file_path.display()
        )
    })?;
    write_strategy_backup_meta(
        &source_root,
        &backup_id,
        &StrategyBackupMeta {
            version: 1,
            created_at: Utc::now().to_rfc3339(),
            source_kind: source_kind.to_string(),
            source_file_name: Some(STRATEGY_RULE_FILE_NAME.to_string()),
            description: Some(description.to_string()),
        },
    )?;

    build_managed_strategy_backup_item(
        &source_root,
        &source_dir,
        StrategyAssetLocation::Backup,
        &backup_id,
    )
}

fn backup_active_strategy_inner(
    app_data_root: &Path,
    source_dir: String,
) -> Result<ManagedStrategyBackupItem, String> {
    backup_active_strategy_with_meta(app_data_root, source_dir, "backup", "手动备份当前生效策略")
}

fn auto_backup_active_strategy_on_entry_inner(
    app_data_root: &Path,
    source_dir: String,
) -> Result<Option<ManagedStrategyBackupItem>, String> {
    let source_root = resolve_source_root(app_data_root, &source_dir)?;
    let active_file_path = source_root.join(STRATEGY_RULE_FILE_NAME);
    if !active_file_path.exists() || !active_file_path.is_file() {
        return Ok(None);
    }
    if has_equivalent_strategy_backup(&source_root, &active_file_path)? {
        return Ok(None);
    }

    backup_active_strategy_with_meta(
        app_data_root,
        source_dir,
        "auto_entry",
        "自动备份：进入策略管理页",
    )
    .map(Some)
}

fn get_strategy_backup_diff_inner(
    app_data_root: &Path,
    source_dir: String,
    backup_id: String,
) -> Result<ManagedStrategyBackupDiff, String> {
    let source_root = resolve_source_root(app_data_root, &source_dir)?;
    let normalized_backup_id = validate_strategy_backup_id(&backup_id)?.to_string();
    let backup_file_path = locate_strategy_asset(&source_root, &normalized_backup_id)
        .map(|(_, asset_dir)| asset_dir.join(STRATEGY_RULE_FILE_NAME))
        .ok_or_else(|| "目标备份策略不存在".to_string())?;
    if !backup_file_path.exists() || !backup_file_path.is_file() {
        return Err("目标备份策略不存在".into());
    }

    let active_file_path = source_root.join(STRATEGY_RULE_FILE_NAME);
    if !active_file_path.exists() || !active_file_path.is_file() {
        return Err("当前没有可对比的生效策略文件".into());
    }

    let backup_text = std::fs::read_to_string(&backup_file_path).map_err(|error| {
        format!(
            "读取备份策略失败: path={}, err={error}",
            backup_file_path.display()
        )
    })?;
    let active_text = std::fs::read_to_string(&active_file_path).map_err(|error| {
        format!(
            "读取当前生效策略失败: path={}, err={error}",
            active_file_path.display()
        )
    })?;
    let (lines, changed_line_count) = build_strategy_backup_diff_lines(&backup_text, &active_text);

    Ok(ManagedStrategyBackupDiff {
        backup_id: normalized_backup_id.clone(),
        backup_label: format!("{normalized_backup_id}/{}", STRATEGY_RULE_FILE_NAME),
        active_label: STRATEGY_RULE_FILE_NAME.to_string(),
        changed_line_count,
        lines,
    })
}

fn create_empty_strategy_backup_inner(
    app_data_root: &Path,
    source_dir: String,
) -> Result<ManagedStrategyBackupItem, String> {
    let source_root = resolve_source_root(app_data_root, &source_dir)?;
    let backup_id = current_strategy_backup_id();
    let backup_dir = managed_strategy_backup_dir(&source_root, &backup_id);
    std::fs::create_dir_all(&backup_dir).map_err(|error| error.to_string())?;
    let backup_file_path = backup_dir.join(STRATEGY_RULE_FILE_NAME);
    std::fs::write(&backup_file_path, EMPTY_STRATEGY_TEMPLATE).map_err(|error| {
        format!(
            "写入空白策略模板失败: path={}, err={error}",
            backup_file_path.display()
        )
    })?;
    write_strategy_backup_meta(
        &source_root,
        &backup_id,
        &StrategyBackupMeta {
            version: 1,
            created_at: Utc::now().to_rfc3339(),
            source_kind: "empty".to_string(),
            source_file_name: Some(STRATEGY_RULE_FILE_NAME.to_string()),
            description: Some("空白模板策略".to_string()),
        },
    )?;

    build_managed_strategy_backup_item(
        &source_root,
        &source_dir,
        StrategyAssetLocation::Backup,
        &backup_id,
    )
}

fn activate_strategy_backup_inner(
    app_data_root: &Path,
    source_dir: String,
    backup_id: String,
) -> Result<ManagedStrategyAssetsStatus, String> {
    let source_root = resolve_source_root(app_data_root, &source_dir)?;
    let normalized_backup_id = validate_strategy_backup_id(&backup_id)?.to_string();
    let backup_file_path = locate_strategy_asset(&source_root, &normalized_backup_id)
        .map(|(_, asset_dir)| asset_dir.join(STRATEGY_RULE_FILE_NAME))
        .ok_or_else(|| "目标备份策略不存在".to_string())?;
    if !backup_file_path.exists() || !backup_file_path.is_file() {
        return Err("目标备份策略不存在".into());
    }
    let active_file_path = source_root.join(STRATEGY_RULE_FILE_NAME);
    if let Some(parent) = active_file_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    std::fs::copy(&backup_file_path, &active_file_path).map_err(|error| {
        format!(
            "设为生效失败: from={}, to={}, err={error}",
            backup_file_path.display(),
            active_file_path.display()
        )
    })?;
    get_managed_strategy_assets_status_inner(app_data_root, &source_dir)
}

fn delete_strategy_backup_inner(
    app_data_root: &Path,
    source_dir: String,
    backup_id: String,
) -> Result<ManagedStrategyAssetsStatus, String> {
    let source_root = resolve_source_root(app_data_root, &source_dir)?;
    let normalized_backup_id = validate_strategy_backup_id(&backup_id)?.to_string();
    let backup_dir = locate_strategy_asset(&source_root, &normalized_backup_id)
        .map(|(_, asset_dir)| asset_dir)
        .unwrap_or_else(|| managed_strategy_backup_dir(&source_root, &normalized_backup_id));
    if backup_dir.exists() {
        std::fs::remove_dir_all(&backup_dir).map_err(|error| {
            format!(
                "删除策略备份失败: path={}, err={error}",
                backup_dir.display()
            )
        })?;
    }
    get_managed_strategy_assets_status_inner(app_data_root, &source_dir)
}

fn update_strategy_backup_description_inner(
    app_data_root: &Path,
    source_dir: String,
    backup_id: String,
    description: String,
) -> Result<ManagedStrategyAssetsStatus, String> {
    let source_root = resolve_source_root(app_data_root, &source_dir)?;
    let normalized_backup_id = validate_strategy_backup_id(&backup_id)?.to_string();
    let normalized_description = normalize_strategy_backup_description(&description)?;

    let mut meta = read_strategy_backup_meta(&source_root, &normalized_backup_id)?;
    meta.description = normalized_description;
    write_strategy_backup_meta(&source_root, &normalized_backup_id, &meta)?;

    get_managed_strategy_assets_status_inner(app_data_root, &source_dir)
}

fn export_strategy_backup_file_inner(
    app: tauri::AppHandle,
    source_dir: String,
    backup_id: String,
    destination_file: String,
) -> Result<ManagedSourceFileExportResult, String> {
    let destination_file = destination_file.trim();
    if destination_file.is_empty() {
        return Err("empty export destination file".into());
    }

    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    let source_root = resolve_source_root(&app_data_root, &source_dir)?;
    let normalized_backup_id = validate_strategy_backup_id(&backup_id)?.to_string();
    let source_path = locate_strategy_asset(&source_root, &normalized_backup_id)
        .map(|(_, asset_dir)| asset_dir.join(STRATEGY_RULE_FILE_NAME))
        .ok_or_else(|| "目标备份策略不存在".to_string())?;
    if !source_path.exists() || !source_path.is_file() {
        return Err("目标备份策略不存在".into());
    }

    let mut source = std::fs::File::open(&source_path).map_err(|error| error.to_string())?;
    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.write(true).truncate(true).create(true);
    let destination_file =
        FilePath::from_str(destination_file).map_err(|error| error.to_string())?;
    let destination_label = decode_percent_encoded_path(&destination_file.to_string());
    let mut target = app
        .fs()
        .open(destination_file, open_options)
        .map_err(|error| error.to_string())?;
    let mut buffer = vec![0u8; IMPORT_BUFFER_SIZE];
    loop {
        let read_bytes = source
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read_bytes == 0 {
            break;
        }
        target
            .write_all(&buffer[..read_bytes])
            .map_err(|error| error.to_string())?;
    }

    target.flush().map_err(|error| error.to_string())?;

    Ok(ManagedSourceFileExportResult {
        file_id: normalized_backup_id,
        file_name: STRATEGY_RULE_FILE_NAME.to_string(),
        source_path: source_path.display().to_string(),
        exported_path: destination_label,
    })
}

fn export_strategy_bundle_inner(
    app: tauri::AppHandle,
    source_dir: String,
    destination_file: String,
) -> Result<ManagedStrategyBundleExportResult, String> {
    let destination_file = destination_file.trim();
    if destination_file.is_empty() {
        return Err("empty export destination file".into());
    }

    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    let source_root = resolve_source_root(&app_data_root, &source_dir)?;
    let active_file_path = source_root.join(STRATEGY_RULE_FILE_NAME);
    let backup_root = managed_strategy_backup_root(&source_root);

    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.write(true).truncate(true).create(true);
    let destination_file =
        FilePath::from_str(destination_file).map_err(|error| error.to_string())?;
    let destination_label = decode_percent_encoded_path(&destination_file.to_string());
    let target_file = app
        .fs()
        .open(destination_file, open_options)
        .map_err(|error| error.to_string())?;
    let mut zip_writer = ZipWriter::new(target_file);
    let file_options = zip_file_options();

    let includes_active_strategy = active_file_path.exists() && active_file_path.is_file();
    if includes_active_strategy {
        zip_writer
            .start_file(format!("active/{STRATEGY_RULE_FILE_NAME}"), file_options)
            .map_err(|error| error.to_string())?;
        let mut source_file =
            std::fs::File::open(&active_file_path).map_err(|error| error.to_string())?;
        std::io::copy(&mut source_file, &mut zip_writer).map_err(|error| error.to_string())?;
    }

    let mut backup_count = 0usize;
    if backup_root.exists() && backup_root.is_dir() {
        backup_count =
            append_directory_to_zip(&mut zip_writer, &backup_root, &backup_root, "backups")?
                as usize;
    }

    zip_writer.finish().map_err(|error| error.to_string())?;

    Ok(ManagedStrategyBundleExportResult {
        exported_path: destination_label,
        backup_count,
        includes_active_strategy,
    })
}

#[tauri::command]
pub fn get_managed_strategy_assets_status(
    app: tauri::AppHandle,
    source_dir: String,
) -> Result<ManagedStrategyAssetsStatus, String> {
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    get_managed_strategy_assets_status_inner(&app_data_root, &source_dir)
}

#[tauri::command]
pub async fn import_managed_strategy_backup(
    app: tauri::AppHandle,
    source_dir: String,
    source_path: String,
) -> Result<ManagedStrategyBackupItem, String> {
    tauri::async_runtime::spawn_blocking(move || {
        import_strategy_backup_inner(app, source_dir, source_path)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_archive_root, strip_archive_root, write_strategy_backup_meta_atomically,
        StrategyBackupMeta,
    };
    use std::{
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn strip_archive_root_removes_exported_source_prefix() {
        assert_eq!(
            strip_archive_root(Path::new("source/stock_data.db"), "source"),
            Path::new("stock_data.db")
        );
        assert_eq!(
            strip_archive_root(
                Path::new("source/strategy_backups/20240101/meta.json"),
                "source"
            ),
            Path::new("strategy_backups/20240101/meta.json")
        );
    }

    #[test]
    fn strip_archive_root_keeps_rootless_paths() {
        assert_eq!(
            strip_archive_root(Path::new("stock_list.csv"), "source"),
            Path::new("stock_list.csv")
        );
    }

    #[test]
    fn normalize_archive_root_defaults_to_source() {
        assert_eq!(normalize_archive_root(""), "source");
        assert_eq!(normalize_archive_root("source"), "source");
        assert_eq!(normalize_archive_root("/nested/source/"), "nested/source");
    }

    #[test]
    fn write_strategy_backup_meta_atomically_replaces_empty_meta() {
        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let temp_dir = std::env::temp_dir().join(format!(
            "lianghua-strategy-meta-test-{}-{unique_suffix}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let meta_path = temp_dir.join("meta.json");
        std::fs::write(&meta_path, "").unwrap();

        let payload = r#"{
  "version": 1,
  "createdAt": "2026-06-06T01:14:16Z",
  "sourceKind": "auto_entry",
  "sourceFileName": "score_rule.toml",
  "description": "自动备份：进入策略管理页"
}"#;
        write_strategy_backup_meta_atomically(&meta_path, payload).unwrap();

        let raw = std::fs::read_to_string(&meta_path).unwrap();
        let meta: StrategyBackupMeta = serde_json::from_str(&raw).unwrap();
        assert_eq!(meta.source_kind, "auto_entry");
        assert!(std::fs::read_dir(&temp_dir).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")));

        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}

#[tauri::command]
pub async fn backup_managed_active_strategy(
    app: tauri::AppHandle,
    source_dir: String,
) -> Result<ManagedStrategyBackupItem, String> {
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || {
        backup_active_strategy_inner(&app_data_root, source_dir)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn auto_backup_managed_active_strategy_on_entry(
    app: tauri::AppHandle,
    source_dir: String,
) -> Result<Option<ManagedStrategyBackupItem>, String> {
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || {
        auto_backup_active_strategy_on_entry_inner(&app_data_root, source_dir)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn get_managed_strategy_backup_diff(
    app: tauri::AppHandle,
    source_dir: String,
    backup_id: String,
) -> Result<ManagedStrategyBackupDiff, String> {
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || {
        get_strategy_backup_diff_inner(&app_data_root, source_dir, backup_id)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn create_managed_empty_strategy_backup(
    app: tauri::AppHandle,
    source_dir: String,
) -> Result<ManagedStrategyBackupItem, String> {
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || {
        create_empty_strategy_backup_inner(&app_data_root, source_dir)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn activate_managed_strategy_backup(
    app: tauri::AppHandle,
    source_dir: String,
    backup_id: String,
) -> Result<ManagedStrategyAssetsStatus, String> {
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || {
        activate_strategy_backup_inner(&app_data_root, source_dir, backup_id)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn delete_managed_strategy_backup(
    app: tauri::AppHandle,
    source_dir: String,
    backup_id: String,
) -> Result<ManagedStrategyAssetsStatus, String> {
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || {
        delete_strategy_backup_inner(&app_data_root, source_dir, backup_id)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn update_managed_strategy_backup_description(
    app: tauri::AppHandle,
    source_dir: String,
    backup_id: String,
    description: String,
) -> Result<ManagedStrategyAssetsStatus, String> {
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || {
        update_strategy_backup_description_inner(&app_data_root, source_dir, backup_id, description)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn export_managed_strategy_backup_file(
    app: tauri::AppHandle,
    source_dir: String,
    backup_id: String,
    destination_file: String,
) -> Result<ManagedSourceFileExportResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        export_strategy_backup_file_inner(app, source_dir, backup_id, destination_file)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn export_managed_strategy_bundle(
    app: tauri::AppHandle,
    source_dir: String,
    destination_file: String,
) -> Result<ManagedStrategyBundleExportResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        export_strategy_bundle_inner(app, source_dir, destination_file)
    })
    .await
    .map_err(|error| error.to_string())?
}
