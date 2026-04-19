use std::{
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

use chrono::{DateTime, Utc};
use lianghua_rs::ui_tools_feat::{
    data_import::{
        copy_directory_recursive, managed_source_file_name, resolve_managed_source_file_path,
        resolve_source_root, validate_target_relative_path,
    },
    data_viewer::{
        ManagedSourceDatasetPreviewResult, ManagedSourceDbPreviewResult,
        preview_managed_source_dataset as core_preview_managed_source_dataset,
        preview_managed_source_stock_data as core_preview_managed_source_stock_data,
    },
};
use serde::{Deserialize, Serialize};
use tauri::{Emitter, Manager};
use tauri_plugin_fs::{FilePath, FsExt};
use zip::{CompressionMethod, ZipWriter, write::FileOptions};

const MANAGED_SOURCE_IMPORT_EVENT: &str = "managed-source-import";
const IMPORT_BUFFER_SIZE: usize = 1024 * 1024;
const IMPORT_PROGRESS_STEP_BYTES: u64 = 32 * 1024 * 1024;
const STRATEGY_BACKUP_DIR_NAME: &str = "strategy_backups";
const STRATEGY_RULE_FILE_NAME: &str = "score_rule.toml";
const STRATEGY_META_FILE_NAME: &str = "meta.json";
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrategyBackupMeta {
    version: u32,
    created_at: String,
    source_kind: String,
    source_file_name: Option<String>,
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
    let file_options = FileOptions::default().compression_method(CompressionMethod::Deflated);
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

fn format_system_time(value: std::time::SystemTime) -> String {
    DateTime::<Utc>::from(value).to_rfc3339()
}

fn current_strategy_backup_id() -> String {
    Utc::now().format("%Y%m%d-%H%M%S-%3f").to_string()
}

fn managed_strategy_backup_root(source_root: &Path) -> PathBuf {
    source_root.join(STRATEGY_BACKUP_DIR_NAME)
}

fn managed_strategy_backup_dir(source_root: &Path, backup_id: &str) -> PathBuf {
    managed_strategy_backup_root(source_root).join(backup_id)
}

fn managed_strategy_backup_file_path(source_root: &Path, backup_id: &str) -> PathBuf {
    managed_strategy_backup_dir(source_root, backup_id).join(STRATEGY_RULE_FILE_NAME)
}

fn managed_strategy_backup_meta_path(source_root: &Path, backup_id: &str) -> PathBuf {
    managed_strategy_backup_dir(source_root, backup_id).join(STRATEGY_META_FILE_NAME)
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

fn read_strategy_backup_meta(source_root: &Path, backup_id: &str) -> Result<StrategyBackupMeta, String> {
    let meta_path = managed_strategy_backup_meta_path(source_root, backup_id);
    let raw = std::fs::read_to_string(&meta_path)
        .map_err(|error| format!("读取策略备份元数据失败: path={}, err={error}", meta_path.display()))?;
    serde_json::from_str(&raw).map_err(|error| {
        format!(
            "解析策略备份元数据失败: path={}, err={error}",
            meta_path.display()
        )
    })
}

fn write_strategy_backup_meta(
    source_root: &Path,
    backup_id: &str,
    meta: &StrategyBackupMeta,
) -> Result<(), String> {
    let meta_path = managed_strategy_backup_meta_path(source_root, backup_id);
    if let Some(parent) = meta_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let payload = serde_json::to_string_pretty(meta).map_err(|error| error.to_string())?;
    std::fs::write(&meta_path, payload).map_err(|error| {
        format!(
            "写入策略备份元数据失败: path={}, err={error}",
            meta_path.display()
        )
    })
}

fn build_managed_strategy_backup_item(
    source_root: &Path,
    source_dir: &str,
    backup_id: &str,
) -> Result<ManagedStrategyBackupItem, String> {
    let normalized_backup_id = validate_strategy_backup_id(backup_id)?;
    let file_path = managed_strategy_backup_file_path(source_root, normalized_backup_id);
    let metadata = std::fs::metadata(&file_path).map_err(|error| {
        format!(
            "读取策略备份文件失败: path={}, err={error}",
            file_path.display()
        )
    })?;
    let meta = read_strategy_backup_meta(source_root, normalized_backup_id)?;
    let modified_at = metadata.modified().ok().map(format_system_time);
    let relative_path = format!(
        "{}/{}/{}/{}",
        source_dir.trim().trim_matches('/'),
        STRATEGY_BACKUP_DIR_NAME,
        normalized_backup_id,
        STRATEGY_RULE_FILE_NAME
    )
    .trim_start_matches('/')
    .to_string();

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
    })
}

fn build_managed_strategy_active_file(source_root: &Path, source_dir: &str) -> ManagedStrategyActiveFile {
    let file_path = source_root.join(STRATEGY_RULE_FILE_NAME);
    let metadata = std::fs::metadata(&file_path).ok();
    let modified_at = metadata
        .as_ref()
        .and_then(|item| item.modified().ok())
        .map(format_system_time);
    let size_bytes = metadata.as_ref().map(std::fs::Metadata::len).unwrap_or(0);
    let relative_path = format!("{}/{}", source_dir.trim().trim_matches('/'), STRATEGY_RULE_FILE_NAME)
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
        let read_bytes = source.read(&mut buffer).map_err(|error| error.to_string())?;
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
    let destination_label = destination_file.to_string();
    let mut target = app
        .fs()
        .open(destination_file, open_options)
        .map_err(|error| error.to_string())?;

    let mut buffer = vec![0u8; IMPORT_BUFFER_SIZE];
    loop {
        let read_bytes = source.read(&mut buffer).map_err(|error| error.to_string())?;
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
        core_preview_managed_source_stock_data(&app_data_root, source_dir, trade_date, ts_code, limit)
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
        .fold(std::path::PathBuf::from(destination_dir), |current, segment| {
            current.join(segment)
        });

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
    let destination_label = destination_file.to_string();
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
        },
    )?;

    build_managed_strategy_backup_item(&source_root, &source_dir, &backup_id)
}

fn backup_active_strategy_inner(
    app_data_root: &Path,
    source_dir: String,
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
            source_kind: "backup".to_string(),
            source_file_name: Some(STRATEGY_RULE_FILE_NAME.to_string()),
        },
    )?;

    build_managed_strategy_backup_item(&source_root, &source_dir, &backup_id)
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
        },
    )?;

    build_managed_strategy_backup_item(&source_root, &source_dir, &backup_id)
}

fn activate_strategy_backup_inner(
    app_data_root: &Path,
    source_dir: String,
    backup_id: String,
) -> Result<ManagedStrategyAssetsStatus, String> {
    let source_root = resolve_source_root(app_data_root, &source_dir)?;
    let normalized_backup_id = validate_strategy_backup_id(&backup_id)?.to_string();
    let backup_file_path = managed_strategy_backup_file_path(&source_root, &normalized_backup_id);
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
    let backup_dir = managed_strategy_backup_dir(&source_root, &normalized_backup_id);
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
    let source_path = managed_strategy_backup_file_path(&source_root, &normalized_backup_id);
    if !source_path.exists() || !source_path.is_file() {
        return Err("目标备份策略不存在".into());
    }

    let mut source = std::fs::File::open(&source_path).map_err(|error| error.to_string())?;
    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.write(true).truncate(true).create(true);
    let destination_file =
        FilePath::from_str(destination_file).map_err(|error| error.to_string())?;
    let destination_label = destination_file.to_string();
    let mut target = app
        .fs()
        .open(destination_file, open_options)
        .map_err(|error| error.to_string())?;
    let mut buffer = vec![0u8; IMPORT_BUFFER_SIZE];
    loop {
        let read_bytes = source.read(&mut buffer).map_err(|error| error.to_string())?;
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
    let destination_label = destination_file.to_string();
    let target_file = app
        .fs()
        .open(destination_file, open_options)
        .map_err(|error| error.to_string())?;
    let mut zip_writer = ZipWriter::new(target_file);
    let file_options = FileOptions::default().compression_method(CompressionMethod::Deflated);

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
        backup_count = append_directory_to_zip(&mut zip_writer, &backup_root, &backup_root, "backups")? as usize;
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

#[tauri::command]
pub async fn backup_managed_active_strategy(
    app: tauri::AppHandle,
    source_dir: String,
) -> Result<ManagedStrategyBackupItem, String> {
    let app_data_root = app
        .path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    tauri::async_runtime::spawn_blocking(move || backup_active_strategy_inner(&app_data_root, source_dir))
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
