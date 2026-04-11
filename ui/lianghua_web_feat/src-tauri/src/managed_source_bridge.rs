use std::{
    io::{Read, Seek, Write},
    path::Path,
    str::FromStr,
};

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
use serde::Serialize;
use tauri::{Emitter, Manager};
use tauri_plugin_fs::{FilePath, FsExt};
use zip::{CompressionMethod, ZipWriter, write::FileOptions};

const MANAGED_SOURCE_IMPORT_EVENT: &str = "managed-source-import";
const IMPORT_BUFFER_SIZE: usize = 1024 * 1024;
const IMPORT_PROGRESS_STEP_BYTES: u64 = 32 * 1024 * 1024;

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
