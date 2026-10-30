use lianghua_app_strategy::research_archive::{
    delete_research_archive as delete_archive, list_research_archives as list_archives,
    read_research_archive as read_archive, save_research_archive as save_archive,
    ResearchArchiveHeader, ResearchArchiveRun,
};
use serde_json::Value;

#[tauri::command]
pub fn list_research_archives(source_path: String) -> Result<Vec<ResearchArchiveHeader>, String> {
    let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
        .read()
        .unwrap_or_else(|e| e.into_inner());
    list_archives(&source_path)
}

#[tauri::command]
pub async fn save_research_archive(
    source_path: String,
    parent_run_id: Option<String>,
    question: String,
    configuration: Value,
    result: Value,
    kind: Option<String>,
    membership_source_run_id: Option<String>,
    status: Option<String>,
) -> Result<ResearchArchiveHeader, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
            .read()
            .unwrap_or_else(|e| e.into_inner());
        save_archive(
            &source_path,
            parent_run_id,
            question,
            configuration,
            result,
            kind.as_deref().unwrap_or("expression_validation"),
            status.as_deref().unwrap_or("completed"),
            None,
            membership_source_run_id.as_deref(),
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn read_research_archive(
    source_path: String,
    run_id: String,
) -> Result<ResearchArchiveRun, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
            .read()
            .unwrap_or_else(|e| e.into_inner());
        read_archive(&source_path, &run_id)
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub fn delete_research_archive(source_path: String, run_id: String) -> Result<(), String> {
    let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
        .read()
        .unwrap_or_else(|e| e.into_inner());
    delete_archive(&source_path, &run_id)
}

#[tauri::command]
pub async fn list_rank_comparison_members(
    source_path: String,
    run_id: String,
    start_date: String,
    end_date: String,
    pool_kind: String,
    top_k: usize,
    membership: String,
    offset: usize,
    limit: usize,
    dimension: Option<String>,
    bucket: Option<String>,
) -> Result<lianghua_app_strategy::research_archive::RankComparisonMemberPage, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
            .read()
            .unwrap_or_else(|e| e.into_inner());
        lianghua_app_strategy::research_archive::list_rank_comparison_members(
            &source_path,
            &run_id,
            &start_date,
            &end_date,
            &pool_kind,
            top_k,
            &membership,
            offset,
            limit,
            dimension.as_deref().zip(bucket.as_deref()),
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn list_research_evidence(
    source_path: String,
    query: lianghua_app_strategy::research_evidence::ResearchEvidenceQuery,
) -> Result<lianghua_app_strategy::research_evidence::ResearchEvidencePage, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
            .read()
            .unwrap_or_else(|e| e.into_inner());
        lianghua_app_strategy::research_evidence::list_research_evidence(&source_path, &query)
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn summarize_research_evidence(
    source_path: String,
    query: lianghua_app_strategy::research_evidence::ResearchEvidenceQuery,
    frequency: String,
) -> Result<Vec<lianghua_app_strategy::research_evidence::ResearchEvidenceSummary>, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
            .read()
            .unwrap_or_else(|e| e.into_inner());
        lianghua_app_strategy::research_evidence::summarize_research_evidence(
            &source_path,
            &query,
            &frequency,
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn export_research_archive(
    app: tauri::AppHandle,
    source_path: String,
    run_id: String,
    destination_file: Option<String>,
) -> Result<String, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
            .read()
            .unwrap_or_else(|e| e.into_inner());
        {
            use std::str::FromStr;
            use tauri_plugin_fs::{FilePath, FsExt};
            let path = lianghua_app_strategy::research_archive::export_research_archive(
                &source_path,
                &run_id,
            )?;
            if let Some(destination) = destination_file {
                let mut options = tauri_plugin_fs::OpenOptions::new();
                options.write(true).truncate(true).create(true);
                let mut target = app
                    .fs()
                    .open(
                        FilePath::from_str(&destination).map_err(|e| e.to_string())?,
                        options,
                    )
                    .map_err(|e| e.to_string())?;
                let mut source = std::fs::File::open(path).map_err(|e| e.to_string())?;
                std::io::copy(&mut source, &mut target).map_err(|e| e.to_string())?;
                Ok(destination)
            } else {
                Ok(path)
            }
        }
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn import_research_archive(
    app: tauri::AppHandle,
    source_path: String,
    package_path: String,
) -> Result<ResearchArchiveHeader, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
            .read()
            .unwrap_or_else(|e| e.into_inner());
        use std::str::FromStr;
        use tauri_plugin_fs::{FilePath, FsExt};
        let mut options = tauri_plugin_fs::OpenOptions::new();
        options.read(true);
        let file = app
            .fs()
            .open(
                FilePath::from_str(&package_path).map_err(|e| e.to_string())?,
                options,
            )
            .map_err(|e| e.to_string())?;
        lianghua_app_strategy::research_archive::import_research_archive(&source_path, file)
    })
    .await
    .map_err(|e| e.to_string())?
}
