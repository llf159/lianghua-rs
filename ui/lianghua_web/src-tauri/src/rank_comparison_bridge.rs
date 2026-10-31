use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex, OnceLock,
};

use lianghua_app_strategy::rank_comparison::{
    read_rank_comparison_baseline as read_baseline, run_rank_comparison as run_comparison,
    RankComparisonRequest, RankComparisonResult,
};
use tauri::Emitter;

// 0 运行中，1 停止，2 撤回，3 结果提交中；提交与取消原子竞争。
static RANK_COMPARISON_TASKS: OnceLock<Mutex<HashMap<String, Arc<AtomicU8>>>> = OnceLock::new();

#[tauri::command]
pub fn edit_strategy_backtest_config(
    configuration: String,
    edit: Option<lianghua_app_strategy::manage::StrategyBacktestEdit>,
) -> Result<lianghua_app_strategy::manage::StrategyBacktestConfig, String> {
    lianghua_app_strategy::manage::edit_strategy_backtest_config(&configuration, edit)
}

#[tauri::command]
pub fn read_rank_comparison_baseline(source_path: String) -> Result<String, String> {
    let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
        .read()
        .unwrap_or_else(|e| e.into_inner());
    read_baseline(&source_path)
}

#[tauri::command]
pub fn cancel_rank_comparison(task_id: String, rollback: bool) -> Result<(), String> {
    let tasks = RANK_COMPARISON_TASKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| "排名对照任务锁已损坏".to_string())?;
    let flag = tasks
        .get(&task_id)
        .ok_or_else(|| "排名对照任务已结束或不存在".to_string())?;
    flag.compare_exchange(
        0,
        if rollback { 2 } else { 1 },
        Ordering::SeqCst,
        Ordering::SeqCst,
    )
    .map_err(|_| "任务已在停止或进入结果提交阶段".to_string())?;
    Ok(())
}

#[tauri::command]
pub async fn run_rank_comparison(
    app: tauri::AppHandle,
    request: RankComparisonRequest,
) -> Result<RankComparisonResult, String> {
    if request.task_id.is_empty() || request.task_id.len() > 128 {
        return Err("排名对照任务编号无效".to_string());
    }
    let settings = serde_json::to_value(&request).map_err(|e| e.to_string())?;
    let task_id = request.task_id.clone();
    let cancelled = Arc::new(AtomicU8::new(0));
    {
        let mut tasks = RANK_COMPARISON_TASKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| "排名对照任务锁已损坏".to_string())?;
        if tasks.contains_key(&task_id) {
            return Err("排名对照任务编号重复".to_string());
        }
        tasks.insert(task_id.clone(), cancelled.clone());
    }
    let worker_id = task_id.clone();
    let source_path = request.source_path.clone();
    let question = request.research_question.clone();
    let notes = request.research_notes.clone();
    let parent = request.parent_run_id.clone();
    let result = tauri::async_runtime::spawn_blocking(move || {
        let _execution = lianghua_app_strategy::research_archive::try_research_execution()?;
        let _archive_access =
            lianghua_app_strategy::research_archive::research_archive_lock(&source_path, false)?;
        let check_cancelled = || matches!(cancelled.load(Ordering::SeqCst), 1 | 2);
        let progress = |phase: &str| -> Result<(), String> {
            if check_cancelled() {
                return Err("排名对照已取消".to_string());
            }
            let _ = app.emit(
                "rank-comparison-progress",
                serde_json::json!({ "task_id": worker_id, "phase": phase }),
            );
            Ok(())
        };
        let journal = {
            let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
                .read()
                .unwrap_or_else(|e| e.into_inner());
            let manifest =
                lianghua_app_strategy::research_archive::research_input_manifest(&source_path)?;
            lianghua_app_strategy::research_archive::save_research_archive(
                &source_path,
                parent,
                question,
                serde_json::json!({"settings":settings,"notes":notes,"input_manifest":manifest}),
                serde_json::json!({}),
                "rank_comparison",
                "running",
                None,
                None,
            )?
        };
        let _lease = lianghua_app_strategy::research_archive::ResearchArchiveLease {
            source_path: source_path.clone(),
            run_id: journal.run_id.clone(),
        };
        let mut result = progress("等待仓库可用").and_then(|_| {
            if request.verify_inputs {
                let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
                    .write()
                    .unwrap_or_else(|e| e.into_inner());
                run_comparison(request, &progress, &check_cancelled)
            } else {
                let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
                    .read()
                    .unwrap_or_else(|e| e.into_inner());
                run_comparison(request, &progress, &check_cancelled)
            }
        });
        let cancellation = cancelled.swap(3, Ordering::SeqCst);
        let membership = result
            .as_ref()
            .ok()
            .and_then(|value| value.membership_staging.clone());
        if (cancellation == 1 || cancellation == 2)
            && !matches!(&result, Err(error) if error.starts_with("清理排名对照临时文件失败："))
        {
            result = Err("排名对照已取消".to_string());
        }
        let status = match &result {
            Ok(_) => "completed",
            Err(error) if error == "排名对照已取消" => "cancelled",
            Err(_) => "failed",
        };
        let payload = match &result {
            Ok(value) => {
                serde_json::to_value(value).map_err(|e| format!("序列化研究结果失败: {e}"))
            }
            Err(error) => Ok(serde_json::json!({ "status": status, "error": error })),
        };
        let saved = payload.and_then(|mut payload| {
            let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if status == "completed" {
                if let Some(path) = membership.as_deref() {
                    let destination =
                        lianghua_app_strategy::research_archive::research_artifact_path(
                            &source_path,
                            &journal.run_id,
                            "members.db",
                        )?;
                    std::fs::rename(path, destination).map_err(|e| e.to_string())?;
                    payload["membership_saved"] = serde_json::json!(true);
                    payload["archive_run_id"] = serde_json::json!(journal.run_id);
                }
            }
            lianghua_app_strategy::research_archive::finish_research_archive(
                &source_path,
                &journal.run_id,
                status,
                None,
                payload,
            )
        });
        let mut cleanup_errors = Vec::new();
        if let Some(path) = &membership {
            for file in [
                path.clone(),
                std::path::PathBuf::from(format!("{}.wal", path.to_string_lossy())),
            ] {
                if let Err(error) = std::fs::remove_file(&file) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        cleanup_errors.push(format!("{}: {error}", file.display()));
                    }
                }
            }
        }
        match (&mut result, saved) {
            (Ok(value), Ok(header)) => {
                value.membership_saved = header.membership_saved;
                value.archive_run_id = Some(header.run_id);
            }
            (Ok(value), Err(error)) => value.archive_error = Some(error),
            (Err(error), Err(archive_error)) => {
                error.push_str(&format!("；失败记录未保存: {archive_error}"))
            }
            (Err(_), Ok(_)) => {}
        }
        if cancellation == 2 {
            lianghua_app_strategy::research_archive::delete_research_archive(
                &source_path,
                &journal.run_id,
            )
            .map_err(|error| format!("任务已停止，但撤回研究存档失败：{error}"))?;
            if let Err(error) = &result {
                if error.starts_with("清理排名对照临时文件失败：") {
                    return Err(error.clone());
                }
            }
            if !cleanup_errors.is_empty() {
                return Err(format!(
                    "任务已停止，但清理成员临时文件失败：{}",
                    cleanup_errors.join("；")
                ));
            }
            return Err("排名对照已停止并撤回本次研究记录".into());
        }
        result
    })
    .await
    .map_err(|e| e.to_string());
    RANK_COMPARISON_TASKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| "排名对照任务锁已损坏".to_string())?
        .remove(&task_id);
    result?
}
