use lianghua_model::task::{ActiveTask, TaskControl, TaskState};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Condvar, Mutex, OnceLock,
    },
};
use tauri::Emitter;

struct Session {
    id: String,
    lane: &'static str,
    source: PathBuf,
    control: Arc<TaskControl>,
    sender: mpsc::Sender<Command>,
    busy: AtomicBool,
    done: (Mutex<Option<Result<(), String>>>, Condvar),
}
enum Command {
    Job(Box<dyn FnOnce() + Send>),
    Finish,
}
struct Entry {
    session: Arc<Session>,
    control: Arc<TaskControl>,
}
static ENTRIES: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
static SESSIONS: OnceLock<Mutex<HashMap<&'static str, Arc<Session>>>> = OnceLock::new();

fn report(app: &tauri::AppHandle, id: &str, message: &str) {
    let _ = app.emit(
        "task-control-status",
        serde_json::json!({"taskId":id,"message":message}),
    );
}

async fn begin(
    app: tauri::AppHandle,
    id: String,
    source: String,
    lane: &'static str,
) -> Result<Arc<Session>, String> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err("任务编号无效".into());
    }
    let source = fs::canonicalize(source).map_err(|e| e.to_string())?;
    let (sender, receiver) = mpsc::channel();
    let session = Arc::new(Session {
        id: id.clone(),
        lane,
        source,
        control: Arc::new(TaskControl::default()),
        sender,
        busy: AtomicBool::new(false),
        done: (Mutex::new(None), Condvar::new()),
    });
    {
        let mut active = SESSIONS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if active.contains_key(lane) {
            return Err("同类任务正在运行，请先停止".into());
        }
        let mut entries = ENTRIES.get_or_init(Default::default).lock().unwrap();
        if entries.contains_key(&id) {
            return Err("任务编号重复".into());
        }
        active.insert(lane, session.clone());
        entries.insert(
            id.clone(),
            Entry {
                session: session.clone(),
                control: session.control.clone(),
            },
        );
    }
    let worker = session.clone();
    let (ready, waiting) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut initialized = false;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> Result<TaskState, String> {
                let _execution = lianghua_app_strategy::research_archive::try_data_execution()
                    .map_err(|error| {
                        let _ = ready.send(Err(error.clone()));
                        error
                    })?;
                // 生命周期内阻止云端恢复或移动仓库；普通读取仍可继续。
                let _source_guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
                    .read()
                    .unwrap_or_else(|e| e.into_inner());
                initialized = true;
                let _ = ready.send(Ok(()));
                loop {
                    match receiver.recv_timeout(std::time::Duration::from_secs(300)) {
                        Ok(Command::Job(job)) => job(),
                        Ok(Command::Finish) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            let _ = worker.control.request();
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                Ok(worker.control.seal())
            },
        ));
        let panicked = result.is_err();
        let result = match result {
            Ok(Ok(TaskState::Stop)) => Err("任务已停止，保留已提交结果".into()),
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(_) => Err("后台任务线程异常退出；请重启应用后继续，已提交结果保留".into()),
        };
        worker.busy.store(false, Ordering::SeqCst);
        // 线程异常时可能仍有计算子线程退出中，保留占用直到应用重启。
        if !panicked {
            SESSIONS
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(worker.lane);
        }
        *worker.done.0.lock().unwrap() = Some(result.clone());
        worker.done.1.notify_all();
        if !initialized {
            ENTRIES
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .remove(&id);
        }
        report(
            &app,
            &id,
            result
                .as_ref()
                .err()
                .map(String::as_str)
                .unwrap_or("任务已完成"),
        );
    });
    tauri::async_runtime::spawn_blocking(move || waiting.recv().map_err(|e| e.to_string())?)
        .await
        .map_err(|e| e.to_string())??;
    Ok(session)
}

#[tauri::command]
pub async fn begin_task_group(
    app: tauri::AppHandle,
    task_id: String,
    source_path: String,
) -> Result<(), String> {
    begin(app, task_id, source_path, "main").await.map(|_| ())
}

#[tauri::command]
pub fn control_task(app: tauri::AppHandle, task_id: String, rollback: bool) -> Result<(), String> {
    if rollback {
        return Err("此任务直接分批写入正式库，不支持撤回；停止会保留已提交结果".into());
    }
    let entries = ENTRIES.get_or_init(Default::default).lock().unwrap();
    let entry = entries.get(&task_id).ok_or("任务已结束或不存在")?;
    if entry.session.done.0.lock().unwrap().is_some() {
        return Err("任务已结束".into());
    }
    entry.control.request()?;
    if task_id == entry.session.id {
        let _ = entry.session.sender.send(Command::Finish);
    }
    report(
        &app,
        &task_id,
        "已请求停止，正在结束当前批次；已提交结果会保留",
    );
    Ok(())
}

async fn finish(session: Arc<Session>) -> Result<(), String> {
    let _ = session.sender.send(Command::Finish);
    tauri::async_runtime::spawn_blocking(move || {
        let mut done = session.done.0.lock().unwrap();
        while done.is_none() {
            done = session.done.1.wait(done).unwrap();
        }
        let result = done.clone().unwrap();
        drop(done);
        ENTRIES
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .retain(|_, entry| !Arc::ptr_eq(&entry.session, &session));
        result
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn finish_task_group(task_id: String) -> Result<(), String> {
    let session = ENTRIES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .get(&task_id)
        .map(|entry| entry.session.clone())
        .ok_or("工作流已结束")?;
    if session.id != task_id {
        return Err("工作流编号不匹配".into());
    }
    finish(session).await
}

pub async fn run<T: Send + 'static>(
    app: tauri::AppHandle,
    id: String,
    source: String,
    group: Option<String>,
    action: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    run_in_lane(app, id, source, group, "main", action).await
}

pub async fn run_in_lane<T: Send + 'static>(
    app: tauri::AppHandle,
    id: String,
    source: String,
    group: Option<String>,
    lane: &'static str,
    action: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    if lane != "main" && group.is_some() {
        return Err("独立下载不能加入串行工作流".into());
    }
    let grouped = group.is_some();
    let session = if let Some(group) = group {
        let entries = ENTRIES.get_or_init(Default::default).lock().unwrap();
        let entry = entries.get(&group).ok_or("工作流已停止或不存在")?;
        if entry.session.id != group
            || entry.session.lane != lane
            || fs::canonicalize(&source).map_err(|e| e.to_string())? != entry.session.source
        {
            return Err("工作流与任务数据目录不匹配".into());
        }
        entry.control.check()?;
        entry.session.clone()
    } else {
        begin(app.clone(), id.clone(), source, lane).await?
    };
    if session.busy.swap(true, Ordering::SeqCst) {
        return Err("工作流中已有阶段正在执行".into());
    }
    let control = if grouped {
        Arc::new(TaskControl::default())
    } else {
        session.control.clone()
    };
    if grouped {
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            session.busy.store(false, Ordering::SeqCst);
            return Err("任务编号无效".into());
        }
        let mut entries = ENTRIES.get_or_init(Default::default).lock().unwrap();
        if entries.contains_key(&id) {
            session.busy.store(false, Ordering::SeqCst);
            return Err("任务编号重复".into());
        }
        entries.insert(
            id.clone(),
            Entry {
                session: session.clone(),
                control: control.clone(),
            },
        );
    }
    let worker = session.clone();
    let cleanup_id = id.clone();
    let (reply, result) = mpsc::sync_channel(1);
    let submitted = session.sender.send(Command::Job(Box::new(move || {
        let outcome = (|| {
            worker.control.check()?;
            report(&app, &id, "正在执行任务");
            let active = if worker.lane == "main" {
                ActiveTask::enter(vec![worker.control.clone(), control.clone()])
            } else {
                ActiveTask::enter_isolated(vec![worker.control.clone(), control.clone()])
            };
            let output = action();
            drop(active);
            let mode = if grouped {
                control.seal()
            } else {
                control.state()
            };
            if mode == TaskState::Stop {
                Err("任务已停止，保留已提交结果".into())
            } else {
                output
            }
        })();
        if grouped {
            ENTRIES
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .remove(&id);
        }
        worker.busy.store(false, Ordering::SeqCst);
        let _ = reply.send(outcome);
    })));
    if submitted.is_err() {
        session.busy.store(false, Ordering::SeqCst);
        if grouped {
            ENTRIES
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .remove(&cleanup_id);
        }
        if !grouped {
            finish(session).await?;
        }
        return Err("工作流已停止，未启动后续阶段".into());
    }
    let output =
        tauri::async_runtime::spawn_blocking(move || result.recv().map_err(|e| e.to_string())?)
            .await
            .map_err(|e| e.to_string())?;
    if !grouped {
        finish(session).await?;
    }
    output
}
