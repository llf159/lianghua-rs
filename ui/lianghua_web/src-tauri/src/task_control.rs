use lianghua_model::task::{ActiveTask, TaskControl, TaskState};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Condvar, Mutex, OnceLock,
    },
};
use tauri::Emitter;

struct Session {
    id: String,
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
static SESSION: Mutex<Option<Arc<Session>>> = Mutex::new(None);

fn report(app: &tauri::AppHandle, id: &str, message: &str) {
    let _ = app.emit(
        "task-control-status",
        serde_json::json!({"taskId":id,"message":message}),
    );
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    scope: String,
    state: String,
    files: BTreeSet<PathBuf>,
}
struct Snapshot {
    source: PathBuf,
    path: PathBuf,
    manifest: Manifest,
}
impl Snapshot {
    fn includes(scope: &str, relative: &Path) -> bool {
        let name = relative
            .components()
            .next()
            .map(|v| v.as_os_str().to_string_lossy())
            .unwrap_or_default();
        match scope {
            "indicator" => name.starts_with("stock_data.db") || name == "ind.toml",
            "score" => {
                name.starts_with("scoring_result.db")
                    || name.starts_with("stock_data.db")
                    || name == "strategy_snapshots"
            }
            "cyq" => name.starts_with("cyq.db") || name.starts_with("stock_data.db"),
            "cyq-chen" => {
                name.contains("cyq_chen.db")
                    || name.starts_with("stock_data.db")
                    || name == "chip_change_rule_backups"
            }
            "similarity" => {
                name.starts_with("strategy_similarity.db") || name.starts_with("stock_data.db")
            }
            _ => {
                name.contains(".db")
                    || name.ends_with(".csv")
                    || name.ends_with(".toml")
                    || name == "strategy_snapshots"
                    || name == "chip_change_rule_backups"
            }
        }
    }
    fn files(source: &Path, scope: &str) -> Result<BTreeSet<PathBuf>, String> {
        let mut files = BTreeSet::new();
        let mut pending = vec![PathBuf::new()];
        while let Some(relative) = pending.pop() {
            for entry in fs::read_dir(source.join(&relative)).map_err(|e| e.to_string())? {
                let entry = entry.map_err(|e| e.to_string())?;
                let relative = relative.join(entry.file_name());
                if !Self::includes(scope, &relative) {
                    continue;
                }
                let kind = entry.file_type().map_err(|e| e.to_string())?;
                if kind.is_symlink() {
                    return Err(format!("撤回快照不支持符号链接：{}", relative.display()));
                }
                if kind.is_dir() {
                    pending.push(relative);
                } else if kind.is_file() {
                    files.insert(relative);
                }
            }
        }
        Ok(files)
    }
    fn save(&self) -> Result<(), String> {
        let mut file =
            fs::File::create(self.path.join("manifest.pending")).map_err(|e| e.to_string())?;
        serde_json::to_writer(&mut file, &self.manifest).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        fs::rename(
            self.path.join("manifest.pending"),
            self.path.join("manifest.json"),
        )
        .map_err(|e| e.to_string())
    }
    fn create(
        source: &Path,
        path: PathBuf,
        scope: &str,
        control: &TaskControl,
    ) -> Result<Self, String> {
        control.check()?;
        fs::create_dir_all(path.join("files")).map_err(|e| format!("创建撤回快照失败：{e}"))?;
        for relative in Self::files(source, scope)? {
            control.check()?;
            let name = relative.file_name().unwrap().to_string_lossy();
            if name.ends_with(".db")
                || name.ends_with(".db.resume.tmp")
                || name.ends_with(".db.maintenance.tmp")
            {
                let conn = duckdb::Connection::open(source.join(&relative))
                    .map_err(|e| format!("准备数据库快照失败 {}：{e}", relative.display()))?;
                conn.execute_batch("CHECKPOINT")
                    .map_err(|e| format!("数据库快照落盘失败：{e}"))?;
            }
        }
        let files = Self::files(source, scope)?;
        for relative in &files {
            control.check()?;
            let destination = path.join("files").join(relative);
            fs::create_dir_all(destination.parent().unwrap()).map_err(|e| e.to_string())?;
            let mut input = fs::File::open(source.join(relative)).map_err(|e| e.to_string())?;
            let mut output = fs::File::create(destination).map_err(|e| e.to_string())?;
            let mut buffer = vec![0_u8; 1024 * 1024];
            loop {
                control.check()?;
                let size = input.read(&mut buffer).map_err(|e| e.to_string())?;
                if size == 0 {
                    break;
                }
                output
                    .write_all(&buffer[..size])
                    .map_err(|e| format!("写入撤回快照失败（请检查剩余空间）：{e}"))?;
            }
            output.sync_all().map_err(|e| e.to_string())?;
        }
        let snapshot = Self {
            source: source.to_owned(),
            path,
            manifest: Manifest {
                scope: scope.into(),
                state: "ready".into(),
                files,
            },
        };
        snapshot.save()?;
        Ok(snapshot)
    }
    fn restore(&mut self) -> Result<(), String> {
        self.manifest.state = "restoring".into();
        self.save()?;
        for relative in
            Self::files(&self.source, &self.manifest.scope)?.difference(&self.manifest.files)
        {
            fs::remove_file(self.source.join(relative))
                .map_err(|e| format!("删除本次新增文件失败：{e}"))?;
        }
        for relative in &self.manifest.files {
            let destination = self.source.join(relative);
            fs::create_dir_all(destination.parent().unwrap()).map_err(|e| e.to_string())?;
            let temporary = destination.with_file_name(format!(
                "{}.task-restore",
                destination.file_name().unwrap().to_string_lossy()
            ));
            fs::copy(self.path.join("files").join(relative), &temporary)
                .map_err(|e| format!("恢复文件失败，快照已保留：{e}"))?;
            fs::File::open(&temporary)
                .and_then(|f| f.sync_all())
                .map_err(|e| e.to_string())?;
            fs::rename(temporary, destination).map_err(|e| format!("替换撤回文件失败：{e}"))?;
        }
        self.commit()
    }
    fn commit(&mut self) -> Result<(), String> {
        self.manifest.state = "committed".into();
        self.save()
    }
    fn recover(source: &Path) -> Result<(), String> {
        let directory = source.join(".task-rollback");
        if !directory.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(&directory).map_err(|e| e.to_string())? {
            let path = entry.map_err(|e| e.to_string())?.path();
            let manifest = path.join("root/manifest.json");
            if manifest.exists() {
                let manifest: Manifest =
                    serde_json::from_reader(fs::File::open(manifest).map_err(|e| e.to_string())?)
                        .map_err(|e| e.to_string())?;
                if manifest.state != "committed" {
                    let mut snapshot = Self {
                        source: source.to_owned(),
                        path: path.join("root"),
                        manifest,
                    };
                    snapshot.restore()?;
                }
            }
            fs::remove_dir_all(path).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

async fn begin(
    app: tauri::AppHandle,
    id: String,
    source: String,
    scope: String,
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
        source,
        control: Arc::new(TaskControl::default()),
        sender,
        busy: AtomicBool::new(false),
        done: (Mutex::new(None), Condvar::new()),
    });
    {
        let mut active = SESSION.lock().unwrap_or_else(|e| e.into_inner());
        if active.is_some() {
            return Err("已有可写任务正在运行，请先停止或撤回".into());
        }
        *active = Some(session.clone());
    }
    ENTRIES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(
            id.clone(),
            Entry {
                session: session.clone(),
                control: session.control.clone(),
            },
        );
    let worker = session.clone();
    let (ready, waiting) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        // 共享源锁阻止云端恢复/仓库迁移；可写任务另由 SESSION 串行，普通读请求仍可完成。
        let _source_guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let result = (|| -> Result<(), String> {
            report(&app, &id, "正在准备撤回快照");
            Snapshot::recover(&worker.source)?;
            let path = worker.source.join(".task-rollback").join(&id);
            let mut snapshot = match Snapshot::create(
                &worker.source,
                path.join("root"),
                &scope,
                &worker.control,
            ) {
                Ok(value) => value,
                Err(error) => {
                    let _ = fs::remove_dir_all(&path);
                    return Err(error);
                }
            };
            let _ = ready.send(Ok(()));
            report(&app, &id, "撤回快照已就绪");
            while let Ok(command) = receiver.recv() {
                match command {
                    Command::Job(job) => job(),
                    Command::Finish => break,
                }
            }
            let mode = worker.control.seal();
            if mode == TaskState::Rollback {
                report(&app, &id, "正在恢复运行前的数据，请等待恢复完成");
                snapshot.restore()?;
            } else {
                snapshot.commit()?;
            }
            fs::remove_dir_all(&path)
                .map_err(|e| format!("任务已结束，但清理撤回快照失败：{e}"))?;
            match mode {
                TaskState::Rollback => Err("任务已撤回，已恢复运行前的数据".into()),
                TaskState::Stop => Err("任务已停止，保留已提交结果".into()),
                _ => Ok(()),
            }
        })();
        let _ = ready.send(result.clone());
        *worker.done.0.lock().unwrap() = Some(result.clone());
        worker.done.1.notify_all();
        ENTRIES
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .remove(&id);
        // 恢复失败时保留占用，防止新写入覆盖现场；重新启动后先恢复快照。
        if result
            .as_ref()
            .err()
            .is_none_or(|e| e.starts_with("任务已停止") || e.starts_with("任务已撤回"))
        {
            *SESSION.lock().unwrap_or_else(|e| e.into_inner()) = None;
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
    begin(app, task_id, source_path, "download".into())
        .await
        .map(|_| ())
}

#[tauri::command]
pub fn control_task(app: tauri::AppHandle, task_id: String, rollback: bool) -> Result<(), String> {
    let entries = ENTRIES.get_or_init(Default::default).lock().unwrap();
    let entry = entries.get(&task_id).ok_or("任务已结束或不存在")?;
    entry.control.request(rollback)?;
    if task_id == entry.session.id {
        let _ = entry.session.sender.send(Command::Finish);
    }
    report(
        &app,
        &task_id,
        if rollback {
            "已请求撤回，正在停止后台计算"
        } else {
            "已请求停止，保留已提交结果"
        },
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
        done.clone().unwrap()
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn finish_task_group(task_id: String) -> Result<(), String> {
    let session = SESSION
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
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
    scope: &str,
    group: Option<String>,
    action: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    let grouped = group.is_some();
    let session = if let Some(group) = group {
        let entries = ENTRIES.get_or_init(Default::default).lock().unwrap();
        let entry = entries.get(&group).ok_or("工作流已停止或不存在")?;
        if entry.session.id != group
            || fs::canonicalize(&source).map_err(|e| e.to_string())? != entry.session.source
        {
            return Err("工作流与任务数据目录不匹配".into());
        }
        entry.control.check()?;
        entry.session.clone()
    } else {
        begin(app.clone(), id.clone(), source, scope.into()).await?
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
    let scope = scope.to_owned();
    let (reply, result) = mpsc::sync_channel(1);
    session
        .sender
        .send(Command::Job(Box::new(move || {
            let outcome = (|| {
                worker.control.check()?;
                let mut snapshot = if grouped {
                    report(&app, &id, "正在准备本阶段的撤回快照");
                    Some(Snapshot::create(
                        &worker.source,
                        worker
                            .source
                            .join(".task-rollback")
                            .join(&worker.id)
                            .join(&id),
                        &scope,
                        &control,
                    )?)
                } else {
                    None
                };
                report(&app, &id, "正在执行任务");
                let _active = ActiveTask::enter(vec![worker.control.clone(), control.clone()]);
                let output = action();
                drop(_active);
                let mode = if grouped {
                    control.seal()
                } else {
                    control.state()
                };
                if let Some(snapshot) = &mut snapshot {
                    if mode == TaskState::Rollback {
                        report(&app, &id, "正在撤回本阶段的数据");
                        snapshot.restore()?;
                    } else {
                        snapshot.commit()?;
                    }
                    fs::remove_dir_all(&snapshot.path).map_err(|e| e.to_string())?;
                }
                match mode {
                    TaskState::Rollback => Err("任务已撤回，已恢复运行前的数据".into()),
                    TaskState::Stop => Err("任务已停止，保留已提交结果".into()),
                    _ => output,
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
        })))
        .map_err(|_| "任务工作线程已退出".to_string())?;
    let output =
        tauri::async_runtime::spawn_blocking(move || result.recv().map_err(|e| e.to_string())?)
            .await
            .map_err(|e| e.to_string())?;
    if !grouped {
        finish(session).await?;
    }
    output
}
