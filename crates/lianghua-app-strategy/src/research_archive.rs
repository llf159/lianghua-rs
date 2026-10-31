use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

static RESEARCH_EXECUTION: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub fn try_research_execution() -> Result<std::sync::MutexGuard<'static, ()>, String> {
    match RESEARCH_EXECUTION.try_lock() {
        Ok(guard) => Ok(guard),
        Err(std::sync::TryLockError::Poisoned(error)) => {
            RESEARCH_EXECUTION.clear_poison();
            Ok(error.into_inner())
        }
        Err(std::sync::TryLockError::WouldBlock) => {
            Err("已有研究或模拟任务正在运行，请等待完成或取消".into())
        }
    }
}

/// Shared access spans a job or archive mutation; cleanup only proceeds with exclusive access.
/// The OS releases locks after process termination, including force kills.
pub fn research_archive_lock(source_path: &str, cleanup: bool) -> Result<Option<File>, String> {
    let root = archive_root(source_path)?;
    fs::create_dir_all(&root).map_err(|e| e.to_string())?;
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(".activity.lock"))
        .map_err(|e| format!("打开研究存档锁失败: {e}"))?;
    #[cfg(not(target_os = "android"))]
    let locked = if cleanup {
        file.try_lock()
    } else {
        file.lock_shared().map_err(std::fs::TryLockError::Error)
    };
    #[cfg(target_os = "android")]
    let locked = {
        use std::os::fd::AsRawFd;
        let operation = if cleanup {
            libc::LOCK_EX | libc::LOCK_NB
        } else {
            libc::LOCK_SH
        };
        if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
            Ok(())
        } else {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                Err(std::fs::TryLockError::WouldBlock)
            } else {
                Err(std::fs::TryLockError::Error(error))
            }
        }
    };
    match locked {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(format!("锁定研究存档失败: {error}")),
    }
}

fn cleanup_research_run(path: &Path, incomplete: bool) {
    for name in [
        ".configuration.json.pending",
        ".result.json.pending",
        ".header.json.pending",
        ".pending-package",
        "evidence.baseline.toml",
    ]
    .into_iter()
    .chain(
        incomplete
            .then_some([
                "members.db",
                "members.db.wal",
                "members.db.tmp",
                "evidence.db",
                "evidence.db.wal",
                "evidence.db.tmp",
                "report.json",
                "report.md",
                "experiment.lhresearch",
            ])
            .into_iter()
            .flatten(),
    ) {
        let file = path.join(name);
        let result = match fs::symlink_metadata(&file) {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&file),
            Ok(_) => fs::remove_file(&file),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            log::warn!("清理研究半成品失败，稍后重试 {}: {error}", file.display());
        }
    }
}

pub struct ResearchArchiveLease {
    pub source_path: String,
    pub run_id: String,
}
impl Drop for ResearchArchiveLease {
    fn drop(&mut self) {
        if read_research_archive_header(&self.source_path, &self.run_id)
            .is_ok_and(|header| header.status == "running")
        {
            let _ = finish_research_archive(
                &self.source_path,
                &self.run_id,
                "interrupted",
                None,
                serde_json::json!({"error":"任务在提交前异常退出；可复制配置完整重跑，未提交阶段不可复用"}),
            );
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchArchiveHeader {
    pub version: u32,
    pub run_id: String,
    pub parent_run_id: Option<String>,
    pub created_ms: u128,
    pub question: String,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub summary: String,
    pub kind: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub membership_saved: bool,
    pub input_identity: String,
    #[serde(default)]
    pub owner_session: String,
    #[serde(default)]
    pub cleanup_lock: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResearchArchiveRun {
    pub header: ResearchArchiveHeader,
    pub configuration: Value,
    pub result: Value,
}

fn archive_root(source_path: &str) -> Result<PathBuf, String> {
    let path = Path::new(source_path.trim());
    if source_path.trim().is_empty() || !path.is_dir() {
        return Err("数据目录不存在，不能使用研究存档".to_string());
    }
    Ok(path.join("research_runs_v1"))
}

fn archive_run_path(source_path: &str, run_id: &str) -> Result<PathBuf, String> {
    if run_id.len() != 49
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        || run_id.as_bytes()[32] != b'-'
    {
        return Err("研究运行编号无效".to_string());
    }
    Ok(archive_root(source_path)?.join(run_id))
}

fn describe_research_archive(header: &mut ResearchArchiveHeader, configuration: &Value) {
    if header.summary.is_empty() {
        let query = configuration
            .get("query")
            .or_else(|| configuration.get("settings"))
            .unwrap_or(configuration);
        let form = configuration.get("form").unwrap_or(query);
        let mut parts = vec![
            form.get("ruleDraft")
                .and_then(|draft| draft.get("name"))
                .and_then(Value::as_str)
                .filter(|name| !name.trim().is_empty())
                .or_else(|| {
                    query
                        .get("manualStrategy")
                        .and_then(|strategy| strategy.get("name"))
                        .and_then(Value::as_str)
                        .filter(|name| !name.trim().is_empty())
                })
                .or_else(|| form.get("importRuleName").and_then(Value::as_str))
                .filter(|name| !name.is_empty())
                .unwrap_or(match header.kind.as_str() {
                    "rank_comparison" => "排名对照",
                    "paper_validation" => "模拟盘",
                    _ if form.pointer("/ruleDraft/kind").and_then(Value::as_str)
                        == Some("combination") =>
                    {
                        "组合策略"
                    }
                    _ => "表达式",
                })
                .to_string(),
        ];
        if let (Some(start), Some(end)) = (
            query
                .get("startDate")
                .or_else(|| query.get("start_date"))
                .and_then(Value::as_str),
            query
                .get("endDate")
                .or_else(|| query.get("end_date"))
                .and_then(Value::as_str),
        ) {
            parts.push(format!("{start} ~ {end}"));
        }
        if let Some(period) = query
            .get("backtestPeriod")
            .or_else(|| query.get("backtest_period"))
            .and_then(Value::as_u64)
        {
            parts.push(format!("{period}日"));
        }
        if let Some(formula) = form
            .get("expression")
            .or_else(|| query.get("when"))
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
        {
            let formula = formula.split_whitespace().collect::<Vec<_>>().join(" ");
            parts.push(if formula.chars().count() > 80 {
                format!("{}…", formula.chars().take(80).collect::<String>())
            } else {
                formula
            });
        }
        header.summary = parts.join(" · ");
    }
    if header.note.is_none() {
        header.note = Some(
            configuration
                .get("notes")
                .map(|notes| {
                    if let Some(text) = notes.as_str() {
                        return text.to_string();
                    }
                    [
                        ("hypothesis", "假设"),
                        ("scope", "范围"),
                        ("conclusion", "结论"),
                    ]
                    .into_iter()
                    .filter_map(|(key, label)| {
                        notes
                            .get(key)
                            .and_then(Value::as_str)
                            .filter(|text| !text.trim().is_empty())
                            .map(|text| format!("{label}：{text}"))
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
                })
                .unwrap_or_default(),
        );
    }
}

pub fn update_research_archive_metadata(
    source_path: &str,
    run_id: &str,
    name: String,
    note: String,
) -> Result<ResearchArchiveHeader, String> {
    let _access = research_archive_lock(source_path, false)?;
    let mut header = read_research_archive_header(source_path, run_id)?;
    if header.status == "running" {
        return Err("运行中的记录尚未完成，请完成后编辑备注".into());
    }
    header.question = name.trim().to_string();
    header.note = Some(note.trim().to_string());
    let path = archive_run_path(source_path, run_id)?;
    let mut file = File::create(path.join(".header.json.pending")).map_err(|e| e.to_string())?;
    serde_json::to_writer(&mut file, &header).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    fs::rename(path.join(".header.json.pending"), path.join("header.json"))
        .map_err(|e| e.to_string())?;
    Ok(header)
}

pub fn save_research_archive(
    source_path: &str,
    parent_run_id: Option<String>,
    question: String,
    mut configuration: Value,
    mut result: Value,
    kind: &str,
    status: &str,
    membership_staging: Option<&Path>,
    membership_source_run_id: Option<&str>,
) -> Result<ResearchArchiveHeader, String> {
    let _access = research_archive_lock(source_path, false)?;
    if !matches!(
        status,
        "draft" | "running" | "completed" | "failed" | "cancelled" | "interrupted"
    ) {
        return Err("不支持的研究存档状态".to_string());
    }
    if !matches!(
        kind,
        "expression_validation" | "rank_comparison" | "paper_validation"
    ) {
        return Err("不支持的研究存档类型".to_string());
    }
    if !configuration.is_object() || !result.is_object() {
        return Err("研究配置和结果必须是对象".to_string());
    }
    let mut note = Some(String::new());
    let membership_source = if let Some(run_id) = membership_source_run_id {
        if status != "completed" || membership_staging.is_some() {
            return Err("不能从该运行复制排名成员".to_string());
        }
        let source = read_research_archive(source_path, run_id)?;
        if source.header.kind != kind
            || source.header.status != "completed"
            || !source.header.membership_saved
        {
            return Err("来源运行不包含排名成员明细".to_string());
        }
        note = source.header.note.clone();
        let notes = configuration.get("notes").cloned();
        let exploratory_filter = configuration.get("exploratory_filter").cloned();
        configuration = source.configuration;
        if let Some(notes) = notes {
            configuration["notes"] = notes;
        }
        if let Some(filter) = exploratory_filter {
            configuration["exploratory_filter"] = filter;
        }
        result = source.result;
        Some(archive_run_path(source_path, run_id)?)
    } else {
        None
    };
    if let Some(parent) = parent_run_id.as_deref() {
        if !archive_run_path(source_path, parent)?
            .join("header.json")
            .is_file()
        {
            return Err("来源实验不存在".to_string());
        }
    }
    let created_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("读取系统时间失败: {e}"))?
        .as_millis();
    let run_id = format!("{:032x}-{:016x}", created_ms, rand::random::<u64>());
    if result.get("archive_run_id").is_some() {
        result["archive_run_id"] = serde_json::json!(run_id);
        if let Some(periods) = result
            .get_mut("additional_periods")
            .and_then(Value::as_array_mut)
        {
            for period in periods {
                period["archive_run_id"] = serde_json::json!(run_id);
            }
        }
    }
    let root = archive_root(source_path)?;
    fs::create_dir_all(&root).map_err(|e| format!("创建研究存档目录失败: {e}"))?;
    let temporary = root.join(format!(".pending-{run_id}"));
    fs::create_dir(&temporary).map_err(|e| format!("创建研究存档失败: {e}"))?;
    let mut header = ResearchArchiveHeader {
        version: 1,
        run_id: run_id.clone(),
        parent_run_id,
        created_ms,
        question: question.trim().to_string(),
        note,
        summary: String::new(),
        kind: kind.to_string(),
        status: status.to_string(),
        membership_saved: membership_staging.is_some() || membership_source.is_some(),
        input_identity: result
            .get("input_identity")
            .and_then(Value::as_str)
            .unwrap_or("unverified")
            .to_string(),
        owner_session: archive_session().to_string(),
        cleanup_lock: true,
    };
    describe_research_archive(&mut header, &configuration);
    let write_result = (|| -> Result<(), String> {
        for (name, value) in [
            (
                "header.json",
                serde_json::to_value(&header).map_err(|e| e.to_string())?,
            ),
            ("configuration.json", configuration),
            ("result.json", result),
        ] {
            let file =
                File::create(temporary.join(name)).map_err(|e| format!("创建{name}失败: {e}"))?;
            let mut writer = BufWriter::new(file);
            serde_json::to_writer(&mut writer, &value)
                .map_err(|e| format!("写入{name}失败: {e}"))?;
            writer.flush().map_err(|e| format!("刷新{name}失败: {e}"))?;
            writer
                .get_ref()
                .sync_all()
                .map_err(|e| format!("同步{name}失败: {e}"))?;
        }
        if let Some(staging) = membership_staging {
            if kind != "rank_comparison"
                || status != "completed"
                || staging.parent() != Some(root.as_path())
                || !staging
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with(".pending-members-") && name.ends_with(".db")
                    })
            {
                return Err("成员存档暂存路径无效".to_string());
            }
            fs::rename(staging, temporary.join("members.db"))
                .map_err(|e| format!("提交成员存档失败: {e}"))?;
        }
        if let Some(source) = membership_source {
            for name in ["members.db", "evidence.db"] {
                if source.join(name).is_file() {
                    fs::copy(source.join(name), temporary.join(name))
                        .map_err(|e| format!("复制研究明细失败: {e}"))?;
                    File::open(temporary.join(name))
                        .and_then(|file| file.sync_all())
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        fs::rename(&temporary, archive_run_path(source_path, &run_id)?)
            .map_err(|e| format!("提交研究存档失败: {e}"))?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
    }
    write_result?;
    Ok(header)
}

pub fn list_research_archives(source_path: &str) -> Result<Vec<ResearchArchiveHeader>, String> {
    let root = archive_root(source_path)?;
    if !root.exists() {
        return Ok(Vec::new());
    }
    let cleanup = research_archive_lock(source_path, true)?;
    let entries = fs::read_dir(root)
        .map_err(|e| format!("读取研究存档目录失败: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    // Old versions did not hold the activity lock. Do not sweep while their owner may be alive.
    let cleanup = cleanup.filter(|_| {
        !entries.iter().any(|entry| {
            if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                return false;
            }
            let header = File::open(entry.path().join("header.json"))
                .ok()
                .and_then(|file| {
                    serde_json::from_reader::<_, ResearchArchiveHeader>(BufReader::new(file)).ok()
                });
            header.is_some_and(|header| {
                if header.status != "running" || header.cleanup_lock {
                    return false;
                }
                #[cfg(any(target_os = "linux", target_os = "android"))]
                {
                    header
                        .owner_session
                        .split_once('-')
                        .and_then(|(pid, _)| pid.parse::<u32>().ok())
                        .is_none_or(|pid| Path::new("/proc").join(pid.to_string()).exists())
                }
                #[cfg(not(any(target_os = "linux", target_os = "android")))]
                {
                    true
                }
            })
        })
    });
    let mut out = Vec::new();
    for entry in entries {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if archive_run_path(source_path, &name).is_err() {
            if cleanup.is_some() {
                let temporary = name
                    .strip_prefix(".pending-")
                    .is_some_and(|id| archive_run_path(source_path, id).is_ok())
                    || name.strip_prefix(".import-").is_some_and(|id| {
                        id.len() == 16 && id.bytes().all(|byte| byte.is_ascii_hexdigit())
                    })
                    || name
                        .strip_prefix(".pending-members-")
                        .or_else(|| name.strip_prefix(".pending-evidence-"))
                        .or_else(|| name.strip_prefix(".pending-ranking-"))
                        .is_some_and(|id| {
                            let id = id
                                .strip_suffix(".db.wal")
                                .or_else(|| id.strip_suffix(".db.tmp"))
                                .or_else(|| id.strip_suffix(".db"))
                                .unwrap_or(id);
                            !id.is_empty()
                                && id.len() <= 80
                                && id
                                    .bytes()
                                    .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
                        });
                if temporary {
                    let result = if entry.file_type().map_err(|e| e.to_string())?.is_dir() {
                        fs::remove_dir_all(entry.path())
                    } else {
                        fs::remove_file(entry.path())
                    };
                    if let Err(error) = result {
                        log::warn!("清理研究暂存文件失败 {}: {error}", entry.path().display());
                    }
                }
            }
            continue;
        }
        if !entry.file_type().map_err(|e| e.to_string())?.is_dir() {
            continue;
        }
        let file = File::open(entry.path().join("header.json"))
            .map_err(|e| format!("读取研究存档{name}失败: {e}"))?;
        let mut header: ResearchArchiveHeader = serde_json::from_reader(BufReader::new(file))
            .map_err(|e| format!("解析研究存档{name}失败: {e}"))?;
        if header.run_id != name || header.version != 1 {
            return Err(format!("研究存档{name}编号或版本不匹配"));
        }
        if cleanup.is_some()
            && header.status == "running"
            && header.owner_session != archive_session()
        {
            header = finish_research_archive_unlocked(
                source_path,
                &name,
                "interrupted",
                None,
                serde_json::json!({"error":"上次进程在运行完成前退出；保留原配置，可复制后完整重跑，未提交阶段不复用"}),
            )?;
        }
        if cleanup.is_some() && header.status != "running" {
            cleanup_research_run(&entry.path(), header.status != "completed");
        }
        if header.summary.is_empty() || header.note.is_none() {
            if let Ok(file) = File::open(entry.path().join("configuration.json")) {
                if let Ok(configuration) = serde_json::from_reader::<_, Value>(BufReader::new(file))
                {
                    describe_research_archive(&mut header, &configuration);
                }
            }
        }
        out.push(header);
    }
    out.sort_by(|a, b| {
        b.created_ms
            .cmp(&a.created_ms)
            .then_with(|| b.run_id.cmp(&a.run_id))
    });
    Ok(out)
}

pub fn read_research_archive_header(
    source_path: &str,
    run_id: &str,
) -> Result<ResearchArchiveHeader, String> {
    let file = File::open(archive_run_path(source_path, run_id)?.join("header.json"))
        .map_err(|e| e.to_string())?;
    let mut header: ResearchArchiveHeader =
        serde_json::from_reader(BufReader::new(file)).map_err(|e| e.to_string())?;
    if header.run_id != run_id || header.version != 1 {
        return Err("研究存档编号或版本不匹配".into());
    }
    if header.summary.is_empty() || header.note.is_none() {
        let file = File::open(archive_run_path(source_path, run_id)?.join("configuration.json"))
            .map_err(|e| e.to_string())?;
        let configuration: Value =
            serde_json::from_reader(BufReader::new(file)).map_err(|e| e.to_string())?;
        describe_research_archive(&mut header, &configuration);
    }
    Ok(header)
}

pub fn read_research_archive(
    source_path: &str,
    run_id: &str,
) -> Result<ResearchArchiveRun, String> {
    let path = archive_run_path(source_path, run_id)?;
    let read = |name: &str| -> Result<Value, String> {
        let file = File::open(path.join(name)).map_err(|e| format!("读取{name}失败: {e}"))?;
        serde_json::from_reader(BufReader::new(file)).map_err(|e| format!("解析{name}失败: {e}"))
    };
    let header = read_research_archive_header(source_path, run_id)?;
    Ok(ResearchArchiveRun {
        header,
        configuration: read("configuration.json")?,
        result: read("result.json")?,
    })
}

pub fn research_input_manifest(source: &str) -> Result<Value, String> {
    use sha2::{Digest, Sha256};
    let mut files = Vec::new();
    let mut configurations = serde_json::Map::new();
    let mut paths = fs::read_dir(source)
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    paths.sort_by_key(|entry| entry.file_name());
    for entry in paths {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !entry.file_type().map_err(|e| e.to_string())?.is_file()
            || !(name.ends_with(".db")
                || name.ends_with(".wal")
                || name.ends_with(".csv")
                || name.ends_with(".toml"))
        {
            continue;
        }
        let meta = entry.metadata().map_err(|e| e.to_string())?;
        let text = if name.ends_with(".toml") || name.ends_with(".csv") {
            Some(fs::read_to_string(entry.path()).map_err(|e| e.to_string())?)
        } else {
            None
        };
        files.push(serde_json::json!({"file":name,"bytes":meta.len(),"modified_ms":meta.modified().ok().and_then(|time|time.duration_since(UNIX_EPOCH).ok()).map(|time|time.as_millis()),"sha256":text.as_ref().map(|text|format!("{:x}",Sha256::digest(text.as_bytes())))}));
        if let Some(text) = text {
            configurations.insert(name, Value::String(text));
        }
    }
    let mut data_status = serde_json::Map::new();
    if crate::data::source_db_path(source).is_file() {
        match crate::data::open_db_readonly(crate::data::source_db_path(source)) {
            Ok(conn) => {
                for (key, sql) in [
                    (
                        "quote_coverage",
                        "SELECT to_json(list(x)) FROM (SELECT adj_type,min(trade_date) AS first_date,max(trade_date) AS last_date,count(*) AS rows FROM stock_data GROUP BY adj_type ORDER BY adj_type) x",
                    ),
                    (
                        "indicator_panel_state",
                        "SELECT to_json(list(x)) FROM (SELECT version,signature,dirty_from FROM indicator_panel_state) x",
                    ),
                    (
                        "indicator_sessions",
                        "SELECT to_json(list(x)) FROM (SELECT min(trade_date) AS first_date,max(trade_date) AS last_date,count(*) AS days FROM indicator_panel_sessions) x",
                    ),
                    (
                        "pending_repairs",
                        "SELECT to_json(list(x)) FROM pending_stock_repair x",
                    ),
                    (
                        "pending_derived_rebuilds",
                        "SELECT to_json(list(x)) FROM pending_derived_rebuild x",
                    ),
                ] {
                    let value = match conn.query_row(sql, [], |row| row.get::<_, Option<String>>(0))
                    {
                        Ok(Some(text)) => {
                            serde_json::from_str(&text).unwrap_or(Value::String(text))
                        }
                        Ok(None) => Value::Array(Vec::new()),
                        Err(error) => {
                            serde_json::json!({"status":"unavailable","reason":error.to_string()})
                        }
                    };
                    data_status.insert(key.into(), value);
                }
            }
            Err(error) => {
                data_status.insert(
                    "source_database".into(),
                    serde_json::json!({"status":"unavailable","reason":error.to_string()}),
                );
            }
        }
    }
    Ok(
        serde_json::json!({"files":files,"configurations":configurations,"data_status":data_status,"application_version":env!("CARGO_PKG_VERSION"),"code_revision":option_env!("LIANGHUA_CODE_REVISION").unwrap_or("未嵌入构建版本"),"label_algorithm":"forward_open_compounded_residual_v1","ranking_algorithm":"native_score_desc_j_asc_nulls_last_code_v1","identity":"metadata_and_configuration_hashes_not_snapshot","limitations":["数据库大小及修改时间不证明内容一致","上市资格及板块来源为当前股票资料；历史ST、行业、概念、退市覆盖不完整","历史评分及输入同源尚未认证"]}),
    )
}

fn archive_session() -> &'static str {
    static SESSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    SESSION.get_or_init(|| format!("{}-{:016x}", std::process::id(), rand::random::<u64>()))
}

/// Only an uncommitted running journal may be completed. Finished experiments are immutable.
pub fn finish_research_archive(
    source_path: &str,
    run_id: &str,
    status: &str,
    configuration: Option<Value>,
    result: Value,
) -> Result<ResearchArchiveHeader, String> {
    let _access = research_archive_lock(source_path, false)?;
    finish_research_archive_unlocked(source_path, run_id, status, configuration, result)
}

fn finish_research_archive_unlocked(
    source_path: &str,
    run_id: &str,
    status: &str,
    configuration: Option<Value>,
    result: Value,
) -> Result<ResearchArchiveHeader, String> {
    if !matches!(
        status,
        "draft" | "completed" | "failed" | "cancelled" | "interrupted"
    ) || !result.is_object()
    {
        return Err("研究完成状态或结果无效".into());
    }
    let mut run = read_research_archive(source_path, run_id)?;
    if run.header.status != "running" {
        return Err("已完成的运行不可覆盖，请创建新运行".into());
    }
    let path = archive_run_path(source_path, run_id)?;
    run.header.status = status.into();
    run.header.membership_saved = status == "completed"
        && (path.join("members.db").is_file() || path.join("evidence.db").is_file());
    run.header.input_identity = result
        .get("input_identity")
        .and_then(Value::as_str)
        .unwrap_or("unverified")
        .into();
    for (name, value) in [
        (
            "configuration.json",
            configuration.unwrap_or(run.configuration),
        ),
        ("result.json", result),
        (
            "header.json",
            serde_json::to_value(&run.header).map_err(|e| e.to_string())?,
        ),
    ] {
        let mut file =
            File::create(path.join(format!(".{name}.pending"))).map_err(|e| e.to_string())?;
        serde_json::to_writer(&mut file, &value).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        fs::rename(path.join(format!(".{name}.pending")), path.join(name))
            .map_err(|e| e.to_string())?;
    }
    cleanup_research_run(&path, status != "completed");
    Ok(run.header)
}

pub fn research_artifact_path(
    source_path: &str,
    run_id: &str,
    filename: &str,
) -> Result<PathBuf, String> {
    if !matches!(
        filename,
        "members.db" | "evidence.db" | "report.json" | "report.md" | "experiment.lhresearch"
    ) {
        return Err("不支持的研究文件".into());
    }
    read_research_archive_header(source_path, run_id)?;
    Ok(archive_run_path(source_path, run_id)?.join(filename))
}

/// Export beside the original journal; the report is portable and contains saved values only.
pub fn export_research_archive(source_path: &str, run_id: &str) -> Result<String, String> {
    let _access = research_archive_lock(source_path, false)?;
    let mut run = read_research_archive(source_path, run_id)?;
    if run.header.status == "running" {
        return Err("运行尚未完成".into());
    }
    if run.header.kind == "expression_validation"
        && run.header.status == "completed"
        && research_artifact_path(source_path, run_id, "evidence.db")?.is_file()
    {
        if let Some(query) = run.configuration.get("query") {
            if let (Some(start), Some(end)) = (
                query.get("startDate").and_then(Value::as_str),
                query.get("endDate").and_then(Value::as_str),
            ) {
                let mut dimensions = vec![
                    "all".to_string(),
                    "board".to_string(),
                    "market_cap".to_string(),
                ];
                if let Some(metric) = query
                    .get("researchGrouping")
                    .and_then(|value| value.get("market_metric"))
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                {
                    dimensions.push(metric.to_string());
                }
                if query
                    .get("researchGrouping")
                    .and_then(|value| value.get("baseline_score_boundaries"))
                    .and_then(Value::as_array)
                    .is_some_and(|values| !values.is_empty())
                {
                    dimensions.push("baseline_score".into());
                }
                let mut reports = Vec::new();
                for period in std::iter::once(&run.result).chain(
                    run.result
                        .get("additional_periods")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten(),
                ) {
                    if let (Some(horizon), Some(combos)) = (
                        period.get("horizon").and_then(Value::as_u64),
                        period.get("combo_results").and_then(Value::as_array),
                    ) {
                        for combo in combos {
                            if let Some(key) = combo.get("combo_key").and_then(Value::as_str) {
                                for dimension in &dimensions {
                                    for frequency in ["all", "quarter", "month"] {
                                        let selection =
                                            crate::research_evidence::ResearchEvidenceQuery {
                                                run_id: run_id.into(),
                                                combo_key: key.into(),
                                                horizon: horizon as usize,
                                                start_date: start.into(),
                                                end_date: end.into(),
                                                dimension: dimension.clone(),
                                                bucket: None,
                                                state: "all".into(),
                                                offset: 0,
                                                limit: 50,
                                            };
                                        reports.push(serde_json::json!({"horizon":horizon,"combo_key":key,"dimension":dimension,"frequency":frequency,"rows":crate::research_evidence::summarize_research_evidence(source_path,&selection,frequency)?}));
                                    }
                                }
                            }
                        }
                    }
                }
                run.result["evidence_report"] = Value::Array(reports);
            }
        }
    }
    let path = research_artifact_path(source_path, run_id, "report.json")?;
    let mut file = BufWriter::new(File::create(&path).map_err(|e| e.to_string())?);
    serde_json::to_writer_pretty(&mut file, &run).map_err(|e| e.to_string())?;
    file.flush().map_err(|e| e.to_string())?;
    let text = format!(
        "# {}\n\n运行：{}\n\n状态：{}；输入身份：{}\n\n摘要：{}\n\n备注：{}\n\n本报告只使用存档配置和结果。详情图如需打开，读取当前行情，不认证为历史输入。成员与标签在同目录数据库中，复制整个运行目录可一并保留。\n\n## 配置\n\n```json\n{}\n```\n\n## 结果\n\n```json\n{}\n```\n",
        run.header.question,
        run_id,
        run.header.status,
        run.header.input_identity,
        run.header.summary,
        run.header.note.as_deref().unwrap_or(""),
        serde_json::to_string_pretty(&run.configuration).map_err(|e| e.to_string())?,
        serde_json::to_string_pretty(&run.result).map_err(|e| e.to_string())?
    );
    fs::write(
        research_artifact_path(source_path, run_id, "report.md")?,
        text,
    )
    .map_err(|e| e.to_string())?;
    let package = research_artifact_path(source_path, run_id, "experiment.lhresearch")?;
    let directory = archive_run_path(source_path, run_id)?;
    let files = [
        "header.json",
        "configuration.json",
        "result.json",
        "members.db",
        "evidence.db",
        "report.md",
    ]
    .into_iter()
    .filter(|name| directory.join(name).is_file())
    .collect::<Vec<_>>();
    let pending = directory.join(".pending-package");
    let mut writer = BufWriter::new(File::create(&pending).map_err(|e| e.to_string())?);
    writer
        .write_all(b"LHRESEARCH1\n")
        .map_err(|e| e.to_string())?;
    writer
        .write_all(&[files.len() as u8])
        .map_err(|e| e.to_string())?;
    for name in files {
        use sha2::{Digest, Sha256};
        let mut file = File::open(directory.join(name)).map_err(|e| e.to_string())?;
        writer
            .write_all(&[name.len() as u8])
            .and_then(|_| writer.write_all(name.as_bytes()))
            .map_err(|e| e.to_string())?;
        writer
            .write_all(
                &file
                    .metadata()
                    .map_err(|e| e.to_string())?
                    .len()
                    .to_le_bytes(),
            )
            .map_err(|e| e.to_string())?;
        let mut digest = Sha256::new();
        let mut buffer = vec![0u8; 256 * 1024];
        loop {
            let count = file.read(&mut buffer).map_err(|e| e.to_string())?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
            writer
                .write_all(&buffer[..count])
                .map_err(|e| e.to_string())?;
        }
        writer
            .write_all(&digest.finalize())
            .map_err(|e| e.to_string())?;
    }
    writer.flush().map_err(|e| e.to_string())?;
    writer.get_ref().sync_all().map_err(|e| e.to_string())?;
    drop(writer);
    fs::rename(pending, &package).map_err(|e| e.to_string())?;
    Ok(package.to_string_lossy().into_owned())
}

pub fn import_research_archive(
    source_path: &str,
    package: impl Read,
) -> Result<ResearchArchiveHeader, String> {
    let _access = research_archive_lock(source_path, false)?;
    use sha2::{Digest, Sha256};
    let root = archive_root(source_path)?;
    fs::create_dir_all(&root).map_err(|e| e.to_string())?;
    let pending = root.join(format!(".import-{:016x}", rand::random::<u64>()));
    fs::create_dir(&pending).map_err(|e| e.to_string())?;
    let result = (|| -> Result<ResearchArchiveHeader, String> {
        let mut reader = BufReader::new(package);
        let mut magic = [0u8; 12];
        reader.read_exact(&mut magic).map_err(|e| e.to_string())?;
        if &magic != b"LHRESEARCH1\n" {
            return Err("不是支持的研究结果包".into());
        }
        let mut count = [0u8; 1];
        reader.read_exact(&mut count).map_err(|e| e.to_string())?;
        if count[0] < 3 || count[0] > 6 {
            return Err("结果包文件数无效".into());
        }
        let mut names = std::collections::HashSet::new();
        for _ in 0..count[0] {
            let mut length = [0u8; 1];
            reader.read_exact(&mut length).map_err(|e| e.to_string())?;
            if length[0] > 32 {
                return Err("结果包文件名无效".into());
            }
            let mut name = vec![0u8; length[0] as usize];
            reader.read_exact(&mut name).map_err(|e| e.to_string())?;
            let name = String::from_utf8(name).map_err(|e| e.to_string())?;
            if ![
                "header.json",
                "configuration.json",
                "result.json",
                "members.db",
                "evidence.db",
                "report.md",
            ]
            .contains(&name.as_str())
                || !names.insert(name.clone())
            {
                return Err("结果包包含未知或重复文件".into());
            }
            let mut size = [0u8; 8];
            reader.read_exact(&mut size).map_err(|e| e.to_string())?;
            let mut remaining = u64::from_le_bytes(size);
            if name.ends_with(".json") && remaining > 128 * 1024 * 1024 {
                return Err("结果包JSON过大".into());
            }
            let mut file = File::create(pending.join(&name)).map_err(|e| e.to_string())?;
            let mut digest = Sha256::new();
            let mut buffer = vec![0u8; 256 * 1024];
            while remaining > 0 {
                let max = buffer.len().min(remaining as usize);
                let n = reader.read(&mut buffer[..max]).map_err(|e| e.to_string())?;
                if n == 0 {
                    return Err("结果包未完整传输".into());
                }
                file.write_all(&buffer[..n]).map_err(|e| e.to_string())?;
                digest.update(&buffer[..n]);
                remaining -= n as u64;
            }
            let mut expected = [0u8; 32];
            reader
                .read_exact(&mut expected)
                .map_err(|e| e.to_string())?;
            if digest.finalize().as_slice() != expected {
                return Err(format!("结果包{name}内容校验失败"));
            }
            file.sync_all().map_err(|e| e.to_string())?;
        }
        let mut trailing = [0u8; 1];
        if reader.read(&mut trailing).map_err(|e| e.to_string())? != 0 {
            return Err("结果包存在多余内容".into());
        }
        let mut original: ResearchArchiveHeader = serde_json::from_reader(
            File::open(pending.join("header.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if original.version != 1
            || !matches!(
                original.status.as_str(),
                "draft" | "completed" | "failed" | "cancelled" | "interrupted"
            )
        {
            return Err("运行版本或完成状态不支持导入".into());
        }
        let mut configuration: Value = serde_json::from_reader(
            File::open(pending.join("configuration.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        describe_research_archive(&mut original, &configuration);
        configuration["imported_original_header"] =
            serde_json::to_value(&original).map_err(|e| e.to_string())?;
        let mut payload: Value = serde_json::from_reader(
            File::open(pending.join("result.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let journal = save_research_archive(
            source_path,
            None,
            original.question.clone(),
            configuration,
            serde_json::json!({}),
            &original.kind,
            "running",
            None,
            None,
        )?;
        let submitted = (|| -> Result<ResearchArchiveHeader, String> {
            for name in ["members.db", "evidence.db"] {
                if pending.join(name).is_file() {
                    fs::rename(
                        pending.join(name),
                        research_artifact_path(source_path, &journal.run_id, name)?,
                    )
                    .map_err(|e| e.to_string())?;
                }
            }
            payload["archive_run_id"] = serde_json::json!(journal.run_id);
            if let Some(periods) = payload
                .get_mut("additional_periods")
                .and_then(Value::as_array_mut)
            {
                for period in periods {
                    period["archive_run_id"] = serde_json::json!(journal.run_id);
                }
            }
            finish_research_archive(
                source_path,
                &journal.run_id,
                &original.status,
                None,
                payload,
            )?;
            update_research_archive_metadata(
                source_path,
                &journal.run_id,
                original.question.clone(),
                original.note.clone().unwrap_or_default(),
            )
        })();
        if submitted.is_err() {
            let _ = fs::remove_dir_all(archive_run_path(source_path, &journal.run_id)?);
        }
        submitted
    })();
    let _ = fs::remove_dir_all(pending);
    result
}

pub fn delete_research_archive(source_path: &str, run_id: &str) -> Result<(), String> {
    let _access = research_archive_lock(source_path, false)?;
    if read_research_archive_header(source_path, run_id)?.status == "running" {
        return Err("运行中的研究不能删除，请先取消".into());
    }
    fs::remove_dir_all(archive_run_path(source_path, run_id)?)
        .map_err(|e| format!("删除研究存档失败: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn archived_rank_members_keep_three_day_missing_and_page_changes() {
        let root = std::env::temp_dir().join(format!(
            "lianghua-member-test-{:016x}",
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let source = root.to_str().unwrap();
        let archive = root.join("research_runs_v1");
        fs::create_dir(&archive).unwrap();
        let staging = archive.join(".pending-members-test.db");
        let db = duckdb::Connection::open(&staging).unwrap();
        db.execute_batch("CREATE TABLE ranking_members(side VARCHAR, trade_date VARCHAR, ts_code VARCHAR, score DOUBLE, rank BIGINT, eligibility SMALLINT);
            CREATE TABLE labels(trade_date VARCHAR, ts_code VARCHAR, residual_return DOUBLE, er_change DOUBLE);
            INSERT INTO ranking_members VALUES
              ('baseline','20260105','A',1,1,1),('candidate','20260105','A',1,1,1),
              ('baseline','20260106','B',1,1,1),('candidate','20260106','B',1,1,1),
              ('baseline','20260107','C',2,1,1),('baseline','20260107','D',1,2,1),
              ('candidate','20260107','C',2,1,1),('candidate','20260107','E',1,2,1);
            INSERT INTO labels VALUES ('20260107','C',1,NULL),('20260107','E',2,NULL);").unwrap();
        drop(db);
        let saved = save_research_archive(
            source,
            None,
            String::new(),
            json!({"settings":{"start_date":"20260107","end_date":"20260107"}}),
            json!({}),
            "rank_comparison",
            "completed",
            Some(&staging),
            None,
        )
        .unwrap();
        assert!(saved.membership_saved);
        let removed = list_rank_comparison_members(
            source,
            &saved.run_id,
            "20260107",
            "20260107",
            "daily",
            100,
            "removed",
            0,
            50,
            None,
        )
        .unwrap();
        assert_eq!(removed.total, 1);
        assert_eq!(removed.items[0].ts_code, "D");
        assert!(removed.items[0].residual_return.is_none());
        let pool = list_rank_comparison_members(
            source,
            &saved.run_id,
            "20260107",
            "20260107",
            "three_day",
            100,
            "all",
            0,
            50,
            None,
        )
        .unwrap();
        assert_eq!(pool.total, 5);
        let retained = pool.items.iter().find(|row| row.ts_code == "A").unwrap();
        assert!(
            retained.baseline_score.is_none()
                && retained.candidate_score.is_none()
                && retained.residual_return.is_none()
        );
        assert!(
            list_rank_comparison_members(
                source,
                &saved.run_id,
                "20260105",
                "20260107",
                "daily",
                100,
                "all",
                0,
                50,
                None,
            )
            .is_err()
        );
        let copied = save_research_archive(
            source,
            Some(saved.run_id.clone()),
            "结论".into(),
            json!({"settings":{},"notes":{"conclusion":"复核"}}),
            json!({}),
            "rank_comparison",
            "completed",
            None,
            Some(&saved.run_id),
        )
        .unwrap();
        assert!(copied.membership_saved);
        delete_research_archive(source, &saved.run_id).unwrap();
        let page = list_rank_comparison_members(
            source,
            &copied.run_id,
            "20260107",
            "20260107",
            "daily",
            100,
            "removed",
            0,
            50,
            None,
        )
        .unwrap();
        assert_eq!(page.items[0].ts_code, "D");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_retains_runs_and_rejects_invalid_parent_and_path() {
        let root = std::env::temp_dir().join(format!(
            "lianghua-archive-test-{:016x}",
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let source = root.to_str().unwrap();
        let first = save_research_archive(
            source,
            None,
            "研究".into(),
            json!({"query":{"startDate":"20240101","endDate":"20241231","backtestPeriod":3},"form":{"importRuleName":"规则A","expression":"C > O"},"notes":{"conclusion":"旧备注"}}),
            json!({"mean": 1.0}),
            "expression_validation",
            "completed",
            None,
            None,
        )
        .unwrap();
        let second = save_research_archive(
            source,
            Some(first.run_id.clone()),
            "复查".into(),
            json!({"period": 5}),
            json!({"mean": 2.0}),
            "rank_comparison",
            "failed",
            None,
            None,
        )
        .unwrap();
        assert_ne!(first.run_id, second.run_id);
        assert_eq!(
            read_research_archive(source, &first.run_id).unwrap().result,
            json!({"mean": 1.0})
        );
        assert_eq!(
            read_research_archive(source, &second.run_id)
                .unwrap()
                .header
                .parent_run_id,
            Some(first.run_id.clone())
        );
        assert_eq!(list_research_archives(source).unwrap().len(), 2);
        assert!(read_research_archive(source, "../escape").is_err());
        assert!(
            save_research_archive(
                source,
                Some("invalid".into()),
                String::new(),
                json!({}),
                json!({}),
                "expression_validation",
                "completed",
                None,
                None,
            )
            .is_err()
        );
        let directory = archive_run_path(source, &first.run_id).unwrap();
        let configuration_before = fs::read(directory.join("configuration.json")).unwrap();
        let result_before = fs::read(directory.join("result.json")).unwrap();
        let mut legacy: Value =
            serde_json::from_reader(File::open(directory.join("header.json")).unwrap()).unwrap();
        legacy.as_object_mut().unwrap().remove("note");
        legacy.as_object_mut().unwrap().remove("summary");
        fs::write(
            directory.join("header.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();
        let record = read_research_archive_header(source, &first.run_id).unwrap();
        assert!(record.summary.contains("规则A · 20240101 ~ 20241231 · 3日"));
        assert_eq!(record.note.as_deref(), Some("结论：旧备注"));
        update_research_archive_metadata(source, &first.run_id, "阈值调整".into(), String::new())
            .unwrap();
        assert_eq!(
            read_research_archive_header(source, &first.run_id)
                .unwrap()
                .note
                .as_deref(),
            Some("")
        );
        let updated = update_research_archive_metadata(
            source,
            &first.run_id,
            "阈值调整".into(),
            "阈值改成5，留作对比".into(),
        )
        .unwrap();
        assert_eq!(updated.run_id, first.run_id);
        assert_eq!(updated.created_ms, first.created_ms);
        assert_eq!(list_research_archives(source).unwrap().len(), 2);
        assert_eq!(
            fs::read(directory.join("configuration.json")).unwrap(),
            configuration_before
        );
        assert_eq!(
            fs::read(directory.join("result.json")).unwrap(),
            result_before
        );
        let bundle = export_research_archive(source, &first.run_id).unwrap();
        let imported = import_research_archive(source, File::open(&bundle).unwrap()).unwrap();
        assert_ne!(imported.run_id, first.run_id);
        assert_eq!(imported.question, "阈值调整");
        assert_eq!(imported.note.as_deref(), Some("阈值改成5，留作对比"));
        assert_eq!(
            read_research_archive(source, &imported.run_id)
                .unwrap()
                .result
                .get("mean"),
            Some(&json!(1.0))
        );
        let mut corrupted = fs::read(bundle).unwrap();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 1;
        assert!(import_research_archive(source, std::io::Cursor::new(corrupted)).is_err());
        delete_research_archive(source, &imported.run_id).unwrap();
        let draft = save_research_archive(
            source,
            None,
            "草稿".into(),
            json!({"form":{"expression":"C > O"}}),
            json!({}),
            "expression_validation",
            "draft",
            None,
            None,
        )
        .unwrap();
        let draft_package = export_research_archive(source, &draft.run_id).unwrap();
        let copied_draft =
            import_research_archive(source, File::open(draft_package).unwrap()).unwrap();
        assert_eq!(copied_draft.status, "draft");
        assert_eq!(
            read_research_archive(source, &copied_draft.run_id)
                .unwrap()
                .configuration["form"]["expression"],
            "C > O"
        );
        delete_research_archive(source, &draft.run_id).unwrap();
        delete_research_archive(source, &copied_draft.run_id).unwrap();

        let interrupted = save_research_archive(
            source,
            None,
            "中断".into(),
            json!({}),
            json!({}),
            "expression_validation",
            "running",
            None,
            None,
        )
        .unwrap();
        let mut stale = interrupted.clone();
        stale.owner_session = "earlier-process".into();
        fs::write(
            archive_run_path(source, &stale.run_id)
                .unwrap()
                .join("header.json"),
            serde_json::to_vec(&stale).unwrap(),
        )
        .unwrap();
        assert!(
            list_research_archives(source)
                .unwrap()
                .iter()
                .any(|header| header.run_id == stale.run_id && header.status == "interrupted")
        );
        delete_research_archive(source, &stale.run_id).unwrap();
        delete_research_archive(source, &second.run_id).unwrap();
        assert_eq!(list_research_archives(source).unwrap().len(), 1);
        fs::remove_dir_all(root).unwrap();
    }
}

#[derive(Debug, Serialize)]
pub struct RankComparisonMemberRow {
    pub trade_date: String,
    pub ts_code: String,
    pub membership: String,
    pub baseline_score: Option<f64>,
    pub baseline_rank: Option<i64>,
    pub candidate_score: Option<f64>,
    pub candidate_rank: Option<i64>,
    pub residual_return: Option<f64>,
    pub entry_date: Option<String>,
    pub label_end_date: Option<String>,
    pub missing_reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RankComparisonMemberPage {
    pub total: u64,
    pub offset: usize,
    pub items: Vec<RankComparisonMemberRow>,
}

pub fn list_rank_comparison_members(
    source_path: &str,
    run_id: &str,
    start_date: &str,
    end_date: &str,
    pool_kind: &str,
    top_k: usize,
    membership: &str,
    offset: usize,
    limit: usize,
    slice: Option<(&str, &str)>,
) -> Result<RankComparisonMemberPage, String> {
    if !matches!(pool_kind, "daily" | "three_day")
        || !matches!(top_k, 0 | 100 | 500 | 1000)
        || (pool_kind == "three_day" && top_k == 0)
        || !matches!(
            membership,
            "all" | "added" | "removed" | "shared" | "missing_label"
        )
        || limit == 0
        || limit > 200
        || offset > i64::MAX as usize
        || chrono::NaiveDate::parse_from_str(start_date, "%Y%m%d").is_err()
        || chrono::NaiveDate::parse_from_str(end_date, "%Y%m%d").is_err()
        || start_date > end_date
    {
        return Err("成员分页参数无效".to_string());
    }
    let archive = read_research_archive(source_path, run_id)?;
    if archive.header.kind != "rank_comparison"
        || !archive.header.membership_saved
        || archive.header.status != "completed"
    {
        return Err("此研究存档不包含排名成员明细".to_string());
    }
    let saved_start = archive
        .configuration
        .get("settings")
        .and_then(|value| value.get("start_date"))
        .and_then(Value::as_str)
        .ok_or_else(|| "存档缺少运行开始日期".to_string())?;
    let saved_end = archive
        .configuration
        .get("settings")
        .and_then(|value| value.get("end_date"))
        .and_then(Value::as_str)
        .ok_or_else(|| "存档缺少运行结束日期".to_string())?;
    if start_date < saved_start || end_date > saved_end {
        return Err("样本日期不能超出该运行的信号区间".to_string());
    }
    let path = archive_run_path(source_path, run_id)?.join("members.db");
    let conn = crate::data::open_db_readonly(path).map_err(|e| format!("打开成员存档失败: {e}"))?;
    let slice_filter = if let Some((dimension, bucket)) = slice {
        let expression = match dimension {
            "all" | "" => "'全部'".to_string(),
            "board" => "o.board".into(),
            "market_cap" => "o.cap_group".into(),
            metric
                if [
                    "MKT_D10", "MKT_F5", "MKT_CP5", "MKT_VA5", "MKT_LO5", "MKT_AC5", "MKT_IS5",
                    "MKT_CF20", "MKT_DT5", "MKT_SC5",
                ]
                .contains(&metric) =>
            {
                format!(
                    "(SELECT bucket FROM market WHERE metric='{metric}' AND trade_date=o.trade_date)"
                )
            }
            _ => return Err("不支持的排名切片".into()),
        };
        format!(
            " AND EXISTS(SELECT 1 FROM observations o WHERE o.ts_code=joined.ts_code AND o.trade_date=joined.trade_date AND {expression}='{}')",
            bucket.replace('\'', "''")
        )
    } else {
        String::new()
    };
    let has_windows: bool = conn
        .query_row(
            "SELECT count(*)>0 FROM information_schema.tables WHERE table_name='label_windows'",
            [],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    let has_observations: bool = conn
        .query_row(
            "SELECT count(*)>0 FROM information_schema.tables WHERE table_name='observations'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    let horizon = archive
        .configuration
        .get("settings")
        .and_then(|value| value.get("backtest_period"))
        .and_then(Value::as_u64)
        .unwrap_or(1)
        .max(1);
    let missing_reason_sql = if has_observations {
        format!(
            "CASE WHEN l.residual_return IS NOT NULL THEN NULL WHEN NOT coalesce((SELECT quote_present FROM observations o WHERE o.ts_code=m.ts_code AND o.trade_date=m.trade_date),false) THEN '信号日行情缺失（历史池成员保留）' WHEN (SELECT count(*) FROM calendar WHERE trade_date>m.trade_date)<{horizon} THEN '评价区间末端标签未成熟' WHEN (SELECT count(*) FROM observations o WHERE o.ts_code=m.ts_code AND o.trade_date>m.trade_date AND quote_present)<{horizon} THEN '个股后续行情不足（含停牌与历史缺失）' ELSE '行情无效或指数/概念/行业扣减资料缺失' END"
        )
    } else {
        "CASE WHEN l.residual_return IS NULL THEN '旧运行未记录缺失原因' END".into()
    };
    let window_columns = if has_windows {
        "w.entry_date,w.label_end_date"
    } else {
        "NULL::VARCHAR AS entry_date,NULL::VARCHAR AS label_end_date"
    };
    let window_join = if has_windows {
        "LEFT JOIN label_windows w ON w.ts_code=m.ts_code AND w.trade_date=m.trade_date"
    } else {
        ""
    };
    let pools = if pool_kind == "daily" {
        "SELECT side, trade_date, ts_code FROM daily WHERE (? = 0 OR member_position <= ?)"
    } else {
        "SELECT cur_day.side, cur_day.trade_date, hist.ts_code FROM days cur_day JOIN days past_day ON past_day.side = cur_day.side AND past_day.day_number BETWEEN cur_day.day_number - 2 AND cur_day.day_number JOIN daily hist ON hist.side = past_day.side AND hist.trade_date = past_day.trade_date WHERE cur_day.day_number >= 3 AND (? = 0 OR hist.member_position <= ?) GROUP BY cur_day.side, cur_day.trade_date, hist.ts_code"
    };
    let readiness = if pool_kind == "daily" { "1" } else { "3" };
    let selection = match membership {
        "all" => "TRUE",
        "added" => "membership = 'added'",
        "removed" => "membership = 'removed'",
        "shared" => "membership = 'shared'",
        "missing_label" => "residual_return IS NULL",
        _ => unreachable!(),
    };
    let query = format!(
        r#"
        WITH days AS (
            SELECT side, trade_date, ROW_NUMBER() OVER (PARTITION BY side ORDER BY trade_date) AS day_number
            FROM (SELECT DISTINCT side, trade_date FROM ranking_members WHERE rank > 0 AND isfinite(score))
        ),
        daily AS (
            SELECT side, trade_date, ts_code,
                ROW_NUMBER() OVER (PARTITION BY side, trade_date ORDER BY rank, ts_code) AS member_position
            FROM ranking_members WHERE eligibility = 1 AND rank > 0 AND isfinite(score)
        ),
        pools AS ({pools}),
        common_days AS (
            SELECT b.trade_date FROM days b JOIN days c ON b.trade_date = c.trade_date
            WHERE b.side = 'baseline' AND c.side = 'candidate' AND b.day_number >= {readiness} AND c.day_number >= {readiness}
        ),
        members AS (
            SELECT COALESCE(b.trade_date, c.trade_date) AS trade_date, COALESCE(b.ts_code, c.ts_code) AS ts_code,
                CASE WHEN b.ts_code IS NULL THEN 'added' WHEN c.ts_code IS NULL THEN 'removed' ELSE 'shared' END AS membership
            FROM (SELECT trade_date, ts_code FROM pools WHERE side = 'baseline') b
            FULL OUTER JOIN (SELECT trade_date, ts_code FROM pools WHERE side = 'candidate') c
              ON b.trade_date = c.trade_date AND b.ts_code = c.ts_code
        ),
        joined AS (
            SELECT m.trade_date, m.ts_code, m.membership,
                b.score AS baseline_score, b.rank AS baseline_rank,
                c.score AS candidate_score, c.rank AS candidate_rank, l.residual_return, {window_columns}, {missing_reason_sql} AS missing_reason
            FROM members m JOIN common_days d ON d.trade_date = m.trade_date
            LEFT JOIN ranking_members b ON b.side = 'baseline' AND b.trade_date = m.trade_date AND b.ts_code = m.ts_code AND b.eligibility = 1 AND b.rank > 0 AND isfinite(b.score)
            LEFT JOIN ranking_members c ON c.side = 'candidate' AND c.trade_date = m.trade_date AND c.ts_code = m.ts_code AND c.eligibility = 1 AND c.rank > 0 AND isfinite(c.score)
            LEFT JOIN labels l ON l.trade_date = m.trade_date AND l.ts_code = m.ts_code
            {window_join}
            WHERE m.trade_date >= ? AND m.trade_date <= ?
        ), filtered AS (SELECT * FROM joined WHERE {selection}{slice_filter})
    "#
    );
    let count_sql = format!("{query} SELECT COUNT(*) FROM filtered");
    let total: i64 = conn
        .query_row(
            &count_sql,
            duckdb::params![top_k as i64, top_k as i64, start_date, end_date],
            |row| row.get(0),
        )
        .map_err(|e| format!("统计排名成员失败: {e}"))?;
    let page_sql = format!(
        "{query} SELECT trade_date, ts_code, membership, baseline_score, baseline_rank, candidate_score, candidate_rank, residual_return, entry_date, label_end_date, missing_reason FROM filtered ORDER BY trade_date DESC, membership, ts_code LIMIT ? OFFSET ?"
    );
    let mut stmt = conn
        .prepare(&page_sql)
        .map_err(|e| format!("准备排名成员分页失败: {e}"))?;
    let mut rows = stmt
        .query(duckdb::params![
            top_k as i64,
            top_k as i64,
            start_date,
            end_date,
            limit as i64,
            offset as i64
        ])
        .map_err(|e| format!("读取排名成员失败: {e}"))?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let residual_return: Option<f64> = row.get(7).map_err(|e| e.to_string())?;
        items.push(RankComparisonMemberRow {
            trade_date: row.get(0).map_err(|e| e.to_string())?,
            ts_code: row.get(1).map_err(|e| e.to_string())?,
            membership: row.get(2).map_err(|e| e.to_string())?,
            baseline_score: row.get(3).map_err(|e| e.to_string())?,
            baseline_rank: row.get(4).map_err(|e| e.to_string())?,
            candidate_score: row.get(5).map_err(|e| e.to_string())?,
            candidate_rank: row.get(6).map_err(|e| e.to_string())?,
            residual_return,
            entry_date: row.get(8).map_err(|e| e.to_string())?,
            label_end_date: row.get(9).map_err(|e| e.to_string())?,
            missing_reason: row.get(10).map_err(|e| e.to_string())?,
        });
    }
    Ok(RankComparisonMemberPage {
        total: total.max(0) as u64,
        offset,
        items,
    })
}
