use std::{
    collections::HashMap,
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use lianghua_provider::crawler::volume_ratio::TencentVolumeRatioData;

type SavedBaseline = (TencentVolumeRatioData, usize, f64);

enum Command {
    Save(String, SavedBaseline),
    Flush(mpsc::Sender<Result<(), String>>),
    Stop,
}

pub(super) struct Store {
    sender: mpsc::Sender<Command>,
    error: Arc<Mutex<Option<String>>>,
    worker: Option<JoinHandle<()>>,
}

impl Store {
    pub(super) fn new(mut conn: rusqlite::Connection, date: String) -> Result<Self, String> {
        let (sender, receiver) = mpsc::channel();
        let error = Arc::new(Mutex::new(None));
        let worker_error = Arc::clone(&error);
        let worker = thread::Builder::new()
            .name("volume-ratio-store".into())
            .spawn(move || {
                let mut pending = HashMap::new();
                let mut deadline = Instant::now() + Duration::from_millis(250);
                loop {
                    let command = if pending.is_empty() {
                        deadline = Instant::now() + Duration::from_millis(250);
                        receiver
                            .recv()
                            .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
                    } else {
                        receiver.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    };
                    let mut reply = None;
                    let mut stop = false;
                    match command {
                        Ok(Command::Save(code, baseline)) => {
                            if pending.is_empty() {
                                deadline = Instant::now() + Duration::from_millis(250);
                            }
                            pending.insert(code, baseline);
                        }
                        Ok(Command::Flush(sender)) => reply = Some(sender),
                        Ok(Command::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                            stop = true
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    // 固定截止时间，不因不断到达的新结果推迟提交。
                    if Instant::now() < deadline && reply.is_none() && !stop {
                        continue;
                    }
                    let result = write_batch(&mut conn, &date, &pending);
                    if result.is_ok() {
                        pending.clear();
                    }
                    *worker_error.lock().unwrap_or_else(|e| e.into_inner()) = result.clone().err();
                    deadline = Instant::now()
                        + if result.is_ok() {
                            Duration::from_millis(250)
                        } else {
                            Duration::from_secs(1)
                        };
                    if let Some(reply) = reply {
                        let _ = reply.send(result.clone());
                    }
                    if stop {
                        if let Err(error) = result {
                            eprintln!("关闭量比缓存时落盘失败: {error}");
                        }
                        break;
                    }
                }
            })
            .map_err(|e| format!("创建量比落盘线程失败: {e}"))?;
        Ok(Self {
            sender,
            error,
            worker: Some(worker),
        })
    }

    pub(super) fn save(&self, code: String, baseline: SavedBaseline) -> Result<(), String> {
        self.sender
            .send(Command::Save(code, baseline))
            .map_err(|_| "量比落盘线程已停止，数据尚未落盘".to_string())
    }

    pub(super) fn error(&self) -> Option<String> {
        self.error.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub(super) fn flush(&self) -> Result<(), String> {
        let (sender, receiver) = mpsc::channel();
        self.sender
            .send(Command::Flush(sender))
            .map_err(|_| "量比落盘线程已停止".to_string())?;
        receiver
            .recv()
            .map_err(|_| "量比落盘线程未完成提交".to_string())?
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // 等待线程退出、连接关闭后，调用方才可删除 SQLite/WAL 或切换日期。
        let _ = self.sender.send(Command::Stop);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn write_batch(
    conn: &mut rusqlite::Connection,
    date: &str,
    pending: &HashMap<String, SavedBaseline>,
) -> Result<(), String> {
    if pending.is_empty() {
        return Ok(());
    }
    let transaction = conn
        .transaction()
        .map_err(|e| format!("写入量比缓存失败: {e}"))?;
    {
        let mut statement = transaction.prepare_cached(
            "INSERT INTO baselines_v2 (code, trade_date, baseline) VALUES (?1, ?2, ?3)
             ON CONFLICT(code) DO UPDATE SET trade_date=excluded.trade_date, baseline=excluded.baseline",
        ).map_err(|e| e.to_string())?;
        for (code, baseline) in pending {
            let bytes = bincode::serde::encode_to_vec(baseline, bincode::config::legacy())
                .map_err(|e| e.to_string())?;
            statement
                .execute(rusqlite::params![code, date, bytes])
                .map_err(|e| e.to_string())?;
        }
    }
    transaction.commit().map_err(|e| e.to_string())
}
