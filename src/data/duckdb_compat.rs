use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    thread,
    time::Duration,
};

#[cfg(target_os = "windows")]
use duckdb::{AccessMode, Config};
use duckdb::{Connection, Result as DuckResult};

const OPEN_RETRY_DELAYS_MS: [u64; 4] = [40, 100, 200, 400];

static DB_WRITE_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

pub fn open_with_retry<P: AsRef<Path>>(path: P) -> DuckResult<Connection> {
    let path = path.as_ref();
    let mut last_error = match Connection::open(path) {
        Ok(conn) => return Ok(conn),
        Err(error) => error,
    };

    if !looks_like_connection_conflict(&last_error) {
        return Err(last_error);
    }

    for delay_ms in OPEN_RETRY_DELAYS_MS {
        thread::sleep(Duration::from_millis(delay_ms));
        match Connection::open(path) {
            Ok(conn) => return Ok(conn),
            Err(error) => last_error = error,
        }
        if !looks_like_connection_conflict(&last_error) {
            return Err(last_error);
        }
    }

    Err(last_error)
}

pub fn open_read_compatible<P: AsRef<Path>>(path: P) -> DuckResult<Connection> {
    let path = path.as_ref();
    let error = match open_with_retry(path) {
        Ok(conn) => return Ok(conn),
        Err(error) => error,
    };

    #[cfg(target_os = "windows")]
    {
        if looks_like_connection_conflict(&error) {
            let config = Config::default().access_mode(AccessMode::ReadOnly)?;
            if let Ok(conn) = Connection::open_with_flags(path, config) {
                return Ok(conn);
            }
        }
    }

    Err(error)
}

pub fn with_db_write_lock<T>(path: impl AsRef<Path>, action: impl FnOnce() -> T) -> T {
    let lock = write_lock_for_path(path.as_ref());
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    action()
}

fn looks_like_connection_conflict(error: &duckdb::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("connection")
        || message.contains("lock")
        || message.contains("being used by another process")
        || message.contains("already")
        || message.contains("different configuration")
}

fn write_lock_for_path(path: &Path) -> Arc<Mutex<()>> {
    let key = path_key(path);
    let locks = DB_WRITE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    locks
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn path_key(path: &Path) -> String {
    let normalized = path
        .canonicalize()
        .unwrap_or_else(|_| absolute_path_fallback(path));
    let key = normalized.to_string_lossy().to_string();

    #[cfg(target_os = "windows")]
    {
        key.to_ascii_lowercase()
    }
    #[cfg(not(target_os = "windows"))]
    {
        key
    }
}

fn absolute_path_fallback(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    std::env::current_dir()
        .map(|current_dir| current_dir.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}
