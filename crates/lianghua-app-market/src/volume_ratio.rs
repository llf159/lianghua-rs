use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

mod store;

use duckdb::{Connection, params};
use lianghua_data::data::{load_stock_list, load_trade_date_list, source_db_path};
use lianghua_provider::crawler::{
    SinaQuote,
    volume_ratio::{
        IntradayVolumeSource, TencentVolumeRatioData, fetch_msn_volume_ratio,
        fetch_tencent_volume_ratio, fetch_volume_ratio_from_source, intraday_trading_minutes,
        parse_volume_ratio_from_source,
    },
};

pub const VOLUME_RATIO_SOURCES: [&str; 7] = [
    "tencent",
    "eastmoney",
    "microsoft",
    "tonghuashun",
    "xueqiu",
    "sse",
    "szse",
];

pub fn load_volume_ratio_averages(
    source_path: &str,
    trade_date: &str,
    ts_code: Option<&str>,
) -> Result<HashMap<String, f64>, String> {
    let conn = crate::data::open_db_readonly(source_db_path(source_path))
        .map_err(|e| format!("打开本地量比日线失败: {e}"))?;
    load_local_volume_averages(&conn, trade_date, ts_code)
}

fn load_local_volume_averages(
    conn: &Connection,
    trade_date: &str,
    ts_code: Option<&str>,
) -> Result<HashMap<String, f64>, String> {
    let mut stmt = conn
        .prepare(
            "WITH recent AS (
                SELECT ts_code, trade_date, TRY_CAST(vol AS DOUBLE) AS volume,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS position
                FROM stock_data
                WHERE adj_type='qfq' AND trade_date < ? AND (? IS NULL OR ts_code=?)
                    AND TRY_CAST(vol AS DOUBLE)>0 AND ISFINITE(TRY_CAST(vol AS DOUBLE))
            )
            SELECT ts_code, AVG(volume) FROM recent WHERE position<=5
            GROUP BY ts_code HAVING COUNT(*)=5 AND COUNT(DISTINCT trade_date)=5",
        )
        .map_err(|e| format!("预编译本地量比基准失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, ts_code, ts_code])
        .map_err(|e| format!("查询本地量比基准失败: {e}"))?;
    let mut values = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取本地量比基准失败: {e}"))?
    {
        values.insert(
            row.get(0)
                .map_err(|e| format!("读取本地量比代码失败: {e}"))?,
            row.get(1)
                .map_err(|e| format!("读取本地量比均量失败: {e}"))?,
        );
    }
    Ok(values)
}

pub(crate) fn current_volume_ratio(
    quote: &SinaQuote,
    average_daily_volume: f64,
    volume_to_hands: f64,
) -> Option<f64> {
    let minutes = intraday_trading_minutes(&quote.time)?;
    if !quote.vol.is_finite()
        || quote.vol < 0.0
        || !average_daily_volume.is_finite()
        || average_daily_volume <= 0.0
    {
        return None;
    }
    let value = quote.vol * volume_to_hands / (average_daily_volume * minutes / 240.0);
    value.is_finite().then_some(value)
}

#[derive(Clone)]
struct Baseline {
    data: TencentVolumeRatioData,
    source: usize,
    observed_high: f64,
    observed_opening_high: f64,
    next_point: usize,
    loaded_at: Instant,
}

impl Baseline {
    fn apply_quote(&mut self, quote: &SinaQuote) -> Option<VolumeRatioValue> {
        let quote_minute = quote.time.replace(':', "");
        let quote_minute = quote_minute.get(..4)?;
        let (minutes, value) = if let Some(minutes) = intraday_trading_minutes(&quote.time) {
            (
                minutes,
                current_volume_ratio(
                    quote,
                    self.data.average_daily_volume,
                    self.data.volume_to_hands,
                )?,
            )
        } else {
            // 午休冻结上午值，盘后冻结收盘值，不能把休市时间或盘后量混入计算。
            let minutes = if quote.time.as_str() > "15:00:00" {
                240.0
            } else if quote.time.as_str() > "11:30:00" && quote.time.as_str() < "13:00:00" {
                120.0
            } else {
                return None;
            };
            let end = self.data.points.partition_point(|point| {
                point.trading_minutes <= minutes && point.time.as_str() <= quote_minute
            });
            let last = self.data.points.get(end.checked_sub(1)?)?;
            (minutes, last.volume_ratio)
        };
        // 曲线时间单调递增，每个历史点只归并一次；实时值每轮仍重新计算。
        while let Some(point) = self.data.points.get(self.next_point) {
            if point.trading_minutes > minutes || point.time.as_str() > quote_minute {
                break;
            }
            self.observed_high = self.observed_high.max(point.volume_ratio);
            if point.trading_minutes <= 5.0 {
                self.observed_opening_high = self.observed_opening_high.max(point.volume_ratio);
            }
            self.next_point += 1;
        }
        let high = self.observed_high.max(value);
        self.observed_high = high;
        if minutes <= 5.0 {
            self.observed_opening_high = self.observed_opening_high.max(value);
        }
        // 必须逐分钟覆盖开盘窗口，不能仅凭末点到达 09:35 判断完整。
        let opening_high = (minutes > 5.0 && self.data.opening_window_complete())
            .then_some(self.observed_opening_high);
        Some(VolumeRatioValue {
            current: value,
            high,
            opening_high,
        })
    }
}

enum Entry {
    Pending(Option<Baseline>),
    Ready(Baseline),
    Failed {
        at: Instant,
        baseline: Option<Baseline>,
    },
}

impl Entry {
    fn apply_quote(&mut self, quote: &SinaQuote) -> Option<VolumeRatioValue> {
        match self {
            Self::Ready(baseline)
            | Self::Pending(Some(baseline))
            | Self::Failed {
                baseline: Some(baseline),
                ..
            } => baseline.apply_quote(quote),
            _ => None,
        }
    }
}

#[derive(Default)]
struct Cache {
    store: Option<store::Store>,
    dirty: HashSet<String>,
    persistence_error: Option<String>,
    source_path: String,
    date: String,
    revision: u64,
    generation: u64,
    last_poll: Option<Instant>,
    entries: HashMap<String, Entry>,
    local_averages: HashMap<String, f64>,
    local_loaded_at: Option<Instant>,
    local_error: Option<String>,
    paused: bool,
    preflight_checked: bool,
    preflight_blocked: bool,
    msn_enabled: bool,
    source_results: [Option<bool>; VOLUME_RATIO_SOURCES.len()],
    next_request_at: [Option<Instant>; VOLUME_RATIO_SOURCES.len()],
    cooldown_until: [Option<Instant>; VOLUME_RATIO_SOURCES.len()],
    request_interval_ms: [u64; VOLUME_RATIO_SOURCES.len()],
    source_cursor: usize,
    recovery_at: [Option<Instant>; VOLUME_RATIO_SOURCES.len()],
    source_successes: [u8; VOLUME_RATIO_SOURCES.len()],
}

impl Cache {
    fn check_before_fetch(&mut self) -> Result<(), String> {
        if self.paused || self.preflight_checked {
            return Ok(());
        }
        let dates = load_trade_date_list(&self.source_path)?;
        if !dates.contains(&self.date) {
            return Err(format!(
                "交易日历未覆盖行情日期 {}，请先更新原始数据和交易日历，再开启量比拉取。",
                self.date
            ));
        }
        let required_date = dates
            .iter()
            .filter(|date| *date < &self.date)
            .max()
            .ok_or("交易日历缺少上一交易日，请先更新交易日历。")?;
        let conn = crate::data::open_db_readonly(source_db_path(&self.source_path))
            .map_err(|e| format!("检查量比原始行情库失败: {e}"))?;
        let latest_date: Option<String> = conn
            .query_row(
                "SELECT MAX(trade_date) FROM stock_data WHERE adj_type='qfq' AND trade_date < ?",
                [&self.date],
                |row| row.get(0),
            )
            .map_err(|e| format!("检查量比日线日期失败: {e}"))?;
        if latest_date.as_ref().is_none_or(|date| date < required_date) {
            return Err(format!(
                "量比拉取需要日线更新至 {required_date}，本地行情日前最新日线为 {}。请先更新原始行情库，再开启量比拉取。",
                latest_date.as_deref().unwrap_or("无数据")
            ));
        }
        drop(conn);
        let stocks = load_stock_list(&self.source_path)?;
        let list_date = stocks.iter().filter_map(|row| row.get(6)).max();
        if list_date.is_none_or(|date| date < required_date) {
            return Err(format!(
                "量比拉取需要股票列表更新至 {required_date}，本地列表日期为 {}。请先刷新股票列表，再开启量比拉取。",
                list_date.map(String::as_str).unwrap_or("未知")
            ));
        }
        // 开启时强制读取最新均量，避免沿用更新原始库之前的 60 秒缓存。
        let baselines = super::daily_baseline::load(&self.source_path, &self.date, true)?;
        self.local_averages = baselines
            .rows
            .iter()
            .filter_map(|(code, row)| row.average_volume.map(|average| (code.clone(), average)))
            .collect();
        self.local_error = baselines.volume_error.clone();
        self.local_loaded_at = Some(Instant::now());
        for (code, entry) in &self.entries {
            let baseline = match entry {
                Entry::Ready(baseline)
                | Entry::Pending(Some(baseline))
                | Entry::Failed {
                    baseline: Some(baseline),
                    ..
                } => baseline,
                _ => continue,
            };
            if let Some(average) = self.local_averages.get(code)
                && (baseline.data.average_daily_volume - average).abs() > average.abs() * 1e-9
            {
                return Err("已有量比缓存使用的五日均量与当前原始库不一致，请到“数据管理 → 量比数据”清空缓存后重新拉取。".into());
            }
        }
        self.preflight_checked = true;
        Ok(())
    }

    fn restore(&mut self) -> Result<(), String> {
        self.store = None;
        self.dirty.clear();
        let mut conn = rusqlite::Connection::open(
            Path::new(&self.source_path).join("intraday_volume_ratio.sqlite3"),
        )
        .map_err(|error| format!("打开量比缓存失败: {error}"))?;
        conn.busy_timeout(Duration::from_secs(2))
            .map_err(|error| error.to_string())?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
            DROP TABLE IF EXISTS baselines_v1;
            CREATE TABLE IF NOT EXISTS baselines_v2 (
                code TEXT PRIMARY KEY, trade_date TEXT NOT NULL, baseline BLOB NOT NULL
            );",
        )
        .map_err(|error| error.to_string())?;
        let transaction = conn.transaction().map_err(|error| error.to_string())?;
        transaction
            .execute(
                "DELETE FROM baselines_v2 WHERE trade_date != ?1",
                [&self.date],
            )
            .map_err(|error| error.to_string())?;
        {
            let mut statement = transaction
                .prepare("SELECT code, baseline FROM baselines_v2 WHERE trade_date = ?1")
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map([&self.date], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(|error| error.to_string())?;
            for row in rows {
                let (code, bytes) = row.map_err(|error| error.to_string())?;
                match bincode::serde::decode_from_slice::<(TencentVolumeRatioData, usize, f64), _>(
                    &bytes,
                    bincode::config::legacy(),
                ) {
                    Ok(((data, source, observed_opening_high), _))
                        if data.trade_date == self.date =>
                    {
                        let baseline = Baseline {
                            data,
                            source,
                            observed_high: 0.0,
                            observed_opening_high,
                            next_point: 0,
                            loaded_at: Instant::now(),
                        };
                        self.entries.insert(code, Entry::Ready(baseline));
                    }
                    _ => {
                        self.persistence_error =
                            Some("部分量比缓存损坏，已跳过并重新补齐".to_string());
                    }
                }
            }
        }
        transaction.commit().map_err(|error| error.to_string())?;
        self.store = Some(store::Store::new(conn, self.date.clone())?);
        Ok(())
    }

    fn persist(&mut self, wait: bool) -> Result<(), String> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        let store = self.store.as_ref().ok_or("量比缓存未打开，数据尚未落盘")?;
        for code in self.dirty.iter().cloned().collect::<Vec<_>>() {
            let baseline = match self.entries.get(&code) {
                Some(Entry::Ready(baseline) | Entry::Pending(Some(baseline)))
                | Some(Entry::Failed {
                    baseline: Some(baseline),
                    ..
                }) => baseline,
                _ => {
                    self.dirty.remove(&code);
                    continue;
                }
            };
            // 缓存锁内只复制待写字段并入队；编码与 SQLite 事务都由独立线程执行。
            store.save(
                code.clone(),
                (
                    baseline.data.clone(),
                    baseline.source,
                    baseline.observed_opening_high,
                ),
                wait,
            )?;
            self.dirty.remove(&code);
        }
        self.persistence_error = None;
        Ok(())
    }

    fn reserve_source(
        &mut self,
        now: Instant,
        eligible: u8,
        attempted: u8,
    ) -> Option<(usize, Duration)> {
        let deadlines: [Instant; VOLUME_RATIO_SOURCES.len()] = std::array::from_fn(|index| {
            self.next_request_at[index]
                .into_iter()
                .chain(self.cooldown_until[index])
                .max()
                .unwrap_or(now)
        });
        let source = (0..VOLUME_RATIO_SOURCES.len())
            .filter(|&index| eligible & (1 << index) != 0 && attempted & (1 << index) == 0)
            .min_by_key(|&index| {
                (
                    deadlines[index].max(now),
                    (index + VOLUME_RATIO_SOURCES.len() - self.source_cursor)
                        % VOLUME_RATIO_SOURCES.len(),
                )
            })?;
        let wait = deadlines[source].saturating_duration_since(now);
        if wait.is_zero() {
            let interval = self.request_interval_ms[source].max(60);
            self.next_request_at[source] = Some(now + Duration::from_millis(interval));
            self.source_cursor = (source + 1) % VOLUME_RATIO_SOURCES.len();
        }
        Some((source, wait))
    }

    fn throttle_source(&mut self, source: usize, now: Instant) {
        self.cooldown_until[source] = Some(now + Duration::from_secs(20));
        self.recovery_at[source] = Some(now + Duration::from_secs(40));
        self.source_successes[source] = 0;
        self.request_interval_ms[source] = self.request_interval_ms[source]
            .max(60)
            .saturating_mul(2)
            .min(2000);
    }

    fn handle_fetch_error(&mut self, source: usize, error: &str, now: Instant) {
        self.source_results[source] = Some(false);
        self.source_successes[source] = 0;
        self.recovery_at[source] = Some(now + Duration::from_secs(20));
        let status = error
            .split("HTTP状态=")
            .nth(1)
            .and_then(|value| value.get(..3))
            .and_then(|value| value.parse::<u16>().ok());
        if status.is_some_and(|status| {
            matches!(status, 401 | 403 | 429) || (500..=599).contains(&status)
        }) {
            self.throttle_source(source, now);
        }
        // 超时、断连与读取失败只重试该股；没有 HTTP 状态不能推断全源限流。
    }

    fn handle_fetch_success(&mut self, source: usize, started: Instant, now: Instant) {
        // 限流前已在途的响应不能作为冷却后恢复的依据。
        if self.cooldown_until[source].is_some_and(|until| started < until) {
            return;
        }
        if self.request_interval_ms[source] <= 60 {
            return;
        }
        self.source_successes[source] = self.source_successes[source].saturating_add(1);
        if self.source_successes[source] >= 8
            && self.recovery_at[source].is_some_and(|at| now >= at)
        {
            self.request_interval_ms[source] = (self.request_interval_ms[source] / 2).max(60);
            self.source_successes[source] = 0;
            self.recovery_at[source] = Some(now + Duration::from_secs(20));
        }
    }

    fn set_paused(&mut self, paused: bool) {
        if self.paused == paused {
            return;
        }
        self.paused = paused;
        self.preflight_checked = false;
        if !paused {
            self.preflight_blocked = false;
        }
        self.generation += 1;
        self.revision += 1;
        // 中止排队和等待限速的任务，但保留已完成基准及其高点。
        self.entries.retain(|_, entry| {
            if let Entry::Pending(previous) = entry {
                if let Some(baseline) = previous.take() {
                    *entry = Entry::Ready(baseline);
                } else {
                    return false;
                }
            }
            true
        });
    }
}

#[derive(Debug, PartialEq)]
pub struct VolumeRatioValue {
    pub current: f64,
    pub high: f64,
    pub opening_high: Option<f64>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct VolumeRatioSourceStatus {
    pub source: &'static str,
    pub state: &'static str,
}

pub struct VolumeRatioSnapshot {
    pub values: HashMap<String, VolumeRatioValue>,
    pub revision: u64,
    pub status: Option<String>,
    pub source_statuses: Vec<VolumeRatioSourceStatus>,
}

static CACHE: OnceLock<Arc<Mutex<Cache>>> = OnceLock::new();
pub type VolumeRatioSessionFetch = Arc<dyn Fn(&str) -> Result<String, String> + Send + Sync>;
static SESSION_FETCHES: OnceLock<Mutex<HashMap<usize, VolumeRatioSessionFetch>>> = OnceLock::new();

pub fn volume_ratio_session_fetch(source: &str) -> Result<Option<VolumeRatioSessionFetch>, String> {
    let index = VOLUME_RATIO_SOURCES
        .iter()
        .position(|name| *name == source)
        .ok_or("未知量比来源")?;
    Ok(SESSION_FETCHES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| "来源会话锁已损坏")?
        .get(&index)
        .cloned())
}

pub fn fetch_eastmoney_intraday_text(
    http: &reqwest::blocking::Client,
    ts_code: &str,
) -> Result<String, String> {
    match volume_ratio_session_fetch("eastmoney")? {
        Some(fetch) => fetch(ts_code),
        None => lianghua_provider::crawler::intraday::fetch_eastmoney_intraday_text(http, ts_code),
    }
}

pub fn set_eastmoney_browser_fetch(fetch: Option<VolumeRatioSessionFetch>) -> Result<(), String> {
    set_volume_ratio_session_fetch("eastmoney", fetch)
}

pub fn set_volume_ratio_session_fetch(
    source: &str,
    fetch: Option<VolumeRatioSessionFetch>,
) -> Result<(), String> {
    let index = VOLUME_RATIO_SOURCES
        .iter()
        .position(|name| *name == source)
        .ok_or("未知量比来源")?;
    {
        let mut sessions = SESSION_FETCHES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| "来源会话锁已损坏")?;
        if let Some(fetch) = fetch {
            sessions.insert(index, fetch);
        } else {
            sessions.remove(&index);
        }
    }
    if let Some(cache) = CACHE.get() {
        let mut state = cache.lock().map_err(|_| "量比缓存锁已损坏")?;
        state.cooldown_until[index] = None;
        state.next_request_at[index] = None;
        state.source_results[index] = None;
        state.request_interval_ms[index] = 0;
        state.recovery_at[index] = None;
        state.source_successes[index] = 0;
        // 会话切换后允许失败股票立即重试，已补齐的基准继续使用。
        for entry in state.entries.values_mut() {
            if let Entry::Failed { at, .. } = entry {
                *at = Instant::now() - Duration::from_secs(60);
            }
        }
    }
    Ok(())
}

static WORKERS: OnceLock<Result<(rayon::ThreadPool, reqwest::blocking::Client), String>> =
    OnceLock::new();

pub fn set_fetch_paused(paused: bool) -> Result<(), String> {
    {
        let mut state = CACHE
            .get_or_init(|| Arc::new(Mutex::new(Cache::default())))
            .lock()
            .map_err(|_| "量比缓存锁已损坏".to_string())?;
        state.set_paused(paused);
        if paused {
            state.persist(true)?;
            if let Some(store) = &state.store {
                store.flush()?;
            }
        }
    }
    if !paused {
        // 更新列表后直接重新开启即可，无需重启应用才能去掉退市股票。
        super::all_market_monitor::source_meta_cache()
            .lock()
            .map_err(|_| "股票基础信息缓存锁已损坏")?
            .clear();
    }
    Ok(())
}

#[derive(serde::Serialize)]
pub struct VolumeRatioCacheStatus {
    pub path: String,
    pub exists: bool,
    pub size_bytes: u64,
    pub stock_count: u64,
    pub trade_dates: Vec<String>,
}

pub fn inspect_cache(source_path: &str) -> Result<VolumeRatioCacheStatus, String> {
    let mut state = CACHE
        .get_or_init(|| Arc::new(Mutex::new(Cache::default())))
        .lock()
        .map_err(|_| "量比缓存锁已损坏")?;
    if state.source_path == source_path {
        state.persist(true)?;
        if let Some(store) = &state.store {
            store.flush()?;
        }
    }
    let path = Path::new(source_path).join("intraday_volume_ratio.sqlite3");
    let mut status = VolumeRatioCacheStatus {
        path: path.to_string_lossy().into_owned(),
        exists: path.exists(),
        size_bytes: 0,
        stock_count: 0,
        trade_dates: Vec::new(),
    };
    if !status.exists {
        return Ok(status);
    }
    let conn =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| format!("打开量比缓存失败: {e}"))?;
    conn.busy_timeout(Duration::from_secs(2))
        .map_err(|e| e.to_string())?;
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='baselines_v2')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    if exists {
        status.stock_count = conn
            .query_row("SELECT COUNT(*) FROM baselines_v2", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|e| e.to_string())? as u64;
        let mut stmt = conn
            .prepare("SELECT DISTINCT trade_date FROM baselines_v2 ORDER BY trade_date DESC")
            .map_err(|e| e.to_string())?;
        status.trade_dates = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
    }
    for suffix in ["", "-wal", "-shm"] {
        match std::fs::metadata(format!("{}{suffix}", status.path)) {
            Ok(metadata) => status.size_bytes += metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("读取量比缓存大小失败: {error}")),
        }
    }
    Ok(status)
}

pub fn clear_cache(source_path: &str) -> Result<(), String> {
    let mut state = CACHE
        .get_or_init(|| Arc::new(Mutex::new(Cache::default())))
        .lock()
        .map_err(|_| "量比缓存锁已损坏")?;
    if state.source_path == source_path {
        // 即使已经暂停，也必须使在途任务失效，避免清空后重新落盘。
        state.generation += 1;
        state.revision += 1;
        state.paused = true;
        state.preflight_checked = false;
        state.preflight_blocked = false;
        state.entries.clear();
        state.source_results = [None; VOLUME_RATIO_SOURCES.len()];
        state.next_request_at = [None; VOLUME_RATIO_SOURCES.len()];
        state.cooldown_until = [None; VOLUME_RATIO_SOURCES.len()];
        state.request_interval_ms = [0; VOLUME_RATIO_SOURCES.len()];
        state.recovery_at = [None; VOLUME_RATIO_SOURCES.len()];
        state.source_successes = [0; VOLUME_RATIO_SOURCES.len()];
        state.dirty.clear();
        state.local_averages.clear();
        state.local_loaded_at = None;
        state.local_error = None;
        state.persistence_error = None;
        state.store = None;
        state.source_path.clear();
    }
    // 所有缓存连接均已关闭，持锁删除主文件和 WAL，损坏缓存也可清理。
    for name in [
        "intraday_volume_ratio.sqlite3",
        "intraday_volume_ratio.sqlite3-wal",
        "intraday_volume_ratio.sqlite3-shm",
    ] {
        match std::fs::remove_file(Path::new(source_path).join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("清空量比缓存失败: {error}")),
        }
    }
    Ok(())
}

pub fn snapshot(
    source_path: &str,
    quotes: &HashMap<String, SinaQuote>,
    trade_date: &str,
) -> Result<VolumeRatioSnapshot, String> {
    let cache = CACHE.get_or_init(|| Arc::new(Mutex::new(Cache::default())));
    let (workers, http) = WORKERS
        .get_or_init(|| {
            let workers = rayon::ThreadPoolBuilder::new()
                .num_threads(8)
                .thread_name(|index| format!("volume-ratio-{index}"))
                .build()
                .map_err(|e| format!("创建量比补齐线程失败: {e}"))?;
            let http = reqwest::blocking::Client::builder()
                .connect_timeout(Duration::from_secs(6))
                .timeout(Duration::from_secs(12))
                .build()
                .map_err(|e| format!("创建量比客户端失败: {e}"))?;
            Ok((workers, http))
        })
        .as_ref()
        .map_err(Clone::clone)?;
    let mut state = cache.lock().map_err(|_| "量比缓存锁已损坏".to_string())?;
    if state.source_path != source_path || state.date != trade_date {
        state.persist(true)?;
        state.store = None;
        state.date = trade_date.to_string();
        state.source_path = source_path.to_string();
        state.entries.clear();
        state.source_results = [None; VOLUME_RATIO_SOURCES.len()];
        state.next_request_at = [None; VOLUME_RATIO_SOURCES.len()];
        state.cooldown_until = [None; VOLUME_RATIO_SOURCES.len()];
        state.request_interval_ms = [0; VOLUME_RATIO_SOURCES.len()];
        state.recovery_at = [None; VOLUME_RATIO_SOURCES.len()];
        state.source_successes = [0; VOLUME_RATIO_SOURCES.len()];
        state.revision += 1;
        state.generation += 1;
        state.local_loaded_at = None;
        state.preflight_checked = false;
        state.preflight_blocked = false;
        state.persistence_error = None;
        if let Err(error) = state.restore() {
            state.persistence_error = Some(error);
        }
    }
    state.last_poll = Some(Instant::now());
    if let Err(error) = state.check_before_fetch() {
        state.local_error = Some(error.clone());
        state.preflight_blocked = true;
        state.set_paused(true);
        return Err(error);
    }
    let now = chrono::Utc::now().with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap());
    // 开关随轮询更新，09:50 前排队的任务也能在门槛到达后使用 MSN。
    state.msn_enabled = now.format("%Y%m%d").to_string().as_str() > trade_date
        || (now.format("%Y%m%d").to_string() == trade_date
            && now.format("%H%M").to_string().as_str() >= "0950");
    if state
        .local_loaded_at
        .is_none_or(|at| at.elapsed() >= Duration::from_secs(60))
    {
        let result = super::daily_baseline::load(source_path, trade_date, false);
        match result {
            Ok(baselines) => {
                state.local_averages = baselines
                    .rows
                    .iter()
                    .filter_map(|(code, row)| {
                        row.average_volume.map(|average| (code.clone(), average))
                    })
                    .collect();
                state.local_error = baselines.volume_error.clone();
            }
            Err(error) => {
                state.local_averages.clear();
                state.local_error = Some(error);
            }
        }
        state.local_loaded_at = Some(Instant::now());
    }
    let mut pending = 0;
    let mut failed = 0;
    let mut skipped = 0;
    let mut values = HashMap::new();
    for (ts_code, quote) in quotes {
        if quote.date.replace('-', "") != trade_date {
            continue;
        }
        // 新股等缺少五根有效日线时没有计算分母，换源或重试也无法补齐。
        // 日线基准更新后会自动重新纳入，无需向网络工作队列提交失败任务。
        let Some(local_average) = state.local_averages.get(ts_code).copied() else {
            skipped += 1;
            continue;
        };
        let retry = match state.entries.get(ts_code) {
            None => true,
            Some(Entry::Failed { at, .. }) => at.elapsed() >= Duration::from_secs(60),
            Some(Entry::Ready(baseline)) => {
                quote.time.as_str() > "09:35:00"
                    && !baseline.data.opening_window_complete()
                    && baseline.loaded_at.elapsed() >= Duration::from_secs(60)
            }
            _ => false,
        };
        if retry && !state.paused {
            let previous = match state.entries.remove(ts_code) {
                Some(Entry::Ready(baseline)) => Some(baseline),
                Some(Entry::Failed { baseline, .. }) => baseline,
                _ => None,
            };
            state
                .entries
                .insert(ts_code.clone(), Entry::Pending(previous.clone()));
            let cache = Arc::clone(cache);
            let ts_code = ts_code.clone();
            let date = trade_date.to_string();
            let http = http.clone();
            let generation = state.generation;
            let opening_window_ended = quote.time.as_str() > "09:35:00";
            // MSN 声明延迟 15 分钟；开盘 20 分钟后才可能覆盖完整五分钟。
            let msn_supported = ts_code.ends_with(".SH") || ts_code.ends_with(".SZ");
            // 两个实时源均按手规范化，仍兼容旧缓存及特殊行情的手/股识别。
            let live_volume_to_hands = if quote.vol > 0.0
                && quote.price > 0.0
                && quote.amount > 0.0
                && quote.amount / (quote.vol * quote.price) < 10.0
            {
                0.01
            } else {
                1.0
            };
            workers.spawn(move || {
                // 同一轮每个来源最多尝试一次；拒绝或失败立即交给其他来源。
                let mut attempted = 0u8;
                let (source, result) = 'fetch: loop {
                    let source = loop {
                        let (source, wait) = if let Ok(mut state) = cache.lock() {
                            if state.generation != generation || state.paused {
                                return;
                            }
                            if state
                                .last_poll
                                .is_none_or(|at| at.elapsed() >= Duration::from_secs(60))
                            {
                                state.entries.remove(&ts_code);
                                state.revision += 1;
                                return;
                            }
                            // 腾讯、东财、同花顺覆盖沪深北；其余来源按交易所开放。
                            let eligible = (1 << 0)
                                | (1 << 1)
                                | (1 << 3)
                                | if msn_supported { 1 << 4 } else { 0 }
                                | if state.msn_enabled && msn_supported {
                                    1 << 2
                                } else {
                                    0
                                }
                                | if ts_code.ends_with(".SH") { 1 << 5 } else { 0 }
                                | if ts_code.ends_with(".SZ") { 1 << 6 } else { 0 };
                            match state.reserve_source(Instant::now(), eligible, attempted) {
                                Some(reservation) => reservation,
                                None => break 'fetch (0, Err("各来源暂不可用".to_string())),
                            }
                        } else {
                            return;
                        };
                        if wait.is_zero() {
                            break source;
                        }
                        std::thread::sleep(wait.min(Duration::from_millis(100)));
                    };
                    let started = Instant::now();
                    let result =
                        if source == 0 {
                            fetch_tencent_volume_ratio(&http, &ts_code, local_average)
                        } else if source == 2 {
                            fetch_msn_volume_ratio(&http, &ts_code, local_average)
                        } else if source >= 3 {
                            let provider = match source {
                                3 => IntradayVolumeSource::Tonghuashun,
                                4 => IntradayVolumeSource::Xueqiu,
                                5 => IntradayVolumeSource::Sse,
                                6 => IntradayVolumeSource::Szse,
                                _ => unreachable!(),
                            };
                            volume_ratio_session_fetch(VOLUME_RATIO_SOURCES[source]).and_then(
                                |fetch| match fetch {
                                    Some(fetch) => {
                                        let raw = fetch(&ts_code)?;
                                        let data = serde_json::from_str(&raw)
                                            .map_err(|e| format!("浏览器分时 JSON 无效: {e}"))?;
                                        parse_volume_ratio_from_source(
                                            &data,
                                            provider,
                                            &ts_code,
                                            local_average,
                                        )
                                    }
                                    None => fetch_volume_ratio_from_source(
                                        &http,
                                        provider,
                                        &ts_code,
                                        local_average,
                                    ),
                                },
                            )
                        } else {
                            fetch_eastmoney_intraday_text(&http, &ts_code).and_then(|raw| {
                            lianghua_provider::crawler::volume_ratio::parse_eastmoney_volume_ratio(
                                &raw, &ts_code, local_average,
                            )
                        })
                        }
                        .and_then(|mut data| {
                            if data.trade_date != date {
                                return Err(format!(
                                    "分时日期 {} 与行情日期 {date} 不一致",
                                    data.trade_date
                                ));
                            }
                            if opening_window_ended && !data.opening_window_complete() {
                                return Err("分时未完整覆盖 09:31–09:35 开盘窗口".to_string());
                            }
                            // 各源曲线都已换算为量比；实时量单位独立于曲线源。
                            data.volume_to_hands = live_volume_to_hands;
                            Ok(data)
                        });
                    match result {
                        Ok(data) => {
                            if let Ok(mut state) = cache.lock() {
                                if state.generation != generation || state.paused {
                                    return;
                                }
                                state.handle_fetch_success(source, started, Instant::now());
                            } else {
                                return;
                            }
                            break (source, Ok(data));
                        }
                        Err(error) => {
                            if let Ok(mut state) = cache.lock() {
                                if state.generation != generation || state.paused {
                                    return;
                                }
                                state.handle_fetch_error(source, &error, Instant::now());
                            } else {
                                return;
                            }
                            attempted |= 1 << source;
                        }
                    }
                };
                if let Ok(mut state) = cache.lock() {
                    if state.generation != generation {
                        return;
                    }
                    if result.is_ok() {
                        state.source_results[source] = Some(true);
                    }
                    // 请求在途期间仍会更新旧基准的高点，提交时保留最新观测。
                    let previous = match state.entries.remove(&ts_code) {
                        Some(Entry::Pending(current)) => current,
                        _ => previous,
                    };
                    state.dirty.insert(ts_code.clone());
                    state.entries.insert(
                        ts_code,
                        match result {
                            Ok(data) => Entry::Ready(Baseline {
                                data,
                                source,
                                observed_high: previous
                                    .as_ref()
                                    .map_or(0.0, |baseline| baseline.observed_high),
                                observed_opening_high: previous
                                    .as_ref()
                                    .map_or(0.0, |baseline| baseline.observed_opening_high),
                                next_point: 0,
                                loaded_at: Instant::now(),
                            }),
                            Err(_) => Entry::Failed {
                                at: Instant::now(),
                                baseline: previous,
                            },
                        },
                    );
                    state.revision += 1;
                    if let Err(error) = state.persist(false) {
                        state.persistence_error = Some(error);
                    }
                }
            });
        }
        if let Some(entry) = state.entries.get_mut(ts_code) {
            let ready = matches!(entry, Entry::Ready(_));
            match entry {
                Entry::Ready(_) => {}
                Entry::Pending(_) => pending += 1,
                Entry::Failed { .. } => failed += 1,
            }
            let previous_opening_high = match &*entry {
                Entry::Ready(baseline)
                | Entry::Pending(Some(baseline))
                | Entry::Failed {
                    baseline: Some(baseline),
                    ..
                } => Some(baseline.observed_opening_high),
                _ => None,
            };
            if let Some(value) = entry.apply_quote(quote) {
                if ready && value.opening_high.is_none() {
                    pending += 1;
                }
                if previous_opening_high.is_some_and(|opening| match &*entry {
                    Entry::Ready(baseline)
                    | Entry::Pending(Some(baseline))
                    | Entry::Failed {
                        baseline: Some(baseline),
                        ..
                    } => baseline.observed_opening_high > opening,
                    _ => false,
                }) {
                    state.dirty.insert(ts_code.clone());
                }
                values.insert(ts_code.clone(), value);
            } else if ready {
                failed += 1;
            }
        }
    }
    if let Err(error) = state.persist(false) {
        state.persistence_error = Some(error);
    }
    let status = Some(format!(
        "{}补齐 {} · 待补 {pending} · 重试 {failed} · 本地不足跳过 {skipped}",
        if state.paused { "已暂停 · " } else { "" },
        values
            .values()
            .filter(|value| value.opening_high.is_some())
            .count(),
    ));
    let now = Instant::now();
    let mut source_statuses: Vec<_> = VOLUME_RATIO_SOURCES
        .into_iter()
        .enumerate()
        .map(|(index, source)| VolumeRatioSourceStatus {
            source,
            state: if state.paused {
                "paused"
            } else if index == 2 && !state.msn_enabled {
                "not_ready"
            } else if state.cooldown_until[index].is_some_and(|until| until > now) {
                "cooling_down"
            } else {
                match state.source_results[index] {
                    Some(true) => "available",
                    Some(false) => "retrying",
                    None => "waiting",
                }
            },
        })
        .collect();
    source_statuses.push(VolumeRatioSourceStatus {
        source: "local",
        state: if state.preflight_blocked {
            "blocked"
        } else if state.local_error.is_some() {
            "data_unavailable"
        } else if state.persistence_error.is_some()
            || state.store.as_ref().and_then(store::Store::error).is_some()
        {
            "cache_error"
        } else if quotes.iter().any(|(code, quote)| {
            quote.date.replace('-', "") == trade_date && !state.local_averages.contains_key(code)
        }) {
            "incomplete"
        } else {
            "available"
        },
    });
    Ok(VolumeRatioSnapshot {
        values,
        revision: state.revision,
        status,
        source_statuses,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lianghua_provider::crawler::volume_ratio::TencentVolumeRatioPoint;

    #[test]
    fn legacy_bincode_baseline_remains_compatible() {
        let baseline = Baseline {
            data: TencentVolumeRatioData {
                trade_date: "20261028".into(),
                average_daily_volume: 100.0,
                volume_to_hands: 1.0,
                points: vec![TencentVolumeRatioPoint {
                    time: "0930".into(),
                    trading_minutes: 1.0,
                    volume_ratio: 2.0,
                }],
            },
            source: 2,
            observed_high: 0.0,
            observed_opening_high: 3.0,
            next_point: 0,
            loaded_at: Instant::now(),
        };
        // bincode 1 的 serialize 使用小端固定宽度整数；手写旧格式校验双向兼容。
        let mut old_bytes = Vec::new();
        old_bytes.extend_from_slice(&8_u64.to_le_bytes());
        old_bytes.extend_from_slice(b"20261028");
        old_bytes.extend_from_slice(&100_f64.to_le_bytes());
        old_bytes.extend_from_slice(&1_f64.to_le_bytes());
        old_bytes.extend_from_slice(&1_u64.to_le_bytes());
        old_bytes.extend_from_slice(&4_u64.to_le_bytes());
        old_bytes.extend_from_slice(b"0930");
        old_bytes.extend_from_slice(&1_f64.to_le_bytes());
        old_bytes.extend_from_slice(&2_f64.to_le_bytes());
        old_bytes.extend_from_slice(&2_u64.to_le_bytes());
        old_bytes.extend_from_slice(&3_f64.to_le_bytes());
        assert_eq!(
            bincode::serde::encode_to_vec(
                (
                    &baseline.data,
                    baseline.source,
                    baseline.observed_opening_high
                ),
                bincode::config::legacy(),
            )
            .unwrap(),
            old_bytes
        );
        let ((data, source, observed_opening_high), consumed): (
            (TencentVolumeRatioData, usize, f64),
            _,
        ) = bincode::serde::decode_from_slice(&old_bytes, bincode::config::legacy()).unwrap();
        assert_eq!(consumed, old_bytes.len());
        assert_eq!(data.trade_date, "20261028");
        assert_eq!(data.points[0].time, "0930");
        assert_eq!(source, 2);
        assert_eq!(observed_opening_high, 3.0);
    }

    #[test]
    fn preflight_checks_previous_session_once_and_rejects_old_baselines() {
        let directory = std::env::temp_dir().join(format!(
            "volume-ratio-preflight-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let source = directory.to_str().unwrap();
        std::fs::write(
            directory.join("trade_calendar.csv"),
            "cal_date\n20261012\n20261013\n20261014\n20261015\n20261016\n20261019\n",
        )
        .unwrap();
        std::fs::write(directory.join("stock_list.csv"), "ts_code,symbol,name,area,industry,list_date,trade_date\n000001.SZ,000001,A,,,19910101,20261015\n").unwrap();
        let conn = Connection::open(source_db_path(source)).unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, vol DOUBLE, close DOUBLE);
            INSERT INTO stock_data SELECT '000001.SZ', '202610' || i::VARCHAR, 'qfq', 100, 10 FROM range(12,16) t(i);").unwrap();
        drop(conn);
        super::super::daily_baseline::load(source, "20261019", false).unwrap();
        let mut cache = Cache {
            source_path: source.into(),
            date: "20261019".into(),
            ..Default::default()
        };
        assert!(
            cache
                .check_before_fetch()
                .unwrap_err()
                .contains("日线更新至 20261016")
        );
        let conn = Connection::open(source_db_path(source)).unwrap();
        conn.execute_batch("INSERT INTO stock_data VALUES ('000001.SZ','20261016','qfq',600,10)")
            .unwrap();
        drop(conn);
        assert!(
            cache
                .check_before_fetch()
                .unwrap_err()
                .contains("股票列表更新至 20261016")
        );
        std::fs::write(directory.join("stock_list.csv"), "ts_code,symbol,name,area,industry,list_date,trade_date\n000001.SZ,000001,A,,,19910101,20261016\n").unwrap();
        cache.check_before_fetch().unwrap();
        assert_eq!(cache.local_averages["000001.SZ"], 200.0);
        std::fs::remove_file(directory.join("trade_calendar.csv")).unwrap();
        cache.check_before_fetch().unwrap(); // 同一轮通过后不再读取文件。
        cache.set_paused(true);
        cache.set_paused(false);
        assert!(cache.check_before_fetch().is_err());
        std::fs::write(
            directory.join("trade_calendar.csv"),
            "cal_date\n20261016\n20261019\n",
        )
        .unwrap();
        cache.entries.insert(
            "000001.SZ".into(),
            Entry::Ready(Baseline {
                data: TencentVolumeRatioData {
                    trade_date: "20261019".into(),
                    average_daily_volume: 100.0,
                    volume_to_hands: 1.0,
                    points: Vec::new(),
                },
                source: 0,
                observed_high: 0.0,
                observed_opening_high: 0.0,
                next_point: 0,
                loaded_at: Instant::now(),
            }),
        );
        assert!(cache.check_before_fetch().unwrap_err().contains("清空缓存"));
        drop(cache);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn cache_management_counts_wal_data_cancels_tasks_and_skips_missing_local_averages() {
        let directory = std::env::temp_dir().join(format!(
            "volume-ratio-manage-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let source = directory.to_str().unwrap();
        assert!(!inspect_cache(source).unwrap().exists);
        let generation = {
            let mut state = CACHE.get().unwrap().lock().unwrap();
            *state = Cache {
                source_path: source.into(),
                date: "20261019".into(),
                ..Default::default()
            };
            state.restore().unwrap();
            state.entries.insert(
                "000001.SZ".into(),
                Entry::Pending(Some(Baseline {
                    data: TencentVolumeRatioData {
                        trade_date: "20261019".into(),
                        average_daily_volume: 100.0,
                        volume_to_hands: 1.0,
                        points: Vec::new(),
                    },
                    source: 0,
                    observed_high: 0.0,
                    observed_opening_high: 0.0,
                    next_point: 0,
                    loaded_at: Instant::now(),
                })),
            );
            state.dirty.insert("000001.SZ".into());
            state.persist(false).unwrap();
            state.generation
        };
        let status = inspect_cache(source).unwrap();
        assert_eq!(status.stock_count, 1);
        assert_eq!(status.trade_dates, ["20261019"]);
        assert!(status.size_bytes > 0);
        clear_cache(source).unwrap();
        {
            let state = CACHE.get().unwrap().lock().unwrap();
            assert!(state.paused && state.entries.is_empty() && state.dirty.is_empty());
            assert!(state.store.is_none() && state.generation > generation);
        }
        assert!(!inspect_cache(source).unwrap().exists);
        for name in [
            "intraday_volume_ratio.sqlite3",
            "intraday_volume_ratio.sqlite3-wal",
            "intraday_volume_ratio.sqlite3-shm",
        ] {
            std::fs::write(directory.join(name), "broken").unwrap();
        }
        clear_cache(source).unwrap();
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
        std::fs::remove_dir(directory).unwrap();

        let cache = CACHE.get_or_init(|| Arc::new(Mutex::new(Cache::default())));
        *cache.lock().unwrap() = Cache {
            source_path: "volume-ratio-skip-test".into(),
            date: "20261028".into(),
            preflight_checked: true,
            local_loaded_at: Some(Instant::now()),
            local_averages: HashMap::from([("000001.SZ".into(), 2400.0)]),
            entries: HashMap::from([(
                "000001.SZ".into(),
                Entry::Ready(Baseline {
                    data: TencentVolumeRatioData {
                        trade_date: "20261028".into(),
                        average_daily_volume: 2400.0,
                        volume_to_hands: 1.0,
                        points: ["0930", "0931", "0932", "0933", "0934", "0935"]
                            .into_iter()
                            .enumerate()
                            .map(|(index, time)| TencentVolumeRatioPoint {
                                time: time.into(),
                                trading_minutes: (index as f64).max(1.0),
                                volume_ratio: 2.0,
                            })
                            .collect(),
                    },
                    source: 0,
                    observed_high: 2.0,
                    observed_opening_high: 2.0,
                    next_point: 0,
                    loaded_at: Instant::now(),
                }),
            )]),
            ..Default::default()
        };
        let quotes = ["000001.SZ", "001999.SZ"]
            .into_iter()
            .map(|code| {
                (
                    code.into(),
                    SinaQuote {
                        ts_code: code.into(),
                        name: String::new(),
                        date: "20261028".into(),
                        time: "09:43:00".into(),
                        vol: 100.0,
                        price: 10.0,
                        open: 10.0,
                        high: 10.0,
                        low: 10.0,
                        pre_close: 10.0,
                        amount: 100000.0,
                        change_pct: None,
                    },
                )
            })
            .collect();
        for _ in 0..2 {
            let result = snapshot("volume-ratio-skip-test", &quotes, "20261028").unwrap();
            assert!(result.values["000001.SZ"].opening_high.is_some());
            assert!(!result.values.contains_key("001999.SZ"));
            let status = result.status.unwrap();
            assert!(status.contains("补齐 1 · 待补 0 · 重试 0"));
            assert!(status.contains("本地不足跳过 1"));
            let state = cache.lock().unwrap();
            assert!(!state.entries.contains_key("001999.SZ"));
            assert!(!state.paused);
        }
        *cache.lock().unwrap() = Cache::default();
    }

    #[test]
    fn persisted_baselines_restore_opening_high_and_isolate_trade_dates() {
        let directory = std::env::temp_dir().join(format!(
            "volume-ratio-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let source_path = directory.to_string_lossy().into_owned();
        let mut cache = Cache {
            source_path: source_path.clone(),
            date: "20261013".into(),
            ..Default::default()
        };
        cache.restore().unwrap();
        let baseline = Baseline {
            data: TencentVolumeRatioData {
                trade_date: cache.date.clone(),
                average_daily_volume: 2400.0,
                volume_to_hands: 1.0,
                points: ["0930", "0931", "0932", "0933", "0934", "0935"]
                    .into_iter()
                    .enumerate()
                    .map(|(index, time)| TencentVolumeRatioPoint {
                        time: time.into(),
                        trading_minutes: (index as f64).max(1.0),
                        volume_ratio: 2.0,
                    })
                    .collect(),
            },
            source: 1,
            observed_high: 7.0,
            observed_opening_high: 3.0,
            next_point: 6,
            loaded_at: Instant::now(),
        };
        cache
            .entries
            .insert("000001.SZ".into(), Entry::Pending(Some(baseline)));
        cache.dirty.insert("000001.SZ".into());
        cache.persist(false).unwrap();
        drop(cache);
        let mut restored = Cache {
            source_path: source_path.clone(),
            date: "20261013".into(),
            ..Default::default()
        };
        restored.restore().unwrap();
        let Entry::Ready(baseline) = restored.entries.get("000001.SZ").unwrap() else {
            panic!("未恢复基准")
        };
        assert!(baseline.data.opening_window_complete());
        assert_eq!(
            (baseline.observed_high, baseline.observed_opening_high),
            (0.0, 3.0)
        );
        assert_eq!(baseline.next_point, 0);
        assert_eq!(baseline.source, 1);
        restored
            .entries
            .get_mut("000001.SZ")
            .unwrap()
            .apply_quote(&SinaQuote {
                time: "09:36:00".into(),
                vol: 100.0,
                date: "20261013".into(),
                ts_code: "000001.SZ".into(),
                name: String::new(),
                open: 0.0,
                high: 0.0,
                low: 0.0,
                pre_close: 0.0,
                price: 0.0,
                amount: 0.0,
                change_pct: None,
            })
            .unwrap();
        let Entry::Ready(baseline) = restored.entries.get("000001.SZ").unwrap() else {
            unreachable!()
        };
        assert_eq!(baseline.observed_high, 2.0);
        rusqlite::Connection::open(Path::new(&source_path).join("intraday_volume_ratio.sqlite3"))
            .unwrap()
            .execute(
                "INSERT INTO baselines_v2 VALUES ('broken', '20261013', X'FF')",
                [],
            )
            .unwrap();
        drop(restored);
        let mut restored = Cache {
            source_path: source_path.clone(),
            date: "20261013".into(),
            ..Default::default()
        };
        restored.restore().unwrap();
        assert_eq!(restored.entries.len(), 1);
        assert!(restored.persistence_error.is_some());
        drop(restored);
        let mut next_day = Cache {
            source_path,
            date: "20261014".into(),
            ..Default::default()
        };
        next_day.restore().unwrap();
        assert!(next_day.entries.is_empty());
        drop(next_day);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn browser_sessions_are_independent_and_eastmoney_remains_compatible() {
        set_volume_ratio_session_fetch("xueqiu", Some(Arc::new(|code| Ok(format!("xq:{code}")))))
            .unwrap();
        set_volume_ratio_session_fetch(
            "tonghuashun",
            Some(Arc::new(|code| Ok(format!("ths:{code}")))),
        )
        .unwrap();
        set_eastmoney_browser_fetch(Some(Arc::new(|code| Ok(format!("em:{code}"))))).unwrap();
        let http = reqwest::blocking::Client::new();
        assert_eq!(
            fetch_eastmoney_intraday_text(&http, "000001.SZ").unwrap(),
            "em:000001.SZ"
        );
        assert_eq!(
            volume_ratio_session_fetch("xueqiu").unwrap().unwrap()("600000.SH").unwrap(),
            "xq:600000.SH"
        );
        set_volume_ratio_session_fetch("xueqiu", None).unwrap();
        assert!(volume_ratio_session_fetch("xueqiu").unwrap().is_none());
        assert_eq!(
            volume_ratio_session_fetch("tonghuashun").unwrap().unwrap()("600000.SH").unwrap(),
            "ths:600000.SH"
        );
        set_volume_ratio_session_fetch("tonghuashun", None).unwrap();
        set_eastmoney_browser_fetch(None).unwrap();
        assert!(volume_ratio_session_fetch("unknown").is_err());
    }

    #[test]
    fn new_sources_rotate_with_independent_cooldown_and_retry_exclusion() {
        let now = Instant::now();
        let mut cache = Cache::default();
        // 沪市可选腾讯、东财、同花顺、雪球、上交所，不开放深交所及未到时刻的 MSN。
        let eligible = 0b0111011;
        for source in [0, 1, 3, 4, 5] {
            assert_eq!(
                cache.reserve_source(now, eligible, 0),
                Some((source, Duration::ZERO))
            );
        }
        assert_eq!(cache.next_request_at[2], None);
        assert_eq!(cache.next_request_at[6], None);
        cache.handle_fetch_error(3, "同花顺 HTTP状态=403;", now);
        cache.handle_fetch_error(5, "上交所 HTTP状态=429;", now);
        assert_eq!(cache.cooldown_until[3], Some(now + Duration::from_secs(20)));
        assert_eq!(cache.cooldown_until[5], Some(now + Duration::from_secs(20)));
        assert_eq!(cache.cooldown_until[4], None);
        assert_eq!(
            cache.reserve_source(now + Duration::from_millis(60), eligible, 0b0001011),
            Some((4, Duration::ZERO))
        );
        assert_eq!(cache.reserve_source(now, eligible, eligible), None);
        assert_eq!(
            cache.reserve_source(now, 1 << 6, 0),
            Some((6, Duration::ZERO))
        );
    }

    #[test]
    fn ready_sources_rotate_and_backoff_recovers_only_after_stable_successes() {
        let now = Instant::now();
        let mut cache = Cache::default();
        // 旧截止时间均已到期：按轮转公平选择，不能永远偏向最旧时间戳。
        cache.next_request_at = [Some(now - Duration::from_secs(1)); VOLUME_RATIO_SOURCES.len()];
        cache.source_cursor = 2;
        assert_eq!(
            cache.reserve_source(now, 0b111, 0),
            Some((2, Duration::ZERO))
        );
        assert_eq!(
            cache.reserve_source(now, 0b111, 0),
            Some((0, Duration::ZERO))
        );
        assert_eq!(
            cache.reserve_source(now, 0b111, 0),
            Some((1, Duration::ZERO))
        );
        assert_eq!(
            cache.reserve_source(now, 0b111, 0),
            Some((2, Duration::from_millis(60)))
        );
        // 等待不会预占请求额度；再次调用仍是同一截止时间。
        assert_eq!(
            cache.next_request_at[2],
            Some(now + Duration::from_millis(60))
        );
        cache.throttle_source(2, now);
        for _ in 0..8 {
            cache.handle_fetch_success(2, now, now + Duration::from_secs(41));
        }
        assert_eq!(cache.source_successes[2], 0);
        assert_eq!(cache.request_interval_ms[2], 120);
        for _ in 0..8 {
            cache.handle_fetch_success(
                2,
                now + Duration::from_secs(21),
                now + Duration::from_secs(22),
            );
        }
        assert_eq!(cache.request_interval_ms[2], 120);
        cache.handle_fetch_error(2, "响应缺竞价", now + Duration::from_secs(30));
        for _ in 0..8 {
            cache.handle_fetch_success(
                2,
                now + Duration::from_secs(41),
                now + Duration::from_secs(41),
            );
        }
        assert_eq!(cache.request_interval_ms[2], 120);
        cache.handle_fetch_success(
            2,
            now + Duration::from_secs(50),
            now + Duration::from_secs(50),
        );
        assert_eq!(cache.request_interval_ms[2], 60);
        assert_eq!(cache.request_interval_ms[0], 0);
        assert_eq!(cache.request_interval_ms[1], 0);
    }

    #[test]
    fn background_store_retries_failed_batches_and_preserves_latest_value_on_close() {
        let directory = std::env::temp_dir().join(format!(
            "volume-ratio-writer-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("cache.sqlite3");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;
            CREATE TABLE baselines_v2 (code TEXT PRIMARY KEY, trade_date TEXT, baseline BLOB);
            CREATE TRIGGER fail_write BEFORE INSERT ON baselines_v2 BEGIN SELECT RAISE(ABORT, 'blocked'); END;").unwrap();
        let store = store::Store::new(conn, "20261019".into()).unwrap();
        let data = TencentVolumeRatioData {
            trade_date: "20261019".into(),
            average_daily_volume: 100.0,
            volume_to_hands: 1.0,
            points: Vec::new(),
        };
        store
            .save("000001.SZ".into(), (data.clone(), 2, 1.0), false)
            .unwrap();
        assert!(store.flush().unwrap_err().contains("blocked"));
        assert!(store.error().is_some());
        let reader = rusqlite::Connection::open(&path).unwrap();
        reader.execute_batch("DROP TRIGGER fail_write").unwrap();
        store.flush().unwrap();
        assert!(store.error().is_none());
        assert_eq!(
            reader
                .query_row("SELECT COUNT(*) FROM baselines_v2", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        store
            .save("000001.SZ".into(), (data, 2, 3.0), false)
            .unwrap();
        drop(store); // 不等 250ms，关闭仍须提交队列并关闭连接。
        let bytes: Vec<u8> = reader
            .query_row("SELECT baseline FROM baselines_v2", [], |r| r.get(0))
            .unwrap();
        let ((_, source, high), _) = bincode::serde::decode_from_slice::<
            (TencentVolumeRatioData, usize, f64),
            _,
        >(&bytes, bincode::config::legacy())
        .unwrap();
        assert_eq!((source, high), (2, 3.0));
        drop(reader);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn sources_have_independent_rate_limits_and_cooldowns() {
        let mut cache = Cache::default();
        let now = Instant::now();
        assert_eq!(
            cache.reserve_source(now, 0b011, 0).unwrap(),
            (0, Duration::ZERO)
        );
        assert_eq!(
            cache.reserve_source(now, 0b011, 0).unwrap(),
            (1, Duration::ZERO)
        );
        assert_eq!(
            cache.reserve_source(now, 0b011, 0).unwrap(),
            (0, Duration::from_millis(60))
        );
        cache.throttle_source(0, now);
        assert_eq!(
            cache
                .reserve_source(now + Duration::from_millis(60), 0b011, 0)
                .unwrap(),
            (1, Duration::ZERO)
        );
        assert_eq!(cache.request_interval_ms[0], 120);
        assert_eq!(cache.request_interval_ms[1], 0);
        cache.throttle_source(1, now);
        assert_eq!(cache.request_interval_ms[1], 120);
        assert!(cache.reserve_source(now, 0b011, 0).unwrap().1 >= Duration::from_secs(20));
        for status in [429, 500, 501, 502, 503, 504] {
            let mut cache = Cache::default();
            cache.handle_fetch_error(0, &format!("腾讯分时 HTTP 错误; HTTP状态={status};"), now);
            assert_eq!(cache.cooldown_until[0], Some(now + Duration::from_secs(20)));
            assert_eq!(cache.request_interval_ms[0], 120);
            assert_eq!(
                cache.reserve_source(now, 0b011, 0).unwrap(),
                (1, Duration::ZERO)
            );
            cache.handle_fetch_error(0, "请求腾讯分时失败（连接失败）", now);
            assert_eq!(cache.cooldown_until[0], Some(now + Duration::from_secs(20)));
        }
        let mut cache = Cache::default();
        cache.handle_fetch_error(1, "请求东财分时失败（请求超时）", now);
        assert_eq!(cache.cooldown_until[1], None);
        cache.handle_fetch_error(1, "读取东财分时失败: UnexpectedEof", now);
        assert_eq!(cache.cooldown_until[1], None);
        assert_eq!(cache.request_interval_ms[1], 0);
        assert_eq!(
            cache.reserve_source(now, 0b011, 0).unwrap(),
            (0, Duration::ZERO)
        );
    }

    #[test]
    fn msn_participates_only_when_enabled_and_has_its_own_cooldown() {
        let now = Instant::now();
        let mut cache = Cache::default();
        cache.reserve_source(now, 0b011, 0).unwrap();
        cache.reserve_source(now, 0b011, 0).unwrap();
        assert_eq!(
            cache.reserve_source(now, 0b011, 0).unwrap(),
            (0, Duration::from_millis(60))
        );
        assert_eq!(cache.next_request_at[2], None);
        assert_eq!(
            cache.reserve_source(now, 0b111, 0).unwrap(),
            (2, Duration::ZERO)
        );
        assert_eq!(
            cache.next_request_at[2],
            Some(now + Duration::from_millis(60))
        );
        cache.handle_fetch_error(2, "MSN HTTP状态=429;", now);
        assert_eq!(cache.request_interval_ms[2], 120);
        assert_eq!(cache.cooldown_until[2], Some(now + Duration::from_secs(20)));
        assert_eq!(
            cache.reserve_source(now, 0b111, 0).unwrap(),
            (0, Duration::from_millis(60))
        );
    }

    #[test]
    fn rejected_sources_are_skipped_until_all_alternatives_are_exhausted() {
        let now = Instant::now();
        let mut cache = Cache::default();
        // MSN 已拒绝：即使它比另外两源更早可用，也不能再选。
        cache.next_request_at[0] = Some(now + Duration::from_millis(100));
        cache.next_request_at[1] = Some(now + Duration::from_millis(200));
        assert_eq!(
            cache.reserve_source(now, 0b111, 0b100),
            Some((0, Duration::from_millis(100)))
        );
        assert_eq!(
            cache.reserve_source(now, 0b111, 0b101),
            Some((1, Duration::from_millis(200)))
        );
        assert_eq!(cache.reserve_source(now, 0b111, 0b111), None);
        assert_eq!(cache.reserve_source(now, 0b011, 0b011), None);
    }

    #[test]
    fn pausing_discards_queued_jobs_and_preserves_completed_baselines() {
        let mut cache = Cache::default();
        let baseline = Baseline {
            data: TencentVolumeRatioData {
                trade_date: "20261009".to_string(),
                average_daily_volume: 300.0,
                volume_to_hands: 1.0,
                points: Vec::new(),
            },
            source: 0,
            observed_high: 2.0,
            observed_opening_high: 1.5,
            next_point: 0,
            loaded_at: Instant::now(),
        };
        cache
            .entries
            .insert("queued".to_string(), Entry::Pending(None));
        cache.entries.insert(
            "refreshing".to_string(),
            Entry::Pending(Some(baseline.clone())),
        );
        cache
            .entries
            .insert("ready".to_string(), Entry::Ready(baseline));
        cache.set_paused(true);
        assert!(cache.paused);
        assert_eq!(cache.generation, 1);
        assert!(!cache.entries.contains_key("queued"));
        for code in ["ready", "refreshing"] {
            let Entry::Ready(baseline) = &cache.entries[code] else {
                panic!("lost completed baseline")
            };
            assert_eq!(baseline.observed_high, 2.0);
            assert_eq!(baseline.observed_opening_high, 1.5);
        }
        cache.set_paused(true);
        assert_eq!(cache.generation, 1);
        cache.set_paused(false);
        assert!(!cache.paused);
        assert_eq!(cache.generation, 2);
        assert_eq!(cache.entries.len(), 2);
    }

    #[test]
    fn local_baseline_uses_five_traded_bars_and_keeps_hand_units() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, vol DOUBLE);
            INSERT INTO stock_data VALUES
                ('920002.BJ','20260924','qfq',100),('920002.BJ','20260928','qfq',200),
                ('920002.BJ','20260929','qfq',300),('920002.BJ','20260930','qfq',400),
                ('920002.BJ','20261008','qfq',500),('920002.BJ','20261009','qfq',99999),
                ('920002.BJ','20261008','raw',99999),('920003.BJ','20260924','qfq',100);
            INSERT INTO stock_data SELECT code,trade_date,adj_type,vol FROM stock_data
                CROSS JOIN (VALUES ('000001.SZ'),('600000.SH'),('688185.SH')) AS codes(code)
                WHERE ts_code='920002.BJ';").unwrap();
        let values = load_local_volume_averages(&conn, "20261009", None).unwrap();
        assert_eq!(values.len(), 4);
        for code in ["920002.BJ", "000001.SZ", "600000.SH", "688185.SH"] {
            assert_eq!(values[code], 300.0);
        }
        let selected = load_local_volume_averages(&conn, "20261009", Some("688185.SH")).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected["688185.SH"], 300.0);
        conn.execute_batch(
            "INSERT INTO stock_data VALUES
            ('paused','20260801','qfq',100), ('paused','20260803','qfq',200),
            ('paused','20260901','qfq',300), ('paused','20260915','qfq',400),
            ('paused','20261008','qfq',500), ('paused','20261007','qfq',0)",
        )
        .unwrap();
        let paused = load_local_volume_averages(&conn, "20261009", Some("paused")).unwrap();
        assert_eq!(paused["paused"], 300.0);
    }

    #[test]
    fn quote_uses_full_day_baseline_without_future_points_or_post_market_volume() {
        let mut baseline = Baseline {
            data: TencentVolumeRatioData {
                trade_date: "20261009".to_string(),
                average_daily_volume: 2400.0,
                volume_to_hands: 1.0,
                points: vec![
                    TencentVolumeRatioPoint {
                        time: "0930".to_string(),
                        trading_minutes: 1.0,
                        volume_ratio: 5.0,
                    },
                    TencentVolumeRatioPoint {
                        time: "1130".to_string(),
                        trading_minutes: 120.0,
                        volume_ratio: 1.0,
                    },
                    TencentVolumeRatioPoint {
                        time: "1400".to_string(),
                        trading_minutes: 180.0,
                        volume_ratio: 8.0,
                    },
                    TencentVolumeRatioPoint {
                        time: "1500".to_string(),
                        trading_minutes: 240.0,
                        volume_ratio: 2.0,
                    },
                ],
            },
            source: 0,
            observed_high: 0.0,
            observed_opening_high: 0.0,
            next_point: 0,
            loaded_at: Instant::now(),
        };
        baseline.data.points.splice(
            1..1,
            (1..=5).map(|minute| TencentVolumeRatioPoint {
                time: format!("093{minute}"),
                trading_minutes: minute as f64,
                volume_ratio: 1.0,
            }),
        );
        let mut quote = SinaQuote {
            date: "20261009".to_string(),
            time: "10:00:00".to_string(),
            ts_code: "000001.SZ".to_string(),
            name: String::new(),
            open: 10.0,
            high: 10.0,
            low: 10.0,
            pre_close: 10.0,
            price: 10.0,
            vol: 300.0,
            amount: 300000.0,
            change_pct: Some(0.0),
        };
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 1.0,
                high: 5.0,
                opening_high: Some(5.0)
            })
        );
        baseline.data.volume_to_hands = 0.01;
        quote.vol *= 100.0;
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 1.0,
                high: 5.0,
                opening_high: Some(5.0)
            })
        );
        baseline.data.volume_to_hands = 1.0;
        quote.vol /= 100.0;
        quote.time = "12:00:00".to_string();
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 1.0,
                high: 5.0,
                opening_high: Some(5.0)
            })
        );
        quote.time = "13:00:00".to_string();
        quote.vol = 7200.0;
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 6.0,
                high: 6.0,
                opening_high: Some(5.0)
            })
        );
        quote.time = "15:30:00".to_string();
        quote.vol = 999999.0;
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 2.0,
                high: 8.0,
                opening_high: Some(5.0)
            })
        );
        let expected = baseline.apply_quote(&quote);
        let mut failed = Entry::Failed {
            at: Instant::now(),
            baseline: Some(baseline),
        };
        assert_eq!(failed.apply_quote(&quote), expected);
        let Entry::Failed { baseline, .. } = failed else {
            unreachable!()
        };
        let mut pending = Entry::Pending(baseline);
        assert_eq!(pending.apply_quote(&quote), expected);
        assert_eq!(Entry::Pending(None).apply_quote(&quote), None);
    }

    #[test]
    fn quote_opening_baseline_waits_for_complete_five_minutes_and_keeps_live_peak() {
        let mut baseline = Baseline {
            data: TencentVolumeRatioData {
                trade_date: "20261009".to_string(),
                average_daily_volume: 2400.0,
                volume_to_hands: 1.0,
                points: vec![
                    TencentVolumeRatioPoint {
                        time: "0930".to_string(),
                        trading_minutes: 1.0,
                        volume_ratio: 2.0,
                    },
                    TencentVolumeRatioPoint {
                        time: "0934".to_string(),
                        trading_minutes: 4.0,
                        volume_ratio: 3.0,
                    },
                ],
            },
            source: 0,
            observed_high: 0.0,
            observed_opening_high: 0.0,
            next_point: 0,
            loaded_at: Instant::now(),
        };
        let mut quote = SinaQuote {
            date: "20261009".to_string(),
            time: "09:34:30".to_string(),
            ts_code: "000001.SZ".to_string(),
            name: String::new(),
            open: 10.0,
            high: 10.0,
            low: 10.0,
            pre_close: 10.0,
            price: 10.0,
            vol: 180.0,
            amount: 180000.0,
            change_pct: Some(0.0),
        };
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 4.0,
                high: 4.0,
                opening_high: None,
            })
        );
        quote.time = "09:36:00".to_string();
        quote.vol = 300.0;
        assert_eq!(baseline.apply_quote(&quote).unwrap().opening_high, None);
        baseline.data.points.push(TencentVolumeRatioPoint {
            time: "0935".to_string(),
            trading_minutes: 5.0,
            volume_ratio: 3.5,
        });
        quote.time = "09:35:00".to_string();
        quote.vol = 200.0;
        assert_eq!(baseline.apply_quote(&quote).unwrap().opening_high, None);
        quote.time = "09:36:00".to_string();
        quote.vol = 300.0;
        // 已有 09:35 末点但中间缺分钟，仍不能确认开盘高点。
        assert_eq!(baseline.apply_quote(&quote).unwrap().opening_high, None);
        baseline.data.points.splice(
            1..1,
            (1..=3).map(|minute| TencentVolumeRatioPoint {
                time: format!("093{minute}"),
                trading_minutes: minute as f64,
                volume_ratio: 1.0,
            }),
        );
        baseline.next_point = 0;
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 5.0,
                high: 5.0,
                opening_high: Some(4.0),
            })
        );
        quote.time = "09:37:00".to_string();
        assert_eq!(
            baseline.apply_quote(&quote).unwrap().opening_high,
            Some(4.0)
        );
    }
}
