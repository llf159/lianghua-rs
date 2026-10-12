use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use duckdb::{Connection, params};
use lianghua_data::data::source_db_path;
use lianghua_provider::crawler::{
    SinaQuote,
    volume_ratio::{
        TencentVolumeRatioData, fetch_eastmoney_volume_ratio, fetch_msn_volume_ratio,
        fetch_tencent_volume_ratio, intraday_trading_minutes,
    },
};

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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Baseline {
    data: TencentVolumeRatioData,
    source: usize,
    #[serde(skip)]
    observed_high: f64,
    observed_opening_high: f64,
    #[serde(skip)]
    next_point: usize,
    #[serde(skip, default = "Instant::now")]
    loaded_at: Instant,
}

impl Baseline {
    fn apply_quote(&mut self, quote: &SinaQuote) -> Option<VolumeRatioValue> {
        let quote_minute = quote.time.replace(':', "");
        let quote_minute = quote_minute.get(..4)?;
        let (minutes, value) = if let Some(minutes) = intraday_trading_minutes(&quote.time) {
            if !quote.vol.is_finite() || quote.vol < 0.0 {
                return None;
            }
            (
                minutes,
                quote.vol * self.data.volume_to_hands
                    / (self.data.average_daily_volume * minutes / 240.0),
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
        error: String,
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
    store: Option<rusqlite::Connection>,
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
    msn_enabled: bool,
    next_request_at: [Option<Instant>; 3],
    cooldown_until: [Option<Instant>; 3],
    request_interval_ms: [u64; 3],
}

impl Cache {
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
                match bincode::deserialize::<Baseline>(&bytes) {
                    Ok(baseline) if baseline.data.trade_date == self.date => {
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
        self.store = Some(conn);
        Ok(())
    }

    fn persist(&mut self) -> Result<(), String> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        let conn = self.store.as_mut().ok_or("量比缓存未打开，数据尚未落盘")?;
        let transaction = conn
            .transaction()
            .map_err(|error| format!("写入量比缓存失败: {error}"))?;
        {
            let mut statement = transaction.prepare("INSERT INTO baselines_v2 (code, trade_date, baseline)
                VALUES (?1, ?2, ?3) ON CONFLICT(code) DO UPDATE SET trade_date=excluded.trade_date, baseline=excluded.baseline")
                .map_err(|error| error.to_string())?;
            for code in &self.dirty {
                let baseline = match self.entries.get(code) {
                    Some(Entry::Ready(baseline) | Entry::Pending(Some(baseline)))
                    | Some(Entry::Failed {
                        baseline: Some(baseline),
                        ..
                    }) => baseline,
                    _ => continue,
                };
                let bytes = bincode::serialize(baseline).map_err(|error| error.to_string())?;
                statement
                    .execute(rusqlite::params![code, self.date, bytes])
                    .map_err(|error| error.to_string())?;
            }
        }
        transaction.commit().map_err(|error| error.to_string())?;
        self.dirty.clear();
        Ok(())
    }

    fn reserve_source(
        &mut self,
        now: Instant,
        allow_msn: bool,
        attempted: u8,
    ) -> Option<(usize, Duration)> {
        let deadlines: [Instant; 3] = std::array::from_fn(|index| {
            self.next_request_at[index]
                .into_iter()
                .chain(self.cooldown_until[index])
                .max()
                .unwrap_or(now)
        });
        let source = (0..if allow_msn { 3 } else { 2 })
            .filter(|&index| attempted & (1 << index) == 0)
            .min_by_key(|&index| deadlines[index])?;
        let wait = deadlines[source].saturating_duration_since(now);
        if wait.is_zero() {
            let interval =
                self.request_interval_ms[source].max(if source == 2 { 500 } else { 100 });
            self.next_request_at[source] = Some(now + Duration::from_millis(interval));
        }
        Some((source, wait))
    }

    fn throttle_source(&mut self, source: usize, now: Instant) {
        self.cooldown_until[source] = Some(now + Duration::from_secs(60));
        self.request_interval_ms[source] = self.request_interval_ms[source]
            .max(if source == 2 { 500 } else { 100 })
            .saturating_mul(2)
            .min(2000);
    }

    fn handle_fetch_error(&mut self, source: usize, error: &str, now: Instant) {
        let status = error
            .split("HTTP状态=")
            .nth(1)
            .and_then(|value| value.get(..3))
            .and_then(|value| value.parse::<u16>().ok());
        if status.is_some_and(|status| status == 429 || (500..=599).contains(&status)) {
            self.throttle_source(source, now);
        }
        // 超时、断连与读取失败只重试该股；没有 HTTP 状态不能推断全源限流。
    }

    fn set_paused(&mut self, paused: bool) {
        if self.paused == paused {
            return;
        }
        self.paused = paused;
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

pub struct VolumeRatioSnapshot {
    pub values: HashMap<String, VolumeRatioValue>,
    pub revision: u64,
    pub status: Option<String>,
}

static CACHE: OnceLock<Arc<Mutex<Cache>>> = OnceLock::new();
type EastmoneyBrowserFetch = Arc<dyn Fn(&str) -> Result<String, String> + Send + Sync>;
static EASTMONEY_BROWSER_FETCH: Mutex<Option<EastmoneyBrowserFetch>> = Mutex::new(None);

pub fn set_eastmoney_browser_fetch(fetch: Option<EastmoneyBrowserFetch>) -> Result<(), String> {
    *EASTMONEY_BROWSER_FETCH
        .lock()
        .map_err(|_| "东财浏览器会话锁已损坏")? = fetch;
    if let Some(cache) = CACHE.get() {
        let mut state = cache.lock().map_err(|_| "量比缓存锁已损坏")?;
        state.cooldown_until[1] = None;
        state.next_request_at[1] = None;
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
    CACHE
        .get_or_init(|| Arc::new(Mutex::new(Cache::default())))
        .lock()
        .map_err(|_| "量比缓存锁已损坏".to_string())?
        .set_paused(paused);
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
        state.date = trade_date.to_string();
        state.source_path = source_path.to_string();
        state.entries.clear();
        state.revision += 1;
        state.generation += 1;
        state.local_loaded_at = None;
        state.persistence_error = None;
        if let Err(error) = state.restore() {
            state.persistence_error = Some(error);
        }
    }
    state.last_poll = Some(Instant::now());
    let now = chrono::Utc::now().with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap());
    // 开关随轮询更新，09:50 前排队的任务也能在门槛到达后使用 MSN。
    state.msn_enabled = now.format("%Y%m%d").to_string() == trade_date
        && now.format("%H%M").to_string().as_str() >= "0950";
    if state
        .local_loaded_at
        .is_none_or(|at| at.elapsed() >= Duration::from_secs(60))
    {
        let result = super::daily_baseline::load(source_path, trade_date);
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
    let mut first_error = None;
    let mut values = HashMap::new();
    for (ts_code, quote) in quotes {
        if quote.date.replace('-', "") != trade_date {
            continue;
        }
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
            let local_average = state.local_averages.get(&ts_code).copied();
            let local_error = state.local_error.clone();
            let opening_window_ended = quote.time.as_str() > "09:35:00";
            // MSN 声明延迟 15 分钟；开盘 20 分钟后才可能覆盖完整五分钟。
            let msn_supported = ts_code.ends_with(".SH") || ts_code.ends_with(".SZ");
            // 分时曲线单位已规范化；实时行情仍来自腾讯，须独立确认手/股单位。
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
                let mut errors = Vec::new();
                let (source, result) = 'fetch: loop {
                    let source = loop {
                        let (source, wait) = if let Ok(mut state) = cache.lock() {
                            if state.generation != generation || state.paused {
                                return;
                            }
                            if state.last_poll.is_none_or(|at| at.elapsed() >= Duration::from_secs(60)) {
                                state.entries.remove(&ts_code);
                                state.revision += 1;
                                return;
                            }
                            if local_average.is_none() {
                                (0, Duration::ZERO)
                            } else {
                                let allow_msn = state.msn_enabled && msn_supported;
                                match state.reserve_source(Instant::now(), allow_msn, attempted) {
                                    Some(reservation) => reservation,
                                    None => break 'fetch (0, Err(errors.join("；"))),
                                }
                            }
                        } else {
                            return;
                        };
                        if wait.is_zero() {
                            break source;
                        }
                        std::thread::sleep(wait.min(Duration::from_millis(100)));
                    };
                    let result = if let Some(average) = local_average {
                        if source == 0 {
                            fetch_tencent_volume_ratio(&http, &ts_code, average)
                        } else if source == 2 {
                            fetch_msn_volume_ratio(&http, &ts_code, average)
                        } else {
                            let browser_fetch = EASTMONEY_BROWSER_FETCH.lock()
                                .map_err(|_| "东财浏览器会话锁已损坏".to_string())
                                .map(|fetch| fetch.clone());
                            browser_fetch.and_then(|fetch| match fetch {
                                Some(fetch) => fetch(&ts_code).and_then(|raw| {
                                    lianghua_provider::crawler::volume_ratio::parse_eastmoney_volume_ratio(
                                        &raw, &ts_code, average,
                                    )
                                }),
                                None => fetch_eastmoney_volume_ratio(&http, &ts_code, average),
                            })
                        }
                    } else {
                        Err(local_error.clone().unwrap_or_else(|| {
                            format!("{ts_code} 本地日线不足 {date} 前五根有成交行情，请先更新日线")
                        }))
                    }
                    .and_then(|mut data| {
                        if data.trade_date != date {
                            return Err(format!(
                                "分时日期 {} 与行情日期 {date} 不一致",
                                data.trade_date
                            ));
                        }
                        if opening_window_ended && !data.opening_window_complete() {
                            return Err("分时未完整覆盖 09:30–09:35 开盘窗口".to_string());
                        }
                        // 三个源的曲线都已换算为量比；实时量单位独立于曲线源。
                        data.volume_to_hands = live_volume_to_hands;
                        Ok(data)
                    });
                    match result {
                        Ok(data) => break (source, Ok(data)),
                        Err(error) if local_average.is_none() => break (source, Err(error)),
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
                            errors.push(format!("{}: {error}", ["腾讯", "东财", "MSN"][source]));
                        }
                    }
                };
                if let Ok(mut state) = cache.lock() {
                    if state.generation != generation {
                        return;
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
                            Err(error) => Entry::Failed {
                                at: Instant::now(),
                                error,
                                baseline: previous,
                            },
                        },
                    );
                    state.revision += 1;
                    if let Err(error) = state.persist() {
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
                Entry::Failed { error, .. } => {
                    failed += 1;
                    first_error.get_or_insert(error.clone());
                }
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
                first_error.get_or_insert_with(|| {
                    format!("{ts_code} 已有分时基准，但当前行情的时间或成交量无法计算量比")
                });
            }
        }
    }
    if let Err(error) = state.persist() {
        state.persistence_error = Some(error);
    }
    let approximate = state
        .entries
        .values()
        .filter(|entry| match entry {
            Entry::Ready(baseline)
            | Entry::Pending(Some(baseline))
            | Entry::Failed {
                baseline: Some(baseline),
                ..
            } => baseline.source == 2,
            _ => false,
        })
        .count();
    let mut status = Some(format!(
        "{}已补齐 {} · 待补齐 {pending} · 失败 {failed}{}",
        if state.paused { "已暂停 · " } else { "" },
        values
            .values()
            .filter(|value| value.opening_high.is_some())
            .count(),
        first_error
            .map(|error| format!("。{error}（60 秒后重试）"))
            .unwrap_or_default()
    ));
    if approximate > 0 {
        let message = format!("MSN 近似基准 {approximate} 只。");
        status = Some(match status {
            Some(status) => format!("{status}。{message}"),
            None => message,
        });
    }
    if let Some(error) = &state.persistence_error {
        status = Some(format!("{}。落盘提示：{error}", status.unwrap_or_default()));
    }
    Ok(VolumeRatioSnapshot {
        values,
        revision: state.revision,
        status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lianghua_provider::crawler::volume_ratio::TencentVolumeRatioPoint;

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
        cache.persist().unwrap();
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
        restored
            .store
            .as_ref()
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
    fn sources_have_independent_rate_limits_and_cooldowns() {
        let mut cache = Cache::default();
        let now = Instant::now();
        assert_eq!(
            cache.reserve_source(now, false, 0).unwrap(),
            (0, Duration::ZERO)
        );
        assert_eq!(
            cache.reserve_source(now, false, 0).unwrap(),
            (1, Duration::ZERO)
        );
        assert_eq!(
            cache.reserve_source(now, false, 0).unwrap(),
            (0, Duration::from_millis(100))
        );
        cache.throttle_source(0, now);
        assert_eq!(
            cache
                .reserve_source(now + Duration::from_millis(100), false, 0)
                .unwrap(),
            (1, Duration::ZERO)
        );
        assert_eq!(cache.request_interval_ms[0], 200);
        assert_eq!(cache.request_interval_ms[1], 0);
        cache.throttle_source(1, now);
        assert_eq!(cache.request_interval_ms[1], 200);
        assert!(cache.reserve_source(now, false, 0).unwrap().1 >= Duration::from_secs(60));
        for status in [429, 500, 501, 502, 503, 504] {
            let mut cache = Cache::default();
            cache.handle_fetch_error(0, &format!("腾讯分时 HTTP 错误; HTTP状态={status};"), now);
            assert_eq!(cache.cooldown_until[0], Some(now + Duration::from_secs(60)));
            assert_eq!(cache.request_interval_ms[0], 200);
            assert_eq!(
                cache.reserve_source(now, false, 0).unwrap(),
                (1, Duration::ZERO)
            );
            cache.handle_fetch_error(0, "请求腾讯分时失败（连接失败）", now);
            assert_eq!(cache.cooldown_until[0], Some(now + Duration::from_secs(60)));
        }
        let mut cache = Cache::default();
        cache.handle_fetch_error(1, "请求东财分时失败（请求超时）", now);
        assert_eq!(cache.cooldown_until[1], None);
        cache.handle_fetch_error(1, "读取东财分时失败: UnexpectedEof", now);
        assert_eq!(cache.cooldown_until[1], None);
        assert_eq!(cache.request_interval_ms[1], 0);
        assert_eq!(
            cache.reserve_source(now, false, 0).unwrap(),
            (0, Duration::ZERO)
        );
    }

    #[test]
    fn msn_participates_only_when_enabled_and_has_its_own_cooldown() {
        let now = Instant::now();
        let mut cache = Cache::default();
        cache.reserve_source(now, false, 0).unwrap();
        cache.reserve_source(now, false, 0).unwrap();
        assert_eq!(
            cache.reserve_source(now, false, 0).unwrap(),
            (0, Duration::from_millis(100))
        );
        assert_eq!(cache.next_request_at[2], None);
        assert_eq!(
            cache.reserve_source(now, true, 0).unwrap(),
            (2, Duration::ZERO)
        );
        assert_eq!(
            cache.next_request_at[2],
            Some(now + Duration::from_millis(500))
        );
        cache.handle_fetch_error(2, "MSN HTTP状态=429;", now);
        assert_eq!(cache.request_interval_ms[2], 1000);
        assert_eq!(cache.cooldown_until[2], Some(now + Duration::from_secs(60)));
        assert_eq!(
            cache.reserve_source(now, true, 0).unwrap(),
            (0, Duration::from_millis(100))
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
            cache.reserve_source(now, true, 0b100),
            Some((0, Duration::from_millis(100)))
        );
        assert_eq!(
            cache.reserve_source(now, true, 0b101),
            Some((1, Duration::from_millis(200)))
        );
        assert_eq!(cache.reserve_source(now, true, 0b111), None);
        assert_eq!(cache.reserve_source(now, false, 0b011), None);
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
            error: "连接失败".to_string(),
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
