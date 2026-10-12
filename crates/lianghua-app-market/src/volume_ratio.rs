use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use duckdb::{Connection, params};
use lianghua_data::data::{source_db_path};
use lianghua_provider::crawler::{
    SinaQuote,
    volume_ratio::{TencentVolumeRatioData, fetch_eastmoney_volume_ratio, fetch_tencent_volume_ratio, intraday_trading_minutes},
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

#[derive(Clone)]
struct Baseline {
    data: TencentVolumeRatioData,
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
        // 分时覆盖到 09:35 后才确认开盘五分钟完整；09:35 的端点仍计入基准。
        let opening_high = (minutes > 5.0
            && self
                .data
                .points
                .last()
                .is_some_and(|point| point.trading_minutes >= 5.0))
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
            | Self::Failed { baseline: Some(baseline), .. } => baseline.apply_quote(quote),
            _ => None,
        }
    }
}

#[derive(Default)]
struct Cache {
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
    next_request_at: [Option<Instant>; 2],
    cooldown_until: [Option<Instant>; 2],
    request_interval_ms: [u64; 2],
}

impl Cache {
    fn reserve_source(&mut self, now: Instant) -> (usize, Duration) {
        let deadlines: [Instant; 2] = std::array::from_fn(|index| {
            self.next_request_at[index].into_iter()
                .chain(self.cooldown_until[index]).max().unwrap_or(now)
        });
        let source = if deadlines[0] <= deadlines[1] { 0 } else { 1 };
        let wait = deadlines[source].saturating_duration_since(now);
        if wait.is_zero() {
            let interval = self.request_interval_ms[source]
                .max(100);
            self.next_request_at[source] = Some(now + Duration::from_millis(interval));
        }
        (source, wait)
    }

    fn throttle_source(&mut self, source: usize, now: Instant) {
        self.cooldown_until[source] = Some(now + Duration::from_secs(60));
        self.request_interval_ms[source] = self.request_interval_ms[source]
            .max(100)
            .saturating_mul(2).min(2000);
    }

    fn handle_fetch_error(&mut self, source: usize, error: &str, now: Instant) {
        let status = error.split("HTTP状态=").nth(1)
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
static WORKERS: OnceLock<Result<(rayon::ThreadPool, reqwest::blocking::Client), String>> =
    OnceLock::new();

pub fn set_fetch_paused(paused: bool) -> Result<(), String> {
    CACHE.get_or_init(|| Arc::new(Mutex::new(Cache::default())))
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
    }
    state.last_poll = Some(Instant::now());
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
                    && baseline
                        .data
                        .points
                        .last()
                        .is_none_or(|point| point.trading_minutes < 5.0)
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
            state.entries.insert(
                ts_code.clone(),
                Entry::Pending(previous.clone()),
            );
            let cache = Arc::clone(cache);
            let ts_code = ts_code.clone();
            let date = trade_date.to_string();
            let http = http.clone();
            let generation = state.generation;
            let local_average = state.local_averages.get(&ts_code).copied();
            let local_error = state.local_error.clone();
            // 分时曲线单位已规范化；实时行情仍来自腾讯，须独立确认手/股单位。
            let live_volume_to_hands = if quote.vol > 0.0 && quote.price > 0.0
                && quote.amount > 0.0 && quote.amount / (quote.vol * quote.price) < 10.0
            { 0.01 } else { 1.0 };
            workers.spawn(move || {
                // 两个平台各自限速/冷却，工作线程自动选择先可用的平台。
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
                            state.reserve_source(Instant::now())
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
                    } else {
                        fetch_eastmoney_volume_ratio(&http, &ts_code, average).map(|mut data| {
                            data.volume_to_hands = live_volume_to_hands;
                            data
                        })
                    }
                } else {
                    Err(local_error.unwrap_or_else(|| {
                        format!("{ts_code} 本地日线不足 {date} 前五根有成交行情，请先更新日线")
                    }))
                }
                .and_then(|data| {
                    if data.trade_date != date {
                        return Err(format!(
                            "分时日期 {} 与行情日期 {date} 不一致",
                            data.trade_date
                        ));
                    }
                    Ok(data)
                });
                if let Ok(mut state) = cache.lock() {
                    if state.generation != generation {
                        return;
                    }
                    if let Err(error) = &result {
                        state.handle_fetch_error(source, error, Instant::now());
                    }
                    state.entries.insert(
                        ts_code,
                        match result {
                            Ok(data) => Entry::Ready(Baseline {
                                data,
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
            if let Some(value) = entry.apply_quote(quote) {
                if ready && value.opening_high.is_none() {
                    pending += 1;
                }
                values.insert(ts_code.clone(), value);
            } else if ready {
                failed += 1;
                first_error.get_or_insert_with(|| format!(
                    "{ts_code} 已有分时基准，但当前行情的时间或成交量无法计算量比"
                ));
            }
        }
    }
    let status = if state.paused {
        Some("分时基准拉取已手动暂停，已补齐数据继续使用；恢复后继续补齐。".to_string())
    } else if pending > 0 || failed > 0 {
        Some(format!(
            "开盘五分钟量比基准：已补齐 {} 只，等待开盘五分钟结束或补齐中 {pending} 只，失败 {failed} 只；基准未完整的股票暂不提示量比新高{}",
            values
                .values()
                .filter(|value| value.opening_high.is_some())
                .count(),
            first_error
                .map(|error| format!("。首条错误：{error}（60 秒后重试）"))
                .unwrap_or_default()
        ))
    } else {
        None
    };
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
    fn sources_have_independent_rate_limits_and_cooldowns() {
        let mut cache = Cache::default();
        let now = Instant::now();
        assert_eq!(cache.reserve_source(now), (0, Duration::ZERO));
        assert_eq!(cache.reserve_source(now), (1, Duration::ZERO));
        assert_eq!(cache.reserve_source(now), (0, Duration::from_millis(100)));
        cache.throttle_source(0, now);
        assert_eq!(cache.reserve_source(now + Duration::from_millis(100)), (1, Duration::ZERO));
        assert_eq!(cache.request_interval_ms[0], 200);
        assert_eq!(cache.request_interval_ms[1], 0);
        cache.throttle_source(1, now);
        assert_eq!(cache.request_interval_ms[1], 200);
        assert!(cache.reserve_source(now).1 >= Duration::from_secs(60));
        for status in [429, 500, 501, 502, 503, 504] {
            let mut cache = Cache::default();
            cache.handle_fetch_error(0, &format!("腾讯分时 HTTP 错误; HTTP状态={status};"), now);
            assert_eq!(cache.cooldown_until[0], Some(now + Duration::from_secs(60)));
            assert_eq!(cache.request_interval_ms[0], 200);
            assert_eq!(cache.reserve_source(now), (1, Duration::ZERO));
            cache.handle_fetch_error(0, "请求腾讯分时失败（连接失败）", now);
            assert_eq!(cache.cooldown_until[0], Some(now + Duration::from_secs(60)));
        }
        let mut cache = Cache::default();
        cache.handle_fetch_error(1, "请求东财分时失败（请求超时）", now);
        assert_eq!(cache.cooldown_until[1], None);
        cache.handle_fetch_error(1, "读取东财分时失败: UnexpectedEof", now);
        assert_eq!(cache.cooldown_until[1], None);
        assert_eq!(cache.request_interval_ms[1], 0);
        assert_eq!(cache.reserve_source(now), (0, Duration::ZERO));

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
            observed_high: 2.0,
            observed_opening_high: 1.5,
            next_point: 0,
            loaded_at: Instant::now(),
        };
        cache.entries.insert("queued".to_string(), Entry::Pending(None));
        cache.entries.insert("refreshing".to_string(), Entry::Pending(Some(baseline.clone())));
        cache.entries.insert("ready".to_string(), Entry::Ready(baseline));
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
        conn.execute_batch("INSERT INTO stock_data VALUES
            ('paused','20260801','qfq',100), ('paused','20260803','qfq',200),
            ('paused','20260901','qfq',300), ('paused','20260915','qfq',400),
            ('paused','20261008','qfq',500), ('paused','20261007','qfq',0)").unwrap();
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
            observed_high: 0.0,
            observed_opening_high: 0.0,
            next_point: 0,
            loaded_at: Instant::now(),
        };
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
        let Entry::Failed { baseline, .. } = failed else { unreachable!() };
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
