use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use duckdb::{Connection, params};
use lianghua_data::data::{load_trade_date_list, open_db_readonly, source_db_path};

#[derive(Debug, Clone, Copy)]
pub(crate) struct DailyBaseline {
    pub latest_close: Option<f64>,
    pub realtime_base_close: Option<f64>,
    pub daily_base_close: Option<f64>,
    pub average_volume: Option<f64>,
}

#[derive(Default)]
pub(crate) struct DailyBaselines {
    pub rows: HashMap<String, DailyBaseline>,
    pub volume_error: Option<String>,
}

struct CacheEntry {
    trade_date: String,
    loaded_at: Instant,
    data: Result<Arc<DailyBaselines>, String>,
}

pub(crate) fn load(source_path: &str, trade_date: &str) -> Result<Arc<DailyBaselines>, String> {
    static CACHE: OnceLock<Mutex<HashMap<String, CacheEntry>>> = OnceLock::new();
    let mut cache = CACHE
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| "监控日线基准缓存锁已损坏".to_string())?;
    if let Some(entry) = cache.get(source_path)
        && entry.trade_date == trade_date
        && entry.loaded_at.elapsed() < Duration::from_secs(60)
    {
        return entry.data.clone();
    }

    let data = (|| {
        // 日历缺失只影响量比，仍然读取五日涨幅需要的收盘价。
        let (mut dates, mut volume_error) = match load_trade_date_list(source_path) {
            Ok(dates) => (dates, None),
            Err(error) => (Vec::new(), Some(error)),
        };
        dates.retain(|date| date.as_str() < trade_date);
        dates.sort_unstable_by(|a, b| b.cmp(a));
        dates.dedup();
        dates.truncate(5);
        if dates.len() != 5 && volume_error.is_none() {
            volume_error = Some("本地交易日历不足五日".to_string());
        }
        let conn = open_db_readonly(source_db_path(source_path))
            .map_err(|e| format!("打开监控日线基准库失败: {e}"))?;
        Ok(Arc::new(DailyBaselines {
            rows: query(&conn, &dates)?,
            volume_error,
        }))
    })();
    cache.insert(
        source_path.to_string(),
        CacheEntry {
            trade_date: trade_date.to_string(),
            loaded_at: Instant::now(),
            data: data.clone(),
        },
    );
    data
}

fn query(conn: &Connection, dates: &[String]) -> Result<HashMap<String, DailyBaseline>, String> {
    let dates: [Option<&str>; 5] =
        std::array::from_fn(|index| dates.get(index).map(String::as_str));
    let mut stmt = conn
        .prepare(
            r#"
            WITH daily AS (
                SELECT ts_code, trade_date, TRY_CAST(close AS DOUBLE) AS close_value,
                    TRY_CAST(vol AS DOUBLE) AS volume,
                    trade_date IN (?, ?, ?, ?, ?) AS volume_date
                FROM stock_data
                WHERE adj_type = 'qfq'
            ), inputs AS (
                SELECT *,
                    CASE WHEN volume_date THEN trade_date END AS baseline_date,
                    CASE WHEN volume_date THEN volume END AS baseline_volume
                FROM daily
            ), baselines AS (
                SELECT ts_code,
                    -- 包装为非空 struct，空收盘价也占据一根日线的位置。
                    ARG_MAX(STRUCT_PACK(value := close_value), trade_date, 6) AS closes,
                    CASE WHEN COUNT(baseline_date) = 5 AND COUNT(DISTINCT baseline_date) = 5
                        AND COUNT(baseline_volume) = 5 AND MIN(baseline_volume) >= 0
                        AND ISFINITE(AVG(baseline_volume)) AND AVG(baseline_volume) > 0
                        THEN AVG(baseline_volume) END AS average_volume
                FROM inputs
                GROUP BY ts_code
            )
            SELECT ts_code,
                closes[1].value, closes[5].value, closes[6].value, average_volume
            FROM baselines
            "#,
        )
        .map_err(|e| format!("预编译监控日线基准失败: {e}"))?;
    let mut rows = stmt
        .query(params![dates[0], dates[1], dates[2], dates[3], dates[4]])
        .map_err(|e| format!("查询监控日线基准失败: {e}"))?;
    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取监控日线基准失败: {e}"))?
    {
        let ts_code = row.get(0).map_err(|e| format!("读取基准代码失败: {e}"))?;
        out.insert(
            ts_code,
            DailyBaseline {
                latest_close: row.get(1).map_err(|e| format!("读取最新收盘失败: {e}"))?,
                realtime_base_close: row
                    .get(2)
                    .map_err(|e| format!("读取实时涨幅基准失败: {e}"))?,
                daily_base_close: row
                    .get(3)
                    .map_err(|e| format!("读取日线涨幅基准失败: {e}"))?,
                average_volume: row.get(4).map_err(|e| format!("读取五日均量失败: {e}"))?,
            },
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combined_baselines_keep_close_positions_and_exact_volume_dates() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR,
                close VARCHAR, vol VARCHAR);
            INSERT INTO stock_data
                SELECT 'normal', '2026100' || i::VARCHAR, 'qfq', i::VARCHAR, (i*100)::VARCHAR
                FROM range(1,9) t(i) ORDER BY i % 3;
            INSERT INTO stock_data
                SELECT code, trade_date, adj_type, close, vol FROM stock_data
                CROSS JOIN (VALUES ('missing'), ('duplicate'), ('invalid'), ('null_close')) codes(code)
                WHERE ts_code='normal';
            DELETE FROM stock_data WHERE ts_code='missing' AND trade_date='20261005';
            INSERT INTO stock_data SELECT * FROM stock_data
                WHERE ts_code='duplicate' AND trade_date='20261005';
            UPDATE stock_data SET vol='invalid' WHERE ts_code='invalid' AND trade_date='20261005';
            UPDATE stock_data SET close=NULL WHERE ts_code='null_close' AND trade_date='20261008';
            INSERT INTO stock_data VALUES ('normal','20261009','raw','99999','99999'),
                ('short','20261008','qfq','8','800');",
        ).unwrap();
        let dates = (3..=7).map(|i| format!("2026100{i}")).collect::<Vec<_>>();
        let baselines = query(&conn, &dates).unwrap();
        assert_eq!(baselines["normal"].latest_close, Some(8.0));
        assert_eq!(baselines["normal"].realtime_base_close, Some(4.0));
        assert_eq!(baselines["normal"].daily_base_close, Some(3.0));
        assert_eq!(baselines["normal"].average_volume, Some(500.0));
        for code in ["missing", "duplicate", "invalid", "short"] {
            assert_eq!(baselines[code].average_volume, None, "{code}");
        }
        assert_eq!(baselines["null_close"].latest_close, None);
        assert_eq!(baselines["null_close"].realtime_base_close, Some(4.0));
        assert_eq!(baselines["null_close"].daily_base_close, Some(3.0));
        assert_eq!(baselines["short"].latest_close, Some(8.0));
        assert_eq!(baselines["short"].realtime_base_close, None);
        assert_eq!(baselines["short"].daily_base_close, None);
        // 日历不足时仍保留涨幅基准，不能用其他历史日期凑满五日均量。
        let baselines = query(&conn, &dates[..4]).unwrap();
        assert_eq!(baselines["normal"].average_volume, None);
        assert_eq!(baselines["normal"].realtime_base_close, Some(4.0));
    }
}
