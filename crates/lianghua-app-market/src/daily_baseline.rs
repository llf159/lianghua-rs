use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use duckdb::{Connection, params};
use lianghua_data::data::{open_db_readonly, source_db_path};

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
        let conn = open_db_readonly(source_db_path(source_path))
            .map_err(|e| format!("打开监控日线基准库失败: {e}"))?;
        Ok(Arc::new(DailyBaselines {
            rows: query(&conn, trade_date)?,
            volume_error: None,
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

fn query(conn: &Connection, trade_date: &str) -> Result<HashMap<String, DailyBaseline>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            WITH baselines AS (
                SELECT ts_code,
                    -- 空收盘价仍占日线位置；均量仅取行情日前最近五根有成交的日线。
                    ARG_MAX(STRUCT_PACK(value := TRY_CAST(close AS DOUBLE)), trade_date, 6) AS closes,
                    ARG_MAX(STRUCT_PACK(day := trade_date, volume := TRY_CAST(vol AS DOUBLE)), trade_date, 5)
                        FILTER (WHERE trade_date < ? AND TRY_CAST(vol AS DOUBLE) > 0
                            AND ISFINITE(TRY_CAST(vol AS DOUBLE))) AS volumes
                FROM stock_data
                WHERE adj_type = 'qfq'
                GROUP BY ts_code
            )
            SELECT ts_code,
                closes[1].value, closes[5].value, closes[6].value,
                CASE WHEN LEN(volumes) = 5
                    AND LEN(LIST_DISTINCT(LIST_TRANSFORM(volumes, x -> x.day))) = 5
                    THEN LIST_AVG(LIST_TRANSFORM(volumes, x -> x.volume)) END AS average_volume
            FROM baselines
            "#,
        )
        .map_err(|e| format!("预编译监控日线基准失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date])
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
    fn combined_baselines_use_five_traded_bars_across_suspensions() {
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
        let baselines = query(&conn, "20261009").unwrap();
        assert_eq!(baselines["normal"].latest_close, Some(8.0));
        assert_eq!(baselines["normal"].realtime_base_close, Some(4.0));
        assert_eq!(baselines["normal"].daily_base_close, Some(3.0));
        assert_eq!(baselines["normal"].average_volume, Some(600.0));
        for code in ["duplicate", "short"] {
            assert_eq!(baselines[code].average_volume, None, "{code}");
        }
        assert_eq!(baselines["null_close"].latest_close, None);
        assert_eq!(baselines["null_close"].realtime_base_close, Some(4.0));
        assert_eq!(baselines["null_close"].daily_base_close, Some(3.0));
        assert_eq!(baselines["short"].latest_close, Some(8.0));
        assert_eq!(baselines["short"].realtime_base_close, None);
        assert_eq!(baselines["short"].daily_base_close, None);
        assert_eq!(baselines["missing"].average_volume, Some(560.0));
        assert_eq!(baselines["invalid"].average_volume, Some(560.0));
        let before_today = query(&conn, "20261008").unwrap();
        assert_eq!(before_today["normal"].average_volume, Some(500.0));

    }
}
