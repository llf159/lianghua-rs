use std::collections::{HashMap, HashSet};

use duckdb::{AccessMode, Config, Connection};

use super::{cyq_chen_db_path, load_stock_list, result_db_path, source_db_path, stock_list_path};

pub struct PendingStockRepair {
    pub ts_code: String,
    pub start_date: String,
    pub end_date: String,
}

pub fn load_pending_stock_repairs(conn: &Connection) -> Result<Vec<PendingStockRepair>, String> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'pending_stock_repair')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("检查行情待补救记录失败: {e}"))?;
    if !exists {
        return Ok(Vec::new());
    }
    let mut stmt = conn
        .prepare("SELECT ts_code, start_date, end_date FROM pending_stock_repair ORDER BY ts_code")
        .map_err(|e| format!("准备行情待补救查询失败: {e}"))?;
    stmt.query_map([], |row| {
        Ok(PendingStockRepair {
            ts_code: row.get(0)?,
            start_date: row.get(1)?,
            end_date: row.get(2)?,
        })
    })
    .map_err(|e| format!("查询行情待补救记录失败: {e}"))?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| format!("读取行情待补救记录失败: {e}"))
}

pub fn load_unfinished_stock_repairs(
    source_dir: &str,
    conn: &Connection,
) -> Result<Vec<PendingStockRepair>, String> {
    let mut pending = load_pending_stock_repairs(conn)?;
    let mut stmt = conn
        .prepare(
            "SELECT ts_code, MIN(trade_date) FROM stock_data WHERE adj_type='qfq' GROUP BY ts_code",
        )
        .map_err(|e| format!("准备股票历史范围检查失败: {e}"))?;
    let mut legacy = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|e| format!("检查股票历史范围失败: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取股票历史范围失败: {e}"))?;
    let legacy_exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='pending_chip_repair')", [], |row| row.get(0)).map_err(|e| e.to_string())?;
    if legacy_exists {
        let mut stmt = conn
            .prepare("SELECT ts_code FROM pending_chip_repair")
            .map_err(|e| e.to_string())?;
        let codes = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        for code in codes {
            if !legacy.iter().any(|(saved, _)| saved == &code) {
                legacy.push((code, None));
            }
        }
    }
    if legacy.is_empty() {
        return Ok(pending);
    }
    let (start_date, end_date): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|e| format!("读取旧补救日期范围失败: {e}"))?;
    let (Some(start_date), Some(end_date)) = (start_date, end_date) else {
        return Ok(pending);
    };
    let mut historical_dates = HashMap::<String, String>::new();
    for (path, table) in [
        (result_db_path(source_dir), "score_summary"),
        (cyq_chen_db_path(source_dir), "cyq_chen_snapshot"),
        (super::cyq_db_path(source_dir), "cyq_snapshot"),
    ] {
        if !path.exists() {
            continue;
        }
        let config = Config::default()
            .access_mode(AccessMode::ReadOnly)
            .map_err(|e| format!("配置旧补救检查只读连接失败: {e}"))?;
        let derived = Connection::open_with_flags(&path, config)
            .map_err(|e| format!("只读检查旧补救历史失败, path={}: {e}", path.display()))?;
        let exists: bool = derived
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ?)",
                [table],
                |row| row.get(0),
            )
            .map_err(|e| format!("检查旧补救历史表失败: {e}"))?;
        if !exists {
            continue;
        }
        let mut stmt = derived
            .prepare(&format!(
                "SELECT ts_code, MIN(trade_date) FROM {table} GROUP BY ts_code"
            ))
            .map_err(|e| format!("准备历史证据查询失败: {e}"))?;
        let dates = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| format!("查询历史证据失败: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("读取历史证据失败: {e}"))?;
        for (code, date) in dates {
            historical_dates
                .entry(code)
                .and_modify(|first| {
                    if date < *first {
                        *first = date.clone();
                    }
                })
                .or_insert(date);
        }
    }
    for (code, first_date) in legacy {
        if historical_dates
            .get(&code)
            .is_some_and(|historical| first_date.as_ref().is_none_or(|first| first > historical))
            && !pending.iter().any(|item| item.ts_code == code)
        {
            pending.push(PendingStockRepair {
                ts_code: code.clone(),
                start_date: historical_dates
                    .get(&code)
                    .map(|history| history.clone().min(start_date.clone()))
                    .unwrap_or_else(|| start_date.clone()),
                end_date: end_date.clone(),
            });
        }
    }
    pending.sort_by(|a, b| a.ts_code.cmp(&b.ts_code));
    Ok(pending)
}

pub fn load_missing_stock_codes(
    source_dir: &str,
    conn: &Connection,
) -> Result<Vec<String>, String> {
    if !stock_list_path(source_dir).exists() {
        return Ok(Vec::new());
    }
    let mut stmt = conn
        .prepare("SELECT ts_code, MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq' GROUP BY ts_code")
        .map_err(|e| format!("准备行情覆盖检查失败: {e}"))?;
    let existing = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| format!("查询行情覆盖失败: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取行情覆盖失败: {e}"))?;
    let latest_date = existing.iter().map(|(_, date)| date.as_str()).max();
    let codes = existing
        .iter()
        .map(|(code, _)| code.as_str())
        .collect::<HashSet<_>>();
    let mut missing = load_stock_list(source_dir)?
        .into_iter()
        .filter(|row| {
            row.get(5).is_none_or(|date| {
                date.trim().is_empty() || latest_date.is_none_or(|latest| date.as_str() <= latest)
            })
        })
        .filter_map(|row| row.first().map(|code| code.trim().to_string()))
        .filter(|code| !code.is_empty() && !codes.contains(code.as_str()))
        .collect::<Vec<_>>();
    missing.sort();
    missing.dedup();
    Ok(missing)
}

pub fn open_source_db_readonly(source_dir: &str) -> Result<Connection, String> {
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(|e| format!("配置行情检查只读连接失败: {e}"))?;
    Connection::open_with_flags(source_db_path(source_dir), config)
        .map_err(|e| format!("打开行情库检查下载状态失败: {e}"))
}

pub fn load_listing_history_gaps(
    source_dir: &str,
    conn: &Connection,
) -> Result<Vec<PendingStockRepair>, String> {
    let mut stmt = conn.prepare("SELECT ts_code, MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type='qfq' GROUP BY ts_code")
        .map_err(|e| format!("读取上市历史覆盖失败: {e}"))?;
    let ranges = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, String>(1)?, row.get::<_, String>(2)?),
            ))
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(|e| e.to_string())?;
    let history_start = ranges
        .values()
        .map(|(first, _)| first)
        .min()
        .cloned()
        .unwrap_or_default();
    let mut gaps = Vec::new();
    for stock in load_stock_list(source_dir)? {
        let (Some(code), Some(listed)) = (stock.first(), stock.get(5)) else {
            continue;
        };
        if chrono::NaiveDate::parse_from_str(listed, "%Y%m%d").is_err() {
            continue;
        }
        let expected = listed.max(&history_start);
        if let Some((first, last)) = ranges.get(code) {
            if first > expected {
                gaps.push(PendingStockRepair {
                    ts_code: code.clone(),
                    start_date: expected.clone(),
                    end_date: last.clone(),
                });
            }
        }
    }
    gaps.sort_by(|a, b| a.ts_code.cmp(&b.ts_code));
    Ok(gaps)
}

pub fn ensure_stock_data_ready_for_derived(source_dir: &str) -> Result<(), String> {
    if !source_db_path(source_dir).exists() {
        return Err("原始行情库不存在，禁止计算后续数据库，请先下载行情。".to_string());
    }
    let conn = open_source_db_readonly(source_dir)?;
    let pending = load_pending_stock_repairs(&conn)?;
    if !pending.is_empty() {
        return Err(format!(
            "{} 只股票的行情修复尚未完成，禁止后续计算，请先完成行情修复（{}）。",
            pending.len(),
            pending
                .iter()
                .take(12)
                .map(|item| item.ts_code.as_str())
                .collect::<Vec<_>>()
                .join("、")
        ));
    }
    let has_rows: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM stock_data WHERE adj_type='qfq')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("读取行情状态失败: {e}"))?;
    if !has_rows {
        return Err("原始行情库没有前复权数据，请先下载行情。".to_string());
    }
    // 起点偏晚可能来自停牌，pre_close 差异可能来自差异化分红，均不直接判为坏数据。

    Ok(())
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingDerivedRebuild {
    pub component: String,
    pub start_date: String,
    pub end_date: String,
}

pub fn load_pending_derived_rebuilds(
    conn: &Connection,
) -> Result<Vec<PendingDerivedRebuild>, String> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'pending_derived_rebuild')",
        [], |row| row.get(0),
    ).map_err(|e| format!("检查后续库待重算记录失败: {e}"))?;
    if !exists {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare("SELECT component, start_date, end_date FROM pending_derived_rebuild ORDER BY component")
        .map_err(|e| format!("读取后续库待重算记录失败: {e}"))?;
    stmt.query_map([], |row| {
        Ok(PendingDerivedRebuild {
            component: row.get(0)?,
            start_date: row.get(1)?,
            end_date: row.get(2)?,
        })
    })
    .map_err(|e| format!("查询后续库待重算记录失败: {e}"))?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| format!("读取后续库待重算记录失败: {e}"))
}

// 新增日期待计算不代表旧结果失效；依赖门禁只拦截与已保存结果重叠的变更。
pub fn pending_rebuild_affects_saved_results(
    source_dir: &str,
    component: &str,
) -> Result<bool, String> {
    let source = open_source_db_readonly(source_dir)?;
    let pending = load_pending_derived_rebuilds(&source)?;
    let Some(pending) = pending.iter().find(|item| item.component == component) else {
        return Ok(false);
    };
    let table = match component {
        "ranking" => "score_summary",
        "similarity" => "strategy_trigger_similarity_rank",
        _ => return Err(format!("不支持的结果依赖: {component}")),
    };
    let path = if component == "similarity" {
        super::similarity_db_path(source_dir)
    } else {
        result_db_path(source_dir)
    };
    if !path.exists() {
        return Ok(false);
    }
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(|e| format!("配置结果依赖只读连接失败: {e}"))?;
    let results =
        Connection::open_with_flags(path, config).map_err(|e| format!("检查结果依赖失败: {e}"))?;
    let exists: bool = results
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=?)",
            [table],
            |row| row.get(0),
        )
        .map_err(|e| format!("检查结果依赖表失败: {e}"))?;
    if !exists {
        return Ok(false);
    }
    results
        .query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE trade_date BETWEEN ? AND ?)"),
            [&pending.start_date, &pending.end_date],
            |row| row.get(0),
        )
        .map_err(|e| format!("检查已保存结果是否失效失败: {e}"))
}

pub fn mark_derived_rebuild_required(
    conn: &Connection,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS pending_derived_rebuild (component VARCHAR PRIMARY KEY, start_date VARCHAR NOT NULL, end_date VARCHAR NOT NULL)")
        .map_err(|e| format!("创建后续库待重算记录失败: {e}"))?;
    for component in ["concept", "ranking", "similarity"] {
        conn.execute("INSERT INTO pending_derived_rebuild VALUES (?, ?, ?) ON CONFLICT (component) DO UPDATE SET start_date = LEAST(pending_derived_rebuild.start_date, excluded.start_date), end_date = GREATEST(pending_derived_rebuild.end_date, excluded.end_date)",
            duckdb::params![component, start_date, end_date])
            .map_err(|e| format!("记录后续库待重算区间失败: {e}"))?;
    }
    Ok(())
}

pub fn clear_pending_derived_rebuild(
    source_dir: &str,
    component: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    let check = open_source_db_readonly(source_dir)?;
    let required = load_pending_derived_rebuilds(&check)?
        .into_iter()
        .any(|item| {
            item.component == component
                && start_date <= item.start_date.as_str()
                && end_date >= item.end_date.as_str()
        });
    drop(check);
    if !required {
        return Ok(());
    }
    let conn = Connection::open(source_db_path(source_dir))
        .map_err(|e| format!("清除待重算记录时打开原始库失败: {e}"))?;
    conn.execute("DELETE FROM pending_derived_rebuild WHERE component = ? AND start_date >= ? AND end_date <= ?",
        duckdb::params![component, start_date, end_date]).map_err(|e| format!("清除后续库待重算记录失败: {e}"))?;
    Ok(())
}

// 调用方须与行情写入共用事务，避免源数据与修复记账只提交一方。
pub fn mark_chip_repairs_required(
    conn: &Connection,
    codes: &HashSet<String>,
) -> Result<(), String> {
    if codes.is_empty() {
        return Ok(());
    }
    conn.execute_batch("CREATE TABLE IF NOT EXISTS pending_chip_repair(ts_code VARCHAR PRIMARY KEY); CREATE TABLE IF NOT EXISTS completed_chip_repair(ts_code VARCHAR, model VARCHAR, PRIMARY KEY(ts_code, model))")
        .map_err(|e| format!("创建筹码修复记录失败: {e}"))?;
    for code in codes {
        conn.execute(
            "INSERT OR REPLACE INTO pending_chip_repair VALUES (?)",
            [code],
        )
        .map_err(|e| format!("记录筹码待修复股票失败: {e}"))?;
        conn.execute("DELETE FROM completed_chip_repair WHERE ts_code=?", [code])
            .map_err(|e| format!("重置筹码修复状态失败: {e}"))?;
    }
    Ok(())
}

pub fn load_pending_chip_repairs_for_model(
    conn: &Connection,
    model: &str,
) -> Result<Vec<String>, String> {
    let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='pending_chip_repair')", [], |row| row.get(0)).map_err(|e| e.to_string())?;
    if !exists {
        return Ok(Vec::new());
    }
    let completed_exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='completed_chip_repair')", [], |row| row.get(0)).map_err(|e| e.to_string())?;
    let sql = if completed_exists {
        "SELECT ts_code FROM pending_chip_repair WHERE NOT EXISTS(SELECT 1 FROM completed_chip_repair done WHERE done.ts_code=pending_chip_repair.ts_code AND done.model=?) ORDER BY ts_code"
    } else {
        "SELECT ts_code FROM pending_chip_repair WHERE ? IS NOT NULL ORDER BY ts_code"
    };
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    stmt.query_map([model], |row| row.get(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())
}

pub fn clear_completed_chip_repairs(
    conn: &Connection,
    source_dir: &str,
    model: &str,
    codes: &[String],
) -> Result<(), String> {
    if codes.is_empty() {
        return Ok(());
    }
    let other = if model == "chen" { "legacy" } else { "chen" };
    let other_exists = if model == "chen" {
        super::cyq_db_path(source_dir).exists()
    } else {
        super::cyq_chen_db_path(source_dir).exists()
    };
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| e.to_string())?;
    let result = (|| -> Result<(), String> {
        conn.execute_batch("CREATE TABLE IF NOT EXISTS completed_chip_repair(ts_code VARCHAR, model VARCHAR, PRIMARY KEY(ts_code, model))").map_err(|e| e.to_string())?;
        for code in codes {
            conn.execute("INSERT OR REPLACE INTO completed_chip_repair SELECT ts_code, ? FROM pending_chip_repair WHERE ts_code=?", duckdb::params![model, code]).map_err(|e| e.to_string())?;
            conn.execute("DELETE FROM pending_chip_repair WHERE ts_code=? AND (? = FALSE OR EXISTS(SELECT 1 FROM completed_chip_repair done WHERE done.ts_code=? AND done.model=?))", duckdb::params![code, other_exists, code, other]).map_err(|e| e.to_string())?;
        }
        conn.execute_batch("COMMIT").map_err(|e| e.to_string())
    })();
    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    result
}

pub struct QfqPriceLinkIssue {
    pub ts_code: String,
    pub samples: Vec<(String, f64, f64)>,
}

pub fn load_qfq_price_link_issues(conn: &Connection) -> Result<Vec<QfqPriceLinkIssue>, String> {
    let has_prices: bool = conn.query_row("SELECT COUNT(*) > 0 FROM information_schema.columns WHERE table_name='stock_data' AND column_name='pre_close'", [], |row| row.get(0)).map_err(|e| e.to_string())?;
    if !has_prices {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare("WITH recent_dates AS (SELECT DISTINCT trade_date FROM stock_data WHERE adj_type='qfq' ORDER BY trade_date DESC LIMIT 61), recent AS (SELECT ts_code, trade_date, pre_close, LAG(close) OVER (PARTITION BY ts_code ORDER BY trade_date) AS previous_close FROM stock_data WHERE adj_type='qfq' AND trade_date >= (SELECT MIN(trade_date) FROM recent_dates)), mismatches AS (SELECT * FROM recent WHERE previous_close IS NOT NULL AND ABS(pre_close - previous_close) > GREATEST(0.011, ABS(previous_close) * 0.0001)) SELECT ts_code, trade_date, CAST(pre_close AS DOUBLE), CAST(previous_close AS DOUBLE) FROM mismatches QUALIFY ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date) <= 6 ORDER BY ts_code, trade_date").map_err(|e| format!("检查复权衔接失败: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
            ))
        })
        .map_err(|e| e.to_string())?;
    let mut issues: Vec<QfqPriceLinkIssue> = Vec::new();
    for row in rows {
        let (code, date, pre_close, previous_close) = row.map_err(|e| e.to_string())?;
        if issues.last().is_none_or(|issue| issue.ts_code != code) {
            issues.push(QfqPriceLinkIssue {
                ts_code: code,
                samples: Vec::new(),
            });
        }
        if let Some(issue) = issues.last_mut() {
            issue.samples.push((date, previous_close, pre_close));
        }
    }
    Ok(issues)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_chip_repair_detects_truncated_source_history() {
        let source_dir = std::env::temp_dir().join(format!(
            "lianghua-legacy-repair-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&source_dir).unwrap();
        let source = source_dir.to_str().unwrap();
        let conn = Connection::open(source_db_path(source)).unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, open DOUBLE DEFAULT 10, high DOUBLE DEFAULT 10, low DOUBLE DEFAULT 10, close DOUBLE DEFAULT 10, pre_close DOUBLE DEFAULT 10, vol DOUBLE DEFAULT 10, amount DOUBLE DEFAULT 100, change DOUBLE DEFAULT 0, pct_chg DOUBLE DEFAULT 0);
            INSERT INTO stock_data(ts_code,trade_date,adj_type) VALUES ('000001.SZ', '20100104', 'qfq'), ('000001.SZ', '20261009', 'qfq'), ('000400.SZ', '20240102', 'qfq'), ('000400.SZ', '20261009', 'qfq');
            CREATE TABLE pending_chip_repair(ts_code VARCHAR PRIMARY KEY); INSERT INTO pending_chip_repair VALUES ('000400.SZ')").unwrap();
        let scoring = Connection::open(result_db_path(source)).unwrap();
        scoring.execute_batch("CREATE TABLE score_summary(ts_code VARCHAR, trade_date VARCHAR); INSERT INTO score_summary VALUES ('000400.SZ', '20110104')").unwrap();
        drop(scoring);
        let pending = load_unfinished_stock_repairs(source, &conn).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].ts_code, "000400.SZ");
        assert_eq!(pending[0].start_date, "20100104");
        assert_eq!(pending[0].end_date, "20261009");
        drop(conn);
        std::fs::remove_dir_all(source_dir).unwrap();
    }

    #[test]
    fn missing_stock_list_ignores_future_listings() {
        let source_dir = std::env::temp_dir().join(format!(
            "lianghua-download-state-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&source_dir).unwrap();
        let source = source_dir.to_str().unwrap();
        std::fs::write(stock_list_path(source), "ts_code,symbol,name,area,industry,list_date\n000001.SZ,1,A,,,20200101\n000002.SZ,2,B,,,20200101\n000003.SZ,3,C,,,20260107\n").unwrap();
        let conn = Connection::open(source_db_path(source)).unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, open DOUBLE DEFAULT 10, high DOUBLE DEFAULT 10, low DOUBLE DEFAULT 10, close DOUBLE DEFAULT 10, pre_close DOUBLE DEFAULT 10, vol DOUBLE DEFAULT 10, amount DOUBLE DEFAULT 100, change DOUBLE DEFAULT 0, pct_chg DOUBLE DEFAULT 0); INSERT INTO stock_data(ts_code,trade_date,adj_type) VALUES ('000001.SZ', '20260106', 'qfq')").unwrap();
        assert_eq!(
            load_missing_stock_codes(source, &conn).unwrap(),
            vec!["000002.SZ"]
        );
        drop(conn);
        std::fs::remove_dir_all(source_dir).unwrap();
    }
}
