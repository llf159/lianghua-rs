use std::collections::{HashMap, HashSet};

use duckdb::{AccessMode, Config, Connection};

use super::{load_stock_list, result_db_path, source_db_path, stock_list_path};

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

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
    let mut stmt = conn.prepare("SELECT component, start_date, end_date FROM pending_derived_rebuild ORDER BY component, start_date")
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

// 下载请求可能含休市日或早于已有历史；只保留实际行情及旧结果涉及的日期。
pub fn load_pending_derived_rebuild_ranges(
    source_dir: &str,
    component: &str,
) -> Result<Vec<PendingDerivedRebuild>, String> {
    let source = open_source_db_readonly(source_dir)?;
    let pending = load_pending_derived_rebuilds(&source)?
        .into_iter()
        .filter(|item| item.component == component)
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    let (path, table) = match component {
        "concept" => (
            super::concept_performance_db_path(source_dir),
            "concept_performance",
        ),
        "ranking" => (result_db_path(source_dir), "score_summary"),
        "similarity" => (
            super::similarity_db_path(source_dir),
            "strategy_trigger_similarity_rank",
        ),
        _ => return Err(format!("不支持的待重算组件: {component}")),
    };
    let results = if path.exists() {
        let conn = super::open_db_readonly(path.to_str().ok_or("结果库路径不是有效 UTF-8")?)
            .map_err(|e| format!("打开待重算结果库失败: {e}"))?;
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=?)",
                [table],
                |row| row.get(0),
            )
            .map_err(|e| format!("检查待重算结果表失败: {e}"))?;
        exists.then_some(conn)
    } else {
        None
    };
    let mut ranges = Vec::new();
    for mut item in pending {
        let (mut first, mut last): (Option<String>, Option<String>) = source.query_row(
            "SELECT MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type='qfq' AND trade_date BETWEEN ? AND ?",
            [&item.start_date, &item.end_date], |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(|e| format!("读取待重算行情日期失败: {e}"))?;
        if let Some(results) = &results {
            let (saved_first, saved_last): (Option<String>, Option<String>) = results.query_row(
                &format!("SELECT MIN(trade_date), MAX(trade_date) FROM {table} WHERE trade_date BETWEEN ? AND ?"),
                [&item.start_date, &item.end_date], |row| Ok((row.get(0)?, row.get(1)?)),
            ).map_err(|e| format!("读取待重算结果日期失败: {e}"))?;
            first = first.into_iter().chain(saved_first).min();
            last = last.into_iter().chain(saved_last).max();
        }
        if let (Some(first), Some(last)) = (first, last) {
            item.start_date = first;
            item.end_date = last;
            ranges.push(item);
        }
    }
    Ok(ranges)
}

// 新增日期待计算不代表旧结果失效；依赖门禁只拦截与已保存结果重叠的变更。
pub fn pending_rebuild_affects_saved_results(
    source_dir: &str,
    component: &str,
) -> Result<bool, String> {
    pending_rebuild_affects_saved_results_in_range(source_dir, component, "00000000", "99999999")
}

pub fn pending_rebuild_affects_saved_results_in_range(
    source_dir: &str,
    component: &str,
    start_date: &str,
    end_date: &str,
) -> Result<bool, String> {
    let source = open_source_db_readonly(source_dir)?;
    let pending = load_pending_derived_rebuilds(&source)?;
    let pending = pending
        .iter()
        .filter(|item| {
            item.component == component
                && item.start_date.as_str() <= end_date
                && item.end_date.as_str() >= start_date
        })
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return Ok(false);
    }
    let table = match component {
        "concept" => "concept_performance",
        "ranking" => "score_summary",
        "similarity" => "strategy_trigger_similarity_rank",
        _ => return Err(format!("不支持的结果依赖: {component}")),
    };
    let path = match component {
        "concept" => super::concept_performance_db_path(source_dir),
        "similarity" => super::similarity_db_path(source_dir),
        _ => result_db_path(source_dir),
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
    for pending in pending {
        let affects_saved: bool = results
            .query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE trade_date BETWEEN ? AND ?)"),
                [
                    pending.start_date.as_str().max(start_date),
                    pending.end_date.as_str().min(end_date),
                ],
                |row| row.get(0),
            )
            .map_err(|e| format!("检查已保存结果是否失效失败: {e}"))?;
        if affects_saved {
            return Ok(true);
        }
    }
    Ok(false)
}

// 调用方提供事务；旧版单区间主键升级后可以记住分段重算的完成情况。
fn ensure_derived_rebuild_table(conn: &Connection) -> Result<(), String> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS pending_derived_rebuild (component VARCHAR NOT NULL, start_date VARCHAR NOT NULL, end_date VARCHAR NOT NULL, PRIMARY KEY(component, start_date))")
        .map_err(|e| format!("创建后续库待重算记录失败: {e}"))?;
    let legacy: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM duckdb_constraints() WHERE table_name='pending_derived_rebuild' AND constraint_type='PRIMARY KEY' AND len(constraint_column_names)=1)",
        [], |row| row.get(0),
    ).map_err(|e| format!("检查待重算记录版本失败: {e}"))?;
    if legacy {
        conn.execute_batch("CREATE TABLE pending_derived_rebuild_upgrade (component VARCHAR NOT NULL, start_date VARCHAR NOT NULL, end_date VARCHAR NOT NULL, PRIMARY KEY(component, start_date));
            INSERT INTO pending_derived_rebuild_upgrade SELECT * FROM pending_derived_rebuild;
            DROP TABLE pending_derived_rebuild;
            ALTER TABLE pending_derived_rebuild_upgrade RENAME TO pending_derived_rebuild")
            .map_err(|e| format!("升级待重算区间记录失败: {e}"))?;
    }
    Ok(())
}

pub fn mark_derived_rebuild_required(
    conn: &Connection,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    mark_derived_components_rebuild_required(
        conn,
        &["concept", "ranking", "similarity"],
        start_date,
        end_date,
    )
}

pub fn mark_derived_components_rebuild_required(
    conn: &Connection,
    components: &[&str],
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    ensure_derived_rebuild_table(conn)?;
    for component in components {
        let (first, last): (String, String) = conn.query_row(
            "SELECT LEAST(COALESCE(MIN(start_date), ?), ?), GREATEST(COALESCE(MAX(end_date), ?), ?) FROM pending_derived_rebuild WHERE component=? AND start_date <= ? AND end_date >= ?",
            duckdb::params![start_date, start_date, end_date, end_date, component, end_date, start_date],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(|e| format!("合并后续库待重算区间失败: {e}"))?;
        conn.execute("DELETE FROM pending_derived_rebuild WHERE component=? AND start_date <= ? AND end_date >= ?",
            duckdb::params![component, last, first]).map_err(|e| format!("更新后续库待重算区间失败: {e}"))?;
        conn.execute(
            "INSERT INTO pending_derived_rebuild VALUES (?, ?, ?)",
            duckdb::params![component, first, last],
        )
        .map_err(|e| format!("记录后续库待重算区间失败: {e}"))?;
    }
    Ok(())
}

pub fn clear_pending_derived_rebuild(
    source_dir: &str,
    component: &str,
    start_date: &str,
    end_date: &str,
) -> Result<bool, String> {
    let check = open_source_db_readonly(source_dir)?;
    let pending = load_pending_derived_rebuilds(&check)?;
    drop(check);
    let original = pending
        .into_iter()
        .filter(|item| item.component == component)
        .collect::<Vec<_>>();
    if original.is_empty() {
        return Ok(false);
    }
    let mut remaining = Vec::new();
    for item in load_pending_derived_rebuild_ranges(source_dir, component)? {
        if item.end_date.as_str() < start_date || item.start_date.as_str() > end_date {
            remaining.push(item);
            continue;
        }
        if item.start_date.as_str() < start_date {
            let previous = chrono::NaiveDate::parse_from_str(start_date, "%Y%m%d")
                .ok()
                .and_then(|date| date.pred_opt())
                .ok_or("重算开始日期无效")?;
            remaining.push(PendingDerivedRebuild {
                component: component.to_string(),
                start_date: item.start_date.clone(),
                end_date: previous.format("%Y%m%d").to_string(),
            });
        }
        if item.end_date.as_str() > end_date {
            let next = chrono::NaiveDate::parse_from_str(end_date, "%Y%m%d")
                .ok()
                .and_then(|date| date.succ_opt())
                .ok_or("重算结束日期无效")?;
            remaining.push(PendingDerivedRebuild {
                component: component.to_string(),
                start_date: next.format("%Y%m%d").to_string(),
                end_date: item.end_date,
            });
        }
    }
    if remaining == original {
        return Ok(false);
    }
    let mut conn = Connection::open(source_db_path(source_dir))
        .map_err(|e| format!("清除待重算记录时打开原始库失败: {e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("开启待重算记录事务失败: {e}"))?;
    ensure_derived_rebuild_table(&tx)?;
    tx.execute(
        "DELETE FROM pending_derived_rebuild WHERE component=?",
        [component],
    )
    .map_err(|e| format!("清除后续库待重算记录失败: {e}"))?;
    for item in remaining {
        tx.execute(
            "INSERT INTO pending_derived_rebuild VALUES (?, ?, ?)",
            duckdb::params![component, item.start_date, item.end_date],
        )
        .map_err(|e| format!("保存未完成的重算区间失败: {e}"))?;
    }
    tx.commit()
        .map_err(|e| format!("提交待重算记录失败: {e}"))?;
    Ok(true)
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

// 仅完整覆盖原始行情的重建可以确认历史修复完成；局部区间不能消除历史欠账。
pub fn complete_chip_rebuild(
    source_dir: &str,
    model: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    let check = open_source_db_readonly(source_dir)?;
    let codes = load_pending_chip_repairs_for_model(&check, model)?;
    if codes.is_empty() {
        return Ok(());
    }
    let (source_start, source_end): (Option<String>, Option<String>) = check
        .query_row(
            "SELECT MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|e| format!("读取筹码重建状态覆盖区间失败: {e}"))?;
    drop(check);
    let (Some(source_start), Some(source_end)) = (source_start, source_end) else {
        return Ok(());
    };
    if start_date > source_start.as_str() || end_date < source_end.as_str() {
        return Ok(());
    }
    let conn = Connection::open(source_db_path(source_dir))
        .map_err(|e| format!("同步筹码重建状态时打开原始库失败: {e}"))?;
    clear_completed_chip_repairs(&conn, source_dir, model, &codes)
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
    fn chip_rebuild_completion_preserves_partial_and_other_model_debts() {
        let source_dir = std::env::temp_dir().join(format!(
            "lianghua-chip-rebuild-state-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&source_dir).unwrap();
        let source = source_dir.to_str().unwrap();
        std::fs::write(crate::data::cyq_db_path(source), []).unwrap();
        std::fs::write(crate::data::cyq_chen_db_path(source), []).unwrap();
        let conn = Connection::open(source_db_path(source)).unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR);
            INSERT INTO stock_data VALUES ('000001.SZ', '20260401', 'qfq'), ('000001.SZ', '20260408', 'qfq')").unwrap();
        mark_chip_repairs_required(&conn, &HashSet::from(["000001.SZ".to_string()])).unwrap();
        drop(conn);

        complete_chip_rebuild(source, "chen", "20260402", "20260408").unwrap();
        let conn = open_source_db_readonly(source).unwrap();
        assert_eq!(
            load_pending_chip_repairs_for_model(&conn, "chen")
                .unwrap()
                .len(),
            1
        );
        drop(conn);
        complete_chip_rebuild(source, "chen", "20260401", "20260408").unwrap();
        let conn = open_source_db_readonly(source).unwrap();
        assert!(
            load_pending_chip_repairs_for_model(&conn, "chen")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            load_pending_chip_repairs_for_model(&conn, "legacy")
                .unwrap()
                .len(),
            1
        );
        drop(conn);
        complete_chip_rebuild(source, "legacy", "20260401", "20260408").unwrap();
        let conn = open_source_db_readonly(source).unwrap();
        assert!(
            load_pending_chip_repairs_for_model(&conn, "legacy")
                .unwrap()
                .is_empty()
        );
        drop(conn);
        std::fs::remove_dir_all(source_dir).unwrap();
    }

    #[test]
    fn old_derived_history_does_not_imply_unfinished_stock_repair() {
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
        assert!(load_pending_stock_repairs(&conn).unwrap().is_empty());
        ensure_stock_data_ready_for_derived(source).unwrap();
        drop(conn);
        std::fs::remove_dir_all(source_dir).unwrap();
    }

    #[test]
    fn derived_rebuild_completion_tracks_actual_dates_and_partial_ranges() {
        let directory = std::env::temp_dir().join(format!(
            "lianghua-derived-ranges-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let source = directory.to_str().unwrap();
        let conn = Connection::open(source_db_path(source)).unwrap();
        conn.execute_batch("CREATE TABLE stock_data(trade_date VARCHAR, adj_type VARCHAR);
            INSERT INTO stock_data VALUES ('20260105','qfq'),('20260106','qfq'),('20260107','qfq'),('20260109','qfq');
            CREATE TABLE pending_derived_rebuild(component VARCHAR PRIMARY KEY, start_date VARCHAR, end_date VARCHAR);
            INSERT INTO pending_derived_rebuild VALUES ('ranking','20260101','20260111')").unwrap();
        drop(conn);
        let results = Connection::open(result_db_path(source)).unwrap();
        results
            .execute_batch(
                "CREATE TABLE score_summary(trade_date VARCHAR);
            INSERT INTO score_summary VALUES ('20260105'),('20260106'),('20260107'),('20260109')",
            )
            .unwrap();
        drop(results);
        assert!(pending_rebuild_affects_saved_results(source, "ranking").unwrap());
        assert!(
            !pending_rebuild_affects_saved_results_in_range(
                source, "ranking", "20260101", "20260104"
            )
            .unwrap()
        );
        assert!(
            !pending_rebuild_affects_saved_results_in_range(
                source, "ranking", "20260110", "20260111"
            )
            .unwrap()
        );
        // 从中间开始重算时，前后欠账均须保留，且旧版单区间表自动升级。
        clear_pending_derived_rebuild(source, "ranking", "20260106", "20260107").unwrap();
        let ranges = load_pending_derived_rebuild_ranges(source, "ranking").unwrap();
        assert_eq!(
            ranges
                .iter()
                .map(|item| (item.start_date.as_str(), item.end_date.as_str()))
                .collect::<Vec<_>>(),
            vec![("20260105", "20260105"), ("20260109", "20260109")]
        );
        clear_pending_derived_rebuild(source, "ranking", "20260105", "20260105").unwrap();
        assert!(pending_rebuild_affects_saved_results(source, "ranking").unwrap());
        clear_pending_derived_rebuild(source, "ranking", "20260109", "20260109").unwrap();
        assert!(!pending_rebuild_affects_saved_results(source, "ranking").unwrap());
        let conn = Connection::open(source_db_path(source)).unwrap();
        mark_derived_rebuild_required(&conn, "20260101", "20260111").unwrap();
        drop(conn);
        // 只需覆盖真实交易日，无须重算请求范围内的休市日。
        clear_pending_derived_rebuild(source, "ranking", "20260105", "20260109").unwrap();
        assert!(
            load_pending_derived_rebuild_ranges(source, "ranking")
                .unwrap()
                .is_empty()
        );
        let conn = Connection::open(source_db_path(source)).unwrap();
        mark_derived_rebuild_required(&conn, "20260112", "20260112").unwrap();
        conn.execute_batch("INSERT INTO stock_data VALUES ('20260112','qfq')")
            .unwrap();
        drop(conn);
        assert!(!pending_rebuild_affects_saved_results(source, "ranking").unwrap());
        // 再次覆盖历史，不能沿用上次已完成的状态。
        let conn = Connection::open(source_db_path(source)).unwrap();
        mark_derived_rebuild_required(&conn, "20260105", "20260112").unwrap();
        drop(conn);
        assert!(pending_rebuild_affects_saved_results(source, "ranking").unwrap());
        std::fs::remove_dir_all(directory).unwrap();
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
