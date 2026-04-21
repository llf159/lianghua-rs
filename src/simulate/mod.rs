pub mod rank;
pub mod rule;
pub mod scene;

use std::collections::HashMap;

use duckdb::{Connection, params};

use crate::data::concept_performance_data::{
    load_concept_trend_series, load_industry_trend_series,
};
use crate::data::{load_trade_date_list, stock_list_path};

pub(crate) const DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS: usize = 60;

#[derive(Debug, Clone, Default)]
pub(super) struct BacktestSampleEligibility {
    trade_date_to_index: HashMap<String, usize>,
    listed_trade_index_by_ts: HashMap<String, usize>,
    min_listed_trade_days: usize,
}

impl BacktestSampleEligibility {
    pub(crate) fn allows_sample(&self, ts_code: &str, trade_date: &str) -> bool {
        if self.min_listed_trade_days == 0 {
            return true;
        }

        let Some(sample_idx) = self.trade_date_to_index.get(trade_date).copied() else {
            return false;
        };
        let Some(listed_idx) = self.listed_trade_index_by_ts.get(ts_code).copied() else {
            return true;
        };

        sample_idx + 1 >= listed_idx + self.min_listed_trade_days
    }
}

pub(crate) fn build_backtest_sample_eligibility(
    source_dir: &str,
    min_listed_trade_days: usize,
) -> Result<BacktestSampleEligibility, String> {
    if min_listed_trade_days == 0 {
        return Ok(BacktestSampleEligibility::default());
    }

    let trade_dates = load_trade_date_list(source_dir)?;
    let mut trade_date_to_index = HashMap::with_capacity(trade_dates.len());
    for (index, trade_date) in trade_dates.iter().enumerate() {
        trade_date_to_index.insert(trade_date.clone(), index);
    }

    let stock_list = stock_list_path(source_dir);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&stock_list)
        .map_err(|e| format!("打开stock_list.csv失败:路径:{:?},错误:{e}", stock_list))?;
    let headers = reader
        .headers()
        .map_err(|e| format!("读取stock_list.csv表头失败:{e}"))?
        .iter()
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();

    let Some(ts_code_idx) = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("ts_code"))
    else {
        return Ok(BacktestSampleEligibility {
            trade_date_to_index,
            listed_trade_index_by_ts: HashMap::new(),
            min_listed_trade_days,
        });
    };
    let Some(list_date_idx) = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("list_date"))
    else {
        return Ok(BacktestSampleEligibility {
            trade_date_to_index,
            listed_trade_index_by_ts: HashMap::new(),
            min_listed_trade_days,
        });
    };

    let mut listed_trade_index_by_ts = HashMap::new();
    for row_result in reader.records() {
        let row = row_result.map_err(|e| format!("解析stock_list.csv失败:{e}"))?;
        let Some(ts_code) = row
            .get(ts_code_idx)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(list_date) = row
            .get(list_date_idx)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };

        let listed_idx = match trade_dates.binary_search_by(|value| value.as_str().cmp(list_date)) {
            Ok(index) => index,
            Err(index) if index < trade_dates.len() => index,
            Err(_) => continue,
        };
        listed_trade_index_by_ts.insert(ts_code.to_string(), listed_idx);
    }

    Ok(BacktestSampleEligibility {
        trade_date_to_index,
        listed_trade_index_by_ts,
        min_listed_trade_days,
    })
}

#[derive(Debug, Clone)]
pub struct ResidualReturnInput {
    pub ts_code: String,
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub concept: String,
    pub industry: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub industry_beta: f64,
    pub start_date: String,
    pub end_date: String,
}

impl ResidualReturnInput {
    fn validate(&self) -> Result<(), String> {
        if self.ts_code.trim().is_empty() {
            return Err("股票代码不能为空".to_string());
        }
        if self.stock_adj_type.trim().is_empty() {
            return Err("股票复权类型不能为空".to_string());
        }
        if self.index_ts_code.trim().is_empty() {
            return Err("指数代码不能为空".to_string());
        }
        if self.start_date.trim().is_empty() || self.end_date.trim().is_empty() {
            return Err("区间日期不能为空".to_string());
        }
        if self.start_date > self.end_date {
            return Err(format!(
                "区间日期非法:start_date({})大于end_date({})",
                self.start_date, self.end_date
            ));
        }
        if !self.index_beta.is_finite() {
            return Err("指数系数必须是有限数字".to_string());
        }
        if !self.concept_beta.is_finite() {
            return Err("概念系数必须是有限数字".to_string());
        }
        if !self.industry_beta.is_finite() {
            return Err("行业系数必须是有限数字".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResidualReturnPoint {
    pub trade_date: String,
    pub stock_pct: f64,
    pub index_pct: f64,
    pub concept_pct: f64,
    pub industry_pct: f64,
    pub expected_pct: f64,
    pub residual_pct: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ResidualFactorSeriesRefs<'a> {
    pub concept_series: Option<&'a HashMap<String, f64>>,
    pub industry_series: Option<&'a HashMap<String, f64>>,
}

pub fn calc_stock_residual_returns(
    source_conn: &Connection,
    source_dir: &str,
    input: &ResidualReturnInput,
) -> Result<Vec<ResidualReturnPoint>, String> {
    calc_stock_residual_returns_with_factor_series(
        source_conn,
        source_dir,
        input,
        ResidualFactorSeriesRefs::default(),
    )
}

pub fn calc_stock_residual_returns_with_factor_series(
    source_conn: &Connection,
    source_dir: &str,
    input: &ResidualReturnInput,
    factor_series: ResidualFactorSeriesRefs<'_>,
) -> Result<Vec<ResidualReturnPoint>, String> {
    input.validate()?;

    let stock_series = load_pct_chg_series(
        source_conn,
        input.ts_code.trim(),
        input.stock_adj_type.trim(),
        input.start_date.trim(),
        input.end_date.trim(),
    )?;
    if stock_series.is_empty() {
        return Ok(Vec::new());
    }

    let index_series = load_pct_chg_series(
        source_conn,
        input.index_ts_code.trim(),
        "ind",
        input.start_date.trim(),
        input.end_date.trim(),
    )?;
    if index_series.is_empty() {
        return Ok(Vec::new());
    }

    let use_concept = input.concept_beta.abs() > f64::EPSILON;
    let use_industry = input.industry_beta.abs() > f64::EPSILON;

    let concept_series_owned: Option<HashMap<String, f64>> = if use_concept
        && !input.concept.trim().is_empty()
        && factor_series.concept_series.is_none()
    {
        let concept_series = load_concept_trend_series(
            source_dir,
            input.concept.trim(),
            Some(input.start_date.trim()),
            Some(input.end_date.trim()),
        )?;
        if concept_series.points.is_empty() {
            return Ok(Vec::new());
        }

        Some(
            concept_series
                .points
                .into_iter()
                .map(|point| (point.trade_date, point.performance_pct))
                .collect(),
        )
    } else {
        None
    };

    let industry_series_owned: Option<HashMap<String, f64>> = if use_industry
        && !input.industry.trim().is_empty()
        && factor_series.industry_series.is_none()
    {
        let industry_series = load_industry_trend_series(
            source_dir,
            input.industry.trim(),
            Some(input.start_date.trim()),
            Some(input.end_date.trim()),
        )?;
        if industry_series.points.is_empty() {
            return Ok(Vec::new());
        }

        Some(
            industry_series
                .points
                .into_iter()
                .map(|point| (point.trade_date, point.performance_pct))
                .collect(),
        )
    } else {
        None
    };

    let concept_map: Option<&HashMap<String, f64>> = if use_concept {
        if input.concept.trim().is_empty() {
            Some(&index_series)
        } else if let Some(series) = factor_series.concept_series {
            if series.is_empty() {
                return Ok(Vec::new());
            }
            Some(series)
        } else {
            concept_series_owned.as_ref()
        }
    } else {
        None
    };

    let industry_map: Option<&HashMap<String, f64>> = if use_industry {
        if input.industry.trim().is_empty() {
            Some(&index_series)
        } else if let Some(series) = factor_series.industry_series {
            if series.is_empty() {
                return Ok(Vec::new());
            }
            Some(series)
        } else {
            industry_series_owned.as_ref()
        }
    } else {
        None
    };

    calc_stock_residual_returns_from_loaded_series(
        input,
        &stock_series,
        &index_series,
        ResidualFactorSeriesRefs {
            concept_series: concept_map,
            industry_series: industry_map,
        },
    )
}

pub(super) fn calc_stock_residual_returns_from_loaded_series(
    input: &ResidualReturnInput,
    stock_series: &HashMap<String, f64>,
    index_series: &HashMap<String, f64>,
    factor_series: ResidualFactorSeriesRefs<'_>,
) -> Result<Vec<ResidualReturnPoint>, String> {
    input.validate()?;
    if stock_series.is_empty() || index_series.is_empty() {
        return Ok(Vec::new());
    }

    let use_concept = input.concept_beta.abs() > f64::EPSILON;
    let use_industry = input.industry_beta.abs() > f64::EPSILON;

    let concept_map: Option<&HashMap<String, f64>> = if use_concept {
        if input.concept.trim().is_empty() {
            Some(index_series)
        } else if let Some(series) = factor_series.concept_series {
            if series.is_empty() {
                return Ok(Vec::new());
            }
            Some(series)
        } else {
            return Ok(Vec::new());
        }
    } else {
        None
    };

    let industry_map: Option<&HashMap<String, f64>> = if use_industry {
        if input.industry.trim().is_empty() {
            Some(index_series)
        } else if let Some(series) = factor_series.industry_series {
            if series.is_empty() {
                return Ok(Vec::new());
            }
            Some(series)
        } else {
            return Ok(Vec::new());
        }
    } else {
        None
    };

    let mut trade_dates = stock_series.keys().map(String::as_str).collect::<Vec<_>>();
    trade_dates.sort_unstable();

    let mut points = Vec::with_capacity(trade_dates.len());
    for trade_date in trade_dates {
        let Some(stock_pct) = stock_series.get(trade_date).copied() else {
            continue;
        };
        let Some(index_pct) = index_series.get(trade_date).copied() else {
            continue;
        };

        let concept_pct = if use_concept {
            let Some(series) = concept_map else {
                continue;
            };
            let Some(value) = series.get(trade_date).copied() else {
                continue;
            };
            value
        } else {
            0.0
        };

        let industry_pct = if use_industry {
            let Some(series) = industry_map else {
                continue;
            };
            let Some(value) = series.get(trade_date).copied() else {
                continue;
            };
            value
        } else {
            0.0
        };

        let expected_pct = input.index_beta * index_pct
            + input.concept_beta * concept_pct
            + input.industry_beta * industry_pct;
        let residual_pct = stock_pct - expected_pct;

        points.push(ResidualReturnPoint {
            trade_date: trade_date.to_string(),
            stock_pct,
            index_pct,
            concept_pct,
            industry_pct,
            expected_pct,
            residual_pct,
        });
    }

    Ok(points)
}

fn load_pct_chg_series(
    conn: &Connection,
    ts_code: &str,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<HashMap<String, f64>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                trade_date,
                TRY_CAST(pct_chg AS DOUBLE)
            FROM stock_data
            WHERE ts_code = ?
              AND adj_type = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译涨跌幅查询失败:{e}"))?;

    let mut rows = stmt
        .query(params![ts_code, adj_type, start_date, end_date])
        .map_err(|e| format!("查询涨跌幅失败:{e}"))?;

    let mut series = HashMap::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取涨跌幅失败:{e}"))? {
        let trade_date: String = row.get(0).map_err(|e| format!("读取trade_date失败:{e}"))?;
        let pct: Option<f64> = row.get(1).map_err(|e| format!("读取pct_chg失败:{e}"))?;

        let Some(pct) = pct.filter(|value| value.is_finite()) else {
            continue;
        };
        series.insert(trade_date, pct);
    }

    Ok(series)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{create_dir_all, write},
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    use crate::{
        data::{concept_performance_db_path, source_db_path},
        simulate::{
            ResidualReturnInput, build_backtest_sample_eligibility, calc_stock_residual_returns,
        },
    };

    fn temp_source_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_residual_{unique}"))
    }

    fn prepare_source_db(source_dir: &str) -> Connection {
        let db_path = source_db_path(source_dir);
        let conn = Connection::open(db_path).expect("open source db");
        conn.execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                pct_chg DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");

        {
            let mut app = conn.appender("stock_data").expect("appender stock_data");
            app.append_row(params!["000001.SZ", "20240102", "qfq", 3.0_f64])
                .expect("row1");
            app.append_row(params!["000001.SZ", "20240103", "qfq", 1.0_f64])
                .expect("row2");

            app.append_row(params!["000300.SH", "20240102", "ind", 1.0_f64])
                .expect("row3");
            app.append_row(params!["000300.SH", "20240103", "ind", 0.5_f64])
                .expect("row4");

            app.flush().expect("flush stock_data");
        }
        conn
    }

    fn prepare_concept_performance_db(source_dir: &str) {
        let db_path = concept_performance_db_path(source_dir);
        let conn = Connection::open(db_path).expect("open concept db");
        conn.execute(
            r#"
            CREATE TABLE concept_performance (
                trade_date VARCHAR,
                performance_type VARCHAR,
                concept VARCHAR,
                performance_pct DOUBLE
            )
            "#,
            [],
        )
        .expect("create concept_performance");

        let mut app = conn
            .appender("concept_performance")
            .expect("appender concept_performance");
        app.append_row(params!["20240102", "industry", "银行", 2.0_f64])
            .expect("industry row1");
        app.append_row(params!["20240103", "industry", "银行", 0.5_f64])
            .expect("industry row2");
        app.flush().expect("flush concept_performance");
    }

    #[test]
    fn backtest_sample_eligibility_uses_list_date_and_trade_calendar() {
        let source_dir = temp_source_dir();
        create_dir_all(&source_dir).expect("create source dir");

        let trade_calendar = std::iter::once("cal_date".to_string())
            .chain((1..=70).map(|index| format!("202300{:02}", index)))
            .chain(
                ["20240102", "20240103", "20240104"]
                    .into_iter()
                    .map(str::to_string),
            )
            .collect::<Vec<_>>()
            .join("\n");
        write(
            source_dir.join("trade_calendar.csv"),
            format!("{trade_calendar}\n"),
        )
        .expect("write trade_calendar");
        write(
            source_dir.join("stock_list.csv"),
            concat!(
                "ts_code,symbol,name,area,industry,list_date,trade_date,total_share,float_share,total_mv,circ_mv,fullname,enname,cnspell,market,exchange,curr_type,list_status,delist_date,is_hs,act_name,act_ent_type\n",
                "000001.SZ,,样本股,,main,20230010,,,,,,,,,,,,,,,\n",
                "000002.SZ,,次新股,,main,20240103,,,,,,,,,,,,,,,\n"
            ),
        )
        .expect("write stock_list");

        let eligibility = build_backtest_sample_eligibility(source_dir.to_str().expect("utf8"), 60)
            .expect("eligibility");

        assert!(eligibility.allows_sample("000001.SZ", "20240102"));
        assert!(eligibility.allows_sample("000001.SZ", "20240103"));
        assert!(!eligibility.allows_sample("000002.SZ", "20240103"));
        assert!(!eligibility.allows_sample("000002.SZ", "20240104"));
    }

    #[test]
    fn calc_stock_residual_returns_uses_index_as_concept_when_concept_empty() {
        let source_dir = temp_source_dir();
        create_dir_all(&source_dir).expect("create source dir");
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");

        let conn = prepare_source_db(source_dir_str);

        let input = ResidualReturnInput {
            ts_code: "000001.SZ".to_string(),
            stock_adj_type: "qfq".to_string(),
            index_ts_code: "000300.SH".to_string(),
            concept: "".to_string(),
            industry: "".to_string(),
            index_beta: 0.5,
            concept_beta: 0.2,
            industry_beta: 0.0,
            start_date: "20240101".to_string(),
            end_date: "20240105".to_string(),
        };

        let points = calc_stock_residual_returns(&conn, source_dir_str, &input).expect("calc ok");
        assert_eq!(points.len(), 2);

        let p0 = &points[0];
        assert_eq!(p0.trade_date, "20240102");
        assert_eq!(p0.concept_pct, 1.0);
        assert_eq!(p0.industry_pct, 0.0);
        assert_eq!(p0.expected_pct, 0.7);
        assert_eq!(p0.residual_pct, 2.3);

        let p1 = &points[1];
        assert_eq!(p1.trade_date, "20240103");
        assert_eq!(p1.concept_pct, 0.5);
        assert_eq!(p1.industry_pct, 0.0);
        assert_eq!(p1.expected_pct, 0.35);
        assert_eq!(p1.residual_pct, 0.65);
    }

    #[test]
    fn calc_stock_residual_returns_uses_industry_series_for_industry_factor() {
        let source_dir = temp_source_dir();
        create_dir_all(&source_dir).expect("create source dir");
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");

        let conn = prepare_source_db(source_dir_str);
        prepare_concept_performance_db(source_dir_str);

        let input = ResidualReturnInput {
            ts_code: "000001.SZ".to_string(),
            stock_adj_type: "qfq".to_string(),
            index_ts_code: "000300.SH".to_string(),
            concept: "".to_string(),
            industry: "银行".to_string(),
            index_beta: 0.0,
            concept_beta: 0.0,
            industry_beta: 1.0,
            start_date: "20240101".to_string(),
            end_date: "20240105".to_string(),
        };

        let points = calc_stock_residual_returns(&conn, source_dir_str, &input).expect("calc ok");
        assert_eq!(points.len(), 2);

        let p0 = &points[0];
        assert_eq!(p0.trade_date, "20240102");
        assert_eq!(p0.industry_pct, 2.0);
        assert_eq!(p0.expected_pct, 2.0);
        assert_eq!(p0.residual_pct, 1.0);

        let p1 = &points[1];
        assert_eq!(p1.trade_date, "20240103");
        assert_eq!(p1.industry_pct, 0.5);
        assert_eq!(p1.expected_pct, 0.5);
        assert_eq!(p1.residual_pct, 0.5);
    }
}
