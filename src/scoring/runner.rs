use rayon::prelude::*;
use std::{path::Path, time};

use crate::scoring::{
    CachedRule,
    data::{DataReader, RowData, ScoreDetails, ScoreSummary, init_duckdb_database, row_into_rt},
    scoring_rules_details_cache,
    tools::{cache_rule_build, calc_query_start_date, warmup_rows_estimate},
};

fn scoring_single_core(
    row_data: RowData,
    ts_code: &str,
    score_start_date: &str,
    rules_cache: &Vec<CachedRule>,
) -> Result<(Vec<ScoreSummary>, Vec<ScoreDetails>), String> {
    let trade_dates = row_data.trade_dates.clone();
    let mut rt = row_into_rt(row_data)?;

    let result = scoring_rules_details_cache(&mut rt, &rules_cache)?;
    let (s, mut d) = (result.0, result.1);

    let keep_from = trade_dates
        .binary_search_by(|d| d.as_str().cmp(score_start_date))
        .unwrap_or_else(|i| i);

    if keep_from >= trade_dates.len() {
        return Ok((Vec::new(), Vec::new()));
    }

    let kept_trade_dates = &trade_dates[keep_from..];
    let kept_scores = &s[keep_from..];

    for rule in &mut d {
        rule.series = rule.series.split_off(keep_from);
    }

    let summary = ScoreSummary::build(ts_code, kept_trade_dates, kept_scores);
    let details = ScoreDetails::build(ts_code, kept_trade_dates, &d);

    Ok((summary, details))
}

fn scoring_all_core(
    db_path: &str,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(Vec<ScoreSummary>, Vec<ScoreDetails>), String> {
    let dr = DataReader::new(db_path)?;
    let tc_list = DataReader::list_ts_code(&dr, adj_type, start_date, end_date)?;
    let mut all_summary: Vec<ScoreSummary> = Vec::with_capacity(8192);
    let mut all_details: Vec<ScoreDetails> = Vec::with_capacity(8192);

    let path = Path::new(db_path);
    let csv_path = path
        .parent()
        .ok_or_else(|| "数据库路径缺少父目录".to_string())?;
    let csv_path = csv_path
        .to_str()
        .ok_or_else(|| "数据库父目录不是有效UTF-8".to_string())?;

    let time = time::Instant::now();
    let warmup_need = warmup_rows_estimate()?;
    let std_start_date = calc_query_start_date(csv_path, warmup_need, start_date)?;

    let rules_cache = cache_rule_build()?;

    let result_collect = tc_list
        .par_chunks(256)
        .map(
            |ts_group| -> Result<(Vec<ScoreSummary>, Vec<ScoreDetails>), String> {
                let worker_reader = DataReader::new(db_path)?;
                let mut group_summary = Vec::new();
                let mut group_details = Vec::new();

                for ts_code in ts_group {
                    let row = worker_reader.load_one(
                        ts_code,
                        adj_type,
                        std_start_date.as_str(),
                        end_date,
                    )?;
                    let (s, d) = scoring_single_core(row, ts_code, start_date, &rules_cache)?;
                    group_summary.extend(s);
                    group_details.extend(d);
                }

                Ok((group_summary, group_details))
            },
        )
        .collect::<Vec<_>>();

    // 单例并行
    // let result_collect = tc_list
    //     .par_iter()
    //     .map(|ts_code| {
    //         let worker_reader = DataReader::new(db_path)?;
    //         let row =
    //             worker_reader.load_one(ts_code, adj_type, std_start_date.as_str(), end_date)?;
    //         scoring_single_core(row, ts_code, start_date, &rules_cache)
    //     })
    //     .collect::<Vec<_>>();

    // 原先串行方案备份
    // for ts_code in tc_list {
    //     let row = dr.load_one(&ts_code, adj_type, std_start_date.as_str(), end_date)?;
    //     let (s, d) = scoring_single_core(row, &ts_code, start_date, &rules_cache)?;
    //     all_summary.extend(s);
    //     all_details.extend(d);
    // }

    for result in result_collect {
        let (a, d) = result?;
        all_summary.extend(a);
        all_details.extend(d);
    }
    println!("排名主流程结束:{:.3?}", time.elapsed());
    Ok((all_summary, all_details))
}

pub fn scoring_all_to_db(
    db_path: &str,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    let out_dir = Path::new("./output");
    let out_db = out_dir.join("scoring_result.db");
    let (all_summary, all_details) = scoring_all_core(db_path, adj_type, start_date, end_date)?;
    init_duckdb_database(&out_db)?;
    let out_db_path = out_db
        .to_str()
        .ok_or_else(|| "结果数据库路径不是有效UTF-8".to_string())?;
    ScoreSummary::write_db(out_db_path, &all_summary)?;
    ScoreDetails::write_db(out_db_path, &all_details)?;
    Ok(())
}

pub fn scoring_single_period(
    db_path: &str,
    ts_code: &str,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(Vec<ScoreSummary>, Vec<ScoreDetails>), String> {
    let dr = DataReader::new(db_path)?;
    let warmup_need = warmup_rows_estimate()?;
    let path = Path::new(db_path);
    let csv_path = path
        .parent()
        .ok_or_else(|| "数据库路径缺少父目录".to_string())?;
    let csv_path = csv_path
        .to_str()
        .ok_or_else(|| "数据库父目录不是有效UTF-8".to_string())?;
    let std_start_date = calc_query_start_date(csv_path, warmup_need, start_date)?;

    let row_data = DataReader::load_one(&dr, ts_code, adj_type, &std_start_date, end_date)?;
    let rules_cache = cache_rule_build()?;
    Ok(scoring_single_core(
        row_data,
        ts_code,
        start_date,
        &rules_cache,
    )?)
}
