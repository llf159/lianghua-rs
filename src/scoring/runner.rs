use rayon::prelude::*;
use std::{collections::HashSet, sync::mpsc::sync_channel, thread, time};

use crate::data::scoring_data::{
    SceneDetails, ScoreBatch, ScoreDetails, ScoreSummary, ScoreWriteMessage, cache_rule_build,
    init_result_db, row_into_rt, write_score_batches_from_channel,
};
use crate::data::{DataReader, RowData, ScoreRule, ScoreScene, result_db_path};
use crate::scoring::{
    CachedRule, RuleSceneMeta, build_scene_score_series, scoring_rules_details_cache,
    tools::{calc_query_need_rows, calc_zhang_pct, load_st_list, warmup_rows_estimate},
};

const SCORING_GROUP_SIZE: usize = 256;
const SCORING_QUEUE_BOUND: usize = 8;

fn fill_scoring_extra_fields(
    row_data: &mut RowData,
    ts_code: &str,
    is_st: bool,
) -> Result<(), String> {
    let zhang = calc_zhang_pct(ts_code, is_st);
    let zhang_series = vec![Some(zhang); row_data.trade_dates.len()];
    row_data.cols.insert("ZHANG".to_string(), zhang_series);
    row_data.validate()
}

fn scoring_single_core(
    row_data: RowData,
    ts_code: &str,
    score_start_date: &str,
    rules_cache: &[CachedRule],
    rule_scene_meta: &[RuleSceneMeta],
    scenes: &[ScoreScene],
) -> Result<(Vec<ScoreSummary>, Vec<ScoreDetails>, Vec<SceneDetails>), String> {
    let trade_dates = row_data.trade_dates.clone();
    let mut rt = row_into_rt(row_data)?;

    let result = scoring_rules_details_cache(&mut rt, &rules_cache)?;
    let (s, mut d) = (result.0, result.1);

    let keep_from = trade_dates
        .binary_search_by(|d| d.as_str().cmp(score_start_date))
        .unwrap_or_else(|i| i);

    if keep_from >= trade_dates.len() {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    }

    let kept_trade_dates = &trade_dates[keep_from..];
    let kept_scores = &s[keep_from..];

    for rule in &mut d {
        rule.series = rule.series.split_off(keep_from);
        rule.triggered = rule.triggered.split_off(keep_from);
    }

    let summary = ScoreSummary::build(ts_code, kept_trade_dates, kept_scores);
    let details = ScoreDetails::build(ts_code, kept_trade_dates, &d);
    let scene_series = build_scene_score_series(rule_scene_meta, &d, scenes);
    let scene_details = SceneDetails::build(ts_code, kept_trade_dates, &scene_series);

    Ok((summary, details, scene_details))
}

fn scoring_group_batch(
    source_dir: &str,
    adj_type: &str,
    score_start_date: &str,
    end_date: &str,
    need_rows: usize,
    rules_cache: &[CachedRule],
    rule_scene_meta: &[RuleSceneMeta],
    scenes: &[ScoreScene],
    st_list: &HashSet<String>,
    ts_group: &[String],
) -> Result<ScoreBatch, String> {
    let worker_reader = DataReader::new(source_dir)?;
    let mut group_summary = Vec::new();
    let mut group_details = Vec::new();
    let mut group_scenes = Vec::new();

    for ts_code in ts_group {
        let mut row = worker_reader.load_one_tail_rows(ts_code, adj_type, end_date, need_rows)?;
        fill_scoring_extra_fields(&mut row, ts_code, st_list.contains(ts_code))?;
        let (s, d, scene_rows) = scoring_single_core(
            row,
            ts_code,
            score_start_date,
            rules_cache,
            rule_scene_meta,
            scenes,
        )?;
        group_summary.extend(s);
        group_details.extend(d);
        group_scenes.extend(scene_rows);
    }

    Ok(ScoreBatch {
        summary_rows: group_summary,
        detail_rows: group_details,
        scene_rows: group_scenes,
    })
}

pub fn scoring_all_to_db(
    source_dir: &str,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    let time = time::Instant::now();
    let out_db = result_db_path(source_dir);
    init_result_db(&out_db)?;

    let dr = DataReader::new(source_dir)?;
    let tc_list = DataReader::list_ts_code(&dr, adj_type, start_date, end_date)?;
    let st_list = load_st_list(source_dir)?;
    let warmup_need = warmup_rows_estimate(source_dir)?;
    let need_rows = calc_query_need_rows(source_dir, warmup_need, start_date, end_date)?;
    let rules_cache = cache_rule_build(source_dir)?;
    let rule_scene_meta: Vec<RuleSceneMeta> = ScoreRule::load_rules(source_dir)?
        .into_iter()
        .map(|rule| RuleSceneMeta {
            scene_name: rule.scene_name,
            stage: rule.stage,
        })
        .collect();
    let scenes = ScoreScene::load_scenes(source_dir)?;

    let out_db_path = out_db
        .to_str()
        .ok_or_else(|| "结果数据库路径不是有效UTF-8".to_string())?;

    let (tx, rx) = sync_channel(SCORING_QUEUE_BOUND);
    let abort_tx = tx.clone();
    let db_path = out_db_path.to_string();
    let start_date_owned = start_date.to_string();
    let end_date_owned = end_date.to_string();
    let writer_handle = thread::spawn(move || {
        write_score_batches_from_channel(&db_path, &start_date_owned, &end_date_owned, rx)
    });

    let compute_result = tc_list.par_chunks(SCORING_GROUP_SIZE).try_for_each_with(
        tx,
        |sender, ts_group| -> Result<(), String> {
            let batch = scoring_group_batch(
                source_dir,
                adj_type,
                start_date,
                end_date,
                need_rows,
                &rules_cache,
                &rule_scene_meta,
                &scenes,
                &st_list,
                ts_group,
            )?;
            sender
                .send(ScoreWriteMessage::Batch(batch))
                .map_err(|e| format!("发送评分批次失败:{e}"))?;
            Ok(())
        },
    );

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(ScoreWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("结果库写线程异常退出".to_string()),
    };

    compute_result?;
    writer_result?;
    println!("排名主流程结束:{:.3?}", time.elapsed());
    Ok(())
}

pub fn scoring_single_period(
    source_dir: &str,
    ts_code: &str,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(Vec<ScoreSummary>, Vec<ScoreDetails>, Vec<SceneDetails>), String> {
    let dr = DataReader::new(source_dir)?;
    let st_list = load_st_list(source_dir)?;
    let warmup_need = warmup_rows_estimate(source_dir)?;
    let need_rows = calc_query_need_rows(source_dir, warmup_need, start_date, end_date)?;

    let mut row_data = DataReader::load_one_tail_rows(&dr, ts_code, adj_type, end_date, need_rows)?;
    fill_scoring_extra_fields(&mut row_data, ts_code, st_list.contains(ts_code))?;
    let rules_cache = cache_rule_build(source_dir)?;
    let rule_scene_meta: Vec<RuleSceneMeta> = ScoreRule::load_rules(source_dir)?
        .into_iter()
        .map(|rule| RuleSceneMeta {
            scene_name: rule.scene_name,
            stage: rule.stage,
        })
        .collect();
    let scenes = ScoreScene::load_scenes(source_dir)?;
    Ok(scoring_single_core(
        row_data,
        ts_code,
        start_date,
        &rules_cache,
        &rule_scene_meta,
        &scenes,
    )?)
}
