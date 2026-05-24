use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::sync_channel,
    thread, time,
};

use crate::data::scoring_data::{
    SceneDetails, ScoreBatch, ScoreDetails, ScoreSummary, ScoreWriteMessage, ScoreWriteProfile,
    cache_rule_build, init_result_db, rank_scene_rows, rank_summary_rows_by_score, row_into_rt,
    write_score_batches_from_channel,
};
use crate::data::{
    DataReader, RowData, RuntimeKeyCollectOptions, ScoreRule, ScoreScene,
    collect_runtime_keys_from_expr_programs, result_db_path, source_db_path,
};
use crate::scoring::{
    CachedRule, RuleSceneMeta, TieBreakWay, build_scene_score_series, scoring_rules_details_cache,
    scoring_rules_total_cache,
    tools::{
        calc_query_need_rows, collect_used_cyq_chen_runtime_keys, cyq_chen_runtime_key_names,
        inject_optional_cyq_chen_fields, inject_stock_extra_fields, load_st_list,
        load_total_share_map, preview_optional_cyq_chen_injection_warnings, warmup_rows_estimate,
    },
};

const SCORING_GROUP_SIZE: usize = 128;
const SCORING_QUEUE_BOUND: usize = 8;
const SCORING_INJECTED_RUNTIME_KEYS: [&str; 2] = ["ZHANG", "TOTAL_MV_YI"];
const SCORING_RUNTIME_ALIASES: [(&str, &str); 0] = [];

#[derive(Debug, Clone, Copy)]
pub enum ScoringMemoryMode {
    All,
    SummaryOnly,
    SummaryAndDetails,
    SceneOnly,
}

#[derive(Debug, Default, Clone)]
pub struct ScoringRunProfile {
    pub total_ms: u64,
    pub init_result_db_ms: u64,
    pub prepare_ms: u64,
    pub compute_and_send_batches_ms: u64,
    pub stock_count: usize,
    pub writer: ScoreWriteProfile,
    pub warnings: Vec<String>,
}

fn format_elapsed_ms(elapsed_ms: u64) -> String {
    if elapsed_ms < 1_000 {
        return format!("{elapsed_ms}ms");
    }

    format!("{:.3}s", elapsed_ms as f64 / 1_000.0)
}

fn log_scoring_run_profile(profile: &ScoringRunProfile) {
    println!(
        "排名计算耗时: 总计={}；初始化={}；准备={}；评分={}；写库={}",
        format_elapsed_ms(profile.total_ms),
        format_elapsed_ms(profile.init_result_db_ms),
        format_elapsed_ms(profile.prepare_ms),
        format_elapsed_ms(profile.compute_and_send_batches_ms),
        format_elapsed_ms(profile.writer.total_ms),
    );
    println!(
        "写库明细: 删索引={}；附加原始库={}；删旧数据={}；接收+写入批次(含等待)={}；总榜排名写入={}；提交={}；卸载原始库={}；建索引={}；批次={}",
        format_elapsed_ms(profile.writer.drop_indexes_ms),
        profile
            .writer
            .attach_source_db_ms
            .map(format_elapsed_ms)
            .unwrap_or_else(|| "-".to_string()),
        format_elapsed_ms(profile.writer.delete_range_ms),
        format_elapsed_ms(profile.writer.receive_and_append_batches_ms),
        format_elapsed_ms(profile.writer.summary_rank_ms),
        format_elapsed_ms(profile.writer.commit_ms),
        profile
            .writer
            .detach_source_db_ms
            .map(format_elapsed_ms)
            .unwrap_or_else(|| "-".to_string()),
        format_elapsed_ms(profile.writer.recreate_indexes_ms),
        profile.writer.batch_count,
    );
}

fn scoring_single_core(
    row_data: RowData,
    ts_code: &str,
    score_start_date: &str,
    rules_cache: &[CachedRule],
    rule_scene_meta: &[RuleSceneMeta],
    scenes: &[ScoreScene],
    memory_mode: ScoringMemoryMode,
) -> Result<(Vec<ScoreSummary>, Vec<ScoreDetails>, Vec<SceneDetails>), String> {
    let trade_dates = row_data.trade_dates.clone();
    let mut rt = row_into_rt(row_data)?;

    let keep_from = trade_dates
        .binary_search_by(|d| d.as_str().cmp(score_start_date))
        .unwrap_or_else(|i| i);

    if keep_from >= trade_dates.len() {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    }

    let kept_trade_dates = &trade_dates[keep_from..];

    if matches!(memory_mode, ScoringMemoryMode::SummaryOnly) {
        let total_scores = scoring_rules_total_cache(&mut rt, rules_cache)?;
        let kept_scores = &total_scores[keep_from..];
        let summary = ScoreSummary::build(ts_code, kept_trade_dates, kept_scores);
        return Ok((summary, Vec::new(), Vec::new()));
    }

    let result = scoring_rules_details_cache(&mut rt, rules_cache)?;
    let (total_scores, mut details_series) = (result.0, result.1);
    let kept_scores = &total_scores[keep_from..];

    for rule in &mut details_series {
        rule.series = rule.series.split_off(keep_from);
        rule.triggered = rule.triggered.split_off(keep_from);
    }

    let summary = if matches!(
        memory_mode,
        ScoringMemoryMode::All | ScoringMemoryMode::SummaryAndDetails
    ) {
        ScoreSummary::build(ts_code, kept_trade_dates, kept_scores)
    } else {
        Vec::new()
    };
    let details = if matches!(
        memory_mode,
        ScoringMemoryMode::All | ScoringMemoryMode::SummaryAndDetails
    ) {
        ScoreDetails::build(ts_code, kept_trade_dates, &details_series)
    } else {
        Vec::new()
    };
    let scene_details = if matches!(
        memory_mode,
        ScoringMemoryMode::All | ScoringMemoryMode::SceneOnly
    ) {
        let scene_series = build_scene_score_series(rule_scene_meta, &details_series, scenes);
        SceneDetails::build(ts_code, kept_trade_dates, kept_scores, &scene_series)
    } else {
        Vec::new()
    };

    Ok((summary, details, scene_details))
}

fn collect_scoring_runtime_keys(rules_cache: &[CachedRule]) -> HashSet<String> {
    let programs = rules_cache
        .iter()
        .map(|rule| &rule.when_ast)
        .collect::<Vec<_>>();
    let cyq_chen_keys = cyq_chen_runtime_key_names();
    let injected_keys = SCORING_INJECTED_RUNTIME_KEYS
        .iter()
        .copied()
        .chain(cyq_chen_keys)
        .collect::<Vec<_>>();

    collect_runtime_keys_from_expr_programs(
        &programs,
        RuntimeKeyCollectOptions {
            always_keys: &[],
            injected_keys: &injected_keys,
            aliases: &SCORING_RUNTIME_ALIASES,
        },
    )
}

fn collect_scoring_used_cyq_chen_runtime_keys(rules_cache: &[CachedRule]) -> HashSet<String> {
    let programs = rules_cache
        .iter()
        .map(|rule| &rule.when_ast)
        .collect::<Vec<_>>();
    collect_used_cyq_chen_runtime_keys(&programs)
}

pub fn preview_scoring_runtime_warnings(
    source_dir: &str,
    strategy_path: Option<&str>,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<String>, String> {
    let warmup_need = warmup_rows_estimate(source_dir, strategy_path)?;
    let rules_cache = cache_rule_build(source_dir, strategy_path)?;
    let used_cyq_chen_keys = collect_scoring_used_cyq_chen_runtime_keys(&rules_cache);
    Ok(preview_optional_cyq_chen_injection_warnings(
        source_dir,
        start_date,
        end_date,
        warmup_need,
        &used_cyq_chen_keys,
    ))
}

fn scoring_stock_batch(
    worker_reader: &DataReader,
    source_dir: &str,
    adj_type: &str,
    score_start_date: &str,
    end_date: &str,
    need_rows: usize,
    rules_cache: &[CachedRule],
    rule_scene_meta: &[RuleSceneMeta],
    scenes: &[ScoreScene],
    st_list: &HashSet<String>,
    total_share_map: &HashMap<String, f64>,
    used_cyq_chen_keys: &HashSet<String>,
    ts_code: &str,
    memory_mode: ScoringMemoryMode,
) -> Result<ScoreBatch, String> {
    let mut row = worker_reader.load_one_tail_rows(ts_code, adj_type, end_date, need_rows)?;
    let _ = inject_optional_cyq_chen_fields(&mut row, source_dir, ts_code, used_cyq_chen_keys);
    inject_stock_extra_fields(
        &mut row,
        ts_code,
        st_list.contains(ts_code),
        total_share_map.get(ts_code).copied(),
    )?;
    let (summary_rows, detail_rows, scene_rows) = scoring_single_core(
        row,
        ts_code,
        score_start_date,
        rules_cache,
        rule_scene_meta,
        scenes,
        memory_mode,
    )?;

    Ok(ScoreBatch {
        summary_rows,
        detail_rows,
        scene_rows,
    })
}

fn scoring_stock_group_batch(
    worker_reader: &DataReader,
    source_dir: &str,
    adj_type: &str,
    score_start_date: &str,
    end_date: &str,
    need_rows: usize,
    rules_cache: &[CachedRule],
    rule_scene_meta: &[RuleSceneMeta],
    scenes: &[ScoreScene],
    st_list: &HashSet<String>,
    total_share_map: &HashMap<String, f64>,
    used_cyq_chen_keys: &HashSet<String>,
    ts_group: &[String],
    memory_mode: ScoringMemoryMode,
) -> Result<ScoreBatch, String> {
    let mut group_batch = ScoreBatch::default();
    for ts_code in ts_group {
        let batch = scoring_stock_batch(
            worker_reader,
            source_dir,
            adj_type,
            score_start_date,
            end_date,
            need_rows,
            rules_cache,
            rule_scene_meta,
            scenes,
            st_list,
            total_share_map,
            used_cyq_chen_keys,
            ts_code,
            memory_mode,
        )?;
        group_batch.extend(batch);
    }
    Ok(group_batch)
}

pub fn scoring_all_to_db(
    source_dir: &str,
    strategy_path: Option<&str>,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<ScoringRunProfile, String> {
    let total_started_at = time::Instant::now();
    let out_db = result_db_path(source_dir);
    let init_result_db_started_at = time::Instant::now();
    init_result_db(&out_db)?;
    let init_result_db_ms = init_result_db_started_at.elapsed().as_millis() as u64;

    let prepare_started_at = time::Instant::now();
    let st_list = load_st_list(source_dir)?;
    let total_share_map = load_total_share_map(source_dir).unwrap_or_default();
    let warmup_need = warmup_rows_estimate(source_dir, strategy_path)?;
    let need_rows = calc_query_need_rows(source_dir, warmup_need, start_date, end_date)?;
    let rules_cache = cache_rule_build(source_dir, strategy_path)?;
    let used_cyq_chen_keys = collect_scoring_used_cyq_chen_runtime_keys(&rules_cache);
    let warnings = preview_optional_cyq_chen_injection_warnings(
        source_dir,
        start_date,
        end_date,
        warmup_need,
        &used_cyq_chen_keys,
    );
    let required_runtime_keys = collect_scoring_runtime_keys(&rules_cache);
    let dr = DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
    let tc_list = DataReader::list_ts_code(&dr, adj_type, start_date, end_date)?;
    let rule_scene_meta: Vec<RuleSceneMeta> =
        ScoreRule::load_rules_with_strategy_path(source_dir, strategy_path)?
            .into_iter()
            .map(|rule| RuleSceneMeta {
                scene_name: rule.scene_name,
                stage: rule.stage,
            })
            .collect();
    let scenes = ScoreScene::load_scenes_with_strategy_path(source_dir, strategy_path)?;
    let prepare_ms = prepare_started_at.elapsed().as_millis() as u64;

    let out_db_path = out_db
        .to_str()
        .ok_or_else(|| "结果数据库路径不是有效UTF-8".to_string())?;
    let source_db = source_db_path(source_dir);
    let source_db_path = source_db
        .to_str()
        .ok_or_else(|| "原始数据库路径不是有效UTF-8".to_string())?;

    let (tx, rx) = sync_channel(SCORING_QUEUE_BOUND);
    let abort_tx = tx.clone();
    let db_path = out_db_path.to_string();
    let source_db_path = source_db_path.to_string();
    let adj_type_owned = adj_type.to_string();
    let start_date_owned = start_date.to_string();
    let end_date_owned = end_date.to_string();
    let writer_handle = thread::spawn(move || {
        write_score_batches_from_channel(
            &db_path,
            Some(&source_db_path),
            &adj_type_owned,
            TieBreakWay::KdjJ,
            &start_date_owned,
            &end_date_owned,
            rx,
        )
    });

    let compute_started_at = time::Instant::now();
    let compute_result = tc_list.par_chunks(SCORING_GROUP_SIZE).try_for_each_with(
        tx,
        |sender, ts_group| -> Result<(), String> {
            let worker_reader =
                DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
            let batch = scoring_stock_group_batch(
                &worker_reader,
                source_dir,
                adj_type,
                start_date,
                end_date,
                need_rows,
                &rules_cache,
                &rule_scene_meta,
                &scenes,
                &st_list,
                &total_share_map,
                &used_cyq_chen_keys,
                ts_group,
                ScoringMemoryMode::All,
            )?;
            sender
                .send(ScoreWriteMessage::Batch(batch))
                .map_err(|e| format!("发送评分批次失败:{e}"))?;
            Ok(())
        },
    );
    let compute_and_send_batches_ms = compute_started_at.elapsed().as_millis() as u64;

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(ScoreWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("结果库写线程异常退出".to_string()),
    };

    compute_result?;
    let writer = writer_result?;

    let profile = ScoringRunProfile {
        total_ms: total_started_at.elapsed().as_millis() as u64,
        init_result_db_ms,
        prepare_ms,
        compute_and_send_batches_ms,
        stock_count: tc_list.len(),
        writer,
        warnings,
    };
    log_scoring_run_profile(&profile);
    Ok(profile)
}

pub fn scoring_all_to_memory(
    source_dir: &str,
    strategy_path: Option<&str>,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(ScoreBatch, ScoringRunProfile), String> {
    scoring_all_to_memory_with_mode(
        source_dir,
        strategy_path,
        adj_type,
        start_date,
        end_date,
        ScoringMemoryMode::All,
    )
}

pub fn scoring_all_to_memory_with_mode(
    source_dir: &str,
    strategy_path: Option<&str>,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
    memory_mode: ScoringMemoryMode,
) -> Result<(ScoreBatch, ScoringRunProfile), String> {
    let total_started_at = time::Instant::now();
    let prepare_started_at = time::Instant::now();
    let st_list = load_st_list(source_dir)?;
    let total_share_map = load_total_share_map(source_dir).unwrap_or_default();
    let warmup_need = warmup_rows_estimate(source_dir, strategy_path)?;
    let need_rows = calc_query_need_rows(source_dir, warmup_need, start_date, end_date)?;
    let rules_cache = cache_rule_build(source_dir, strategy_path)?;
    let used_cyq_chen_keys = collect_scoring_used_cyq_chen_runtime_keys(&rules_cache);
    let warnings = preview_optional_cyq_chen_injection_warnings(
        source_dir,
        start_date,
        end_date,
        warmup_need,
        &used_cyq_chen_keys,
    );
    let required_runtime_keys = collect_scoring_runtime_keys(&rules_cache);
    let dr = DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
    let tc_list = DataReader::list_ts_code(&dr, adj_type, start_date, end_date)?;
    let rule_scene_meta: Vec<RuleSceneMeta> =
        ScoreRule::load_rules_with_strategy_path(source_dir, strategy_path)?
            .into_iter()
            .map(|rule| RuleSceneMeta {
                scene_name: rule.scene_name,
                stage: rule.stage,
            })
            .collect();
    let scenes = ScoreScene::load_scenes_with_strategy_path(source_dir, strategy_path)?;
    let prepare_ms = prepare_started_at.elapsed().as_millis() as u64;

    let compute_started_at = time::Instant::now();
    let batch = tc_list
        .par_chunks(SCORING_GROUP_SIZE)
        .map(|ts_group| -> Result<ScoreBatch, String> {
            let worker_reader =
                DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
            scoring_stock_group_batch(
                &worker_reader,
                source_dir,
                adj_type,
                start_date,
                end_date,
                need_rows,
                &rules_cache,
                &rule_scene_meta,
                &scenes,
                &st_list,
                &total_share_map,
                &used_cyq_chen_keys,
                ts_group,
                memory_mode,
            )
        })
        .try_reduce(ScoreBatch::default, |mut left, right| {
            left.extend(right);
            Ok(left)
        })?;
    let compute_and_send_batches_ms = compute_started_at.elapsed().as_millis() as u64;

    let batch_count = (tc_list.len() + SCORING_GROUP_SIZE - 1) / SCORING_GROUP_SIZE;

    let mut batch = batch;
    if !batch.summary_rows.is_empty() {
        rank_summary_rows_by_score(&mut batch.summary_rows);
    }
    if !batch.scene_rows.is_empty() {
        rank_scene_rows(&mut batch.scene_rows);
    }

    let profile = ScoringRunProfile {
        total_ms: total_started_at.elapsed().as_millis() as u64,
        init_result_db_ms: 0,
        prepare_ms,
        compute_and_send_batches_ms,
        stock_count: tc_list.len(),
        writer: ScoreWriteProfile {
            batch_count,
            ..ScoreWriteProfile::default()
        },
        warnings,
    };
    Ok((batch, profile))
}

pub fn scoring_single_period(
    source_dir: &str,
    strategy_path: Option<&str>,
    ts_code: &str,
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(Vec<ScoreSummary>, Vec<ScoreDetails>, Vec<SceneDetails>), String> {
    let st_list = load_st_list(source_dir)?;
    let total_share_map = load_total_share_map(source_dir).unwrap_or_default();
    let warmup_need = warmup_rows_estimate(source_dir, strategy_path)?;
    let need_rows = calc_query_need_rows(source_dir, warmup_need, start_date, end_date)?;
    let rules_cache = cache_rule_build(source_dir, strategy_path)?;
    let used_cyq_chen_keys = collect_scoring_used_cyq_chen_runtime_keys(&rules_cache);
    let required_runtime_keys = collect_scoring_runtime_keys(&rules_cache);
    let dr = DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;

    let mut row_data = DataReader::load_one_tail_rows(&dr, ts_code, adj_type, end_date, need_rows)?;
    let _ =
        inject_optional_cyq_chen_fields(&mut row_data, source_dir, ts_code, &used_cyq_chen_keys);
    inject_stock_extra_fields(
        &mut row_data,
        ts_code,
        st_list.contains(ts_code),
        total_share_map.get(ts_code).copied(),
    )?;
    let rule_scene_meta: Vec<RuleSceneMeta> =
        ScoreRule::load_rules_with_strategy_path(source_dir, strategy_path)?
            .into_iter()
            .map(|rule| RuleSceneMeta {
                scene_name: rule.scene_name,
                stage: rule.stage,
            })
            .collect();
    let scenes = ScoreScene::load_scenes_with_strategy_path(source_dir, strategy_path)?;
    Ok(scoring_single_core(
        row_data,
        ts_code,
        start_date,
        &rules_cache,
        &rule_scene_meta,
        &scenes,
        ScoringMemoryMode::All,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::{RuleTag, ScopeWay, collect_assigned_names_from_expr_program},
        expr::parser::{Parser, lex_all},
    };

    fn cached_rule(name: &str, expression: &str) -> CachedRule {
        let tokens = lex_all(expression);
        let mut parser = Parser::new(tokens);
        let when_ast = parser.parse_main().expect("expression should parse");
        let assigned_names = collect_assigned_names_from_expr_program(&when_ast);

        CachedRule {
            name: name.to_string(),
            scope_windows: 1,
            scope_way: ScopeWay::Last,
            points: 1.0,
            dist_points: None,
            tag: RuleTag::Normal,
            when_src: expression.to_string(),
            when_ast,
            assigned_names,
        }
    }

    #[test]
    fn scoring_runtime_key_collection_skips_injected_fields() {
        let rules = vec![cached_rule(
            "rule_a",
            "M := MA(C, 5); M > MY_SCORE_IND AND ZHANG > 0 AND TOTAL_MV_YI <= 300 AND CYQ_TPR > 0.6",
        )];

        let keys = collect_scoring_runtime_keys(&rules);

        for required_key in ["C", "MY_SCORE_IND"] {
            assert!(keys.contains(required_key), "missing {required_key}");
        }
        assert!(!keys.contains("TOTAL_MV"));
        for injected_key in ["ZHANG", "TOTAL_MV_YI", "CYQ_TPR"] {
            assert!(!keys.contains(injected_key), "unexpected {injected_key}");
        }
        assert!(!keys.contains("O"));
    }
}
