use std::sync::Arc;

use lianghua_model::scoring::{SceneBacktestRow, SceneDetails, ScoreDetails, ScoreSummary};

use super::{RuleScoreSeries, SceneScoreSeries};

pub fn build_score_summaries(
    ts_code: &str,
    trade_dates: &[String],
    total_scores: &[f64],
) -> Vec<ScoreSummary> {
    trade_dates
        .iter()
        .zip(total_scores)
        .map(|(trade_date, total_score)| ScoreSummary {
            ts_code: ts_code.to_string(),
            trade_date: trade_date.clone(),
            total_score: *total_score,
            rank: None,
        })
        .collect()
}

pub fn build_score_details(
    ts_code: &str,
    trade_dates: &[String],
    rule_score_series: &[RuleScoreSeries],
) -> Vec<ScoreDetails> {
    let mut out = Vec::new();
    for rule in rule_score_series {
        if trade_dates.len() != rule.series.len() || trade_dates.len() != rule.triggered.len() {
            continue;
        }
        for (index, trade_date) in trade_dates.iter().enumerate() {
            if !rule.triggered[index] {
                continue;
            }
            out.push(ScoreDetails {
                ts_code: ts_code.to_string(),
                trade_date: trade_date.clone(),
                rule_name: rule.name.clone(),
                rule_score: rule.series[index],
            });
        }
    }
    out
}

pub fn build_scene_details(
    ts_code: &str,
    trade_dates: &[String],
    total_scores: &[f64],
    scene_score_series: &[SceneScoreSeries],
) -> Vec<SceneDetails> {
    let mut out = Vec::new();
    for scene in scene_score_series {
        if trade_dates.len() != scene.triggered.len()
            || trade_dates.len() != total_scores.len()
            || trade_dates.len() != scene.stage_score.len()
            || trade_dates.len() != scene.risk_score.len()
            || trade_dates.len() != scene.confirm_strength.len()
            || trade_dates.len() != scene.risk_intensity.len()
            || trade_dates.len() != scene.stage.len()
        {
            continue;
        }

        for (index, trade_date) in trade_dates.iter().enumerate() {
            if !scene.triggered[index] {
                continue;
            }
            out.push(SceneDetails {
                ts_code: ts_code.to_string(),
                trade_date: trade_date.clone(),
                scene_name: scene.name.clone(),
                direction: scene.direction.as_str().to_string(),
                stage: scene.stage[index].map(|stage| stage.as_str().to_string()),
                stage_score: scene.stage_score[index],
                risk_score: scene.risk_score[index],
                confirm_strength: scene.confirm_strength[index],
                risk_intensity: scene.risk_intensity[index],
                total_score: total_scores[index],
                scene_rank: None,
            });
        }
    }
    out
}

pub fn build_scene_backtest_rows(
    ts_code: &str,
    trade_dates: &[String],
    scene_score_series: &[SceneScoreSeries],
) -> Vec<SceneBacktestRow> {
    let ts_code: Arc<str> = Arc::from(ts_code);
    let trade_dates = trade_dates
        .iter()
        .map(|trade_date| Arc::<str>::from(trade_date.as_str()))
        .collect::<Vec<_>>();
    let mut out = Vec::new();

    for scene in scene_score_series {
        if trade_dates.len() != scene.triggered.len() || trade_dates.len() != scene.stage.len() {
            continue;
        }
        let scene_name: Arc<str> = Arc::from(scene.name.as_str());
        for index in 0..trade_dates.len() {
            if !scene.triggered[index] {
                continue;
            }
            out.push(SceneBacktestRow {
                ts_code: Arc::clone(&ts_code),
                trade_date: Arc::clone(&trade_dates[index]),
                scene_name: Arc::clone(&scene_name),
                stage: scene.stage[index],
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use lianghua_data::data::SceneDirection;
    use lianghua_model::scoring::SceneResolvedStage;

    use super::{build_scene_backtest_rows, build_scene_details};
    use crate::scoring::SceneScoreSeries;

    #[test]
    fn compact_scene_rows_match_full_scene_rows() {
        let trade_dates = vec!["20240102".to_string(), "20240103".to_string()];
        let total_scores = vec![51.0, 49.0];
        let scene_series = vec![SceneScoreSeries {
            name: "趋势".to_string(),
            direction: SceneDirection::Long,
            stage: vec![Some(SceneResolvedStage::Confirm), None],
            stage_score: vec![3.0, 1.0],
            risk_score: vec![0.0, 0.0],
            confirm_strength: vec![1.0, 0.0],
            risk_intensity: vec![0.0, 0.0],
            triggered: vec![true, true],
        }];

        let full = build_scene_details("000001.SZ", &trade_dates, &total_scores, &scene_series);
        let compact = build_scene_backtest_rows("000001.SZ", &trade_dates, &scene_series);
        assert_eq!(full.len(), compact.len());
        for (full, compact) in full.iter().zip(compact) {
            assert_eq!(full.ts_code, compact.ts_code.as_ref());
            assert_eq!(full.trade_date, compact.trade_date.as_ref());
            assert_eq!(full.scene_name, compact.scene_name.as_ref());
            assert_eq!(
                full.stage.as_deref(),
                compact.stage.map(|stage| stage.as_str())
            );
        }
    }
}
