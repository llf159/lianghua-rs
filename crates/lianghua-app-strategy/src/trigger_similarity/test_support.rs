use crate::trigger_similarity::StrategyTriggerSimilarityRow;

pub(in crate::trigger_similarity) fn similarity_row(
    ts_code: &str,
    end_trade_date: &str,
) -> StrategyTriggerSimilarityRow {
    StrategyTriggerSimilarityRow {
        ts_code: ts_code.to_string(),
        name: None,
        industry: None,
        concept: None,
        candidate_start_trade_date: end_trade_date.to_string(),
        candidate_end_trade_date: end_trade_date.to_string(),
        outcome_start_trade_date: end_trade_date.to_string(),
        outcome_end_trade_date: end_trade_date.to_string(),
        similarity_score: 90.0,
        trigger_similarity: 90.0,
        price_volume_similarity: Some(90.0),
        indicator_similarity: Some(90.0),
        market_similarity: Some(90.0),
        matched_rule_count: 1,
        matched_rule_names: vec!["test".to_string()],
        candidate_trigger_count: 1,
        forward_return_pct: 1.0,
        forward_excess_return_pct: Some(0.5),
        mfe_pct: 2.0,
        mae_pct: -1.0,
        total_score: Some(1.0),
        rank: Some(1),
    }
}
