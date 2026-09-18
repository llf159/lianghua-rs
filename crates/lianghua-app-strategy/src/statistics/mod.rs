mod backtest;
mod common;
mod detail_cache;
mod market;
mod page;
#[cfg(test)]
mod test_support;
mod universe;
mod validation;

pub use backtest::{
    RankLayerBacktestData, RankLayerBucketSummary, RankLayerMarketValueSummary,
    RankLayerSampleGroup, RankTopKPeriodSummaryData, RankTopKSummaryData, RuleDecayValidation,
    RuleLayerBacktestData, RuleLayerBacktestDefaultsData, RuleLayerPointPayload,
    RuleLayerRuleSummary, SceneLayerBacktestData, SceneLayerBacktestDefaultsData,
    SceneLayerPointPayload, SceneLayerSceneSummary, SceneLayerStateAvgResidualReturn,
    get_rule_layer_backtest_defaults, get_scene_layer_backtest_defaults, run_rank_layer_backtest,
    run_rule_layer_backtest, run_scene_layer_backtest, run_transient_rank_layer_backtest,
    run_transient_rule_layer_backtest, run_transient_scene_layer_backtest,
};
pub use detail_cache::get_cached_rule_layer_backtest_detail;
pub use market::{
    MarketAnalysisData, MarketAnalysisSnapshot, MarketContributionData, MarketContributorItem,
    MarketRankItem, get_market_analysis, get_market_contribution,
};
pub use page::{
    SceneContributionSummary, SceneStageRow, SceneStatisticsPageData, StrategyChartPayload,
    StrategyChartPoint, StrategyDailyRow, StrategyHeatmapCell, StrategyOverviewPayload,
    StrategyStatisticsDetailData, StrategyStatisticsPageData, TriggeredStockRow,
    get_scene_statistics_page, get_strategy_statistics_detail, get_strategy_statistics_page,
    get_strategy_triggered_stocks,
};
pub use validation::{
    RuleExpressionValidationData, RuleExpressionValidationManualStrategy,
    RuleValidationComboResult, RuleValidationDailyMetric, RuleValidationIncrementalData,
    RuleValidationIncrementalFold, RuleValidationReturnDistributionBucket,
    RuleValidationSampleGroups, RuleValidationSampleRow, RuleValidationSampleStats,
    RuleValidationSimilarityRow, RuleValidationTriggerCountStats, RuleValidationUnknownConfig,
    RuleValidationUnknownValue, RuleValidationWalkForwardData, RuleValidationWalkForwardFold,
    ValidationCoreRuleOption, get_validation_core_rule_options, run_rule_expression_validation,
};

#[cfg(test)]
mod tests {

    #[test]
    fn expression_validation_api_drops_auto_scoring_symbols() {
        let sources = [
            ("app statistics", include_str!("mod.rs")),
            ("app statistics common", include_str!("common.rs")),
            ("app statistics page", include_str!("page.rs")),
            ("app statistics universe", include_str!("universe.rs")),
            ("app statistics market", include_str!("market.rs")),
            (
                "app statistics detail cache",
                include_str!("detail_cache.rs"),
            ),
            ("app statistics backtest", include_str!("backtest/mod.rs")),
            (
                "app statistics backtest summary",
                include_str!("backtest/summary.rs"),
            ),
            (
                "app statistics backtest rank layer",
                include_str!("backtest/rank_layer.rs"),
            ),
            (
                "app statistics validation",
                include_str!("validation/mod.rs"),
            ),
            (
                "app statistics validation scores",
                include_str!("validation/scores.rs"),
            ),
            (
                "app statistics validation samples",
                include_str!("validation/samples.rs"),
            ),
            (
                "app statistics validation similarity",
                include_str!("validation/similarity.rs"),
            ),
            (
                "app statistics validation walk forward",
                include_str!("validation/walk_forward.rs"),
            ),
            (
                "tauri commands",
                include_str!("../../../../ui/lianghua_web/src-tauri/src/lib.rs"),
            ),
            (
                "frontend api",
                include_str!("../../../../ui/lianghua_web/src/apis/strategyTrigger.ts"),
            ),
            (
                "frontend page",
                include_str!(
                    "../../../../ui/lianghua_web/src/pages/desktop/SceneLayerBacktestPage.tsx"
                ),
            ),
        ];
        let forbidden = [
            ["suggested", "points"].join("_"),
            ["suggested", "total", "points"].join("_"),
            ["suggested", "dist", "points"].join("_"),
            ["calibration", "score"].join("_"),
            ["recommended", "candidate", "key"].join("_"),
            ["best", "combo", "key"].join("_"),
            ["point", "scale", "description"].join("_"),
            ["run_rule_expression", "calibration"].join("_"),
        ];
        for (label, source) in sources {
            for symbol in &forbidden {
                assert!(!source.contains(symbol), "{label} 仍包含 {symbol}");
            }
        }
    }
}
