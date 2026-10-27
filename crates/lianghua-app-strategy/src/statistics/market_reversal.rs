use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

#[derive(Deserialize)]
struct ModelAsset {
    version: String,
    horizon_sessions: usize,
    features: Vec<String>,
    models: Vec<Model>,
}

#[derive(Deserialize)]
struct RegressionModel {
    features: Vec<String>,
    training_days: usize,
    training_successes: usize,
    training_last_date: String,
    training_labels_through: String,
    mean: Vec<f64>,
    scale: Vec<f64>,
    coefficients: Vec<f64>,
}

#[derive(Deserialize)]
struct Model {
    valid_from: String,
    review_after: String,
    side: i32,
    history_days: usize,
    history_successes: usize,
    history_last_date: String,
    history_labels_through: String,
    base_probability: f64,
    price: Option<RegressionModel>,
    evidence: Option<RegressionModel>,
    evaluation_days: usize,
    evaluation_successes: usize,
}

#[derive(Debug, Serialize)]
pub struct MarketReversalEvidence {
    label: String,
    contribution: f64,
}

#[derive(Debug, Serialize)]
pub struct MarketReversalEstimate {
    status: &'static str,
    method: Option<&'static str>,
    direction: Option<String>,
    probability: Option<f64>,
    horizon_sessions: usize,
    reason: String,
    version: String,
    training_days: usize,
    training_successes: usize,
    training_last_date: Option<String>,
    training_labels_through: Option<String>,
    review_after: Option<String>,
    evaluation_days: usize,
    evaluation_successes: usize,
    evidence: Vec<MarketReversalEvidence>,
}

pub(super) fn estimate_reversal(
    analysis_date: &str,
    features: [Option<f64>; 8],
) -> MarketReversalEstimate {
    let mut result = MarketReversalEstimate {
        status: "unavailable",
        method: None,
        direction: None,
        probability: None,
        horizon_sessions: 5,
        reason: String::new(),
        version: "reversal-5d-v2".to_string(),
        training_days: 0,
        training_successes: 0,
        training_last_date: None,
        training_labels_through: None,
        review_after: None,
        evaluation_days: 0,
        evaluation_successes: 0,
        evidence: Vec::new(),
    };
    let Some(direction) = features[0].filter(|v| v.is_finite()) else {
        result.reason = "10日方向数据不足。".to_string();
        return result;
    };
    let side = if direction < -0.2 {
        result.direction = Some("up".to_string());
        1
    } else if direction > 0.2 {
        result.direction = Some("down".to_string());
        -1
    } else {
        result.status = "not_applicable";
        result.reason = "当前为震荡，没有明确原方向，方向反转事件不适用。".to_string();
        return result;
    };
    static ASSET: OnceLock<Result<ModelAsset, serde_json::Error>> = OnceLock::new();
    let Ok(asset) =
        ASSET.get_or_init(|| serde_json::from_str(include_str!("market_reversal_model.json")))
    else {
        result.reason = "反转模型资产无法读取。".to_string();
        return result;
    };
    let expected_features = [
        "direction",
        "recent",
        "today",
        "breadth",
        "attack_rate",
        "defense_rate",
        "kept_rate",
        "repair_rate",
    ];
    if asset.version != result.version
        || asset.horizon_sessions != result.horizon_sessions
        || !asset
            .features
            .iter()
            .map(String::as_str)
            .eq(expected_features)
    {
        result.reason = "反转模型版本与页面输入口径不匹配。".to_string();
        return result;
    }
    let Some(model) = asset
        .models
        .iter()
        .filter(|model| model.side == side && model.valid_from.as_str() <= analysis_date)
        .max_by(|left, right| left.valid_from.cmp(&right.valid_from))
    else {
        result.reason = "该分析日之前尚无可用训练历史。".to_string();
        return result;
    };
    if model.history_days == 0
        || model.history_successes > model.history_days
        || model.history_last_date > model.history_labels_through
        || model.history_labels_through >= model.valid_from
        || !model.base_probability.is_finite()
        || (model.base_probability
            - (model.history_successes as f64 + 0.5) / (model.history_days as f64 + 1.0))
            .abs()
            > 1e-12
    {
        result.reason = "历史发生率资产或训练时间边界无效。".to_string();
        return result;
    }
    result.status = "estimated";
    result.method = Some("historical_rate");
    result.probability = Some(model.base_probability);
    result.training_days = model.history_days;
    result.training_successes = model.history_successes;
    result.training_last_date = Some(model.history_last_date.clone());
    result.training_labels_through = Some(model.history_labels_through.clone());
    result.review_after = Some(model.review_after.clone());
    result.evaluation_days = model.evaluation_days;
    result.evaluation_successes = model.evaluation_successes;
    result.reason =
        "价格条件或模型支持不足，按同方向历史频率估计；尚未按今日强弱细分。".to_string();
    let labels = [
        "10日方向",
        "5日价格运动",
        "当日价格运动",
        "上涨参与范围",
        "上攻站稳",
        "下探收回",
        "成果保留",
        "跌破修复",
    ];
    for (method, candidate, count) in [
        ("evidence", model.evidence.as_ref(), 8),
        ("price", model.price.as_ref(), 3),
    ] {
        let Some(candidate) = candidate else { continue };
        if candidate.training_successes > candidate.training_days
            || (method == "evidence"
                && (candidate.training_days < 200
                    || candidate.training_successes < 20
                    || candidate.training_days - candidate.training_successes < 20))
            || (method == "price" && candidate.training_days < 60)
            || candidate.training_last_date > candidate.training_labels_through
            || candidate.training_labels_through >= model.valid_from
            || candidate.coefficients.len() != count + 1
            || candidate.mean.len() != count
            || candidate.scale.len() != count
            || !candidate
                .features
                .iter()
                .map(String::as_str)
                .eq(expected_features[..count].iter().copied())
            || candidate
                .coefficients
                .iter()
                .chain(&candidate.mean)
                .any(|v| !v.is_finite())
            || candidate.scale.iter().any(|v| !v.is_finite() || *v <= 0.0)
            || features[..count]
                .iter()
                .any(|value| value.is_none_or(|v| !v.is_finite()))
        {
            continue;
        }
        let mut score = candidate.coefficients[0];
        let mut evidence = Vec::with_capacity(count);
        for (index, value) in features[..count].iter().enumerate() {
            let contribution = (value.unwrap() - candidate.mean[index]) / candidate.scale[index]
                * candidate.coefficients[index + 1];
            score += contribution;
            evidence.push(MarketReversalEvidence {
                label: labels[index].to_string(),
                contribution,
            });
        }
        if !score.is_finite() {
            continue;
        }
        result.probability = Some(if score >= 0.0 {
            1.0 / (1.0 + (-score).exp())
        } else {
            let exponential = score.exp();
            exponential / (1.0 + exponential)
        });
        result.method = Some(method);
        result.training_days = candidate.training_days;
        result.training_successes = candidate.training_successes;
        result.training_last_date = Some(candidate.training_last_date.clone());
        result.training_labels_through = Some(candidate.training_labels_through.clone());
        result.reason = if method == "evidence" {
            "完整证据模型估计；校准样本有限。"
        } else {
            "价格模型估计；小样本采用收缩，校准仍有限。"
        }
        .to_string();
        evidence.sort_by(|a, b| b.contribution.abs().total_cmp(&a.contribution.abs()));
        result.evidence = evidence;
        break;
    }
    if analysis_date > model.review_after.as_str() {
        result.reason.push_str(&format!(
            "已过建议复核日 {}，仍使用原训练结果，需更新模型。",
            model.review_after
        ));
    }
    result
}
