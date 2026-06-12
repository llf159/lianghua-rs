use std::cmp::Ordering;

pub(crate) const EPS: f64 = 1e-12;

#[derive(Debug, Clone, Default)]
pub(crate) struct ProfitLossSums {
    positive_sum: f64,
    negative_loss_sum: f64,
}

impl ProfitLossSums {
    pub(crate) fn push(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        if value > EPS {
            self.positive_sum += value;
        } else if value < -EPS {
            self.negative_loss_sum += value.abs();
        }
    }

    pub(crate) fn merge(&mut self, other: ProfitLossSums) {
        self.positive_sum += other.positive_sum;
        self.negative_loss_sum += other.negative_loss_sum;
    }

    pub(crate) fn ratio(self) -> Option<f64> {
        if self.positive_sum > EPS && self.negative_loss_sum > EPS {
            Some(self.positive_sum / self.negative_loss_sum)
        } else {
            None
        }
    }
}

pub(crate) fn calc_profit_loss_sums(values: &[f64]) -> ProfitLossSums {
    let mut sums = ProfitLossSums::default();
    for value in values {
        sums.push(*value);
    }
    sums
}

pub(crate) fn calc_top_bottom_spread(rule_scores: &[f64], residuals: &[f64]) -> Option<f64> {
    if rule_scores.len() != residuals.len() || rule_scores.len() < 2 {
        return None;
    }

    let mut min_score = f64::INFINITY;
    let mut max_score = f64::NEG_INFINITY;
    for score in rule_scores {
        min_score = min_score.min(*score);
        max_score = max_score.max(*score);
    }
    if (max_score - min_score).abs() < EPS {
        return None;
    }

    let mut ordered = rule_scores
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<(usize, f64)>>();
    ordered.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

    let half = ordered.len() / 2;
    if half == 0 {
        return None;
    }

    let low_sum: f64 = ordered
        .iter()
        .take(half)
        .map(|(idx, _)| residuals[*idx])
        .sum();
    let high_sum: f64 = ordered
        .iter()
        .rev()
        .take(half)
        .map(|(idx, _)| residuals[*idx])
        .sum();

    Some(high_sum / half as f64 - low_sum / half as f64)
}

pub(crate) fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

pub(crate) fn sample_std(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let avg = mean(values)?;
    let var = values
        .iter()
        .map(|v| {
            let d = *v - avg;
            d * d
        })
        .sum::<f64>()
        / (values.len() as f64 - 1.0);
    Some(var.sqrt())
}

pub(crate) fn calc_t_value(mean: Option<f64>, std: Option<f64>, sample_count: usize) -> Option<f64> {
    match (mean, std) {
        (Some(m), Some(s)) if sample_count > 1 && s.abs() >= EPS => {
            Some(m * (sample_count as f64).sqrt() / s)
        }
        _ => None,
    }
}

pub(crate) fn spearman_corr(x: &[f64], y: &[f64]) -> Option<f64> {
    if x.len() != y.len() || x.len() < 2 {
        return None;
    }
    let xr = average_ranks(x);
    let yr = average_ranks(y);
    pearson_corr(&xr, &yr)
}

/// 计算平均排名。
///
/// 先将值 snap 到 EPS 精度以消除亚精度噪声，然后稳定排序（等值按原索引），
/// 等值组分配平均排名。
pub(crate) fn average_ranks(values: &[f64]) -> Vec<f64> {
    let mut indexed: Vec<(usize, f64)> = values
        .iter()
        .copied()
        .enumerate()
        .map(|(i, v)| (i, snap_to_eps(v)))
        .collect();

    // 稳定排序：按 snap 后的值排，等值按原索引 tie-break
    indexed.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

    let mut ranks = vec![0.0_f64; values.len()];
    let mut i = 0usize;
    while i < indexed.len() {
        let mut j = i + 1;
        while j < indexed.len() && (indexed[j].1 - indexed[i].1).abs() < EPS {
            j += 1;
        }

        let avg_rank = (i + 1 + j) as f64 / 2.0;
        for k in i..j {
            ranks[indexed[k].0] = avg_rank;
        }
        i = j;
    }

    ranks
}

/// Pearson 相关系数。
///
/// 结果会 clamp 到 [-1.0, 1.0] 以防止浮点舍入越界。
pub(crate) fn pearson_corr(x: &[f64], y: &[f64]) -> Option<f64> {
    if x.len() != y.len() || x.len() < 2 {
        return None;
    }

    let mean_x = mean(x)?;
    let mean_y = mean(y)?;

    let mut cov = 0.0_f64;
    let mut var_x = 0.0_f64;
    let mut var_y = 0.0_f64;

    for (vx, vy) in x.iter().zip(y.iter()) {
        let dx = *vx - mean_x;
        let dy = *vy - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }

    if var_x <= EPS || var_y <= EPS {
        return None;
    }

    Some((cov / (var_x.sqrt() * var_y.sqrt())).clamp(-1.0, 1.0))
}

/// 将值 snap（四舍五入）到最近 EPS 精度，消除亚精度浮点噪声。
#[inline]
fn snap_to_eps(value: f64) -> f64 {
    if value.is_finite() {
        (value / EPS).round() * EPS
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pearson_corr_clamps_to_one() {
        // 完全正相关的两组 rank 值
        let x: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let y = x.clone();
        let r = pearson_corr(&x, &y).expect("should compute");
        assert!(r >= 0.9999 && r <= 1.0, "r={r}");
    }

    #[test]
    fn pearson_corr_clamps_to_minus_one() {
        let x: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let y: Vec<f64> = (0..100).map(|i| -i as f64).collect();
        let r = pearson_corr(&x, &y).expect("should compute");
        assert!(r <= -0.9999 && r >= -1.0, "r={r}");
    }

    #[test]
    fn average_ranks_tie_break_deterministic() {
        // 两个相等值，应分配到相同的平均排名
        let values = vec![1.0, 1.0, 3.0];
        let ranks = average_ranks(&values);
        assert!((ranks[0] - 1.5).abs() < 1e-9, "rank0={}", ranks[0]);
        assert!((ranks[1] - 1.5).abs() < 1e-9, "rank1={}", ranks[1]);
        assert!((ranks[2] - 3.0).abs() < 1e-9, "rank2={}", ranks[2]);
    }

    #[test]
    fn average_ranks_snaps_sub_eps_noise() {
        // 亚精度噪声不应改变排名
        let values = vec![1.0 + 1e-13, 1.0 - 1e-13, 2.0];
        let ranks = average_ranks(&values);
        // 前两个应被 snap 到相同值，共享排名 1.5
        assert!((ranks[0] - 1.5).abs() < 1e-9, "rank0={}", ranks[0]);
        assert!((ranks[1] - 1.5).abs() < 1e-9, "rank1={}", ranks[1]);
        assert!((ranks[2] - 3.0).abs() < 1e-9, "rank2={}", ranks[2]);
    }

    #[test]
    fn spearman_perfect_positive() {
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![2.0, 4.0, 6.0, 8.0, 10.0];
        let r = spearman_corr(&x, &y).expect("should compute");
        assert!((r - 1.0).abs() < 1e-9, "r={r}");
    }

    #[test]
    fn spearman_perfect_negative() {
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![5.0, 4.0, 3.0, 2.0, 1.0];
        let r = spearman_corr(&x, &y).expect("should compute");
        assert!((r + 1.0).abs() < 1e-9, "r={r}");
    }

    #[test]
    fn spearman_too_few_values() {
        assert_eq!(spearman_corr(&[1.0], &[1.0]), None);
        assert_eq!(spearman_corr(&[], &[]), None);
    }

    #[test]
    fn mean_empty_returns_none() {
        let empty: [f64; 0] = [];
        assert_eq!(mean(&empty), None);
    }

    #[test]
    fn sample_std_single_element() {
        assert_eq!(sample_std(&[5.0_f64]), None);
    }

    #[test]
    fn top_bottom_spread_near_constant_score() {
        // 分差小于 EPS 时返回 None
        let scores = vec![0.5, 0.50000000000001, 0.50000000000002];
        let residuals = vec![0.1, 0.2, 0.3];
        assert_eq!(calc_top_bottom_spread(&scores, &residuals), None);
    }
}
