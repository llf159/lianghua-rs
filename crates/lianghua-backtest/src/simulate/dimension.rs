const EPS: f64 = 1e-12;

#[derive(Debug, Clone, Copy)]
pub struct SignalPairMoments {
    pub universe_count: usize,
    pub left_trigger_count: usize,
    pub right_trigger_count: usize,
    pub joint_trigger_count: usize,
    pub left_sum: f64,
    pub right_sum: f64,
    pub left_square_sum: f64,
    pub right_square_sum: f64,
    pub cross_sum: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SignalPairMetrics {
    pub union_trigger_count: usize,
    pub jaccard: Option<f64>,
    pub phi: Option<f64>,
    pub score_pearson: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LinearOrthogonalDiagnostic {
    pub basis_coefficients: Vec<f64>,
    pub explained_variance_ratio: Option<f64>,
    pub residual_variance_ratio: Option<f64>,
}

pub fn calc_signal_pair_metrics(moments: SignalPairMoments) -> SignalPairMetrics {
    let union_trigger_count = moments
        .left_trigger_count
        .saturating_add(moments.right_trigger_count)
        .saturating_sub(moments.joint_trigger_count);
    let jaccard = (union_trigger_count > 0)
        .then_some(moments.joint_trigger_count as f64 / union_trigger_count as f64);

    let only_left = moments
        .left_trigger_count
        .saturating_sub(moments.joint_trigger_count);
    let only_right = moments
        .right_trigger_count
        .saturating_sub(moments.joint_trigger_count);
    let neither = moments
        .universe_count
        .saturating_sub(moments.left_trigger_count + only_right);
    let phi_denominator = (moments.left_trigger_count as f64
        * (moments
            .universe_count
            .saturating_sub(moments.left_trigger_count)) as f64
        * moments.right_trigger_count as f64
        * (moments
            .universe_count
            .saturating_sub(moments.right_trigger_count)) as f64)
        .sqrt();
    let phi = (phi_denominator > EPS).then_some(
        (moments.joint_trigger_count as f64 * neither as f64
            - only_left as f64 * only_right as f64)
            / phi_denominator,
    );

    let sample_count = moments.universe_count as f64;
    let centered_cross = moments.cross_sum - moments.left_sum * moments.right_sum / sample_count;
    let centered_left =
        moments.left_square_sum - moments.left_sum * moments.left_sum / sample_count;
    let centered_right =
        moments.right_square_sum - moments.right_sum * moments.right_sum / sample_count;
    let pearson_denominator = (centered_left.max(0.0) * centered_right.max(0.0)).sqrt();
    let score_pearson = (sample_count >= 2.0 && pearson_denominator > EPS)
        .then_some((centered_cross / pearson_denominator).clamp(-1.0, 1.0));

    SignalPairMetrics {
        union_trigger_count,
        jaccard,
        phi,
        score_pearson,
    }
}

pub fn calc_distance_correlation(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.len() < 3 {
        return None;
    }
    if left
        .iter()
        .chain(right.iter())
        .any(|value| !value.is_finite())
    {
        return None;
    }

    let count = left.len();
    let mut left_row_means = vec![0.0; count];
    let mut right_row_means = vec![0.0; count];
    for row in 0..count {
        for column in 0..count {
            left_row_means[row] += (left[row] - left[column]).abs();
            right_row_means[row] += (right[row] - right[column]).abs();
        }
        left_row_means[row] /= count as f64;
        right_row_means[row] /= count as f64;
    }
    let left_mean = left_row_means.iter().sum::<f64>() / count as f64;
    let right_mean = right_row_means.iter().sum::<f64>() / count as f64;

    let mut covariance_square = 0.0;
    let mut left_variance_square = 0.0;
    let mut right_variance_square = 0.0;
    for row in 0..count {
        for column in 0..count {
            let left_centered =
                (left[row] - left[column]).abs() - left_row_means[row] - left_row_means[column]
                    + left_mean;
            let right_centered =
                (right[row] - right[column]).abs() - right_row_means[row] - right_row_means[column]
                    + right_mean;
            covariance_square += left_centered * right_centered;
            left_variance_square += left_centered * left_centered;
            right_variance_square += right_centered * right_centered;
        }
    }
    let scale = (count * count) as f64;
    covariance_square /= scale;
    left_variance_square /= scale;
    right_variance_square /= scale;
    let denominator = (left_variance_square * right_variance_square).sqrt();
    if denominator <= EPS {
        return None;
    }
    Some(
        (covariance_square.max(0.0) / denominator)
            .sqrt()
            .clamp(0.0, 1.0),
    )
}

pub fn calc_linear_orthogonal_diagnostics(
    correlation_matrix: &[Vec<f64>],
    ridge_lambda: f64,
) -> Result<Vec<LinearOrthogonalDiagnostic>, String> {
    if !ridge_lambda.is_finite() || ridge_lambda < 0.0 {
        return Err("岭正则系数必须是有限的非负数".to_string());
    }
    let dimension = correlation_matrix.len();
    if correlation_matrix
        .iter()
        .any(|row| row.len() != dimension || row.iter().any(|value| !value.is_finite()))
    {
        return Err("相关矩阵必须是有限数值组成的方阵".to_string());
    }

    let mut diagnostics = Vec::with_capacity(dimension);
    for target in 0..dimension {
        if correlation_matrix[target][target] <= EPS {
            diagnostics.push(LinearOrthogonalDiagnostic {
                basis_coefficients: vec![0.0; target],
                explained_variance_ratio: None,
                residual_variance_ratio: None,
            });
            continue;
        }
        if target == 0 {
            diagnostics.push(LinearOrthogonalDiagnostic {
                basis_coefficients: Vec::new(),
                explained_variance_ratio: Some(0.0),
                residual_variance_ratio: Some(1.0),
            });
            continue;
        }

        let mut system = Vec::with_capacity(target);
        let mut target_covariance = Vec::with_capacity(target);
        for row in 0..target {
            let mut values = correlation_matrix[row][..target].to_vec();
            values[row] += ridge_lambda;
            system.push(values);
            target_covariance.push(correlation_matrix[row][target]);
        }
        let Some(coefficients) = solve_linear_system(system, target_covariance.clone()) else {
            diagnostics.push(LinearOrthogonalDiagnostic {
                basis_coefficients: vec![0.0; target],
                explained_variance_ratio: None,
                residual_variance_ratio: None,
            });
            continue;
        };

        let coefficient_target_cross = coefficients
            .iter()
            .zip(target_covariance.iter())
            .map(|(coefficient, covariance)| coefficient * covariance)
            .sum::<f64>();
        let fitted_variance = coefficients
            .iter()
            .enumerate()
            .map(|(row, left)| {
                coefficients
                    .iter()
                    .enumerate()
                    .map(|(column, right)| left * correlation_matrix[row][column] * right)
                    .sum::<f64>()
            })
            .sum::<f64>();
        let residual_variance_ratio =
            (1.0 - 2.0 * coefficient_target_cross + fitted_variance).clamp(0.0, 1.0);
        diagnostics.push(LinearOrthogonalDiagnostic {
            basis_coefficients: coefficients,
            explained_variance_ratio: Some(1.0 - residual_variance_ratio),
            residual_variance_ratio: Some(residual_variance_ratio),
        });
    }
    Ok(diagnostics)
}

fn solve_linear_system(mut matrix: Vec<Vec<f64>>, mut values: Vec<f64>) -> Option<Vec<f64>> {
    for pivot in 0..values.len() {
        let best_row = (pivot..values.len()).max_by(|left, right| {
            matrix[*left][pivot]
                .abs()
                .total_cmp(&matrix[*right][pivot].abs())
        })?;
        if matrix[best_row][pivot].abs() <= EPS {
            return None;
        }
        matrix.swap(pivot, best_row);
        values.swap(pivot, best_row);

        for row in (pivot + 1)..values.len() {
            let factor = matrix[row][pivot] / matrix[pivot][pivot];
            matrix[row][pivot] = 0.0;
            for column in (pivot + 1)..values.len() {
                matrix[row][column] -= factor * matrix[pivot][column];
            }
            values[row] -= factor * values[pivot];
        }
    }

    let mut solution = vec![0.0; values.len()];
    for row in (0..values.len()).rev() {
        let remainder = ((row + 1)..values.len())
            .map(|column| matrix[row][column] * solution[column])
            .sum::<f64>();
        solution[row] = (values[row] - remainder) / matrix[row][row];
    }
    solution
        .iter()
        .all(|value| value.is_finite())
        .then_some(solution)
}

#[cfg(test)]
mod tests {
    use super::{
        SignalPairMoments, calc_distance_correlation, calc_linear_orthogonal_diagnostics,
        calc_signal_pair_metrics,
    };

    #[test]
    fn pair_metrics_include_zero_filled_universe() {
        let metrics = calc_signal_pair_metrics(SignalPairMoments {
            universe_count: 4,
            left_trigger_count: 2,
            right_trigger_count: 2,
            joint_trigger_count: 1,
            left_sum: 2.0,
            right_sum: 2.0,
            left_square_sum: 2.0,
            right_square_sum: 2.0,
            cross_sum: 1.0,
        });
        assert_eq!(metrics.union_trigger_count, 3);
        assert_eq!(metrics.jaccard, Some(1.0 / 3.0));
        assert_eq!(metrics.phi, Some(0.0));
        assert_eq!(metrics.score_pearson, Some(0.0));
    }

    #[test]
    fn distance_correlation_detects_non_linear_dependence() {
        let left = [-2.0, -1.0, 0.0, 1.0, 2.0];
        let right = [4.0, 1.0, 0.0, 1.0, 4.0];
        assert!(calc_distance_correlation(&left, &right).unwrap() > 0.45);
    }

    #[test]
    fn ridge_diagnostic_reports_redundant_second_strategy() {
        let diagnostics =
            calc_linear_orthogonal_diagnostics(&[vec![1.0, 1.0], vec![1.0, 1.0]], 1e-6).unwrap();
        assert_eq!(diagnostics[0].residual_variance_ratio, Some(1.0));
        assert!(diagnostics[1].residual_variance_ratio.unwrap() < 1e-6);
    }
}
