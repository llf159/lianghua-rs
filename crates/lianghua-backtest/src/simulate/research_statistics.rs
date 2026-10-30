use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResearchStatisticsSettings {
    pub block_length: usize,
    pub resamples: usize,
    pub seed: u64,
}
impl Default for ResearchStatisticsSettings {
    fn default() -> Self {
        Self {
            block_length: 10,
            resamples: 500,
            seed: 0,
        }
    }
}
impl ResearchStatisticsSettings {
    pub fn validate(&self) -> Result<(), String> {
        if self.block_length == 0
            || self.block_length > 252
            || !(100..=5000).contains(&self.resamples)
        {
            return Err("日期块长须为1至252，重采样数须为100至5000".into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default, Serialize)]
pub struct ResearchSeriesStatistics {
    pub valid_days: usize,
    pub calendar_days: usize,
    pub mean: Option<f64>,
    pub hac_t: Option<f64>,
    pub normal_approx_p: Option<f64>,
    pub family_adjusted_p: Option<f64>,
    pub lower: Option<f64>,
    pub upper: Option<f64>,
    pub bootstrap_valid: usize,
    pub status: String,
}

pub fn calc_calendar_hac_t(values: &[Option<f64>], lag: usize) -> Option<f64> {
    let valid = values
        .iter()
        .flatten()
        .filter(|value| value.is_finite())
        .copied()
        .collect::<Vec<_>>();
    if valid.len() < 2 {
        return None;
    }
    let mean = valid.iter().sum::<f64>() / valid.len() as f64;
    let n = valid.len() as f64;
    let mut variance = valid
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / n;
    let lag = lag.min(values.len().saturating_sub(1));
    for offset in 1..=lag {
        let covariance = values
            .iter()
            .skip(offset)
            .zip(values)
            .filter_map(|(right, left)| right.zip(*left))
            .filter(|(a, b)| a.is_finite() && b.is_finite())
            .map(|(a, b)| (a - mean) * (b - mean))
            .sum::<f64>()
            / n;
        variance += 2. * (1. - offset as f64 / (lag + 1) as f64) * covariance;
    }
    (variance > 1e-12).then(|| mean / (variance / n).sqrt())
}

/// Missing observations retain their market-calendar positions in both HAC and block resampling.
/// The interval describes a historically selected experiment; it does not establish independent discovery.
pub fn describe_research_series(
    values: &[Option<f64>],
    holding_period: usize,
    settings: &ResearchStatisticsSettings,
    family_size: usize,
) -> Result<ResearchSeriesStatistics, String> {
    settings.validate()?;
    let mut out = ResearchSeriesStatistics {
        calendar_days: values.len(),
        ..Default::default()
    };
    let valid = values
        .iter()
        .flatten()
        .filter(|value| value.is_finite())
        .copied()
        .collect::<Vec<_>>();
    out.valid_days = valid.len();
    if valid.is_empty() {
        out.status = "insufficient".into();
        return Ok(out);
    }
    let mean = valid.iter().sum::<f64>() / valid.len() as f64;
    out.mean = Some(mean);
    if valid.len() < 20 || values.len() < settings.block_length {
        out.status = "insufficient".into();
        return Ok(out);
    }
    out.hac_t = calc_calendar_hac_t(values, holding_period.saturating_sub(1));
    if let Some(t) = out.hac_t {
        // Two-sided normal approximation, explicitly separate from finite-sample/independent evidence.
        let z = t.abs();
        let x = 1. / (1. + 0.2316419 * z);
        let tail = (-0.5 * z * z).exp() / std::f64::consts::TAU.sqrt()
            * x
            * (0.319381530
                + x * (-0.356563782 + x * (1.781477937 + x * (-1.821255978 + x * 1.330274429))));
        let p = (2. * tail).clamp(0., 1.);
        out.normal_approx_p = Some(p);
        out.family_adjusted_p = Some((p * family_size.max(1) as f64).min(1.));
    }
    let mut random = settings.seed.wrapping_add(0x9e3779b97f4a7c15);
    let mut means = Vec::with_capacity(settings.resamples);
    for _ in 0..settings.resamples {
        let mut sum = 0.;
        let mut count = 0usize;
        let mut sampled = 0usize;
        while sampled < values.len() {
            // SplitMix64 is fixed here so archived seeds are reproducible across platforms and rand upgrades.
            random = random.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = random;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^= z >> 31;
            let start = (z % values.len() as u64) as usize;
            for offset in 0..settings.block_length.min(values.len() - sampled) {
                if let Some(value) =
                    values[(start + offset) % values.len()].filter(|value| value.is_finite())
                {
                    sum += value;
                    count += 1;
                }
            }
            sampled += settings.block_length.min(values.len() - sampled);
        }
        if count > 0 {
            means.push(sum / count as f64);
        }
    }
    means.sort_by(f64::total_cmp);
    out.bootstrap_valid = means.len();
    if means.len() >= 100 {
        out.lower = Some(means[(means.len() - 1) * 25 / 1000]);
        out.upper = Some(means[(means.len() - 1) * 975 / 1000]);
    }
    out.status = "descriptive".into();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn research_statistics_retains_calendar_gaps_and_reproduces_zero_difference() {
        let settings = ResearchStatisticsSettings {
            block_length: 5,
            resamples: 100,
            seed: 7,
        };
        let mut values = vec![Some(0.); 40];
        values[7] = None;
        values[22] = None;
        let stats = describe_research_series(&values, 3, &settings, 10).unwrap();
        assert_eq!((stats.valid_days, stats.calendar_days), (38, 40));
        assert_eq!(stats.mean, Some(0.));
        assert_eq!((stats.lower, stats.upper), (Some(0.), Some(0.)));
        assert_eq!(stats.hac_t, None);
        values = vec![Some(2.); 40];
        values[7] = None;
        let a = describe_research_series(&values, 3, &settings, 1).unwrap();
        let b = describe_research_series(&values, 3, &settings, 1).unwrap();
        assert_eq!((a.lower, a.upper), (b.lower, b.upper));
        let gapped = [Some(1.), None, Some(2.), Some(3.), None, Some(5.)];
        let compressed = gapped
            .iter()
            .flatten()
            .copied()
            .map(Some)
            .collect::<Vec<_>>();
        assert!(
            (calc_calendar_hac_t(&gapped, 1).unwrap()
                - calc_calendar_hac_t(&compressed, 1).unwrap())
            .abs()
                > 1e-6
        );
    }
}
