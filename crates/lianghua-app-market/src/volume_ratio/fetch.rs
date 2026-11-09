use super::*;
use lianghua_provider::crawler::volume_ratio::{
    IntradayVolumeSource, fetch_msn_volume_ratio, fetch_tencent_volume_ratio,
    fetch_volume_ratio_from_source, parse_eastmoney_volume_ratio, parse_volume_ratio_from_source,
};

pub(super) struct Task {
    pub code: String,
    pub average: f64,
    pub opening_window_ended: bool,
    pub live_volume_to_hands: f64,
    pub remaining_sources: u8,
    pub queued_at: Instant,
}

pub(super) fn run_worker(source: usize, cache: Arc<Mutex<Cache>>, http: reqwest::blocking::Client) {
    loop {
        let (mut task, generation, date) = {
            let Ok(mut state) = cache.lock() else {
                return;
            };
            loop {
                let (task, wait) = state.take_task(source, Instant::now());
                if let Some(task) = task {
                    break (task, state.generation, state.date.clone());
                }
                // 等待期间释放缓存锁；新任务、会话切换及其他来源失败时主动唤醒。
                state = match wait {
                    Some(wait) => match FETCH_READY[source].wait_timeout(state, wait) {
                        Ok((state, _)) => state,
                        Err(_) => return,
                    },
                    None => match FETCH_READY[source].wait(state) {
                        Ok(state) => state,
                        Err(_) => return,
                    },
                };
            }
        };
        let started = Instant::now();
        let result = match source {
            0 => fetch_tencent_volume_ratio(&http, &task.code, task.average),
            1 => fetch_eastmoney_intraday_text(&http, &task.code)
                .and_then(|raw| parse_eastmoney_volume_ratio(&raw, &task.code, task.average)),
            2 => fetch_msn_volume_ratio(&http, &task.code, task.average),
            _ => {
                let provider = match source {
                    3 => IntradayVolumeSource::Tonghuashun,
                    4 => IntradayVolumeSource::Xueqiu,
                    5 => IntradayVolumeSource::Sse,
                    6 => IntradayVolumeSource::Szse,
                    _ => unreachable!(),
                };
                volume_ratio_session_fetch(VOLUME_RATIO_SOURCES[source]).and_then(|fetch| {
                    match fetch {
                        Some(fetch) => {
                            let raw = fetch(&task.code)?;
                            let data = serde_json::from_str(&raw)
                                .map_err(|e| format!("浏览器分时 JSON 无效: {e}"))?;
                            parse_volume_ratio_from_source(
                                &data,
                                provider,
                                &task.code,
                                task.average,
                            )
                        }
                        None => fetch_volume_ratio_from_source(
                            &http,
                            provider,
                            &task.code,
                            task.average,
                        ),
                    }
                })
            }
        }
        .and_then(|mut data| {
            if data.trade_date != date {
                return Err(format!(
                    "分时日期 {} 与行情日期 {date} 不一致",
                    data.trade_date
                ));
            }
            if task.opening_window_ended && !data.opening_window_complete() {
                return Err("分时未完整覆盖 09:31–09:35 开盘窗口".to_string());
            }
            data.volume_to_hands = task.live_volume_to_hands;
            Ok(data)
        });

        let Ok(mut state) = cache.lock() else {
            return;
        };
        state.in_flight[source] -= 1;
        // 旧请求仍占用本源线程，但不能提交到暂停、清空或切换后的任务代次。
        if state.generation != generation || state.paused {
            continue;
        }
        match &result {
            Ok(_) => {
                state.handle_fetch_success(source, started, Instant::now());
                state.source_results[source] = Some(true);
            }
            Err(error) => {
                state.handle_fetch_error(source, error, Instant::now());
                task.remaining_sources &= !(1 << source);
                let active = task.remaining_sources
                    & if state.msn_enabled {
                        u8::MAX
                    } else {
                        !(1 << 2)
                    };
                if active != 0 {
                    state.enqueue(task);
                    wake_sources(active);
                    continue;
                }
            }
        }
        // 旧基准只保留在 entries 中；不再为排队任务复制整条分时曲线。
        let previous = match state.entries.remove(&task.code) {
            Some(Entry::Pending(previous)) => previous,
            _ => None,
        };
        let entry = match result {
            Ok(data) => {
                state.dirty.insert(task.code.clone());
                Entry::Ready(Baseline {
                    data,
                    source,
                    observed_high: previous.as_ref().map_or(0.0, |b| b.observed_high),
                    observed_opening_high: previous
                        .as_ref()
                        .map_or(0.0, |b| b.observed_opening_high),
                    next_point: 0,
                    loaded_at: Instant::now(),
                })
            }
            Err(_) => Entry::Failed {
                at: Instant::now(),
                baseline: previous,
            },
        };
        state.entries.insert(task.code, entry);
        state.revision += 1;
        if let Err(error) = state.persist(false) {
            state.persistence_error = Some(error);
        }
    }
}
