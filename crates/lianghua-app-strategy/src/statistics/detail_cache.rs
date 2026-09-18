use std::hash::{Hash, Hasher};

use crate::statistics::validation::RuleValidationComboResult;
use rand::random;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
pub(in crate::statistics) struct ActiveRuleBacktestDetailCache {
    pub(in crate::statistics) source_path: String,
    pub(in crate::statistics) directory: PathBuf,
}

pub(in crate::statistics) static RULE_BACKTEST_DETAIL_CACHE: OnceLock<
    Mutex<Option<ActiveRuleBacktestDetailCache>>,
> = OnceLock::new();

pub(in crate::statistics) struct RuleBacktestDetailCacheWriter {
    pub(in crate::statistics) directory: PathBuf,
    pub(in crate::statistics) committed: bool,
}

impl RuleBacktestDetailCacheWriter {
    pub(in crate::statistics) fn new() -> Result<Self, String> {
        let cache = RULE_BACKTEST_DETAIL_CACHE.get_or_init(|| Mutex::new(None));
        let previous = cache
            .lock()
            .map_err(|_| "策略回测明细缓存锁已损坏".to_string())?
            .take();
        if let Some(previous) = previous {
            let _ = fs::remove_dir_all(previous.directory);
        }

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let cache_root = std::env::temp_dir().join("lianghua-rule-backtest-details");
        let _ = fs::remove_dir_all(&cache_root);
        let directory = cache_root.join(format!("{created_at:x}-{:x}", random::<u64>()));
        fs::create_dir_all(&directory)
            .map_err(|error| format!("创建策略回测明细缓存失败: {error}"))?;
        Ok(Self {
            directory,
            committed: false,
        })
    }

    pub(in crate::statistics) fn write(
        &self,
        rule_name: &str,
        detail: &RuleValidationComboResult,
    ) -> Result<(), String> {
        let file = File::create(rule_backtest_detail_cache_path(&self.directory, rule_name))
            .map_err(|error| format!("创建策略 {rule_name} 明细缓存失败: {error}"))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, detail)
            .map_err(|error| format!("写入策略 {rule_name} 明细缓存失败: {error}"))?;
        writer
            .flush()
            .map_err(|error| format!("刷新策略 {rule_name} 明细缓存失败: {error}"))
    }

    pub(in crate::statistics) fn commit(mut self, source_path: &str) -> Result<(), String> {
        let cache = RULE_BACKTEST_DETAIL_CACHE.get_or_init(|| Mutex::new(None));
        let previous = cache
            .lock()
            .map_err(|_| "策略回测明细缓存锁已损坏".to_string())?
            .replace(ActiveRuleBacktestDetailCache {
                source_path: source_path.trim().to_string(),
                directory: self.directory.clone(),
            });
        self.committed = true;
        if let Some(previous) = previous {
            let _ = fs::remove_dir_all(previous.directory);
        }
        Ok(())
    }
}

impl Drop for RuleBacktestDetailCacheWriter {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}

pub(in crate::statistics) fn rule_backtest_detail_cache_path(
    directory: &Path,
    rule_name: &str,
) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    rule_name.hash(&mut hasher);
    directory.join(format!("{:016x}.json", hasher.finish()))
}

pub fn get_cached_rule_layer_backtest_detail(
    source_path: String,
    rule_name: String,
) -> Result<serde_json::Value, String> {
    let rule_name = rule_name.trim();
    if rule_name.is_empty() {
        return Err("rule_name不能为空".to_string());
    }
    let cache = RULE_BACKTEST_DETAIL_CACHE.get_or_init(|| Mutex::new(None));
    let directory = {
        let active = cache
            .lock()
            .map_err(|_| "策略回测明细缓存锁已损坏".to_string())?;
        let active = active
            .as_ref()
            .ok_or_else(|| "当前没有已完成的策略回测明细缓存，请重新执行策略回测".to_string())?;
        if active.source_path != source_path.trim() {
            return Err("策略回测明细缓存与当前数据目录不一致，请重新执行策略回测".to_string());
        }
        active.directory.clone()
    };
    let file = File::open(rule_backtest_detail_cache_path(&directory, rule_name))
        .map_err(|error| format!("读取策略 {rule_name} 明细缓存失败: {error}"))?;
    let detail: serde_json::Value = serde_json::from_reader(BufReader::new(file))
        .map_err(|error| format!("解析策略 {rule_name} 明细缓存失败: {error}"))?;
    if detail.get("combo_key").and_then(serde_json::Value::as_str) != Some(rule_name) {
        return Err(format!("策略 {rule_name} 明细缓存校验失败"));
    }
    Ok(detail)
}
