use lianghua_app_facade::cloud_sync;
use std::{collections::BTreeMap, fs, path::Path};

fn main() -> Result<(), String> {
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("estimate") if args.len() == 4 => {
            let estimate = cloud_sync::estimate_upload(Path::new(&args[3]), Path::new(&args[2]), &BTreeMap::new())?;
            println!("{}", serde_json::to_string_pretty(&estimate).map_err(|e| e.to_string())?);
        }
        Some("test") if args.len() == 3 => {
            // 配置由权限受限的文件读取，避免密码出现在进程参数或诊断输出中。
            let config: cloud_sync::WebDavConfig = serde_json::from_slice(&fs::read(&args[2]).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
            if !config.folder.starts_with("lhtest-") { return Err("真实写入诊断只允许 lhtest- 开头的独立测试目录".into()); }
            let mut root = reqwest::Url::parse(&config.endpoint).map_err(|e| e.to_string())?;
            root.path_segments_mut().map_err(|_| "地址不支持目录")?.pop_if_empty().push(&config.folder).push("");
            let cleanup = reqwest::blocking::Client::builder().timeout(std::time::Duration::from_secs(30)).build().map_err(|e| e.to_string())?;
            let app_a = tempfile::tempdir().map_err(|e| e.to_string())?;
            let app_b = tempfile::tempdir().map_err(|e| e.to_string())?;
            let source_a = tempfile::tempdir().map_err(|e| e.to_string())?;
            let source_b = tempfile::tempdir().map_err(|e| e.to_string())?;
            let result: Result<(), String> = (|| {
                cloud_sync::save_config(app_a.path(), config.clone())?;
                cloud_sync::save_config(app_b.path(), config.clone())?;
                cloud_sync::test_connection(app_a.path())?;
                fs::write(source_a.path().join("score_rule.toml"), "version = 1\n").map_err(|e| e.to_string())?;
                fs::write(source_a.path().join("ind.toml"), "period = 20\n").map_err(|e| e.to_string())?;
                let first = cloud_sync::sync(app_a.path(), source_a.path(), BTreeMap::new(), false, |stage| eprintln!("{stage}"))?;
                let received = cloud_sync::sync(app_b.path(), source_b.path(), BTreeMap::new(), false, |stage| eprintln!("{stage}"))?;
                let duplicate_started = std::time::Instant::now();
                let duplicate = cloud_sync::sync(app_a.path(), source_a.path(), BTreeMap::new(), false, |stage| eprintln!("{stage}"))?;
                let duplicate_elapsed_ms = duplicate_started.elapsed().as_millis();
                if first.uploaded != 2 || received.downloaded != 2 || duplicate.uploaded != 0 { return Err("真实同步结果不符合预期".into()); }
                fs::write(source_a.path().join("score_rule.toml"), "version = 2\n").map_err(|e| e.to_string())?;
                fs::write(source_b.path().join("score_rule.toml"), "version = 3\n").map_err(|e| e.to_string())?;
                let changed = cloud_sync::sync(app_a.path(), source_a.path(), BTreeMap::new(), false, |_| {})?;
                let selected = cloud_sync::list_cloud(app_a.path())?.into_iter().find(|item| item.path == "bundles/strategies.zip" && item.is_head).ok_or("云端缺少策略版本")?;
                let conflict = cloud_sync::sync(app_b.path(), source_b.path(), BTreeMap::new(), false, |_| {})?;
                if conflict.conflicts != ["bundles/strategies.zip"] { return Err("真实云端未保留冲突分支".into()); }
                let restored = cloud_sync::restore(app_b.path(), source_b.path(), &selected.path, &selected.version.id, BTreeMap::new(), |_| {})?;
                let versions = cloud_sync::list_cloud(app_b.path())?;
                if versions.iter().filter(|item| item.path == "bundles/strategies.zip" && item.is_head).count() != 1 || !source_b.path().join("strategy_backups").exists() { return Err("真实恢复校验失败".into()); }
                println!("{}", serde_json::json!({"connection":"passed","four_bundle_layout":"passed","upload":"passed","download":"passed","deduplication":"passed","conflict":"passed","restore_and_backup":"passed","uploaded_bytes":first.uploaded_bytes+received.uploaded_bytes+duplicate.uploaded_bytes+changed.uploaded_bytes+conflict.uploaded_bytes+restored.uploaded_bytes,"unchanged_sync_uploaded_bytes":duplicate.uploaded_bytes,"unchanged_sync_elapsed_ms":duplicate_elapsed_ms}));
                Ok(())
            })();
            // 只清理本诊断明确创建的临时目录，且即使失败也尝试清理。
            let response = cleanup.delete(root.clone()).basic_auth(&config.username, Some(&config.password)).send().map_err(|e| e.without_url().to_string())?;
            if response.status().as_u16() == 403 {
                // 坚果云禁止删除顶层同步文件夹，但允许清理其中的测试内容。
                for path in ["objects/", "manifest.json.gz", ".bootstrap/"] {
                    let response = cleanup.delete(root.join(path).map_err(|e| e.to_string())?)
                        .basic_auth(&config.username, Some(&config.password))
                        .send().map_err(|e| e.without_url().to_string())?;
                    if !response.status().is_success() && response.status().as_u16() != 404 {
                        return Err(format!("清理测试内容 {path} 失败：HTTP {}", response.status()));
                    }
                }
                eprintln!("测试内容已清理；坚果云禁止 WebDAV 删除顶层文件夹，空测试文件夹可在网页版删除。");
            } else if !response.status().is_success() && response.status().as_u16() != 404 {
                return Err(format!("临时测试目录清理失败：HTTP {}", response.status()));
            }
            result?;
        }
        _ => return Err("用法：cloud_sync_diagnostics estimate <source目录> <本机应用目录>；或 test <临时 WebDAV 配置文件>（仅 lhtest- 子目录）".into()),
    }
    Ok(())
}
