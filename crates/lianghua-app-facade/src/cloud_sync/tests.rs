use super::*;
use duckdb::Connection;
use lianghua_data::data::open_db_readonly;
use std::{
    io::{BufRead, BufReader},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[test]
fn two_devices_keep_conflicts_deduplicate_and_recover() {
    // 最小 DAV 服务检查 If-Match 并模拟坚果云忽略 If-None-Match，走生产 HTTP、压缩和持久化路径。
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let stopped = Arc::new(AtomicBool::new(false));
    let objects = Arc::new(Mutex::new(BTreeMap::<String, Vec<u8>>::new()));
    let object_puts = Arc::new(Mutex::new(0usize));
    let requests = Arc::new(Mutex::new(0usize));
    let mutations = Arc::new(Mutex::new(0usize));
    let worker_mutations = mutations.clone();
    let worker_requests = requests.clone();
    let worker_stop = stopped.clone();
    let worker_objects = objects.clone();
    let worker_puts = object_puts.clone();
    let worker = std::thread::spawn(move || {
        let mut directories = BTreeSet::new();
        for socket in listener.incoming() {
            if worker_stop.load(Ordering::Relaxed) {
                break;
            }
            *worker_requests.lock().unwrap() += 1;
            let mut socket = socket.unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut first = String::new();
            reader.read_line(&mut first).unwrap();
            let parts: Vec<_> = first.split_whitespace().collect();
            let (method, path) = (parts[0], parts[1]);
            if matches!(method, "PUT" | "DELETE" | "MKCOL") {
                *worker_mutations.lock().unwrap() += 1;
            }
            assert!(
                path.starts_with("/dav/lianghua-rs/")
                    || path.starts_with("/dav/watch-transfer/")
                    || path.starts_with("/dav/strategy-transfer/")
                    || path.starts_with("/dav/settings-transfer/")
            );
            let mut headers = BTreeMap::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                let (key, value) = line.split_once(':').unwrap();
                headers.insert(key.to_lowercase(), value.trim().to_string());
            }
            let size = headers
                .get("content-length")
                .map(|v| v.parse::<usize>().unwrap())
                .unwrap_or(0);
            let mut body = vec![0; size];
            reader.read_exact(&mut body).unwrap();
            let mut data = worker_objects.lock().unwrap();
            let etag = data.get(path).map(|v| format!("\"{}\"", digest(v)));
            let mut response = vec![];
            let status = match method {
                "MKCOL" => {
                    if directories.insert(path.to_string()) {
                        201
                    } else {
                        405
                    }
                }
                "HEAD" => {
                    if data.contains_key(path) {
                        200
                    } else {
                        404
                    }
                }
                "GET" => {
                    if let Some(bytes) = data.get(path) {
                        response = bytes.clone();
                        200
                    } else {
                        404
                    }
                }
                "PUT" => {
                    if headers
                        .get("if-match")
                        .is_some_and(|v| Some(v) != etag.as_ref())
                    {
                        412
                    } else {
                        if path.contains("/objects/") {
                            *worker_puts.lock().unwrap() += 1;
                        }
                        data.insert(path.into(), body);
                        201
                    }
                }
                "DELETE" => {
                    if data.remove(path).is_some() || directories.remove(path) {
                        204
                    } else {
                        404
                    }
                }
                _ => 405,
            };
            let etag_header = if method == "GET" {
                etag.map(|v| format!("ETag: {v}\r\n")).unwrap_or_default()
            } else {
                String::new()
            };
            write!(socket, "HTTP/1.1 {status} Status\r\nContent-Length: {}\r\n{etag_header}Connection: close\r\n\r\n", response.len()).unwrap();
            socket.write_all(&response).unwrap();
        }
    });
    let temp = tempfile::tempdir().unwrap();
    let app_a = temp.path().join("app-a");
    let source_a = temp.path().join("source-a");
    let app_b = temp.path().join("app-b");
    let source_b = temp.path().join("source-b");
    fs::create_dir_all(&source_a).unwrap();
    fs::create_dir_all(&source_b).unwrap();
    let config = WebDavConfig {
        endpoint: format!("http://{address}/dav/"),
        folder: "lianghua-rs".into(),
        username: "test".into(),
        password: "test".into(),
        has_password: true,
    };
    save_config(&app_a, config.clone()).unwrap();
    save_config(&app_b, config.clone()).unwrap();
    fs::write(source_a.join("score_rule.toml"), "version = 1\n").unwrap();
    let first = sync(&app_a, &source_a, BTreeMap::new(), false, |_| {}).unwrap();
    assert_eq!(first.uploaded, 1);
    // 接收端已有自己的内容，只下载时先本地备份，不向云端写入任何请求。
    let app_download = temp.path().join("app-download");
    let source_download = temp.path().join("source-download");
    save_config(&app_download, config.clone()).unwrap();
    fs::create_dir_all(&source_download).unwrap();
    fs::write(source_download.join("score_rule.toml"), "version = 99\n").unwrap();
    let mutations_before = *mutations.lock().unwrap();
    let received = sync(
        &app_download,
        &source_download,
        BTreeMap::new(),
        true,
        |_| {},
    )
    .unwrap();
    assert_eq!(received.uploaded, 0);
    assert_eq!(received.uploaded_bytes, 0);
    assert_eq!(received.downloaded, 1);
    assert_eq!(
        fs::read_to_string(source_download.join("score_rule.toml")).unwrap(),
        "version = 1\n"
    );
    let backup = fs::read_dir(source_download.join("strategy_backups"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert_eq!(
        fs::read_to_string(backup.join("score_rule.toml")).unwrap(),
        "version = 99\n"
    );
    let repeated = sync(
        &app_download,
        &source_download,
        BTreeMap::new(),
        true,
        |_| {},
    )
    .unwrap();
    // 自动备份计入接收后的本地基线；未编辑时既不重传，也不重复接收。
    assert_eq!(repeated.uploaded, 0);
    assert_eq!(repeated.downloaded, 0);
    assert_eq!(*mutations.lock().unwrap(), mutations_before);
    // 损坏的下载不落盘，恢复云端对象后可以重试。
    let first_snapshot = local_snapshots(&app_a, &source_a, &BTreeMap::new()).unwrap();
    let object_path = format!(
        "/dav/lianghua-rs/objects/{}.gz",
        digest(&first_snapshot["bundles/strategies.zip"])
    );
    let original_object = objects
        .lock()
        .unwrap()
        .insert(object_path.clone(), encode(b"damaged").unwrap())
        .unwrap();
    assert!(sync(&app_b, &source_b, BTreeMap::new(), false, |_| {}).is_err());
    assert!(!source_b.join("score_rule.toml").exists());
    objects.lock().unwrap().insert(object_path, original_object);
    let second = sync(&app_b, &source_b, BTreeMap::new(), false, |_| {}).unwrap();
    assert_eq!(second.downloaded, 1);
    let requests_before = *requests.lock().unwrap();
    let unchanged = sync(&app_a, &source_a, BTreeMap::new(), false, |_| {}).unwrap();
    assert_eq!(*requests.lock().unwrap() - requests_before, 5);
    assert_eq!(unchanged.uploaded, 0);
    assert_eq!(*object_puts.lock().unwrap(), 1);
    fs::write(source_a.join("score_rule.toml"), "version = 2\n").unwrap();
    fs::write(source_b.join("score_rule.toml"), "version = 3\n").unwrap();
    sync(&app_a, &source_a, BTreeMap::new(), false, |_| {}).unwrap();
    let cloud_only = sync(
        &app_download,
        &source_download,
        BTreeMap::new(),
        false,
        |_| {},
    )
    .unwrap();
    assert_eq!(cloud_only.uploaded, 0);
    assert_eq!(cloud_only.downloaded, 1);
    assert!(cloud_only.conflicts.is_empty());
    assert_eq!(
        fs::read(source_download.join("score_rule.toml")).unwrap(),
        b"version = 2\n"
    );
    let conflict = sync(&app_b, &source_b, BTreeMap::new(), false, |_| {}).unwrap();
    assert_eq!(conflict.conflicts, ["bundles/strategies.zip"]);
    let mutations_before = *mutations.lock().unwrap();
    let conflicted_download = sync(
        &app_download,
        &source_download,
        BTreeMap::new(),
        true,
        |_| {},
    )
    .unwrap();
    assert_eq!(conflicted_download.conflicts, ["bundles/strategies.zip"]);
    assert_eq!(conflicted_download.downloaded, 0);
    assert_eq!(
        fs::read_to_string(source_download.join("score_rule.toml")).unwrap(),
        "version = 2\n"
    );
    assert_eq!(*mutations.lock().unwrap(), mutations_before);
    assert_eq!(
        fs::read_to_string(source_a.join("score_rule.toml")).unwrap(),
        "version = 2\n"
    );
    assert_eq!(
        fs::read_to_string(source_b.join("score_rule.toml")).unwrap(),
        "version = 3\n"
    );
    let conflict_items = list_cloud(&app_a, &source_a).unwrap();
    assert_eq!(
        conflict_items
            .iter()
            .filter(|item| item.path == "bundles/strategies.zip" && item.is_local_base)
            .count(),
        1
    );
    let selected = conflict_items
        .into_iter()
        .find(|v| v.version.hash == digest(&local_snapshots(&app_a, &source_a, &BTreeMap::new()).unwrap()["bundles/strategies.zip"]))
        .unwrap();
    restore(
        &app_b,
        &source_b,
        &selected.path,
        &selected.version.id,
        BTreeMap::new(),
        |_| {},
    )
    .unwrap();
    assert_eq!(
        fs::read_to_string(source_b.join("score_rule.toml")).unwrap(),
        "version = 2\n"
    );
    let backup = fs::read_dir(source_b.join("strategy_backups"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert_eq!(
        fs::read_to_string(backup.join("score_rule.toml")).unwrap(),
        "version = 3\n"
    );
    assert_eq!(
        list_cloud(&app_b, &source_b)
            .unwrap()
            .iter()
            .filter(|v| v.path == selected.path && v.is_head)
            .count(),
        1
    );
    // 恢复后的自动备份已计入本地基线，再检查云端删除不重传。
    sync(&app_b, &source_b, BTreeMap::new(), false, |_| {}).unwrap();
    // 锁住云端时另一个客户端拒绝修改；过时 ETag 无法覆盖目录。
    let mut lock = dav::Dav::new(&config).unwrap();
    lock.acquire().unwrap();
    assert!(dav::Dav::new(&config).unwrap().acquire().is_err());
    let mut stale = dav::Dav::new(&config).unwrap();
    stale.read_manifest().unwrap();
    lock.release().unwrap();
    assert!(stale.persist().is_err());
    // 删除云端当前版本不会清理本地，也不会重新上传同一基线。
    let head = list_cloud(&app_b, &source_b)
        .unwrap()
        .into_iter()
        .find(|v| v.path == selected.path && v.is_head)
        .unwrap();
    delete_cloud(&app_b, &head.path, &head.version.id).unwrap();
    sync(&app_b, &source_b, BTreeMap::new(), false, |_| {}).unwrap();
    assert!(
        list_cloud(&app_b, &source_b)
            .unwrap()
            .iter()
            .all(|v| v.path != selected.path || !v.is_head)
    );
    assert_eq!(
        fs::read_to_string(source_b.join("score_rule.toml")).unwrap(),
        "version = 2\n"
    );
    // 配置、自选和浏览习惯是三个独立包，修改一个不重传其他包。
    fs::write(source_a.join("ind.toml"), "period = 20\n").unwrap();
    fs::write(source_a.join("chart_indicators.toml"), "width = 1\n").unwrap();
    fs::write(app_a.join("watch_observe.json"), "{\"symbols\":[]}").unwrap();
    let preferences = BTreeMap::from([
        ("lh_default_board_filter_v1".into(), "all".into()),
        ("lh_intraday_monitor_watchlist_v1".into(), "[]".into()),
    ]);
    let before = *object_puts.lock().unwrap();
    sync(&app_a, &source_a, preferences.clone(), false, |_| {}).unwrap();
    assert_eq!(*object_puts.lock().unwrap() - before, 3);
    let heads = list_cloud(&app_a, &source_a).unwrap();
    for path in [
        "bundles/indicator_config.zip",
        "bundles/watchlists.zip",
        "bundles/browser_settings.zip",
    ] {
        assert!(heads.iter().any(|item| item.path == path && item.is_head));
    }
    let app_c = temp.path().join("app-c");
    let source_c = temp.path().join("source-c");
    save_config(&app_c, config.clone()).unwrap();
    let received = sync(&app_c, &source_c, BTreeMap::new(), false, |_| {}).unwrap();
    assert_eq!(received.settings, preferences);
    assert_eq!(
        fs::read_to_string(source_c.join("ind.toml")).unwrap(),
        "period = 20\n"
    );
    assert!(app_c.join("watch_observe.json").exists());
    fs::write(source_a.join("ind.toml"), "period = 21\n").unwrap();
    let before = *object_puts.lock().unwrap();
    sync(&app_a, &source_a, preferences, false, |_| {}).unwrap();
    assert_eq!(*object_puts.lock().unwrap() - before, 1);
    // 管理同步不接收或上传自选；自选同步不会读取/改写策略与榜单。
    let app_scoped = temp.path().join("app-scoped");
    let source_scoped = temp.path().join("source-scoped");
    save_config(&app_scoped, config.clone()).unwrap();
    fs::create_dir_all(&source_scoped).unwrap();
    fs::write(app_scoped.join("watch_observe.json"), "[]").unwrap();
    sync_scoped(
        &app_scoped,
        &source_scoped,
        BTreeMap::new(),
        true,
        SyncScope::Management,
        |_| {},
    )
    .unwrap();
    assert_eq!(
        fs::read_to_string(app_scoped.join("watch_observe.json")).unwrap(),
        "[]"
    );
    fs::write(source_scoped.join("score_rule.toml"), "local only").unwrap();
    let before = *object_puts.lock().unwrap();
    let received = sync_scoped(
        &app_scoped,
        &source_scoped,
        BTreeMap::new(),
        true,
        SyncScope::Watchlists,
        |_| {},
    )
    .unwrap();
    assert_eq!(received.downloaded, 1);
    assert_eq!(
        fs::read(source_scoped.join("score_rule.toml")).unwrap(),
        b"local only"
    );
    assert_eq!(*object_puts.lock().unwrap(), before);
    let watch_before = fs::read(app_a.join("watch_observe.json")).unwrap();
    fs::write(app_a.join("watch_observe.json"), "[]").unwrap();
    let before = *object_puts.lock().unwrap();
    sync_scoped(
        &app_a,
        &source_a,
        BTreeMap::new(),
        false,
        SyncScope::Management,
        |_| {},
    )
    .unwrap();
    assert_eq!(*object_puts.lock().unwrap(), before);
    fs::write(app_a.join("watch_observe.json"), watch_before).unwrap();
    // 自选弹窗的三个方向：上传不接收，下载不写云端，合并去重并收敛冲突。
    let transfer_a = temp.path().join("transfer-a");
    let transfer_b = temp.path().join("transfer-b");
    let mut transfer_config = config.clone();
    transfer_config.folder = "watch-transfer".into();
    save_config(&transfer_a, transfer_config.clone()).unwrap();
    save_config(&transfer_b, transfer_config).unwrap();
    let first_rows = br#"[{"tsCode":"000001.SZ","name":"A","tag":"","concept":""}]"#;
    let second_rows = br#"[{"tsCode":"600000.SH","name":"B","tag":"","concept":""}]"#;
    fs::write(transfer_a.join("watch_observe.json"), "[]").unwrap();
    fs::write(transfer_b.join("watch_observe.json"), "[]").unwrap();
    let sent = transfer_watchlists(
        &transfer_a,
        &source_a,
        BTreeMap::new(),
        Some(serde_json::from_slice(first_rows).unwrap()),
        WatchlistTransferMode::Upload,
        |_| {},
    )
    .unwrap();
    assert_eq!(sent.result.uploaded, 1);
    assert_eq!(sent.result.downloaded, 0);
    assert!(sent.last_modified.is_some());
    assert_eq!(
        fs::read(transfer_a.join("watch_observe.json")).unwrap(),
        b"[]"
    );
    let before = *mutations.lock().unwrap();
    let received = transfer_watchlists(
        &transfer_b,
        &source_b,
        BTreeMap::new(),
        Some(serde_json::from_slice(second_rows).unwrap()),
        WatchlistTransferMode::Download,
        |_| {},
    )
    .unwrap();
    assert_eq!(*mutations.lock().unwrap(), before);
    assert_eq!(received.result.uploaded_bytes, 0);
    assert_eq!(received.last_modified, sent.last_modified);
    let received_rows: Vec<lianghua_app_market::watch_observe::WatchObserveStoredRow> =
        read_json(&transfer_b.join("watch_observe.json")).unwrap();
    assert_eq!(received_rows.len(), 1);
    assert_eq!(received_rows[0].ts_code, "000001.SZ");
    let backup = fs::read_dir(source_b.join("cloud_sync_backups"))
        .unwrap()
        .map(|entry| entry.unwrap().path().join("watchlists.zip"))
        .find(|path| path.exists())
        .unwrap();
    let saved = bundles::unpack("bundles/watchlists.zip", &fs::read(backup).unwrap()).unwrap();
    let saved_rows: Vec<lianghua_app_market::watch_observe::WatchObserveStoredRow> =
        serde_json::from_slice(&saved["app/watch_observe.json"]).unwrap();
    assert_eq!(saved_rows.len(), 1);
    assert_eq!(saved_rows[0].ts_code, "600000.SH");
    fs::write(transfer_b.join("watch_observe.json"), second_rows).unwrap();
    let merged = transfer_watchlists(
        &transfer_b,
        &source_b,
        BTreeMap::new(),
        None,
        WatchlistTransferMode::Merge,
        |_| {},
    )
    .unwrap();
    assert_eq!(merged.result.uploaded, 1);
    let merged_rows: Vec<lianghua_app_market::watch_observe::WatchObserveStoredRow> =
        read_json(&transfer_b.join("watch_observe.json")).unwrap();
    assert_eq!(merged_rows.len(), 2);
    let unchanged = transfer_watchlists(
        &transfer_b,
        &source_b,
        merged.result.settings.clone(),
        None,
        WatchlistTransferMode::Merge,
        |_| {},
    )
    .unwrap();
    assert_eq!(unchanged.result.uploaded, 0);
    assert_eq!(unchanged.last_modified, merged.last_modified);
    // 旧设备从较早基线修改，产生两个分支；合并必须保留全部分支股票。
    fs::write(
        transfer_a.join("watch_observe.json"),
        br#"[{"tsCode":"300001.SZ","name":"C","tag":"","concept":""}]"#,
    )
    .unwrap();
    sync_scoped(
        &transfer_a,
        &source_a,
        BTreeMap::new(),
        false,
        SyncScope::Watchlists,
        |_| {},
    )
    .unwrap();
    assert!(
        transfer_watchlists(
            &transfer_a,
            &source_a,
            BTreeMap::new(),
            None,
            WatchlistTransferMode::Download,
            |_| {}
        )
        .err()
        .unwrap()
        .contains("多个版本")
    );
    transfer_watchlists(
        &transfer_b,
        &source_b,
        merged.result.settings,
        None,
        WatchlistTransferMode::Merge,
        |_| {},
    )
    .unwrap();
    let merged_rows: Vec<lianghua_app_market::watch_observe::WatchObserveStoredRow> =
        read_json(&transfer_b.join("watch_observe.json")).unwrap();
    assert_eq!(merged_rows.len(), 3);
    assert_eq!(
        list_cloud(&transfer_a, &source_a)
            .unwrap()
            .iter()
            .filter(|item| item.path == "bundles/watchlists.zip" && item.is_head)
            .count(),
        1
    );
    // 明确上传空名单可传播删除，仍保留云端历史版本。
    fs::write(transfer_b.join("watch_observe.json"), "[]").unwrap();
    transfer_watchlists(
        &transfer_b,
        &source_b,
        BTreeMap::new(),
        None,
        WatchlistTransferMode::Upload,
        |_| {},
    )
    .unwrap();
    transfer_watchlists(
        &transfer_a,
        &source_a,
        BTreeMap::new(),
        None,
        WatchlistTransferMode::Download,
        |_| {},
    )
    .unwrap();
    assert_eq!(
        fs::read(transfer_a.join("watch_observe.json")).unwrap(),
        b"[]"
    );
    // 策略复用同一传输流程：上传不改本地、下载只读云端、合并保留双方当前版本。
    let strategy_a = temp.path().join("strategy-a");
    let strategy_b = temp.path().join("strategy-b");
    fs::create_dir_all(&strategy_a).unwrap();
    fs::create_dir_all(&strategy_b).unwrap();
    let mut strategy_config = config.clone();
    strategy_config.folder = "strategy-transfer".into();
    save_config(&transfer_a, strategy_config.clone()).unwrap();
    save_config(&transfer_b, strategy_config).unwrap();
    fs::write(strategy_a.join("score_rule.toml"), "rules = [1]").unwrap();
    fs::write(strategy_b.join("score_rule.toml"), "rules = [2]").unwrap();
    fs::write(strategy_b.join("ind.toml"), "local indicator").unwrap();
    let sent = transfer_strategies(
        &transfer_a,
        &strategy_a,
        WatchlistTransferMode::Upload,
        |_| {},
    )
    .unwrap();
    assert_eq!(sent.result.uploaded, 1);
    assert_eq!(
        fs::read(strategy_a.join("score_rule.toml")).unwrap(),
        b"rules = [1]"
    );
    let before = *mutations.lock().unwrap();
    let received = transfer_strategies(
        &transfer_b,
        &strategy_b,
        WatchlistTransferMode::Download,
        |_| {},
    )
    .unwrap();
    assert_eq!(received.result.downloaded, 1);
    assert_eq!(*mutations.lock().unwrap(), before);
    assert_eq!(
        fs::read(strategy_b.join("score_rule.toml")).unwrap(),
        b"rules = [1]"
    );
    assert_eq!(
        fs::read(strategy_b.join("ind.toml")).unwrap(),
        b"local indicator"
    );
    assert!(strategy_b.join("strategy_backups").is_dir());
    fs::write(strategy_b.join("score_rule.toml"), "rules = [3]").unwrap();
    let merged = transfer_strategies(
        &transfer_b,
        &strategy_b,
        WatchlistTransferMode::Merge,
        |_| {},
    )
    .unwrap();
    assert_eq!(merged.result.uploaded, 1);
    assert_eq!(
        fs::read(strategy_b.join("score_rule.toml")).unwrap(),
        b"rules = [3]"
    );
    let kept = strategy_b
        .join("strategy_backups")
        .join(format!("cloud-{}", digest(b"rules = [1]")));
    assert_eq!(
        fs::read(kept.join("score_rule.toml")).unwrap(),
        b"rules = [1]"
    );
    assert!(kept.join("meta.json").exists());
    let unchanged = transfer_strategies(
        &transfer_b,
        &strategy_b,
        WatchlistTransferMode::Merge,
        |_| {},
    )
    .unwrap();
    assert_eq!(unchanged.result.skipped, 1);
    // 预览只读云端；预览后发生本地/远端编辑都拒绝旧选择，最终可分别选两份当前策略。
    fs::write(strategy_a.join("chip_change_rule.toml"), "chip = 1").unwrap();
    transfer_strategies(
        &transfer_a,
        &strategy_a,
        WatchlistTransferMode::Upload,
        |_| {},
    )
    .unwrap();
    fs::write(strategy_b.join("chip_change_rule.toml"), "chip = 2").unwrap();
    let before = *mutations.lock().unwrap();
    let reviewed = preview(
        &transfer_b,
        &strategy_b,
        "bundles/strategies.zip",
        None,
        &BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(*mutations.lock().unwrap(), before);
    let reviewed = serde_json::to_value(reviewed).unwrap();
    let heads: Vec<String> = serde_json::from_value(reviewed["heads"].clone()).unwrap();
    let choices = BTreeMap::from([
        ("source/score_rule.toml".into(), "local".into()),
        ("source/chip_change_rule.toml".into(), heads[0].clone()),
    ]);
    fs::write(strategy_b.join("score_rule.toml"), "rules = [4]").unwrap();
    assert!(
        resolve_strategies(
            &transfer_b,
            &strategy_b,
            StrategyResolution {
                local_hash: serde_json::from_value(reviewed["localHash"].clone()).unwrap(),
                heads: heads.clone(),
                choices: choices.clone(),
            },
            |_| {}
        )
        .err()
        .unwrap()
        .contains("本地策略")
    );
    fs::write(strategy_b.join("score_rule.toml"), "rules = [3]").unwrap();
    fs::write(strategy_a.join("chip_change_rule.toml"), "chip = 3").unwrap();
    transfer_strategies(
        &transfer_a,
        &strategy_a,
        WatchlistTransferMode::Upload,
        |_| {},
    )
    .unwrap();
    assert!(
        resolve_strategies(
            &transfer_b,
            &strategy_b,
            StrategyResolution {
                local_hash: serde_json::from_value(reviewed["localHash"].clone()).unwrap(),
                heads,
                choices,
            },
            |_| {}
        )
        .err()
        .unwrap()
        .contains("云端策略")
    );
    let reviewed = serde_json::to_value(
        preview(
            &transfer_b,
            &strategy_b,
            "bundles/strategies.zip",
            None,
            &BTreeMap::new(),
        )
        .unwrap(),
    )
    .unwrap();
    let heads: Vec<String> = serde_json::from_value(reviewed["heads"].clone()).unwrap();
    resolve_strategies(
        &transfer_b,
        &strategy_b,
        StrategyResolution {
            local_hash: serde_json::from_value(reviewed["localHash"].clone()).unwrap(),
            heads: heads.clone(),
            choices: BTreeMap::from([
                ("source/score_rule.toml".into(), "local".into()),
                ("source/chip_change_rule.toml".into(), heads[0].clone()),
            ]),
        },
        |_| {},
    )
    .unwrap();
    assert_eq!(
        fs::read(strategy_b.join("score_rule.toml")).unwrap(),
        b"rules = [3]"
    );
    assert_eq!(
        fs::read(strategy_b.join("chip_change_rule.toml")).unwrap(),
        b"chip = 3"
    );
    let files = local_files(&transfer_b, &strategy_b, &BTreeMap::new()).unwrap();
    assert!(files.values().any(|bytes| bytes.as_slice() == b"chip = 2"));

    // 模拟前端设置写入失败：旧值不能上传成新版本，部分写入也必须等待人工处理。
    let settings_a = temp.path().join("settings-a");
    let settings_b = temp.path().join("settings-b");
    fs::create_dir_all(&settings_a).unwrap();
    fs::create_dir_all(&settings_b).unwrap();
    let mut settings_config = config.clone();
    settings_config.folder = "settings-transfer".into();
    save_config(&transfer_a, settings_config.clone()).unwrap();
    save_config(&transfer_b, settings_config).unwrap();
    let new_settings = BTreeMap::from([("am_refresh_seconds".into(), "1".into())]);
    let old_settings = BTreeMap::from([("am_refresh_seconds".into(), "7".into())]);
    sync_scoped(
        &transfer_a,
        &settings_a,
        new_settings.clone(),
        false,
        SyncScope::Management,
        |_| {},
    )
    .unwrap();
    sync_scoped(
        &transfer_b,
        &settings_b,
        old_settings.clone(),
        true,
        SyncScope::Management,
        |_| {},
    )
    .unwrap();
    let retried = sync_scoped(
        &transfer_b,
        &settings_b,
        old_settings,
        false,
        SyncScope::Management,
        |_| {},
    )
    .unwrap();
    assert_eq!(retried.uploaded, 0);
    assert_eq!(retried.settings, new_settings);
    let partial = sync_scoped(
        &transfer_b,
        &settings_b,
        BTreeMap::from([("am_refresh_seconds".into(), "9".into())]),
        false,
        SyncScope::Management,
        |_| {},
    )
    .unwrap();
    assert_eq!(partial.uploaded, 0);
    assert_eq!(partial.conflicts, ["bundles/browser_settings.zip"]);
    let confirmed = sync_scoped(
        &transfer_b,
        &settings_b,
        new_settings,
        false,
        SyncScope::Management,
        |_| {},
    )
    .unwrap();
    assert_eq!(confirmed.uploaded, 0);
    assert_eq!(confirmed.downloaded, 0);
    stopped.store(true, Ordering::Relaxed);
    let _ = TcpStream::connect(address);
    worker.join().unwrap();
}

#[test]
fn database_snapshot_keeps_five_dates_and_preserves_other_local_history() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&target).unwrap();
    lianghua_data::data::scoring_store::init_result_db(&source.join("scoring_result.db")).unwrap();
    {
        let conn = Connection::open(source.join("scoring_result.db")).unwrap();
        for day in 1..=7 {
            conn.execute(
                "INSERT INTO score_summary VALUES ('000001.SZ', ?, 1, 1)",
                [format!("202610{day:02}")],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO rule_details VALUES ('000001.SZ', ?, 'rule', 1)",
                [format!("202610{day:02}")],
            )
            .unwrap();
        }
    }
    let exported = database::export(&source).unwrap();
    assert_eq!(exported.len(), 1);
    let day = "ranking/scoring_result.zip";
    assert_eq!(exported[day], database::export(&source).unwrap()[day]);
    database::import(&target, day, &exported[day]).unwrap();
    {
        let conn = Connection::open(target.join("scoring_result.db")).unwrap();
        conn.execute(
            "INSERT INTO score_summary VALUES ('000002.SZ', '20260901', 2, 1)",
            [],
        )
        .unwrap();
    }
    database::import(&target, day, &exported[day]).unwrap();
    let conn = open_db_readonly(target.join("scoring_result.db")).unwrap();
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM score_summary", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        6
    );
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM score_summary WHERE trade_date IN ('20261001', '20261002')",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    drop(conn);
    assert!(database::import(&target, "ranking/20261006.zip", &exported[day]).is_err());
    let legacy = database::export_day(&source, "ranking/20261007.zip")
        .unwrap()
        .unwrap();
    database::import(&target, "ranking/20261007.zip", &legacy).unwrap();
    let mut manifest = Manifest::default();
    for day in 1..=7 {
        for kind in ["ranking", "similarity"] {
            manifest.add(
                &format!("{kind}/202610{day:02}.zip"),
                &digest(&[day]),
                vec![],
                1,
                1,
            );
        }
    }
    manifest.prune();
    assert_eq!(
        manifest
            .entries
            .keys()
            .filter(|p| p.starts_with("ranking/"))
            .count(),
        5
    );
    assert_eq!(
        manifest
            .entries
            .keys()
            .filter(|p| p.starts_with("similarity/"))
            .count(),
        1
    );
}

#[test]
#[cfg(unix)]
fn local_sync_paths_allow_system_aliases_and_reject_links_inside_roots() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let real = temp.path().join("real");
    let alias = temp.path().join("alias");
    fs::create_dir_all(real.join("app")).unwrap();
    fs::create_dir_all(real.join("source")).unwrap();
    symlink(&real, &alias).unwrap();
    let app = alias.join("app");
    let source = alias.join("source");
    assert_eq!(
        target_path(&app, &source, "source/strategy_backups/new/score_rule.toml").unwrap(),
        source.join("strategy_backups/new/score_rule.toml")
    );
    assert_eq!(
        target_path(&app, &source, "app/watch_observe.json").unwrap(),
        app.join("watch_observe.json")
    );

    // 根目录内部的链接即使目标尚不存在，也不能用于恢复。
    symlink(temp.path().join("missing"), source.join("score_rule.toml")).unwrap();
    assert!(target_path(&app, &source, "source/score_rule.toml").is_err());
    symlink(&real, source.join("strategy_backups")).unwrap();
    assert!(target_path(&app, &source, "source/strategy_backups/new/meta.json").is_err());
    symlink(temp.path().join("missing"), app.join("watch_observe.json")).unwrap();
    assert!(target_path(&app, &source, "app/watch_observe.json").is_err());
    let linked_root = alias.join("linked-source");
    symlink(real.join("source"), &linked_root).unwrap();
    assert!(target_path(&app, &linked_root, "source/ind.toml").is_err());
}

#[test]
fn rejects_unsafe_paths_credentials_and_corrupt_payloads() {
    for path in [
        "source/../secret.json",
        "/etc/passwd",
        "source/cloud_sync_backups/../../secret.json",
        "settings/lh_data_download_draft_v1",
        "app/cloud-webdav.json",
    ] {
        assert!(validate_path(path).is_err());
    }
    let files = BTreeMap::from([
        ("source/score_rule.toml".into(), b"[invalid".to_vec()),
        (
            "source/strategy_backups/broken.toml".into(),
            b"version = 9\n".to_vec(),
        ),
    ]);
    let bundle = bundles::snapshots(&files).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("score_rule.toml"), "version = 1\n").unwrap();
    assert!(
        apply(
            temp.path(),
            &source,
            "bundles/strategies.zip",
            &bundle["bundles/strategies.zip"],
            &mut SyncResult::default()
        )
        .is_err()
    );
    assert_eq!(
        fs::read_to_string(source.join("score_rule.toml")).unwrap(),
        "version = 1\n"
    );
    assert!(
        bundles::unpack(
            "bundles/indicator_config.zip",
            &bundle["bundles/strategies.zip"]
        )
        .is_err()
    );
    fs::write(source.join("chip_change_rule.toml"), "[invalid").unwrap();
    apply(
        temp.path(),
        &source,
        "source/chip_change_rule.toml",
        b"version = 1",
        &mut SyncResult::default(),
    )
    .unwrap();
    assert_eq!(
        fs::read(source.join("chip_change_rule.toml")).unwrap(),
        b"version = 1"
    );
    let snapshots =
        bundles::snapshots(&local_files(temp.path(), &source, &BTreeMap::new()).unwrap()).unwrap();
    let files = bundles::unpack(
        "bundles/strategies.zip",
        &snapshots["bundles/strategies.zip"],
    )
    .unwrap();
    assert!(files.values().any(|bytes| bytes.as_slice() == b"[invalid"));
    assert!(decode(b"corrupt", 10).is_err());
    assert!(decode(&encode(b"too large").unwrap(), 2).is_err());
    let config = WebDavConfig {
        endpoint: "https://dav.jianguoyun.com/dav/".into(),
        folder: "..".into(),
        username: "test".into(),
        password: "test".into(),
        has_password: true,
    };
    assert!(config.root_url().is_err());
}
