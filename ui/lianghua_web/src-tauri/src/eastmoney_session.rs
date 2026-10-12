use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant},
};
use tauri::Manager;

#[tauri::command]
pub async fn open_eastmoney_verification(app: tauri::AppHandle) -> Result<(), String> {
    if let Some(window) = app.get_webview_window("eastmoney-verification") {
        #[cfg(not(target_os = "android"))]
        {
            window.show().map_err(|e| e.to_string())?;
            return window.set_focus().map_err(|e| e.to_string());
        }
        #[cfg(target_os = "android")]
        {
            let (tx, rx) = mpsc::channel();
            window
                .with_webview(move |webview| {
                    webview.jni_handle().exec(move |env, activity, _| {
                        let result = env
                            .call_method(activity, "showVerificationWindow", "()V", &[])
                            .map(|_| ())
                            .map_err(|e| e.to_string());
                        let _ = tx.send(result);
                    });
                })
                .map_err(|e| e.to_string())?;
            return tauri::async_runtime::spawn_blocking(move || {
                rx.recv_timeout(Duration::from_secs(6))
                    .map_err(|_| "打开东财验证页面超时".to_string())?
            })
            .await
            .map_err(|e| e.to_string())?;
        }
    }
    let builder = tauri::WebviewWindowBuilder::new(
        &app,
        "eastmoney-verification",
        tauri::WebviewUrl::External("https://quote.eastmoney.com/sz000001.html".parse().unwrap()),
    )
    .title("东方财富验证 · 完成后返回实时监控")
    .inner_size(800.0, 650.0);
    #[cfg(target_os = "android")]
    let builder = builder.activity_name("EastmoneyVerificationActivity");
    builder
        .build()
        .map_err(|e| format!("打开东财验证页面失败: {e}"))?;
    Ok(())
}

fn fetch_in_browser(app: &tauri::AppHandle, ts_code: &str) -> Result<String, String> {
    let symbol = lianghua_provider::crawler::ts_code_to_tencent_code(ts_code)?;
    let window = app
        .get_webview_window("eastmoney-verification")
        .ok_or("东财验证窗口已关闭，请重新打开并验证")?;
    if window.url().map_err(|e| e.to_string())?.host_str() != Some("quote.eastmoney.com") {
        return Err("请先完成东财验证并返回股票行情页面".to_string());
    }
    static REQUEST_ID: AtomicU64 = AtomicU64::new(0);
    let key = format!(
        "__lianghua_em_{}",
        REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    );
    let secid = format!(
        "{}.{}",
        if symbol.starts_with("sh") { 1 } else { 0 },
        &symbol[2..]
    );
    let url = format!(
        "https://push2.eastmoney.com/api/qt/stock/trends2/sse?secid={secid}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f17&fields2=f51,f52,f53,f54,f55,f56,f57,f58&ndays=1&iscr=1&iscca=0&mpi=1000&ut=fa5fd1943c7b386f172d6893dbfba10b"
    );
    // 沿用官网当前的 EventSource，收到完整快照即关闭，不订阅后续增量。
    window.eval(format!(r#"
        (() => {{
            const key = {key:?};
            const stream = new EventSource({url:?}, {{withCredentials: true}});
            const results = window.__lianghuaEmResults ||= Object.create(null);
            const finish = value => {{
                clearTimeout(timer);
                stream.close();
                results[key] = value;
                setTimeout(() => delete results[key], 30000);
            }};
            const timer = setTimeout(() => finish({{error: '东财浏览器请求超时，请检查验证是否失效'}}), 12000);
            stream.onmessage = event => {{
                try {{
                    const data = JSON.parse(event.data);
                    if (data.full !== 0 && (data.rc !== 0 || Array.isArray(data.data?.trends))) {{
                        finish({{body: JSON.stringify(data)}});
                    }}
                }} catch (_) {{
                    finish({{error: '东财浏览器返回了无效的分时数据'}});
                }}
            }};
            stream.onerror = () => finish({{error: '东财浏览器请求失败，请检查验证页面'}});
        }})();
    "#)).map_err(|e| format!("启动东财浏览器请求失败: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("东财浏览器响应超时，请保持验证窗口打开".to_string());
        }
        let (tx, rx) = mpsc::channel();
        window
            .eval_with_callback(
                format!(
                    r#"
            (() => {{
                const results = window.__lianghuaEmResults;
                const value = results?.[{key:?}] ?? null;
                if (value !== null) delete results[{key:?}];
                return value;
            }})()
        "#
                ),
                move |value| {
                    let _ = tx.send(value);
                },
            )
            .map_err(|e| format!("读取东财浏览器响应失败: {e}"))?;
        let raw = rx
            .recv_timeout(remaining.min(Duration::from_secs(1)))
            .map_err(|_| "东财验证窗口未响应，请保持窗口打开".to_string())?;
        let response: serde_json::Value =
            serde_json::from_str(&raw).map_err(|e| format!("东财浏览器响应格式错误: {e}"))?;
        if let Some(body) = response["body"].as_str() {
            return Ok(body.to_string());
        }
        if let Some(error) = response["error"].as_str() {
            return Err(error.to_string());
        }
        if !response.is_null() {
            return Err("东财浏览器响应缺少数据".to_string());
        }
        std::thread::sleep(Duration::from_millis(200).min(remaining));
    }
}

#[tauri::command]
pub async fn use_eastmoney_verified_session(app: tauri::AppHandle) -> Result<(), String> {
    tauri::async_runtime::spawn_blocking(move || {
        let raw = fetch_in_browser(&app, "000001.SZ")?;
        // 只检查会话能取得可解析分时；实际量比仍使用每股本地五日均量。
        lianghua_provider::crawler::volume_ratio::parse_eastmoney_volume_ratio(
            &raw,
            "000001.SZ",
            100000.0,
        )?;
        lianghua_app_market::volume_ratio::set_eastmoney_browser_fetch(Some(Arc::new(
            move |ts_code| fetch_in_browser(&app, ts_code),
        )))
    })
    .await
    .map_err(|e| format!("检查东财验证会话失败: {e}"))?
}

#[tauri::command]
pub fn stop_eastmoney_verified_session() -> Result<(), String> {
    lianghua_app_market::volume_ratio::set_eastmoney_browser_fetch(None)
}
