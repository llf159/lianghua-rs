use lianghua_provider::crawler::volume_ratio::{
    parse_eastmoney_volume_ratio, parse_volume_ratio_from_source, volume_ratio_request,
    IntradayVolumeSource,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tauri::Manager;

#[derive(Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LoginSource {
    Eastmoney,
    Xueqiu,
    Tonghuashun,
}

struct SessionConfig {
    index: usize,
    source: &'static str,
    label: &'static str,
    title: &'static str,
    domain: &'static str,
    home: &'static str,
    #[cfg(target_os = "android")]
    activity: &'static str,
}

impl LoginSource {
    fn config(self) -> SessionConfig {
        match self {
            Self::Eastmoney => SessionConfig {
                index: 0,
                source: "eastmoney",
                label: "eastmoney-verification",
                title: "东方财富登录/验证 · 完成后返回监控",
                domain: "eastmoney.com",
                home: "https://quote.eastmoney.com/sz000001.html",
                #[cfg(target_os = "android")]
                activity: "EastmoneyVerificationActivity",
            },
            Self::Xueqiu => SessionConfig {
                index: 1,
                source: "xueqiu",
                label: "xueqiu-verification",
                title: "雪球登录 · 完成后返回监控",
                domain: "xueqiu.com",
                home: "https://xueqiu.com/hq",
                #[cfg(target_os = "android")]
                activity: "XueqiuVerificationActivity",
            },
            Self::Tonghuashun => SessionConfig {
                index: 2,
                source: "tonghuashun",
                label: "tonghuashun-verification",
                title: "同花顺登录 · 完成后返回监控",
                domain: "10jqka.com.cn",
                home: "https://stockpage.10jqka.com.cn/600000/",
                #[cfg(target_os = "android")]
                activity: "TonghuashunVerificationActivity",
            },
        }
    }
}

impl SessionConfig {
    fn accepts(&self, url: &tauri::Url) -> bool {
        url.scheme() == "https"
            && url.host_str().is_some_and(|host| {
                host == self.domain
                    || host
                        .strip_suffix(self.domain)
                        .is_some_and(|prefix| prefix.ends_with('.'))
            })
    }
}

static SESSION_GENERATIONS: Mutex<[u64; 3]> = Mutex::new([0; 3]);
#[derive(Clone)]
struct SessionTransport {
    kind: &'static str,
    reason: Option<String>,
}
static SESSION_TRANSPORTS: Mutex<[Option<SessionTransport>; 3]> = Mutex::new([None, None, None]);

struct NativeSession {
    http: reqwest::blocking::Client,
    source: LoginSource,
}

impl NativeSession {
    fn fetch(&self, ts_code: &str) -> Result<String, String> {
        let request = match self.source {
            LoginSource::Eastmoney => {
                lianghua_provider::crawler::intraday::eastmoney_intraday_request(ts_code, false)?
            }
            LoginSource::Xueqiu => volume_ratio_request(IntradayVolumeSource::Xueqiu, ts_code)?,
            LoginSource::Tonghuashun => {
                volume_ratio_request(IntradayVolumeSource::Tonghuashun, ts_code)?
            }
        };
        let response = request
            .http(&self.http)?
            .send()
            .map_err(|e| format!("登录会话 HTTP 请求失败: {e:?}"))?;
        if !response.status().is_success() {
            return Err(format!("登录会话 HTTP状态={};", response.status().as_u16()));
        }
        response
            .text()
            .map_err(|e| format!("读取登录会话响应失败: {e}"))
    }
}

fn validate_response(source: LoginSource, raw: &str, ts_code: &str) -> Result<(), String> {
    match source {
        LoginSource::Eastmoney => {
            parse_eastmoney_volume_ratio(raw, ts_code, 100000.0)?;
        }
        LoginSource::Xueqiu | LoginSource::Tonghuashun => {
            let data = serde_json::from_str(raw).map_err(|e| format!("分时 JSON 无效: {e}"))?;
            parse_volume_ratio_from_source(
                &data,
                match source {
                    LoginSource::Xueqiu => IntradayVolumeSource::Xueqiu,
                    _ => IntradayVolumeSource::Tonghuashun,
                },
                ts_code,
                100000.0,
            )?;
        }
    }
    Ok(())
}

#[derive(serde::Serialize)]
pub struct DataSourceSessionStatus {
    source: &'static str,
    window_open: bool,
    page_ready: bool,
    enabled: bool,
    transport: Option<&'static str>,
    transport_reason: Option<String>,
}

#[tauri::command]
pub fn data_source_sessions(app: tauri::AppHandle) -> Result<Vec<DataSourceSessionStatus>, String> {
    [
        LoginSource::Eastmoney,
        LoginSource::Xueqiu,
        LoginSource::Tonghuashun,
    ]
    .into_iter()
    .map(|source| {
        let config = source.config();
        let window = app.get_webview_window(config.label);
        let transport = SESSION_TRANSPORTS
            .lock()
            .map_err(|_| "登录传输状态锁损坏")?[config.index]
            .clone();
        Ok(DataSourceSessionStatus {
            source: config.source,
            window_open: window.is_some(),
            // 此状态只表示位于来源网站，不冒充已经登录的判断。
            page_ready: window
                .as_ref()
                .and_then(|w| w.url().ok())
                .is_some_and(|u| config.accepts(&u)),
            enabled: lianghua_app_market::volume_ratio::volume_ratio_session_fetch(config.source)?
                .is_some(),
            transport: transport.as_ref().map(|transport| transport.kind),
            transport_reason: transport.and_then(|transport| transport.reason),
        })
    })
    .collect()
}

#[tauri::command]
pub async fn open_data_source_login(
    app: tauri::AppHandle,
    source: LoginSource,
) -> Result<(), String> {
    let config = source.config();
    if let Some(window) = app.get_webview_window(config.label) {
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
                    .map_err(|_| "打开登录页面超时".to_string())?
            })
            .await
            .map_err(|e| e.to_string())?;
        }
    }
    let builder = tauri::WebviewWindowBuilder::new(
        &app,
        config.label,
        tauri::WebviewUrl::External(
            config
                .home
                .parse()
                .map_err(|e| format!("登录地址错误: {e}"))?,
        ),
    )
    .title(config.title)
    .inner_size(800.0, 650.0);
    #[cfg(target_os = "android")]
    let builder = builder.activity_name(config.activity);
    let window = builder
        .build()
        .map_err(|e| format!("打开登录页面失败: {e}"))?;
    #[cfg(not(target_os = "android"))]
    {
        let retained = window.clone();
        window.on_window_event(move |event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                match retained.hide() {
                    Ok(()) => api.prevent_close(),
                    Err(error) => eprintln!("隐藏登录窗口失败: {error}"),
                }
            }
        });
    }
    Ok(())
}

fn fetch_in_browser(
    app: &tauri::AppHandle,
    source: LoginSource,
    ts_code: &str,
) -> Result<String, String> {
    let config = source.config();
    let window = app
        .get_webview_window(config.label)
        .ok_or("登录窗口已关闭，请重新打开登录页面")?;
    if !config.accepts(&window.url().map_err(|e| e.to_string())?) {
        return Err("请先完成登录，并返回对应数据源网站页面".into());
    }
    // 请求地址与字段只由后端来源适配器生成，不允许远程页面或前端提交任意请求。
    let request = match source {
        LoginSource::Eastmoney => {
            lianghua_provider::crawler::intraday::eastmoney_intraday_request(ts_code, true)?
        }
        LoginSource::Xueqiu => volume_ratio_request(IntradayVolumeSource::Xueqiu, ts_code)?,
        LoginSource::Tonghuashun => {
            volume_ratio_request(IntradayVolumeSource::Tonghuashun, ts_code)?
        }
    };
    static REQUEST_ID: AtomicU64 = AtomicU64::new(0);
    let key = format!(
        "__lianghua_request_{}",
        REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    );
    let request = serde_json::to_string(&request).map_err(|e| e.to_string())?;
    let key_json = serde_json::to_string(&key).map_err(|e| e.to_string())?;
    let stream = matches!(source, LoginSource::Eastmoney);
    window.eval(format!(r#"
        (() => {{
            const key = {key_json};
            const request = {request};
            const results = window.__lianghuaSessionResults ||= Object.create(null);
            let done = false;
            let stream;
            const controller = new AbortController();
            const finish = value => {{
                if (done) return;
                done = true;
                clearTimeout(timer);
                controller.abort();
                stream?.close();
                results[key] = value;
                setTimeout(() => delete results[key], 30000);
            }};
            const timer = setTimeout(() => finish({{error: '登录会话请求超时，请检查登录是否失效'}}), 12000);
            if ({stream}) {{
                stream = new EventSource(request.url, {{withCredentials: true}});
                stream.onmessage = event => {{
                    try {{
                        const data = JSON.parse(event.data);
                        if (data.full !== 0 && (data.rc !== 0 || Array.isArray(data.data?.trends)))
                            finish({{body: JSON.stringify(data)}});
                    }} catch (_) {{ finish({{error: '浏览器分时数据格式无效'}}); }}
                }};
                stream.onerror = () => finish({{error: '浏览器分时请求失败，请检查验证页面'}});
            }} else {{
                // Referer、Cookie、Origin 等由浏览器管理，保留 HttpOnly Cookie 及站点规则。
                const headers = {{...request.headers}};
                delete headers.Referer;
                if (headers['X-Fuyao-Auth']) {{
                    headers.sw8 = '1-' + btoa(crypto.randomUUID().replaceAll('-', '')) + '-'
                        + btoa(crypto.randomUUID().replaceAll('-', '').slice(0, 16))
                        + '-10-' + btoa('news-p-fe-app-stock-page<browser>') + '-'
                        + btoa('lianghua') + '-' + btoa(location.hostname);
                }}
                fetch(request.url, {{method: request.method, headers,
                    body: request.body === null ? undefined : JSON.stringify(request.body),
                    credentials: 'include', signal: controller.signal}})
                .then(async response => {{
                    if (!response.ok) throw new Error('浏览器分时 HTTP状态=' + response.status + ';');
                    finish({{body: await response.text()}});
                }})
                .catch(error => finish({{error: String(error)}}));
            }}
        }})();
    "#)).map_err(|e| format!("启动登录会话请求失败: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("浏览器响应超时，请保持登录窗口打开".into());
        }
        let (tx, rx) = mpsc::channel();
        window
            .eval_with_callback(
                format!(
                    r#"
            (() => {{
                const results = window.__lianghuaSessionResults;
                const value = results?.[{key_json}] ?? null;
                if (value !== null) delete results[{key_json}];
                return value;
            }})()
        "#
                ),
                move |value| {
                    let _ = tx.send(value);
                },
            )
            .map_err(|e| format!("读取登录会话响应失败: {e}"))?;
        let raw = rx
            .recv_timeout(remaining.min(Duration::from_secs(1)))
            .map_err(|_| "登录窗口未响应，请保持窗口打开".to_string())?;
        let response: serde_json::Value =
            serde_json::from_str(&raw).map_err(|e| format!("浏览器响应格式错误: {e}"))?;
        if let Some(body) = response["body"].as_str() {
            return Ok(body.into());
        }
        if let Some(error) = response["error"].as_str() {
            return Err(error.into());
        }
        if !response.is_null() {
            return Err("浏览器响应缺少数据".into());
        }
        std::thread::sleep(Duration::from_millis(200).min(remaining));
    }
}

#[tauri::command]
pub async fn use_data_source_session(
    app: tauri::AppHandle,
    source: LoginSource,
    ts_code: Option<String>,
) -> Result<(), String> {
    let config = source.config();
    let generation = {
        let mut generations = SESSION_GENERATIONS
            .lock()
            .map_err(|_| "登录会话状态锁损坏")?;
        generations[config.index] += 1;
        generations[config.index]
    };
    tauri::async_runtime::spawn_blocking(move || {
        let ts_code = ts_code.unwrap_or_else(|| "000001.SZ".into());
        // 仅启用时读取 Cookie/UA 并探测一次，正常拉取不再轮询 WebView。
        let native = (|| -> Result<NativeSession, String> {
            let window = app
                .get_webview_window(config.label)
                .ok_or("登录窗口已关闭")?;
            if !config.accepts(&window.url().map_err(|e| e.to_string())?) {
                return Err("请先返回来源网站页面".into());
            }
            let url = match source {
                LoginSource::Eastmoney => {
                    "https://push2.eastmoney.com/api/qt/stock/trends2/get".to_string()
                }
                LoginSource::Xueqiu => {
                    volume_ratio_request(IntradayVolumeSource::Xueqiu, &ts_code)?.url
                }
                LoginSource::Tonghuashun => {
                    volume_ratio_request(IntradayVolumeSource::Tonghuashun, &ts_code)?.url
                }
            };
            let cookies = window
                .cookies_for_url(url.parse().map_err(|e| format!("请求地址无效: {e}"))?)
                .map_err(|e| format!("读取登录 Cookie 失败: {e}"))?;
            if cookies.is_empty() && !matches!(source, LoginSource::Eastmoney) {
                return Err("登录页面未提供可用于该接口的 Cookie".into());
            }
            let cookies = cookies
                .iter()
                .map(|cookie| format!("{}={}", cookie.name(), cookie.value()))
                .collect::<Vec<_>>()
                .join("; ");
            let mut cookie_header = reqwest::header::HeaderValue::from_str(&cookies)
                .map_err(|_| "登录 Cookie 无法用于 HTTP 请求")?;
            cookie_header.set_sensitive(true);
            let (tx, rx) = mpsc::channel();
            window
                .eval_with_callback("navigator.userAgent", move |value| {
                    let _ = tx.send(value);
                })
                .map_err(|e| format!("读取浏览器标识失败: {e}"))?;
            let user_agent: String = serde_json::from_str(
                &rx.recv_timeout(Duration::from_secs(6))
                    .map_err(|_| "读取浏览器标识超时")?,
            )
            .map_err(|_| "浏览器标识格式错误")?;
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(reqwest::header::COOKIE, cookie_header);
            let session = NativeSession {
                source,
                http: reqwest::blocking::Client::builder()
                    .default_headers(headers)
                    .user_agent(user_agent)
                    .https_only(true)
                    .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                        // 东财会跳转到 push2delay；只跟随本站 HTTPS，禁止把会话带出来源域。
                        if attempt.previous().len() < 5 && source.config().accepts(attempt.url()) {
                            attempt.follow()
                        } else {
                            attempt.stop()
                        }
                    }))
                    .connect_timeout(Duration::from_secs(6))
                    .timeout(Duration::from_secs(12))
                    .build()
                    .map_err(|e| format!("创建登录 HTTP 会话失败: {e}"))?,
            };
            let raw = session.fetch(&ts_code)?;
            validate_response(source, &raw, &ts_code)?;
            Ok(session)
        })();
        // 只在启用时选择传输，避免每只股票重复双发请求。
        let (transport, fetch): (
            SessionTransport,
            lianghua_app_market::volume_ratio::VolumeRatioSessionFetch,
        ) = match native {
            Ok(session) => (
                SessionTransport {
                    kind: "rust",
                    reason: None,
                },
                Arc::new(move |ts_code| session.fetch(ts_code)),
            ),
            Err(native_error) => {
                let raw = fetch_in_browser(&app, source, &ts_code)
                    .and_then(|raw| {
                        validate_response(source, &raw, &ts_code)?;
                        Ok(raw)
                    })
                    .map_err(|browser_error| {
                        format!("Rust 探测失败：{native_error}；WebView 探测失败：{browser_error}")
                    })?;
                drop(raw);
                (
                    SessionTransport {
                        kind: "webview",
                        reason: Some(native_error),
                    },
                    Arc::new(move |ts_code| fetch_in_browser(&app, source, ts_code)),
                )
            }
        };
        let generations = SESSION_GENERATIONS
            .lock()
            .map_err(|_| "登录会话状态锁损坏")?;
        if generations[config.index] != generation {
            return Err("登录会话启用已取消或被新请求替代".into());
        }
        lianghua_app_market::volume_ratio::set_volume_ratio_session_fetch(
            config.source,
            Some(fetch),
        )?;
        SESSION_TRANSPORTS
            .lock()
            .map_err(|_| "登录传输状态锁损坏")?[config.index] = Some(transport);
        Ok(())
    })
    .await
    .map_err(|e| format!("检查登录会话失败: {e}"))?
}

#[tauri::command]
pub fn stop_data_source_session(source: LoginSource) -> Result<(), String> {
    let config = source.config();
    let mut generations = SESSION_GENERATIONS
        .lock()
        .map_err(|_| "登录会话状态锁损坏")?;
    generations[config.index] += 1;
    // 停用仅移除后台回调，保留窗口及登录状态，后续可再次启用。
    lianghua_app_market::volume_ratio::set_volume_ratio_session_fetch(config.source, None)?;
    SESSION_TRANSPORTS
        .lock()
        .map_err(|_| "登录传输状态锁损坏")?[config.index] = None;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn login_domains_are_exact_or_subdomains_and_require_https() {
        for source in [
            LoginSource::Eastmoney,
            LoginSource::Xueqiu,
            LoginSource::Tonghuashun,
        ] {
            let config = source.config();
            assert!(config.accepts(&config.home.parse().unwrap()));
            assert!(config.accepts(
                &format!("https://passport.{}/", config.domain)
                    .parse()
                    .unwrap()
            ));
            assert!(!config.accepts(&format!("https://evil{}/", config.domain).parse().unwrap()));
            assert!(!config.accepts(
                &format!("https://{}.example.org/", config.domain)
                    .parse()
                    .unwrap()
            ));
            assert!(!config.accepts(&format!("http://{}/", config.domain).parse().unwrap()));
            assert!(!config.accepts(&"tauri://localhost".parse().unwrap()));
        }
    }
}
