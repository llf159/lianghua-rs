use super::{Manifest, WebDavConfig, atomic_write, digest};
use chrono::Utc;
use reqwest::{
    Method, StatusCode,
    blocking::{Client, Response},
    header,
};
use std::{
    io::Read,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant},
};

pub(super) struct Dav {
    client: Client,
    config: WebDavConfig,
    root: reqwest::Url,
    pub manifest: Manifest,
    etag: Option<String>,
    owner: String,
    locked: bool,
    bootstrapping: bool,
    renewed: Instant,
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
}

impl Dav {
    pub fn new(config: &WebDavConfig) -> Result<Self, String> {
        let root = config.root_url()?;
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(120))
                .connect_timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| e.to_string())?,
            config: config.clone(),
            root,
            manifest: Manifest::default(),
            etag: None,
            owner: digest(
                format!(
                    "{}-{}",
                    Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                    std::process::id()
                )
                .as_bytes(),
            ),
            locked: false,
            bootstrapping: false,
            renewed: Instant::now(),
            uploaded_bytes: 0,
            downloaded_bytes: 0,
        })
    }

    fn request(
        &mut self,
        method: Method,
        path: &str,
        body: Option<&[u8]>,
        condition: Option<(&str, &str)>,
    ) -> Result<Response, String> {
        let url = self.root.join(path).map_err(|e| e.to_string())?;
        for attempt in 0..3 {
            if !cfg!(test) {
                // 所有连接共享预算，反复点击同步也不能重复领取突发额度。
                // 空闲时最多连续 8 个请求，之后每 3.2 秒恢复一次请求额度。
                static REQUEST_PACE: Mutex<(Option<Instant>, f64)> = Mutex::new((None, 8.0));
                let mut pace = REQUEST_PACE
                    .lock()
                    .map_err(|_| "请求限速状态异常，请重启应用")?;
                if let Some(last) = pace.0 {
                    pace.1 = (pace.1 + last.elapsed().as_secs_f64() / 3.2).min(8.0);
                }
                if pace.1 < 1.0 {
                    std::thread::sleep(Duration::from_secs_f64((1.0 - pace.1) * 3.2));
                    pace.1 = 0.0;
                } else {
                    pace.1 -= 1.0;
                }
                pace.0 = Some(Instant::now());
            }
            let mut request = self
                .client
                .request(method.clone(), url.clone())
                .basic_auth(&self.config.username, Some(&self.config.password));
            if let Some(body) = body {
                request = request.body(body.to_vec());
                // 包括重试和控制文件，保守计入实际请求携带的上传字节。
                self.uploaded_bytes += body.len() as u64;
            }
            if let Some((name, value)) = condition {
                request = request.header(name, value);
            }
            // 不把 URL 或响应正文写入错误，避免凭据/服务端回显进入日志。
            let response = request
                .send()
                .map_err(|e| format!("WebDAV 请求失败：{}；可稍后重试", e.without_url()))?;
            if matches!(response.status().as_u16(), 429 | 502 | 503 | 504) && attempt < 2 {
                let seconds = response
                    .headers()
                    .get(header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(2 << attempt)
                    .clamp(1, 30);
                std::thread::sleep(Duration::from_secs(seconds));
                continue;
            }
            return Ok(response);
        }
        unreachable!()
    }

    pub fn error(status: StatusCode) -> String {
        match status.as_u16() {
            401 | 403 => "WebDAV 认证或权限失败，请检查坚果云账号及第三方应用密码".into(),
            412 => "云端已被另一台设备修改，请重新同步；本地数据已保留".into(),
            423 => "云端正在同步，请稍后再试".into(),
            429 => "坚果云请求频率受限，请稍后再试".into(),
            _ => format!("WebDAV 返回 HTTP {}，请检查连接和云端空间", status.as_u16()),
        }
    }

    pub fn read_manifest(&mut self) -> Result<(), String> {
        let response = self.request(Method::GET, "manifest.json.gz", None, None)?;
        if response.status() == StatusCode::NOT_FOUND {
            self.manifest = Manifest::default();
            self.etag = None;
            return Ok(());
        }
        if !response.status().is_success() {
            return Err(Self::error(response.status()));
        }
        self.etag = Some(
            response
                .headers()
                .get(header::ETAG)
                .and_then(|v| v.to_str().ok())
                .filter(|v| !v.starts_with("W/"))
                .ok_or("WebDAV 未提供强 ETag，无法保证多设备安全同步")?
                .to_string(),
        );
        let mut bytes = Vec::new();
        response
            .take(32 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        self.downloaded_bytes += bytes.len() as u64;
        self.manifest = serde_json::from_slice(&super::decode(&bytes, 128 * 1024 * 1024)?)
            .map_err(|e| format!("云端目录损坏，已停止同步：{e}"))?;
        self.manifest.validate()?;
        Ok(())
    }

    pub fn acquire(&mut self) -> Result<(), String> {
        self.read_manifest()?;
        if self.etag.is_none() {
            // 只在索引不存在时建目录；正常同步避免两次重复 MKCOL。
            // 根目录必须是用户指定的子文件夹，绝不在 /dav/ 直接写文件。
            for path in ["", "objects/"] {
                let response =
                    self.request(Method::from_bytes(b"MKCOL").unwrap(), path, Some(b""), None)?;
                if !response.status().is_success()
                    && response.status() != StatusCode::METHOD_NOT_ALLOWED
                {
                    return Err(Self::error(response.status()));
                }
            }
        }
        // 坚果云忽略 PUT 的 If-None-Match，首次创建改用原子 MKCOL。
        // 正式目录存在后继续由已验证有效的 If-Match 和租约保护并发写入。
        if self.etag.is_none() {
            let response = self.request(
                Method::from_bytes(b"MKCOL").unwrap(),
                ".bootstrap/",
                Some(b""),
                None,
            )?;
            if response.status().is_success() {
                self.bootstrapping = true;
                self.read_manifest()?;
            } else if response.status() == StatusCode::METHOD_NOT_ALLOWED {
                self.read_manifest()?;
                if self.etag.is_none() {
                    return Err("云端首次初始化正在进行，请稍后重试；若先前初始化意外退出且其他设备均已停止，可在坚果云删除同步文件夹内的 .bootstrap 子文件夹后重试".into());
                }
            } else {
                return Err(Self::error(response.status()));
            }
            if self.etag.is_none() {
                let path = ".bootstrap/probe";
                let result = (|| {
                    let response = self.request(Method::PUT, path, Some(b"probe"), None)?;
                    if !response.status().is_success() {
                        return Err(Self::error(response.status()));
                    }
                    let response = self.request(Method::GET, path, None, None)?;
                    if !response.status().is_success() {
                        return Err(Self::error(response.status()));
                    }
                    if !response
                        .headers()
                        .get(header::ETAG)
                        .and_then(|v| v.to_str().ok())
                        .is_some_and(|v| !v.is_empty() && !v.starts_with("W/"))
                    {
                        return Err("WebDAV 缺少强 ETag，不能安全同步".into());
                    }
                    let response = self.request(
                        Method::PUT,
                        path,
                        Some(b"probe"),
                        Some(("If-Match", "\"lianghua-invalid-etag\"")),
                    )?;
                    if response.status() != StatusCode::PRECONDITION_FAILED {
                        return Err("WebDAV 不支持可靠的 If-Match，已停止同步以保护策略".into());
                    }
                    Ok(())
                })();
                let _ = self.request(Method::DELETE, path, None, None);
                result?;
            }
        }
        if self
            .manifest
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires > Utc::now().timestamp())
        {
            return Err("另一台设备正在同步；若其意外关闭，锁会在十分钟内自动释放".into());
        }
        self.manifest.lease = Some(super::Lease {
            owner: self.owner.clone(),
            expires: Utc::now().timestamp() + 600,
        });
        self.persist()?;
        self.locked = true;
        self.release_bootstrap()?;
        Ok(())
    }

    pub fn persist(&mut self) -> Result<(), String> {
        let bytes = super::encode(&serde_json::to_vec(&self.manifest).map_err(|e| e.to_string())?)?;
        let etag = self.etag.clone();
        if etag.is_none() && !self.bootstrapping {
            return Err("首次写入缺少排他初始化锁，已停止同步".into());
        }
        let condition = etag.as_deref().map(|v| ("If-Match", v));
        let response = self.request(Method::PUT, "manifest.json.gz", Some(&bytes), condition)?;
        if !response.status().is_success() {
            self.locked = false;
            return Err(Self::error(response.status()));
        }
        // 部分 DAV 服务 PUT 不返回 ETag，重新 GET 并核对锁的所有者。
        let expected = self.manifest.clone();
        self.read_manifest()?;
        if self.manifest != expected {
            self.locked = false;
            return Err("云端目录写入校验失败，已停止同步".into());
        }
        self.renewed = Instant::now();
        Ok(())
    }

    pub fn heartbeat(&mut self) -> Result<(), String> {
        if self.locked && self.renewed.elapsed() >= Duration::from_secs(120) {
            self.manifest.lease = Some(super::Lease {
                owner: self.owner.clone(),
                expires: Utc::now().timestamp() + 600,
            });
            self.persist()?;
        }
        Ok(())
    }

    pub fn upload(&mut self, hash: &str, bytes: &[u8]) -> Result<(), String> {
        self.heartbeat()?;
        let path = format!("objects/{hash}.gz");
        // 补偿上传成功但目录提交前中断留下的对象，避免重试再耗上传额度。
        let existing = self.request(Method::HEAD, &path, None, None)?;
        if existing.status().is_success() {
            return Ok(());
        }
        if existing.status() != StatusCode::NOT_FOUND {
            return Err(Self::error(existing.status()));
        }
        let response = self.request(Method::PUT, &path, Some(bytes), None)?;
        if !response.status().is_success() {
            return Err(Self::error(response.status()));
        }
        Ok(())
    }

    fn release_bootstrap(&mut self) -> Result<(), String> {
        if self.bootstrapping {
            let response = self.request(Method::DELETE, ".bootstrap/", None, None)?;
            if !response.status().is_success() && response.status() != StatusCode::NOT_FOUND {
                return Err(Self::error(response.status()));
            }
            self.bootstrapping = false;
        }
        Ok(())
    }

    pub fn download(&mut self, hash: &str, cache: &Path) -> Result<Vec<u8>, String> {
        let path = cache.join(format!("{hash}.gz"));
        if let Ok(bytes) = std::fs::read(&path) {
            if let Ok(raw) = super::decode(&bytes, 512 * 1024 * 1024) {
                if digest(&raw) == hash {
                    return Ok(raw);
                }
            }
        }
        if let Some(pack) = self.manifest.packs.get(hash).cloned() {
            let raw_pack = self.download(&pack, cache)?;
            let mut archive = zip::ZipArchive::new(std::io::Cursor::new(raw_pack))
                .map_err(|e| format!("云端小文件包损坏：{e}"))?;
            let entry = archive
                .by_name(hash)
                .map_err(|_| "云端小文件包缺少所选内容")?;
            if entry.size() > 64 * 1024 {
                return Err("云端小文件包成员超过大小限制".into());
            }
            let mut raw = Vec::new();
            entry
                .take(64 * 1024 + 1)
                .read_to_end(&mut raw)
                .map_err(|e| e.to_string())?;
            if raw.len() > 64 * 1024 || digest(&raw) != hash {
                return Err("小文件包内容校验失败，未替换本地数据".into());
            }
            atomic_write(&path, &super::encode(&raw)?)?;
            return Ok(raw);
        }
        self.heartbeat()?;
        let response = self.request(Method::GET, &format!("objects/{hash}.gz"), None, None)?;
        if !response.status().is_success() {
            return Err(Self::error(response.status()));
        }
        let mut bytes = Vec::new();
        response
            .take(512 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        self.downloaded_bytes += bytes.len() as u64;
        let raw = super::decode(&bytes, 512 * 1024 * 1024)?;
        if digest(&raw) != hash {
            return Err("下载文件校验失败，未替换本地数据".into());
        }
        atomic_write(&path, &bytes)?;
        Ok(raw)
    }

    pub fn garbage_collect(&mut self) -> Result<(), String> {
        let referenced = self.manifest.hashes();
        for hash in self.manifest.garbage.clone() {
            if !referenced.contains(&hash) {
                self.heartbeat()?;
                let object_hash = if let Some(pack) = self.manifest.packs.remove(&hash) {
                    if self.manifest.packs.values().any(|other| other == &pack) {
                        self.manifest.garbage.remove(&hash);
                        continue;
                    }
                    pack
                } else {
                    hash.clone()
                };
                self.manifest.garbage.insert(object_hash.clone());
                let response = self.request(
                    Method::DELETE,
                    &format!("objects/{object_hash}.gz"),
                    None,
                    None,
                )?;
                if !response.status().is_success() && response.status() != StatusCode::NOT_FOUND {
                    self.manifest.garbage.insert(object_hash);
                    continue;
                }
                self.manifest.garbage.remove(&object_hash);
            }
            self.manifest.garbage.remove(&hash);
        }
        Ok(())
    }

    pub fn release(&mut self) -> Result<(), String> {
        if self.locked {
            self.manifest.lease = None;
            self.persist()?;
            self.locked = false;
        }
        Ok(())
    }
}

impl Drop for Dav {
    fn drop(&mut self) {
        if self.locked {
            let _ = self.release();
        }
        let _ = self.release_bootstrap();
    }
}
