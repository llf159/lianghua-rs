import { useEffect, useState } from "react";
import {
  getCloudConfig,
  saveCloudConfig,
  testCloudConnection,
  type CloudConfig,
} from "../../../apis/cloudSync";
import "../css/CloudSync.css";

export default function CloudSyncSettingsModal({
  onClose,
}: {
  onClose: () => void;
}) {
  const [config, setConfig] = useState<CloudConfig | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  useEffect(() => {
    let cancelled = false;
    void getCloudConfig()
      .then((value) => {
        if (!cancelled) setConfig(value);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, []);
  async function save(test: boolean) {
    if (!config || busy) return;
    setBusy(true);
    setError("");
    setNotice("");
    try {
      await saveCloudConfig(config);
      setConfig(await getCloudConfig());
      if (test) await testCloudConnection();
      setNotice(test ? "已保存，连接正常。" : "配置已保存。");
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div
      className="settings-modal-backdrop cloud-modal-overlay"
      onClick={(event) => {
        if (!busy && event.target === event.currentTarget) onClose();
      }}
    >
      <section
        className="settings-modal settings-modal-narrow cloud-config-modal"
        role="dialog"
        aria-modal="true"
        aria-label="网盘同步配置"
      >
        <div className="cloud-config-head">
          <div>
            <h3>网盘同步</h3>
            <p>WebDAV · 坚果云</p>
          </div>
          <button
            className="cloud-config-close"
            type="button"
            aria-label="关闭网盘同步配置"
            onClick={onClose}
            disabled={busy}
          >
            ×
          </button>
        </div>
        {config ? (
          <fieldset className="cloud-config-fields" disabled={busy}>
            <section className="cloud-config-section" aria-label="连接配置">
              <h4>连接</h4>
              <label className="settings-field cloud-config-wide">
                <span>服务器地址</span>
                <input
                  type="url"
                  inputMode="url"
                  value={config.endpoint}
                  onChange={(e) =>
                    setConfig({ ...config, endpoint: e.target.value })
                  }
                  autoCapitalize="none"
                  spellCheck={false}
                />
              </label>
              <label className="settings-field cloud-config-wide">
                <span>同步文件夹</span>
                <input
                  value={config.folder}
                  onChange={(e) =>
                    setConfig({ ...config, folder: e.target.value })
                  }
                  placeholder="lianghua-rs"
                />
              </label>
            </section>
            <section className="cloud-config-section" aria-label="账号配置">
              <h4>账号</h4>
              <label className="settings-field">
                <span>账号 / 邮箱</span>
                <input
                  value={config.username}
                  onChange={(e) =>
                    setConfig({ ...config, username: e.target.value })
                  }
                  autoCapitalize="none"
                  autoComplete="username"
                  spellCheck={false}
                />
              </label>
              <label className="settings-field">
                <span>应用密码</span>
                <input
                  type="password"
                  value={config.password}
                  onChange={(e) =>
                    setConfig({ ...config, password: e.target.value })
                  }
                  placeholder={
                    config.hasPassword ? "已保存，留空保持" : "输入应用密码"
                  }
                  autoComplete="new-password"
                />
              </label>
              <div className="cloud-config-hint cloud-config-wide">
                <p>在坚果云安全选项中创建应用密码，凭据仅存本机。</p>
                <a
                  href="https://help.jianguoyun.com/?p=2064"
                  target="_blank"
                  rel="noreferrer"
                >
                  获取应用密码 ↗
                </a>
              </div>
            </section>
            <div className="cloud-config-footer">
              <button
                className="settings-secondary-btn"
                type="button"
                onClick={() => void save(false)}
              >
                保存
              </button>
              <button
                className="settings-primary-btn"
                type="button"
                onClick={() => void save(true)}
              >
                {busy ? "处理中…" : "保存并测试"}
              </button>
            </div>
          </fieldset>
        ) : (
          !error && <p className="cloud-config-loading">读取配置中…</p>
        )}
        {error && (
          <p className="cloud-error" role="alert">
            {error}
          </p>
        )}
        {notice && (
          <p className="cloud-notice" role="status">
            {notice}
          </p>
        )}
      </section>
    </div>
  );
}
