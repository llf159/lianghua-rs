import { useEffect, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import {
  cloudPathLabel,
  deleteCloudVersion,
  estimateCloudUpload,
  listCloudData,
  syncCloud,
  type CloudItem,
  type UploadEstimate,
} from "../../../apis/cloudSync";
import "../css/CloudSync.css";

function bytes(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(2)} MB`;
}

export default function CloudSyncPanel({
  disabled,
  onSynced,
  onBusyChange,
}: {
  disabled: boolean;
  onSynced: () => void;
  onBusyChange: (busy: boolean) => void;
}) {
  const [busy, setBusy] = useState(false);
  const [estimate, setEstimate] = useState<UploadEstimate | null>(null);
  const [progress, setProgress] = useState("");
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [conflicts, setConflicts] = useState<string[]>([]);
  const [manager, setManager] = useState(false);
  const [items, setItems] = useState<CloudItem[]>([]);
  const [filter, setFilter] = useState("");
  const [showHistory, setShowHistory] = useState(false);
  const [selected, setSelected] = useState<CloudItem | null>(null);
  const [action, setAction] = useState<"restore" | "delete" | null>(null);
  useEffect(() => {
    let cancelled = false;
    const subscription = listen<string>("cloud-sync-progress", (event) => {
      if (!cancelled) setProgress(event.payload);
    });
    return () => {
      cancelled = true;
      void subscription.then((unlisten) => unlisten());
    };
  }, []);
  async function operate(
    kind: "sync" | "download" | "list" | "restore" | "delete" | "estimate",
    item?: CloudItem,
  ) {
    if (busy || disabled) return;
    setBusy(true);
    onBusyChange(true);
    setError("");
    setNotice("");
    setProgress(kind === "list" ? "正在读取云端目录…" : "正在准备…");
    try {
      if (kind === "estimate") {
        setEstimate(await estimateCloudUpload());
      } else if (kind === "list") {
        setItems(await listCloudData());
        setManager(true);
      } else if (kind === "delete" && item) {
        await deleteCloudVersion(item);
        setNotice(`已删除云端版本：${item.path}；本地副本保留。`);
        setItems(await listCloudData());
      } else {
        const result = await syncCloud(
          kind === "restore" ? item : undefined,
          kind === "download",
        );
        setConflicts(result.conflicts);
        setNotice(
          `${kind === "download" ? "下载完成" : "同步完成"} · 上传 ${result.uploaded} 项 · 接收 ${result.downloaded} 项${result.skipped ? ` · 跳过 ${result.skipped} 项` : ""} · 上传 ${bytes(result.uploadedBytes)} / 下载 ${bytes(result.downloadedBytes)}${result.warnings.length ? `；${result.warnings.join("；")}` : ""}`,
        );
        onSynced();
        if (manager) setItems(await listCloudData());
      }
      setSelected(null);
      setAction(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
      onBusyChange(false);
      setProgress("");
    }
  }
  const visible = items.filter(
    (item) =>
      (showHistory || item.isHead) &&
      `${item.path} ${cloudPathLabel(item.path)}`
        .toLowerCase()
        .includes(filter.toLowerCase()),
  );
  return (
    <section
      className="settings-card cloud-panel cloud-panel-management"
      aria-label="网盘同步"
      aria-busy={busy}
    >
      <div className="cloud-sync-toolbar">
        <div className="cloud-sync-heading">
          <h3>网盘同步</h3>
          <p>
            同步策略、图表配置、浏览习惯与榜单快照；自选数据在自选观察页单独管理。
          </p>
        </div>
        <div className="cloud-sync-actions">
          <button
            className="settings-primary-btn"
            type="button"
            disabled={busy || disabled}
            onClick={() => void operate("sync")}
            title="上传本机改动并接收云端内容"
          >
            双向同步
          </button>
          <button
            className="settings-secondary-btn"
            type="button"
            disabled={busy || disabled}
            onClick={() => void operate("download")}
            title="仅接收云端当前版本，替换前备份本地；冲突分组跳过"
          >
            下载云端
          </button>
        </div>
      </div>
      <div className="cloud-sync-secondary">
        <div className="cloud-sync-tools">
          <button
            type="button"
            disabled={busy || disabled}
            onClick={() => void operate("list")}
          >
            云端数据管理
          </button>
          <button
            type="button"
            disabled={busy || disabled}
            onClick={() => void operate("estimate")}
          >
            测算上传量
          </button>
        </div>
        <details className="cloud-sync-help">
          <summary>同步说明</summary>
          <p>
            双向同步上传并接收改动，下载仅接收云端。替换前自动备份，冲突分组跳过。
          </p>
        </details>
      </div>
      {busy && progress && (
        <p className="cloud-notice cloud-progress" role="status">
          {progress}
        </p>
      )}
      {notice && (
        <p className="cloud-notice" role="status">
          {notice}
        </p>
      )}
      {estimate && (
        <div className="cloud-estimate" role="status">
          <div className="cloud-estimate-head">
            <h4>上传量测算</h4>
            <button
              type="button"
              onClick={() => setEstimate(null)}
              aria-label="关闭上传量测算"
            >
              ×
            </button>
          </div>
          <div className="cloud-estimate-grid">
            <div>
              <span>首次压缩上传</span>
              <strong>{bytes(estimate.initialUploadBytes)}</strong>
              <small>{estimate.uploadObjects} 个数据对象</small>
            </div>
            <div>
              <span>每日榜单快照</span>
              <strong>
                {bytes(
                  estimate.rankingDailyBytes + estimate.similarityDailyBytes,
                )}
              </strong>
              <small>总榜与相似榜合计</small>
            </div>
            <div>
              <span>31 天预估</span>
              <strong>{bytes(estimate.monthly31DaysBytes)}</strong>
              <small>
                约占 1 GB 的{" "}
                {((estimate.monthly31DaysBytes / 1_000_000_000) * 100).toFixed(
                  1,
                )}
                %
              </small>
            </div>
          </div>
          <p>
            总榜五日快照约 {bytes(estimate.rankingDailyBytes)}，相似榜单日快照约{" "}
            {bytes(estimate.similarityDailyBytes)}；22 天约{" "}
            {bytes(estimate.monthly22DaysBytes)}。
          </p>
          {estimate.missingSimilarity && (
            <p className="cloud-estimate-warning">
              本机暂无相似榜结果，当前预估未包含相似榜流量，生成后请重新测算。
            </p>
          )}
          <p>
            {estimate.monthly31DaysBytes > 1_000_000_000
              ? "当前估算已超过月额度。"
              : estimate.missingSimilarity
                ? `每天上传的相似榜快照需低于约 ${bytes(Math.max(0, (1_000_000_000 - estimate.monthly31DaysBytes - estimate.initialUploadBytes) / 31))}，并给重算、配置修改和索引留出余量。`
                : "仍需给额外重算和索引请求留出余量。"}
          </p>
          <p className="cloud-estimate-footnote">
            测算不连接网盘。月估算包含当前配置，首月首次快照上传另计；反复重算产生新内容、修改策略及请求索引会增加流量。删除旧日期不会返还已用上传额度。
          </p>
        </div>
      )}
      {error && (
        <p className="cloud-error" role="alert">
          {error}
        </p>
      )}
      {conflicts.length > 0 && (
        <div className="cloud-conflicts">
          <strong>同步冲突，已保留各份数据：</strong>
          <ul>
            {conflicts.map((path) => (
              <li key={path}>{cloudPathLabel(path)}</li>
            ))}
          </ul>
          <p>在云端数据管理中恢复所需版本。</p>
        </div>
      )}
      {manager && (
        <div className="cloud-manager">
          <div className="cloud-manager-head">
            <div>
              <h4>云端数据</h4>
              <p>查看当前文件与历史快照，并在需要时恢复指定版本。</p>
            </div>
            <button
              className="cloud-manager-close"
              type="button"
              disabled={busy}
              onClick={() => setManager(false)}
            >
              收起 <span aria-hidden="true">×</span>
            </button>
          </div>
          <div className="cloud-manager-summary">
            <div>
              <span>当前数据</span>
              <strong>{items.filter((item) => item.isHead).length}</strong>
              <small>个云端项目</small>
            </div>
            <div>
              <span>本次显示</span>
              <strong>{visible.length}</strong>
              <small>{showHistory ? "包含历史版本" : "仅当前版本"}</small>
            </div>
            <div>
              <span>相似榜快照</span>
              <strong
                className={
                  items.some(
                    (item) =>
                      item.isHead && item.path.startsWith("similarity/"),
                  )
                    ? "is-ready"
                    : "is-muted"
                }
              >
                {items.some(
                  (item) => item.isHead && item.path.startsWith("similarity/"),
                )
                  ? "已就绪"
                  : "未发现"}
              </strong>
              <small>最新一天结果</small>
            </div>
          </div>
          <div className="cloud-manager-controls">
            <label className="cloud-manager-search">
              <span aria-hidden="true">⌕</span>
              <input
                aria-label="筛选云端文件"
                placeholder="搜索策略、日期或配置"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
              />
            </label>
            <label className="cloud-history-toggle">
              <input
                type="checkbox"
                checked={showHistory}
                onChange={(e) => setShowHistory(e.target.checked)}
              />
              <span className="cloud-history-switch" aria-hidden="true" />
              显示历史版本
            </label>
            <button
              className="settings-secondary-btn"
              type="button"
              disabled={busy || disabled}
              onClick={() => void operate("list")}
            >
              ↻ 刷新
            </button>
          </div>
          <div className="cloud-manager-safety">
            <span aria-hidden="true">✓</span>
            恢复前会备份本地文件，冲突不会自动覆盖；删除只影响选中的云端版本。
          </div>
          {visible.length === 0 ? (
            <div className="cloud-empty">
              <span aria-hidden="true">☁</span>
              <strong>{filter ? "没有匹配的数据" : "云端还没有数据"}</strong>
            </div>
          ) : (
            <div
              className={`cloud-item-list${showHistory ? " cloud-item-list-history" : ""}`}
            >
              {visible.map((item) => (
                <article
                  className={`cloud-item${item.conflict && item.isHead ? " has-conflict" : item.isHead ? " is-current" : " is-history"}`}
                  key={`${item.path}:${item.version.id}`}
                >
                  <div className="cloud-item-main">
                    <div className="cloud-item-title">
                      <strong>{cloudPathLabel(item.path)}</strong>
                      <span
                        className={
                          item.conflict && item.isHead
                            ? "is-conflict"
                            : item.isHead
                              ? "is-current"
                              : "is-history"
                        }
                      >
                        {item.conflict && item.isHead
                          ? "待处理冲突"
                          : item.isHead
                            ? "当前版本"
                            : "历史版本"}
                      </span>
                    </div>
                    <code>{item.path}</code>
                    <div className="cloud-item-meta">
                      <span>
                        <small>更新时间</small>
                        <strong>
                          {new Date(item.version.createdAt).toLocaleString(
                            "zh-CN",
                          )}
                        </strong>
                      </span>
                      <span>
                        <small>压缩大小</small>
                        <strong>{bytes(item.version.compressedSize)}</strong>
                      </span>
                      <span>
                        <small>原始大小</small>
                        <strong>{bytes(item.version.size)}</strong>
                      </span>
                      <span>
                        <small>版本标识</small>
                        <strong>{item.version.hash.slice(0, 12)}</strong>
                      </span>
                    </div>
                  </div>
                  <div className="cloud-item-actions">
                    <button
                      className="settings-secondary-btn"
                      type="button"
                      disabled={busy || disabled}
                      onClick={() => {
                        setSelected(item);
                        setAction("restore");
                      }}
                    >
                      恢复并启用
                    </button>
                    <button
                      className="settings-danger-btn"
                      type="button"
                      disabled={busy || disabled}
                      onClick={() => {
                        setSelected(item);
                        setAction("delete");
                      }}
                    >
                      删除
                    </button>
                  </div>
                </article>
              ))}
            </div>
          )}
        </div>
      )}
      {selected && action && (
        <div className="settings-modal-backdrop cloud-modal-overlay">
          <section
            className="settings-modal settings-modal-narrow cloud-version-dialog"
            role="dialog"
            aria-modal="true"
            aria-label={action === "restore" ? "恢复云端版本" : "删除云端版本"}
          >
            <h3>
              {action === "restore" ? "恢复并启用这个版本" : "删除这个云端版本"}
            </h3>
            <div className="cloud-dialog-file">
              <strong>{cloudPathLabel(selected.path)}</strong>
              <code>{selected.path}</code>
            </div>
            <div className="cloud-dialog-meta">
              <span>
                <small>版本时间</small>
                <strong>
                  {new Date(selected.version.createdAt).toLocaleString("zh-CN")}
                </strong>
              </span>
              <span>
                <small>版本标识</small>
                <strong>{selected.version.hash.slice(0, 12)}</strong>
              </span>
            </div>
            <p className="cloud-dialog-copy">
              {action === "restore"
                ? "会先保留当前本地内容，再启用选定版本；该文件的冲突分支将合并，历史版本仍可恢复。"
                : "仅删除这个云端版本，本地数据保留。删除后相同的已同步本地内容不会自动上传；删除的版本将无法再从云端恢复。"}
            </p>
            <div className="settings-actions cloud-dialog-actions">
              <button
                className="settings-secondary-btn"
                type="button"
                disabled={busy}
                onClick={() => {
                  setSelected(null);
                  setAction(null);
                }}
              >
                取消
              </button>
              <button
                className={
                  action === "delete"
                    ? "settings-danger-btn"
                    : "settings-primary-btn"
                }
                type="button"
                disabled={busy || disabled}
                onClick={() => void operate(action, selected)}
              >
                {busy
                  ? "处理中…"
                  : action === "delete"
                    ? "确认删除"
                    : "确认恢复"}
              </button>
            </div>
          </section>
        </div>
      )}
    </section>
  );
}
