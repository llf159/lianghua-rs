import { useEffect, useState } from "react";
import {
  previewCloudData,
  resolveCloudStrategies,
  type CloudPreview,
} from "../../../apis/cloudSync";

function CloudTextDiff({
  before,
  after,
  beforeLabel,
  afterLabel,
}: {
  before: string | undefined;
  after: string | undefined;
  beforeLabel: string;
  afterLabel: string;
}) {
  const left = before === undefined ? [] : before.split("\n");
  const right = after === undefined ? [] : after.split("\n");
  if (
    left.length * right.length > 1_000_000 ||
    left.length + right.length > 10_000
  ) {
    return (
      <div className="cloud-text-diff">
        <p className="cloud-text-diff-legend">
          文本较长，分开展示两份完整原文，请核对后选择来源。
        </p>
        <strong>
          {beforeLabel}
          {before === undefined ? "（文件不存在）" : ""}
        </strong>
        <pre className="cloud-text-diff-lines">{before}</pre>
        <strong>
          {afterLabel}
          {after === undefined ? "（文件不存在）" : ""}
        </strong>
        <pre className="cloud-text-diff-lines">{after}</pre>
      </div>
    );
  }
  const rows: { kind: string; text: string; left?: number; right?: number }[] =
    [];
  // 超长文本回退为完整增删，避免二次方内存；绝不省略待核对的内容。
  {
    const lengths = Array.from(
      { length: left.length + 1 },
      () => new Uint32Array(right.length + 1),
    );
    for (let i = left.length - 1; i >= 0; i--) {
      for (let j = right.length - 1; j >= 0; j--) {
        lengths[i][j] =
          left[i] === right[j]
            ? lengths[i + 1][j + 1] + 1
            : Math.max(lengths[i + 1][j], lengths[i][j + 1]);
      }
    }
    let i = 0;
    let j = 0;
    while (i < left.length || j < right.length) {
      if (i < left.length && j < right.length && left[i] === right[j]) {
        rows.push({ kind: "same", text: left[i], left: ++i, right: ++j });
      } else if (
        j < right.length &&
        (i === left.length || lengths[i][j + 1] >= lengths[i + 1][j])
      ) {
        rows.push({ kind: "added", text: right[j], right: ++j });
      } else {
        rows.push({ kind: "removed", text: left[i], left: ++i });
      }
    }
  }
  return (
    <div className="cloud-text-diff">
      <div className="cloud-text-diff-legend">
        − {beforeLabel}
        {before === undefined ? "（文件不存在）" : ""} ／ + {afterLabel}
        {after === undefined ? "（文件不存在）" : ""}
      </div>
      <div
        className="cloud-text-diff-lines"
        tabIndex={0}
        aria-label={`${beforeLabel}与${afterLabel}内容差异`}
      >
        {rows.map((row, index) => (
          <div className={`cloud-diff-line is-${row.kind}`} key={index}>
            <span>{row.left ?? ""}</span>
            <span>{row.right ?? ""}</span>
            <code>
              {row.kind === "added" ? "+" : row.kind === "removed" ? "−" : " "}{" "}
              {row.text}
            </code>
          </div>
        ))}
        {before === after && <p>内容完全相同</p>}
      </div>
    </div>
  );
}

export default function CloudDataReview({
  path,
  versionId,
  disabled,
  onBusyChange,
  onResolved,
  onLoaded,
}: {
  path: string;
  versionId?: string;
  disabled: boolean;
  onBusyChange: (busy: boolean) => void;
  onResolved: () => void;
  onLoaded?: (preview: CloudPreview) => void;
}) {
  const [preview, setPreview] = useState<CloudPreview | null>(null);
  const [remoteId, setRemoteId] = useState(versionId ?? "");
  const [filePath, setFilePath] = useState("");
  const [choices, setChoices] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [revision, setRevision] = useState(0);
  const [changedOnly, setChangedOnly] = useState(true);
  useEffect(() => {
    let cancelled = false;
    void previewCloudData(path, versionId)
      .then((data) => {
        if (cancelled) return;
        setPreview(data);
        onLoaded?.(data);
        setRemoteId(versionId ?? data.heads[0] ?? "");
        setChoices({});
        setError("");
      })
      .catch((reason) => {
        if (!cancelled) {
          setPreview(null);
          setError(String(reason));
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [path, versionId, revision, onLoaded]);
  useEffect(() => {
    onBusyChange(loading || saving);
    return () => onBusyChange(false);
  }, [loading, saving, onBusyChange]);

  const local = preview?.sides.find((side) => side.id === "local");
  const base = preview?.sides.find((side) => side.id === "base");
  const remote = preview?.sides.find((side) => side.id === remoteId);
  const cloudSides =
    preview?.sides.filter(
      (side) => side.id !== "local" && side.id !== "base",
    ) ?? [];
  const activePaths = [
    "source/score_rule.toml",
    "source/chip_change_rule.toml",
  ].filter((name) =>
    preview?.sides.some(
      (side) =>
        side.id !== "base" && side.files.some((file) => file.path === name),
    ),
  );
  const names = [
    ...new Set(
      [
        ...(local?.files ?? []),
        ...(remote?.files ?? []),
        ...(base?.files ?? []),
      ].map((file) => file.path),
    ),
  ].sort(
    (a, b) =>
      Number(activePaths.includes(b)) - Number(activePaths.includes(a)) ||
      a.localeCompare(b),
  );
  const changed = names.filter(
    (name) =>
      local?.files.find((file) => file.path === name)?.hash !==
      remote?.files.find((file) => file.path === name)?.hash,
  );
  const visible = changedOnly ? changed : names;
  const selectedPath = visible.includes(filePath) ? filePath : visible[0];
  const localFile = local?.files.find((file) => file.path === selectedPath);
  const remoteFile = remote?.files.find((file) => file.path === selectedPath);
  const baseFile = base?.files.find((file) => file.path === selectedPath);
  const selections: Record<string, string> = { ...choices };
  for (const name of activePaths) {
    const present =
      preview?.sides.filter(
        (side) =>
          side.id !== "base" &&
          (side.id === "local" || preview?.heads.includes(side.id)) &&
          side.files.some((file) => file.path === name),
      ) ?? [];
    const hashes = new Set(
      present.map(
        (side) => side.files.find((file) => file.path === name)?.hash,
      ),
    );
    if (!selections[name] && hashes.size === 1)
      selections[name] =
        present.find((side) => side.id === "local")?.id ?? present[0].id;
  }
  const canResolve =
    path === "bundles/strategies.zip" &&
    !!preview?.heads.length &&
    activePaths.every((name) => selections[name]);

  async function resolve() {
    if (!preview || !canResolve || disabled || saving) return;
    setSaving(true);
    setError("");
    setNotice("");
    try {
      await resolveCloudStrategies(preview, selections);
      setNotice(
        "已按所选来源合并。各端不同的当前策略和历史资产均已保留为备份。",
      );
      setPreview(null);
      setLoading(true);
      setRevision((value) => value + 1);
      onResolved();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="cloud-data-review" aria-busy={loading || saving}>
      <div className="cloud-review-toolbar">
        <strong>文件内容对比</strong>
        <button
          type="button"
          disabled={disabled || loading || saving}
          onClick={() => {
            setLoading(true);
            setPreview(null);
            setRevision((value) => value + 1);
          }}
        >
          刷新差异
        </button>
      </div>
      {loading && <p role="status">正在下载并校验对比内容…</p>}
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
      {preview && (
        <>
          <p className="cloud-watch-hint">
            {preview.baseAvailable
              ? "按上次同步的云端内容核对改动；本地保留的历史和自动备份也会列出。"
              : "本机没有可用的上次同步内容，仅展示实际差异，无法判断哪端修改。"}
          </p>
          <label className="cloud-review-source">
            对比云端版本
            <select
              value={remoteId}
              disabled={disabled || saving}
              onChange={(event) => setRemoteId(event.target.value)}
            >
              {!cloudSides.length && <option value="">云端暂无版本</option>}
              {cloudSides.map((side, index) => (
                <option key={side.id} value={side.id}>
                  分支 {index + 1} ·{" "}
                  {side.createdAt
                    ? new Date(side.createdAt).toLocaleString("zh-CN")
                    : ""}{" "}
                  · {side.id.slice(0, 8)}
                  {preview.heads.includes(side.id) ? "（当前）" : "（历史）"}
                </option>
              ))}
            </select>
          </label>
          <label className="cloud-review-filter">
            <input
              type="checkbox"
              checked={changedOnly}
              onChange={(event) => setChangedOnly(event.target.checked)}
            />
            仅看差异（{changed.length} / {names.length} 个文件）
          </label>
          <div
            className="cloud-review-files"
            role="list"
            aria-label="差异文件列表"
          >
            {visible.map((name) => {
              const a = local?.files.find((file) => file.path === name);
              const b = remote?.files.find((file) => file.path === name);
              const c = base?.files.find((file) => file.path === name);
              const localChanged = a?.hash !== c?.hash;
              const cloudChanged = b?.hash !== c?.hash;
              const status =
                a?.hash === b?.hash
                  ? "两端相同"
                  : !a
                    ? "仅云端有"
                    : !b
                      ? "仅本地有"
                      : !base
                        ? "内容不同"
                        : localChanged && cloudChanged
                          ? "两端均与基线不同"
                          : localChanged
                            ? "本地与基线不同"
                            : "云端与基线不同";
              return (
                <button
                  type="button"
                  className={name === selectedPath ? "is-selected" : ""}
                  key={name}
                  onClick={() => setFilePath(name)}
                >
                  <code>{name.replace(/^source\//, "")}</code>
                  <span>{status}</span>
                </button>
              );
            })}
            {!visible.length && (
              <p>{names.length ? "两端所有文件内容相同" : "暂无可对比文件"}</p>
            )}
          </div>
          {selectedPath && (
            <div className="cloud-review-content">
              <strong>{selectedPath}</strong>
              <CloudTextDiff
                before={localFile?.text}
                after={remoteFile?.text}
                beforeLabel="当前本地"
                afterLabel="所选云端"
              />
              {base && (
                <details>
                  <summary>查看上次同步基线与本地的差异</summary>
                  <CloudTextDiff
                    before={baseFile?.text}
                    after={localFile?.text}
                    beforeLabel="上次同步云端"
                    afterLabel="当前本地"
                  />
                </details>
              )}
            </div>
          )}
          {path === "bundles/strategies.zip" && preview.heads.length > 0 && (
            <div className="cloud-strategy-resolution">
              <strong>分别选择合并后启用的策略</strong>
              <p>
                按完整文件选择，不拼接公式。所有分支的历史资产和未选中的当前策略均保留为备份；同名备份连同备注另存。
              </p>
              {activePaths.map((name) => (
                <label key={name}>
                  {name.endsWith("/score_rule.toml") ? "打分策略" : "筹码策略"}
                  <select
                    value={selections[name] ?? ""}
                    disabled={disabled || saving}
                    onChange={(event) => {
                      setChoices({ ...choices, [name]: event.target.value });
                      setFilePath(name);
                      if (event.target.value !== "local")
                        setRemoteId(event.target.value);
                      setChangedOnly(false);
                    }}
                  >
                    <option value="" disabled>
                      请选择来源
                    </option>
                    {preview.sides
                      .filter(
                        (side) =>
                          side.id !== "base" &&
                          (side.id === "local" ||
                            preview.heads.includes(side.id)) &&
                          side.files.some((file) => file.path === name),
                      )
                      .map((side) => (
                        <option key={side.id} value={side.id}>
                          {side.id === "local"
                            ? "当前本地"
                            : `云端分支 ${cloudSides.findIndex((item) => item.id === side.id) + 1} · ${side.id.slice(0, 8)}`}
                        </option>
                      ))}
                  </select>
                </label>
              ))}
              <button
                type="button"
                className="settings-primary-btn"
                disabled={disabled || loading || saving || !canResolve}
                onClick={() => void resolve()}
              >
                {saving ? "正在保留并合并…" : "按所选来源合并并启用"}
              </button>
            </div>
          )}
        </>
      )}
    </section>
  );
}
