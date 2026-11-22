import CloudStrategyDiff from "./CloudStrategyDiff";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  previewCloudData,
  resolveCloudStrategies,
  type CloudItem,
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
  const container = useRef<HTMLDivElement>(null);
  const [changeIndex, setChangeIndex] = useState(-1);
  const { rows, limited } = useMemo(() => {
    const left =
      before === undefined ? [] : before.replace(/\r\n/g, "\n").split("\n");
    const right =
      after === undefined ? [] : after.replace(/\r\n/g, "\n").split("\n");
    const aligned: {
      left?: string;
      right?: string;
      leftNumber?: number;
      rightNumber?: number;
      changed: boolean;
      start: boolean;
    }[] = [];
    const limited =
      left.length * right.length > 1_000_000 ||
      left.length + right.length > 10_000;
    const lengths = limited
      ? []
      : Array.from(
          { length: left.length + 1 },
          () => new Uint32Array(right.length + 1),
        );
    if (!limited) {
      for (let i = left.length - 1; i >= 0; i--) {
        for (let j = right.length - 1; j >= 0; j--) {
          lengths[i][j] =
            left[i] === right[j]
              ? lengths[i + 1][j + 1] + 1
              : Math.max(lengths[i + 1][j], lengths[i][j + 1]);
        }
      }
    }
    let i = 0;
    let j = 0;
    while (i < left.length || j < right.length) {
      if (i < left.length && j < right.length && left[i] === right[j]) {
        aligned.push({
          left: left[i],
          right: right[j],
          leftNumber: ++i,
          rightNumber: ++j,
          changed: false,
          start: false,
        });
        continue;
      }
      const removed: number[] = [];
      const added: number[] = [];
      while (
        (i < left.length || j < right.length) &&
        !(i < left.length && j < right.length && left[i] === right[j])
      ) {
        if (limited) {
          if (i < left.length) removed.push(i++);
          if (j < right.length) added.push(j++);
        } else if (
          j < right.length &&
          (i === left.length || lengths[i][j + 1] >= lengths[i + 1][j])
        ) {
          added.push(j++);
        } else {
          removed.push(i++);
        }
      }
      for (let k = 0; k < Math.max(removed.length, added.length); k++) {
        aligned.push({
          left: removed[k] === undefined ? undefined : left[removed[k]],
          right: added[k] === undefined ? undefined : right[added[k]],
          leftNumber: removed[k] === undefined ? undefined : removed[k] + 1,
          rightNumber: added[k] === undefined ? undefined : added[k] + 1,
          changed: true,
          start: k === 0,
        });
      }
    }
    return { rows: aligned, limited };
  }, [before, after]);
  const changes = rows.filter((row) => row.start).length;
  return (
    <div className="cloud-text-diff">
      <div className="cloud-diff-navigation">
        <span>
          {changes
            ? `${changes} 处差异${changeIndex >= 0 ? ` · 第 ${changeIndex + 1} 处` : ""}`
            : "内容完全相同"}
        </span>
        {[-1, 1].map((direction) => (
          <button
            key={direction}
            type="button"
            disabled={!changes}
            onClick={() => {
              const next =
                changeIndex < 0
                  ? direction > 0
                    ? 0
                    : changes - 1
                  : (changeIndex + direction + changes) % changes;
              setChangeIndex(next);
              const target =
                container.current?.querySelectorAll<HTMLElement>(
                  "[data-change]",
                )[next];
              if (target && container.current)
                container.current.scrollTo({
                  top: target.offsetTop - container.current.offsetTop - 52,
                  behavior: "smooth",
                });
            }}
          >
            {direction < 0 ? "上一处" : "下一处"}
          </button>
        ))}
      </div>
      {limited && (
        <p className="cloud-text-diff-legend">
          文本较长，按行并排展示完整原文；行位置变化可能扩大高亮范围。
        </p>
      )}
      <div
        className="cloud-side-diff"
        ref={container}
        tabIndex={0}
        aria-label={`${beforeLabel}与${afterLabel}内容对比`}
      >
        <div className="cloud-side-diff-head">
          <strong>
            {beforeLabel}
            {before === undefined ? "（文件不存在）" : ""}
          </strong>
          <strong>
            {afterLabel}
            {after === undefined ? "（文件不存在）" : ""}
          </strong>
        </div>
        {rows.map((row, index) => {
          let prefix = 0;
          let suffix = 0;
          if (
            row.changed &&
            row.left !== undefined &&
            row.right !== undefined
          ) {
            while (
              prefix < row.left.length &&
              prefix < row.right.length &&
              row.left[prefix] === row.right[prefix]
            )
              prefix++;
            while (
              suffix < row.left.length - prefix &&
              suffix < row.right.length - prefix &&
              row.left[row.left.length - suffix - 1] ===
                row.right[row.right.length - suffix - 1]
            )
              suffix++;
          }
          return (
            <div
              className="cloud-side-diff-row"
              key={index}
              data-change={row.start ? "true" : undefined}
            >
              {[
                { text: row.left, number: row.leftNumber },
                { text: row.right, number: row.rightNumber },
              ].map((cell, side) => (
                <div
                  key={side}
                  className={`cloud-side-diff-cell${row.changed ? (side ? " is-added" : " is-removed") : ""}${cell.text === undefined ? " is-absent" : ""}`}
                >
                  <span className="cloud-diff-number">{cell.number}</span>
                  <code>
                    {cell.text === undefined ? (
                      ""
                    ) : row.changed ? (
                      <>
                        {cell.text.slice(0, prefix)}
                        <mark>
                          {cell.text.slice(prefix, cell.text.length - suffix) ||
                            " "}
                        </mark>
                        {suffix ? cell.text.slice(-suffix) : ""}
                      </>
                    ) : (
                      cell.text || " "
                    )}
                  </code>
                </div>
              ))}
            </div>
          );
        })}
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
  onResolved: (items: CloudItem[]) => void;
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
  const [changedOnly, setChangedOnly] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
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
  const visible = (changedOnly ? changed : names).filter(
    (name) =>
      path !== "bundles/strategies.zip" ||
      showHistory ||
      activePaths.includes(name),
  );
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
      const result = await resolveCloudStrategies(preview, selections);
      setNotice(
        "已按所选来源合并。各端不同的当前策略和历史资产均已保留为备份。",
      );
      setPreview(null);
      setLoading(true);
      setRevision((value) => value + 1);
      onResolved(result.items);
    } catch (reason) {
      setError(String(reason));
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="cloud-data-review" aria-busy={loading || saving}>
      <div className="cloud-review-toolbar">
        <strong>
          {path === "bundles/strategies.zip"
            ? "选择要使用的策略版本"
            : "查看版本内容"}
        </strong>
        <button
          type="button"
          disabled={disabled || loading || saving}
          onClick={() => {
            setLoading(true);
            setPreview(null);
            setRevision((value) => value + 1);
          }}
        >
          刷新版本
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
          {path === "bundles/strategies.zip" && preview.heads.length > 0 && (
            <div className="cloud-strategy-resolution">
              <p>分别选择要继续使用的打分和筹码策略，未选版本会保留为备份。</p>
              <p>
                本机显示文件修改时间，云端显示上传时间；时间较晚不一定代表内容更合适。
              </p>
              {activePaths.map((name) => {
                const sources = preview.sides.filter(
                  (side) =>
                    side.id !== "base" &&
                    (side.id === "local" || preview.heads.includes(side.id)) &&
                    side.files.some((file) => file.path === name),
                );
                const same =
                  new Set(
                    sources.map(
                      (side) =>
                        side.files.find((file) => file.path === name)?.hash,
                    ),
                  ).size === 1;
                return (
                  <fieldset
                    className="cloud-strategy-choice"
                    key={name}
                    disabled={disabled || saving}
                  >
                    <legend>
                      {name.endsWith("/score_rule.toml")
                        ? "打分策略"
                        : "筹码策略"}
                    </legend>
                    <p>
                      {same
                        ? "各版本内容一致，已保留本机版本。"
                        : selections[name]
                          ? "已选择，可继续确认。"
                          : "内容有差异，请点选要使用的版本。"}
                    </p>
                    <div className="cloud-strategy-choice-options">
                      {sources.map((side) => {
                        const file = side.files.find(
                          (file) => file.path === name,
                        );
                        const time =
                          side.id === "local"
                            ? file?.modifiedAt
                            : side.createdAt;
                        return (
                          <label
                            key={side.id}
                            className={
                              selections[name] === side.id ? "is-selected" : ""
                            }
                          >
                            <input
                              type="radio"
                              name={`strategy-source-${name}`}
                              value={side.id}
                              checked={selections[name] === side.id}
                              onChange={() => {
                                setChoices({ ...choices, [name]: side.id });
                                setFilePath(name);
                                if (side.id !== "local") setRemoteId(side.id);
                              }}
                            />
                            <span>
                              <strong>
                                {side.id === "local"
                                  ? "使用本机版本"
                                  : "使用云端版本"}
                              </strong>
                              <time dateTime={time ?? undefined}>
                                {time
                                  ? new Date(time).toLocaleString("zh-CN", {
                                      hour12: false,
                                    })
                                  : "修改时间未知"}
                              </time>
                              <small>
                                {side.id === "local"
                                  ? "文件修改时间"
                                  : "上传时间"}
                              </small>
                            </span>
                          </label>
                        );
                      })}
                    </div>
                  </fieldset>
                );
              })}
              <div className="cloud-strategy-confirm">
                <span role="status">
                  {canResolve
                    ? "选择已完成"
                    : `还有 ${activePaths.filter((name) => !selections[name]).length} 项需要选择`}
                </span>
                <button
                  type="button"
                  className="settings-primary-btn"
                  disabled={disabled || loading || saving || !canResolve}
                  onClick={() => void resolve()}
                >
                  {saving ? "正在保存…" : "确认使用所选策略"}
                </button>
              </div>
            </div>
          )}
          <details
            className="cloud-review-details"
            open={path !== "bundles/strategies.zip"}
          >
            <summary>查看具体内容差异（可选）</summary>
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
                {cloudSides.map((side) => (
                  <option key={side.id} value={side.id}>
                    云端 ·{" "}
                    {side.createdAt
                      ? new Date(side.createdAt).toLocaleString("zh-CN")
                      : ""}{" "}
                    {preview.heads.includes(side.id) ? "（当前）" : "（历史）"}
                    {side.files.length === local?.files.length &&
                    side.files.every((file) =>
                      local.files.some(
                        (item) =>
                          item.path === file.path && item.hash === file.hash,
                      ),
                    )
                      ? " · 内容与当前本地相同"
                      : " · 内容与当前本地不同"}
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
            {path === "bundles/strategies.zip" && (
              <label className="cloud-review-filter">
                <input
                  type="checkbox"
                  checked={showHistory}
                  onChange={(event) => setShowHistory(event.target.checked)}
                />
                包含历史备份文件（默认只看当前打分、筹码策略）
              </label>
            )}
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
                <p>
                  {names.length
                    ? "当前筛选范围没有差异，可取消“仅看差异”查看原文。"
                    : "暂无可对比文件"}
                </p>
              )}
            </div>
            {selectedPath && (
              <div className="cloud-review-content">
                <strong>{selectedPath}</strong>
                {(localFile?.structured != null ||
                  remoteFile?.structured != null) &&
                (!localFile || localFile.structured != null) &&
                (!remoteFile || remoteFile.structured != null) ? (
                  <>
                    <CloudStrategyDiff
                      key={`${selectedPath}:${remoteId}`}
                      local={localFile}
                      remote={remoteFile}
                      remoteLabel="所选云端版本"
                    />
                    <details>
                      <summary>查看完整原文对比（含排版、注释）</summary>
                      <CloudTextDiff
                        before={localFile?.text}
                        after={remoteFile?.text}
                        beforeLabel="当前本地"
                        afterLabel="所选云端"
                      />
                    </details>
                  </>
                ) : (
                  <CloudTextDiff
                    key={`${selectedPath}:${remoteId}`}
                    before={localFile?.text}
                    after={remoteFile?.text}
                    beforeLabel="当前本地"
                    afterLabel="所选云端版本"
                  />
                )}
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
          </details>
        </>
      )}
    </section>
  );
}
