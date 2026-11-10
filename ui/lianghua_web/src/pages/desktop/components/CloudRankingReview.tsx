import { useEffect, useState } from "react";
import {
  previewCloudData,
  resolveCloudRanking,
  type CloudItem,
  type CloudPreview,
} from "../../../apis/cloudSync";

export default function CloudRankingReview({
  path,
  versionId,
  disabled,
  onBusyChange,
  onResolved,
}: {
  path: string;
  versionId?: string;
  disabled: boolean;
  onBusyChange: (busy: boolean) => void;
  onResolved: (items: CloudItem[]) => void;
}) {
  const [preview, setPreview] = useState<CloudPreview | null>(null);
  const [source, setSource] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let cancelled = false;
    setBusy(true);
    setPreview(null);
    setSource("");
    setConfirmed(false);
    setError("");
    void previewCloudData(path, versionId)
      .then((value) => {
        if (!cancelled) setPreview(value);
      })
      .catch((reason) => {
        if (!cancelled) setError(String(reason));
      })
      .finally(() => {
        if (!cancelled) setBusy(false);
      });
    return () => {
      cancelled = true;
    };
  }, [path, versionId, revision]);
  useEffect(() => {
    onBusyChange(busy);
    return () => onBusyChange(false);
  }, [busy, onBusyChange]);
  const selected = preview?.sides.find((side) => side.id === source);
  const overwrite =
    selected?.dates.filter((date) => preview?.localDates.includes(date)) ?? [];
  return (
    <section className="cloud-ranking-review" aria-busy={busy}>
      <div className="cloud-ranking-toolbar">
        <div>
          <h4>选择排名来源</h4>
          <p>排名与计算策略一起处理</p>
        </div>
        <button
          type="button"
          disabled={busy || disabled}
          onClick={() => setRevision((value) => value + 1)}
        >
          刷新
        </button>
      </div>
      {busy && (
        <p className="cloud-ranking-status" role="status">
          正在读取并校验排名…
        </p>
      )}
      {error && (
        <p className="cloud-error" role="alert">
          {error}
        </p>
      )}
      {preview && (
        <>
          <div className="cloud-ranking-local-summary">
            <span>本地历史</span>
            <strong>{preview.localDates.length} 个结果日期</strong>
            <span>
              {preview.localDates.length
                ? `${preview.localDates[0]} — ${preview.localDates.at(-1)}`
                : "暂无排名"}
            </span>
          </div>
          <p className="cloud-ranking-hint">
            总榜包只包含最新结果所用策略对应的日期。同快照可按日更新；不同或未知来源须确认整库替换，不创建备份。
          </p>
          <div className="cloud-ranking-sources">
            {preview.sides
              .filter((side) => side.id !== "base")
              .map((side, index) => (
                <article
                  className={`cloud-ranking-source${source === side.id ? " is-selected" : ""}${side.receiveError ? " is-unavailable" : ""}`}
                  key={side.id}
                >
                  <label className="cloud-ranking-source-choice">
                    <input
                      type="radio"
                      name="ranking-source"
                      checked={source === side.id}
                      disabled={
                        busy ||
                        disabled ||
                        !side.dates.length ||
                        !!side.receiveError
                      }
                      onChange={() => {
                        setSource(side.id);
                        setConfirmed(false);
                      }}
                    />
                    <span>
                      <strong>
                        {side.id === "local"
                          ? "使用本机"
                          : `使用云端${preview.heads.includes(side.id) ? "分支" : "历史版本"} ${index}`}
                      </strong>
                      <small>
                        {side.id === "local"
                          ? "保留本地，发布本机排名包"
                          : side.createdAt
                            ? new Date(side.createdAt).toLocaleString("zh-CN")
                            : "云端排名包"}
                      </small>
                    </span>
                    <em
                      className={
                        side.id !== "local" && side.replaceAll
                          ? "is-destructive"
                          : ""
                      }
                    >
                      {side.id === "local"
                        ? "本机"
                        : side.receiveError
                          ? "不可接收"
                          : side.replaceAll
                            ? "整库替换"
                            : "按日更新"}
                    </em>
                  </label>
                  <div className="cloud-ranking-source-body">
                    <div className="cloud-ranking-date-label">
                      <span>包内日期</span>
                      <strong>{side.dates.length} 天</strong>
                    </div>
                    <div className="cloud-ranking-dates">
                      {side.dates.length ? (
                        side.dates.map((date) => <span key={date}>{date}</span>)
                      ) : (
                        <span>暂无结果</span>
                      )}
                    </div>
                    {side.receiveError && (
                      <p className="cloud-ranking-source-error">
                        {side.receiveError}
                      </p>
                    )}
                    {path.startsWith("ranking/") && (
                      <details className="cloud-ranking-strategy">
                        <summary>查看计算策略</summary>
                        {side.files.some((file) =>
                          file.path.startsWith("策略快照/"),
                        ) ? (
                          side.files
                            .filter((file) => file.path.startsWith("策略快照/"))
                            .map((file) => (
                              <pre key={file.path}>{file.text}</pre>
                            ))
                        ) : (
                          <p>旧结果未记录策略来源。</p>
                        )}
                      </details>
                    )}
                  </div>
                </article>
              ))}
          </div>
          {selected && (
            <div
              className={`cloud-ranking-decision${selected.replaceAll && source !== "local" ? " is-destructive" : ""}`}
            >
              <h4>
                {source === "local"
                  ? "发布本机排名"
                  : selected.replaceAll
                    ? "替换本地全部排名"
                    : "更新本地排名"}
              </h4>
              {source === "local" ? (
                <p>
                  发布包内 {selected.dates.length} 个日期。本地{" "}
                  {preview.localDates.length}{" "}
                  个历史日期保留，云端当前分支由本机包取代。
                </p>
              ) : selected.replaceAll ? (
                <>
                  <div className="cloud-ranking-replacement">
                    <div>
                      <span>删除本地全部历史</span>
                      <strong>
                        {preview.localDates.length}
                        <small> 个日期</small>
                      </strong>
                    </div>
                    <span aria-hidden="true">→</span>
                    <div>
                      <span>接收云端包</span>
                      <strong>
                        {selected.dates.length}
                        <small> 个日期</small>
                      </strong>
                    </div>
                  </div>
                  <p>
                    策略快照不同或来源无法确认。全部本地排名及关联明细将删除，只保留所选云端包；不备份，不能通过本次操作撤销。
                  </p>
                </>
              ) : (
                <>
                  <p>替换日期：{overwrite.join("、") || "无"}</p>
                  <p>
                    新增日期：
                    {selected.dates
                      .filter((date) => !preview.localDates.includes(date))
                      .join("、") || "无"}
                  </p>
                  <p>
                    其余 {preview.localDates.length - overwrite.length}{" "}
                    个历史日期保留。不备份被替换的数据。
                  </p>
                </>
              )}
              {source !== "local" && (
                <p className="cloud-ranking-hint">
                  计算策略随排名接收，当前编辑策略保持原样。云端当前分支将按所选版本统一。
                  {path.startsWith("similarity/")
                    ? "同时采用包内的相似榜参数配置。"
                    : ""}
                </p>
              )}
              <label className="cloud-ranking-confirm">
                <input
                  type="checkbox"
                  checked={confirmed}
                  disabled={busy || disabled}
                  onChange={(event) => setConfirmed(event.target.checked)}
                />
                <span>
                  {source !== "local" && selected.replaceAll
                    ? `我确认删除本地全部 ${preview.localDates.length} 个日期的排名，且不备份`
                    : "我确认以上来源和更新范围，不创建备份"}
                </span>
              </label>
              <button
                type="button"
                className={
                  selected.replaceAll && source !== "local"
                    ? "cloud-ranking-submit is-destructive"
                    : "cloud-ranking-submit"
                }
                disabled={
                  busy || disabled || !confirmed || !!selected.receiveError
                }
                onClick={() => {
                  setBusy(true);
                  setError("");
                  void resolveCloudRanking(preview, source)
                    .then((result) => onResolved(result.items))
                    .catch((reason) => {
                      setError(String(reason));
                      setConfirmed(false);
                    })
                    .finally(() => setBusy(false));
                }}
              >
                {busy
                  ? "正在处理…"
                  : source === "local"
                    ? "确认发布本机排名"
                    : selected.replaceAll
                      ? "确认删除全部历史并接收"
                      : "确认更新所列日期"}
              </button>
            </div>
          )}
        </>
      )}
    </section>
  );
}
