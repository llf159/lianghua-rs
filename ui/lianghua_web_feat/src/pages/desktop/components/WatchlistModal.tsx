import { useEffect, useMemo, useState } from "react";
import { intradayMonitorPage } from "../../../apis/reader";
import { DEFAULT_DATE_OPTION } from "../../../shared/tradeDate";
import { STOCK_PICK_BOARD_OPTIONS } from "../../../shared/stockPickShared";
import { normalizeTsCode } from "../../../shared/stockCode";
import "../css/WatchlistModal.css";

type WatchlistModalProps = {
  open: boolean;
  sourcePath: string;
  currentCodes: string[];
  onChangeCodes: (codes: string[]) => void;
  onClose: () => void;
};

const MARKET_CAP_PRESETS = [
  { label: "不限", min: "", max: "" },
  { label: "<50亿", min: "", max: "50" },
  { label: "50-200亿", min: "50", max: "200" },
  { label: "200-1000亿", min: "200", max: "1000" },
  { label: ">1000亿", min: "1000", max: "" },
] as const;

type BoardOption = (typeof STOCK_PICK_BOARD_OPTIONS)[number];
type SourceKind = "total" | "scene";

function parseRawCodes(raw: string): { valid: string[]; invalid: string[] } {
  const parts = raw
    .split(/[\s,;|，；、\n]+/)
    .map((item) => item.trim())
    .filter((item) => item !== "");

  const valid: string[] = [];
  const invalid: string[] = [];
  const seen = new Set<string>();

  for (const part of parts) {
    const code = normalizeTsCode(part);
    if (!code) {
      invalid.push(part);
      continue;
    }
    if (!seen.has(code)) {
      seen.add(code);
      valid.push(code);
    }
  }

  return { valid, invalid };
}

function codesToText(codes: string[]): string {
  return codes.join(" ");
}

export default function WatchlistModal({
  open,
  sourcePath,
  currentCodes,
  onChangeCodes,
  onClose,
}: WatchlistModalProps) {
  const [draftText, setDraftText] = useState(() => codesToText(currentCodes));
  const [sourceKind, setSourceKind] = useState<SourceKind>("total");
  const [sourceBoard, setSourceBoard] = useState<BoardOption>("全部");
  const [sourceLimit, setSourceLimit] = useState("200");
  const [sceneName, setSceneName] = useState("");
  const [sceneOptions, setSceneOptions] = useState<string[]>([]);
  const [sourceLoading, setSourceLoading] = useState(false);
  const [sourceMessage, setSourceMessage] = useState("");
  const [sourceError, setSourceError] = useState("");
  const [totalMvMinInput, setTotalMvMinInput] = useState("");
  const [totalMvMaxInput, setTotalMvMaxInput] = useState("");

  const parseResult = useMemo(() => parseRawCodes(draftText), [draftText]);
  const currentCount = parseResult.valid.length;

  useEffect(() => {
    if (!open) return;
    setDraftText(codesToText(currentCodes));
    setSourceMessage("");
    setSourceError("");
  }, [currentCodes, open]);

  useEffect(() => {
    if (!open || sourcePath.trim() === "") {
      setSceneOptions([]);
      return;
    }

    let cancelled = false;
    void intradayMonitorPage({
      sourcePath: sourcePath.trim(),
      rankMode: "scene",
      rankDate: DEFAULT_DATE_OPTION,
      limit: 1,
    })
      .then((data) => {
        if (cancelled) return;
        const nextOptions = Array.from(
          new Set(
            (data.sceneOptions ?? [])
              .map((item) => item.trim())
              .filter((item) => item !== ""),
          ),
        );
        setSceneOptions(nextOptions);
        setSceneName((current) =>
          current !== "" && nextOptions.includes(current)
            ? current
            : (nextOptions[0] ?? ""),
        );
      })
      .catch(() => {
        if (!cancelled) {
          setSceneOptions([]);
          setSceneName("");
        }
      });

    return () => {
      cancelled = true;
    };
  }, [open, sourcePath]);

  if (!open) return null;

  function saveCodes(nextCodes = parseResult.valid) {
    onChangeCodes(nextCodes);
    setDraftText(codesToText(nextCodes));
  }

  function parseMarketCapRange() {
    const minText = totalMvMinInput.trim();
    const maxText = totalMvMaxInput.trim();
    const totalMvMin = minText ? Number(minText) : undefined;
    const totalMvMax = maxText ? Number(maxText) : undefined;

    if (
      (minText && !Number.isFinite(totalMvMin)) ||
      (maxText && !Number.isFinite(totalMvMax))
    ) {
      return {
        error: "市值筛选必须输入数字。",
        totalMvMin: undefined,
        totalMvMax: undefined,
      };
    }

    if (
      totalMvMin !== undefined &&
      totalMvMax !== undefined &&
      totalMvMin > totalMvMax
    ) {
      return {
        error: "市值下限不能大于上限。",
        totalMvMin: undefined,
        totalMvMax: undefined,
      };
    }

    return { error: "", totalMvMin, totalMvMax };
  }

  function parseSourceLimit() {
    const trimmed = sourceLimit.trim();
    const limit = trimmed ? Number(trimmed) : 200;
    if (!Number.isInteger(limit) || limit <= 0) {
      return { error: "排名前 N 只必须是正整数。", limit: 200 };
    }
    return {
      error: "",
      limit: Math.max(1, Math.min(1000, limit)),
    };
  }

  async function applySourceCodes() {
    const trimmedSourcePath = sourcePath.trim();
    if (trimmedSourcePath === "") {
      setSourceError("请先到数据管理页确认目录。");
      setSourceMessage("");
      return;
    }

    const {
      error: marketCapError,
      totalMvMin,
      totalMvMax,
    } = parseMarketCapRange();
    if (marketCapError) {
      setSourceError(marketCapError);
      setSourceMessage("");
      return;
    }

    const { error: limitError, limit } = parseSourceLimit();
    if (limitError) {
      setSourceError(limitError);
      setSourceMessage("");
      return;
    }

    setSourceLoading(true);
    setSourceError("");
    setSourceMessage("");
    try {
      const result = await intradayMonitorPage({
        sourcePath: trimmedSourcePath,
        rankMode: sourceKind,
        rankDate: DEFAULT_DATE_OPTION,
        sceneName: sourceKind === "scene" && sceneName ? sceneName : undefined,
        limit,
        board: sourceBoard === "全部" ? undefined : sourceBoard,
        totalMvMin,
        totalMvMax,
      });
      const nextCodes = Array.from(
        new Set(
          (result.rows ?? [])
            .map((row) => row.ts_code.trim())
            .filter((code) => code !== ""),
        ),
      );
      onChangeCodes(nextCodes);
      setDraftText(codesToText(nextCodes));
      setSourceMessage(`已取得 ${nextCodes.length} 只，已写入待保存名单。`);
    } catch (error) {
      setSourceError(`取得名单失败: ${String(error)}`);
    } finally {
      setSourceLoading(false);
    }
  }

  function handleClear() {
    saveCodes([]);
  }

  function handleSave() {
    saveCodes();
    onClose();
  }

  return (
    <div className="watchlist-modal-mask" onClick={onClose}>
      <div
        className="watchlist-modal"
        role="dialog"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="watchlist-modal-head">
          <div>
            <h4>名单管理</h4>
            <p>当前待保存 {currentCount} 只，开启名单模式后仅刷新这些股票。</p>
          </div>
          <button
            type="button"
            className="watchlist-modal-close"
            onClick={onClose}
          >
            关闭
          </button>
        </div>

        <div className="watchlist-modal-summary watchlist-modal-section">
          <div>
            <strong>{currentCount}</strong>
            <span>有效代码</span>
          </div>
          <div>
            <strong>{parseResult.invalid.length}</strong>
            <span>无法识别</span>
          </div>
          <div>
            <strong>{sceneOptions.length}</strong>
            <span>可选场景</span>
          </div>
        </div>

        <div className="watchlist-modal-section watchlist-modal-panel">
          <div className="watchlist-modal-panel-head">
            <label className="watchlist-modal-label">从榜单生成</label>
            <span>板块和总市值筛选会应用到本次取名单</span>
          </div>
          <div className="watchlist-source-grid">
            <label className="watchlist-modal-field">
              <span>来源</span>
              <select
                className="watchlist-modal-select"
                value={sourceKind}
                onChange={(event) =>
                  setSourceKind(
                    event.target.value === "scene" ? "scene" : "total",
                  )
                }
              >
                <option value="total">排名</option>
                <option value="scene">场景</option>
              </select>
            </label>
            {sourceKind === "scene" ? (
              <label className="watchlist-modal-field">
                <span>场景</span>
                <select
                  className="watchlist-modal-select"
                  value={sceneName}
                  onChange={(event) => setSceneName(event.target.value)}
                >
                  {sceneOptions.map((item) => (
                    <option key={item} value={item}>
                      {item}
                    </option>
                  ))}
                  {sceneOptions.length === 0 ? (
                    <option value="">暂无场景</option>
                  ) : null}
                </select>
              </label>
            ) : null}
            <label className="watchlist-modal-field">
              <span>板块</span>
              <select
                className="watchlist-modal-select"
                value={sourceBoard}
                onChange={(event) =>
                  setSourceBoard(event.target.value as BoardOption)
                }
              >
                {STOCK_PICK_BOARD_OPTIONS.map((board) => (
                  <option key={board} value={board}>
                    {board}
                  </option>
                ))}
              </select>
            </label>
            <label className="watchlist-modal-field">
              <span>排名前 N 只</span>
              <input
                type="number"
                className="watchlist-modal-num-input"
                value={sourceLimit}
                min={1}
                max={1000}
                onChange={(event) => setSourceLimit(event.target.value)}
              />
            </label>
            <label className="watchlist-modal-field">
              <span>总市值最小(亿)</span>
              <input
                type="number"
                step={0.01}
                value={totalMvMinInput}
                onChange={(event) => setTotalMvMinInput(event.target.value)}
                placeholder="不限"
              />
            </label>
            <label className="watchlist-modal-field">
              <span>总市值最大(亿)</span>
              <input
                type="number"
                step={0.01}
                value={totalMvMaxInput}
                onChange={(event) => setTotalMvMaxInput(event.target.value)}
                placeholder="不限"
              />
            </label>
            <div
              className="watchlist-market-presets"
              role="group"
              aria-label="本次取名单的市值快捷筛选"
            >
              {MARKET_CAP_PRESETS.map((preset) => (
                <button
                  key={preset.label}
                  type="button"
                  className={
                    totalMvMinInput === preset.min &&
                    totalMvMaxInput === preset.max
                      ? "is-active"
                      : ""
                  }
                  onClick={() => {
                    setTotalMvMinInput(preset.min);
                    setTotalMvMaxInput(preset.max);
                  }}
                >
                  {preset.label}
                </button>
              ))}
            </div>
            <div className="watchlist-source-actions">
              <button
                type="button"
                className="watchlist-modal-btn watchlist-modal-import-btn"
                onClick={() => void applySourceCodes()}
                disabled={sourceLoading}
              >
                {sourceLoading ? "取得中..." : "生成名单"}
              </button>
            </div>
          </div>
          {sourceMessage ? (
            <div className="watchlist-source-message">{sourceMessage}</div>
          ) : null}
          {sourceError ? (
            <div className="watchlist-source-error">{sourceError}</div>
          ) : null}
        </div>

        <div className="watchlist-modal-section watchlist-modal-panel">
          <div className="watchlist-modal-panel-head">
            <label className="watchlist-modal-label">批量编辑</label>
            <span>支持逗号、空格、换行和中文分隔符</span>
          </div>
          <textarea
            className="watchlist-modal-textarea"
            value={draftText}
            onChange={(event) => setDraftText(event.target.value)}
            placeholder={
              "示例：000001.SZ 600000.SH 300750.SZ\n也支持逗号、分号、换行"
            }
            rows={6}
          />
          <div className="watchlist-modal-parse-feedback">
            {parseResult.valid.length > 0 ? (
              <span className="watchlist-parse-ok">
                已识别 {parseResult.valid.length} 只
              </span>
            ) : null}
            {parseResult.invalid.length > 0 ? (
              <span className="watchlist-parse-err">
                {parseResult.invalid.length} 个无法识别:{" "}
                {parseResult.invalid.join("、")}
              </span>
            ) : null}
          </div>
        </div>

        <div className="watchlist-modal-actions">
          <button
            type="button"
            className="watchlist-modal-btn watchlist-modal-btn-secondary"
            onClick={handleClear}
          >
            清空名单
          </button>
          <div className="watchlist-modal-action-group">
            <button
              type="button"
              className="watchlist-modal-btn watchlist-modal-btn-secondary"
              onClick={onClose}
            >
              取消
            </button>
            <button
              type="button"
              className="watchlist-modal-btn watchlist-modal-btn-primary"
              onClick={handleSave}
            >
              保存名单
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
