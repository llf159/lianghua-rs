import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getSceneLayerBacktestDefaults,
  runSceneLayerBacktest,
  type SceneLayerBacktestData,
} from "../../apis/strategyTrigger";
import { readStoredSourcePath } from "../../shared/storage";
import "./css/SceneLayerBacktestPage.css";

function formatDateLabel(value?: string | null) {
  if (!value || value.length !== 8) {
    return "--";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function formatNumber(value?: number | null, digits = 4) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(digits);
}

function normalizeDateInput(value: string) {
  return value.replaceAll("-", "").trim();
}

function compactDateToInput(value?: string | null) {
  if (!value || !/^\d{8}$/.test(value)) {
    return "";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function stateRank(state: string) {
  switch (state) {
    case "fail":
      return 0;
    case "observe":
      return 1;
    case "trigger":
      return 2;
    case "confirm":
      return 3;
    default:
      return 1;
  }
}

function formatPercent(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(digits)}%`;
}

const INDEX_OPTIONS = [
  { value: "000001.SH", label: "上证指数" },
  { value: "399001.SZ", label: "深证成指" },
  { value: "399006.SZ", label: "创业板指" },
  { value: "000300.SH", label: "沪深300" },
  { value: "000905.SH", label: "中证500" },
  { value: "000852.SH", label: "中证1000" },
  { value: "000688.SH", label: "科创50" },
] as const;

export default function SceneLayerBacktestPage() {
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [sceneOptions, setSceneOptions] = useState<string[]>([]);
  const [sceneName, setSceneName] = useState("");
  const [stockAdjType, setStockAdjType] = useState("qfq");
  const [indexTsCode, setIndexTsCode] = useState<string>(INDEX_OPTIONS[0].value);
  const [indexBeta, setIndexBeta] = useState("0.5");
  const [conceptBeta, setConceptBeta] = useState("0.2");
  const [boardBeta, setBoardBeta] = useState("0.0");
  const [startDateInput, setStartDateInput] = useState("");
  const [endDateInput, setEndDateInput] = useState("");
  const [minSamplesPerSceneDay, setMinSamplesPerSceneDay] = useState("5");
  const [backtestPeriod, setBacktestPeriod] = useState("1");
  const [loading, setLoading] = useState(false);
  const [initializing, setInitializing] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<SceneLayerBacktestData | null>(null);

  useEffect(() => {
    let cancelled = false;
    const init = async () => {
      setInitializing(true);
      try {
        const resolved = await ensureManagedSourcePath();
        if (cancelled) {
          return;
        }
        setSourcePath(resolved);

        const defaults = await getSceneLayerBacktestDefaults(resolved);
        if (cancelled) {
          return;
        }
        setSceneOptions(defaults.scene_options ?? []);
        setSceneName(defaults.resolved_scene_name ?? defaults.scene_options?.[0] ?? "");
        setStartDateInput(compactDateToInput(defaults.start_date));
        setEndDateInput(compactDateToInput(defaults.end_date));
      } catch (initError) {
        if (!cancelled) {
          setError(`读取场景默认参数失败: ${String(initError)}`);
        }
      } finally {
        if (!cancelled) {
          setInitializing(false);
        }
      }
    };

    void init();
    return () => {
      cancelled = true;
    };
  }, []);

  const layerSummary = useMemo(() => {
    if (!result || result.points.length === 0) {
      return [] as Array<{ state: string; count: number; avg: number }>;
    }

    const acc = new Map<string, { sum: number; count: number }>();
    for (const point of result.points) {
      for (const item of point.state_avg_residual_returns ?? []) {
        const state = (item.scene_state || "unknown").trim().toLowerCase() || "unknown";
        const value = item.avg_residual_return;
        if (value === null || value === undefined || !Number.isFinite(value)) {
          continue;
        }
        const prev = acc.get(state) ?? { sum: 0, count: 0 };
        prev.sum += value;
        prev.count += 1;
        acc.set(state, prev);
      }
    }

    return Array.from(acc.entries())
      .map(([state, agg]) => ({
        state,
        count: agg.count,
        avg: agg.count > 0 ? agg.sum / agg.count : 0,
      }))
      .sort((a, b) => stateRank(a.state) - stateRank(b.state) || a.state.localeCompare(b.state));
  }, [result]);

  const layerSpreadSummary = useMemo(() => {
    if (layerSummary.length < 2) {
      return null;
    }
    const low = layerSummary[0];
    const high = layerSummary[layerSummary.length - 1];
    return {
      lowState: low.state,
      lowAvg: low.avg,
      highState: high.state,
      highAvg: high.avg,
      spread: high.avg - low.avg,
    };
  }, [layerSummary]);

  const isAllScenesResult = Boolean(result?.is_all_scenes);
  const allSceneSummaries = result?.all_scene_summaries ?? [];

  async function onRunBacktest() {
    const normalizedStart = normalizeDateInput(startDateInput);
    const normalizedEnd = normalizeDateInput(endDateInput);

    if (!sourcePath.trim()) {
      setError("当前数据目录为空，请先在数据管理页确认目录。");
      return;
    }
    if (!sceneName.trim()) {
      setError("请选择场景名。");
      return;
    }
    if (!indexTsCode.trim()) {
      setError("请选择指数。");
      return;
    }
    if (!normalizedStart || !normalizedEnd) {
      setError("请填写开始和结束日期。");
      return;
    }
    if (normalizedStart > normalizedEnd) {
      setError("开始日期不能晚于结束日期。");
      return;
    }

    setLoading(true);
    setError("");
    try {
      const data = await runSceneLayerBacktest({
        sourcePath,
        sceneName: sceneName.trim(),
        stockAdjType: stockAdjType.trim() || "qfq",
        indexTsCode: indexTsCode.trim(),
        indexBeta: Number(indexBeta),
        conceptBeta: Number(conceptBeta),
        boardBeta: Number(boardBeta),
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minSamplesPerSceneDay: Math.max(1, Number(minSamplesPerSceneDay) || 1),
        backtestPeriod: Math.max(1, Number(backtestPeriod) || 1),
      });
      setResult(data);
    } catch (runError) {
      setResult(null);
      setError(`执行场景整体回测失败: ${String(runError)}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="scene-layer-page">
      <section className="scene-layer-card">
        <h2 className="scene-layer-title">场景整体回测</h2>
        <p className="scene-layer-caption">
          使用 scene_details 中的场景状态与排序，计算各场景状态下的分层残差收益、Top-Bottom Spread、IC / ICIR。
        </p>

        <div className="scene-layer-source-note">
          当前数据目录：<strong>{sourcePath || "--"}</strong>
        </div>

        <div className="scene-layer-form-grid">
          <label className="scene-layer-field">
            <span>场景名（现有）</span>
            <select
              value={sceneName}
              onChange={(event) => setSceneName(event.target.value)}
              disabled={initializing || sceneOptions.length === 0}
            >
              {sceneOptions.length === 0 ? <option value="">暂无场景</option> : null}
              {sceneOptions.length > 0 ? <option value="__ALL__">全部场景</option> : null}
              {sceneOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
          <label className="scene-layer-field">
            <span>股票复权</span>
            <input value={stockAdjType} onChange={(event) => setStockAdjType(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>指数</span>
            <select value={indexTsCode} onChange={(event) => setIndexTsCode(event.target.value)}>
              {INDEX_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label className="scene-layer-field">
            <span>指数 Beta</span>
            <input type="number" step="0.01" value={indexBeta} onChange={(event) => setIndexBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>概念 Beta</span>
            <input type="number" step="0.01" value={conceptBeta} onChange={(event) => setConceptBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>板块 Beta</span>
            <input type="number" step="0.01" value={boardBeta} onChange={(event) => setBoardBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>开始日期（scene_details 最早）</span>
            <input type="date" value={startDateInput} onChange={(event) => setStartDateInput(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>结束日期（scene_details 最晚）</span>
            <input type="date" value={endDateInput} onChange={(event) => setEndDateInput(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>场景日最少样本</span>
            <input type="number" min="1" value={minSamplesPerSceneDay} onChange={(event) => setMinSamplesPerSceneDay(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>回测周期（天）</span>
            <input type="number" min="1" value={backtestPeriod} onChange={(event) => setBacktestPeriod(event.target.value)} />
          </label>
        </div>

        <div className="scene-layer-actions">
          <button type="button" className="scene-layer-primary-btn" onClick={() => void onRunBacktest()} disabled={loading || initializing || sceneOptions.length === 0}>
            {loading ? "回测中..." : "执行场景整体回测"}
          </button>
        </div>

        {error ? <div className="scene-layer-error">{error}</div> : null}
      </section>


      {result ? (
        <section className="scene-layer-card">
          <div className="scene-layer-summary-grid">
            <div className="scene-layer-summary-item">
              <span>场景</span>
              <strong>{isAllScenesResult ? "全部场景" : result.scene_name}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>区间</span>
              <strong>{formatDateLabel(result.start_date)} ~ {formatDateLabel(result.end_date)}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>{isAllScenesResult ? "场景数" : "有效交易日"}</span>
              <strong>{isAllScenesResult ? allSceneSummaries.length : result.points.length}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>最小样本阈值</span>
              <strong>{result.min_samples_per_scene_day}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>回测周期（天）</span>
              <strong>{result.backtest_period}</strong>
            </div>
            {!isAllScenesResult ? (
              <>
                <div className="scene-layer-summary-item">
                  <span>Spread 均值（日度高层-低层）</span>
                  <strong>{formatPercent(result.spread_mean)}</strong>
                </div>
                <div className="scene-layer-summary-item">
                  <span>IC 均值</span>
                  <strong>{formatNumber(result.ic_mean)}</strong>
                </div>
                <div className="scene-layer-summary-item">
                  <span>ICIR</span>
                  <strong>{formatNumber(result.icir)}</strong>
                </div>
                <div className="scene-layer-summary-item">
                  <span>板块 Beta</span>
                  <strong>{formatNumber(result.board_beta, 2)}</strong>
                </div>
                <div className="scene-layer-summary-item">
                  <span>分层均值差（整体）</span>
                  <strong>{layerSpreadSummary ? formatPercent(layerSpreadSummary.spread) : "--"}</strong>
                </div>
                <div className="scene-layer-summary-item">
                  <span>高低层对比</span>
                  <strong>
                    {layerSpreadSummary
                      ? `${layerSpreadSummary.highState} - ${layerSpreadSummary.lowState}`
                      : "--"}
                  </strong>
                </div>
              </>
            ) : null}
          </div>

          {isAllScenesResult ? (
            allSceneSummaries.length === 0 ? (
              <div className="scene-layer-empty">当前没有可回测的场景。</div>
            ) : (
              <div className="scene-layer-layer-summary">
                <h3>全部场景汇总（按 Spread 均值降序）</h3>
                <div className="scene-layer-layer-grid">
                  {allSceneSummaries.map((item) => (
                    <div key={item.scene_name} className="scene-layer-layer-item">
                      <span className="scene-layer-layer-state">{item.scene_name}</span>
                      <span>有效交易日：{item.point_count}</span>
                      <span>Spread 均值：{formatPercent(item.spread_mean)}</span>
                      <span>IC 均值：{formatNumber(item.ic_mean)}</span>
                      <span>ICIR：{formatNumber(item.icir)}</span>
                    </div>
                  ))}
                </div>
              </div>
            )
          ) : layerSummary.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>分层差总结</h3>
              <div className="scene-layer-layer-grid">
                {layerSummary.map((item) => (
                  <div key={item.state} className="scene-layer-layer-item">
                    <span className="scene-layer-layer-state">{item.state}</span>
                    <span>均值残差：{formatPercent(item.avg)}</span>
                    <span>出现天数：{item.count}</span>
                  </div>
                ))}
              </div>
              {layerSpreadSummary ? (
                <div className="scene-layer-layer-footnote">
                  整体分层差 = {layerSpreadSummary.highState}({formatPercent(layerSpreadSummary.highAvg)}) - {layerSpreadSummary.lowState}({formatPercent(layerSpreadSummary.lowAvg)}) = <strong>{formatPercent(layerSpreadSummary.spread)}</strong>
                </div>
              ) : null}
            </div>
          ) : null}

          {!isAllScenesResult && result.points.length === 0 ? (
            <div className="scene-layer-empty">当前条件下没有可展示的回测结果。</div>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}
