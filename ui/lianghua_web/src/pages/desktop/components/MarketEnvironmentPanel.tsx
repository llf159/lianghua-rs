import { useEffect, useRef, useState } from "react";
import {
  getMarketEnvironment,
  type MarketEnvironmentData,
  type MarketEnvironmentDimension,
  type MarketEnvironmentGroup,
  type MarketEnvironmentPoint,
} from "../../../apis/strategyTrigger";
import "../css/MarketEnvironmentPanel.css";

const GROUP_ORDER: MarketEnvironmentGroup[] = ["direction", "volume", "risk"];
const GROUP_LABELS: Record<MarketEnvironmentGroup, string> = {
  direction: "方向与强弱",
  volume: "量价与领涨结构",
  risk: "波动与风险",
};

function formatDate(value?: string | null) {
  return value?.length === 8
    ? `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
    : "--";
}

function formatNumber(value: number | null | undefined, unit: string) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  const digits = unit === "%" || unit === "个百分点" ? 2 : 3;
  const text = Math.abs(value) < 0.0005 ? (0).toFixed(digits) : value.toFixed(digits);
  return unit ? `${text} ${unit}` : text;
}

function formatChange(value: number | null | undefined, unit: string) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  const digits = unit === "%" || unit === "个百分点" ? 2 : 3;
  if (Math.abs(value) < 0.0005) {
    return `持平 0.${"0".repeat(digits)}${unit ? ` ${unit}` : ""}`;
  }
  return `${value > 0 ? "↑ +" : "↓ -"}${Math.abs(value).toFixed(digits)}${unit ? ` ${unit}` : ""}`;
}

function formatPercent(value?: number | null, digits = 1) {
  return value === null || value === undefined || !Number.isFinite(value)
    ? "--"
    : `${(value * 100).toFixed(digits)}%`;
}

function formatSignedPercent(value?: number | null) {
  return value === null || value === undefined || !Number.isFinite(value)
    ? "--"
    : `${value > 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function HistorySparkline({ points }: { points: MarketEnvironmentPoint[] }) {
  const values = points
    .map((point) => point.value)
    .filter((value): value is number => value !== null && Number.isFinite(value));
  if (values.length < 2) {
    return <p className="market-env-history-empty">历史样本不足</p>;
  }
  const low = Math.min(...values);
  const high = Math.max(...values);
  const span = high - low || 1;
  const width = 280;
  const height = 46;
  const pad = 3;
  const x = (index: number) =>
    pad + ((width - pad * 2) * index) / Math.max(1, points.length - 1);
  const y = (value: number) =>
    pad + (height - pad * 2) * (1 - (value - low) / span);
  let path = "";
  let drawing = false;
  points.forEach((point, index) => {
    if (point.value === null || !Number.isFinite(point.value)) {
      drawing = false;
      return;
    }
    path += `${drawing ? "L" : "M"}${x(index).toFixed(1)},${y(point.value).toFixed(1)} `;
    drawing = true;
  });
  return (
    <svg
      className="market-env-sparkline"
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={`近${points.length}个交易日走势`}
    >
      <line x1={pad} x2={width - pad} y1={height - pad} y2={height - pad} />
      <path d={path.trim()} />
    </svg>
  );
}

function DimensionRow({ dimension }: { dimension: MarketEnvironmentDimension }) {
  const first = dimension.history[0]?.trade_date;
  const last = dimension.history.at(-1)?.trade_date;
  return (
    <article className="market-env-reading">
      <div className="market-env-reading-head">
        <span>{dimension.label}</span>
        <strong>{formatNumber(dimension.value, dimension.unit)}</strong>
      </div>
      <div className="market-env-reading-meta">
        <span>近5日变化 {formatChange(dimension.change_5d, dimension.unit)}</span>
        <span>
          {dimension.value === null
            ? "样本不足"
            : `${dimension.sample_count.toLocaleString("zh-CN")} ${dimension.sample_label}`}
        </span>
      </div>
      <p>{dimension.interpretation}</p>
      <details className="market-env-history">
        <summary>近20日走势</summary>
        <HistorySparkline points={dimension.history} />
        <span>
          {formatDate(first)} 至 {formatDate(last)} · 数值按展示单位换算
        </span>
      </details>
    </article>
  );
}

export default function MarketEnvironmentPanel({
  sourcePath,
  referenceTradeDate,
}: {
  sourcePath: string;
  referenceTradeDate?: string;
}) {
  const [data, setData] = useState<MarketEnvironmentData | null>(null);
  const [loadedKey, setLoadedKey] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const requestId = useRef(0);
  const requestKey = `${sourcePath}|${referenceTradeDate ?? ""}`;

  useEffect(() => {
    requestId.current += 1;
    setData(null);
    setLoadedKey("");
    setError("");
    setLoading(false);
  }, [sourcePath, referenceTradeDate]);

  useEffect(
    () => () => {
      requestId.current += 1;
    },
    [],
  );

  const refresh = async () => {
    const id = ++requestId.current;
    const key = requestKey;
    setLoading(true);
    setError("");
    setData(null);
    setLoadedKey("");
    try {
      const next = await getMarketEnvironment({ sourcePath, referenceTradeDate });
      if (id === requestId.current) {
        setData(next);
        setLoadedKey(key);
      }
    } catch (failure) {
      if (id === requestId.current) {
        setError(`读取市场环境失败：${String(failure)}`);
      }
    } finally {
      if (id === requestId.current) {
        setLoading(false);
      }
    }
  };

  const visibleData = loadedKey === requestKey ? data : null;
  const coverage = visibleData?.coverage;
  const reversal = visibleData?.reversal;
  const grouped = GROUP_ORDER.map((group) => ({
    group,
    label: GROUP_LABELS[group],
    items: visibleData?.dimensions.filter((dimension) => dimension.group === group) ?? [],
  })).filter((group) => group.items.length > 0);

  return (
    <section className="scene-layer-card market-environment" aria-busy={loading}>
      <div className="market-env-heading">
        <div>
          <h3>市场环境</h3>
        </div>
        <button
          type="button"
          className="scene-layer-primary-btn"
          disabled={loading}
          onClick={() => void refresh()}
        >
          {loading ? "计算中..." : "刷新环境"}
        </button>
      </div>

      {error ? (
        <div className="scene-layer-error" role="alert">
          {error}
        </div>
      ) : null}
      {loading ? (
        <p className="scene-layer-caption" role="status">
          正在读取原始日线并聚合全市场截面...
        </p>
      ) : null}
      {!loading && !visibleData && !error ? (
        <p className="scene-layer-caption">尚未读取。点击“刷新环境”后按当前参考日计算。</p>
      ) : null}

      {visibleData ? (
        <>
          <div className="market-env-summary">
            <div className="market-env-dates">
              <span>截至 {formatDate(visibleData.analysis_trade_date ?? visibleData.reference_trade_date)}</span>
              <span>全市场 · 近5／10个交易日</span>
            </div>
            {visibleData.fallback_reason ? (
              <p className="market-env-notice">{visibleData.fallback_reason}</p>
            ) : null}
            <h4 className="market-env-verdict">{visibleData.headline}</h4>
            {visibleData.status === "ok" ? (
              <div className="market-env-returns">
                <span>等权近5日 <b>{formatSignedPercent(visibleData.return_5d_pct)}</b></span>
                <span>近10日 <b>{formatSignedPercent(visibleData.return_10d_pct)}</b></span>
                <span>近10日上涨股票 <b>{formatNumber(visibleData.breadth_10d_pct, "%")}</b></span>
              </div>
            ) : null}
            <div className="market-env-narrative">
              {visibleData.summary.split("\n\n").map((paragraph, index) => (
                <p key={index}>{paragraph}</p>
              ))}
            </div>
            {reversal ? (
              <div className="market-env-reversal">
                <div className="market-env-reading-head">
                  <span>{reversal.direction === "up" ? "向上" : reversal.direction === "down" ? "向下" : "方向"}反转概率 · 未来{reversal.horizon_sessions}个交易日</span>
                  <strong>
                    {reversal.status === "not_applicable" ? "不适用"
                      : reversal.probability === null ? "数据不足"
                        : reversal.probability < 0.001 ? "<0.1%"
                          : reversal.probability > 0.999 ? ">99.9%"
                            : formatPercent(reversal.probability, 1)}
                  </strong>
                </div>
                <p>{reversal.reason}</p>
              </div>
            ) : null}
          </div>

          <details className="market-env-evidence" key={loadedKey}>
            <summary>查看判断依据与数据</summary>
            <div className="market-env-dates">
              <span>参考日 {formatDate(visibleData.reference_trade_date)}</span>
              <span>库中最新行情 {formatDate(visibleData.latest_trade_date)}</span>
              {visibleData.phase ? <span>{visibleData.phase}</span> : null}
            </div>
            {reversal ? (
              <details className="market-env-other">
                <summary>反转概率的定义与依据</summary>
                <p>沿用当前10日整体方向。未来第4、5个交易日均转为相反方向，且两日收盘均越过本分析日的固定等权市场参考位，才算形成并站住反转；最后一天刚翻向、短暂反弹或回撤均不算。震荡时此事件不适用。</p>
                {reversal.method ? (
                  <>
                    <p>本次使用{reversal.method === "evidence" ? "完整证据模型" : reversal.method === "price" ? "价格模型（10日方向、5日及当日价格运动）" : "同方向历史发生率"}。完整证据模型支持不足时采用收缩价格模型；价格条件仍不足时使用有真实样本支持的平滑历史频率。</p>
                    <p>截至 {formatDate(visibleData.analysis_trade_date)} · {reversal.version} · 建议复核日 {formatDate(reversal.review_after)}</p>
                    <p>本次估计使用 {reversal.training_days} 个历史日，其中反转 {reversal.training_successes} 日；最后训练参考日 {formatDate(reversal.training_last_date)}，标签已知截至 {formatDate(reversal.training_labels_through)}。该方向此前向前评价 {reversal.evaluation_days} 日，其中反转 {reversal.evaluation_successes} 日。</p>
                    <p>市场日期为样本，连续日期的结果会重叠。模型按季度冻结；选取分析日之前最近可用版本，超过建议复核日会提示更新，刷新不会自动增加训练样本。</p>
                  </>
                ) : null}
                {reversal.evidence.length > 0 ? (
                  <>
                    <p>以下为相对训练均值的模型贡献：正值提高所示方向反转估计，负值降低；单位为对数几率，不是百分点，也不是独立因果证据。</p>
                    <div className="market-env-reading-meta">
                      {reversal.evidence.map((item) => (
                        <span key={item.label}>{item.label} {item.contribution > 0 ? "+" : ""}{item.contribution.toFixed(2)}</span>
                      ))}
                    </div>
                  </>
                ) : null}
              </details>
            ) : null}
            {coverage ? (
              <p className="market-env-coverage">
                应有样本 {coverage.expected_stocks.toLocaleString("zh-CN")} 只 · 报价{" "}
                {coverage.quoted_stocks.toLocaleString("zh-CN")} 只 · 覆盖{" "}
                {formatPercent(coverage.ratio)}（要求 {formatPercent(coverage.required_ratio, 0)}） ·
                停牌或缺口 {coverage.suspended_or_gapped_stocks.toLocaleString("zh-CN")} 只 ·
                行情缺失 {coverage.missing_stocks.toLocaleString("zh-CN")} 只 · 上市资格未知{" "}
                {coverage.unknown_listing_stocks.toLocaleString("zh-CN")} 只
              </p>
            ) : null}
            {visibleData.feedback.length > 0 ? (
              <div className="market-env-feedback">
                <h4>近期进退 · 近5个交易日</h4>
                {visibleData.feedback.map((reading) => (
                  <article className="market-env-feedback-row" key={reading.key}>
                    <div className="market-env-reading-head">
                      <span>{reading.label}</span>
                      <strong>{formatNumber(reading.rate_pct, "%")}</strong>
                    </div>
                    <p>{reading.description}</p>
                    <div className="market-env-reading-meta">
                      <span>较前5日 {formatChange(reading.change_5d_pp, "个百分点")}</span>
                      <span>
                        {reading.successes.toLocaleString("zh-CN")} / {reading.events.toLocaleString("zh-CN")} 次 ·
                        涉及 {reading.stocks.toLocaleString("zh-CN")} 只股票
                      </span>
                    </div>
                  </article>
                ))}
              </div>
            ) : null}
            {grouped.length > 0 ? (
              <details className="market-env-other">
                <summary>更多环境读数与短历史</summary>
                <div className="market-env-groups">
                  {grouped.map((group) => (
                    <section className="market-env-group" key={group.group}>
                      <h4>{group.label}</h4>
                      {group.items.map((dimension) => (
                        <DimensionRow key={dimension.key} dimension={dimension} />
                      ))}
                    </section>
                  ))}
                </div>
              </details>
            ) : null}
          </details>
        </>
      ) : null}
    </section>
  );
}
