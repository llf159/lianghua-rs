import { useState } from "react";
import {
  getMarketStructure,
  type MarketStructureData,
  type StructureEventKind,
  type StructureState,
} from "../../../apis/strategyTrigger";
import "../css/MarketStructurePanel.css";

function formatDate(value: string | null | undefined) {
  return value?.length === 8
    ? `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
    : "--";
}

function formatPrice(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? "--" : value.toFixed(2);
}

function formatActivityRatio(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value.toFixed(3)}倍 · ${value > 1 ? "放量" : value < 1 ? "缩量" : "持平"}`;
}

function formatSignedPercent(value: number) {
  return `${value > 0 ? "+" : ""}${value.toFixed(3)}`;
}

const stateLabels: Record<StructureState, string> = {
  up: "上涨结构有效",
  down: "下降结构有效",
  up_broken: "上涨结构已失效",
  down_broken: "下降结构已失效",
  insufficient: "结构尚未形成",
};

const eventLabels: Record<StructureEventKind, string> = {
  up_started: "上涨转换启动",
  down_started: "下降转换启动",
  up_renewed: "上涨N形延续",
  down_renewed: "下降N形延续",
  up_invalidated: "跌破上涨防线",
  down_invalidated: "突破下降防线",
  up_reclaimed: "收回上涨防线",
  down_reclaimed: "回到下降防线下",
  failed_up_breakout: "向上突破失败",
  failed_down_breakout: "向下突破失败",
};

export default function MarketStructurePanel({
  sourcePath,
  referenceTradeDate,
}: {
  sourcePath: string;
  referenceTradeDate?: string;
}) {
  const [data, setData] = useState<MarketStructureData | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [degree, setDegree] = useState(0);
  const [activityScope, setActivityScope] = useState<"all_a" | "sh_sz">("all_a");
  const activity = data?.activity;
  const totals = activity?.scopes.find((scope) => scope.scope === activityScope);

  return (
    <section className="scene-layer-card market-structure" aria-busy={loading}>
      <div className="market-structure-heading">
        <div>
          <h3>市场结构周期</h3>
          <p className="scene-layer-caption">
            守住防线则持续，破位则改变，收回则重新核对；点击刷新后按当前参考日读取。
          </p>
        </div>
        <button
          type="button"
          className="scene-layer-primary-btn"
          disabled={loading}
          onClick={async () => {
            setLoading(true);
            setError("");
            setData(null);
            try {
              setData(
                await getMarketStructure({ sourcePath, referenceTradeDate }),
              );
            } catch (failure) {
              setError(`读取结构周期失败：${String(failure)}`);
            } finally {
              setLoading(false);
            }
          }}
        >
          {loading ? "读取中..." : "刷新周期"}
        </button>
      </div>
      {error ? (
        <div className="scene-layer-error" role="alert">
          {error}
        </div>
      ) : null}
      {loading ? (
        <p className="scene-layer-caption" role="status">
          正在回放本地指数结构...
        </p>
      ) : null}
      {!loading && !data && !error ? (
        <p className="scene-layer-caption">
          点击“刷新周期”读取当前参考日的结构。
        </p>
      ) : null}
      {data ? (
        <>
          <div className="market-structure-summary" role="status">
            <span>行情截至 {formatDate(data.trade_date)}</span>
            <strong>{data.summary}</strong>
            {data.trade_date !== data.latest_trade_date &&
            data.latest_trade_date ? (
              <span>
                历史参考日 · 库中最新行情 {formatDate(data.latest_trade_date)}
              </span>
            ) : null}
          </div>
          {activity ? (
            <section className="market-structure-activity" aria-label="市场总体量能">
              <div className="market-structure-heading">
                <div>
                  <h4>市场总体量能</h4>
                  <p className="scene-layer-caption">
                    {activityScope === "all_a" ? "沪深北" : "沪深"}股票日线汇总 · {totals?.observed_stocks ?? 0}只 · {formatDate(data.trade_date)}
                  </p>
                </div>
                <select
                  aria-label="量能汇总范围"
                  value={activityScope}
                  onChange={(event) => setActivityScope(event.target.value as "all_a" | "sh_sz")}
                >
                  <option value="all_a">沪深北</option>
                  <option value="sh_sz">沪深</option>
                </select>
              </div>
              {totals ? (
                <div className="market-structure-activity-readings">
                  {[
                    { label: "总成交量", values: totals.volume, unit: "亿手", scale: 100_000_000 },
                    { label: "总成交额", values: totals.amount, unit: "亿元", scale: 100_000_000 },
                  ].map(({ label, values, unit, scale }) => (
                    <article key={label}>
                      <span>{label}</span>
                      <strong>
                        {values.total == null ? "--" : `${(values.total / scale).toFixed(2)} ${unit}`}
                      </strong>
                      <dl>
                        <div><dt>当日 / 前20日</dt><dd>{formatActivityRatio(values.day_ratio)}</dd></div>
                        <div><dt>近5日 / 更早20日</dt><dd>{formatActivityRatio(values.week_ratio)}</dd></div>
                        <div><dt>近5日 / 前5日</dt><dd>{formatActivityRatio(values.week_change)}</dd></div>
                      </dl>
                    </article>
                  ))}
                </div>
              ) : null}
              {totals && (totals.invalid_volume_stocks > 0 || totals.invalid_amount_stocks > 0) ? (
                <p className="scene-layer-caption">
                  当日无效成交量 {totals.invalid_volume_stocks}只、成交额 {totals.invalid_amount_stocks}只，对应总量暂缺。
                </p>
              ) : null}
              <p className="scene-layer-caption">
                各窗口取日均值，分母不含分子窗口；1倍为基准。缺少完整窗口或成交数据时显示“--”。
              </p>
              <div className="market-structure-outlook">
                <h4>含总体量能的走势估计</h4>
                {activity.outlook.length ? (
                  <div className="market-structure-outlook-readings">
                    {activity.outlook.map((estimate) => (
                      <article key={estimate.horizon}>
                        <span>未来{estimate.horizon}交易日指数收益估计</span>
                        <strong>{formatSignedPercent(estimate.expected_return_pct)}%</strong>
                        <small title="保持价格结构不变，相对全部量比为1时的估计变化。">
                          总体量能调整 {formatSignedPercent(estimate.activity_adjustment_pp)} 个百分点
                        </small>
                      </article>
                    ))}
                  </div>
                ) : <p className="scene-layer-caption">{activity.outlook_status}</p>}
                <p className="scene-layer-caption">
                  沪深300、中证500、中证1000等权收益的研究估计，使用沪深北总量能；目前改善幅度较小，不代表上涨概率。
                </p>
              </div>
            </section>
          ) : null}
          <div className="market-structure-degrees" aria-label="周期层级">
            {data.consensus.map((level) => (
              <button
                type="button"
                key={level.degree}
                className={degree === level.degree ? "selected" : ""}
                aria-pressed={degree === level.degree}
                onClick={() => setDegree(level.degree)}
              >
                <strong>
                  {
                    [
                      "小周期 · 基础波段",
                      "中周期 · 父结构",
                      "大周期 · 祖先结构",
                    ][level.degree]
                  }
                </strong>
                <span>{level.description}</span>
                <small>
                  上涨 {level.up_count} · 下降 {level.down_count} · 失效{" "}
                  {level.broken_count} · 未形成或缺价 {level.unavailable_count}
                </small>
              </button>
            ))}
          </div>
          <div className="market-structure-indexes">
            {data.indexes.map((index) => {
              const level = index.levels[degree];
              const lastEvent = level?.events.at(-1);
              const rising =
                level?.state === "up" || level?.state === "up_broken";
              const chart = index.chart;
              const values = chart.flatMap((point) =>
                point.guards[degree] == null
                  ? [point.close]
                  : [point.close, point.guards[degree]!],
              );
              const min = values.length ? Math.min(...values) : 0;
              const max = values.length ? Math.max(...values) : 1;
              const span = max - min || 1;
              const closePoints = chart
                .map(
                  (point, i) =>
                    `${12 + (i / Math.max(1, chart.length - 1)) * 416},${152 - ((point.close - min) / span) * 130}`,
                )
                .join(" ");
              return (
                <article className="market-structure-index" key={index.code}>
                  <div className="market-structure-heading">
                    <div>
                      <h4>{index.name}</h4>
                      <span className="scene-layer-caption">
                        {index.code} · {formatDate(index.trade_date)}
                      </span>
                    </div>
                    <span
                      className={`market-structure-state ${index.current ? level?.state : "insufficient"}`}
                    >
                      {index.current && level
                        ? level.stage === "started"
                          ? rising
                            ? "向上转换尝试"
                            : "向下转换尝试"
                          : stateLabels[level.state]
                        : "参考日缺少指数价格"}
                    </span>
                  </div>
                  {level ? (
                    <>
                      <p className="market-structure-phase">
                        {
                          {
                            advance: "上涨推进",
                            pullback: "上涨中的回调",
                            decline: "下跌推进",
                            rebound: "下跌中的反弹",
                            transition: "原结构失效，等待新结构",
                            unknown: "等待完整波段结构",
                          }[level.phase]
                        }
                        {level.stage !== "unresolved" ? (
                          <span>
                            {" "}
                            ·{" "}
                            {level.stage === "started"
                              ? "刚启动，尚未走出后续N形"
                              : "已有N形延续证据"}
                          </span>
                        ) : null}
                      </p>
                      <dl className="market-structure-metrics">
                        <div>
                          <dt>收盘</dt>
                          <dd>{formatPrice(index.close)}</dd>
                        </div>
                        <div>
                          <dt>{rising ? "上涨支撑防线" : "下降压力防线"}</dt>
                          <dd>{formatPrice(level.guard?.price)}</dd>
                        </div>
                        <div>
                          <dt>相对防线</dt>
                          <dd>
                            {level.guard_distance_pct == null
                              ? "--"
                              : `${level.guard_distance_pct > 0 ? "+" : ""}${level.guard_distance_pct.toFixed(2)}%`}
                          </dd>
                        </div>
                        <div>
                          <dt>当前状态开始</dt>
                          <dd className="date">
                            {formatDate(level.state_since)}
                          </dd>
                        </div>
                      </dl>
                      {level.guard ? (
                        <div className="market-structure-rule">
                          <p>
                            <strong>持续判断：</strong>
                            {rising ? "收盘守住" : "收盘不越过"}{" "}
                            {formatPrice(level.guard.price)}，该层结构有效。
                          </p>
                          <p>
                            <strong>改变判断：</strong>
                            {rising ? "跌破" : "突破"}该价立即失效；
                            {rising ? "收回" : "回到其下"}
                            后重新核对。新方向N形接管后按新防线判断。
                          </p>
                          {level.can_restore_previous &&
                          level.breakout_price != null ? (
                            <p>
                              转换位置 {formatPrice(level.breakout_price)}；
                              {rising ? "跌回其下" : "涨回其上"}
                              时撤销转换尝试并核对旧结构。
                            </p>
                          ) : null}
                          <p className="scene-layer-caption">
                            防线来自 {formatDate(level.guard.trade_date)}{" "}
                            的转折，{formatDate(level.guard.confirmed_date)}{" "}
                            确认。
                          </p>
                        </div>
                      ) : (
                        <p className="scene-layer-caption">
                          这一层尚无可判定的防线。
                        </p>
                      )}
                      {chart.length > 1 ? (
                        <figure className="market-structure-chart">
                          <svg
                            viewBox="0 0 440 170"
                            role="img"
                            aria-label={`${index.name}最近${chart.length}根日线收盘与当前所选周期的防线`}
                          >
                            <line
                              x1="12"
                              y1="152"
                              x2="428"
                              y2="152"
                              className="chart-baseline"
                            />
                            <polyline
                              points={closePoints}
                              className="chart-close"
                            />
                            {chart.slice(1).map((point, i) => {
                              const guard = point.guards[degree];
                              const prior = chart[i].guards[degree];
                              if (
                                guard == null ||
                                prior == null ||
                                guard !== prior
                              )
                                return null;
                              return (
                                <line
                                  key={point.trade_date}
                                  x1={12 + (i / (chart.length - 1)) * 416}
                                  x2={12 + ((i + 1) / (chart.length - 1)) * 416}
                                  y1={152 - ((prior - min) / span) * 130}
                                  y2={152 - ((guard - min) / span) * 130}
                                  className={`chart-guard ${point.states[degree]}`}
                                />
                              );
                            })}
                            <text x="12" y="14">
                              {formatPrice(max)}
                            </text>
                            <text x="12" y="166">
                              {formatPrice(min)}
                            </text>
                          </svg>
                          <figcaption>
                            {formatDate(chart[0].trade_date)} →{" "}
                            {formatDate(chart[chart.length - 1].trade_date)} ·
                            蓝线收盘 · 红线上涨支撑 · 绿线下降压力 ·
                            虚线为失效防线
                          </figcaption>
                        </figure>
                      ) : null}
                      <p className="scene-layer-caption">
                        {level.child_state
                          ? `子结构：${stateLabels[level.child_state]}。`
                          : ""}
                        {level.parent_state
                          ? `父结构：${stateLabels[level.parent_state]}。`
                          : ""}
                      </p>
                      {lastEvent ? (
                        <p className="scene-layer-caption">
                          最近事件：{formatDate(lastEvent.trade_date)} ·{" "}
                          {eventLabels[lastEvent.kind]} · 位置{" "}
                          {formatPrice(lastEvent.level_price)}
                        </p>
                      ) : null}
                      <details className="market-structure-events">
                        <summary>
                          最近的结构事件（{level.events.length}）
                        </summary>
                        {level.events.length ? (
                          <ol>
                            {[...level.events].reverse().map((event, i) => (
                              <li
                                key={`${event.trade_date}-${event.kind}-${i}`}
                              >
                                <time>{formatDate(event.trade_date)}</time>
                                <span>{eventLabels[event.kind]}</span>
                                <span>
                                  收盘 {formatPrice(event.price)} · 位置{" "}
                                  {formatPrice(event.level_price)}
                                </span>
                              </li>
                            ))}
                          </ol>
                        ) : (
                          <p>这一层尚无结构事件。</p>
                        )}
                      </details>
                    </>
                  ) : (
                    <p className="scene-layer-caption">
                      请先下载该指数日线行情。
                    </p>
                  )}
                </article>
              );
            })}
          </div>
          <details className="market-structure-method">
            <summary>周期与证据口径</summary>
            <p>
              小周期用 {data.base_swing_pct}%
              反向波动确认基础波段。中周期从子波段的同类转折中提取局部高低点，大周期再从中周期递归提取；层级随结构形成，不对应固定天数。三类指数分别判断，保留分歧。
            </p>
            <p>
              转折只有确认后才可作为防线，确认日期与低点或高点发生日期分别展示。已知防线破位、突破或收回时，当天收盘更新；日线不判断盘中一度破位后又收回的路径。
            </p>
            <p>
              防线来自该层已经突破的N形起点。上涨延续只上移防线，下降延续只下移；子结构改变不直接改写父结构。新方向尚未N形延续时，返回转换位置可恢复旧结构；形成后续N形则由新结构接管。
            </p>
            <p>
              这里描述指数价格结构。上涨结构并未在历史样本中稳定提高打板成功率，打板容错与接力仍需分别观察。
            </p>
            {activity ? <p>
              总体量能按本地股票日线成交量、成交额逐只汇总，不含基金、债券及额外盘后成交字段。
              走势估计的拟合数据截至{formatDate(activity.model_fit_end)}，历史验证数据截至{formatDate(activity.validation_data_asof)}；
              量能调整是相对各量比均为1时的变化，价格结构防线仍按上述规则确定。
            </p> : null}
          </details>
        </>
      ) : null}
    </section>
  );
}
