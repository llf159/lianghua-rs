import { useLocation, useNavigate } from "react-router-dom";
import type {
  RuleExpressionValidationData,
  RuleValidationComboResult,
  RuleValidationSampleRow,
  RuleValidationUnknownConfig,
} from "../../apis/strategyTrigger";
import DetailsLink from "../../shared/DetailsLink";
import { readStoredSourcePath } from "../../shared/storage";
import "./css/ExpressionValidationSamplesPage.css";

export const EXPRESSION_VALIDATION_SAMPLES_ROUTE_PATH = "scene-layer/expression-validation-samples";
export const EXPRESSION_VALIDATION_SAMPLES_ROUTE = `/backtest/${EXPRESSION_VALIDATION_SAMPLES_ROUTE_PATH}`;

export type SceneLayerValidationReturnState = {
  sourcePath?: string;
  stockAdjType: string;
  indexTsCode: string;
  indexBeta: string;
  conceptBeta: string;
  industryBeta: string;
  startDateInput: string;
  endDateInput: string;
  minSamplesPerDay: string;
  backtestPeriod: string;
  validationImportRuleName: string;
  validationExpression: string;
  validationScopeWay: "ANY" | "LAST" | "EACH" | "RECENT" | "CONSEC";
  validationConsecThresholdText: string;
  validationScopeWindowsText: string;
  validationEnableUnknown: boolean;
  validationUnknownConfigs: RuleValidationUnknownConfig[];
  validationSampleLimitText: string;
  validationResult: RuleExpressionValidationData;
  validationSelectedComboKey: string;
};

export type ExpressionValidationSamplesData = {
  importRuleName: string;
  importRuleExplain: string;
  expression: string;
  combo: RuleValidationComboResult;
  comboParamSummary: string;
  sampleLimitPerGroup: number;
  sourcePath?: string | null;
};

export type ExpressionValidationSamplesLocationState = ExpressionValidationSamplesData & {
  sceneLayerReturnState: SceneLayerValidationReturnState;
};

type SampleSourceBucket = "positive" | "negative" | "random";

type MergedSampleRow = RuleValidationSampleRow & {
  dedupeKey: string;
  board_label: string;
  volatility_group_label: string;
  source_bucket: SampleSourceBucket;
  source_label: string;
  source_order: number;
  source_index: number;
};

type BoardSourceSection = {
  key: SampleSourceBucket;
  label: string;
  rows: MergedSampleRow[];
};

type VolatilitySection = {
  label: string;
  sampleCount: number;
  boards: Array<{
    label: string;
    sampleCount: number;
    sourceGroups: BoardSourceSection[];
  }>;
};

const SAMPLE_SOURCE_META: Array<{
  key: SampleSourceBucket;
  label: string;
  order: number;
}> = [
  { key: "positive", label: "正向", order: 0 },
  { key: "negative", label: "负向", order: 1 },
  { key: "random", label: "随机", order: 2 },
];

const VOLATILITY_GROUP_ORDER = ["高波动", "常规波动", "其他波动"] as const;

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

function formatPercentPoint(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(digits)}%`;
}

function formatExpressionSummary(value: string, combo: RuleValidationComboResult) {
  const trimmed = value.trim();
  if (trimmed.length > 0) {
    return trimmed;
  }
  return combo.formula || "--";
}

function normalizeBoardLabel(value?: string | null) {
  const trimmed = (value ?? "").trim();
  return trimmed.length > 0 ? trimmed : "未分类板块";
}

function normalizeVolatilityGroupLabel(value?: string | null) {
  const trimmed = (value ?? "").trim();
  if (trimmed === "高波动" || trimmed === "常规波动" || trimmed === "其他波动") {
    return trimmed;
  }
  return trimmed.length > 0 ? trimmed : "其他波动";
}

function getVolatilityGroupOrder(label: string) {
  const index = VOLATILITY_GROUP_ORDER.indexOf(label as (typeof VOLATILITY_GROUP_ORDER)[number]);
  if (index >= 0) {
    return index;
  }
  return VOLATILITY_GROUP_ORDER.length;
}

function getBoardOrder(label: string) {
  if (label.includes("主板")) {
    return 0;
  }
  if (label.includes("创业板")) {
    return 1;
  }
  if (label.includes("科创板")) {
    return 2;
  }
  if (label.includes("北交所")) {
    return 3;
  }
  if (label.toUpperCase().includes("ST")) {
    return 4;
  }
  if (label === "未分类板块") {
    return 6;
  }
  return 5;
}

function compareBoardLabels(left: string, right: string) {
  const orderDiff = getBoardOrder(left) - getBoardOrder(right);
  if (orderDiff !== 0) {
    return orderDiff;
  }
  return left.localeCompare(right, "zh-CN");
}

function compareSampleRows(left: MergedSampleRow, right: MergedSampleRow) {
  const dateDiff = right.trade_date.localeCompare(left.trade_date);
  if (dateDiff !== 0) {
    return dateDiff;
  }

  const scoreDiff = right.rule_score - left.rule_score;
  if (scoreDiff !== 0) {
    return scoreDiff;
  }

  const residualDiff = right.residual_return - left.residual_return;
  if (residualDiff !== 0) {
    return residualDiff;
  }

  const tsCodeDiff = left.ts_code.localeCompare(right.ts_code, "zh-CN");
  if (tsCodeDiff !== 0) {
    return tsCodeDiff;
  }

  const sourceDiff = left.source_order - right.source_order;
  if (sourceDiff !== 0) {
    return sourceDiff;
  }

  return left.source_index - right.source_index;
}

function buildSourceSummary(sourceGroups: BoardSourceSection[]) {
  const counts = sourceGroups
    .map((group) => ({
      label: group.label,
      count: group.rows.length,
    }))
    .filter((item) => item.count > 0);

  if (counts.length === 0) {
    return "暂无样本来源";
  }

  return counts.map((item) => `${item.label} ${item.count}`).join(" · ");
}

function buildDisplaySamples(combo: RuleValidationComboResult) {
  return SAMPLE_SOURCE_META.flatMap((meta) => {
    const rows = combo.sample_groups[meta.key] ?? [];
    return rows.map((row, index) => ({
      ...row,
      dedupeKey: `${meta.key}__${row.ts_code}__${row.trade_date}__${index}`,
      board_label: normalizeBoardLabel(row.board),
      volatility_group_label: normalizeVolatilityGroupLabel(row.volatility_group),
      source_bucket: meta.key,
      source_label: meta.label,
      source_order: meta.order,
      source_index: index,
    }));
  });
}

function buildVolatilitySections(displaySamples: MergedSampleRow[]) {
  const grouped = new Map<string, Map<string, Map<SampleSourceBucket, MergedSampleRow[]>>>();

  VOLATILITY_GROUP_ORDER.forEach((label) => {
    grouped.set(label, new Map());
  });

  displaySamples.forEach((row) => {
    const sectionKey = row.volatility_group_label;
    const boardKey = row.board_label;
    const boardGroups = grouped.get(sectionKey) ?? new Map<string, Map<SampleSourceBucket, MergedSampleRow[]>>();
    const sourceGroups = boardGroups.get(boardKey) ?? new Map<SampleSourceBucket, MergedSampleRow[]>();
    const rows = sourceGroups.get(row.source_bucket) ?? [];
    rows.push(row);
    sourceGroups.set(row.source_bucket, rows);
    boardGroups.set(boardKey, sourceGroups);
    grouped.set(sectionKey, boardGroups);
  });

  const sectionLabels = Array.from(grouped.keys()).sort((left, right) => {
    const orderDiff = getVolatilityGroupOrder(left) - getVolatilityGroupOrder(right);
    if (orderDiff !== 0) {
      return orderDiff;
    }
    return left.localeCompare(right, "zh-CN");
  });

  return sectionLabels.map((label) => {
    const boardGroups = grouped.get(label) ?? new Map<string, Map<SampleSourceBucket, MergedSampleRow[]>>();
    const boards = Array.from(boardGroups.entries())
      .sort(([leftLabel], [rightLabel]) => compareBoardLabels(leftLabel, rightLabel))
      .map(([boardLabel, sourceGroupsMap]) => {
        const sourceGroups = SAMPLE_SOURCE_META.map((meta) => ({
          key: meta.key,
          label: meta.label,
          rows: [...(sourceGroupsMap.get(meta.key) ?? [])].sort(compareSampleRows),
        })).filter((group) => group.rows.length > 0);

        return {
          label: boardLabel,
          sampleCount: sourceGroups.reduce((sum, group) => sum + group.rows.length, 0),
          sourceGroups,
        };
      });

    return {
      label,
      sampleCount: boards.reduce((sum, board) => sum + board.sampleCount, 0),
      boards,
    } satisfies VolatilitySection;
  });
}

export function ExpressionValidationSamplesPanel({
  data,
  layout = "page",
}: {
  data: ExpressionValidationSamplesData;
  layout?: "page" | "modal";
}) {
  const expressionSummary = formatExpressionSummary(data.expression, data.combo);
  const displaySamples = buildDisplaySamples(data.combo);
  const volatilitySections = buildVolatilitySections(displaySamples);
  const boardCount = new Set(displaySamples.map((row) => row.board_label)).size;
  const activeVolatilityGroupCount = volatilitySections.filter((section) => section.sampleCount > 0).length;
  const sourcePath = data.sourcePath?.trim() || undefined;
  const isModalLayout = layout === "modal";

  return (
    <>
      {isModalLayout ? (
        <section className="expression-validation-samples-card expression-validation-samples-compact-overview">
          <div className="expression-validation-samples-compact-summary">
            <span className="expression-validation-samples-compact-title">{data.combo.combo_label}</span>
            <span className="expression-validation-samples-compact-chip">
              总样本 <strong>{data.combo.sample_stats.total_samples}</strong>
            </span>
            <span className="expression-validation-samples-compact-chip">
              展示 <strong>{displaySamples.length}</strong>
            </span>
            <span className="expression-validation-samples-compact-chip">
              正向 <strong>{data.combo.sample_stats.positive_count}</strong>
            </span>
            <span className="expression-validation-samples-compact-chip">
              负向 <strong>{data.combo.sample_stats.negative_count}</strong>
            </span>
            <span className="expression-validation-samples-compact-chip">
              随机 <strong>{data.combo.sample_stats.random_count}</strong>
            </span>
            <span className="expression-validation-samples-compact-chip">
              板块 <strong>{boardCount}</strong>
            </span>
            <span className="expression-validation-samples-compact-chip">
              波动段 <strong>{activeVolatilityGroupCount}</strong>
            </span>
            <span className="expression-validation-samples-compact-chip">
              每板块每方向 <strong>{data.sampleLimitPerGroup}</strong>
            </span>
          </div>

          <div className="expression-validation-samples-compact-meta">
            <div className="expression-validation-samples-compact-row">
              <span>参数</span>
              <strong>{data.comboParamSummary}</strong>
            </div>
            <div className="expression-validation-samples-compact-row">
              <span>导入规则</span>
              <strong>
                {data.importRuleName || "--"}
                {data.importRuleExplain ? ` · ${data.importRuleExplain}` : ""}
              </strong>
            </div>
          </div>
        </section>
      ) : (
        <>
          <section className="expression-validation-samples-card">
            <div className="expression-validation-samples-summary-grid">
              <div className="expression-validation-samples-summary-item">
                <span>组合</span>
                <strong>{data.combo.combo_label}</strong>
              </div>
              <div className="expression-validation-samples-summary-item">
                <span>总样本</span>
                <strong>{data.combo.sample_stats.total_samples}</strong>
              </div>
              <div className="expression-validation-samples-summary-item">
                <span>展示样本</span>
                <strong>{displaySamples.length}</strong>
              </div>
              <div className="expression-validation-samples-summary-item">
                <span>正向</span>
                <strong>{data.combo.sample_stats.positive_count}</strong>
              </div>
              <div className="expression-validation-samples-summary-item">
                <span>负向</span>
                <strong>{data.combo.sample_stats.negative_count}</strong>
              </div>
              <div className="expression-validation-samples-summary-item">
                <span>随机</span>
                <strong>{data.combo.sample_stats.random_count}</strong>
              </div>
              <div className="expression-validation-samples-summary-item">
                <span>命中板块</span>
                <strong>{boardCount}</strong>
              </div>
              <div className="expression-validation-samples-summary-item">
                <span>命中波动率段</span>
                <strong>{activeVolatilityGroupCount}</strong>
              </div>
              <div className="expression-validation-samples-summary-item">
                <span>每板块每方向</span>
                <strong>{data.sampleLimitPerGroup} 条</strong>
              </div>
            </div>
          </section>

          <section className="expression-validation-samples-card expression-validation-samples-context-grid">
            <div className="expression-validation-samples-context-box">
              <strong>组合参数</strong>
              <p>{data.comboParamSummary}</p>
            </div>

            <div className="expression-validation-samples-context-box">
              <strong>导入规则</strong>
              <p>
                {data.importRuleName || "--"}
                {data.importRuleExplain ? ` · ${data.importRuleExplain}` : ""}
              </p>
            </div>

            <div className="expression-validation-samples-context-box">
              <strong>表达式摘要</strong>
              <p>{expressionSummary}</p>
            </div>

            <div className="expression-validation-samples-context-box expression-validation-samples-context-box-wide">
              <strong>替换后表达式</strong>
              <p>{data.combo.formula || "--"}</p>
            </div>
          </section>
        </>
      )}

      <section className="expression-validation-samples-card expression-validation-samples-groups-card">
        <div className="expression-validation-samples-placeholder-head expression-validation-samples-groups-head">
          <div>
            <strong>按波动率 / 板块 / 样本来源查看样本</strong>
            <span>
              同一波动率段内先按板块，再按正向 / 负向 / 随机拆表；每个板块下每个方向最多展示 {data.sampleLimitPerGroup} 条。
            </span>
          </div>
          <div className="expression-validation-samples-source-legend">
            {SAMPLE_SOURCE_META.map((meta) => (
              <span
                key={meta.key}
                className={`expression-validation-samples-source-pill expression-validation-samples-source-pill-${meta.key}`}
              >
                {meta.label}
              </span>
            ))}
          </div>
        </div>

        <div className="expression-validation-samples-volatility-list">
          {volatilitySections.map((section) => (
            <section key={section.label} className="expression-validation-samples-volatility-section">
              <div className="expression-validation-samples-volatility-head">
                <div>
                  <h3>{section.label}</h3>
                  <p>
                    {section.sampleCount > 0
                      ? `${section.boards.length} 个板块 · ${section.sampleCount} 条展示样本`
                      : "当前组合在该波动率段暂无样本。"}
                  </p>
                </div>
                <span className="expression-validation-samples-volatility-badge">
                  {section.sampleCount} 条
                </span>
              </div>

              {section.boards.length > 0 ? (
                <div className="expression-validation-samples-board-grid">
                  {section.boards.map((board) => (
                    <article key={`${section.label}-${board.label}`} className="expression-validation-samples-board-card">
                      <div className="expression-validation-samples-board-head">
                        <div>
                          <strong>{board.label}</strong>
                          <span>{buildSourceSummary(board.sourceGroups)}</span>
                        </div>
                        <span className="expression-validation-samples-board-count">{board.sampleCount} 条</span>
                      </div>

                      <div className="expression-validation-samples-board-source-list">
                        {board.sourceGroups.map((sourceGroup) => (
                          <section
                            key={`${section.label}-${board.label}-${sourceGroup.key}`}
                            className="expression-validation-samples-board-source-section"
                          >
                            <div className="expression-validation-samples-board-source-head">
                              <span
                                className={`expression-validation-samples-source-pill expression-validation-samples-source-pill-${sourceGroup.key}`}
                              >
                                {sourceGroup.label}
                              </span>
                              <span className="expression-validation-samples-board-source-count">
                                {sourceGroup.rows.length} 条
                              </span>
                            </div>

                            <div className="expression-validation-samples-table-wrap">
                              <table className="expression-validation-samples-table">
                                <thead>
                                  <tr>
                                    <th>代码</th>
                                    <th>名称</th>
                                    <th>参考日</th>
                                    <th>触发得分</th>
                                    <th>残差收益</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {sourceGroup.rows.map((row) => (
                                    <tr key={`${row.dedupeKey}-${row.source_bucket}`}>
                                      <td>{row.ts_code}</td>
                                      <td title={row.name ?? row.ts_code}>
                                        <DetailsLink
                                          className="expression-validation-samples-stock-link"
                                          tsCode={row.ts_code}
                                          tradeDate={row.trade_date}
                                          sourcePath={sourcePath}
                                          title={`查看 ${row.name || row.ts_code} 详情`}
                                        >
                                          {row.name || row.ts_code}
                                        </DetailsLink>
                                      </td>
                                      <td>{formatDateLabel(row.trade_date)}</td>
                                      <td>{formatNumber(row.rule_score)}</td>
                                      <td>{formatPercentPoint(row.residual_return)}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </section>
                        ))}
                      </div>
                    </article>
                  ))}
                </div>
              ) : (
                <div className="expression-validation-samples-empty-block">
                  当前组合在 {section.label} 里还没有可展示的样本。
                </div>
              )}
            </section>
          ))}
        </div>
      </section>
    </>
  );
}

export default function ExpressionValidationSamplesPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const locationState =
    location.state && typeof location.state === "object"
      ? (location.state as ExpressionValidationSamplesLocationState)
      : null;

  function handleBack() {
    if (locationState?.sceneLayerReturnState) {
      navigate("/backtest/scene-layer", {
        state: {
          validationReturnState: locationState.sceneLayerReturnState,
        },
      });
      return;
    }

    navigate("/backtest/scene-layer");
  }

  if (!locationState) {
    return (
      <div className="expression-validation-samples-page">
        <section className="expression-validation-samples-card expression-validation-samples-card-empty">
          <div className="expression-validation-samples-header">
            <div>
              <h2 className="expression-validation-samples-title">表达式验证样本</h2>
              <p className="expression-validation-samples-caption">
                当前没有可用的路由上下文，请从场景分层回测结果区重新点击样本入口。
              </p>
            </div>
            <button
              type="button"
              className="expression-validation-samples-back-btn"
              onClick={handleBack}
            >
              返回回测页
            </button>
          </div>
        </section>
      </div>
    );
  }

  const sourcePath =
    locationState.sourcePath?.trim() ||
    locationState.sceneLayerReturnState.sourcePath?.trim() ||
    readStoredSourcePath();

  return (
    <div className="expression-validation-samples-page">
      <section className="expression-validation-samples-card">
        <div className="expression-validation-samples-header">
          <div>
            <h2 className="expression-validation-samples-title">表达式验证样本</h2>
            <p className="expression-validation-samples-caption">
              当前页按波动率、板块和样本来源拆解当前组合，并支持直接跳转到带参考日的个股详情。
            </p>
          </div>
          <button
            type="button"
            className="expression-validation-samples-back-btn"
            onClick={handleBack}
          >
            返回回测页
          </button>
        </div>
      </section>

      <ExpressionValidationSamplesPanel
        data={{
          importRuleName: locationState.importRuleName,
          importRuleExplain: locationState.importRuleExplain,
          expression: locationState.expression,
          combo: locationState.combo,
          comboParamSummary: locationState.comboParamSummary,
          sampleLimitPerGroup: locationState.sampleLimitPerGroup,
          sourcePath,
        }}
      />
    </div>
  );
}
