import { useState, type ReactNode } from "react";
import type { StrategyManageRuleItem } from "../../../../apis/strategyManage";
import type {
  BacktestReturnMetric,
  ValidationCoreRuleOption,
} from "../../../../apis/strategyTrigger";
import { INDEX_OPTIONS } from "../../../../shared/backtestCommonParams";
import {
  VALIDATION_MAX_SAMPLE_LIMIT,
  VALIDATION_SCOPE_WAY_OPTIONS,
  buildEmptyUnknownConfig,
  hasValidUnknownConfig,
  inferUnknownConfigs,
  type ValidationDirection,
  type ValidationScopeWayOption,
  type ValidationUnknownConfigDraft,
} from "../../../../shared/expressionValidation";
import { STOCK_PICK_BOARD_OPTIONS } from "../../../../shared/stockPickShared";

const CORE_RULE_LIMIT = 8;

export type ExpressionBacktestFormValues = {
  importRuleName: string;
  direction: ValidationDirection;
  scopeWay: ValidationScopeWayOption;
  consecThresholdText: string;
  scopeWindowsText: string;
  holdingPeriodText: string;
  expression: string;
  startDateInput: string;
  endDateInput: string;
  boardFilter: (typeof STOCK_PICK_BOARD_OPTIONS)[number];
  totalMvMinText: string;
  totalMvMaxText: string;
  minListedTradeDaysText: string;
  walkForwardFoldsText: string;
  sampleLimitText: string;
  minSamplesPerDayText: string;
  stockAdjType: string;
  returnMetric: BacktestReturnMetric;
  indexTsCode: string;
  indexBetaText: string;
  conceptBetaText: string;
  industryBetaText: string;
  enableUnknown: boolean;
  unknownConfigs: ValidationUnknownConfigDraft[];
  coreRuleNames: string[];
};

export function ExpressionBacktestForm({
  values,
  strategyOptions,
  indicatorNames,
  coreRuleOptions,
  boardOptions,
  loading,
  disabled,
  error,
  onChange,
  onApplyRule,
  onRun,
  extraParameters,
}: {
  values: ExpressionBacktestFormValues;
  strategyOptions: StrategyManageRuleItem[];
  indicatorNames: string[];
  coreRuleOptions: ValidationCoreRuleOption[];
  boardOptions: string[];
  loading: boolean;
  disabled: boolean;
  error: string;
  onChange: (patch: Partial<ExpressionBacktestFormValues>) => void;
  onApplyRule: (ruleName: string) => void;
  onRun: () => void;
  extraParameters?: ReactNode;
}) {
  const [editorOpen, setEditorOpen] = useState(false);
  const [coreRuleKeyword, setCoreRuleKeyword] = useState("");
  const coreRuleKeywordText = coreRuleKeyword.trim().toLowerCase();
  const coreRuleOptionByName = new Map(
    coreRuleOptions.map((item) => [item.name, item]),
  );
  const visibleCoreRules = strategyOptions
    .filter(
      (item) =>
        !coreRuleKeywordText ||
        item.name.toLowerCase().includes(coreRuleKeywordText),
    )
    .sort((left, right) => {
      const leftOption = coreRuleOptionByName.get(left.name);
      const rightOption = coreRuleOptionByName.get(right.name);
      const leftUsable = Boolean(
        leftOption &&
        (leftOption.native_recompute || leftOption.valid_trigger_count > 0),
      );
      const rightUsable = Boolean(
        rightOption &&
        (rightOption.native_recompute || rightOption.valid_trigger_count > 0),
      );
      return (
        Number(!leftUsable) - Number(!rightUsable) ||
        (rightOption?.trigger_count ?? 0) - (leftOption?.trigger_count ?? 0) ||
        left.name.localeCompare(right.name)
      );
    });
  const coreRuleUnavailableReason = (ruleName: string) => {
    const option = coreRuleOptionByName.get(ruleName);
    if (!option) {
      return "结果库没有该策略的触发记录，需先执行排名计算";
    }
    if (option.native_recompute)
      return "运行时隔离重算当前策略，不依赖历史评分库";
    if (option.valid_trigger_count === 0) {
      return `结果库中 ${option.trigger_count} 次触发的分数全部为 0，需重跑排名计算`;
    }
    return `结果库触发 ${option.trigger_count} 次，其中 ${option.valid_trigger_count} 次为非零分数`;
  };

  function toggleCoreRule(ruleName: string, checked: boolean) {
    if (checked) {
      if (
        values.coreRuleNames.length >= CORE_RULE_LIMIT ||
        values.coreRuleNames.includes(ruleName)
      ) {
        return;
      }
      onChange({ coreRuleNames: [...values.coreRuleNames, ruleName] });
      return;
    }
    onChange({
      coreRuleNames: values.coreRuleNames.filter((item) => item !== ruleName),
    });
  }

  function updateUnknownConfig(
    index: number,
    patch: Partial<ValidationUnknownConfigDraft>,
  ) {
    onChange({
      unknownConfigs: values.unknownConfigs.map((config, configIndex) =>
        configIndex === index ? { ...config, ...patch } : config,
      ),
    });
  }

  return (
    <section className="expression-backtest-card">
      {error ? (
        <div className="expression-backtest-error" role="alert">
          {error}
        </div>
      ) : null}
      <div className="expression-backtest-form-row">
        <label className="expression-backtest-field expression-backtest-source">
          <span>导入策略</span>
          <select
            value={values.importRuleName}
            onChange={(event) => onApplyRule(event.target.value)}
          >
            <option value="">不导入</option>
            {strategyOptions.map((item) => (
              <option key={item.name} value={item.name}>
                {item.name}
              </option>
            ))}
          </select>
        </label>
        <label className="expression-backtest-field">
          <span>开始日期</span>
          <input
            type="date"
            value={values.startDateInput}
            onChange={(event) =>
              onChange({ startDateInput: event.target.value })
            }
          />
        </label>
        <label className="expression-backtest-field">
          <span>结束日期</span>
          <input
            type="date"
            value={values.endDateInput}
            onChange={(event) => onChange({ endDateInput: event.target.value })}
          />
        </label>
        <label className="expression-backtest-field">
          <span title="holding period：回测持有期，同时是 walk-forward 的 purge 交易日数">
            持有期 · 交易日
          </span>
          <input
            type="number"
            min={1}
            step={1}
            value={values.holdingPeriodText}
            onChange={(event) =>
              onChange({ holdingPeriodText: event.target.value })
            }
          />
        </label>
        <label className="expression-backtest-field">
          <span>回测指标</span>
          <select
            value={values.returnMetric}
            onChange={(event) =>
              onChange({
                returnMetric: event.target.value as BacktestReturnMetric,
              })
            }
          >
            <option value="residual_return">残差收益率（%）</option>
            <option value="rise_amount">上涨金额（亿）</option>
          </select>
          {values.returnMetric === "rise_amount" ? (
            <span>上涨金额按残差收益率与当日估算市值计算。</span>
          ) : null}
        </label>
      </div>

      <details
        className="research-editor expression-backtest-editor"
        open={editorOpen}
        onToggle={(event) => setEditorOpen(event.currentTarget.open)}
      >
        <summary>编辑策略</summary>
        <div className="expression-backtest-form-row">
          <label className="expression-backtest-field">
            <span>方向</span>
            <select
              value={values.direction}
              onChange={(event) =>
                onChange({
                  direction: event.target.value as ValidationDirection,
                })
              }
            >
              <option value="positive">正向</option>
              <option value="negative">负向</option>
            </select>
          </label>
          <label className="expression-backtest-field">
            <span>触发方式</span>
            <select
              value={values.scopeWay}
              onChange={(event) =>
                onChange({
                  scopeWay: event.target.value as ValidationScopeWayOption,
                })
              }
            >
              {VALIDATION_SCOPE_WAY_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          {values.scopeWay === "CONSEC" ? (
            <label className="expression-backtest-field">
              <span>连续触发阈值</span>
              <input
                type="number"
                min={1}
                step={1}
                value={values.consecThresholdText}
                onChange={(event) =>
                  onChange({ consecThresholdText: event.target.value })
                }
              />
            </label>
          ) : null}
          <label className="expression-backtest-field">
            <span>观察窗口</span>
            <input
              type="number"
              min={1}
              step={1}
              value={values.scopeWindowsText}
              onChange={(event) =>
                onChange({ scopeWindowsText: event.target.value })
              }
            />
          </label>
        </div>
        <label className="expression-backtest-field expression-backtest-field-full">
          <span>表达式</span>
          <textarea
            rows={4}
            value={values.expression}
            onChange={(event) => onChange({ expression: event.target.value })}
            placeholder="例如: C > REF(C, N) and V > MA(V, M)"
          />
        </label>
      </details>
      {!editorOpen && values.expression.trim() ? (
        <p className="expression-backtest-expression-preview">
          <span>表达式</span>
          <code>{values.expression}</code>
        </p>
      ) : null}

      <details className="expression-backtest-advanced">
        <summary>更多参数</summary>
        <fieldset className="research-parameter-group">
          <legend>评价范围</legend>
          <div className="expression-backtest-form-row">
            <label className="expression-backtest-field">
              <span title="板块过滤与全局“排除ST”设置共同生效；含 ST 板块选项">
                板块 / ST
              </span>
              <select
                value={values.boardFilter}
                onChange={(event) =>
                  onChange({
                    boardFilter: event.target
                      .value as (typeof STOCK_PICK_BOARD_OPTIONS)[number],
                  })
                }
              >
                {boardOptions.map((board) => (
                  <option key={board} value={board}>
                    {board}
                  </option>
                ))}
              </select>
            </label>
            <label className="expression-backtest-field">
              <span>推测市值最小(亿)</span>
              <input
                type="number"
                min={0}
                step={1}
                value={values.totalMvMinText}
                onChange={(event) =>
                  onChange({ totalMvMinText: event.target.value })
                }
              />
            </label>
            <label className="expression-backtest-field">
              <span>推测市值最大(亿)</span>
              <input
                type="number"
                min={0}
                step={1}
                value={values.totalMvMaxText}
                onChange={(event) =>
                  onChange({ totalMvMaxText: event.target.value })
                }
              />
            </label>
            <label className="expression-backtest-field">
              <span>最少上市交易日</span>
              <input
                type="number"
                min={0}
                step={1}
                value={values.minListedTradeDaysText}
                onChange={(event) =>
                  onChange({ minListedTradeDaysText: event.target.value })
                }
              />
            </label>
          </div>
        </fieldset>
        <fieldset className="research-parameter-group">
          <legend>回测设置</legend>
          <div className="expression-backtest-form-row">
            <label className="expression-backtest-field">
              <span title="expanding walk-forward 的样本外窗口数量；train 与 test 之间按 holding period 做 purge">
                样本外窗口数
              </span>
              <input
                type="number"
                min={1}
                max={8}
                step={1}
                value={values.walkForwardFoldsText}
                onChange={(event) =>
                  onChange({ walkForwardFoldsText: event.target.value })
                }
              />
            </label>
            <label className="expression-backtest-field">
              <span>股票复权</span>
              <input
                value={values.stockAdjType}
                onChange={(event) =>
                  onChange({ stockAdjType: event.target.value })
                }
              />
            </label>
            <label className="expression-backtest-field">
              <span>指数</span>
              <select
                value={values.indexTsCode}
                onChange={(event) =>
                  onChange({ indexTsCode: event.target.value })
                }
              >
                {INDEX_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="expression-backtest-field">
              <span>指数 Beta</span>
              <input
                type="number"
                step="0.01"
                value={values.indexBetaText}
                onChange={(event) =>
                  onChange({ indexBetaText: event.target.value })
                }
              />
            </label>
            <label className="expression-backtest-field">
              <span>概念 Beta</span>
              <input
                type="number"
                step="0.01"
                value={values.conceptBetaText}
                onChange={(event) =>
                  onChange({ conceptBetaText: event.target.value })
                }
              />
            </label>
            <label className="expression-backtest-field">
              <span>行业 Beta</span>
              <input
                type="number"
                step="0.01"
                value={values.industryBetaText}
                onChange={(event) =>
                  onChange({ industryBetaText: event.target.value })
                }
              />
            </label>
            <label className="expression-backtest-field">
              <span title="规则回测按实际触发数过滤的每日最小样本数">
                日最少样本（触发数）
              </span>
              <input
                type="number"
                min={1}
                step={1}
                value={values.minSamplesPerDayText}
                onChange={(event) =>
                  onChange({ minSamplesPerDayText: event.target.value })
                }
              />
            </label>
            <label className="expression-backtest-field">
              <span>样本展示上限/组</span>
              <input
                type="number"
                min={1}
                max={VALIDATION_MAX_SAMPLE_LIMIT}
                step={1}
                value={values.sampleLimitText}
                onChange={(event) =>
                  onChange({ sampleLimitText: event.target.value })
                }
              />
            </label>
          </div>
        </fieldset>
        {extraParameters}
        <label className="expression-backtest-field expression-backtest-field-checkbox">
          <span>参数研究（UNKNOWN）</span>
          <input
            type="checkbox"
            checked={values.enableUnknown}
            onChange={(event) => {
              const checked = event.target.checked;
              onChange({
                enableUnknown: checked,
                unknownConfigs: checked
                  ? hasValidUnknownConfig(values.unknownConfigs)
                    ? values.unknownConfigs
                    : inferUnknownConfigs(values.expression, indicatorNames)
                  : [],
              });
            }}
          />
        </label>
        {values.enableUnknown ? (
          <div className="expression-backtest-unknown-block">
            <div className="expression-backtest-unknown-head">
              <strong>参数研究</strong>
              <span>每个参数组合单独回测，会增加计算时间。</span>
            </div>
            <div className="expression-backtest-unknown-list">
              {values.unknownConfigs.map((config, index) => (
                <div
                  key={`unknown-config-${index}`}
                  className="expression-backtest-unknown-row"
                >
                  <label className="expression-backtest-field">
                    <span>参数</span>
                    <input
                      value={config.name}
                      onChange={(event) =>
                        updateUnknownConfig(index, { name: event.target.value })
                      }
                    />
                  </label>
                  <label className="expression-backtest-field">
                    <span>起始</span>
                    <input
                      type="number"
                      step="any"
                      value={config.start}
                      onChange={(event) =>
                        updateUnknownConfig(index, {
                          start: event.target.value,
                        })
                      }
                    />
                  </label>
                  <label className="expression-backtest-field">
                    <span>结束</span>
                    <input
                      type="number"
                      step="any"
                      value={config.end}
                      onChange={(event) =>
                        updateUnknownConfig(index, { end: event.target.value })
                      }
                    />
                  </label>
                  <label className="expression-backtest-field">
                    <span>步长</span>
                    <input
                      type="number"
                      step="any"
                      value={config.step}
                      onChange={(event) =>
                        updateUnknownConfig(index, { step: event.target.value })
                      }
                    />
                  </label>
                  <button
                    type="button"
                    className="expression-backtest-secondary-btn"
                    onClick={() =>
                      onChange({
                        unknownConfigs:
                          values.unknownConfigs.length <= 1
                            ? [buildEmptyUnknownConfig()]
                            : values.unknownConfigs.filter(
                                (_, configIndex) => configIndex !== index,
                              ),
                      })
                    }
                  >
                    删除
                  </button>
                </div>
              ))}
            </div>
            <div className="expression-backtest-unknown-actions">
              <button
                type="button"
                className="expression-backtest-secondary-btn"
                onClick={() =>
                  onChange({
                    unknownConfigs: inferUnknownConfigs(
                      values.expression,
                      indicatorNames,
                    ),
                  })
                }
              >
                自动识别
              </button>
              <button
                type="button"
                className="expression-backtest-secondary-btn"
                onClick={() =>
                  onChange({
                    unknownConfigs: [
                      ...values.unknownConfigs,
                      buildEmptyUnknownConfig(),
                    ],
                  })
                }
              >
                + 增加参数
              </button>
            </div>
          </div>
        ) : null}

        <fieldset className="research-parameter-group expression-backtest-core-picker">
          <legend>
            核心策略对照 · 已选 {values.coreRuleNames.length} /{" "}
            {CORE_RULE_LIMIT}
          </legend>
          <input
            className="expression-backtest-core-search"
            value={coreRuleKeyword}
            placeholder="搜索策略..."
            onChange={(event) => setCoreRuleKeyword(event.target.value)}
          />
          <div className="expression-backtest-core-list">
            {visibleCoreRules.length > 0 ? (
              visibleCoreRules.map((item) => {
                const checked = values.coreRuleNames.includes(item.name);
                const option = coreRuleOptionByName.get(item.name);
                const unusable =
                  !option ||
                  (!option.native_recompute &&
                    option.valid_trigger_count === 0);
                return (
                  <label
                    key={`core-rule-${item.name}`}
                    className={`expression-backtest-core-option${unusable ? " expression-backtest-core-option-disabled" : ""}`}
                    title={coreRuleUnavailableReason(item.name)}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={
                        !checked &&
                        (unusable ||
                          values.coreRuleNames.length >= CORE_RULE_LIMIT)
                      }
                      onChange={(event) =>
                        toggleCoreRule(item.name, event.target.checked)
                      }
                    />
                    <span>{item.name}</span>
                    {!option?.native_recompute ? (
                      <span className="expression-backtest-core-option-meta">
                        {unusable
                          ? option
                            ? `分数 0 / ${option.trigger_count}`
                            : "结果库无记录"
                          : `触发 ${option.trigger_count}`}
                      </span>
                    ) : null}
                  </label>
                );
              })
            ) : (
              <span className="expression-backtest-core-empty">
                没有匹配的策略。
              </span>
            )}
          </div>
          <div className="expression-backtest-core-selected">
            <span className="expression-backtest-core-selected-label">
              已选：
            </span>
            {values.coreRuleNames.length > 0 ? (
              values.coreRuleNames.map((ruleName) => (
                <button
                  key={`core-selected-${ruleName}`}
                  type="button"
                  className="expression-backtest-core-chip"
                  onClick={() => toggleCoreRule(ruleName, false)}
                  title={`移除 ${ruleName}`}
                >
                  {ruleName} ×
                </button>
              ))
            ) : (
              <span className="expression-backtest-core-empty">未选择</span>
            )}
          </div>
        </fieldset>
      </details>

      <div className="expression-backtest-actions">
        <button
          type="button"
          className="expression-backtest-primary-btn"
          disabled={loading || disabled}
          onClick={() => {
            if (!values.expression.trim()) setEditorOpen(true);
            onRun();
          }}
        >
          {loading ? "回测中…" : "运行回测"}
        </button>
      </div>
    </section>
  );
}
