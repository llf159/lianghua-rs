import { useState } from "react";
import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatUnknownValuesForCombo } from "../../../../shared/expressionValidation";
import { ExpressionValidationSamplesPanel } from "../../ExpressionValidationSamplesPage";
import type { ExpressionValidationContext } from "../../../../shared/expressionValidation";

type SampleTabKey = "positive" | "negative" | "random";

function buildTabCombo(combo: RuleValidationComboResult, activeTab: SampleTabKey): RuleValidationComboResult {
  return {
    ...combo,
    sample_groups: {
      positive: activeTab === "positive" ? combo.sample_groups.positive : [],
      negative: activeTab === "negative" ? combo.sample_groups.negative : [],
      random: activeTab === "random" ? combo.sample_groups.random : [],
    },
  };
}

export function ExpressionSamplesCheck({
  combo,
  context,
}: {
  combo: RuleValidationComboResult;
  context: ExpressionValidationContext;
}) {
  const [activeTab, setActiveTab] = useState<SampleTabKey>("positive");
  const sampleStats = combo.sample_stats;
  const activeSampleCount = combo.sample_groups[activeTab].length;

  return (
    <div className="expression-backtest-section">
      <h3>样本检查</h3>
      <p className="expression-backtest-caption">
        正样本按残差收益从高到低、负样本从低到高展示；点击股票名称可在详情浮层查看个股，主研究结果本身不依赖浮层。
      </p>
      <div className="expression-backtest-tabs" role="tablist">
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === "positive"}
          className={
            activeTab === "positive"
              ? "expression-backtest-tab expression-backtest-tab-active"
              : "expression-backtest-tab"
          }
          onClick={() => setActiveTab("positive")}
        >
          正样本 <span>{sampleStats.positive_count}</span>
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === "negative"}
          className={
            activeTab === "negative"
              ? "expression-backtest-tab expression-backtest-tab-active"
              : "expression-backtest-tab"
          }
          onClick={() => setActiveTab("negative")}
        >
          负样本 <span>{sampleStats.negative_count}</span>
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === "random"}
          className={
            activeTab === "random"
              ? "expression-backtest-tab expression-backtest-tab-active"
              : "expression-backtest-tab"
          }
          onClick={() => setActiveTab("random")}
        >
          随机样本 <span>{sampleStats.random_count}</span>
        </button>
      </div>
      {activeSampleCount === 0 ? (
        <div className="expression-backtest-empty">当前组合没有该分组的样本。</div>
      ) : (
        <ExpressionValidationSamplesPanel
          data={{
            importRuleName: context.importRuleName,
            importRuleExplain: context.importRuleExplain,
            expression: context.expression,
            combo: buildTabCombo(combo, activeTab),
            comboParamSummary: formatUnknownValuesForCombo(combo),
            sampleLimitPerGroup: context.sampleLimitPerGroup,
            sourcePath: context.sourcePath,
          }}
          layout="modal"
        />
      )}
    </div>
  );
}
