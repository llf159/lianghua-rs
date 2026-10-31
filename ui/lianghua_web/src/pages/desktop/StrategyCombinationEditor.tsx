import type { StrategyManageRuleCondition } from "../../apis/strategyManage";
import "./css/StrategyManagePage.css";

export default function StrategyCombinationEditor({
  conditions,
  combinationBonusPointTexts,
  combinationHitPointTexts,
  maxPointsText,
  maxBonusPointsText,
  addCombinationCondition,
  removeCombinationCondition,
  updateCombinationCondition,
  updateCombinationHitPoints,
  setMaxPointsText,
  setMaxBonusPointsText,
}: {
  conditions: StrategyManageRuleCondition[];
  combinationBonusPointTexts: string[];
  combinationHitPointTexts: string[];
  maxPointsText: string;
  maxBonusPointsText: string;
  addCombinationCondition: () => void;
  removeCombinationCondition: (index: number) => void;
  updateCombinationCondition: (
    index: number,
    field: "name" | "when" | "bonus_points",
    value: string,
  ) => void;
  updateCombinationHitPoints: (hitCount: number, value: string) => void;
  setMaxPointsText: (value: string) => void;
  setMaxBonusPointsText: (value: string) => void;
}) {
  return (
    <div className="strategy-manage-combination-editor strategy-manage-field-span-full">
      <div className="strategy-manage-combination-intro">
        <strong>组合计分</strong>
        <span>{conditions.length} 条条件</span>
      </div>

      <section className="strategy-manage-combination-section">
        <div className="strategy-manage-combination-section-head">
          <div>
            <strong>基础条件</strong>
          </div>
          <button
            className="strategy-manage-inline-btn"
            type="button"
            onClick={addCombinationCondition}
          >
            添加条件
          </button>
        </div>
        <div className="strategy-manage-combination-list">
          {conditions.map((condition, index) => (
            <article
              className="strategy-manage-combination-row"
              aria-label={`条件 ${index + 1}`}
              key={`condition-${index}`}
            >
              <div className="strategy-manage-combination-row-head">
                <strong>条件 {index + 1}</strong>
                <button
                  className="strategy-manage-inline-btn is-danger"
                  type="button"
                  onClick={() => removeCombinationCondition(index)}
                  disabled={conditions.length <= 1}
                >
                  删除
                </button>
              </div>
              <label className="strategy-manage-field">
                <span>条件名称</span>
                <input
                  value={condition.name}
                  onChange={(event) =>
                    updateCombinationCondition(
                      index,
                      "name",
                      event.target.value,
                    )
                  }
                  placeholder="例如：价格突破"
                />
              </label>
              <label className="strategy-manage-field">
                <span>触发表达式</span>
                <textarea
                  rows={4}
                  value={condition.when}
                  onChange={(event) =>
                    updateCombinationCondition(
                      index,
                      "when",
                      event.target.value,
                    )
                  }
                  placeholder="例如：C > HHV(REF(H, 1), 20)"
                />
              </label>
              <label className="strategy-manage-field">
                <span>满足此条件额外加分</span>
                <input
                  type="number"
                  step="any"
                  value={combinationBonusPointTexts[index] ?? ""}
                  onChange={(event) =>
                    updateCombinationCondition(
                      index,
                      "bonus_points",
                      event.target.value,
                    )
                  }
                />
              </label>
            </article>
          ))}
        </div>
      </section>

      <section className="strategy-manage-combination-section">
        <div className="strategy-manage-combination-section-head">
          <div>
            <strong>命中数得分</strong>
          </div>
        </div>
        <div className="strategy-manage-hit-score-grid">
          {combinationHitPointTexts.map((points, index) => {
            const hitCount = index + 1;
            return (
              <label
                className="strategy-manage-hit-score-item"
                key={`hit-score-${hitCount}`}
              >
                <span>命中 {hitCount} 条</span>
                <input
                  type="number"
                  step="any"
                  value={points}
                  onChange={(event) =>
                    updateCombinationHitPoints(hitCount, event.target.value)
                  }
                />
              </label>
            );
          })}
        </div>
      </section>

      <section className="strategy-manage-combination-section">
        <div className="strategy-manage-combination-section-head">
          <div>
            <strong>分数范围限制</strong>
          </div>
        </div>
        <div className="strategy-manage-combination-cap-grid">
          <label className="strategy-manage-field">
            <span>额外分的正负限制值</span>
            <input
              type="number"
              min="0"
              step="any"
              value={maxBonusPointsText}
              onChange={(event) => setMaxBonusPointsText(event.target.value)}
              placeholder="例如 5：限制在 -5～5"
            />
          </label>
          <label className="strategy-manage-field">
            <span>最终总分的正负限制值</span>
            <input
              type="number"
              min="0"
              step="any"
              value={maxPointsText}
              onChange={(event) => setMaxPointsText(event.target.value)}
              placeholder="例如 5：限制在 -5～5"
            />
          </label>
        </div>
      </section>
    </div>
  );
}
