import type {
  StrategyManageRuleItem,
  StrategyManageRuleCondition,
  StrategyManageRuleKind,
} from "../apis/strategyManage";
import type { TrackDefinition } from "../apis/strategyTracking";
import type { DistPointRow } from "./strategyDistPoints";

export type ResearchRuleDraft = {
  name: string;
  kind: StrategyManageRuleKind;
  fixedPointsText: string;
  scoreMode: "fixed" | "dist";
  distPointRows: DistPointRow[];
  conditions: StrategyManageRuleCondition[];
  bonusPointTexts: string[];
  hitPointTexts: string[];
  maxPointsText: string;
  maxBonusPointsText: string;
  tracking?: TrackDefinition[];
  captureTexts?: Record<string, string>;
};

export function createResearchRuleDraft(
  rule?: StrategyManageRuleItem,
): ResearchRuleDraft {
  return {
    tracking: rule?.tracking ? structuredClone(rule.tracking) : [],
    captureTexts: {},
    name: rule?.name ?? "",
    kind: rule?.kind ?? "single",
    fixedPointsText: String(rule?.points ?? 1),
    scoreMode: rule?.dist_points?.length ? "dist" : "fixed",
    distPointRows:
      rule?.dist_points?.map((point) => ({
        min: String(point.min),
        max: String(point.max),
        points: String(point.points),
      })) ?? [],
    conditions: rule?.conditions.map((condition) => ({ ...condition })) ?? [],
    bonusPointTexts:
      rule?.conditions.map((condition) => String(condition.bonus_points)) ?? [],
    hitPointTexts:
      rule?.conditions.map((_, index) =>
        String(rule.points_by_hits?.[index + 1] ?? ""),
      ) ?? [],
    maxPointsText: rule?.max_points == null ? "" : String(rule.max_points),
    maxBonusPointsText:
      rule?.max_bonus_points == null ? "" : String(rule.max_bonus_points),
  };
}
