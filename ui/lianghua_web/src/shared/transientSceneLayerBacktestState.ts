import type { RuleLayerBacktestData } from "../apis/strategyTrigger";

let strategyBacktestResult: RuleLayerBacktestData | null = null;

export function readTransientStrategyBacktestResult() {
  return strategyBacktestResult;
}

export function writeTransientStrategyBacktestResult(result: RuleLayerBacktestData | null) {
  strategyBacktestResult = result;
}
