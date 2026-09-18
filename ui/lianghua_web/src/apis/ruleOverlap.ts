import { invoke } from '@tauri-apps/api/core'

export type RuleOverlapPair = {
  left_rule_name: string
  right_rule_name: string
  joint_trigger_count: number
  union_trigger_count: number
  jaccard: number | null
}

export type RuleOverlapRule = {
  rule_name: string
  trigger_count: number
  coverage: number | null
  top_overlap_rule: string | null
  top_overlap_jaccard: number | null
}

export type RuleOverlapData = {
  start_date: string
  end_date: string
  universe_sample_count: number
  rules: RuleOverlapRule[]
  pairs: RuleOverlapPair[]
}

export async function getRuleOverlapDiagnostics(query: {
  sourcePath: string
  startDate: string
  endDate: string
  ruleNames?: string[]
}) {
  return invoke<RuleOverlapData>('get_rule_overlap_diagnostics', {
    sourcePath: query.sourcePath,
    startDate: query.startDate,
    endDate: query.endDate,
    ruleNames: query.ruleNames ?? [],
  })
}
