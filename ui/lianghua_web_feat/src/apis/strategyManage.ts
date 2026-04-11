import { invoke } from '@tauri-apps/api/core'

export type StrategyManageDistPoint = {
  min: number
  max: number
  points: number
}

export type StrategyManageSceneItem = {
  index: number
  name: string
  observe_threshold: number
  trigger_threshold: number
  confirm_threshold: number
  fail_threshold: number
  evidence_score: number
  rule_count: number
}

export type StrategyManageSceneDraft = {
  name: string
  observe_threshold: number
  trigger_threshold: number
  confirm_threshold: number
  fail_threshold: number
  evidence_score: number
}

export type StrategyManageRuleItem = {
  index: number
  name: string
  scene_name: string
  stage: string
  scope_way: string
  scope_windows: number
  points: number
  scene_points: number
  explain: string
  when: string
  dist_points?: StrategyManageDistPoint[] | null
}

export type StrategyManageRuleDraft = {
  name: string
  scene_name: string
  stage: string
  scope_way: string
  scope_windows: number
  when: string
  points: number
  scene_points: number
  dist_points?: StrategyManageDistPoint[] | null
  explain: string
}

export type StrategyManagePageData = {
  scenes: StrategyManageSceneItem[]
  rules: StrategyManageRuleItem[]
}

export async function getStrategyManagePage(sourcePath: string) {
  return invoke<StrategyManagePageData>('get_strategy_manage_page', { sourcePath })
}

export async function checkStrategyManageSceneDraft(
  sourcePath: string,
  draft: StrategyManageSceneDraft,
  originalName?: string,
) {
  return invoke<string>('check_strategy_manage_scene_draft', {
    sourcePath,
    originalName,
    draft,
  })
}

export async function createStrategyManageScene(
  sourcePath: string,
  draft: StrategyManageSceneDraft,
) {
  return invoke<StrategyManagePageData>('create_strategy_manage_scene', {
    sourcePath,
    draft,
  })
}

export async function updateStrategyManageScene(
  sourcePath: string,
  originalName: string,
  draft: StrategyManageSceneDraft,
) {
  return invoke<StrategyManagePageData>('update_strategy_manage_scene', {
    sourcePath,
    originalName,
    draft,
  })
}

export async function checkStrategyManageRuleDraft(
  sourcePath: string,
  draft: StrategyManageRuleDraft,
  originalName?: string,
) {
  return invoke<string>('check_strategy_manage_rule_draft', {
    sourcePath,
    originalName,
    draft,
  })
}

export async function createStrategyManageRule(
  sourcePath: string,
  draft: StrategyManageRuleDraft,
) {
  return invoke<StrategyManagePageData>('create_strategy_manage_rule', {
    sourcePath,
    draft,
  })
}

export async function removeStrategyManageRules(sourcePath: string, names: string[]) {
  return invoke<StrategyManagePageData>('remove_strategy_manage_rules', {
    sourcePath,
    names,
  })
}

export async function updateStrategyManageRule(
  sourcePath: string,
  originalName: string,
  draft: StrategyManageRuleDraft,
) {
  return invoke<StrategyManagePageData>('update_strategy_manage_rule', {
    sourcePath,
    originalName,
    draft,
  })
}
