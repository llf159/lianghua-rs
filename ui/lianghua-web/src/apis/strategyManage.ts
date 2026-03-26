import { invoke } from '@tauri-apps/api/core'
import { open } from '@tauri-apps/plugin-dialog'

export type StrategyManageDistPoint = {
  min: number
  max: number
  points: number
}

export type StrategyManageRuleItem = {
  index: number
  name: string
  scope_way: string
  scope_windows: number
  points: number
  explain: string
  when: string
  tag: string
  dist_points?: StrategyManageDistPoint[] | null
}

export type StrategyManageRuleDraft = {
  name: string
  scope_way: string
  scope_windows: number
  when: string
  points: number
  dist_points?: StrategyManageDistPoint[] | null
  explain: string
  tag: string
}

export type StrategyManagePageData = {
  rules: StrategyManageRuleItem[]
}

export async function getStrategyManagePage(sourcePath: string) {
  return invoke<StrategyManagePageData>('get_strategy_manage_page', {
    sourcePath,
  })
}

export async function addStrategyManageRule(sourcePath: string) {
  return invoke<StrategyManagePageData>('add_strategy_manage_rule', {
    sourcePath,
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

export async function exportStrategyRuleFile(sourcePath: string) {
  const picked = await open({
    multiple: false,
    directory: true,
  })

  if (!picked || Array.isArray(picked)) {
    return null
  }

  return invoke<string>('export_strategy_rule_file', {
    sourcePath,
    destinationDir: picked,
  })
}
