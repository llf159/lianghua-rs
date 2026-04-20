import { useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import {
  checkStrategyManageSceneDraft,
  checkStrategyManageRuleDraft,
  createStrategyManageScene,
  createStrategyManageRule,
  getStrategyManagePage,
  removeStrategyManageScene,
  removeStrategyManageRules,
  saveStrategyManageRefactorFile,
  updateStrategyManageScene,
  updateStrategyManageRule,
  type StrategyManageDistPoint,
  type StrategyManagePageData,
  type StrategyManageRuleDraft,
  type StrategyManageRuleItem,
  type StrategyManageSceneDraft,
  type StrategyManageSceneItem,
} from '../../apis/strategyManage'
import StrategyAssetModal from './StrategyAssetModal'
import './css/StrategyManagePage.css'

const SCOPE_OPTIONS = ['LAST', 'ANY', 'EACH', 'RECENT', 'CONSEC>=2'] as const
const STAGE_OPTIONS = ['base', 'trigger', 'confirm', 'risk', 'fail'] as const
const SCENE_DIRECTION_OPTIONS = ['long', 'short'] as const
const STRATEGY_RULE_FILE_NAME = 'score_rule.toml'

type SyntaxGuideFunction = {
  name: string
  signature: string
  returns: string
  description: string
  example: string
}

type SyntaxGuideField = {
  name: string
  scope: string
  description: string
  example: string
}

type SyntaxGuideFieldSection = {
  title: string
  note: string
  fields: SyntaxGuideField[]
}

const SYNTAX_GUIDE_FUNCTIONS: SyntaxGuideFunction[] = [
  { name: 'ABS', signature: 'ABS(x)', returns: '数值序列', description: '取绝对值。', example: '输入 [-2, 3] -> 输出 [2, 3]' },
  { name: 'MAX', signature: 'MAX(a, b)', returns: '数值序列', description: '逐项取较大值。', example: 'a=[1,5], b=[2,3] -> [2,5]' },
  { name: 'MIN', signature: 'MIN(a, b)', returns: '数值序列', description: '逐项取较小值。', example: 'a=[1,5], b=[2,3] -> [1,3]' },
  { name: 'DIV', signature: 'DIV(a, b)', returns: '数值序列', description: '安全除法，除数为 0 时返回 0。', example: 'a=[6,5], b=[2,0] -> [3,0]' },
  { name: 'COUNT', signature: 'COUNT(cond, n)', returns: '数值序列', description: '统计最近 n 根里条件成立的次数。', example: 'cond=[真,假,真,真], n=3 -> [1,1,2,2]' },
  { name: 'MA', signature: 'MA(x, n)', returns: '数值序列', description: '简单移动平均。', example: 'x=[1,2,3,4], n=3 -> [空,空,2,3]' },
  { name: 'REF', signature: 'REF(x, n)', returns: '数值序列', description: '取 n 根之前的值。', example: 'x=[10,11,12,13], n=2 -> [空,空,10,11]' },
  { name: 'HHV', signature: 'HHV(x, n)', returns: '数值序列', description: '最近 n 根最高值。', example: 'x=[1,3,2,5], n=3 -> [空,空,3,5]' },
  { name: 'LLV', signature: 'LLV(x, n)', returns: '数值序列', description: '最近 n 根最低值。', example: 'x=[1,3,2,0], n=3 -> [空,空,1,0]' },
  { name: 'SUM', signature: 'SUM(x, n)', returns: '数值序列', description: '最近 n 根求和。', example: 'x=[1,2,3,4], n=3 -> [空,空,6,9]' },
  { name: 'STD', signature: 'STD(x, n)', returns: '数值序列', description: '最近 n 根标准差。', example: 'x=[1,3,3], n=2 -> [空,1,0]' },
  { name: 'IF', signature: 'IF(cond, a, b)', returns: '数值序列', description: '条件成立取 a，否则取 b。', example: 'cond=[真,假,真], a=[1,1,1], b=[0,0,0] -> [1,0,1]' },
  { name: 'CROSS', signature: 'CROSS(a, b)', returns: '布尔序列', description: 'a 当根上穿 b。', example: 'a=[1,2,4], b=[3,2,3] -> [假,假,真]' },
  { name: 'EMA', signature: 'EMA(x, n)', returns: '数值序列', description: '指数移动平均。', example: 'x=[1,2,3], n=3 -> [1,1.5,2.25]' },
  { name: 'SMA', signature: 'SMA(x, n, m)', returns: '数值序列', description: '平滑移动平均，权重约为 m / n。', example: 'x=[1,2,3], n=3, m=1 -> [1,1.33,1.89]' },
  { name: 'BARSLAST', signature: 'BARSLAST(cond)', returns: '数值序列', description: '距离上一次 cond 成立已经过去几根；首次命中前为空。', example: 'cond=[假,真,假,假,真] -> [空,0,1,2,0]' },
  { name: 'RSV', signature: 'RSV(C, H, L, n)', returns: '数值序列', description: '最近 n 根 RSV，常用于 KDJ。', example: 'C=[8,9,10], H=[10,11,12], L=[6,7,8], n=3 -> [空,空,66.67]' },
  { name: 'GRANK', signature: 'GRANK(x, n)', returns: '数值序列', description: '最近 n 根里，当前值按从大到小的排名；1 表示最大。', example: 'x=[1,4,2], n=3 -> [空,空,2]' },
  { name: 'LRANK', signature: 'LRANK(x, n)', returns: '数值序列', description: '最近 n 根里，当前值按从小到大的排名；1 表示最小。', example: 'x=[3,2,1], n=3 -> [空,空,1]' },
  { name: 'GTOPCOUNT', signature: 'GTOPCOUNT(x, cond, win, topn)', returns: '数值序列', description: '最近 win 根按 x 从大到小取前 topn，统计其中 cond 成立的个数。', example: 'x=[1,5,3], cond=[真,假,真], win=3, topn=2 -> [空,空,1]' },
  { name: 'LTOPCOUNT', signature: 'LTOPCOUNT(x, cond, win, topn)', returns: '数值序列', description: '最近 win 根按 x 从小到大取前 topn，统计其中 cond 成立的个数。', example: 'x=[1,5,3], cond=[真,假,真], win=3, topn=2 -> [空,空,2]' },
  { name: 'GET', signature: 'GET(cond, x, n)', returns: '数值序列', description: '向前回看最近 n 根，取最后一次 cond 成立时对应的 x；不包含当前这根。', example: '可写 GET(CROSS(C, MA(C, 5)), C, 20) 取最近一次上穿时的收盘价' },
]

const SYNTAX_GUIDE_FIELD_SECTIONS: SyntaxGuideFieldSection[] = [
  {
    title: '5. 常用行情字段',
    note: '这些字段来自历史 K 线或实时拼接后的 K 线序列，大部分表达式都可以直接使用。',
    fields: [
      { name: 'C / O / H / L / V', scope: '通用', description: '收盘 / 开盘 / 最高 / 最低 / 成交量。', example: 'C > O AND V > MA(V, 5)' },
      { name: 'AMOUNT', scope: '通用', description: '成交额。', example: 'AMOUNT > MA(AMOUNT, 10)' },
      { name: 'PRE_CLOSE', scope: '通用', description: '昨收价。', example: 'C > PRE_CLOSE' },
      { name: 'CHANGE / PCT_CHG', scope: '通用', description: '涨跌额 / 涨跌幅；其中 PCT_CHG 的单位是百分比。', example: 'PCT_CHG >= 5' },
      { name: 'TURNOVER_RATE', scope: '通用', description: '换手率。', example: 'TURNOVER_RATE > 8' },
    ],
  },
  {
    title: '6. 额外常数字段',
    note: '这些字段由后端运行时统一注入，后续新增常量字段也会沿用这套入口。',
    fields: [
      { name: 'ZHANG', scope: '通用', description: '涨停幅比例，例如普通股约 0.095、创业板/科创板约 0.195、北交所约 0.295、ST 约 0.045。', example: 'PCT_CHG >= ZHANG * 100' },
      { name: 'TOTAL_MV_YI', scope: '通用', description: '总市值，单位“亿”；优先由历史 TOTAL_MV 列换算得到。', example: 'TOTAL_MV_YI <= 300' },
    ],
  },
  {
    title: '7. 指标列自动注入',
    note: 'DataReader 会把 stock_data 里实际存在的数值列自动转成大写变量；这通常是行情基础列之外的指标列，不等于 stock_list.csv 里的市值字段都会天然出现在这里。',
    fields: [
      { name: '已落库指标列 / 自定义数值列', scope: '按数据源实际情况', description: '只有已经写进 stock_data 的数值列才可直接引用，变量名会自动转成大写。', example: 'MY_IND > MA(MY_IND, 5)' },
    ],
  },
  {
    title: '8. 实时监控模板附加字段',
    note: '下面这些字段只在“实时监控”页面的模板表达式中可用，策略打分、选股或统计表达式里不要直接写。',
    fields: [
      { name: 'REALTIME_CHANGE_OPEN_PCT', scope: '实时监控', description: '当前价相对今开涨跌幅，单位是百分比。', example: 'REALTIME_CHANGE_OPEN_PCT >= 2' },
      { name: 'REALTIME_FALL_FROM_HIGH_PCT', scope: '实时监控', description: '当前价相对于今日高点的跌幅，单位是百分比；计算口径为 max((今日高点 - 当前价) / 今日高点, 0) × 100%。', example: 'REALTIME_FALL_FROM_HIGH_PCT <= 1.5' },
      { name: 'REALTIME_VOL_RATIO', scope: '实时监控', description: '当前实时累计成交量 ÷ stock_data 中最新历史日的 vol，通常可理解为“相对上一交易日日成交量”的倍数。', example: 'REALTIME_VOL_RATIO >= 2' },
      { name: 'VOL_RATIO', scope: '实时监控', description: 'REALTIME_VOL_RATIO 的别名，基准相同。', example: 'VOL_RATIO >= 2' },
    ],
  },
  {
    title: '9. 表达式选股附加字段',
    note: '下面这些字段只在“表达式选股”页面运行时注入，策略打分和实时监控模板默认不会注入。',
    fields: [
      { name: 'rank / RANK', scope: '表达式选股', description: '个股在 score_summary 中按交易日对齐后的排名序列；1 表示当日排名第一。', example: 'rank <= 100 AND C > MA(C, 20)' },
    ],
  },
]

type BusyAction = 'idle' | 'loading' | 'saving' | 'deleting'
type EditorMode = 'create' | 'edit'
type SceneEditorMode = 'create' | 'edit'
type DeleteSceneTarget = Pick<StrategyManageSceneItem, 'name' | 'rule_count'>
type RefactorSceneDraft = StrategyManageSceneDraft & { id: string }
type RefactorRuleDraft = StrategyManageRuleDraft

function formatNumber(value: number, digits = 2) {
  if (!Number.isFinite(value)) {
    return '--'
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits)
}

type DistScoreSummary = {
  segmentCount: number
  intervalMin: number
  intervalMax: number
  pointsMin: number
  pointsMax: number
}

function buildDistScoreSummary(items?: StrategyManageDistPoint[] | null): DistScoreSummary | null {
  if (!items || items.length === 0) {
    return null
  }

  let intervalMin = items[0].min
  let intervalMax = items[0].max
  let pointsMin = items[0].points
  let pointsMax = items[0].points

  for (const item of items) {
    if (item.min < intervalMin) {
      intervalMin = item.min
    }
    if (item.max > intervalMax) {
      intervalMax = item.max
    }
    if (item.points < pointsMin) {
      pointsMin = item.points
    }
    if (item.points > pointsMax) {
      pointsMax = item.points
    }
  }

  return {
    segmentCount: items.length,
    intervalMin,
    intervalMax,
    pointsMin,
    pointsMax,
  }
}

function hasDistPoints(items?: StrategyManageDistPoint[] | null) {
  return Boolean(items && items.length > 0)
}

function buildEmptyDraft(sceneName = ''): StrategyManageRuleDraft {
  return {
    name: '',
    scene_name: sceneName,
    stage: 'base',
    scope_way: 'LAST',
    scope_windows: 1,
    when: '',
    points: 0,
    dist_points: null,
    explain: '',
  }
}

function buildEmptySceneDraft(): StrategyManageSceneDraft {
  return {
    name: '',
    direction: 'long',
    observe_threshold: 1,
    trigger_threshold: 2,
    confirm_threshold: 3,
    fail_threshold: 1,
  }
}

function buildSceneDraftFromScene(scene: StrategyManageSceneItem): StrategyManageSceneDraft {
  return {
    name: scene.name,
    direction: scene.direction,
    observe_threshold: scene.observe_threshold,
    trigger_threshold: scene.trigger_threshold,
    confirm_threshold: scene.confirm_threshold,
    fail_threshold: scene.fail_threshold,
  }
}

function buildDraftFromRule(rule: StrategyManageRuleItem): StrategyManageRuleDraft {
  return {
    name: rule.name,
    scene_name: rule.scene_name,
    stage: rule.stage,
    scope_way: rule.scope_way,
    scope_windows: rule.scope_windows,
    when: rule.when,
    points: rule.points,
    dist_points: rule.dist_points ?? null,
    explain: rule.explain,
  }
}

function buildPreparedSceneDraft(draft: StrategyManageSceneDraft): StrategyManageSceneDraft {
  return {
    ...draft,
    name: draft.name.trim(),
    direction: draft.direction.trim().toLowerCase(),
  }
}

function createRefactorSceneDraft(name = ''): RefactorSceneDraft {
  return {
    id: `${Date.now()}-${Math.random()}`,
    name,
    direction: 'long',
    observe_threshold: 1,
    trigger_threshold: 2,
    confirm_threshold: 3,
    fail_threshold: 1,
  }
}

function parseRequiredNumber(value: string, label: string) {
  const trimmed = value.trim()
  if (!trimmed) {
    throw new Error(`${label} 不能为空`)
  }
  const parsed = Number(trimmed)
  if (!Number.isFinite(parsed)) {
    throw new Error(`${label} 必须是合法数字`)
  }
  return parsed
}

function parseRequiredInteger(value: string, label: string, min?: number) {
  const parsed = parseRequiredNumber(value, label)
  if (!Number.isInteger(parsed)) {
    throw new Error(`${label} 必须是整数`)
  }
  if (typeof min === 'number' && parsed < min) {
    throw new Error(`${label} 必须 >= ${min}`)
  }
  return parsed
}

function distPointsToText(items?: StrategyManageDistPoint[] | null) {
  if (!items || items.length === 0) {
    return ''
  }
  return items.map((item) => `${item.min},${item.max},${item.points}`).join('\n')
}

function parseDistPointsText(raw: string) {
  const trimmed = raw.trim()
  if (!trimmed) {
    return null
  }

  return trimmed.split('\n').map((line, index) => {
    const parts = line
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
    if (parts.length !== 3) {
      throw new Error(`字典得分第 ${index + 1} 行格式错误，应为 min,max,points`)
    }

    const min = Number(parts[0])
    const max = Number(parts[1])
    const points = Number(parts[2])
    if (!Number.isInteger(min) || !Number.isInteger(max) || !Number.isFinite(points)) {
      throw new Error(`字典得分第 ${index + 1} 行存在非法数值`)
    }
    return { min, max, points }
  })
}

function buildPreparedDraft(
  draft: StrategyManageRuleDraft,
  scoreMode: 'fixed' | 'dist',
  fixedPointsText: string,
  distPointsText: string,
) {
  const nextDraft: StrategyManageRuleDraft = {
    ...draft,
    name: draft.name.trim(),
    scene_name: draft.scene_name.trim(),
    stage: draft.stage.trim(),
    scope_way: draft.scope_way.trim(),
    when: draft.when.trim(),
    explain: draft.explain.trim(),
  }

  if (scoreMode === 'dist') {
    nextDraft.dist_points = parseDistPointsText(distPointsText)
  } else {
    const parsed = Number(fixedPointsText.trim())
    if (!Number.isFinite(parsed)) {
      throw new Error('固定分值必须是合法数字')
    }
    nextDraft.points = parsed
    nextDraft.dist_points = null
  }

  return nextDraft
}

function sceneStageSummary(rules: StrategyManageRuleItem[]) {
  const counts = new Map<string, number>()
  for (const rule of rules) {
    counts.set(rule.stage, (counts.get(rule.stage) ?? 0) + 1)
  }
  return Array.from(counts.entries())
    .map(([stage, count]) => `${stage} ${count}`)
    .join(' / ')
}

function buildRuleSearchText(rule: StrategyManageRuleItem) {
  return `${rule.name} ${rule.scene_name} ${rule.stage} ${rule.scope_way} ${rule.explain} ${rule.when}`.toLowerCase()
}

function buildSceneSearchText(scene: StrategyManageSceneItem) {
  return `${scene.name} ${scene.direction}`.toLowerCase()
}

export default function StrategyManagePage() {
  const [sourcePath, setSourcePath] = useState('')
  const [scenes, setScenes] = useState<StrategyManageSceneItem[]>([])
  const [rules, setRules] = useState<StrategyManageRuleItem[]>([])
  const [searchKeyword, setSearchKeyword] = useState('')
  const [selectedSceneName, setSelectedSceneName] = useState('')
  const [busyAction, setBusyAction] = useState<BusyAction>('loading')
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [editorError, setEditorError] = useState('')
  const [checkNotice, setCheckNotice] = useState('')
  const [sceneEditorError, setSceneEditorError] = useState('')
  const [editorMode, setEditorMode] = useState<EditorMode>('create')
  const [sceneEditorMode, setSceneEditorMode] = useState<SceneEditorMode>('create')
  const [editingOriginalName, setEditingOriginalName] = useState('')
  const [editingSceneOriginalName, setEditingSceneOriginalName] = useState('')
  const [draft, setDraft] = useState<StrategyManageRuleDraft | null>(null)
  const [sceneDraft, setSceneDraft] = useState<StrategyManageSceneDraft | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<StrategyManageRuleItem | null>(null)
  const [deleteSceneTarget, setDeleteSceneTarget] = useState<DeleteSceneTarget | null>(null)
  const [scoreMode, setScoreMode] = useState<'fixed' | 'dist'>('fixed')
  const [scopeWindowsText, setScopeWindowsText] = useState('1')
  const [fixedPointsText, setFixedPointsText] = useState('0')
  const [distPointsText, setDistPointsText] = useState('')
  const [observeThresholdText, setObserveThresholdText] = useState('1')
  const [triggerThresholdText, setTriggerThresholdText] = useState('2')
  const [confirmThresholdText, setConfirmThresholdText] = useState('3')
  const [failThresholdText, setFailThresholdText] = useState('1')
  const [sceneDirectionText, setSceneDirectionText] = useState('long')
  const [isSyntaxGuideOpen, setIsSyntaxGuideOpen] = useState(false)
  const [isBulkEditorOpen, setIsBulkEditorOpen] = useState(false)
  const [refactorFileName, setRefactorFileName] = useState('score_rule_refactor.toml')
  const [refactorScenes, setRefactorScenes] = useState<RefactorSceneDraft[]>([])
  const [refactorRules, setRefactorRules] = useState<RefactorRuleDraft[]>([])
  const [bulkRuleSceneFilter, setBulkRuleSceneFilter] = useState('ALL')
  const [bulkRuleKeyword, setBulkRuleKeyword] = useState('')
  const [bulkActiveSceneName, setBulkActiveSceneName] = useState('')
  const [bulkNewSceneId, setBulkNewSceneId] = useState('')
  const [bulkError, setBulkError] = useState('')
  const [isAssetModalOpen, setIsAssetModalOpen] = useState(false)

  const selectedScene = useMemo(
    () => scenes.find((item) => item.name === selectedSceneName) ?? null,
    [scenes, selectedSceneName],
  )
  const selectedSceneRules = useMemo(
    () => rules.filter((item) => item.scene_name === selectedSceneName),
    [rules, selectedSceneName],
  )
  const normalizedSearchKeyword = searchKeyword.trim().toLowerCase()
  const filteredScenes = useMemo(() => {
    if (!normalizedSearchKeyword) {
      return scenes
    }

    return scenes.filter((scene) => {
      if (buildSceneSearchText(scene).includes(normalizedSearchKeyword)) {
        return true
      }

      return rules.some(
        (rule) =>
          rule.scene_name === scene.name &&
          buildRuleSearchText(rule).includes(normalizedSearchKeyword),
      )
    })
  }, [normalizedSearchKeyword, rules, scenes])
  const selectedSceneFilteredRules = useMemo(() => {
    if (!normalizedSearchKeyword) {
      return selectedSceneRules
    }

    return selectedSceneRules.filter((rule) =>
      buildRuleSearchText(rule).includes(normalizedSearchKeyword),
    )
  }, [normalizedSearchKeyword, selectedSceneRules])
  const bulkFilteredRules = useMemo(() => {
    const chosenRuleNames = new Set(refactorRules.map((item) => item.name))
    const sceneFiltered =
      bulkRuleSceneFilter === 'ALL'
        ? rules
        : rules.filter((item) => item.scene_name === bulkRuleSceneFilter)

    const notChosen = sceneFiltered.filter((item) => !chosenRuleNames.has(item.name))
    const keyword = bulkRuleKeyword.trim().toLowerCase()
    if (!keyword) {
      return notChosen
    }

    return notChosen.filter((item) => {
      const haystack = `${item.name} ${item.scene_name} ${item.stage} ${item.scope_way} ${item.explain} ${item.when}`.toLowerCase()
      return haystack.includes(keyword)
    })
  }, [rules, bulkRuleSceneFilter, bulkRuleKeyword, refactorRules])

  const bulkValidationIssues = useMemo(() => {
    const issues: string[] = []

    const fileName = refactorFileName.trim()
    if (!fileName) {
      issues.push('输出文件名不能为空')
    }
    if (fileName !== STRATEGY_RULE_FILE_NAME) {
      issues.push(`输出文件名必须为 ${STRATEGY_RULE_FILE_NAME}（覆盖策略文件）`)
    }

    if (refactorScenes.length === 0) {
      issues.push('至少需要一个 Scene')
    }

    if (refactorRules.length === 0) {
      issues.push('至少需要一条 Rule')
    }

    const sceneNameSet = new Set<string>()
    for (const scene of refactorScenes) {
      const name = scene.name.trim()
      if (!name) {
        issues.push('存在空 Scene 名称')
        continue
      }
      const normalizedDirection = scene.direction.trim().toLowerCase()
      if (!SCENE_DIRECTION_OPTIONS.includes(normalizedDirection as (typeof SCENE_DIRECTION_OPTIONS)[number])) {
        issues.push(`Scene ${name} 的 direction 非法（仅支持 long/short）`)
      }
      if (sceneNameSet.has(name)) {
        issues.push(`Scene 名称重复: ${name}`)
      }
      sceneNameSet.add(name)
    }

    const ruleNameSet = new Set<string>()
    for (const rule of refactorRules) {
      const ruleName = rule.name.trim()
      if (!ruleName) {
        issues.push('存在空 Rule 名称')
      } else if (ruleNameSet.has(ruleName)) {
        issues.push(`Rule 名称重复: ${ruleName}`)
      }
      ruleNameSet.add(ruleName)

      if (!rule.scene_name.trim()) {
        issues.push(`Rule ${rule.name || '(未命名)'} 未设置 Scene`)
      } else if (!sceneNameSet.has(rule.scene_name.trim())) {
        issues.push(`Rule ${rule.name || '(未命名)'} 关联的 Scene 不存在: ${rule.scene_name}`)
      }

      if (!rule.when.trim()) {
        issues.push(`Rule ${rule.name || '(未命名)'} 的表达式不能为空`)
      }

      if (!Number.isInteger(rule.scope_windows) || rule.scope_windows < 1) {
        issues.push(`Rule ${rule.name || '(未命名)'} 的 scope_windows 必须是 >= 1 的整数`)
      }
    }

    const sourceRuleNameSet = new Set(rules.map((item) => item.name))
    const unclassifiedRules = Array.from(sourceRuleNameSet).filter((name) => !ruleNameSet.has(name))
    if (unclassifiedRules.length > 0) {
      issues.push(`仍有 ${unclassifiedRules.length} 条原始 Rule 未分类`) 
    }

    return Array.from(new Set(issues))
  }, [refactorFileName, refactorScenes, refactorRules, rules])

  const refactorSceneNames = useMemo(
    () => refactorScenes.map((item) => item.name.trim()).filter(Boolean),
    [refactorScenes],
  )

  const refactorRulesByScene = useMemo(() => {
    const grouped = new Map<string, Array<{ rule: RefactorRuleDraft; index: number }>>()
    refactorSceneNames.forEach((name) => grouped.set(name, []))
    refactorRules.forEach((rule, index) => {
      const sceneName = rule.scene_name.trim()
      const list = grouped.get(sceneName)
      if (list) {
        list.push({ rule, index })
      }
    })
    return grouped
  }, [refactorRules, refactorSceneNames])


  async function loadPage() {
    setBusyAction('loading')
    setError('')
    try {
      const resolvedSourcePath = await ensureManagedSourcePath()
      const data = await getStrategyManagePage(resolvedSourcePath)
      setSourcePath(resolvedSourcePath)
      setScenes(data.scenes ?? [])
      setRules(data.rules ?? [])
      setSelectedSceneName((current) =>
        data.scenes.some((item) => item.name === current) ? current : '',
      )
    } catch (loadError) {
      setError(`读取策略管理失败: ${String(loadError)}`)
      setNotice('')
    } finally {
      setBusyAction('idle')
    }
  }

  useEffect(() => {
    void loadPage()
  }, [])

  function applyPageData(data: StrategyManagePageData, preferSceneName?: string) {
    setScenes(data.scenes ?? [])
    setRules(data.rules ?? [])
    setSelectedSceneName((current) => {
      const nextName = preferSceneName || current || ''
      return data.scenes.some((item) => item.name === nextName)
        ? nextName
        : ''
    })
  }

  function openCreateEditor(sceneName: string) {
    const nextDraft = buildEmptyDraft(sceneName)
    setEditorMode('create')
    setEditingOriginalName('')
    setDraft(nextDraft)
    setScoreMode('fixed')
    setScopeWindowsText(String(nextDraft.scope_windows))
    setFixedPointsText('0')
    setDistPointsText('')
    setEditorError('')
    setCheckNotice('')
    setError('')
    setNotice('')
  }

  function openCreateSceneEditor() {
    const nextDraft = buildEmptySceneDraft()
    setSceneEditorMode('create')
    setEditingSceneOriginalName('')
    setSceneDraft(nextDraft)
    setSceneDirectionText(nextDraft.direction)
    setObserveThresholdText(String(nextDraft.observe_threshold))
    setTriggerThresholdText(String(nextDraft.trigger_threshold))
    setConfirmThresholdText(String(nextDraft.confirm_threshold))
    setFailThresholdText(String(nextDraft.fail_threshold))
    setSceneEditorError('')
    setError('')
    setNotice('')
  }

  function openBulkEditor() {
    const initialSceneName = 'new_scene'
    const initialScene = createRefactorSceneDraft(initialSceneName)
    setRefactorFileName(STRATEGY_RULE_FILE_NAME)
    setRefactorScenes([initialScene])
    setRefactorRules([])
    setBulkRuleSceneFilter('ALL')
    setBulkRuleKeyword('')
    setBulkActiveSceneName(initialSceneName)
    setBulkNewSceneId(initialScene.id)
    setBulkError('')
    setIsBulkEditorOpen(true)
    setError('')
    setNotice('')
  }

  function closeBulkEditor() {
    setIsBulkEditorOpen(false)
    setBulkError('')
    setBulkRuleKeyword('')
    setBulkActiveSceneName('')
    setBulkNewSceneId('')
  }

  function addRefactorScene() {
    setRefactorScenes((current) => {
      const nextName = `scene_${current.length + 1}`
      const nextScene = createRefactorSceneDraft(nextName)
      setBulkActiveSceneName(nextName)
      setBulkNewSceneId(nextScene.id)
      return [...current, nextScene]
    })
  }

  function updateRefactorScene(sceneId: string, key: keyof RefactorSceneDraft, value: string) {
    setRefactorScenes((current) =>
      current.map((item) => {
        if (item.id !== sceneId) {
          return item
        }
        if (key === 'name') {
          return { ...item, name: value }
        }
        if (key === 'direction') {
          return { ...item, direction: value }
        }
        return { ...item, [key]: Number(value) }
      }),
    )
    setBulkNewSceneId((current) => (current === sceneId ? sceneId : current))
  }

  function removeRefactorScene(sceneId: string) {
    const removingScene = refactorScenes.find((item) => item.id === sceneId)
    const fallbackSceneName = refactorScenes.find((item) => item.id !== sceneId)?.name ?? ''
    setRefactorScenes((current) => current.filter((item) => item.id !== sceneId))
    if (removingScene) {
      setRefactorRules((current) => current.filter((item) => item.scene_name !== removingScene.name))
      if (bulkActiveSceneName === removingScene.name) {
        setBulkActiveSceneName(fallbackSceneName)
      }
      if (bulkNewSceneId === removingScene.id) {
        setBulkNewSceneId('')
      }
    }
  }

  function addRuleToRefactor(rule: StrategyManageRuleItem) {
    if (!bulkActiveSceneName.trim()) {
      setBulkError('请先点击一个 Scene 篮子')
      return
    }
    setRefactorRules((current) => {
      const exists = current.some((item) => item.name === rule.name)
      const uniqueName = exists ? `${rule.name}_${current.length + 1}` : rule.name
      return [
        ...current,
        {
          ...buildDraftFromRule(rule),
          name: uniqueName,
          scene_name: bulkActiveSceneName,
        },
      ]
    })
    setBulkError('')
  }

  function removeRuleFromScene(ruleIndex: number) {
    setRefactorRules((current) => current.filter((_, idx) => idx !== ruleIndex))
  }

  function openEditSceneEditor(scene: StrategyManageSceneItem) {
    const nextDraft = buildSceneDraftFromScene(scene)
    setSceneEditorMode('edit')
    setEditingSceneOriginalName(scene.name)
    setSceneDraft(nextDraft)
    setSceneDirectionText(nextDraft.direction)
    setObserveThresholdText(String(nextDraft.observe_threshold))
    setTriggerThresholdText(String(nextDraft.trigger_threshold))
    setConfirmThresholdText(String(nextDraft.confirm_threshold))
    setFailThresholdText(String(nextDraft.fail_threshold))
    setSceneEditorError('')
    setError('')
    setNotice('')
  }

  function openEditEditor(rule: StrategyManageRuleItem) {
    const nextDraft = buildDraftFromRule(rule)
    setEditorMode('edit')
    setEditingOriginalName(rule.name)
    setDraft(nextDraft)
    setScopeWindowsText(String(nextDraft.scope_windows))
    if (hasDistPoints(rule.dist_points)) {
      setScoreMode('dist')
      setDistPointsText(distPointsToText(rule.dist_points))
      setFixedPointsText(String(rule.points))
    } else {
      setScoreMode('fixed')
      setFixedPointsText(String(rule.points))
      setDistPointsText('')
    }
    setEditorError('')
    setCheckNotice('')
    setError('')
    setNotice('')
  }

  async function onSaveDraft() {
    if (!draft) {
      return
    }
    if (!sourcePath.trim()) {
      setEditorError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    let preparedDraft: StrategyManageRuleDraft
    try {
      preparedDraft = buildPreparedDraft(
        {
          ...draft,
          scope_windows: parseRequiredInteger(scopeWindowsText, '窗口', 1),
        },
        scoreMode,
        fixedPointsText,
        distPointsText,
      )
      const message = await checkStrategyManageRuleDraft(
        sourcePath,
        preparedDraft,
        editorMode === 'edit' ? editingOriginalName : undefined,
      )
      setNotice(message)
      setEditorError('')
      setCheckNotice(message)
    } catch (checkError) {
      setEditorError(`策略校验失败: ${String(checkError)}`)
      return
    }

    setBusyAction('saving')
    setEditorError('')
    try {
      const data =
        editorMode === 'create'
          ? await createStrategyManageRule(sourcePath, preparedDraft)
          : await updateStrategyManageRule(sourcePath, editingOriginalName, preparedDraft)
      applyPageData(data, preparedDraft.scene_name)
      setDraft(null)
      setEditingOriginalName('')
      setNotice(editorMode === 'create' ? '规则已创建。' : '规则已更新。')
    } catch (saveError) {
      setEditorError(`保存策略失败: ${String(saveError)}`)
      setNotice('')
    } finally {
      setBusyAction('idle')
    }
  }

  async function onCheckDraft() {
    if (!draft) {
      return
    }
    if (!sourcePath.trim()) {
      setEditorError('当前数据目录为空，无法检查草稿。')
      return
    }
    try {
      const preparedDraft = buildPreparedDraft(
        {
          ...draft,
          scope_windows: parseRequiredInteger(scopeWindowsText, '窗口', 1),
        },
        scoreMode,
        fixedPointsText,
        distPointsText,
      )
      const message = await checkStrategyManageRuleDraft(
        sourcePath,
        preparedDraft,
        editorMode === 'edit' ? editingOriginalName : undefined,
      )
      setCheckNotice(message)
      setEditorError('')
    } catch (checkError) {
      setEditorError(`检查策略失败: ${String(checkError)}`)
      setCheckNotice('')
    }
  }

  async function onSaveSceneDraft() {
    if (!sceneDraft) {
      return
    }
    if (!sourcePath.trim()) {
      setSceneEditorError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    let preparedSceneDraft: StrategyManageSceneDraft
    try {
      preparedSceneDraft = buildPreparedSceneDraft({
        ...sceneDraft,
        direction: sceneDirectionText,
        observe_threshold: parseRequiredNumber(observeThresholdText, 'observe_threshold'),
        trigger_threshold: parseRequiredNumber(triggerThresholdText, 'trigger_threshold'),
        confirm_threshold: parseRequiredNumber(confirmThresholdText, 'confirm_threshold'),
        fail_threshold: parseRequiredNumber(failThresholdText, 'fail_threshold'),
      })
    } catch (parseError) {
      setSceneEditorError(String(parseError))
      return
    }
    try {
      const message = await checkStrategyManageSceneDraft(
        sourcePath,
        preparedSceneDraft,
        sceneEditorMode === 'edit' ? editingSceneOriginalName : undefined,
      )
      setNotice(message)
      setSceneEditorError('')
    } catch (checkError) {
      setSceneEditorError(`scene 校验失败: ${String(checkError)}`)
      return
    }

    setBusyAction('saving')
    try {
      const data =
        sceneEditorMode === 'create'
          ? await createStrategyManageScene(sourcePath, preparedSceneDraft)
          : await updateStrategyManageScene(sourcePath, editingSceneOriginalName, preparedSceneDraft)
      applyPageData(data, preparedSceneDraft.name)
      setSceneDraft(null)
      setEditingSceneOriginalName('')
      setNotice(sceneEditorMode === 'create' ? 'scene 已创建。' : 'scene 已更新。')
    } catch (saveError) {
      setSceneEditorError(`保存 scene 失败: ${String(saveError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onSaveBulkScene() {
    if (!sourcePath.trim()) {
      setBulkError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    if (bulkValidationIssues.length > 0) {
      setBulkError(`请先修正后再保存：${bulkValidationIssues[0]}`)
      return
    }

    setBusyAction('saving')
    setBulkError('')
    setError('')
    try {
      const outputPath = await saveStrategyManageRefactorFile(sourcePath, refactorFileName.trim(), {
        scenes: refactorScenes.map((scene) => ({
          name: scene.name.trim(),
          direction: scene.direction.trim().toLowerCase(),
          observe_threshold: scene.observe_threshold,
          trigger_threshold: scene.trigger_threshold,
          confirm_threshold: scene.confirm_threshold,
          fail_threshold: scene.fail_threshold,
        })),
        rules: refactorRules.map((rule) => ({
          ...rule,
          name: rule.name.trim(),
          scene_name: rule.scene_name.trim(),
          stage: rule.stage.trim(),
          scope_way: rule.scope_way.trim(),
          when: rule.when.trim(),
          explain: rule.explain.trim(),
        })),
      })
      setNotice(`策略重构文件已保存: ${outputPath}`)
      setIsBulkEditorOpen(false)
    } catch (bulkSaveError) {
      setBulkError(`整体编辑保存失败: ${String(bulkSaveError)}`)
      setNotice('')
    } finally {
      setBusyAction('idle')
    }
  }

  async function onConfirmDeleteScene() {
    if (!deleteSceneTarget) {
      return
    }
    if (!sourcePath.trim()) {
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }
    setBusyAction('deleting')
    setError('')
    try {
      const data = await removeStrategyManageScene(sourcePath, deleteSceneTarget.name)
      applyPageData(data)
      if (editingSceneOriginalName === deleteSceneTarget.name) {
        setSceneDraft(null)
        setEditingSceneOriginalName('')
      }
      if (selectedSceneName === deleteSceneTarget.name) {
        setSelectedSceneName('')
      }
      setNotice(`已删除 scene: ${deleteSceneTarget.name}`)
      setDeleteSceneTarget(null)
    } catch (deleteError) {
      setError(`删除 scene 失败: ${String(deleteError)}`)
      setNotice('')
    } finally {
      setBusyAction('idle')
    }
  }

  async function onConfirmDelete() {
    if (!deleteTarget) {
      return
    }
    if (!sourcePath.trim()) {
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }
    setBusyAction('deleting')
    setError('')
    try {
      const data = await removeStrategyManageRules(sourcePath, [deleteTarget.name])
      applyPageData(data, deleteTarget.scene_name)
      if (editingOriginalName === deleteTarget.name) {
        setDraft(null)
        setEditingOriginalName('')
      }
      setNotice(`已删除规则: ${deleteTarget.name}`)
      setDeleteTarget(null)
    } catch (deleteError) {
      setError(`删除规则失败: ${String(deleteError)}`)
      setNotice('')
    } finally {
      setBusyAction('idle')
    }
  }

  const isBusy = busyAction !== 'idle'
  const isEditing = draft !== null
  const isDeleteSceneBlocked = Boolean(deleteSceneTarget && deleteSceneTarget.rule_count > 0)
  const bulkTotalRuleCount = rules.length
  const bulkClassifiedCount = refactorRules.length
  const bulkPendingCount = bulkFilteredRules.length

  useEffect(() => {
    if (!isSyntaxGuideOpen) {
      return
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        setIsSyntaxGuideOpen(false)
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [isSyntaxGuideOpen])

  return (
    <div className="strategy-manage-page">
      <section className="strategy-manage-card">
        <div className="strategy-manage-section-head">
          <div>
            <h2 className="strategy-manage-title">策略管理</h2>
            <p className="strategy-manage-note">
              编辑当前生效的策略文件；历史导入和备份都收纳在顶部的策略资产中心里。
            </p>
          </div>
          <span className="strategy-manage-tip">当前共 {rules.length} 条 rule</span>
        </div>

        <div className="strategy-manage-toolbar">
          <div className="strategy-manage-toolbar-left">
            <button
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-accent"
              type="button"
              onClick={() => setIsAssetModalOpen(true)}
            >
              策略资产中心
            </button>
            <button
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-primary"
              type="button"
              onClick={openCreateSceneEditor}
            >
              新建 Scene
            </button>
            <button
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
              type="button"
              onClick={openBulkEditor}
            >
              策略整体编辑
            </button>
            <button
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
              type="button"
              onClick={() => setIsSyntaxGuideOpen(true)}
            >
              语法说明书
            </button>
            {isEditing ? <span className="strategy-manage-tip">当前有未提交草稿</span> : null}
          </div>
        </div>

        <div className="strategy-manage-summary">
          <div className="strategy-manage-summary-item">
            <span>Scene 数量</span>
            <strong>{scenes.length}</strong>
          </div>
          <div className="strategy-manage-summary-item">
            <span>Rule 数量</span>
            <strong>{rules.length}</strong>
          </div>
        </div>

        <div className="strategy-manage-filter-grid">
          <label className="strategy-manage-field strategy-manage-field-span-full">
            <span>策略搜索</span>
            <input
              value={searchKeyword}
              onChange={(event) => setSearchKeyword(event.target.value)}
              placeholder="搜索 scene / rule 名称 / 表达式 / 说明 / stage / scope"
            />
          </label>
        </div>

        {notice ? <div className="strategy-manage-message strategy-manage-message-notice">{notice}</div> : null}
        {error ? <div className="strategy-manage-message strategy-manage-message-error">{error}</div> : null}
      </section>

      <section className="strategy-manage-card">
        <div className="strategy-manage-list-head">
          <strong>Scene 总览</strong>
          <span>
            {normalizedSearchKeyword
              ? `匹配 ${filteredScenes.length} / ${scenes.length} 个 scene`
              : '点击 scene 打开浮窗'}
          </span>
        </div>
        {filteredScenes.length === 0 ? (
          <div className="strategy-manage-empty">没有匹配当前搜索条件的 scene / rule。</div>
        ) : scenes.length === 0 ? (
          <div className="strategy-manage-empty">当前规则文件里没有 scene。</div>
        ) : (
          <div className="strategy-manage-scene-grid">
            {filteredScenes.map((scene) => {
              const sceneRules = rules.filter((item) => item.scene_name === scene.name)
              const matchedRuleCount = normalizedSearchKeyword
                ? sceneRules.filter((item) =>
                    buildRuleSearchText(item).includes(normalizedSearchKeyword),
                  ).length
                : sceneRules.length
              return (
                <button
                  key={scene.name}
                  type="button"
                  className="strategy-manage-scene-card"
                  onClick={() => setSelectedSceneName(scene.name)}
                >
                  <div className="strategy-manage-scene-card-head">
                    <strong>{scene.name}</strong>
                    <div className="strategy-manage-rule-card-actions">
                      <span>
                        {normalizedSearchKeyword
                          ? `命中 ${matchedRuleCount} / ${scene.rule_count} 条`
                          : `${scene.rule_count} 条规则`}
                      </span>
                      <button
                        type="button"
                        className="strategy-manage-inline-btn"
                        onClick={(event) => {
                          event.stopPropagation()
                          openEditSceneEditor(scene)
                        }}
                      >
                        配置
                      </button>
                      <button
                        type="button"
                        className="strategy-manage-inline-btn is-danger"
                        onClick={(event) => {
                          event.stopPropagation()
                          setDeleteSceneTarget({ name: scene.name, rule_count: scene.rule_count })
                        }}
                        disabled={isBusy}
                      >
                        删除
                      </button>
                    </div>
                  </div>
                  <div className="strategy-manage-scene-metrics">
                    <span>direction {scene.direction}</span>
                    <span>observe {formatNumber(scene.observe_threshold)}</span>
                    <span>trigger {formatNumber(scene.trigger_threshold)}</span>
                    <span>confirm {formatNumber(scene.confirm_threshold)}</span>
                    <span>fail {formatNumber(scene.fail_threshold)}</span>
                  </div>
                  <p className="strategy-manage-note">{sceneStageSummary(sceneRules) || '暂无规则'}</p>
                </button>
              )
            })}
          </div>
        )}
      </section>

      {deleteTarget ? (
        <div className="strategy-manage-modal-backdrop strategy-manage-modal-backdrop-confirm" role="presentation">
          <div className="strategy-manage-modal" role="dialog" aria-modal="true">
            <h3>删除 Rule</h3>
            <p>
              即将删除规则：<strong>{deleteTarget.name}</strong>
            </p>
            <div className="strategy-manage-modal-actions">
              <button
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-danger"
                type="button"
                onClick={() => void onConfirmDelete()}
                disabled={isBusy}
              >
                {busyAction === 'deleting' ? '删除中...' : '确认删除'}
              </button>
              <button className="strategy-manage-toolbar-btn" type="button" onClick={() => setDeleteTarget(null)} disabled={isBusy}>
                取消
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {deleteSceneTarget ? (
        <div className="strategy-manage-modal-backdrop strategy-manage-modal-backdrop-confirm" role="presentation">
          <div className="strategy-manage-modal" role="dialog" aria-modal="true">
            <h3>删除 Scene</h3>
            <p>
              即将删除 scene：<strong>{deleteSceneTarget.name}</strong>
            </p>
            {deleteSceneTarget.rule_count > 0 ? (
              <p className="strategy-manage-note">当前 scene 下还有 {deleteSceneTarget.rule_count} 条 rule，请先删除这些 rule 后再删除 scene。</p>
            ) : null}
            <div className="strategy-manage-modal-actions">
              <button
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-danger"
                type="button"
                onClick={() => void onConfirmDeleteScene()}
                disabled={isBusy || isDeleteSceneBlocked}
              >
                {isDeleteSceneBlocked ? '请先清空 Rule' : busyAction === 'deleting' ? '删除中...' : '确认删除'}
              </button>
              <button
                className="strategy-manage-toolbar-btn"
                type="button"
                onClick={() => setDeleteSceneTarget(null)}
                disabled={isBusy}
              >
                取消
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {selectedScene ? (
        <div className="strategy-manage-modal-backdrop" role="presentation">
          <div className="strategy-manage-modal strategy-manage-editor-modal" role="dialog" aria-modal="true">
            <div className="strategy-manage-section-head">
              <div>
                <h3 className="strategy-manage-subtitle">{selectedScene.name}</h3>
                <p className="strategy-manage-note">当前 scene 下共 {selectedSceneRules.length} 条规则。</p>
              </div>
              <div className="strategy-manage-toolbar-right">
                <button
                  className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-primary"
                  type="button"
                  onClick={() => openCreateEditor(selectedScene.name)}
                >
                  新建 Rule
                </button>
                <button className="strategy-manage-toolbar-btn" type="button" onClick={() => setSelectedSceneName('')}>
                  关闭
                </button>
              </div>
            </div>

            <div className="strategy-manage-scene-metrics-panel">
              <span>direction {selectedScene.direction}</span>
              <span>observe {formatNumber(selectedScene.observe_threshold)}</span>
              <span>trigger {formatNumber(selectedScene.trigger_threshold)}</span>
              <span>confirm {formatNumber(selectedScene.confirm_threshold)}</span>
              <span>fail {formatNumber(selectedScene.fail_threshold)}</span>
            </div>

            <div className="strategy-manage-list-panel strategy-manage-list-panel-full">
              <div className="strategy-manage-list-head">
                <strong>Rule 列表</strong>
                <span>
                  {normalizedSearchKeyword
                    ? `命中 ${selectedSceneFilteredRules.length} / ${selectedSceneRules.length} 条`
                    : `${selectedSceneRules.length} 条`}
                </span>
              </div>
              {selectedSceneFilteredRules.length === 0 ? (
                <div className="strategy-manage-empty">当前 scene 下没有匹配搜索条件的规则。</div>
              ) : selectedSceneRules.length === 0 ? (
                <div className="strategy-manage-empty">当前 scene 下还没有规则。</div>
              ) : (
                <div className="strategy-manage-scene-rule-list">
                  {selectedSceneFilteredRules.map((rule) => {
                    const distScoreSummary = buildDistScoreSummary(rule.dist_points)

                    return (
                      <article className="strategy-manage-rule-card strategy-manage-rule-card-compact" key={rule.name}>
                        <div className="strategy-manage-rule-card-head">
                          <div>
                            <div className="strategy-manage-rule-card-name">{rule.name}</div>
                          </div>
                          <div className="strategy-manage-rule-card-actions">
                            <button className="strategy-manage-inline-btn" type="button" onClick={() => openEditEditor(rule)}>
                              编辑
                            </button>
                            <button className="strategy-manage-inline-btn is-danger" type="button" onClick={() => setDeleteTarget(rule)} disabled={isBusy}>
                              删除
                            </button>
                          </div>
                        </div>
                        <div className="strategy-manage-rule-metrics">
                          {distScoreSummary ? (
                            <div className="strategy-manage-summary-item strategy-manage-summary-item-dist-score">
                              <span>得分</span>
                              <strong>字典分 · {distScoreSummary.segmentCount} 段</strong>
                              <small>
                                区间 {formatNumber(distScoreSummary.intervalMin)} ~ {formatNumber(distScoreSummary.intervalMax)} ·
                                分值 {formatNumber(distScoreSummary.pointsMin)} ~ {formatNumber(distScoreSummary.pointsMax)}
                              </small>
                            </div>
                          ) : (
                            <div className="strategy-manage-summary-item">
                              <span>得分</span>
                              <strong>{formatNumber(rule.points)}</strong>
                            </div>
                          )}
                          <div className="strategy-manage-summary-item">
                            <span>Stage</span>
                            <strong>{rule.stage}</strong>
                          </div>
                          <div className="strategy-manage-summary-item">
                            <span>Scope</span>
                            <strong>{rule.scope_way}</strong>
                          </div>
                          <div className="strategy-manage-summary-item">
                            <span>Windows</span>
                            <strong>{rule.scope_windows}</strong>
                          </div>
                        </div>
                        <pre className="strategy-manage-expression-preview">{rule.when}</pre>
                      </article>
                    )
                  })}
                </div>
              )}
            </div>
          </div>
        </div>
      ) : null}

      {draft ? (
        <div className="strategy-manage-modal-backdrop strategy-manage-modal-backdrop-top" role="presentation">
          <div className="strategy-manage-modal strategy-manage-editor-modal" role="dialog" aria-modal="true">
            <div className="strategy-manage-list-head">
              <strong>{editorMode === 'create' ? '新建 Rule' : `编辑 Rule · ${editingOriginalName}`}</strong>
              <span>{draft.scene_name || '未选择 scene'}</span>
            </div>
            {editorError ? <div className="strategy-manage-message strategy-manage-message-error">{editorError}</div> : null}
            {checkNotice ? <div className="strategy-manage-message strategy-manage-message-notice">{checkNotice}</div> : null}
            <div className="strategy-manage-editor-grid">
              <label className="strategy-manage-field">
                <span>名称</span>
                <input value={draft.name} onChange={(event) => setDraft({ ...draft, name: event.target.value })} />
              </label>
              <label className="strategy-manage-field">
                <span>Scene</span>
                <select value={draft.scene_name} onChange={(event) => setDraft({ ...draft, scene_name: event.target.value })}>
                  {scenes.map((scene) => (
                    <option key={scene.name} value={scene.name}>
                      {scene.name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="strategy-manage-field">
                <span>Stage</span>
                <select value={draft.stage} onChange={(event) => setDraft({ ...draft, stage: event.target.value })}>
                  {STAGE_OPTIONS.map((item) => (
                    <option key={item} value={item}>
                      {item}
                    </option>
                  ))}
                </select>
              </label>
              <label className="strategy-manage-field">
                <span>Scope</span>
                <select value={draft.scope_way} onChange={(event) => setDraft({ ...draft, scope_way: event.target.value })}>
                  {SCOPE_OPTIONS.map((item) => (
                    <option key={item} value={item}>
                      {item}
                    </option>
                  ))}
                </select>
              </label>
              <label className="strategy-manage-field">
                <span>窗口</span>
                <input
                  type="number"
                  min={1}
                  step={1}
                  value={scopeWindowsText}
                  onChange={(event) => setScopeWindowsText(event.target.value)}
                />
              </label>
              <label className="strategy-manage-field strategy-manage-field-span-full">
                <span>说明</span>
                <input value={draft.explain} onChange={(event) => setDraft({ ...draft, explain: event.target.value })} />
              </label>
              <label className="strategy-manage-field strategy-manage-field-span-full">
                <span>表达式</span>
                <textarea rows={8} value={draft.when} onChange={(event) => setDraft({ ...draft, when: event.target.value })} />
              </label>
              <div className="strategy-manage-field strategy-manage-field-span-full">
                <span>得分方式</span>
                <div className="strategy-manage-score-mode">
                  <button
                    type="button"
                    className={scoreMode === 'fixed' ? 'strategy-manage-score-mode-btn is-active' : 'strategy-manage-score-mode-btn'}
                    onClick={() => setScoreMode('fixed')}
                  >
                    固定分
                  </button>
                  <button
                    type="button"
                    className={scoreMode === 'dist' ? 'strategy-manage-score-mode-btn is-active' : 'strategy-manage-score-mode-btn'}
                    onClick={() => setScoreMode('dist')}
                  >
                    区间字典
                  </button>
                </div>
              </div>
              {scoreMode === 'fixed' ? (
                <label className="strategy-manage-field strategy-manage-field-span-full">
                  <span>固定分值</span>
                  <input value={fixedPointsText} onChange={(event) => setFixedPointsText(event.target.value)} />
                </label>
              ) : (
                <label className="strategy-manage-field strategy-manage-field-span-full">
                  <span>区间字典，每行 `min,max,points`</span>
                  <textarea rows={6} value={distPointsText} onChange={(event) => setDistPointsText(event.target.value)} />
                </label>
              )}
            </div>
            <div className="strategy-manage-editor-actions">
              <button
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
                type="button"
                onClick={() => void onCheckDraft()}
                disabled={isBusy}
              >
                检查草稿
              </button>
              <button className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-primary" type="button" onClick={() => void onSaveDraft()} disabled={isBusy}>
                {busyAction === 'saving' ? '保存中...' : '保存'}
              </button>
              <button
                className="strategy-manage-toolbar-btn"
                type="button"
                onClick={() => {
                  setDraft(null)
                  setEditingOriginalName('')
                  setEditorError('')
                }}
                disabled={isBusy}
              >
                取消
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {sceneDraft ? (
        <div className="strategy-manage-modal-backdrop" role="presentation">
          <div className="strategy-manage-modal" role="dialog" aria-modal="true">
            <div className="strategy-manage-list-head">
              <strong>{sceneEditorMode === 'create' ? '新建 Scene' : `配置 Scene · ${editingSceneOriginalName}`}</strong>
              <span>scene 方向、阈值与证据分</span>
            </div>
            {sceneEditorError ? <div className="strategy-manage-message strategy-manage-message-error">{sceneEditorError}</div> : null}
            <div className="strategy-manage-editor-grid strategy-manage-editor-grid-scene">
              <label className="strategy-manage-field strategy-manage-field-span-full">
                <span>名称</span>
                <input value={sceneDraft.name} onChange={(event) => setSceneDraft({ ...sceneDraft, name: event.target.value })} />
              </label>
              <label className="strategy-manage-field">
                <span>direction</span>
                <select value={sceneDirectionText} onChange={(event) => setSceneDirectionText(event.target.value)}>
                  {SCENE_DIRECTION_OPTIONS.map((item) => (
                    <option key={item} value={item}>
                      {item}
                    </option>
                  ))}
                </select>
              </label>
              <label className="strategy-manage-field">
                <span>observe_threshold</span>
                <input
                  type="number"
                  step="0.1"
                  value={observeThresholdText}
                  onChange={(event) => setObserveThresholdText(event.target.value)}
                />
              </label>
              <label className="strategy-manage-field">
                <span>trigger_threshold</span>
                <input
                  type="number"
                  step="0.1"
                  value={triggerThresholdText}
                  onChange={(event) => setTriggerThresholdText(event.target.value)}
                />
              </label>
              <label className="strategy-manage-field">
                <span>confirm_threshold</span>
                <input
                  type="number"
                  step="0.1"
                  value={confirmThresholdText}
                  onChange={(event) => setConfirmThresholdText(event.target.value)}
                />
              </label>
              <label className="strategy-manage-field">
                <span>fail_threshold</span>
                <input
                  type="number"
                  step="0.1"
                  value={failThresholdText}
                  onChange={(event) => setFailThresholdText(event.target.value)}
                />
              </label>
            </div>
            <div className="strategy-manage-editor-actions">
              <button className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-primary" type="button" onClick={() => void onSaveSceneDraft()} disabled={isBusy}>
                {busyAction === 'saving' ? '保存中...' : '保存'}
              </button>
              <button
                className="strategy-manage-toolbar-btn"
                type="button"
                onClick={() => {
                  setSceneDraft(null)
                  setEditingSceneOriginalName('')
                  setSceneEditorError('')
                }}
                disabled={isBusy}
              >
                取消
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {isBulkEditorOpen ? (
        <div className="strategy-manage-modal-backdrop" role="presentation">
          <div className="strategy-manage-modal strategy-manage-editor-modal" role="dialog" aria-modal="true">
            <div className="strategy-manage-list-head strategy-manage-bulk-head">
              <strong>策略整理台</strong>
              <span>候选池点选加入；Scene 方块内点 Rule 即移出</span>
            </div>
            {bulkError ? <div className="strategy-manage-message strategy-manage-message-error">{bulkError}</div> : null}

            <div className="strategy-manage-bulk-top-inline">
              <span className="strategy-manage-tip">将直接覆盖：{refactorFileName}</span>
              <div className="strategy-manage-bulk-kpi">
                <div className="strategy-manage-summary-item">
                  <span>总 Rule</span>
                  <strong>{bulkTotalRuleCount}</strong>
                </div>
                <div className="strategy-manage-summary-item">
                  <span>已分类</span>
                  <strong>{bulkClassifiedCount}</strong>
                </div>
                <div className="strategy-manage-summary-item">
                  <span>待分类</span>
                  <strong>{bulkPendingCount}</strong>
                </div>
              </div>
            </div>

            <div className="strategy-manage-list-head">
              <div className="strategy-manage-bulk-row-actions">
                <span className="strategy-manage-tip">当前放入目标：{bulkActiveSceneName || '未选择'}</span>
                <button className="strategy-manage-inline-btn" type="button" onClick={addRefactorScene}>新增 Scene</button>
              </div>
            </div>

            <div className="strategy-manage-bulk-simple-board">
              <section className="strategy-manage-bulk-board-col">
                <div className="strategy-manage-list-head">
                  <strong>候选 Rule 池（点击放入当前目标 Scene）</strong>
                  <span>{bulkFilteredRules.length} 条待分类</span>
                </div>
                <div className="strategy-manage-bulk-filter-bar strategy-manage-bulk-filter-bar-simple strategy-manage-bulk-filter-bar-compact">
                  <label className="strategy-manage-field">
                    <span>按 scene 过滤</span>
                    <select value={bulkRuleSceneFilter} onChange={(event) => setBulkRuleSceneFilter(event.target.value)}>
                      <option value="ALL">全部</option>
                      {scenes.map((scene) => (
                        <option key={scene.name} value={scene.name}>{scene.name}</option>
                      ))}
                    </select>
                  </label>
                  <label className="strategy-manage-field strategy-manage-field-grow">
                    <span>关键词</span>
                    <input
                      value={bulkRuleKeyword}
                      onChange={(event) => setBulkRuleKeyword(event.target.value)}
                      placeholder="名称 / 表达式 / 说明"
                    />
                  </label>
                </div>
                <div className="strategy-manage-bulk-rule-list strategy-manage-bulk-rule-pool">
                  {bulkFilteredRules.map((rule) => (
                    <button
                      key={rule.name}
                      type="button"
                      className="strategy-manage-bulk-rule-item strategy-manage-bulk-rule-click"
                      onClick={() => addRuleToRefactor(rule)}
                    >
                      <div>
                        <div className="strategy-manage-rule-card-name">{rule.name}</div>
                        <div className="strategy-manage-tip">{rule.scene_name} · {rule.stage} · {rule.scope_way}</div>
                      </div>
                    </button>
                  ))}
                </div>
              </section>

              <section className="strategy-manage-bulk-board-col strategy-manage-bulk-board-col-center">
                <div className="strategy-manage-list-head">
                  <strong>Scene 篮子区（点击 Scene 设为放入目标）</strong>
                  <span>{refactorRules.length} 条已加入</span>
                </div>
                <div className="strategy-manage-bulk-validation">
                  <div className="strategy-manage-bulk-validation-head">
                    <strong>校验</strong>
                    <span>{bulkValidationIssues.length === 0 ? '通过' : `${bulkValidationIssues.length} 个问题`}</span>
                  </div>
                  {bulkValidationIssues.length > 0 ? (
                    <ul className="strategy-manage-bulk-issue-list">
                      {bulkValidationIssues.slice(0, 6).map((issue) => (
                        <li key={issue}>{issue}</li>
                      ))}
                      {bulkValidationIssues.length > 6 ? <li key="__more">... 另有 {bulkValidationIssues.length - 6} 项</li> : null}
                    </ul>
                  ) : (
                    <p className="strategy-manage-note">当前结构合法，可直接保存覆盖策略文件。</p>
                  )}
                </div>
                <div className="strategy-manage-bulk-scene-strip">
                  {refactorScenes.map((scene) => {
                    const sceneName = scene.name.trim()
                    const bucket = sceneName ? (refactorRulesByScene.get(sceneName) ?? []) : []
                    const active = bulkActiveSceneName === sceneName
                    const isNewScene = bulkNewSceneId === scene.id
                    return (
                      <article key={scene.id} className={active ? 'strategy-manage-bulk-scene-box is-active' : 'strategy-manage-bulk-scene-box'}>
                        <button
                          type="button"
                          className="strategy-manage-bulk-scene-anchor"
                          onClick={() => setBulkActiveSceneName(sceneName)}
                        >
                          <div className="strategy-manage-bulk-row-head">
                            <strong>{sceneName || '未命名 Scene'}</strong>
                            <span>{bucket.length} 条</span>
                          </div>
                        </button>

                        {isNewScene ? (
                          <div className="strategy-manage-editor-grid strategy-manage-editor-grid-scene">
                            <label className="strategy-manage-field strategy-manage-field-span-full">
                              <span>Scene 名称</span>
                              <input value={scene.name} onChange={(event) => updateRefactorScene(scene.id, 'name', event.target.value)} />
                            </label>
                            <label className="strategy-manage-field">
                              <span>direction</span>
                              <select value={scene.direction} onChange={(event) => updateRefactorScene(scene.id, 'direction', event.target.value)}>
                                {SCENE_DIRECTION_OPTIONS.map((item) => (
                                  <option key={item} value={item}>
                                    {item}
                                  </option>
                                ))}
                              </select>
                            </label>
                            <label className="strategy-manage-field"><span>observe</span><input type="number" step="0.1" value={scene.observe_threshold} onChange={(event) => updateRefactorScene(scene.id, 'observe_threshold', event.target.value)} /></label>
                            <label className="strategy-manage-field"><span>trigger</span><input type="number" step="0.1" value={scene.trigger_threshold} onChange={(event) => updateRefactorScene(scene.id, 'trigger_threshold', event.target.value)} /></label>
                            <label className="strategy-manage-field"><span>confirm</span><input type="number" step="0.1" value={scene.confirm_threshold} onChange={(event) => updateRefactorScene(scene.id, 'confirm_threshold', event.target.value)} /></label>
                            <label className="strategy-manage-field"><span>fail</span><input type="number" step="0.1" value={scene.fail_threshold} onChange={(event) => updateRefactorScene(scene.id, 'fail_threshold', event.target.value)} /></label>
                          </div>
                        ) : (
                          <div className="strategy-manage-bulk-scene-metrics">
                            <span>direction {scene.direction}</span>
                            <span>observe {formatNumber(scene.observe_threshold)}</span>
                            <span>trigger {formatNumber(scene.trigger_threshold)}</span>
                            <span>confirm {formatNumber(scene.confirm_threshold)}</span>
                            <span>fail {formatNumber(scene.fail_threshold)}</span>
                          </div>
                        )}

                        <div className="strategy-manage-bulk-row-actions">
                          {isNewScene ? (
                            <button className="strategy-manage-inline-btn" type="button" onClick={() => setBulkNewSceneId('')}>
                              完成配置
                            </button>
                          ) : (
                            <button className="strategy-manage-inline-btn" type="button" onClick={() => setBulkNewSceneId(scene.id)}>
                              编辑配置
                            </button>
                          )}
                          <button className="strategy-manage-inline-btn is-danger" type="button" onClick={() => removeRefactorScene(scene.id)}>
                            删除 Scene
                          </button>
                        </div>

                        <div className="strategy-manage-bulk-scene-rules">
                          {bucket.length === 0 ? <div className="strategy-manage-empty">篮子内暂无 Rule</div> : null}
                          {bucket.map(({ rule, index }) => (
                            <button
                              key={`${rule.name}-${index}`}
                              type="button"
                              className="strategy-manage-bulk-bucket-item strategy-manage-bulk-rule-click"
                              onClick={() => removeRuleFromScene(index)}
                            >
                              <div className="strategy-manage-rule-card-name">{rule.name || '未命名 Rule'}</div>
                              <div className="strategy-manage-tip">{rule.stage} · {rule.scope_way} · 点击移出</div>
                            </button>
                          ))}
                        </div>
                      </article>
                    )
                  })}
                </div>
              </section>
            </div>

            <div className="strategy-manage-editor-actions">
              <button className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-primary" type="button" onClick={() => void onSaveBulkScene()} disabled={isBusy}>
                {busyAction === 'saving' ? '保存中...' : '保存为新策略文件'}
              </button>
              <button className="strategy-manage-toolbar-btn" type="button" onClick={closeBulkEditor} disabled={isBusy}>
                取消
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {isSyntaxGuideOpen ? (
        <div
          className="strategy-manage-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              setIsSyntaxGuideOpen(false)
            }
          }}
        >
          <div className="strategy-manage-modal strategy-manage-guide-modal" role="dialog" aria-modal="true">
            <div className="strategy-manage-section-head">
              <div>
                <h3 className="strategy-manage-subtitle">策略语法说明书</h3>
                <p className="strategy-manage-note">
                  表达式支持多句，最后一句作为最终结果；常见字段可直接写 <code>C / O / H / L / V</code>。
                </p>
              </div>
              <button
                type="button"
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
                onClick={() => setIsSyntaxGuideOpen(false)}
              >
                关闭
              </button>
            </div>

            <section className="strategy-manage-guide-section">
              <h4>1. 赋值</h4>
              <p>用 <code>:=</code> 给中间变量命名，用 <code>;</code> 分隔多句。</p>
              <pre className="strategy-manage-text-block strategy-manage-text-block-code">{`N := 20;
BASE := MA(C, N);
VOL_OK := V > MA(V, 5);
C > BASE AND VOL_OK`}</pre>
            </section>

            <section className="strategy-manage-guide-section">
              <h4>2. 表达式</h4>
              <div className="strategy-manage-guide-chip-list">
                <span className="strategy-manage-guide-chip">算术：+ - * /</span>
                <span className="strategy-manage-guide-chip">比较：&gt; &gt;= &lt; &lt;= == !=</span>
                <span className="strategy-manage-guide-chip">逻辑：AND OR NOT</span>
                <span className="strategy-manage-guide-chip">分组：(...)</span>
              </div>
              <pre className="strategy-manage-text-block strategy-manage-text-block-code">{`C > O AND V > MA(V, 5)
NOT(CROSS(C, MA(C, 10)))
IF(C > O, C - O, 0)`}</pre>
            </section>

            <section className="strategy-manage-guide-section">
              <h4>3. 返回结果</h4>
              <p>最后一条语句建议返回布尔序列或数值序列。</p>
              <div className="strategy-manage-guide-result-grid">
                <div className="strategy-manage-rule-metric">
                  <span>布尔序列例子</span>
                  <strong>C &gt; MA(C, 20)</strong>
                </div>
                <div className="strategy-manage-rule-metric">
                  <span>数值序列例子</span>
                  <strong>COUNT(C &gt; O, 5)</strong>
                </div>
              </div>
            </section>

            <section className="strategy-manage-guide-section">
              <h4>4. 支持的函数</h4>
              <div className="strategy-manage-guide-table-wrap">
                <table className="strategy-manage-guide-table">
                  <thead>
                    <tr>
                      <th>函数</th>
                      <th>签名</th>
                      <th>返回</th>
                      <th>作用</th>
                      <th>输入输出例子</th>
                    </tr>
                  </thead>
                  <tbody>
                    {SYNTAX_GUIDE_FUNCTIONS.map((item) => (
                      <tr key={item.name}>
                        <td><code>{item.name}</code></td>
                        <td><code>{item.signature}</code></td>
                        <td>{item.returns}</td>
                        <td>{item.description}</td>
                        <td>{item.example}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            {SYNTAX_GUIDE_FIELD_SECTIONS.map((section) => (
              <section key={section.title} className="strategy-manage-guide-section">
                <h4>{section.title}</h4>
                <p>{section.note}</p>
                <div className="strategy-manage-guide-table-wrap">
                  <table className="strategy-manage-guide-table">
                    <thead>
                      <tr>
                        <th>字段</th>
                        <th>范围</th>
                        <th>作用</th>
                        <th>例子</th>
                      </tr>
                    </thead>
                    <tbody>
                      {section.fields.map((item) => (
                        <tr key={`${section.title}-${item.name}`}>
                          <td><code>{item.name}</code></td>
                          <td>{item.scope}</td>
                          <td>{item.description}</td>
                          <td>{item.example}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
            ))}
          </div>
        </div>
      ) : null}

      <StrategyAssetModal
        open={isAssetModalOpen}
        onClose={() => setIsAssetModalOpen(false)}
        onActivated={() => {
          void loadPage()
        }}
      />
    </div>
  )
}
