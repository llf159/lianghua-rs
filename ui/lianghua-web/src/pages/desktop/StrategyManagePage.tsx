import { useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import {
  checkStrategyManageRuleDraft,
  createStrategyManageRule,
  exportStrategyRuleFile,
  getStrategyManagePage,
  removeStrategyManageRules,
  updateStrategyManageRule,
  type StrategyManageDistPoint,
  type StrategyManageRuleDraft,
  type StrategyManageRuleItem,
} from '../../apis/strategyManage'
import { readStoredSourcePath } from '../../shared/storage'
import './css/StrategyManagePage.css'

const TAG_OPTIONS = ['Normal', 'Opportunity', 'Rare'] as const
const SCOPE_OPTIONS = ['LAST', 'ANY', 'EACH', 'RECENT', 'CONSEC'] as const
const TAG_FILTER_OPTIONS = ['ALL', ...TAG_OPTIONS] as const
const SCORE_METHOD_FILTER_OPTIONS = ['ALL', 'fixed', 'dist'] as const
const SORT_OPTIONS = [
  { value: 'index', label: '按原顺序' },
  { value: 'score', label: '按分值强度' },
  { value: 'scoreMethod', label: '按得分方式' },
  { value: 'tag', label: '按标签' },
  { value: 'scope', label: '按得分方法' },
  { value: 'name', label: '按名称' },
] as const

type EditorMode = 'create' | 'edit'
type BusyAction = 'idle' | 'checking' | 'creating' | 'saving' | 'deleting' | 'exporting'
type ScoreMode = 'fixed' | 'dist'
type SortMode = (typeof SORT_OPTIONS)[number]['value']
type ScopeMode = (typeof SCOPE_OPTIONS)[number]

type SyntaxGuideFunction = {
  name: string
  signature: string
  returns: string
  description: string
  example: string
}

const SYNTAX_GUIDE_FUNCTIONS: SyntaxGuideFunction[] = [
  {
    name: 'ABS',
    signature: 'ABS(x)',
    returns: '数值序列',
    description: '取绝对值。',
    example: '输入 [-2, 3] -> 输出 [2, 3]',
  },
  {
    name: 'MAX',
    signature: 'MAX(a, b)',
    returns: '数值序列',
    description: '逐项取较大值。',
    example: 'a=[1, 5], b=[2, 3] -> [2, 5]',
  },
  {
    name: 'MIN',
    signature: 'MIN(a, b)',
    returns: '数值序列',
    description: '逐项取较小值。',
    example: 'a=[1, 5], b=[2, 3] -> [1, 3]',
  },
  {
    name: 'DIV',
    signature: 'DIV(a, b)',
    returns: '数值序列',
    description: '安全除法，除数为 0 时返回 0。',
    example: 'a=[6, 5], b=[2, 0] -> [3, 0]',
  },
  {
    name: 'COUNT',
    signature: 'COUNT(cond, n)',
    returns: '数值序列',
    description: '统计最近 n 根里条件成立的次数。',
    example: 'cond=[真, 假, 真, 真], n=3 -> [1, 1, 2, 2]',
  },
  {
    name: 'MA',
    signature: 'MA(x, n)',
    returns: '数值序列',
    description: '简单移动平均。',
    example: 'x=[1, 2, 3, 4], n=3 -> [空, 空, 2, 3]',
  },
  {
    name: 'REF',
    signature: 'REF(x, n)',
    returns: '数值序列',
    description: '取 n 根之前的值。',
    example: 'x=[10, 11, 12, 13], n=2 -> [空, 空, 10, 11]',
  },
  {
    name: 'HHV',
    signature: 'HHV(x, n)',
    returns: '数值序列',
    description: '最近 n 根最高值。',
    example: 'x=[1, 3, 2, 5], n=3 -> [空, 空, 3, 5]',
  },
  {
    name: 'LLV',
    signature: 'LLV(x, n)',
    returns: '数值序列',
    description: '最近 n 根最低值。',
    example: 'x=[1, 3, 2, 0], n=3 -> [空, 空, 1, 0]',
  },
  {
    name: 'SUM',
    signature: 'SUM(x, n)',
    returns: '数值序列',
    description: '最近 n 根求和。',
    example: 'x=[1, 2, 3, 4], n=3 -> [空, 空, 6, 9]',
  },
  {
    name: 'STD',
    signature: 'STD(x, n)',
    returns: '数值序列',
    description: '最近 n 根标准差。',
    example: 'x=[1, 3, 3], n=2 -> [空, 1, 0]',
  },
  {
    name: 'IF',
    signature: 'IF(cond, a, b)',
    returns: '数值序列',
    description: '条件成立取 a，否则取 b。',
    example: 'cond=[真, 假, 真], a=[1, 1, 1], b=[0, 0, 0] -> [1, 0, 1]',
  },
  {
    name: 'CROSS',
    signature: 'CROSS(a, b)',
    returns: '布尔序列',
    description: 'a 当根上穿 b。',
    example: 'a=[1, 2, 4], b=[3, 2, 3] -> [假, 假, 真]',
  },
  {
    name: 'EMA',
    signature: 'EMA(x, n)',
    returns: '数值序列',
    description: '指数移动平均。',
    example: 'x=[1, 2, 3], n=3 -> [1, 1.5, 2.25]',
  },
  {
    name: 'SMA',
    signature: 'SMA(x, n, m)',
    returns: '数值序列',
    description: '平滑移动平均。',
    example: 'x=[3, 6, 9], n=3, m=1 -> [3, 4, 5.67]',
  },
  {
    name: 'BARSLAST',
    signature: 'BARSLAST(cond)',
    returns: '数值序列',
    description: '距离上一次条件成立过去了多少根。',
    example: 'cond=[假, 假, 真, 假, 假, 真] -> [空, 空, 0, 1, 2, 0]',
  },
  {
    name: 'RSV',
    signature: 'RSV(c, h, l, n)',
    returns: '数值序列',
    description: '按最近 n 根高低点计算 RSV。',
    example: 'c=[8, 9, 10], h=[10, 10, 10], l=[6, 7, 8], n=3 -> [空, 空, 100]',
  },
  {
    name: 'GRANK',
    signature: 'GRANK(x, n)',
    returns: '数值序列',
    description: '最近 n 根内，大值排前的名次，1 表示最大。',
    example: 'x=[5, 3, 4], n=3 -> [空, 空, 2]',
  },
  {
    name: 'LRANK',
    signature: 'LRANK(x, n)',
    returns: '数值序列',
    description: '最近 n 根内，小值排前的名次，1 表示最小。',
    example: 'x=[5, 3, 1], n=3 -> [空, 空, 1]',
  },
  {
    name: 'GTOPCOUNT',
    signature: 'GTOPCOUNT(value, cond, win, topn)',
    returns: '数值序列',
    description: '最近 win 根里，value 最大的前 topn 根中，cond 成立了几次。',
    example: 'value=[5, 1, 4], cond=[真, 假, 真], win=3, topn=2 -> [空, 空, 2]',
  },
  {
    name: 'LTOPCOUNT',
    signature: 'LTOPCOUNT(value, cond, win, topn)',
    returns: '数值序列',
    description: '最近 win 根里，value 最小的前 topn 根中，cond 成立了几次。',
    example: 'value=[5, 1, 4], cond=[真, 假, 真], win=3, topn=2 -> [空, 空, 1]',
  },
  {
    name: 'GET',
    signature: 'GET(cond, value, n)',
    returns: '数值序列',
    description: '向前回看最近 n 根，取最后一次 cond 成立时的 value。',
    example: 'cond=[假, 真, 假, 假], value=[10, 11, 12, 13], n=3 -> [空, 空, 11, 11]',
  },
]

function formatNumber(value: number, digits = 2) {
  if (!Number.isFinite(value)) {
    return '--'
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits)
}

function normalizeTag(tag: string) {
  return tag.trim().toLowerCase()
}

function hasDistPoints(items?: StrategyManageDistPoint[] | null) {
  return Boolean(items && items.length > 0)
}

function getRuleScoreMethod(rule: Pick<StrategyManageRuleItem, 'dist_points'>) {
  return hasDistPoints(rule.dist_points) ? 'dist' : 'fixed'
}

function buildDraftFromRule(rule: StrategyManageRuleItem): StrategyManageRuleDraft {
  return {
    name: rule.name,
    scope_way: rule.scope_way,
    scope_windows: rule.scope_windows,
    when: rule.when,
    points: rule.points,
    dist_points: rule.dist_points ?? null,
    explain: rule.explain,
    tag: rule.tag,
  }
}

function buildEmptyDraft(): StrategyManageRuleDraft {
  return {
    name: '',
    scope_way: 'LAST',
    scope_windows: 1,
    when: '',
    points: 0,
    dist_points: null,
    explain: '',
    tag: 'Normal',
  }
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

function parseFixedPointsText(raw: string) {
  const trimmed = raw.trim()
  if (!trimmed) {
    throw new Error('固定分值不能为空')
  }

  const parsed = Number(trimmed)
  if (!Number.isFinite(parsed)) {
    throw new Error('固定分值必须是合法数字')
  }
  return parsed
}

function buildScoreSpotlightData(rule: Pick<StrategyManageRuleItem, 'points' | 'dist_points'>) {
  if (!hasDistPoints(rule.dist_points)) {
    return {
      headline: `${formatNumber(rule.points)} 分`,
      strength: Math.abs(rule.points),
    }
  }

  const distPoints = rule.dist_points ?? []
  const pointValues = distPoints.map((item) => item.points)
  const maxPoint = Math.max(...pointValues)
  const minPoint = Math.min(...pointValues)
  return {
    headline: `${formatNumber(minPoint)} ~ ${formatNumber(maxPoint)} 分`,
    strength: Math.max(...pointValues.map((value) => Math.abs(value))),
  }
}

function buildDistPointsStats(items?: StrategyManageDistPoint[] | null) {
  if (!items || items.length === 0) {
    return null
  }

  const mins = items.map((item) => item.min)
  const maxs = items.map((item) => item.max)
  const points = items.map((item) => item.points)
  return {
    segments: items.length,
    coverMin: Math.min(...mins),
    coverMax: Math.max(...maxs),
    pointMin: Math.min(...points),
    pointMax: Math.max(...points),
  }
}

function formatSignedPoints(points: number) {
  const formatted = formatNumber(Math.abs(points))
  return `${points >= 0 ? '+' : '-'}${formatted}分`
}

function buildDistPointsVerbalText(items?: StrategyManageDistPoint[] | null) {
  if (!items || items.length === 0) {
    return '--'
  }

  return items
    .map((item) => `${item.min}~${item.max}天:${formatSignedPoints(item.points)}`)
    .join('\n')
}

function buildRulePreviewText(value: string) {
  return value.trim() === '' ? '--' : value
}

function isSingleLineText(value: string) {
  return !value.includes('\n')
}

function buildPreparedDraft(
  draft: StrategyManageRuleDraft,
  scoreMode: ScoreMode,
  distPointsText: string,
  fixedPointsText: string,
) {
  const parsedDistPoints = scoreMode === 'dist' ? parseDistPointsText(distPointsText) : null
  if (scoreMode === 'dist' && (!parsedDistPoints || parsedDistPoints.length === 0)) {
    throw new Error('当前选择的是字典分，至少需要填写一条字典得分')
  }
  const parsedFixedPoints = scoreMode === 'fixed' ? parseFixedPointsText(fixedPointsText) : draft.points

  return {
    ...draft,
    dist_points: parsedDistPoints,
    points: parsedFixedPoints,
  } satisfies StrategyManageRuleDraft
}

function parseScopeWayDraft(scopeWay: string): { mode: ScopeMode; consecThreshold: number } {
  const normalized = scopeWay.trim().toUpperCase()
  if (normalized.startsWith('CONSEC>=')) {
    const raw = Number(normalized.slice('CONSEC>='.length))
    return {
      mode: 'CONSEC',
      consecThreshold: Number.isInteger(raw) && raw > 0 ? raw : 2,
    }
  }

  if (SCOPE_OPTIONS.includes(normalized as ScopeMode)) {
    return { mode: normalized as ScopeMode, consecThreshold: 2 }
  }

  return { mode: 'LAST', consecThreshold: 2 }
}

function buildScopeWayValue(mode: ScopeMode, consecThreshold: number) {
  if (mode === 'CONSEC') {
    const normalized = Math.max(1, Math.floor(consecThreshold || 1))
    return `CONSEC>=${normalized}`
  }
  return mode
}

function normalizePositiveIntegerInput(raw: string, fallback: number) {
  const trimmed = raw.trim()
  if (!trimmed) {
    return Math.max(1, Math.floor(fallback || 1))
  }

  const parsed = Number(trimmed)
  if (!Number.isFinite(parsed)) {
    return Math.max(1, Math.floor(fallback || 1))
  }

  return Math.max(1, Math.floor(parsed))
}

function getScopeFilterValue(scopeWay: string) {
  const normalized = scopeWay.trim().toUpperCase()
  return normalized.startsWith('CONSEC>=') ? 'CONSEC' : normalized
}

function getSortScopeRank(scopeWay: string) {
  const normalized = scopeWay.trim().toUpperCase()
  if (normalized.startsWith('CONSEC>=')) {
    const raw = Number(normalized.slice('CONSEC>='.length))
    const threshold = Number.isInteger(raw) && raw > 0 ? raw : 999
    return 100 + threshold
  }

  const fixedOrder = ['LAST', 'ANY', 'EACH', 'RECENT']
  const index = fixedOrder.findIndex((item) => item === normalized)
  return index >= 0 ? index : 999
}

export default function StrategyManagePage() {
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath())
  const [rules, setRules] = useState<StrategyManageRuleItem[]>([])
  const [editorMode, setEditorMode] = useState<EditorMode | null>(null)
  const [editingOriginalName, setEditingOriginalName] = useState<string | null>(null)
  const [draft, setDraft] = useState<StrategyManageRuleDraft | null>(null)
  const [distPointsText, setDistPointsText] = useState('')
  const [fixedPointsText, setFixedPointsText] = useState('')
  const [scopeWindowsInput, setScopeWindowsInput] = useState('1')
  const [consecThresholdInput, setConsecThresholdInput] = useState('2')
  const [scoreMode, setScoreMode] = useState<ScoreMode>('fixed')
  const [deleteTarget, setDeleteTarget] = useState<StrategyManageRuleItem | null>(null)
  const [loading, setLoading] = useState(true)
  const [busyAction, setBusyAction] = useState<BusyAction>('idle')
  const [pageError, setPageError] = useState('')
  const [editorError, setEditorError] = useState('')
  const [notice, setNotice] = useState('')
  const [checkNotice, setCheckNotice] = useState('')
  const [searchKeyword, setSearchKeyword] = useState('')
  const [tagFilter, setTagFilter] = useState<(typeof TAG_FILTER_OPTIONS)[number]>('ALL')
  const [scoreMethodFilter, setScoreMethodFilter] = useState<(typeof SCORE_METHOD_FILTER_OPTIONS)[number]>('ALL')
  const [scopeFilter, setScopeFilter] = useState('ALL')
  const [sortMode, setSortMode] = useState<SortMode>('index')
  const [isSyntaxGuideOpen, setIsSyntaxGuideOpen] = useState(false)

  const sourcePathTrimmed = sourcePath.trim()

  const opportunityCount = useMemo(
    () => rules.filter((item) => normalizeTag(item.tag) === 'opportunity').length,
    [rules],
  )
  const rareCount = useMemo(
    () => rules.filter((item) => normalizeTag(item.tag) === 'rare').length,
    [rules],
  )
  const fixedScoreCount = useMemo(
    () => rules.filter((item) => getRuleScoreMethod(item) === 'fixed').length,
    [rules],
  )
  const distScoreCount = useMemo(
    () => rules.filter((item) => getRuleScoreMethod(item) === 'dist').length,
    [rules],
  )
  const scopeFilterOptions = useMemo(() => {
    const items = Array.from(new Set(rules.map((item) => getScopeFilterValue(item.scope_way))))
    return ['ALL', ...items.sort((left, right) => getSortScopeRank(left) - getSortScopeRank(right) || left.localeCompare(right))]
  }, [rules])

  async function loadPage() {
    setLoading(true)
    setPageError('')
    try {
      const resolvedSourcePath = await ensureManagedSourcePath()
      const data = await getStrategyManagePage(resolvedSourcePath)
      setSourcePath(resolvedSourcePath)
      setRules(data.rules ?? [])
    } catch (loadError) {
      setRules([])
      setPageError(`读取策略规则失败: ${String(loadError)}`)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    let cancelled = false

    const runLoad = async () => {
      try {
        const resolvedSourcePath = await ensureManagedSourcePath()
        const data = await getStrategyManagePage(resolvedSourcePath)
        if (!cancelled) {
          setLoading(false)
          setPageError('')
          setSourcePath(resolvedSourcePath)
          setRules(data.rules ?? [])
        }
      } catch (loadError) {
        if (!cancelled) {
          setLoading(false)
          setRules([])
          setPageError(`读取策略规则失败: ${String(loadError)}`)
        }
      }
    }

    setLoading(true)
    void runLoad()
    const onFocus = () => {
      void runLoad()
    }
    window.addEventListener('focus', onFocus)
    return () => {
      cancelled = true
      window.removeEventListener('focus', onFocus)
    }
  }, [])

  const filteredRules = useMemo(() => {
    const keyword = searchKeyword.trim().toLowerCase()
    const nextRules = rules.filter((rule) => {
      if (tagFilter !== 'ALL' && rule.tag !== tagFilter) {
        return false
      }
      if (scoreMethodFilter !== 'ALL' && getRuleScoreMethod(rule) !== scoreMethodFilter) {
        return false
      }
      if (scopeFilter !== 'ALL' && getScopeFilterValue(rule.scope_way) !== scopeFilter) {
        return false
      }
      if (!keyword) {
        return true
      }

      const searchPool = [rule.name, rule.explain, rule.when, rule.tag, rule.scope_way]
        .join('\n')
        .toLowerCase()
      return searchPool.includes(keyword)
    })

    return nextRules.sort((left, right) => {
      switch (sortMode) {
        case 'score':
          return buildScoreSpotlightData(right).strength - buildScoreSpotlightData(left).strength
        case 'scoreMethod':
          return getRuleScoreMethod(left).localeCompare(getRuleScoreMethod(right))
        case 'tag':
          return normalizeTag(left.tag).localeCompare(normalizeTag(right.tag)) || left.index - right.index
        case 'scope':
          return getSortScopeRank(left.scope_way) - getSortScopeRank(right.scope_way) || left.index - right.index
        case 'name':
          return left.name.localeCompare(right.name)
        case 'index':
        default:
          return left.index - right.index
      }
    })
  }, [rules, scoreMethodFilter, scopeFilter, searchKeyword, sortMode, tagFilter])

  const editorDistPreview = useMemo(() => {
    if (scoreMode !== 'dist') {
      return { error: '', items: null as StrategyManageDistPoint[] | null }
    }
    try {
      return { error: '', items: parseDistPointsText(distPointsText) }
    } catch (previewError) {
      return { error: String(previewError), items: null as StrategyManageDistPoint[] | null }
    }
  }, [distPointsText, scoreMode])
  const draftScopeState = useMemo(
    () => parseScopeWayDraft(draft?.scope_way ?? 'LAST'),
    [draft?.scope_way],
  )

  function clearEditor() {
    setEditorMode(null)
    setEditingOriginalName(null)
    setDraft(null)
    setDistPointsText('')
    setFixedPointsText('')
    setScopeWindowsInput('1')
    setConsecThresholdInput('2')
    setScoreMode('fixed')
    setCheckNotice('')
    setEditorError('')
  }

  function openCreateEditor() {
    setEditorMode('create')
    setEditingOriginalName(null)
    setDraft(buildEmptyDraft())
    setDistPointsText('')
    setFixedPointsText('')
    setScopeWindowsInput('1')
    setConsecThresholdInput('2')
    setScoreMode('fixed')
    setCheckNotice('')
    setEditorError('')
    setNotice('')
    setPageError('')
  }

  function openEditEditor(rule: StrategyManageRuleItem) {
    setEditorMode('edit')
    setEditingOriginalName(rule.name)
    setDraft(buildDraftFromRule(rule))
    setDistPointsText(distPointsToText(rule.dist_points))
    setFixedPointsText(String(rule.points))
    setScopeWindowsInput(String(rule.scope_windows))
    setConsecThresholdInput(String(parseScopeWayDraft(rule.scope_way).consecThreshold))
    setScoreMode(hasDistPoints(rule.dist_points) ? 'dist' : 'fixed')
    setCheckNotice('')
    setEditorError('')
    setNotice('')
    setPageError('')
  }

  async function runAction(
    action: Exclude<BusyAction, 'idle'>,
    errorPrefix: string,
    setErrorTarget: (message: string) => void,
    runner: () => Promise<void>,
  ) {
    setBusyAction(action)
    setErrorTarget('')
    try {
      await runner()
    } catch (actionError) {
      setErrorTarget(`${errorPrefix}: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  function getPreparedDraft() {
    if (!draft) {
      throw new Error('当前没有可操作的草稿')
    }
    return buildPreparedDraft(draft, scoreMode, distPointsText, fixedPointsText)
  }

  async function onCheckDraft() {
    if (!sourcePathTrimmed) {
      setEditorError('当前数据目录为空，无法检查策略草稿。')
      return
    }

    try {
      const preparedDraft = getPreparedDraft()
      await runAction('checking', '检查策略失败', setEditorError, async () => {
        const message = await checkStrategyManageRuleDraft(
          sourcePathTrimmed,
          preparedDraft,
          editorMode === 'edit' ? editingOriginalName ?? undefined : undefined,
        )
        setCheckNotice(message)
        setNotice('')
      })
    } catch (actionError) {
      setEditorError(`检查策略失败: ${String(actionError)}`)
    }
  }

  async function onSaveDraft() {
    if (!sourcePathTrimmed) {
      setEditorError(editorMode === 'create' ? '当前数据目录为空，无法新增策略。' : '当前数据目录为空，无法保存策略。')
      return
    }

    let preparedDraft: StrategyManageRuleDraft
    try {
      preparedDraft = getPreparedDraft()
    } catch (actionError) {
      setEditorError(String(actionError))
      return
    }
    if (editorMode === 'create') {
      await runAction('creating', '新增策略失败', setEditorError, async () => {
        const data = await createStrategyManageRule(sourcePathTrimmed, preparedDraft)
        setRules(data.rules ?? [])
        setNotice(`已新增策略：${preparedDraft.name.trim()}`)
        clearEditor()
      })
      return
    }

    if (!editingOriginalName) {
      setEditorError('缺少待修改策略名')
      return
    }

    await runAction('saving', '保存策略失败', setEditorError, async () => {
      const data = await updateStrategyManageRule(sourcePathTrimmed, editingOriginalName, preparedDraft)
      setRules(data.rules ?? [])
      setNotice(`已保存策略：${preparedDraft.name.trim()}`)
      clearEditor()
    })
  }

  async function onConfirmDelete() {
    if (!deleteTarget || !sourcePathTrimmed) {
      return
    }

    await runAction('deleting', '删除策略失败', setPageError, async () => {
      const data = await removeStrategyManageRules(sourcePathTrimmed, [deleteTarget.name])
      setRules(data.rules ?? [])
      setNotice(`已删除策略：${deleteTarget.name}`)
      if (editingOriginalName === deleteTarget.name) {
        clearEditor()
      }
      setDeleteTarget(null)
    })
  }

  async function onExportRuleFile() {
    if (!sourcePathTrimmed) {
      setPageError('当前数据目录为空，无法导出策略文件。')
      return
    }

    await runAction('exporting', '导出策略文件失败', setPageError, async () => {
      const exportedPath = await exportStrategyRuleFile(sourcePathTrimmed)
      if (!exportedPath) {
        return
      }
      setNotice(`已导出策略文件到 ${exportedPath}`)
    })
  }

  const filteredCount = filteredRules.length
  const isEditing = editorMode !== null && draft !== null

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
          </div>
          <span className="strategy-manage-tip">当前共 {rules.length} 条策略</span>
        </div>

        <div className="strategy-manage-source-note">
          当前数据目录：<strong>{sourcePathTrimmed || '--'}</strong>
        </div>

        <div className="strategy-manage-toolbar">
          <div className="strategy-manage-toolbar-left">
            <button
              type="button"
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-primary"
              onClick={openCreateEditor}
              disabled={busyAction !== 'idle'}
            >
              新增策略
            </button>
            <button
              type="button"
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
              onClick={() => setIsSyntaxGuideOpen(true)}
            >
              语法说明书
            </button>
            {isEditing ? <span className="strategy-manage-tip">当前有未提交草稿</span> : null}
          </div>
          <div className="strategy-manage-toolbar-right">
            <button
              type="button"
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
              onClick={() => void loadPage()}
              disabled={busyAction !== 'idle'}
            >
              刷新列表
            </button>
            <button
              type="button"
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
              onClick={() => void onExportRuleFile()}
              disabled={busyAction !== 'idle'}
            >
              {busyAction === 'exporting' ? '导出中...' : '导出策略文件'}
            </button>
          </div>
        </div>

        <div className="strategy-manage-summary">
          <div className="strategy-manage-summary-item">
            <span>策略总数</span>
            <strong>{rules.length}</strong>
          </div>
          <div className="strategy-manage-summary-item">
            <span>当前筛选后</span>
            <strong>{filteredCount}</strong>
          </div>
          <div className="strategy-manage-summary-item">
            <span>固定分策略</span>
            <strong>{fixedScoreCount}</strong>
          </div>
          <div className="strategy-manage-summary-item">
            <span>字典分策略</span>
            <strong>{distScoreCount}</strong>
          </div>
          <div className="strategy-manage-summary-item">
            <span>机会类策略</span>
            <strong>{opportunityCount}</strong>
          </div>
          <div className="strategy-manage-summary-item">
            <span>稀有类策略</span>
            <strong>{rareCount}</strong>
          </div>
        </div>

        <div className="strategy-manage-filter-grid">
          <label className="strategy-manage-field">
            <span>搜索</span>
            <input
              value={searchKeyword}
              onChange={(event) => setSearchKeyword(event.target.value)}
              placeholder="按策略名 / 说明 / 表达式搜索"
            />
          </label>

          <label className="strategy-manage-field">
            <span>标签</span>
            <select value={tagFilter} onChange={(event) => setTagFilter(event.target.value as (typeof TAG_FILTER_OPTIONS)[number])}>
              {TAG_FILTER_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  {item === 'ALL' ? '全部' : item}
                </option>
              ))}
            </select>
          </label>

          <label className="strategy-manage-field">
            <span>得分方式</span>
            <select
              value={scoreMethodFilter}
              onChange={(event) =>
                setScoreMethodFilter(event.target.value as (typeof SCORE_METHOD_FILTER_OPTIONS)[number])
              }
            >
              <option value="ALL">全部</option>
              <option value="fixed">固定分</option>
              <option value="dist">字典分</option>
            </select>
          </label>

          <label className="strategy-manage-field">
            <span>得分方法</span>
            <select
              value={scopeFilter}
                  onChange={(event) => setScopeFilter(event.target.value)}
                >
                  {scopeFilterOptions.map((item) => (
                    <option key={item} value={item}>
                      {item === 'ALL' ? '全部' : item === 'CONSEC' ? 'CONSEC>=' : item}
                    </option>
                  ))}
                </select>
          </label>

          <label className="strategy-manage-field">
            <span>排序</span>
            <select value={sortMode} onChange={(event) => setSortMode(event.target.value as SortMode)}>
              {SORT_OPTIONS.map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
        </div>

        {pageError ? <div className="strategy-manage-message strategy-manage-message-error">{pageError}</div> : null}
        {notice ? <div className="strategy-manage-message strategy-manage-message-notice">{notice}</div> : null}
      </section>

      <section className="strategy-manage-card">
        <div className="strategy-manage-list-head">
          <h3 className="strategy-manage-subtitle">策略列表</h3>
          <span>{loading ? '读取中...' : `${filteredCount} 条`}</span>
        </div>

        {loading ? (
          <div className="strategy-manage-empty">读取策略规则中...</div>
        ) : filteredRules.length === 0 ? (
          <div className="strategy-manage-empty">当前筛选条件下没有策略。</div>
        ) : (
          <div className="strategy-manage-list">
            {filteredRules.map((rule) => {
              const scoreData = buildScoreSpotlightData(rule)
              const distStats = buildDistPointsStats(rule.dist_points)
              return (
                <article key={rule.name} className="strategy-manage-rule-card">
                  <div className="strategy-manage-rule-card-head">
                    <div className="strategy-manage-rule-card-name">{rule.name}</div>
                    <div className="strategy-manage-rule-card-actions">
                      <button
                        type="button"
                        className="strategy-manage-inline-btn"
                        onClick={(event) => {
                          event.stopPropagation()
                          openEditEditor(rule)
                        }}
                      >
                        修改
                      </button>
                      <button
                        type="button"
                        className="strategy-manage-inline-btn is-danger"
                        onClick={(event) => {
                          event.stopPropagation()
                          setDeleteTarget(rule)
                        }}
                      >
                        删除
                      </button>
                    </div>
                  </div>

                  <div className="strategy-manage-rule-metrics">
                    <div className="strategy-manage-rule-metric is-score">
                      <span>得分</span>
                      <strong>{scoreData.headline}</strong>
                    </div>
                    <div className="strategy-manage-rule-metric">
                      <span>判定方式</span>
                      <strong>{rule.scope_way}</strong>
                    </div>
                    <div className="strategy-manage-rule-metric">
                      <span>周期</span>
                      <strong>{rule.scope_windows}</strong>
                    </div>
                    <div className="strategy-manage-rule-metric">
                      <span>标签</span>
                      <strong>{rule.tag}</strong>
                    </div>
                  </div>

                  <div className="strategy-manage-detail-grid">
                    <section className="strategy-manage-detail-section">
                      <h4>表达式</h4>
                      <pre
                        className={
                          isSingleLineText(rule.when)
                            ? 'strategy-manage-text-block strategy-manage-text-block-code is-single-line'
                            : 'strategy-manage-text-block strategy-manage-text-block-code'
                        }
                      >
                        {buildRulePreviewText(rule.when)}
                      </pre>
                    </section>

                    <section className="strategy-manage-detail-section">
                      <h4>说明</h4>
                      <pre
                        className={
                          isSingleLineText(rule.explain)
                            ? 'strategy-manage-text-block is-single-line'
                            : 'strategy-manage-text-block'
                        }
                      >
                        {buildRulePreviewText(rule.explain)}
                      </pre>
                    </section>

                    {distStats ? (
                      <section className="strategy-manage-detail-section">
                        <h4>字典区间</h4>
                        <pre className="strategy-manage-text-block strategy-manage-text-block-code">
                          {buildDistPointsVerbalText(rule.dist_points)}
                        </pre>
                      </section>
                    ) : null}
                  </div>
                </article>
              )
            })}
          </div>
        )}
      </section>

      {deleteTarget ? (
        <div className="strategy-manage-modal-backdrop" role="presentation">
          <div className="strategy-manage-modal" role="dialog" aria-modal="true" aria-labelledby="strategy-manage-delete-title">
            <h3 id="strategy-manage-delete-title">确认删除策略</h3>
            <p>即将删除策略：<strong>{deleteTarget.name}</strong></p>
            <p>这是第二次确认。确认后会直接写回 `score_rule.toml`。</p>
            <div className="strategy-manage-modal-actions">
              <button
                type="button"
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
                onClick={() => setDeleteTarget(null)}
                disabled={busyAction !== 'idle'}
              >
                取消
              </button>
              <button
                type="button"
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-danger"
                onClick={() => void onConfirmDelete()}
                disabled={busyAction !== 'idle'}
              >
                {busyAction === 'deleting' ? '删除中...' : '确认删除'}
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
          <div
            className="strategy-manage-modal strategy-manage-guide-modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="strategy-manage-guide-title"
          >
            <div className="strategy-manage-section-head">
              <div>
                <h3 className="strategy-manage-subtitle" id="strategy-manage-guide-title">
                  策略语法说明书
                </h3>
                <p className="strategy-manage-note">
                  表达式支持多句，最后一句会作为策略最终结果；常见字段可直接写
                  <code> C / O / H / L / V </code>
                  ，函数名不区分大小写。
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
              <p>
                最后一条语句建议返回布尔序列或数值序列。布尔结果适合做“是否命中”，数值结果适合做进一步比较或计数。
              </p>
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
                        <td>
                          <code>{item.name}</code>
                        </td>
                        <td>
                          <code>{item.signature}</code>
                        </td>
                        <td>{item.returns}</td>
                        <td>{item.description}</td>
                        <td>{item.example}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </div>
        </div>
      ) : null}

      {draft && editorMode ? (
        <div className="strategy-manage-modal-backdrop" role="presentation">
          <div className="strategy-manage-modal strategy-manage-editor-modal" role="dialog" aria-modal="true">
            <div className="strategy-manage-section-head">
              <div>
                <h3 className="strategy-manage-subtitle">
                  {editorMode === 'create' ? '新增策略' : `修改策略 · ${editingOriginalName ?? '--'}`}
                </h3>
                <p className="strategy-manage-note">
                  这是独立编辑窗口。先完整填写草稿，再检查，再保存；修改和新增共用同一套编辑体验。
                </p>
              </div>
              <button
                type="button"
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
                onClick={clearEditor}
                disabled={busyAction !== 'idle'}
              >
                关闭
              </button>
            </div>

            {editorError ? <div className="strategy-manage-message strategy-manage-message-error">{editorError}</div> : null}
            {checkNotice ? <div className="strategy-manage-message strategy-manage-message-notice">{checkNotice}</div> : null}

            <div className="strategy-manage-editor-grid">
              <label className="strategy-manage-field">
                <span>策略名</span>
                <input
                  value={draft.name}
                  onChange={(event) => setDraft((current) => (current ? { ...current, name: event.target.value } : current))}
                />
              </label>

              <label className="strategy-manage-field">
                <span>标签</span>
                <select
                  value={draft.tag}
                  onChange={(event) =>
                    setDraft((current) => (current ? { ...current, tag: event.target.value } : current))
                  }
                >
                  {TAG_OPTIONS.map((item) => (
                    <option key={item} value={item}>
                      {item}
                    </option>
                  ))}
                </select>
              </label>

              <label className="strategy-manage-field">
                <span>判定方式</span>
                <select
                  value={draftScopeState.mode}
                  onChange={(event) =>
                    setDraft((current) =>
                      current
                        ? {
                            ...current,
                            scope_way: buildScopeWayValue(event.target.value as ScopeMode, draftScopeState.consecThreshold),
                          }
                        : current,
                    )
                  }
                >
                  {SCOPE_OPTIONS.map((item) => (
                    <option key={item} value={item}>
                      {item === 'CONSEC' ? 'CONSEC>=' : item}
                    </option>
                  ))}
                </select>
              </label>

              {draftScopeState.mode === 'CONSEC' ? (
                <label className="strategy-manage-field">
                  <span>连续阈值</span>
                  <input
                    type="number"
                    min={1}
                    step={1}
                    value={consecThresholdInput}
                    onChange={(event) =>
                      {
                        const raw = event.target.value
                        setConsecThresholdInput(raw)
                        if (!raw.trim()) {
                          return
                        }
                        setDraft((current) =>
                          current
                            ? {
                                ...current,
                                scope_way: buildScopeWayValue(
                                  'CONSEC',
                                  normalizePositiveIntegerInput(raw, draftScopeState.consecThreshold),
                                ),
                              }
                            : current,
                        )
                      }
                    }
                    onBlur={() => {
                      const normalized = normalizePositiveIntegerInput(
                        consecThresholdInput,
                        draftScopeState.consecThreshold,
                      )
                      setConsecThresholdInput(String(normalized))
                      setDraft((current) =>
                        current
                          ? {
                              ...current,
                              scope_way: buildScopeWayValue('CONSEC', normalized),
                            }
                          : current,
                      )
                    }}
                  />
                </label>
              ) : null}

              <label className="strategy-manage-field">
                <span>周期</span>
                <input
                  type="number"
                  min={1}
                  step={1}
                  value={scopeWindowsInput}
                  onChange={(event) => {
                    const raw = event.target.value
                    setScopeWindowsInput(raw)
                    if (!raw.trim()) {
                      return
                    }
                    setDraft((current) =>
                      current
                        ? {
                            ...current,
                            scope_windows: normalizePositiveIntegerInput(raw, current.scope_windows),
                          }
                        : current,
                    )
                  }}
                  onBlur={() => {
                    if (!draft) {
                      return
                    }
                    const normalized = normalizePositiveIntegerInput(scopeWindowsInput, draft.scope_windows)
                    setScopeWindowsInput(String(normalized))
                    setDraft((current) =>
                      current ? { ...current, scope_windows: normalized } : current,
                    )
                  }}
                />
              </label>

              <label className="strategy-manage-field strategy-manage-field-span-full">
                <span>说明</span>
                <textarea
                  rows={2}
                  wrap="off"
                  value={draft.explain}
                  onChange={(event) =>
                    setDraft((current) => (current ? { ...current, explain: event.target.value } : current))
                  }
                />
              </label>

              <div className="strategy-manage-field strategy-manage-field-span-full">
                <span>分值模式</span>
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
                    字典分
                  </button>
                </div>
              </div>

              {scoreMode === 'fixed' ? (
                <label className="strategy-manage-field strategy-manage-field-span-full">
                  <span>固定分值</span>
                  <input
                    type="number"
                    step="0.1"
                    value={fixedPointsText}
                    onChange={(event) => setFixedPointsText(event.target.value)}
                  />
                </label>
              ) : (
                <div className="strategy-manage-field strategy-manage-field-span-full">
                  <span>字典分值</span>
                  <textarea
                    rows={6}
                    wrap="off"
                    value={distPointsText}
                    onChange={(event) => setDistPointsText(event.target.value)}
                    placeholder="每行一条：min,max,points"
                  />
                  {editorDistPreview.error ? (
                    <div className="strategy-manage-message strategy-manage-message-error">{editorDistPreview.error}</div>
                  ) : editorDistPreview.items && editorDistPreview.items.length > 0 ? (
                    <div className="strategy-manage-dist-preview">
                      <div className="strategy-manage-rule-metrics strategy-manage-rule-metrics-compact">
                        <div className="strategy-manage-rule-metric">
                          <span>分段数</span>
                          <strong>{buildDistPointsStats(editorDistPreview.items)?.segments}</strong>
                        </div>
                        <div className="strategy-manage-rule-metric">
                          <span>覆盖区间</span>
                          <strong>
                            {buildDistPointsStats(editorDistPreview.items)?.coverMin} ~{' '}
                            {buildDistPointsStats(editorDistPreview.items)?.coverMax}
                          </strong>
                        </div>
                        <div className="strategy-manage-rule-metric">
                          <span>分值范围</span>
                          <strong>
                            {formatNumber(buildDistPointsStats(editorDistPreview.items)?.pointMin ?? 0)} ~{' '}
                            {formatNumber(buildDistPointsStats(editorDistPreview.items)?.pointMax ?? 0)}
                          </strong>
                        </div>
                      </div>
                      <pre className="strategy-manage-text-block strategy-manage-text-block-code">
                        {buildDistPointsVerbalText(editorDistPreview.items)}
                      </pre>
                    </div>
                  ) : null}
                </div>
              )}

              <label className="strategy-manage-field strategy-manage-field-span-full">
                <span>表达式</span>
                <textarea
                  rows={9}
                  wrap="off"
                  spellCheck={false}
                  value={draft.when}
                  onChange={(event) =>
                    setDraft((current) => (current ? { ...current, when: event.target.value } : current))
                  }
                />
              </label>
            </div>

            <div className="strategy-manage-editor-actions">
              <button
                type="button"
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
                onClick={clearEditor}
                disabled={busyAction !== 'idle'}
              >
                取消
              </button>
              <button
                type="button"
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
                onClick={() => void onCheckDraft()}
                disabled={busyAction !== 'idle'}
              >
                {busyAction === 'checking' ? '检查中...' : '检查草稿'}
              </button>
              <button
                type="button"
                className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-primary"
                onClick={() => void onSaveDraft()}
                disabled={busyAction !== 'idle'}
              >
                {busyAction === 'creating'
                  ? '新增中...'
                  : busyAction === 'saving'
                    ? '保存中...'
                    : editorMode === 'create'
                      ? '检查后保存新增'
                      : '检查后保存修改'}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}
