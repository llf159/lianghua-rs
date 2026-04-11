import { useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import {
  checkStrategyManageSceneDraft,
  checkStrategyManageRuleDraft,
  createStrategyManageScene,
  createStrategyManageRule,
  getStrategyManagePage,
  removeStrategyManageRules,
  updateStrategyManageScene,
  updateStrategyManageRule,
  type StrategyManageDistPoint,
  type StrategyManagePageData,
  type StrategyManageRuleDraft,
  type StrategyManageRuleItem,
  type StrategyManageSceneDraft,
  type StrategyManageSceneItem,
} from '../../apis/strategyManage'
import './css/StrategyManagePage.css'

const SCOPE_OPTIONS = ['LAST', 'ANY', 'EACH', 'RECENT', 'CONSEC>=2'] as const
const STAGE_OPTIONS = ['base', 'trigger', 'confirm', 'risk', 'fail'] as const

type SyntaxGuideFunction = {
  name: string
  signature: string
  returns: string
  description: string
  example: string
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
]

type BusyAction = 'idle' | 'loading' | 'saving' | 'deleting'
type EditorMode = 'create' | 'edit'
type SceneEditorMode = 'create' | 'edit'

function formatNumber(value: number, digits = 2) {
  if (!Number.isFinite(value)) {
    return '--'
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits)
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
    scene_points: 1,
    dist_points: null,
    explain: '',
  }
}

function buildEmptySceneDraft(): StrategyManageSceneDraft {
  return {
    name: '',
    observe_threshold: 1,
    trigger_threshold: 2,
    confirm_threshold: 3,
    fail_threshold: 1,
    evidence_score: 1,
  }
}

function buildSceneDraftFromScene(scene: StrategyManageSceneItem): StrategyManageSceneDraft {
  return {
    name: scene.name,
    observe_threshold: scene.observe_threshold,
    trigger_threshold: scene.trigger_threshold,
    confirm_threshold: scene.confirm_threshold,
    fail_threshold: scene.fail_threshold,
    evidence_score: scene.evidence_score,
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
    scene_points: rule.scene_points,
    dist_points: rule.dist_points ?? null,
    explain: rule.explain,
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

export default function StrategyManagePage() {
  const [sourcePath, setSourcePath] = useState('')
  const [scenes, setScenes] = useState<StrategyManageSceneItem[]>([])
  const [rules, setRules] = useState<StrategyManageRuleItem[]>([])
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
  const [scoreMode, setScoreMode] = useState<'fixed' | 'dist'>('fixed')
  const [fixedPointsText, setFixedPointsText] = useState('0')
  const [distPointsText, setDistPointsText] = useState('')
  const [isSyntaxGuideOpen, setIsSyntaxGuideOpen] = useState(false)

  const selectedScene = useMemo(
    () => scenes.find((item) => item.name === selectedSceneName) ?? null,
    [scenes, selectedSceneName],
  )
  const selectedSceneRules = useMemo(
    () => rules.filter((item) => item.scene_name === selectedSceneName),
    [rules, selectedSceneName],
  )

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
    setFixedPointsText('0')
    setDistPointsText('')
    setEditorError('')
    setCheckNotice('')
    setError('')
    setNotice('')
  }

  function openCreateSceneEditor() {
    setSceneEditorMode('create')
    setEditingSceneOriginalName('')
    setSceneDraft(buildEmptySceneDraft())
    setSceneEditorError('')
    setError('')
    setNotice('')
  }

  function openEditSceneEditor(scene: StrategyManageSceneItem) {
    setSceneEditorMode('edit')
    setEditingSceneOriginalName(scene.name)
    setSceneDraft(buildSceneDraftFromScene(scene))
    setSceneEditorError('')
    setError('')
    setNotice('')
  }

  function openEditEditor(rule: StrategyManageRuleItem) {
    const nextDraft = buildDraftFromRule(rule)
    setEditorMode('edit')
    setEditingOriginalName(rule.name)
    setDraft(nextDraft)
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
      preparedDraft = buildPreparedDraft(draft, scoreMode, fixedPointsText, distPointsText)
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
      const preparedDraft = buildPreparedDraft(draft, scoreMode, fixedPointsText, distPointsText)
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

    try {
      const message = await checkStrategyManageSceneDraft(
        sourcePath,
        sceneDraft,
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
          ? await createStrategyManageScene(sourcePath, sceneDraft)
          : await updateStrategyManageScene(sourcePath, editingSceneOriginalName, sceneDraft)
      applyPageData(data, sceneDraft.name)
      setSceneDraft(null)
      setEditingSceneOriginalName('')
      setNotice(sceneEditorMode === 'create' ? 'scene 已创建。' : 'scene 已更新。')
    } catch (saveError) {
      setSceneEditorError(`保存 scene 失败: ${String(saveError)}`)
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
          <span className="strategy-manage-tip">当前共 {rules.length} 条 rule</span>
        </div>

        <div className="strategy-manage-toolbar">
          <div className="strategy-manage-toolbar-left">
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

        {notice ? <div className="strategy-manage-message strategy-manage-message-notice">{notice}</div> : null}
        {error ? <div className="strategy-manage-message strategy-manage-message-error">{error}</div> : null}
      </section>

      <section className="strategy-manage-card">
        <div className="strategy-manage-list-head">
          <strong>Scene 总览</strong>
          <span>点击 scene 打开浮窗</span>
        </div>
        {scenes.length === 0 ? (
          <div className="strategy-manage-empty">当前规则文件里没有 scene。</div>
        ) : (
          <div className="strategy-manage-scene-grid">
            {scenes.map((scene) => {
              const sceneRules = rules.filter((item) => item.scene_name === scene.name)
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
                      <span>{scene.rule_count} 条规则</span>
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
                    </div>
                  </div>
                  <div className="strategy-manage-scene-metrics">
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
        <div className="strategy-manage-modal-backdrop" role="presentation">
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
              <span>observe {formatNumber(selectedScene.observe_threshold)}</span>
              <span>trigger {formatNumber(selectedScene.trigger_threshold)}</span>
              <span>confirm {formatNumber(selectedScene.confirm_threshold)}</span>
              <span>fail {formatNumber(selectedScene.fail_threshold)}</span>
              <span>evidence {formatNumber(selectedScene.evidence_score)}</span>
            </div>

            <div className="strategy-manage-list-panel strategy-manage-list-panel-full">
              <div className="strategy-manage-list-head">
                <strong>Rule 列表</strong>
                <span>{selectedSceneRules.length} 条</span>
              </div>
              {selectedSceneRules.length === 0 ? (
                <div className="strategy-manage-empty">当前 scene 下还没有规则。</div>
              ) : (
                <div className="strategy-manage-scene-rule-list">
                  {selectedSceneRules.map((rule) => (
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
                        <div className="strategy-manage-summary-item">
                          <span>得分</span>
                          <strong>{hasDistPoints(rule.dist_points) ? '区间字典' : formatNumber(rule.points)}</strong>
                        </div>
                        <div className="strategy-manage-summary-item">
                          <span>Scene 分</span>
                          <strong>{formatNumber(rule.scene_points)}</strong>
                        </div>
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
                  ))}
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
                  value={draft.scope_windows}
                  onChange={(event) =>
                    setDraft({
                      ...draft,
                      scope_windows: Math.max(1, Number(event.target.value) || 1),
                    })
                  }
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
              <label className="strategy-manage-field strategy-manage-field-span-full">
                <span>Scene 分</span>
                <input
                  type="number"
                  step="0.1"
                  value={draft.scene_points}
                  onChange={(event) => setDraft({ ...draft, scene_points: Number(event.target.value) || 0 })}
                />
              </label>
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
              <span>scene 阈值与证据分</span>
            </div>
            {sceneEditorError ? <div className="strategy-manage-message strategy-manage-message-error">{sceneEditorError}</div> : null}
            <div className="strategy-manage-editor-grid strategy-manage-editor-grid-scene">
              <label className="strategy-manage-field strategy-manage-field-span-full">
                <span>名称</span>
                <input value={sceneDraft.name} onChange={(event) => setSceneDraft({ ...sceneDraft, name: event.target.value })} />
              </label>
              <label className="strategy-manage-field">
                <span>observe_threshold</span>
                <input
                  type="number"
                  step="0.1"
                  value={sceneDraft.observe_threshold}
                  onChange={(event) => setSceneDraft({ ...sceneDraft, observe_threshold: Number(event.target.value) || 0 })}
                />
              </label>
              <label className="strategy-manage-field">
                <span>trigger_threshold</span>
                <input
                  type="number"
                  step="0.1"
                  value={sceneDraft.trigger_threshold}
                  onChange={(event) => setSceneDraft({ ...sceneDraft, trigger_threshold: Number(event.target.value) || 0 })}
                />
              </label>
              <label className="strategy-manage-field">
                <span>confirm_threshold</span>
                <input
                  type="number"
                  step="0.1"
                  value={sceneDraft.confirm_threshold}
                  onChange={(event) => setSceneDraft({ ...sceneDraft, confirm_threshold: Number(event.target.value) || 0 })}
                />
              </label>
              <label className="strategy-manage-field">
                <span>fail_threshold</span>
                <input
                  type="number"
                  step="0.1"
                  value={sceneDraft.fail_threshold}
                  onChange={(event) => setSceneDraft({ ...sceneDraft, fail_threshold: Number(event.target.value) || 0 })}
                />
              </label>
              <label className="strategy-manage-field">
                <span>evidence_score</span>
                <input
                  type="number"
                  step="0.1"
                  value={sceneDraft.evidence_score}
                  onChange={(event) => setSceneDraft({ ...sceneDraft, evidence_score: Number(event.target.value) || 0 })}
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
          </div>
        </div>
      ) : null}
    </div>
  )
}
