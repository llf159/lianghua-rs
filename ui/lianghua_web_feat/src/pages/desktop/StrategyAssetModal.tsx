import { useCallback, useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  activateCyqChenStrategyBackup,
  backupCyqChenStrategyFile,
  createEmptyCyqChenStrategyBackup,
  deleteCyqChenStrategyBackup,
  exportCyqChenActiveStrategyFile,
  exportCyqChenStrategyBackupFile,
  exportCyqChenStrategyBundle,
  getCyqChenStrategyBackupDiff,
  getCyqChenStrategyPage,
  importCyqChenStrategyBackup,
  type CyqChenStrategyBackupDiff,
  type CyqChenStrategyBackupItem,
  type CyqChenStrategyPageData,
} from '../../apis/cyqChen'
import {
  activateManagedStrategyBackup,
  backupManagedActiveStrategy,
  createManagedEmptyStrategyBackup,
  deleteManagedStrategyBackup,
  exportManagedActiveStrategy,
  exportManagedStrategyBackupFile,
  exportManagedStrategyBundle,
  getManagedStrategyAssetsStatus,
  getManagedStrategyBackupDiff,
  importManagedStrategyBackup,
  updateManagedStrategyBackupDescription,
  type ManagedStrategyAssetsStatus,
  type ManagedStrategyBackupDiff,
  type ManagedStrategyBackupItem,
} from '../../apis/strategyAssets'
import ConfirmDialog from '../../shared/ConfirmDialog'
import './css/StrategyAssetModal.css'

type BusyAction =
  | 'idle'
  | 'loading'
  | 'importing'
  | 'creating-empty'
  | 'backing-up'
  | 'exporting-active'
  | 'exporting-bundle'
  | `activating:${string}`
  | `deleting:${string}`
  | `exporting:${string}`
  | `diffing:${string}`
  | `saving-desc:${string}`
  | 'backing-up-chip'
  | 'importing-chip'
  | 'exporting-chip-active'
  | 'creating-empty-chip'
  | 'exporting-chip-bundle'
  | `activating-chip:${string}`
  | `deleting-chip:${string}`
  | `diffing-chip:${string}`
  | `exporting-chip:${string}`

type BackupViewMode = 'managed' | 'auto'
type AssetKind = 'rank' | 'chip'
type StrategyBackupDiffData = ManagedStrategyBackupDiff | CyqChenStrategyBackupDiff

type StrategyAssetModalProps = {
  open: boolean
  sourcePath?: string
  initialAssetKind?: AssetKind
  onClose: () => void
  onActivated?: () => void
}

function formatTime(value: string | null | undefined) {
  if (!value) {
    return '--'
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

function formatBytes(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value) || value < 0) {
    return '--'
  }

  const units = ['B', 'KB', 'MB', 'GB']
  let current = value
  let index = 0
  while (current >= 1024 && index < units.length - 1) {
    current /= 1024
    index += 1
  }
  return `${current.toFixed(index === 0 ? 0 : 1)} ${units[index]}`
}

function describeBackupSource(item: { sourceKind?: string }) {
  if (item.sourceKind === 'imported') {
    return '外部导入'
  }
  if (item.sourceKind === 'auto_entry') {
    return '自动备份'
  }
  if (item.sourceKind === 'empty') {
    return '空白模板'
  }
  return '手动备份'
}

function backupSourceTagClass(sourceKind?: string) {
  if (sourceKind === 'imported') {
    return 'strategy-asset-tag is-imported'
  }
  if (sourceKind === 'empty') {
    return 'strategy-asset-tag is-empty'
  }
  if (sourceKind === 'auto_entry') {
    return 'strategy-asset-tag is-auto'
  }
  return 'strategy-asset-tag'
}

function backupSourceTagLabel(sourceKind?: string) {
  if (sourceKind === 'imported') {
    return '导入'
  }
  if (sourceKind === 'empty') {
    return '模板'
  }
  if (sourceKind === 'auto_entry') {
    return '自动'
  }
  return '备份'
}

export default function StrategyAssetModal(props: StrategyAssetModalProps) {
  const { open, sourcePath = '', initialAssetKind = 'rank', onClose, onActivated } = props
  const navigate = useNavigate()
  const [status, setStatus] = useState<ManagedStrategyAssetsStatus | null>(null)
  const [chipStatus, setChipStatus] = useState<CyqChenStrategyPageData | null>(null)
  const [busyAction, setBusyAction] = useState<BusyAction>('idle')
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [chipError, setChipError] = useState('')
  const [descriptionDrafts, setDescriptionDrafts] = useState<Record<string, string>>({})
  const [pendingDeleteBackup, setPendingDeleteBackup] = useState<ManagedStrategyBackupItem | null>(null)
  const [pendingDeleteChipBackup, setPendingDeleteChipBackup] = useState<CyqChenStrategyBackupItem | null>(null)
  const [backupViewMode, setBackupViewMode] = useState<BackupViewMode>('managed')
  const [activeAssetKind, setActiveAssetKind] = useState<AssetKind>('rank')
  const [diffData, setDiffData] = useState<StrategyBackupDiffData | null>(null)

  const isBusy = busyAction !== 'idle'
  const autoBackups = useMemo(
    () => status?.backups.filter((item) => item.sourceKind === 'auto_entry') ?? [],
    [status?.backups],
  )
  const managedBackups = useMemo(
    () => status?.backups.filter((item) => item.sourceKind !== 'auto_entry' && item.sourceKind !== 'rank_compute') ?? [],
    [status?.backups],
  )
  const chipAutoBackups = useMemo(
    () => chipStatus?.backups.filter((item) => item.sourceKind === 'auto_entry') ?? [],
    [chipStatus?.backups],
  )
  const chipManagedBackups = useMemo(
    () => chipStatus?.backups.filter((item) => item.sourceKind !== 'auto_entry') ?? [],
    [chipStatus?.backups],
  )
  const visibleBackups = backupViewMode === 'auto' ? autoBackups : managedBackups
  const visibleChipBackups = backupViewMode === 'auto' ? chipAutoBackups : chipManagedBackups
  const backupCount = managedBackups.length
  const autoBackupCount = autoBackups.length
  const chipBackupCount = chipManagedBackups.length
  const chipAutoBackupCount = chipAutoBackups.length
  const latestBackup = useMemo(
    () => (managedBackups.length ? managedBackups[0] : null),
    [managedBackups],
  )
  const latestChipBackup = useMemo(
    () => (chipManagedBackups.length ? chipManagedBackups[0] : null),
    [chipManagedBackups],
  )

  const loadStatus = useCallback(async () => {
    setBusyAction('loading')
    setError('')
    setChipError('')
    try {
      const nextStatus = await getManagedStrategyAssetsStatus()
      setStatus(nextStatus)
      setDescriptionDrafts(
        nextStatus.backups.reduce<Record<string, string>>((accumulator, item) => {
          accumulator[item.backupId] = item.description ?? ''
          return accumulator
        }, {}),
      )
      const nextSourcePath = sourcePath.trim() || nextStatus.sourcePath
      if (nextSourcePath.trim()) {
        try {
          const nextChipStatus = await getCyqChenStrategyPage(nextSourcePath)
          setChipStatus(nextChipStatus)
        } catch (loadChipError) {
          setChipStatus(null)
          setChipError(`读取筹码策略资产失败: ${String(loadChipError)}`)
        }
      } else {
        setChipStatus(null)
      }
    } catch (loadError) {
      setError(`读取策略资产失败: ${String(loadError)}`)
    } finally {
      setBusyAction('idle')
    }
  }, [sourcePath])

  useEffect(() => {
    if (!open) {
      setPendingDeleteBackup(null)
      setPendingDeleteChipBackup(null)
      setBackupViewMode('managed')
      setActiveAssetKind(initialAssetKind)
      setDiffData(null)
      setChipStatus(null)
      setChipError('')
      return
    }
    setActiveAssetKind(initialAssetKind)
    void loadStatus()
  }, [open, loadStatus, initialAssetKind])

  useEffect(() => {
    if (!open) {
      return
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        onClose()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [open, onClose])

  async function onImport() {
    setBusyAction('importing')
    setError('')
    try {
      const item = await importManagedStrategyBackup()
      if (!item) {
        setBusyAction('idle')
        return
      }
      const nextStatus = await getManagedStrategyAssetsStatus()
      setStatus(nextStatus)
      setNotice(`策略已导入到 ${item.folderName}，可在备份区设为生效。`)
    } catch (actionError) {
      setNotice('')
      setError(`导入策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onBackupActive() {
    setBusyAction('backing-up')
    setError('')
    try {
      const item = await backupManagedActiveStrategy()
      const nextStatus = await getManagedStrategyAssetsStatus()
      setStatus(nextStatus)
      setNotice(`当前生效策略已备份到 ${item.folderName}。`)
    } catch (actionError) {
      setNotice('')
      setError(`备份当前策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onCreateEmptyStrategy() {
    setBusyAction('creating-empty')
    setError('')
    try {
      const item = await createManagedEmptyStrategyBackup()
      const nextStatus = await getManagedStrategyAssetsStatus()
      setStatus(nextStatus)
      setNotice(`已创建空白模板策略 ${item.folderName}，可先设为生效再进入编辑。`)
    } catch (actionError) {
      setNotice('')
      setError(`创建空白策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onExportActive() {
    setBusyAction('exporting-active')
    setError('')
    try {
      const result = await exportManagedActiveStrategy()
      if (!result) {
        setBusyAction('idle')
        return
      }
      setNotice(`当前生效策略已导出到 ${result.exportedPath}。`)
    } catch (actionError) {
      setNotice('')
      setError(`导出当前策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onExportBundle() {
    setBusyAction('exporting-bundle')
    setError('')
    try {
      const result = await exportManagedStrategyBundle()
      if (!result) {
        setBusyAction('idle')
        return
      }
      setNotice(
        `策略资产包已导出到 ${result.exportedPath}，包含${result.includesActiveStrategy ? '当前生效策略和' : ''}${result.backupCount} 个备份文件。`,
      )
    } catch (actionError) {
      setNotice('')
      setError(`导出策略资产包失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onActivate(item: ManagedStrategyBackupItem) {
    setBusyAction(`activating:${item.backupId}`)
    setError('')
    try {
      const nextStatus = await activateManagedStrategyBackup(item.backupId)
      setStatus(nextStatus)
      setNotice(`已将 ${item.folderName} 设为当前生效策略。`)
      onActivated?.()
    } catch (actionError) {
      setNotice('')
      setError(`设为生效失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onExportBackup(item: ManagedStrategyBackupItem) {
    setBusyAction(`exporting:${item.backupId}`)
    setError('')
    try {
      const result = await exportManagedStrategyBackupFile(item.backupId)
      if (!result) {
        setBusyAction('idle')
        return
      }
      setNotice(`${item.folderName} 已导出到 ${result.exportedPath}。`)
    } catch (actionError) {
      setNotice('')
      setError(`导出备份策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onViewDiff(item: ManagedStrategyBackupItem) {
    setBusyAction(`diffing:${item.backupId}`)
    setError('')
    setNotice('')
    try {
      const diff = await getManagedStrategyBackupDiff(item.backupId)
      setDiffData(diff)
    } catch (actionError) {
      setDiffData(null)
      setError(`查看 diff 失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onDeleteBackup(item: ManagedStrategyBackupItem) {
    setBusyAction(`deleting:${item.backupId}`)
    setError('')
    try {
      const nextStatus = await deleteManagedStrategyBackup(item.backupId)
      setStatus(nextStatus)
      setDiffData((current) => (current?.backupId === item.backupId ? null : current))
      setNotice(`已删除策略备份 ${item.folderName}。`)
    } catch (actionError) {
      setNotice('')
      setError(`删除备份策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onSaveDescription(item: ManagedStrategyBackupItem) {
    const draft = (descriptionDrafts[item.backupId] ?? '').trim()
    const current = (item.description ?? '').trim()
    if (draft === current) {
      return
    }

    setBusyAction(`saving-desc:${item.backupId}`)
    setError('')
    try {
      const nextStatus = await updateManagedStrategyBackupDescription(item.backupId, draft)
      setStatus(nextStatus)
      setDescriptionDrafts(
        nextStatus.backups.reduce<Record<string, string>>((accumulator, backup) => {
          accumulator[backup.backupId] = backup.description ?? ''
          return accumulator
        }, {}),
      )
      setNotice(`已更新 ${item.folderName} 的说明。`)
    } catch (actionError) {
      setNotice('')
      setError(`保存说明失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onBackupChipStrategy() {
    const nextSourcePath = sourcePath.trim() || status?.sourcePath.trim() || ''
    if (!nextSourcePath) {
      setChipError('当前数据目录为空，请先确认数据源。')
      return
    }
    setBusyAction('backing-up-chip')
    setChipError('')
    setNotice('')
    try {
      const page = await backupCyqChenStrategyFile(nextSourcePath)
      setChipStatus(page)
      setNotice('当前筹码策略已备份。')
    } catch (actionError) {
      setChipError(`备份筹码策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onCreateEmptyChipStrategy() {
    const nextSourcePath = sourcePath.trim() || status?.sourcePath.trim() || ''
    if (!nextSourcePath) {
      setChipError('当前数据目录为空，请先确认数据源。')
      return
    }
    setBusyAction('creating-empty-chip')
    setChipError('')
    setNotice('')
    try {
      const page = await createEmptyCyqChenStrategyBackup(nextSourcePath)
      setChipStatus(page)
      setNotice('已创建空白筹码策略，可先恢复为当前再进入编辑。')
    } catch (actionError) {
      setChipError(`创建空白筹码策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onImportChipStrategy() {
    const nextSourcePath = sourcePath.trim() || status?.sourcePath.trim() || ''
    if (!nextSourcePath) {
      setChipError('当前数据目录为空，请先确认数据源。')
      return
    }
    setBusyAction('importing-chip')
    setChipError('')
    setNotice('')
    try {
      const page = await importCyqChenStrategyBackup(nextSourcePath)
      if (!page) {
        setBusyAction('idle')
        return
      }
      setChipStatus(page)
      setNotice('筹码策略已导入到备份区。')
    } catch (actionError) {
      setChipError(`导入筹码策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onExportChipActive() {
    const nextSourcePath = sourcePath.trim() || status?.sourcePath.trim() || ''
    if (!nextSourcePath) {
      setChipError('当前数据目录为空，请先确认数据源。')
      return
    }
    setBusyAction('exporting-chip-active')
    setChipError('')
    setNotice('')
    try {
      const result = await exportCyqChenActiveStrategyFile(nextSourcePath)
      if (!result) {
        setBusyAction('idle')
        return
      }
      setNotice(`当前筹码策略已导出到 ${result.exportedPath}。`)
    } catch (actionError) {
      setChipError(`导出当前筹码策略失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onExportChipBundle() {
    const nextSourcePath = sourcePath.trim() || status?.sourcePath.trim() || ''
    if (!nextSourcePath) {
      setChipError('当前数据目录为空，请先确认数据源。')
      return
    }
    setBusyAction('exporting-chip-bundle')
    setChipError('')
    setNotice('')
    try {
      const result = await exportCyqChenStrategyBundle(nextSourcePath)
      if (!result) {
        setBusyAction('idle')
        return
      }
      setNotice(
        `筹码策略资产包已导出到 ${result.exportedPath}，包含${result.includesActiveStrategy ? '当前生效策略和' : ''}${result.backupCount} 个备份文件。`,
      )
    } catch (actionError) {
      setChipError(`导出筹码策略资产包失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  function navigateToStrategyPage(kind: AssetKind) {
    onClose()
    navigate(kind === 'chip' ? '/strategy/chip-change' : '/strategy/rules')
  }

  async function onActivateChipBackup(item: CyqChenStrategyBackupItem) {
    const nextSourcePath = sourcePath.trim() || status?.sourcePath.trim() || ''
    if (!nextSourcePath) {
      setChipError('当前数据目录为空，请先确认数据源。')
      return
    }
    setBusyAction(`activating-chip:${item.backupId}`)
    setChipError('')
    setNotice('')
    try {
      const page = await activateCyqChenStrategyBackup(nextSourcePath, item.backupId)
      setChipStatus(page)
      setNotice(`已恢复筹码策略备份 ${item.fileName}。`)
      onActivated?.()
    } catch (actionError) {
      setChipError(`恢复筹码策略备份失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onExportChipBackup(item: CyqChenStrategyBackupItem) {
    const nextSourcePath = sourcePath.trim() || status?.sourcePath.trim() || ''
    if (!nextSourcePath) {
      setChipError('当前数据目录为空，请先确认数据源。')
      return
    }
    setBusyAction(`exporting-chip:${item.backupId}`)
    setChipError('')
    setNotice('')
    try {
      const result = await exportCyqChenStrategyBackupFile(nextSourcePath, item.backupId)
      if (!result) {
        setBusyAction('idle')
        return
      }
      setNotice(`${item.fileName} 已导出到 ${result.exportedPath}。`)
    } catch (actionError) {
      setChipError(`导出筹码策略备份失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onViewChipDiff(item: CyqChenStrategyBackupItem) {
    const nextSourcePath = sourcePath.trim() || status?.sourcePath.trim() || ''
    if (!nextSourcePath) {
      setChipError('当前数据目录为空，请先确认数据源。')
      return
    }
    setBusyAction(`diffing-chip:${item.backupId}`)
    setChipError('')
    setNotice('')
    try {
      const diff = await getCyqChenStrategyBackupDiff(nextSourcePath, item.backupId)
      setDiffData(diff)
    } catch (actionError) {
      setDiffData(null)
      setChipError(`查看筹码策略 diff 失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onDeleteChipBackup(item: CyqChenStrategyBackupItem) {
    const nextSourcePath = sourcePath.trim() || status?.sourcePath.trim() || ''
    if (!nextSourcePath) {
      setChipError('当前数据目录为空，请先确认数据源。')
      return
    }
    setBusyAction(`deleting-chip:${item.backupId}`)
    setChipError('')
    setNotice('')
    try {
      const page = await deleteCyqChenStrategyBackup(nextSourcePath, item.backupId)
      setChipStatus(page)
      setDiffData((current) => (current?.backupId === item.backupId ? null : current))
      setNotice(`已删除筹码策略备份 ${item.fileName}。`)
    } catch (actionError) {
      setChipError(`删除筹码策略备份失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  function renderDiffPanel() {
    if (!diffData) {
      return null
    }

    return (
      <section className="strategy-asset-diff">
        <div className="strategy-asset-section-head strategy-asset-diff-title">
          <div>
            <h4>策略 diff</h4>
            <p>
              {diffData.backupLabel} 对比当前生效 {diffData.activeLabel}，
              {diffData.changedLineCount === 0
                ? '没有差异。'
                : `共 ${diffData.changedLineCount} 行差异。`}
            </p>
          </div>
          <button className="strategy-asset-btn strategy-asset-btn-ghost" type="button" onClick={() => setDiffData(null)} disabled={isBusy}>
            关闭 diff
          </button>
        </div>
        <div className="strategy-asset-diff-head">
          <span>备份</span>
          <span>当前</span>
          <span>内容</span>
        </div>
        <div className="strategy-asset-diff-body">
          {diffData.lines.map((line, index) => (
            <div key={`${line.kind}-${line.backupLine ?? 'n'}-${line.activeLine ?? 'n'}-${index}`} className={`strategy-asset-diff-row is-${line.kind}`}>
              <span>{line.backupLine ?? ''}</span>
              <span>{line.activeLine ?? ''}</span>
              <code>
                {line.kind === 'backup'
                  ? '- '
                  : line.kind === 'active'
                    ? '+ '
                    : line.kind === 'omitted'
                      ? '... '
                      : '  '}
                {line.text || ' '}
              </code>
            </div>
          ))}
        </div>
      </section>
    )
  }

  if (!open) {
    return null
  }

  return (
    <div
      className="strategy-asset-modal-backdrop"
      role="presentation"
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose()
        }
      }}
    >
      <div className="strategy-asset-modal" role="dialog" aria-modal="true">
        <div className="strategy-asset-head">
          <div>
            <h3>策略资产中心</h3>
            <p>排名策略和筹码策略分区管理，当前文件、备份和恢复动作集中在这里。</p>
          </div>
          <div className="strategy-asset-actions">
            <button className="strategy-asset-btn" type="button" onClick={() => void loadStatus()} disabled={isBusy}>
              {busyAction === 'loading' ? '刷新中...' : '刷新'}
            </button>
            <button className="strategy-asset-btn strategy-asset-btn-ghost" type="button" onClick={onClose} disabled={isBusy}>
              关闭
            </button>
          </div>
        </div>

        <div className="strategy-asset-switcher" role="tablist" aria-label="策略资产类型">
          <button
            className={activeAssetKind === 'rank' ? 'strategy-asset-tab is-active' : 'strategy-asset-tab'}
            type="button"
            role="tab"
            aria-selected={activeAssetKind === 'rank'}
            onClick={() => {
              setActiveAssetKind('rank')
              setDiffData(null)
            }}
          >
            <span>排名策略</span>
            <strong>{backupCount}</strong>
          </button>
          <button
            className={activeAssetKind === 'chip' ? 'strategy-asset-tab is-active' : 'strategy-asset-tab'}
            type="button"
            role="tab"
            aria-selected={activeAssetKind === 'chip'}
            onClick={() => {
              setActiveAssetKind('chip')
              setDiffData(null)
            }}
          >
            <span>筹码策略</span>
            <strong>{chipBackupCount}</strong>
          </button>
        </div>

        <div className="strategy-asset-summary">
          <article className="strategy-asset-summary-card">
            <span>排名策略</span>
            <strong>{status?.active.exists ? '已配置' : '缺少文件'}</strong>
            <small>{latestBackup ? `最近备份 ${formatTime(latestBackup.createdAt)}` : '暂无手动/导入备份'}</small>
          </article>
          <article className="strategy-asset-summary-card">
            <span>筹码策略</span>
            <strong>{chipStatus?.exists ? '已配置' : '草稿/缺少文件'}</strong>
            <small>{latestChipBackup ? `最近备份 ${formatTime(latestChipBackup.createdAt ?? latestChipBackup.modifiedAt)}` : `${chipStatus?.strategies.length ?? 0} 条策略`}</small>
          </article>
          <article className="strategy-asset-summary-card">
            <span>资产目录</span>
            <strong title={status?.sourcePath ?? sourcePath}>
              {(status?.sourcePath ?? sourcePath) || '读取中...'}
            </strong>
            <small>{status?.backupRootPath ? `排名备份：${status.backupRootPath}` : '等待读取目录'}</small>
          </article>
        </div>

        {notice ? <div className="strategy-asset-notice">{notice}</div> : null}
        {error ? <div className="strategy-asset-error">{error}</div> : null}

        {activeAssetKind === 'rank' ? (
          <div className="strategy-asset-workspace">
            <div className="strategy-asset-command-bar">
              <button className="strategy-asset-btn strategy-asset-btn-primary" type="button" onClick={() => void onImport()} disabled={isBusy}>
                {busyAction === 'importing' ? '导入中...' : '导入排名策略'}
              </button>
              <button className="strategy-asset-btn" type="button" onClick={() => void onCreateEmptyStrategy()} disabled={isBusy}>
                {busyAction === 'creating-empty' ? '创建中...' : '新增空白排名策略'}
              </button>
              <button className="strategy-asset-btn" type="button" onClick={() => void onBackupActive()} disabled={isBusy || !status?.active.exists}>
                {busyAction === 'backing-up' ? '备份中...' : '备份当前排名策略'}
              </button>
              <button className="strategy-asset-btn" type="button" onClick={() => void onExportActive()} disabled={isBusy || !status?.active.exists}>
                {busyAction === 'exporting-active' ? '导出中...' : '导出当前排名策略'}
              </button>
              <button className="strategy-asset-btn" type="button" onClick={() => void onExportBundle()} disabled={isBusy}>
                {busyAction === 'exporting-bundle' ? '打包中...' : '导出资产包'}
              </button>
              <button className="strategy-asset-btn" type="button" onClick={() => navigateToStrategyPage('rank')} disabled={isBusy}>
                进入打分策略页
              </button>
              <button
                className={backupViewMode === 'auto' ? 'strategy-asset-btn strategy-asset-btn-active' : 'strategy-asset-btn'}
                type="button"
                onClick={() => setBackupViewMode((current) => (current === 'auto' ? 'managed' : 'auto'))}
                disabled={isBusy}
              >
                {backupViewMode === 'auto' ? '手动/导入备份' : `自动备份 (${autoBackupCount})`}
              </button>
            </div>

            <section className="strategy-asset-current">
              <div className="strategy-asset-section-head">
                <div>
                  <h4>当前排名策略</h4>
                  <p>score_rule.toml</p>
                </div>
                <span className={status?.active.exists ? 'strategy-asset-pill is-live' : 'strategy-asset-pill'}>
                  {status?.active.exists ? '生效中' : '缺少'}
                </span>
              </div>
              <div className="strategy-asset-current-grid">
                <div className="strategy-asset-meta strategy-asset-meta-wide">
                  <span>活动文件</span>
                  <strong title={status?.active.absolutePath ?? ''}>{status?.active.absolutePath ?? '--'}</strong>
                </div>
                <div className="strategy-asset-meta">
                  <span>大小</span>
                  <strong>{formatBytes(status?.active.sizeBytes)}</strong>
                </div>
                <div className="strategy-asset-meta">
                  <span>最后修改</span>
                  <strong>{formatTime(status?.active.modifiedAt)}</strong>
                </div>
              </div>
            </section>

            <section className="strategy-asset-backups">
              <div className="strategy-asset-section-head">
                <div>
                  <h4>{backupViewMode === 'auto' ? '自动备份' : '排名策略备份'}</h4>
                  <p>{backupViewMode === 'auto' ? '进入策略管理页时生成。' : '导入、空白模板和手动备份。'}</p>
                </div>
                <span className="strategy-asset-pill">{visibleBackups.length} 份</span>
              </div>

              {visibleBackups.length ? (
                <div className="strategy-asset-backup-list">
                  {visibleBackups.map((item) => {
                    const isActivating = busyAction === `activating:${item.backupId}`
                    const isDeleting = busyAction === `deleting:${item.backupId}`
                    const isExporting = busyAction === `exporting:${item.backupId}`
                    const isDiffing = busyAction === `diffing:${item.backupId}`
                    const isSavingDesc = busyAction === `saving-desc:${item.backupId}`
                    const descriptionDraft = descriptionDrafts[item.backupId] ?? ''
                    const descriptionDirty = descriptionDraft.trim() !== (item.description ?? '').trim()
                    return (
                      <article key={item.backupId} className="strategy-asset-backup-card">
                        <div className="strategy-asset-backup-head">
                          <div>
                            <strong>{item.folderName}</strong>
                            <p>{describeBackupSource(item)}</p>
                          </div>
                          <span
                            className={backupSourceTagClass(item.sourceKind)}
                          >
                            {backupSourceTagLabel(item.sourceKind)}
                          </span>
                        </div>

                        <div className="strategy-asset-backup-meta-grid">
                          <div className="strategy-asset-meta">
                            <span>创建时间</span>
                            <strong>{formatTime(item.createdAt)}</strong>
                          </div>
                          <div className="strategy-asset-meta">
                            <span>文件大小</span>
                            <strong>{formatBytes(item.sizeBytes)}</strong>
                          </div>
                          <div className="strategy-asset-meta strategy-asset-meta-desc">
                            <span>说明</span>
                            <div className="strategy-asset-desc-editor">
                              <input
                                className="strategy-asset-desc-input"
                                type="text"
                                placeholder="备注"
                                maxLength={120}
                                value={descriptionDraft}
                                onChange={(event) => {
                                  const nextValue = event.target.value
                                  setDescriptionDrafts((prev) => ({ ...prev, [item.backupId]: nextValue }))
                                }}
                                disabled={isBusy}
                              />
                              <button
                                className="strategy-asset-btn"
                                type="button"
                                onClick={() => void onSaveDescription(item)}
                                disabled={(isBusy && !isSavingDesc) || isSavingDesc || !descriptionDirty}
                              >
                                {isSavingDesc ? '保存中...' : '保存'}
                              </button>
                            </div>
                          </div>
                        </div>

                        <div className="strategy-asset-backup-path" title={item.absolutePath}>
                          {item.absolutePath}
                        </div>

                        <div className="strategy-asset-backup-actions">
                          <button className="strategy-asset-btn strategy-asset-btn-primary" type="button" onClick={() => void onActivate(item)} disabled={isBusy}>
                            {isActivating ? '生效中...' : '设为生效'}
                          </button>
                          <button className="strategy-asset-btn" type="button" onClick={() => void onExportBackup(item)} disabled={isBusy}>
                            {isExporting ? '导出中...' : '导出'}
                          </button>
                          <button className="strategy-asset-btn" type="button" onClick={() => void onViewDiff(item)} disabled={isBusy}>
                            {isDiffing ? '对比中...' : diffData?.backupId === item.backupId ? '刷新 diff' : '查看 diff'}
                          </button>
                          <button className="strategy-asset-btn strategy-asset-btn-danger" type="button" onClick={() => setPendingDeleteBackup(item)} disabled={isBusy}>
                            {isDeleting ? '删除中...' : '删除'}
                          </button>
                        </div>

                        {diffData?.backupId === item.backupId ? renderDiffPanel() : null}
                      </article>
                    )
                  })}
                </div>
              ) : (
                <div className="strategy-asset-empty">
                  {backupViewMode === 'auto'
                    ? '还没有自动备份。'
                    : '还没有排名策略备份。'}
                </div>
              )}
            </section>
          </div>
        ) : (
          <div className="strategy-asset-workspace">
            {chipError ? <div className="strategy-asset-error">{chipError}</div> : null}
            <div className="strategy-asset-command-bar">
              <button
                className="strategy-asset-btn strategy-asset-btn-primary"
                type="button"
                onClick={() => void onImportChipStrategy()}
                disabled={isBusy}
              >
                {busyAction === 'importing-chip' ? '导入中...' : '导入筹码策略'}
              </button>
              <button
                className="strategy-asset-btn"
                type="button"
                onClick={() => void onCreateEmptyChipStrategy()}
                disabled={isBusy}
              >
                {busyAction === 'creating-empty-chip' ? '创建中...' : '新增空白筹码策略'}
              </button>
              <button
                className="strategy-asset-btn"
                type="button"
                onClick={() => void onBackupChipStrategy()}
                disabled={isBusy || !chipStatus?.exists}
              >
                {busyAction === 'backing-up-chip' ? '备份中...' : '备份当前筹码策略'}
              </button>
              <button
                className="strategy-asset-btn"
                type="button"
                onClick={() => void onExportChipActive()}
                disabled={isBusy || !chipStatus?.exists}
              >
                {busyAction === 'exporting-chip-active' ? '导出中...' : '导出当前筹码策略'}
              </button>
              <button
                className="strategy-asset-btn"
                type="button"
                onClick={() => void onExportChipBundle()}
                disabled={isBusy}
              >
                {busyAction === 'exporting-chip-bundle' ? '打包中...' : '导出资产包'}
              </button>
              <button className="strategy-asset-btn" type="button" onClick={() => navigateToStrategyPage('chip')} disabled={isBusy}>
                进入筹码策略页
              </button>
              <button
                className={backupViewMode === 'auto' ? 'strategy-asset-btn strategy-asset-btn-active' : 'strategy-asset-btn'}
                type="button"
                onClick={() => setBackupViewMode((current) => (current === 'auto' ? 'managed' : 'auto'))}
                disabled={isBusy}
              >
                {backupViewMode === 'auto' ? '手动/导入备份' : `自动备份 (${chipAutoBackupCount})`}
              </button>
            </div>

            <section className="strategy-asset-current">
              <div className="strategy-asset-section-head">
                <div>
                  <h4>当前筹码策略</h4>
                  <p>chip_change_rule.toml</p>
                </div>
                <span className={chipStatus?.exists ? 'strategy-asset-pill is-live' : 'strategy-asset-pill'}>
                  {chipStatus?.exists ? '生效中' : '未落盘'}
                </span>
              </div>
              <div className="strategy-asset-current-grid">
                <div className="strategy-asset-meta strategy-asset-meta-wide">
                  <span>活动文件</span>
                  <strong title={chipStatus?.filePath ?? ''}>{chipStatus?.filePath ?? '--'}</strong>
                </div>
                <div className="strategy-asset-meta">
                  <span>策略数量</span>
                  <strong>{chipStatus?.strategies.length ?? 0}</strong>
                </div>
                <div className="strategy-asset-meta">
                  <span>备份数量</span>
                  <strong>{chipBackupCount}</strong>
                </div>
              </div>
            </section>

            <section className="strategy-asset-backups">
              <div className="strategy-asset-section-head">
                <div>
                  <h4>{backupViewMode === 'auto' ? '自动备份' : '筹码策略备份'}</h4>
                  <p>{backupViewMode === 'auto' ? '进入筹码策略页或运行筹码计算时生成。' : '导入、空白模板和手动备份。'}</p>
                </div>
                <span className="strategy-asset-pill">{visibleChipBackups.length} 份</span>
              </div>

              {visibleChipBackups.length ? (
                <div className="strategy-asset-backup-list strategy-asset-chip-backup-list">
                  {visibleChipBackups.map((item) => {
                    const isActivatingChip = busyAction === `activating-chip:${item.backupId}`
                    const isDeletingChip = busyAction === `deleting-chip:${item.backupId}`
                    const isDiffingChip = busyAction === `diffing-chip:${item.backupId}`
                    const isExportingChip = busyAction === `exporting-chip:${item.backupId}`
                    return (
                      <article key={item.backupId} className="strategy-asset-backup-card">
                        <div className="strategy-asset-backup-head">
                          <div>
                            <strong>{item.fileName}</strong>
                            <p>{describeBackupSource(item)}</p>
                          </div>
                          <span className={backupSourceTagClass(item.sourceKind)}>
                            {backupSourceTagLabel(item.sourceKind)}
                          </span>
                        </div>
                        <div className="strategy-asset-backup-meta-grid strategy-asset-chip-meta-grid">
                          <div className="strategy-asset-meta">
                            <span>创建时间</span>
                            <strong>{formatTime(item.createdAt ?? item.modifiedAt)}</strong>
                          </div>
                          <div className="strategy-asset-meta">
                            <span>文件大小</span>
                            <strong>{formatBytes(item.sizeBytes)}</strong>
                          </div>
                        </div>
                        <div className="strategy-asset-backup-path" title={item.filePath}>
                          {item.filePath}
                        </div>
                        <div className="strategy-asset-backup-actions">
                          <button
                            className="strategy-asset-btn strategy-asset-btn-primary"
                            type="button"
                            onClick={() => void onActivateChipBackup(item)}
                            disabled={isBusy}
                          >
                            {isActivatingChip ? '恢复中...' : '恢复为当前'}
                          </button>
                          <button
                            className="strategy-asset-btn"
                            type="button"
                            onClick={() => void onExportChipBackup(item)}
                            disabled={isBusy}
                          >
                            {isExportingChip ? '导出中...' : '导出'}
                          </button>
                          <button
                            className="strategy-asset-btn"
                            type="button"
                            onClick={() => void onViewChipDiff(item)}
                            disabled={isBusy || !chipStatus?.exists}
                          >
                            {isDiffingChip ? '对比中...' : diffData?.backupId === item.backupId ? '刷新 diff' : '查看 diff'}
                          </button>
                          <button
                            className="strategy-asset-btn strategy-asset-btn-danger"
                            type="button"
                            onClick={() => setPendingDeleteChipBackup(item)}
                            disabled={isBusy}
                          >
                            {isDeletingChip ? '删除中...' : '删除'}
                          </button>
                        </div>

                        {diffData?.backupId === item.backupId ? renderDiffPanel() : null}
                      </article>
                    )
                  })}
                </div>
              ) : (
                <div className="strategy-asset-empty">
                  {backupViewMode === 'auto'
                    ? '还没有自动备份。'
                    : '还没有筹码策略备份。'}
                </div>
              )}
            </section>
          </div>
        )}
      </div>

      <ConfirmDialog
        open={pendingDeleteBackup !== null}
        title="确认删除策略备份"
        message={pendingDeleteBackup ? `确认删除策略备份 ${pendingDeleteBackup.folderName} 吗？` : ''}
        confirmText="确认删除"
        cancelText="取消"
        danger
        busy={isBusy}
        onCancel={() => setPendingDeleteBackup(null)}
        onConfirm={() => {
          if (!pendingDeleteBackup) {
            return
          }
          const target = pendingDeleteBackup
          setPendingDeleteBackup(null)
          void onDeleteBackup(target)
        }}
      />
      <ConfirmDialog
        open={pendingDeleteChipBackup !== null}
        title="确认删除筹码策略备份"
        message={pendingDeleteChipBackup ? `确认删除筹码策略备份 ${pendingDeleteChipBackup.fileName} 吗？` : ''}
        confirmText="确认删除"
        cancelText="取消"
        danger
        busy={isBusy}
        onCancel={() => setPendingDeleteChipBackup(null)}
        onConfirm={() => {
          if (!pendingDeleteChipBackup) {
            return
          }
          const target = pendingDeleteChipBackup
          setPendingDeleteChipBackup(null)
          void onDeleteChipBackup(target)
        }}
      />
    </div>
  )
}
