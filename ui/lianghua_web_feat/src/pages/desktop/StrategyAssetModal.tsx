import { useEffect, useMemo, useState } from 'react'
import {
  activateManagedStrategyBackup,
  backupManagedActiveStrategy,
  createManagedEmptyStrategyBackup,
  deleteManagedStrategyBackup,
  exportManagedActiveStrategy,
  exportManagedStrategyBackupFile,
  exportManagedStrategyBundle,
  getManagedStrategyAssetsStatus,
  importManagedStrategyBackup,
  updateManagedStrategyBackupDescription,
  type ManagedStrategyAssetsStatus,
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
  | `saving-desc:${string}`

type StrategyAssetModalProps = {
  open: boolean
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

function describeBackupSource(item: ManagedStrategyBackupItem) {
  if (item.sourceKind === 'imported') {
    return '外部导入'
  }
  if (item.sourceKind === 'empty') {
    return '空白模板'
  }
  return '手动备份'
}

export default function StrategyAssetModal(props: StrategyAssetModalProps) {
  const { open, onClose, onActivated } = props
  const [status, setStatus] = useState<ManagedStrategyAssetsStatus | null>(null)
  const [busyAction, setBusyAction] = useState<BusyAction>('idle')
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [descriptionDrafts, setDescriptionDrafts] = useState<Record<string, string>>({})
  const [pendingDeleteBackup, setPendingDeleteBackup] = useState<ManagedStrategyBackupItem | null>(null)

  const isBusy = busyAction !== 'idle'
  const backupCount = status?.backups.length ?? 0
  const latestBackup = useMemo(
    () => (status?.backups.length ? status.backups[0] : null),
    [status?.backups],
  )

  async function loadStatus() {
    setBusyAction('loading')
    setError('')
    try {
      const nextStatus = await getManagedStrategyAssetsStatus()
      setStatus(nextStatus)
      setDescriptionDrafts(
        nextStatus.backups.reduce<Record<string, string>>((accumulator, item) => {
          accumulator[item.backupId] = item.description ?? ''
          return accumulator
        }, {}),
      )
    } catch (loadError) {
      setError(`读取策略资产失败: ${String(loadError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  useEffect(() => {
    if (!open) {
      setPendingDeleteBackup(null)
      return
    }
    void loadStatus()
  }, [open])

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

  async function onDeleteBackup(item: ManagedStrategyBackupItem) {
    setBusyAction(`deleting:${item.backupId}`)
    setError('')
    try {
      const nextStatus = await deleteManagedStrategyBackup(item.backupId)
      setStatus(nextStatus)
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
            <p>把当前生效策略和历史备份放在一起管理。导入文件会自动收纳到时间戳目录，避免互相覆盖。</p>
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

        <div className="strategy-asset-summary">
          <article className="strategy-asset-summary-card">
            <span>当前生效</span>
            <strong>{status?.active.exists ? '已配置' : '缺少策略文件'}</strong>
            <small>{status?.active.modifiedAt ? `更新于 ${formatTime(status.active.modifiedAt)}` : '尚未导入当前生效策略'}</small>
          </article>
          <article className="strategy-asset-summary-card">
            <span>备份数量</span>
            <strong>{backupCount}</strong>
            <small>{latestBackup ? `最近一份：${formatTime(latestBackup.createdAt)}` : '还没有策略备份'}</small>
          </article>
          <article className="strategy-asset-summary-card">
            <span>备份目录</span>
            <strong title={status?.backupRootPath ?? ''}>
              {status?.backupRootPath ?? '读取中...'}
            </strong>
            <small>每次导入与手动备份都会生成独立时间目录</small>
          </article>
        </div>

        <div className="strategy-asset-command-bar">
          <button className="strategy-asset-btn strategy-asset-btn-primary" type="button" onClick={() => void onImport()} disabled={isBusy}>
            {busyAction === 'importing' ? '导入中...' : '导入策略到备份区'}
          </button>
          <button className="strategy-asset-btn" type="button" onClick={() => void onCreateEmptyStrategy()} disabled={isBusy}>
            {busyAction === 'creating-empty' ? '创建中...' : '新增空白策略'}
          </button>
          <button className="strategy-asset-btn" type="button" onClick={() => void onBackupActive()} disabled={isBusy || !status?.active.exists}>
            {busyAction === 'backing-up' ? '备份中...' : '备份当前生效策略'}
          </button>
          <button className="strategy-asset-btn" type="button" onClick={() => void onExportActive()} disabled={isBusy || !status?.active.exists}>
            {busyAction === 'exporting-active' ? '导出中...' : '导出当前生效策略'}
          </button>
          <button className="strategy-asset-btn" type="button" onClick={() => void onExportBundle()} disabled={isBusy}>
            {busyAction === 'exporting-bundle' ? '打包中...' : '导出策略资产包'}
          </button>
        </div>

        {notice ? <div className="strategy-asset-notice">{notice}</div> : null}
        {error ? <div className="strategy-asset-error">{error}</div> : null}

        <section className="strategy-asset-current">
          <div className="strategy-asset-section-head">
            <div>
              <h4>当前生效策略</h4>
              <p>策略编辑页和现有详情页都会直接读取这份活动文件。</p>
            </div>
            <span className={status?.active.exists ? 'strategy-asset-pill is-live' : 'strategy-asset-pill'}>
              {status?.active.exists ? 'LIVE' : 'MISSING'}
            </span>
          </div>
          <div className="strategy-asset-current-grid">
            <div className="strategy-asset-meta">
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
              <h4>备份策略</h4>
              <p>导入文件与手动备份都会保存在这里，支持导出、删除和设为当前生效。</p>
            </div>
            <span className="strategy-asset-pill">{backupCount} 份</span>
          </div>

          {status?.backups.length ? (
            <div className="strategy-asset-backup-list">
              {status.backups.map((item) => {
                const isActivating = busyAction === `activating:${item.backupId}`
                const isDeleting = busyAction === `deleting:${item.backupId}`
                const isExporting = busyAction === `exporting:${item.backupId}`
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
                        className={
                          item.sourceKind === 'imported'
                            ? 'strategy-asset-tag is-imported'
                            : item.sourceKind === 'empty'
                              ? 'strategy-asset-tag is-empty'
                              : 'strategy-asset-tag'
                        }
                      >
                        {item.sourceKind === 'imported' ? '导入' : item.sourceKind === 'empty' ? '模板' : '备份'}
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
                      <div className="strategy-asset-meta">
                        <span>说明</span>
                        <div className="strategy-asset-desc-editor">
                          <input
                            className="strategy-asset-desc-input"
                            type="text"
                            placeholder="例如：回测基线版本 / 导入后待清洗"
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
                            {isSavingDesc ? '保存中...' : '保存说明'}
                          </button>
                        </div>
                      </div>
                    </div>

                    <div className="strategy-asset-backup-path" title={item.absolutePath}>
                      {item.absolutePath}
                    </div>

                    <div className="strategy-asset-backup-actions">
                      <button className="strategy-asset-btn strategy-asset-btn-primary" type="button" onClick={() => void onActivate(item)} disabled={isBusy}>
                        {isActivating ? '生效中...' : '设为当前生效'}
                      </button>
                      <button className="strategy-asset-btn" type="button" onClick={() => void onExportBackup(item)} disabled={isBusy}>
                        {isExporting ? '导出中...' : '导出'}
                      </button>
                      <button className="strategy-asset-btn strategy-asset-btn-danger" type="button" onClick={() => setPendingDeleteBackup(item)} disabled={isBusy}>
                        {isDeleting ? '删除中...' : '删除'}
                      </button>
                    </div>
                  </article>
                )
              })}
            </div>
          ) : (
            <div className="strategy-asset-empty">
              还没有策略备份。你可以先导入外部策略文件，或者把当前生效策略备份一份。
            </div>
          )}
        </section>
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
    </div>
  )
}
