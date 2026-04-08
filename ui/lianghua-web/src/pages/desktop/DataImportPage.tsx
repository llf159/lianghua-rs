import { useCallback, useEffect, useRef, useState } from 'react'
import {
  MANAGED_SOURCE_FILES,
  clearManagedSourceData,
  exportManagedCacheData,
  exportManagedSourceFile,
  exportManagedSourceDirectory,
  importManagedCacheData,
  importManagedSourceDirectory,
  importManagedSourceFile,
  inspectManagedSourceStatus,
  isDirectoryImportSupported,
  removeManagedSourceFile,
  type ManagedSourceImportProgress,
  type ManagedSourceImportProgressCallback,
  type ManagedSourceDirectoryImportResult,
  type ManagedSourceFileId,
  type ManagedSourceStatus,
} from '../../apis/managedSource'
import './css/DataImportPage.css'

type BusyAction =
  | 'idle'
  | 'loading'
  | 'importing-dir'
  | 'exporting'
  | 'exporting-cache'
  | 'importing-cache'
  | 'clearing'
  | `deleting-file:${ManagedSourceFileId}`
  | `exporting-file:${ManagedSourceFileId}`
  | `file:${ManagedSourceFileId}`

function formatImportTime(value: string | null) {
  if (!value) {
    return '暂无导入记录'
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }

  return date.toLocaleString()
}

function formatBytes(value: number | null) {
  if (value === null || !Number.isFinite(value) || value < 0) {
    return '--'
  }

  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let current = value
  let unitIndex = 0

  while (current >= 1024 && unitIndex < units.length - 1) {
    current /= 1024
    unitIndex += 1
  }

  return `${current.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`
}

function buildDirectoryImportNotice(result: ManagedSourceDirectoryImportResult) {
  const missingLabels = result.missingFileIds
    .map((fileId) => MANAGED_SOURCE_FILES.find((item) => item.id === fileId)?.label ?? fileId)
    .join('、')

  if (result.importedFileIds.length === 0) {
    return `扫描完成，但目录里没有找到可导入文件: ${result.scannedPath}`
  }

  if (result.missingFileIds.length === 0) {
    return `目录扫描完成，已导入 ${result.importedFileIds.length} 个文件: ${result.scannedPath}`
  }

  return `目录扫描完成，已导入 ${result.importedFileIds.length} 个文件；仍缺少 ${missingLabels}: ${result.scannedPath}`
}

function findManagedSourceFileLabel(targetRelativePath: string) {
  const normalizedTargetPath = targetRelativePath.replace(/\\/g, '/').trim()
  return (
    MANAGED_SOURCE_FILES.find(
      (item) =>
        normalizedTargetPath === item.targetRelativePathSuffix ||
        normalizedTargetPath.endsWith(`/${item.targetRelativePathSuffix}`),
    )?.label ?? targetRelativePath
  )
}

export default function DataImportPage() {
  const directoryImportSupported = isDirectoryImportSupported()
  const isMobileClient = !directoryImportSupported
  const [status, setStatus] = useState<ManagedSourceStatus | null>(null)
  const [busyAction, setBusyAction] = useState<BusyAction>('loading')
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const activeImportSessionRef = useRef<number | null>(null)
  const nextImportSessionRef = useRef(0)
  const busyActionRef = useRef<BusyAction>('loading')

  const importedCount = status?.items.filter((item) => item.isImported).length ?? 0
  const isBusy = busyAction !== 'idle'

  const updateBusyAction = useCallback((nextBusyAction: BusyAction) => {
    busyActionRef.current = nextBusyAction
    setBusyAction(nextBusyAction)
  }, [])

  useEffect(() => {
    busyActionRef.current = busyAction
  }, [busyAction])

  function applyStatus(nextStatus: ManagedSourceStatus) {
    setStatus(nextStatus)
  }

  function onImportProgress(progress: ManagedSourceImportProgress) {
    const fileLabel = findManagedSourceFileLabel(progress.targetRelativePath)

    if (progress.phase === 'failed') {
      setNotice('')
      setError(`${fileLabel} 导入失败: ${progress.error ?? '未知错误'}`)
      return
    }

    if (progress.phase === 'completed') {
      const totalText = progress.totalBytes ? ` / ${formatBytes(progress.totalBytes)}` : ''
      setNotice(`${fileLabel} 导入完成，已复制 ${formatBytes(progress.bytesCopied)}${totalText}。`)
      return
    }

    if (progress.phase === 'progress' || progress.phase === 'started') {
      const totalText = progress.totalBytes ? ` / ${formatBytes(progress.totalBytes)}` : ''
      const copiedText = formatBytes(progress.bytesCopied)
      setNotice(`${fileLabel} 导入中，已复制 ${copiedText}${totalText}。大文件会持续一段时间。`)
    }
  }

  const reconcileStatus = useCallback((nextStatus: ManagedSourceStatus) => {
    applyStatus(nextStatus)
    setNotice((currentNotice) => {
      if (busyActionRef.current !== 'idle') {
        return currentNotice
      }
      return currentNotice.includes('导入中') ? '' : currentNotice
    })
  }, [])

  async function runAction<T>(
    nextBusyAction: BusyAction,
    errorPrefix: string,
    task: () => Promise<T>,
  ) {
    updateBusyAction(nextBusyAction)
    setError('')

    try {
      return await task()
    } catch (actionError) {
      setNotice('')
      setError(`${errorPrefix}: ${String(actionError)}`)
      return null
    } finally {
      updateBusyAction('idle')
    }
  }

  async function runImportAction<T>(
    nextBusyAction: Extract<BusyAction, 'importing-dir' | `file:${ManagedSourceFileId}`>,
    errorPrefix: string,
    task: (onProgress: ManagedSourceImportProgressCallback) => Promise<T>,
  ) {
    const importSessionId = nextImportSessionRef.current + 1
    nextImportSessionRef.current = importSessionId
    activeImportSessionRef.current = importSessionId

    const progressHandler: ManagedSourceImportProgressCallback = (progress) => {
      if (activeImportSessionRef.current !== importSessionId) {
        return
      }
      onImportProgress(progress)
    }

    try {
      return await runAction(nextBusyAction, errorPrefix, () => task(progressHandler))
    } finally {
      if (activeImportSessionRef.current === importSessionId) {
        activeImportSessionRef.current = null
      }
    }
  }

  useEffect(() => {
    let cancelled = false

    const load = async () => {
      updateBusyAction('loading')
      try {
        const nextStatus = await inspectManagedSourceStatus()
        if (cancelled) {
          return
        }
        reconcileStatus(nextStatus)
        setError('')
      } catch (loadError) {
        if (!cancelled) {
          setError(`读取数据管理状态失败: ${String(loadError)}`)
        }
      } finally {
        if (!cancelled) {
          updateBusyAction('idle')
        }
      }
    }

    void load()
    return () => {
      cancelled = true
    }
  }, [reconcileStatus, updateBusyAction])

  useEffect(() => {
    let cancelled = false

    const refreshWhenVisible = async () => {
      try {
        const nextStatus = await inspectManagedSourceStatus()
        if (cancelled) {
          return
        }
        reconcileStatus(nextStatus)
      } catch {
      }
    }

    const handleFocus = () => {
      void refreshWhenVisible()
    }
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        void refreshWhenVisible()
      }
    }

    window.addEventListener('focus', handleFocus)
    document.addEventListener('visibilitychange', handleVisibilityChange)
    return () => {
      cancelled = true
      window.removeEventListener('focus', handleFocus)
      document.removeEventListener('visibilitychange', handleVisibilityChange)
    }
  }, [reconcileStatus])

  async function onRefresh() {
    const nextStatus = await runAction('loading', '刷新导入状态失败', () =>
      inspectManagedSourceStatus(),
    )
    if (!nextStatus) {
      return
    }

    reconcileStatus(nextStatus)
    setNotice('')
  }

  async function onImportDirectory() {
    const result = await runImportAction('importing-dir', '目录扫描导入失败', (progressHandler) =>
      importManagedSourceDirectory(undefined, progressHandler),
    )
    if (!result) {
      return
    }

    applyStatus(result.status)
    setNotice(buildDirectoryImportNotice(result))
  }

  async function onImportFile(fileId: ManagedSourceFileId) {
    const nextStatus = await runImportAction(`file:${fileId}`, '手动导入失败', (progressHandler) =>
      importManagedSourceFile(fileId, undefined, progressHandler),
    )
    if (!nextStatus) {
      return
    }

    applyStatus(nextStatus)
    const fileLabel = MANAGED_SOURCE_FILES.find((item) => item.id === fileId)?.label ?? fileId
    setNotice(`${fileLabel} 导入完成。`)
  }

  async function onClearImportedData() {
    const nextStatus = await runAction('clearing', '清空导入目录失败', () =>
      clearManagedSourceData(),
    )
    if (!nextStatus) {
      return
    }

    applyStatus(nextStatus)
    setNotice('当前应用数据目录下的导入文件已清空。')
  }

  async function onDeleteFile(fileId: ManagedSourceFileId) {
    const nextStatus = await runAction(`deleting-file:${fileId}`, '删除文件失败', () =>
      removeManagedSourceFile(fileId),
    )
    if (!nextStatus) {
      return
    }

    applyStatus(nextStatus)
    const fileLabel = MANAGED_SOURCE_FILES.find((item) => item.id === fileId)?.label ?? fileId
    setNotice(`${fileLabel} 已从当前应用数据目录删除。`)
  }

  async function onExportFile(fileId: ManagedSourceFileId) {
    const result = await runAction(`exporting-file:${fileId}`, '导出文件失败', () =>
      exportManagedSourceFile(fileId),
    )
    if (!result) {
      return
    }

    const fileLabel = MANAGED_SOURCE_FILES.find((item) => item.id === fileId)?.label ?? fileId
    setNotice(`${fileLabel} 已导出到 ${result.exportedPath}`)
  }

  async function onExportData() {
    const result = await runAction('exporting', '导出数据失败', () => exportManagedSourceDirectory())
    if (!result) {
      return
    }

    setNotice(`已导出 ${result.fileCount} 个文件到 ${result.exportedPath}`)
  }

  async function onExportCacheData() {
    const result = await runAction('exporting-cache', '导出缓存失败', () => exportManagedCacheData())
    if (!result) {
      return
    }

    setNotice(
      `已导出本地缓存到 ${result.exportedPath}，包含 localStorage ${result.localStorageCount} 项、sessionStorage ${result.sessionStorageCount} 项。`,
    )
  }

  async function onImportCacheData() {
    const result = await runAction('importing-cache', '导入缓存失败', () => importManagedCacheData())
    if (!result) {
      return
    }

    const refreshedStatus = await inspectManagedSourceStatus()
    applyStatus(refreshedStatus)
    setNotice(
      `已导入本地缓存 ${result.importedPath}，写入 localStorage ${result.localStorageCount} 项、sessionStorage ${result.sessionStorageCount} 项。`,
    )
  }

  return (
    <div className="settings-page">
      <section className="settings-card">
        <div className="settings-head">
          <div>
            <h2 className="settings-title">数据管理</h2>
            <p className="settings-subtitle">
              所有业务与计算文件统一落到系统 `AppData/source/`。目录导入会递归扫描，手动导入会校验文件名并写入固定目标路径，也支持导出当前应用数据目录，以及导入/导出本地缓存。移动端导出会改成系统保存单个备份文件，不再依赖文件夹选择器。
            </p>
          </div>

          <div className="settings-actions">
            <button className="settings-secondary-btn" type="button" onClick={() => void onRefresh()} disabled={isBusy}>
              {busyAction === 'loading' ? '刷新中...' : '刷新状态'}
            </button>
            <button className="settings-secondary-btn" type="button" onClick={() => void onExportData()} disabled={isBusy}>
              {busyAction === 'exporting' ? '导出中...' : (isMobileClient ? '导出当前目录 ZIP' : '一键导出当前目录')}
            </button>
            <button className="settings-secondary-btn" type="button" onClick={() => void onExportCacheData()} disabled={isBusy}>
              {busyAction === 'exporting-cache' ? '导出中...' : '导出缓存数据'}
            </button>
            <button className="settings-secondary-btn" type="button" onClick={() => void onImportCacheData()} disabled={isBusy}>
              {busyAction === 'importing-cache' ? '导入中...' : '导入缓存数据'}
            </button>
            <button className="settings-danger-btn" type="button" onClick={() => void onClearImportedData()} disabled={isBusy}>
              {busyAction === 'clearing' ? '清空中...' : '清空当前目录'}
            </button>
          </div>
        </div>

        <div className="settings-summary-grid">
          <div className="settings-summary-item">
            <span>当前状态</span>
            <strong>{status?.isReady ? '数据已齐备' : '仍有缺失文件'}</strong>
          </div>
          <div className="settings-summary-item">
            <span>已导入数量</span>
            <strong>
              {status ? `${importedCount} / ${MANAGED_SOURCE_FILES.length}` : '读取中...'}
            </strong>
          </div>
          <div className="settings-summary-item">
            <span>固定数据目录</span>
            <strong>{status?.sourceDir ?? '读取中...'}</strong>
          </div>
          <div className="settings-summary-item settings-summary-item-wide">
            <span>当前应用数据根路径</span>
            <strong title={status?.sourcePath ?? ''}>{status?.sourcePath ?? '读取中...'}</strong>
          </div>
          <div className="settings-summary-item">
            <span>最近导入时间</span>
            <strong>{formatImportTime(status?.importedAt ?? null)}</strong>
          </div>
        </div>

        <div className="settings-path-layout">
          <div className="settings-field settings-field-actions">
            <span>导入操作</span>
            <div className="settings-inline-actions">
              <button
                className="settings-primary-btn settings-primary-btn-alt"
                type="button"
                onClick={() => void onImportDirectory()}
                disabled={isBusy || !directoryImportSupported}
              >
                {busyAction === 'importing-dir' ? '扫描中...' : '扫描目录并导入'}
              </button>
            </div>
            <small>
              {directoryImportSupported
                ? '程序固定写入 `AppData/source/`，规则文件 `score_rule.toml`、指标配置 `ind.toml` 也都按这套平铺方式导入。'
                : '当前平台不支持文件夹选择，请使用下方逐个导入。'}
            </small>
          </div>
        </div>

        {notice ? <div className="settings-notice">{notice}</div> : null}
        {error ? <div className="settings-error">{error}</div> : null}
      </section>

      <section className="settings-card">
        <div className="settings-section-head">
          <h3 className="settings-subtitle-head">文件清单</h3>
          <p className="settings-section-note">电脑里能读取到的文件都可以手动导入，最终都会写到固定的 `AppData/source/`；每个文件也支持单独导出和删除。</p>
        </div>

        <div className="settings-file-list">
          {MANAGED_SOURCE_FILES.map((file) => {
            const itemStatus = status?.items.find((item) => item.id === file.id)
            const isFileBusy = busyAction === `file:${file.id}`
            const isDeleteBusy = busyAction === `deleting-file:${file.id}`
            const isExportBusy = busyAction === `exporting-file:${file.id}`
            return (
              <article key={file.id} className="settings-file-card">
                <div className="settings-file-row">
                  <div>
                    <h4>{file.label}</h4>
                    <p>{file.description}</p>
                  </div>
                  <span className={itemStatus?.isImported ? 'settings-badge ok' : 'settings-badge'}>
                    {itemStatus?.isImported ? '已导入' : '缺失'}
                  </span>
                </div>

                <div className="settings-file-meta">
                  <span>要求文件名</span>
                  <strong>{file.fileName}</strong>
                </div>
                <div className="settings-file-meta">
                  <span>标准源目录示例</span>
                  <strong>{file.expectedSourcePath}</strong>
                </div>
                <div className="settings-file-meta">
                  <span>目标路径</span>
                  <strong title={itemStatus?.targetPath ?? ''}>{itemStatus?.targetPath ?? '读取中...'}</strong>
                </div>

                <div className="settings-file-actions">
                  <button className="settings-primary-btn" type="button" onClick={() => void onImportFile(file.id)} disabled={isBusy}>
                    {isFileBusy ? '导入中...' : '选择文件导入'}
                  </button>
                  <button
                    className="settings-secondary-btn"
                    type="button"
                    onClick={() => void onExportFile(file.id)}
                    disabled={isBusy || !itemStatus?.isImported}
                  >
                    {isExportBusy ? '导出中...' : '导出文件'}
                  </button>
                  <button
                    className="settings-danger-btn"
                    type="button"
                    onClick={() => void onDeleteFile(file.id)}
                    disabled={isBusy || !itemStatus?.isImported}
                  >
                    {isDeleteBusy ? '删除中...' : '删除文件'}
                  </button>
                </div>
              </article>
            )
          })}
        </div>
      </section>

    </div>
  )
}
