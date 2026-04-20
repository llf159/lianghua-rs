import { invoke } from '@tauri-apps/api/core'
import { listen, type UnlistenFn } from '@tauri-apps/api/event'
import { appDataDir, join } from '@tauri-apps/api/path'
import { open, save } from '@tauri-apps/plugin-dialog'
import { BaseDirectory, exists, mkdir, readDir, readTextFile, remove, writeTextFile } from '@tauri-apps/plugin-fs'
import {
  readStoredSourceImportTimestamp,
  writeStoredSourceImportTimestamp,
  writeStoredSourcePath,
} from '../shared/storage'

export const DEFAULT_MANAGED_SOURCE_DIR = 'source'

export const MANAGED_SOURCE_FILES = [
  {
    id: 'source-db',
    label: '原始行情库',
    description: '行情与指标原始库。',
    fileName: 'stock_data.db',
    expectedSourcePath: 'source/stock_data.db',
    targetRelativePathSuffix: 'stock_data.db',
    extensions: ['db'],
    scanPathHints: [] as string[],
  },
  {
    id: 'stock-list',
    label: '股票列表',
    description: '名称、市值和股票基础信息。',
    fileName: 'stock_list.csv',
    expectedSourcePath: 'source/stock_list.csv',
    targetRelativePathSuffix: 'stock_list.csv',
    extensions: ['csv'],
    scanPathHints: [] as string[],
  },
  {
    id: 'trade-calendar',
    label: '交易日历',
    description: '用于日期选择和交易日推导。',
    fileName: 'trade_calendar.csv',
    expectedSourcePath: 'source/trade_calendar.csv',
    targetRelativePathSuffix: 'trade_calendar.csv',
    extensions: ['csv'],
    scanPathHints: [] as string[],
  },
  {
    id: 'result-db',
    label: '评分结果库',
    description: '排名总览与个股详情使用的结果库。',
    fileName: 'scoring_result.db',
    expectedSourcePath: 'source/scoring_result.db',
    targetRelativePathSuffix: 'scoring_result.db',
    extensions: ['db'],
    scanPathHints: [] as string[],
  },
  {
    id: 'concept-performance-db',
    label: '概念表现库',
    description: '概念行情表现聚合结果库。',
    fileName: 'concept_performance.db',
    expectedSourcePath: 'source/concept_performance.db',
    targetRelativePathSuffix: 'concept_performance.db',
    extensions: ['db'],
    scanPathHints: ['concept'],
  },
  {
    id: 'score-rule',
    label: '规则文件',
    description: '个股详情里的规则说明和表达式来源。',
    fileName: 'score_rule.toml',
    expectedSourcePath: 'source/score_rule.toml',
    targetRelativePathSuffix: 'score_rule.toml',
    extensions: ['toml'],
    scanPathHints: ['rule'],
  },
  {
    id: 'indicator-config',
    label: '指标配置',
    description: '后端计算指标表达式使用的配置。',
    fileName: 'ind.toml',
    expectedSourcePath: 'source/ind.toml',
    targetRelativePathSuffix: 'ind.toml',
    extensions: ['toml'],
    scanPathHints: ['ind'],
  },
  {
    id: 'ths-concepts',
    label: '同花顺概念',
    description: '概念列展示使用的映射表。',
    fileName: 'stock_concepts.csv',
    expectedSourcePath: 'source/stock_concepts.csv',
    targetRelativePathSuffix: 'stock_concepts.csv',
    extensions: ['csv'],
    scanPathHints: [] as string[],
  },
] as const

export type ManagedSourceFileId = (typeof MANAGED_SOURCE_FILES)[number]['id']

export type ManagedSourceFileStatus = (typeof MANAGED_SOURCE_FILES)[number] & {
  isImported: boolean
  targetPath: string
}

export type ManagedSourceStatus = {
  sourceDir: string
  sourcePath: string
  importedAt: string | null
  isReady: boolean
  items: ManagedSourceFileStatus[]
}

export type ManagedSourceExportResult = {
  sourcePath: string
  exportedPath: string
  fileCount: number
}

export type ManagedSourceFileExportResult = {
  fileId: ManagedSourceFileId
  fileName: string
  sourcePath: string
  exportedPath: string
}

export type ManagedSourceDirectoryImportResult = {
  scannedPath: string
  importedFileIds: ManagedSourceFileId[]
  missingFileIds: ManagedSourceFileId[]
  status: ManagedSourceStatus
}

export type ManagedSourceZipImportResult = {
  sourcePath: string
  importedPath: string
  extractedFileCount: number
}

export type ManagedSourceCacheBackup = {
  version: 1
  exportedAt: string
  localStorage: Record<string, string>
  sessionStorage: Record<string, string>
}

export type ManagedSourceCacheExportResult = {
  exportedPath: string
  localStorageCount: number
  sessionStorageCount: number
}

export type ManagedSourceCacheImportResult = {
  importedPath: string
  localStorageCount: number
  sessionStorageCount: number
}

export type ManagedSourceImportProgress = {
  importId: string
  targetRelativePath: string
  phase: 'started' | 'progress' | 'completed' | 'failed'
  bytesCopied: number
  totalBytes: number | null
  error: string | null
}

export type ManagedSourceImportProgressCallback = (progress: ManagedSourceImportProgress) => void

export type ManagedSourceDbPreviewRow = {
  tsCode: string
  tradeDate: string
  adjType: string
  open: number | null
  high: number | null
  low: number | null
  close: number | null
  preClose: number | null
  pctChg: number | null
  vol: number | null
  amount: number | null
  tor: number | null
}

export type ManagedSourceDbPreviewResult = {
  sourcePath: string
  dbPath: string
  rowCount: number
  matchedRows: number
  minTradeDate: string | null
  maxTradeDate: string | null
  rows: ManagedSourceDbPreviewRow[]
}

export type ManagedSourceDatasetId =
  | 'stock-data-base'
  | 'stock-data-indicators'
  | 'score-summary'
  | 'rule-details'
  | 'scene-details'
  | 'concept-performance'
  | 'stock-list-csv'
  | 'trade-calendar-csv'
  | 'stock-concepts-csv'

export type ManagedSourceDatasetPreviewResult = {
  sourcePath: string
  targetPath: string
  datasetId: ManagedSourceDatasetId
  datasetLabel: string
  rowCount: number
  matchedRows: number
  columns: string[]
  rows: string[][]
}

const MANAGED_SOURCE_IMPORT_EVENT = 'managed-source-import'
const IMPORT_TERMINAL_EVENT_WAIT_MS = 1200

function isMobileClient() {
  if (typeof navigator === 'undefined') {
    return false
  }

  return /android|iphone|ipad|ipod/i.test(navigator.userAgent.toLowerCase())
}

function buildExportStamp() {
  return new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
}

export function isDirectoryImportSupported() {
  return !isMobileClient()
}

export async function allowImportPath(path: string, directory: boolean, recursive: boolean) {
  await invoke('allow_import_path', { path, directory, recursive })
}

function createImportId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
}

async function copyImportFileToAppData(
  sourcePath: string,
  targetRelativePath: string,
  onProgress?: ManagedSourceImportProgressCallback,
) {
  const importId = createImportId()
  let unlisten: UnlistenFn | null = null

  try {
    let resolveCompletion!: (progress: ManagedSourceImportProgress) => void
    let rejectCompletion!: (error: Error) => void
    const progressState: {
      lastProgress: ManagedSourceImportProgress | null
      terminalProgress: ManagedSourceImportProgress | null
    } = {
      lastProgress: null,
      terminalProgress: null,
    }
    const completionPromise = new Promise<ManagedSourceImportProgress>((resolve, reject) => {
      resolveCompletion = resolve
      rejectCompletion = (error) => reject(error)
    })
    void completionPromise.catch(() => {})

    unlisten = await listen<ManagedSourceImportProgress>(MANAGED_SOURCE_IMPORT_EVENT, (event) => {
      const payload = event.payload
      if (payload.importId !== importId || payload.targetRelativePath !== targetRelativePath) {
        return
      }

      progressState.lastProgress = payload
      onProgress?.(payload)

      if (payload.phase === 'completed') {
        progressState.terminalProgress = payload
        resolveCompletion(payload)
        return
      }

      if (payload.phase === 'failed') {
        progressState.terminalProgress = payload
        rejectCompletion(new Error(payload.error ?? `导入失败: ${targetRelativePath}`))
      }
    })

    await invoke('copy_import_file_to_appdata', { sourcePath, targetRelativePath, importId }).then(
      () => undefined,
      (error) => {
        throw new Error(String(error))
      },
    )

    if (progressState.terminalProgress?.phase === 'completed') {
      return
    }

    let terminalEventTimer: ReturnType<typeof setTimeout> | null = null
    const receivedTerminalEvent = await Promise.race([
      completionPromise.then(() => true),
      new Promise<boolean>((resolve) => {
        terminalEventTimer = setTimeout(() => resolve(false), IMPORT_TERMINAL_EVENT_WAIT_MS)
      }),
    ]).finally(() => {
      if (terminalEventTimer !== null) {
        clearTimeout(terminalEventTimer)
      }
    })
    if (receivedTerminalEvent) {
      return
    }

    if (
      progressState.lastProgress &&
      progressState.lastProgress.phase !== 'completed' &&
      progressState.lastProgress.phase !== 'failed'
    ) {
      onProgress?.({
        ...progressState.lastProgress,
        phase: 'completed',
        bytesCopied:
          progressState.lastProgress.totalBytes ?? progressState.lastProgress.bytesCopied,
        error: null,
      })
    }
  } finally {
    unlisten?.()
  }
}

function buildRelativePath(...parts: string[]) {
  return parts
    .flatMap((part) => part.split('/'))
    .map((part) => part.trim())
    .filter((part) => part !== '')
    .join('/')
}

function decodePathLike(value: string) {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

function extractFileName(pathLike: string) {
  const decoded = decodePathLike(pathLike).replace(/[?#].*$/, '')
  const slashSegment = decoded.split(/[\\/]/).pop() ?? decoded
  return slashSegment.split(':').pop()?.trim() ?? slashSegment.trim()
}

function scoreSourceCandidate(sourcePath: string, targetFile: (typeof MANAGED_SOURCE_FILES)[number]) {
  const fileName = extractFileName(sourcePath).toLowerCase()
  if (fileName !== targetFile.fileName.toLowerCase()) {
    return null
  }

  const decodedPath = decodePathLike(sourcePath).toLowerCase()
  let score = 100
  targetFile.scanPathHints.forEach((token) => {
    if (decodedPath.includes(token.toLowerCase())) {
      score += 20
    }
  })
  return score
}

async function resolveAbsoluteTargetPath(relativePath: string) {
  const basePath = await appDataDir()
  return join(basePath, ...relativePath.split('/'))
}

async function resolveManagedSourceRootPath(sourceDir: string) {
  return resolveAbsoluteTargetPath(sourceDir)
}

async function ensureTargetParentDir(relativePath: string) {
  const segments = relativePath.split('/')
  segments.pop()
  if (segments.length === 0) {
    return
  }
  await mkdir(segments.join('/'), { baseDir: BaseDirectory.AppData, recursive: true })
}

function getTargetRelativePath(targetFile: (typeof MANAGED_SOURCE_FILES)[number], sourceDir: string) {
  return buildRelativePath(sourceDir, targetFile.targetRelativePathSuffix)
}

async function copyManagedSourceFileToTarget(
  sourcePath: string,
  targetFile: (typeof MANAGED_SOURCE_FILES)[number],
  sourceDir: string,
  onProgress?: ManagedSourceImportProgressCallback,
) {
  const targetRelativePath = getTargetRelativePath(targetFile, sourceDir)
  await ensureTargetParentDir(targetRelativePath)
  await copyImportFileToAppData(sourcePath, targetRelativePath, onProgress)
}

async function findFilesByNameInDirectory(rootPath: string) {
  const queue = [rootPath]
  const visited = new Set<string>()
  const matches = new Map<ManagedSourceFileId, { sourcePath: string; score: number }>()

  while (queue.length > 0) {
    const currentPath = queue.shift()
    if (!currentPath || visited.has(currentPath)) {
      continue
    }
    visited.add(currentPath)

    const entries = await readDir(currentPath)
    for (const entry of entries) {
      const entryPath = await join(currentPath, entry.name)
      if (entry.isDirectory) {
        queue.push(entryPath)
        continue
      }
      if (!entry.isFile) {
        continue
      }

      MANAGED_SOURCE_FILES.forEach((targetFile) => {
        const score = scoreSourceCandidate(entryPath, targetFile)
        if (score === null) {
          return
        }
        const current = matches.get(targetFile.id)
        if (!current || score > current.score) {
          matches.set(targetFile.id, { sourcePath: entryPath, score })
        }
      })
    }
  }

  return matches
}

function findManagedSourceFile(fileId: ManagedSourceFileId) {
  return MANAGED_SOURCE_FILES.find((item) => item.id === fileId) ?? null
}

function snapshotStorage(storage: Storage | null | undefined) {
  if (!storage) {
    return {}
  }

  const out: Record<string, string> = {}
  for (let index = 0; index < storage.length; index += 1) {
    const key = storage.key(index)
    if (!key) {
      continue
    }
    const value = storage.getItem(key)
    if (value === null) {
      continue
    }
    out[key] = value
  }
  return out
}

function applyStorageSnapshot(storage: Storage | null | undefined, payload: Record<string, string>) {
  if (!storage) {
    return 0
  }

  Object.entries(payload).forEach(([key, value]) => {
    storage.setItem(key, value)
  })
  return Object.keys(payload).length
}

export async function ensureManagedSourcePath(_sourceDirInput?: string) {
  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  const sourcePath = await resolveManagedSourceRootPath(sourceDir)
  writeStoredSourcePath(sourcePath)
  return sourcePath
}

export async function updateManagedSourceDirectory(_sourceDirInput: string) {
  await ensureManagedSourcePath()
  return inspectManagedSourceStatus()
}

export async function inspectManagedSourceStatus(_sourceDirInput?: string): Promise<ManagedSourceStatus> {
  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  const sourcePath = await ensureManagedSourcePath(sourceDir)
  const items = await Promise.all(
    MANAGED_SOURCE_FILES.map(async (item) => {
      const targetRelativePath = getTargetRelativePath(item, sourceDir)
      return {
        ...item,
        isImported: await exists(targetRelativePath, { baseDir: BaseDirectory.AppData }),
        targetPath: await resolveAbsoluteTargetPath(targetRelativePath),
      }
    }),
  )

  return {
    sourceDir,
    sourcePath,
    importedAt: readStoredSourceImportTimestamp(),
    isReady: items.every((item) => item.isImported),
    items,
  }
}

export async function previewManagedSourceStockData(
  _sourceDirInput?: string,
  filters?: {
    tradeDate?: string
    tsCode?: string
    limit?: number
  },
) {
  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  await ensureManagedSourcePath(sourceDir)

  return invoke<ManagedSourceDbPreviewResult>('preview_managed_source_stock_data', {
    sourceDir,
    tradeDate: filters?.tradeDate?.trim() ? filters.tradeDate.trim() : null,
    tsCode: filters?.tsCode?.trim() ? filters.tsCode.trim().toUpperCase() : null,
    limit: filters?.limit ?? null,
  })
}

export async function previewManagedSourceDataset(
  datasetId: ManagedSourceDatasetId,
  _sourceDirInput?: string,
  filters?: {
    tradeDate?: string
    tsCode?: string
    limit?: number
  },
) {
  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  await ensureManagedSourcePath(sourceDir)

  return invoke<ManagedSourceDatasetPreviewResult>('preview_managed_source_dataset', {
    sourceDir,
    datasetId,
    tradeDate: filters?.tradeDate?.trim() ? filters.tradeDate.trim() : null,
    tsCode: filters?.tsCode?.trim() ? filters.tsCode.trim().toUpperCase() : null,
    limit: filters?.limit ?? null,
  })
}

export async function importManagedSourceFile(
  fileId: ManagedSourceFileId,
  _sourceDirInput?: string,
  onProgress?: ManagedSourceImportProgressCallback,
) {
  const targetFile = findManagedSourceFile(fileId)
  if (!targetFile) {
    throw new Error(`未知导入项: ${fileId}`)
  }

  const picked = await open({
    multiple: false,
    directory: false,
    filters: [{ name: targetFile.label, extensions: [...targetFile.extensions] }],
  })

  if (!picked || Array.isArray(picked)) {
    return null
  }

  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  const pickedFileName = extractFileName(picked)
  if (pickedFileName.toLowerCase() !== targetFile.fileName.toLowerCase()) {
    throw new Error(`文件名不匹配，应为 ${targetFile.fileName}，当前选择的是 ${pickedFileName}`)
  }

  await ensureManagedSourcePath(sourceDir)
  await allowImportPath(picked, false, false)
  await copyManagedSourceFileToTarget(picked, targetFile, sourceDir, onProgress)
  writeStoredSourceImportTimestamp(new Date().toISOString())
  return inspectManagedSourceStatus(sourceDir)
}

export async function importManagedSourceDirectory(
  _sourceDirInput?: string,
  onProgress?: ManagedSourceImportProgressCallback,
) {
  const picked = await open({
    multiple: false,
    directory: true,
  })

  if (!picked || Array.isArray(picked)) {
    return null
  }

  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  await allowImportPath(picked, true, true)
  await ensureManagedSourcePath(sourceDir)

  const matchedFiles = await findFilesByNameInDirectory(picked)
  const importedFileIds: ManagedSourceFileId[] = []
  const missingFileIds: ManagedSourceFileId[] = []

  for (const targetFile of MANAGED_SOURCE_FILES) {
    const matched = matchedFiles.get(targetFile.id)
    if (!matched) {
      missingFileIds.push(targetFile.id)
      continue
    }
    await copyManagedSourceFileToTarget(matched.sourcePath, targetFile, sourceDir, onProgress)
    importedFileIds.push(targetFile.id)
  }

  if (importedFileIds.length > 0) {
    writeStoredSourceImportTimestamp(new Date().toISOString())
  }

  return {
    scannedPath: picked,
    importedFileIds,
    missingFileIds,
    status: await inspectManagedSourceStatus(sourceDir),
  } satisfies ManagedSourceDirectoryImportResult
}

export async function importManagedSourceZip(_sourceDirInput?: string) {
  const picked = await open({
    multiple: false,
    directory: false,
    filters: [{ name: 'ZIP', extensions: ['zip'] }],
  })

  if (!picked || Array.isArray(picked)) {
    return null
  }

  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  await ensureManagedSourcePath(sourceDir)
  await allowImportPath(picked, false, false)

  const result = await invoke<ManagedSourceZipImportResult>('import_managed_source_zip', {
    sourceDir,
    sourcePath: picked,
  })
  writeStoredSourceImportTimestamp(new Date().toISOString())
  return result
}

export async function clearManagedSourceData(_sourceDirInput?: string) {
  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  await ensureManagedSourcePath(sourceDir)
  if (await exists(sourceDir, { baseDir: BaseDirectory.AppData })) {
    await remove(sourceDir, { baseDir: BaseDirectory.AppData, recursive: true })
  }
  writeStoredSourceImportTimestamp('')
  return inspectManagedSourceStatus(sourceDir)
}

export async function removeManagedSourceFile(
  fileId: ManagedSourceFileId,
  _sourceDirInput?: string,
) {
  const targetFile = findManagedSourceFile(fileId)
  if (!targetFile) {
    throw new Error(`未知文件项: ${fileId}`)
  }

  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  await ensureManagedSourcePath(sourceDir)
  const targetRelativePath = getTargetRelativePath(targetFile, sourceDir)
  if (await exists(targetRelativePath, { baseDir: BaseDirectory.AppData })) {
    await remove(targetRelativePath, { baseDir: BaseDirectory.AppData })
  }

  return inspectManagedSourceStatus(sourceDir)
}

export async function exportManagedSourceDirectory(_sourceDirInput?: string) {
  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  await ensureManagedSourcePath(sourceDir)

  if (isMobileClient()) {
    const targetPath = await save({
      filters: [{ name: 'ZIP', extensions: ['zip'] }],
      defaultPath: `lianghua-source-${buildExportStamp()}.zip`,
    })

    if (!targetPath) {
      return null
    }

    return invoke<ManagedSourceExportResult>('export_managed_source_directory_mobile', {
      sourceDir,
      destinationFile: targetPath,
    })
  }

  const picked = await open({
    multiple: false,
    directory: true,
  })

  if (!picked || Array.isArray(picked)) {
    return null
  }

  return invoke<ManagedSourceExportResult>('export_managed_source_directory', {
    sourceDir,
    destinationDir: picked,
  })
}

export async function exportManagedSourceFile(
  fileId: ManagedSourceFileId,
  _sourceDirInput?: string,
) {
  const targetFile = findManagedSourceFile(fileId)
  if (!targetFile) {
    throw new Error(`未知文件项: ${fileId}`)
  }

  const sourceDir = DEFAULT_MANAGED_SOURCE_DIR
  await ensureManagedSourcePath(sourceDir)

  const targetRelativePath = getTargetRelativePath(targetFile, sourceDir)
  if (!(await exists(targetRelativePath, { baseDir: BaseDirectory.AppData }))) {
    throw new Error(`${targetFile.label} 当前还没导入，无法导出`)
  }

  const targetPath = await save({
    filters: [{ name: targetFile.label, extensions: [...targetFile.extensions] }],
    defaultPath: targetFile.fileName,
  })

  if (!targetPath) {
    return null
  }

  return invoke<ManagedSourceFileExportResult>('export_managed_source_file', {
    sourceDir,
    fileId,
    destinationFile: targetPath,
  })
}

export async function exportManagedCacheData() {
  const backup: ManagedSourceCacheBackup = {
    version: 1,
    exportedAt: new Date().toISOString(),
    localStorage: snapshotStorage(typeof window === 'undefined' ? null : window.localStorage),
    sessionStorage: snapshotStorage(typeof window === 'undefined' ? null : window.sessionStorage),
  }

  const targetPath = await save({
    filters: [{ name: 'JSON', extensions: ['json'] }],
    defaultPath: `lianghua-cache-${buildExportStamp()}.json`,
  })

  if (!targetPath) {
    return null
  }

  await writeTextFile(targetPath, JSON.stringify(backup, null, 2))
  return {
    exportedPath: targetPath,
    localStorageCount: Object.keys(backup.localStorage).length,
    sessionStorageCount: Object.keys(backup.sessionStorage).length,
  } satisfies ManagedSourceCacheExportResult
}

export async function importManagedCacheData() {
  const picked = await open({
    multiple: false,
    directory: false,
    filters: [{ name: 'JSON', extensions: ['json'] }],
  })

  if (!picked || Array.isArray(picked)) {
    return null
  }

  const rawText = await readTextFile(picked)
  let parsed: unknown
  try {
    parsed = JSON.parse(rawText)
  } catch {
    throw new Error('缓存备份文件不是合法 JSON')
  }

  if (
    !parsed ||
    typeof parsed !== 'object' ||
    Array.isArray(parsed) ||
    !('localStorage' in parsed) ||
    !('sessionStorage' in parsed)
  ) {
    throw new Error('缓存备份文件格式不正确')
  }

  const localStoragePayload =
    parsed.localStorage && typeof parsed.localStorage === 'object' && !Array.isArray(parsed.localStorage)
      ? Object.fromEntries(
          Object.entries(parsed.localStorage as Record<string, unknown>).filter(
            (entry): entry is [string, string] => typeof entry[1] === 'string',
          ),
        )
      : {}
  const sessionStoragePayload =
    parsed.sessionStorage && typeof parsed.sessionStorage === 'object' && !Array.isArray(parsed.sessionStorage)
      ? Object.fromEntries(
          Object.entries(parsed.sessionStorage as Record<string, unknown>).filter(
            (entry): entry is [string, string] => typeof entry[1] === 'string',
          ),
        )
      : {}

  const localStorageCount = applyStorageSnapshot(
    typeof window === 'undefined' ? null : window.localStorage,
    localStoragePayload,
  )
  const sessionStorageCount = applyStorageSnapshot(
    typeof window === 'undefined' ? null : window.sessionStorage,
    sessionStoragePayload,
  )

  return {
    importedPath: picked,
    localStorageCount,
    sessionStorageCount,
  } satisfies ManagedSourceCacheImportResult
}
