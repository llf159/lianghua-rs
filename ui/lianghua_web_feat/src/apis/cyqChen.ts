import { invoke } from '@tauri-apps/api/core'
import { open, save } from '@tauri-apps/plugin-dialog'

export type CyqChenHolder = 'main' | 'retail'

export type CyqChenDirection = 'buy' | 'sell'

export type CyqChenStrategyDraft = {
  name: string
  holder: CyqChenHolder
  direction: CyqChenDirection
  when: string
  bias: number
}

export type CyqChenKlineRow = {
  tradeDate: string
  open?: number | null
  high?: number | null
  low?: number | null
  close?: number | null
  turnoverRate?: number | null
}

export type CyqChenBin = {
  index: number
  price: number
  priceLow: number
  priceHigh: number
  mainChip: number
  retailChip: number
  totalChip: number
}

export type CyqChenSnapshot = {
  tradeDate?: string | null
  close: number
  minPrice: number
  maxPrice: number
  mainTotal: number
  retailTotal: number
  totalChips: number
  bins: CyqChenBin[]
}

export type CyqChenSingleStockData = {
  resolvedTsCode: string
  startDate: string
  endDate: string
  outputStartDate?: string | null
  kline: CyqChenKlineRow[]
  snapshots: CyqChenSnapshot[]
}

export type CyqChenSingleStockRequest = {
  sourcePath: string
  tsCode: string
  startDate?: string | null
  endDate?: string | null
  warmupDays: number
  bucketPct: number
  strategies: CyqChenStrategyDraft[]
}

export type CyqChenStrategyBackupItem = {
  backupId: string
  fileName: string
  filePath: string
  modifiedAt?: string | null
  sizeBytes: number
}

export type CyqChenStrategyPageData = {
  filePath: string
  exists: boolean
  strategies: CyqChenStrategyDraft[]
  backups: CyqChenStrategyBackupItem[]
}

export type CyqChenStrategyFileExportResult = {
  exportedPath: string
}

export type CyqChenStrategyBackupDiffLine = {
  kind: 'context' | 'backup' | 'active' | 'omitted'
  backupLine: number | null
  activeLine: number | null
  text: string
}

export type CyqChenStrategyBackupDiff = {
  backupId: string
  backupLabel: string
  activeLabel: string
  changedLineCount: number
  lines: CyqChenStrategyBackupDiffLine[]
}

export async function runCyqChenSingleStockTest(request: CyqChenSingleStockRequest) {
  return invoke<CyqChenSingleStockData>('run_cyq_chen_single_stock_test', { request })
}

export async function getCyqChenStrategyPage(sourcePath: string) {
  return invoke<CyqChenStrategyPageData>('get_cyq_chen_strategy_page', { sourcePath })
}

export async function saveCyqChenStrategyFile(
  sourcePath: string,
  strategies: CyqChenStrategyDraft[],
) {
  return invoke<CyqChenStrategyPageData>('save_cyq_chen_strategy_file', {
    sourcePath,
    draft: { strategies },
  })
}

export async function checkCyqChenStrategyFileDraft(strategies: CyqChenStrategyDraft[]) {
  return invoke<string>('check_cyq_chen_strategy_file_draft', {
    draft: { strategies },
  })
}

export async function backupCyqChenStrategyFile(sourcePath: string) {
  return invoke<CyqChenStrategyPageData>('backup_cyq_chen_strategy_file', { sourcePath })
}

export async function importCyqChenStrategyBackup(sourcePath: string) {
  const picked = await open({
    multiple: false,
    directory: false,
    filters: [{ name: 'TOML', extensions: ['toml'] }],
  })

  if (!picked || Array.isArray(picked)) {
    return null
  }

  return invoke<CyqChenStrategyPageData>('import_cyq_chen_strategy_backup', {
    sourcePath,
    sourceFile: picked,
  })
}

export async function activateCyqChenStrategyBackup(sourcePath: string, backupId: string) {
  return invoke<CyqChenStrategyPageData>('activate_cyq_chen_strategy_backup', {
    sourcePath,
    backupId,
  })
}

export async function deleteCyqChenStrategyBackup(sourcePath: string, backupId: string) {
  return invoke<CyqChenStrategyPageData>('delete_cyq_chen_strategy_backup', {
    sourcePath,
    backupId,
  })
}

export async function exportCyqChenActiveStrategyFile(sourcePath: string) {
  const targetPath = await save({
    filters: [{ name: 'TOML', extensions: ['toml'] }],
    defaultPath: 'chip_change_rule.toml',
  })

  if (!targetPath) {
    return null
  }

  return invoke<CyqChenStrategyFileExportResult>('export_cyq_chen_active_strategy_file', {
    sourcePath,
    destinationFile: targetPath,
  })
}

export async function exportCyqChenStrategyBackupFile(
  sourcePath: string,
  backupId: string,
) {
  const targetPath = await save({
    filters: [{ name: 'TOML', extensions: ['toml'] }],
    defaultPath: backupId,
  })

  if (!targetPath) {
    return null
  }

  return invoke<CyqChenStrategyFileExportResult>('export_cyq_chen_strategy_backup_file', {
    sourcePath,
    backupId,
    destinationFile: targetPath,
  })
}

export async function getCyqChenStrategyBackupDiff(sourcePath: string, backupId: string) {
  return invoke<CyqChenStrategyBackupDiff>('get_cyq_chen_strategy_backup_diff', {
    sourcePath,
    backupId,
  })
}
