import { invoke } from '@tauri-apps/api/core'
import { open, save } from '@tauri-apps/plugin-dialog'
import {
  DEFAULT_MANAGED_SOURCE_DIR,
  allowImportPath as allowManagedImportPath,
  ensureManagedSourcePath,
  exportManagedSourceFile,
  type ManagedSourceFileExportResult,
} from './managedSource'

export type ManagedStrategyActiveFile = {
  fileName: string
  relativePath: string
  absolutePath: string
  exists: boolean
  modifiedAt: string | null
  sizeBytes: number
}

export type ManagedStrategyBackupItem = {
  backupId: string
  folderName: string
  relativePath: string
  absolutePath: string
  createdAt: string
  modifiedAt: string | null
  sizeBytes: number
  sourceKind: string
  sourceFileName: string | null
}

export type ManagedStrategyAssetsStatus = {
  sourcePath: string
  backupRootPath: string
  active: ManagedStrategyActiveFile
  backups: ManagedStrategyBackupItem[]
}

export type ManagedStrategyBundleExportResult = {
  exportedPath: string
  backupCount: number
  includesActiveStrategy: boolean
}

export async function getManagedStrategyAssetsStatus() {
  await ensureManagedSourcePath(DEFAULT_MANAGED_SOURCE_DIR)
  return invoke<ManagedStrategyAssetsStatus>('get_managed_strategy_assets_status', {
    sourceDir: DEFAULT_MANAGED_SOURCE_DIR,
  })
}

export async function importManagedStrategyBackup() {
  const picked = await open({
    multiple: false,
    directory: false,
    filters: [{ name: 'TOML', extensions: ['toml'] }],
  })

  if (!picked || Array.isArray(picked)) {
    return null
  }

  await allowManagedImportPath(picked, false, false)
  await ensureManagedSourcePath(DEFAULT_MANAGED_SOURCE_DIR)
  return invoke<ManagedStrategyBackupItem>('import_managed_strategy_backup', {
    sourceDir: DEFAULT_MANAGED_SOURCE_DIR,
    sourcePath: picked,
  })
}

export async function backupManagedActiveStrategy() {
  await ensureManagedSourcePath(DEFAULT_MANAGED_SOURCE_DIR)
  return invoke<ManagedStrategyBackupItem>('backup_managed_active_strategy', {
    sourceDir: DEFAULT_MANAGED_SOURCE_DIR,
  })
}

export async function activateManagedStrategyBackup(backupId: string) {
  await ensureManagedSourcePath(DEFAULT_MANAGED_SOURCE_DIR)
  return invoke<ManagedStrategyAssetsStatus>('activate_managed_strategy_backup', {
    sourceDir: DEFAULT_MANAGED_SOURCE_DIR,
    backupId,
  })
}

export async function deleteManagedStrategyBackup(backupId: string) {
  await ensureManagedSourcePath(DEFAULT_MANAGED_SOURCE_DIR)
  return invoke<ManagedStrategyAssetsStatus>('delete_managed_strategy_backup', {
    sourceDir: DEFAULT_MANAGED_SOURCE_DIR,
    backupId,
  })
}

export async function exportManagedStrategyBackupFile(backupId: string) {
  const targetPath = await save({
    filters: [{ name: 'TOML', extensions: ['toml'] }],
    defaultPath: `${backupId}-score_rule.toml`,
  })

  if (!targetPath) {
    return null
  }

  await ensureManagedSourcePath(DEFAULT_MANAGED_SOURCE_DIR)
  return invoke<ManagedSourceFileExportResult>('export_managed_strategy_backup_file', {
    sourceDir: DEFAULT_MANAGED_SOURCE_DIR,
    backupId,
    destinationFile: targetPath,
  })
}

export async function exportManagedStrategyBundle() {
  const targetPath = await save({
    filters: [{ name: 'ZIP', extensions: ['zip'] }],
    defaultPath: 'strategy-assets.zip',
  })

  if (!targetPath) {
    return null
  }

  await ensureManagedSourcePath(DEFAULT_MANAGED_SOURCE_DIR)
  return invoke<ManagedStrategyBundleExportResult>('export_managed_strategy_bundle', {
    sourceDir: DEFAULT_MANAGED_SOURCE_DIR,
    destinationFile: targetPath,
  })
}

export async function exportManagedActiveStrategy() {
  return exportManagedSourceFile('score-rule')
}
