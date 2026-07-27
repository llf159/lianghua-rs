export const SOURCE_PATH_KEY = 'lh_source_path'
export const SOURCE_IMPORTED_AT_KEY = 'lh_source_imported_at'
export const SOURCE_DIR_KEY = 'lh_source_dir'

export function readStoredSourcePath() {
  if (typeof window === 'undefined') {
    return ''
  }

  return window.localStorage.getItem(SOURCE_PATH_KEY) ?? ''
}

export function writeStoredSourcePath(value: string) {
  if (typeof window !== 'undefined') {
    window.localStorage.setItem(SOURCE_PATH_KEY, value)
  }
  return value
}

export function readStoredSourceImportTimestamp() {
  if (typeof window === 'undefined') {
    return null
  }

  const raw = window.localStorage.getItem(SOURCE_IMPORTED_AT_KEY)?.trim()
  return raw ? raw : null
}

export function writeStoredSourceImportTimestamp(value: string) {
  if (typeof window === 'undefined') {
    return value
  }

  if (value.trim() === '') {
    window.localStorage.removeItem(SOURCE_IMPORTED_AT_KEY)
    return ''
  }

  window.localStorage.setItem(SOURCE_IMPORTED_AT_KEY, value)
  return value
}

export function readJsonStorage<T>(storage: Storage | null | undefined, key: string) {
  if (!storage) {
    return null
  }

  try {
    const raw = storage.getItem(key)
    if (!raw) {
      return null
    }
    return JSON.parse(raw) as T
  } catch {
    return null
  }
}

export function writeJsonStorage(storage: Storage | null | undefined, key: string, value: unknown) {
  if (!storage) {
    return false
  }

  try {
    storage.setItem(key, JSON.stringify(value))
    return true
  } catch {
    return false
  }
}
