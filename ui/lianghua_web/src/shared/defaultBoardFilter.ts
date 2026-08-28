const DEFAULT_BOARD_FILTER_STORAGE_KEY = 'lh_default_board_filter_v1'

export const DEFAULT_BOARD_FILTER_OPTIONS = [
  '全部',
  '主板',
  '科创板',
  '创业板',
  '北交所',
  'ST',
] as const

export type DefaultBoardFilter = (typeof DEFAULT_BOARD_FILTER_OPTIONS)[number]

export const DEFAULT_BOARD_FILTER_DEFAULT: DefaultBoardFilter = '全部'

export function normalizeDefaultBoardFilter(
  value: string | null | undefined,
): DefaultBoardFilter {
  return value && DEFAULT_BOARD_FILTER_OPTIONS.includes(value as DefaultBoardFilter)
    ? value as DefaultBoardFilter
    : DEFAULT_BOARD_FILTER_DEFAULT
}

export function readStoredDefaultBoardFilter() {
  if (typeof window === 'undefined') {
    return DEFAULT_BOARD_FILTER_DEFAULT
  }

  return normalizeDefaultBoardFilter(
    window.localStorage.getItem(DEFAULT_BOARD_FILTER_STORAGE_KEY),
  )
}

export function writeStoredDefaultBoardFilter(nextValue: DefaultBoardFilter) {
  const normalizedValue = normalizeDefaultBoardFilter(nextValue)
  if (typeof window !== 'undefined') {
    window.localStorage.setItem(DEFAULT_BOARD_FILTER_STORAGE_KEY, normalizedValue)
  }
  return normalizedValue
}
