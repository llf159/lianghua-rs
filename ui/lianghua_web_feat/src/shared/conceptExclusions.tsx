import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren,
} from 'react'
import { readJsonStorage } from './storage'

const CONCEPT_EXCLUSION_STORAGE_KEY = 'lh_concept_exclusions'
const EXCLUDE_ST_BOARD_STORAGE_KEY = 'lh_exclude_st_board'
const CONCEPT_SPLIT_PATTERN = /[;,，；|、/\n]+/
const ST_BOARD_KEY = 'st'

type ConceptExclusionContextValue = {
  excludedConcepts: string[]
  setExcludedConcepts: (nextConcepts: string[]) => void
  excludeStBoard: boolean
  setExcludeStBoard: (nextValue: boolean) => void
}

const ConceptExclusionContext = createContext<ConceptExclusionContextValue | null>(null)

function toConceptMatchKey(value: string) {
  return value.trim().toLocaleLowerCase()
}

function normalizeConceptList(values: readonly string[]) {
  const seen = new Set<string>()
  const normalizedValues: string[] = []

  values.forEach((value) => {
    const normalizedValue = value.trim()
    if (!normalizedValue) {
      return
    }

    const matchKey = toConceptMatchKey(normalizedValue)
    if (seen.has(matchKey)) {
      return
    }

    seen.add(matchKey)
    normalizedValues.push(normalizedValue)
  })

  return normalizedValues
}

function readStoredConceptExclusions() {
  if (typeof window === 'undefined') {
    return []
  }

  const storedValue = readJsonStorage<unknown>(window.localStorage, CONCEPT_EXCLUSION_STORAGE_KEY)
  if (!Array.isArray(storedValue)) {
    return []
  }

  return normalizeConceptList(storedValue.filter((item): item is string => typeof item === 'string'))
}

function writeStoredConceptExclusions(nextConcepts: readonly string[]) {
  if (typeof window === 'undefined') {
    return
  }

  const normalizedValues = normalizeConceptList(nextConcepts)
  if (normalizedValues.length === 0) {
    window.localStorage.removeItem(CONCEPT_EXCLUSION_STORAGE_KEY)
    return
  }

  window.localStorage.setItem(CONCEPT_EXCLUSION_STORAGE_KEY, JSON.stringify(normalizedValues))
}

function readStoredExcludeStBoard() {
  if (typeof window === 'undefined') {
    return false
  }

  const storedValue = readJsonStorage<unknown>(window.localStorage, EXCLUDE_ST_BOARD_STORAGE_KEY)
  return storedValue === true
}

function writeStoredExcludeStBoard(nextValue: boolean) {
  if (typeof window === 'undefined') {
    return
  }

  if (!nextValue) {
    window.localStorage.removeItem(EXCLUDE_ST_BOARD_STORAGE_KEY)
    return
  }

  window.localStorage.setItem(EXCLUDE_ST_BOARD_STORAGE_KEY, JSON.stringify(true))
}

export function splitConceptText(value: unknown) {
  if (typeof value !== 'string') {
    return []
  }

  const normalizedValue = value.trim()
  if (!normalizedValue) {
    return []
  }

  const parts = normalizedValue
    .split(CONCEPT_SPLIT_PATTERN)
    .map((item) => item.trim())
    .filter(Boolean)

  return parts.length > 0 ? normalizeConceptList(parts) : [normalizedValue]
}

export function parseConceptDraft(value: string) {
  return normalizeConceptList(value.split(CONCEPT_SPLIT_PATTERN))
}

export function formatConceptDraft(values: readonly string[]) {
  return normalizeConceptList(values).join('\n')
}

export function filterConceptItems(
  concepts: readonly string[],
  excludedConcepts: readonly string[],
) {
  const normalizedConcepts = normalizeConceptList(concepts)
  if (normalizedConcepts.length === 0 || excludedConcepts.length === 0) {
    return normalizedConcepts
  }

  const excludedSet = new Set(excludedConcepts.map(toConceptMatchKey))
  return normalizedConcepts.filter((item) => !excludedSet.has(toConceptMatchKey(item)))
}

export function filterBoardItems(boards: readonly string[], excludeStBoard: boolean) {
  const normalizedBoards = normalizeConceptList(boards)
  if (!excludeStBoard) {
    return normalizedBoards
  }

  return normalizedBoards.filter((item) => !isStBoard(item))
}

export function isStBoard(value: unknown) {
  if (typeof value !== 'string') {
    return false
  }
  return toConceptMatchKey(value) === ST_BOARD_KEY
}

export function formatConceptText(
  value: unknown,
  excludedConcepts: readonly string[],
  emptyText = '--',
) {
  const filteredItems = filterConceptItems(splitConceptText(value), excludedConcepts)
  return filteredItems.length > 0 ? filteredItems.join('、') : emptyText
}

export function ConceptExclusionProvider({ children }: PropsWithChildren) {
  const [excludedConcepts, setExcludedConceptState] = useState<string[]>(() => readStoredConceptExclusions())
  const [excludeStBoard, setExcludeStBoardState] = useState<boolean>(() => readStoredExcludeStBoard())

  useEffect(() => {
    const handleStorage = (event: StorageEvent) => {
      if (event.key === CONCEPT_EXCLUSION_STORAGE_KEY) {
        setExcludedConceptState(readStoredConceptExclusions())
      }
      if (event.key === EXCLUDE_ST_BOARD_STORAGE_KEY) {
        setExcludeStBoardState(readStoredExcludeStBoard())
      }
    }

    window.addEventListener('storage', handleStorage)
    return () => {
      window.removeEventListener('storage', handleStorage)
    }
  }, [])

  const contextValue = useMemo<ConceptExclusionContextValue>(
    () => ({
      excludedConcepts,
      excludeStBoard,
      setExcludedConcepts: (nextConcepts) => {
        const normalizedValues = normalizeConceptList(nextConcepts)
        setExcludedConceptState(normalizedValues)
        writeStoredConceptExclusions(normalizedValues)
      },
      setExcludeStBoard: (nextValue) => {
        const normalizedValue = Boolean(nextValue)
        setExcludeStBoardState(normalizedValue)
        writeStoredExcludeStBoard(normalizedValue)
      },
    }),
    [excludedConcepts, excludeStBoard],
  )

  return (
    <ConceptExclusionContext.Provider value={contextValue}>
      {children}
    </ConceptExclusionContext.Provider>
  )
}

export function useConceptExclusions() {
  const contextValue = useContext(ConceptExclusionContext)
  if (!contextValue) {
    throw new Error('useConceptExclusions must be used within ConceptExclusionProvider')
  }
  return contextValue
}
