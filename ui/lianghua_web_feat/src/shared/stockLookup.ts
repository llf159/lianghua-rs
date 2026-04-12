import { sanitizeCodeInput, splitTsCode } from './stockCode'

export type StockLookupLike = {
  ts_code: string
  name: string
  cnspell?: string | null
}

export function normalizeLookupKeyword(raw: string) {
  return raw.trim().toUpperCase()
}

export function getLookupDigits(tsCode: string) {
  return sanitizeCodeInput(splitTsCode(tsCode))
}

function normalizeCnspell(value: string | null | undefined) {
  return value?.trim().toUpperCase() ?? ''
}

export function getStockLookupMatchScore(row: StockLookupLike, keyword: string) {
  const code = getLookupDigits(row.ts_code).toUpperCase()
  const tsCode = row.ts_code.trim().toUpperCase()
  const name = row.name.trim().toUpperCase()
  const cnspell = normalizeCnspell(row.cnspell)
  if (keyword === '') {
    return null
  }
  if (code === keyword || tsCode === keyword) {
    return 0
  }
  if (name === keyword) {
    return 1
  }
  if (cnspell !== '' && cnspell === keyword) {
    return 2
  }
  if (code.startsWith(keyword) || tsCode.startsWith(keyword)) {
    return 3
  }
  if (name.startsWith(keyword)) {
    return 4
  }
  if (cnspell !== '' && cnspell.startsWith(keyword)) {
    return 5
  }
  if (code.includes(keyword) || tsCode.includes(keyword)) {
    return 6
  }
  if (name.includes(keyword)) {
    return 7
  }
  if (cnspell !== '' && cnspell.includes(keyword)) {
    return 8
  }
  return null
}

export function buildStockLookupCandidates<T extends StockLookupLike>(
  rows: T[],
  rawKeyword: string,
  limit: number,
) {
  const keyword = normalizeLookupKeyword(rawKeyword)
  if (keyword === '') {
    return [] as T[]
  }

  return rows
    .flatMap((row) => {
      const score = getStockLookupMatchScore(row, keyword)
      return score === null ? [] : [{ row, score }]
    })
    .sort((left, right) => {
      if (left.score !== right.score) {
        return left.score - right.score
      }
      return left.row.ts_code.localeCompare(right.row.ts_code)
    })
    .slice(0, limit)
    .map((entry) => entry.row)
}

export function findExactStockLookupMatch<T extends StockLookupLike>(rows: T[], rawKeyword: string) {
  const keyword = normalizeLookupKeyword(rawKeyword)
  if (keyword === '') {
    return null
  }

  const matches = rows.filter((row) => {
    const code = getLookupDigits(row.ts_code).toUpperCase()
    const tsCode = row.ts_code.trim().toUpperCase()
    const name = row.name.trim().toUpperCase()
    const cnspell = normalizeCnspell(row.cnspell)
    return code === keyword || tsCode === keyword || name === keyword || (cnspell !== '' && cnspell === keyword)
  })

  return matches.length === 1 ? matches[0] : null
}
