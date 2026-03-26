export const DEFAULT_DATE_OPTION = ''

export function normalizeTradeDates(values: string[]) {
  const unique = new Set<string>()
  values.forEach((value) => {
    const next = value.trim()
    if (next !== '') {
      unique.add(next)
    }
  })
  return [...unique].sort((left, right) => right.localeCompare(left))
}

export function pickDateValue(currentValue: string, options: string[]) {
  if (options.length === 0) {
    return DEFAULT_DATE_OPTION
  }
  return options.includes(currentValue) ? currentValue : options[0]
}

export function findFirstPopulatedString<Row extends Record<string, unknown>>(
  rows: Row[],
  key: keyof Row,
) {
  const matched = rows.find((row) => typeof row[key] === 'string' && String(row[key]).trim() !== '')
  const value = matched?.[key]
  return typeof value === 'string' && value.trim() !== '' ? value.trim() : null
}

export function normalizeDateValue(value: unknown) {
  if (typeof value !== 'string' && typeof value !== 'number') {
    return ''
  }

  const text = String(value).trim()
  if (text === '') {
    return ''
  }

  const isoMatch = text.match(/^(\d{4}-\d{2}-\d{2})/)
  if (isoMatch?.[1]) {
    return isoMatch[1]
  }

  const compactMatch = text.match(/^(\d{8})/)
  if (compactMatch?.[1]) {
    return compactMatch[1]
  }

  const digits = text.replace(/\D/g, '')
  if (digits.length >= 8) {
    return digits.slice(0, 8)
  }

  return text
}
