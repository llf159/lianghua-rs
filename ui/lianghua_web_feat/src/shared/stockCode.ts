export function sanitizeCodeInput(raw: string) {
  return raw.replace(/\D/g, '').slice(0, 6)
}

export function stdTsCode(code: string) {
  const normalized = code.trim().toUpperCase()
  if (normalized.includes('.')) {
    return normalized
  }
  if (normalized.startsWith('30') || normalized.startsWith('00')) {
    return `${normalized}.SZ`
  }
  if (normalized.startsWith('60') || normalized.startsWith('68')) {
    return `${normalized}.SH`
  }
  return `${normalized}.BJ`
}

export function splitTsCode(tsCode: string) {
  return tsCode.split('.')[0] ?? tsCode
}

export function normalizeTsCodeValue(raw: string) {
  const normalized = raw.trim().toUpperCase()
  if (normalized === '') {
    return ''
  }

  if (normalized.includes('.')) {
    return normalized
  }

  const digits = normalized.replace(/\D/g, '')
  if (digits.length === 6) {
    return stdTsCode(digits)
  }

  return normalized
}

export function normalizeTsCode(value: unknown) {
  if (typeof value !== 'string' && typeof value !== 'number') {
    return null
  }

  const raw = String(value).trim().toUpperCase()
  if (raw === '') {
    return null
  }

  if (/^\d{6}\.[A-Z]{2}$/.test(raw)) {
    return raw
  }

  const digits = raw.replace(/\D/g, '')
  if (digits.length !== 6) {
    return null
  }

  return stdTsCode(digits)
}
