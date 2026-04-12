import { sanitizeCodeInput, splitTsCode } from './stockCode'

export type DetailsRouteInput = {
  tsCode: string
  tradeDate?: string | null
  sourcePath?: string | null
}

export function buildDetailsPath({ tsCode, tradeDate, sourcePath }: DetailsRouteInput) {
  const code = sanitizeCodeInput(splitTsCode(tsCode))
  if (code === '') {
    return '/details'
  }

  const params = new URLSearchParams()
  params.set('tsCode', code)

  if (tradeDate?.trim()) {
    params.set('tradeDate', tradeDate.trim())
  }

  if (sourcePath?.trim()) {
    params.set('sourcePath', sourcePath.trim())
  }

  return `/details?${params.toString()}`
}

export function buildLinkedDetailsPath({ tsCode, tradeDate, sourcePath }: DetailsRouteInput) {
  const code = sanitizeCodeInput(splitTsCode(tsCode))
  if (code === '') {
    return '/details-linked'
  }

  const params = new URLSearchParams()
  params.set('tsCode', code)

  if (tradeDate?.trim()) {
    params.set('tradeDate', tradeDate.trim())
  }

  if (sourcePath?.trim()) {
    params.set('sourcePath', sourcePath.trim())
  }

  return `/details-linked?${params.toString()}`
}
