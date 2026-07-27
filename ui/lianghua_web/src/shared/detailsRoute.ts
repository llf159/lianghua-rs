import { sanitizeCodeInput, splitTsCode } from './stockCode'

export type DetailsRouteInput = {
  tsCode: string
  tradeDate?: string | null
  intervalStartTradeDate?: string | null
  intervalEndTradeDate?: string | null
  sourcePath?: string | null
  autoRealtime?: boolean | null
}

export function buildLinkedDetailsPath({
  tsCode,
  tradeDate,
  intervalStartTradeDate,
  intervalEndTradeDate,
  sourcePath,
  autoRealtime,
}: DetailsRouteInput) {
  const code = sanitizeCodeInput(splitTsCode(tsCode))
  if (code === '') {
    return '/details-linked'
  }

  const params = new URLSearchParams()
  params.set('tsCode', code)

  if (tradeDate?.trim()) {
    params.set('tradeDate', tradeDate.trim())
  }

  if (intervalStartTradeDate?.trim()) {
    params.set('intervalStartTradeDate', intervalStartTradeDate.trim())
  }

  if (intervalEndTradeDate?.trim()) {
    params.set('intervalEndTradeDate', intervalEndTradeDate.trim())
  }

  if (sourcePath?.trim()) {
    params.set('sourcePath', sourcePath.trim())
  }

  if (autoRealtime) {
    params.set('autoRealtime', '1')
  }

  return `/details-linked?${params.toString()}`
}
