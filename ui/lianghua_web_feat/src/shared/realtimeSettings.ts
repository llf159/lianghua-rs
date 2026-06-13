export type RealtimeQuoteProvider = 'sina' | 'tencent'

const REALTIME_QUOTE_PROVIDER_STORAGE_KEY = 'lh_realtime_quote_provider_v1'
const DEFAULT_REALTIME_QUOTE_PROVIDER: RealtimeQuoteProvider = 'tencent'

export function normalizeRealtimeQuoteProvider(value: unknown): RealtimeQuoteProvider {
  return value === 'sina' ? 'sina' : DEFAULT_REALTIME_QUOTE_PROVIDER
}

export function getRealtimeQuoteProviderLabel(value: RealtimeQuoteProvider) {
  return value === 'tencent' ? '腾讯' : '新浪'
}

export function readStoredRealtimeQuoteProvider(): RealtimeQuoteProvider {
  if (typeof window === 'undefined') {
    return DEFAULT_REALTIME_QUOTE_PROVIDER
  }

  return normalizeRealtimeQuoteProvider(
    window.localStorage.getItem(REALTIME_QUOTE_PROVIDER_STORAGE_KEY),
  )
}

export function writeStoredRealtimeQuoteProvider(value: RealtimeQuoteProvider) {
  const normalized = normalizeRealtimeQuoteProvider(value)
  if (typeof window !== 'undefined') {
    window.localStorage.setItem(REALTIME_QUOTE_PROVIDER_STORAGE_KEY, normalized)
  }
  return normalized
}
