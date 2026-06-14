import { invoke } from '@tauri-apps/api/core'

export type MarketDataCacheSettings = {
  enabled: boolean
}

export async function getMarketDataCacheSettings() {
  return invoke<MarketDataCacheSettings>('get_market_data_cache_settings')
}

export async function saveMarketDataCacheSettings(enabled: boolean) {
  return invoke<MarketDataCacheSettings>('save_market_data_cache_settings', { enabled })
}
