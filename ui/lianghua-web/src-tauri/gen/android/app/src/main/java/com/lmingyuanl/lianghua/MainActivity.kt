package com.lmingyuanl.lianghua

import android.os.Bundle
import androidx.activity.enableEdgeToEdge

class MainActivity : TauriActivity() {
  private external fun initRustlsPlatformVerifier(): Boolean

  override fun onCreate(savedInstanceState: Bundle?) {
    enableEdgeToEdge()
    if (!initRustlsPlatformVerifier()) {
      Logger.warn("MainActivity", "rustls-platform-verifier 初始化失败，HTTPS 请求可能不可用")
    }
    super.onCreate(savedInstanceState)
  }
}
