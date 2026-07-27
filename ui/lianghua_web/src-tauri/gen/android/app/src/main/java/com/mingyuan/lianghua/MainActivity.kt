package com.mingyuan.lianghua

import android.os.Bundle
import android.util.Log
import androidx.activity.enableEdgeToEdge

class MainActivity : TauriActivity() {
  private external fun initRustlsPlatformVerifier(): Boolean

  override fun onCreate(savedInstanceState: Bundle?) {
    enableEdgeToEdge()
    try {
      if (!initRustlsPlatformVerifier()) {
        Log.w(TAG, "rustls platform verifier init returned false")
      }
    } catch (error: UnsatisfiedLinkError) {
      Log.e(TAG, "rustls platform verifier native init missing", error)
    } catch (error: Throwable) {
      Log.e(TAG, "rustls platform verifier init failed", error)
    }

    super.onCreate(savedInstanceState)
  }

  companion object {
    private const val TAG = "MainActivity"
  }
}
