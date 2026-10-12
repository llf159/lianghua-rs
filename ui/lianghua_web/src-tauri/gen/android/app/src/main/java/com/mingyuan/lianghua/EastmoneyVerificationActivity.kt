package com.mingyuan.lianghua

import android.content.Intent
import android.os.Bundle
import android.view.Gravity
import android.webkit.WebView
import android.widget.Button
import android.widget.FrameLayout
import androidx.activity.OnBackPressedCallback

class EastmoneyVerificationActivity : TauriActivity() {
  fun showVerificationWindow() {
    runOnUiThread {
      startActivity(Intent(this, EastmoneyVerificationActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
    }
  }

  override fun onCreate(savedInstanceState: Bundle?) {
    super.onCreate(savedInstanceState)
    val button = Button(this).apply {
      text = "返回监控，保留验证窗口"
      setOnClickListener { returnToMonitor() }
    }
    val layout = FrameLayout.LayoutParams(
      FrameLayout.LayoutParams.WRAP_CONTENT,
      FrameLayout.LayoutParams.WRAP_CONTENT,
      Gravity.BOTTOM or Gravity.END
    ).apply {
      val spacing = (24 * resources.displayMetrics.density).toInt()
      setMargins(spacing, spacing, spacing, spacing)
    }
    addContentView(button, layout)
  }

  override fun onWebViewCreate(webView: WebView) {
    super.onWebViewCreate(webView)
    onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
      override fun handleOnBackPressed() { returnToMonitor() }
    })
  }

  private fun returnToMonitor() {
    startActivity(Intent(this, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
  }
}
