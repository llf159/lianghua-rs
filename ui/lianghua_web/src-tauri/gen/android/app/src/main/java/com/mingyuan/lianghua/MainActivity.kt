package com.mingyuan.lianghua

import android.os.Bundle
import android.os.Build
import android.os.Environment
import android.content.Intent
import android.content.pm.PackageManager
import android.Manifest
import android.net.Uri
import android.provider.DocumentsContract
import android.provider.Settings
import java.io.File
import android.util.Log
import androidx.activity.enableEdgeToEdge

class MainActivity : TauriActivity() {
  private external fun initRustlsPlatformVerifier(): Boolean
  @Volatile private var warehousePickedDirectory: String? = null
  @Volatile private var warehousePickerActive = false

  fun startWarehouseDirectoryPicker() {
    warehousePickedDirectory = null
    warehousePickerActive = true
    runOnUiThread {
      try {
        startActivityForResult(Intent(Intent.ACTION_OPEN_DOCUMENT_TREE), 9174)
      } catch (_: android.content.ActivityNotFoundException) {
        warehousePickedDirectory = ""
        warehousePickerActive = false
      }
    }
  }

  fun pollWarehouseDirectoryPicker(): String? {
    if (warehousePickerActive) return null
    return warehousePickedDirectory ?: ""
  }

  override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
    super.onActivityResult(requestCode, resultCode, data)
    if (requestCode == 9174) {
      warehousePickedDirectory = if (resultCode == RESULT_OK) data?.data?.toString() ?: "" else ""
      warehousePickerActive = false
    }
  }

  fun hasWarehouseStorageAccess(): Boolean {
    return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
      Environment.isExternalStorageManager()
    } else {
      checkSelfPermission(Manifest.permission.WRITE_EXTERNAL_STORAGE) == PackageManager.PERMISSION_GRANTED
    }
  }

  fun requestWarehouseStorageAccess() {
    runOnUiThread {
      if (hasWarehouseStorageAccess()) return@runOnUiThread
      if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
        val intent = Intent(Settings.ACTION_MANAGE_APP_ALL_FILES_ACCESS_PERMISSION).apply {
          data = Uri.parse("package:$packageName")
        }
        try {
          startActivity(intent)
        } catch (_: android.content.ActivityNotFoundException) {
          startActivity(Intent(Settings.ACTION_MANAGE_ALL_FILES_ACCESS_PERMISSION))
        }
      } else {
        requestPermissions(arrayOf(Manifest.permission.WRITE_EXTERNAL_STORAGE), 1001)
      }
    }
  }

  fun resolveWarehouseDirectory(uriText: String): String? {
    val uri = Uri.parse(uriText)
    if (uri.scheme != "content" || uri.authority != "com.android.externalstorage.documents") return null
    val documentId = DocumentsContract.getTreeDocumentId(uri)
    if (!documentId.startsWith("primary:")) return null
    val storageRoot = Environment.getExternalStorageDirectory().canonicalFile
    val relativePath = documentId.removePrefix("primary:")
    val selected = File(storageRoot, relativePath).canonicalFile
    if (selected == storageRoot || !selected.path.startsWith(storageRoot.path + File.separator)) return null
    return selected.path
  }

  override fun onCreate(savedInstanceState: Bundle?) {
    enableEdgeToEdge()
    super.onCreate(savedInstanceState)

    try {
      if (!initRustlsPlatformVerifier()) {
        Log.w(TAG, "rustls platform verifier init returned false")
      }
    } catch (error: UnsatisfiedLinkError) {
      Log.e(TAG, "rustls platform verifier native init missing", error)
    } catch (error: Throwable) {
      Log.e(TAG, "rustls platform verifier init failed", error)
    }
  }

  companion object {
    private const val TAG = "MainActivity"
  }
}
