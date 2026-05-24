import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('/src/pages/desktop/')) {
            const pageName = id.split('/src/pages/desktop/')[1]?.split('.')[0]
            return pageName ? `page-${pageName}` : undefined
          }
          if (!id.includes('node_modules')) {
            return undefined
          }
          if (id.includes('react') || id.includes('scheduler')) {
            return 'vendor-react'
          }
          if (id.includes('@tauri-apps')) {
            return 'vendor-tauri'
          }
          return undefined
        },
      },
    },
  },
  server: {
    watch: {
      ignored: ['**/src-tauri/target/**', '**/target/**'],
    },
  },
})
