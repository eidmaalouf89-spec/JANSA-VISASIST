import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    fs: {
      // Allow serving files from the output directory (symlinked in public/)
      allow: [
        '.',
        path.resolve(__dirname, '../output'),
      ],
    },
    // Proxy /api/* requests to Flask backend (port 5000)
    proxy: {
      '/api': {
        target: 'http://localhost:5000',
        changeOrigin: true,
      },
    },
  },
})
