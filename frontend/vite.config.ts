import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// Backend runs on 127.0.0.1:8765 (python -m lindley); proxy API calls to it in dev.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:8765',
    },
  },
})
