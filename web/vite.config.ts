import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Dev-mode CORS avoidance: proxy /api to the real Java server (AdvisorHttpServer, default 8090)
// instead of adding CORS headers there. In production the SPA is served by that same process
// (see SpaResourceHandler pattern in Omnigate, not yet ported here), so this proxy is dev-only.
const backendTarget = process.env.POLYGRES_ADVISOR_DEV_BACKEND_URL ?? 'http://localhost:8090'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': { target: backendTarget, changeOrigin: true },
    },
  },
})
