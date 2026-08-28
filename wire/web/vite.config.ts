import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Unlike advisor/web, this SPA talks DIRECTLY to Polywire's admin API from the browser (see
// src/api/client.ts) -- there is no dev-time proxy here and no /api rewriting. The admin URL and
// token are supplied at runtime via the Connect screen and stored in sessionStorage.
export default defineConfig({
  plugins: [react()],
})
