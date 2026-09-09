import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { readFileSync } from 'fs'
import { fileURLToPath, URL } from 'url'

// Serve the dev server over https when SSL_CERTFILE/SSL_KEYFILE are set — the
// same pair the backend uses. The page and the API have to share a scheme:
// an https page calling http:// is mixed content and is blocked. See
// backend/app/config.py for why plain http costs us downloads and clipboard.
const { SSL_CERTFILE, SSL_KEYFILE } = process.env
const https =
  SSL_CERTFILE && SSL_KEYFILE
    ? { cert: readFileSync(SSL_CERTFILE), key: readFileSync(SSL_KEYFILE) }
    : undefined

export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: './',
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    // Multi-user deployment: reachable from other machines on the LAN.
    host: true,
    https,
  },
  build: {
    outDir: 'dist',
  },
})
