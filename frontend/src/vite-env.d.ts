/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_APP_MODE?: 'prod' | 'demo'
  readonly VITE_API_PORT?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
