// Centralized API base URL - uses current hostname so it works over the network.
// Port can be overridden via VITE_API_PORT (e.g. demo build targets 8001).
const _apiPort = (import.meta.env.VITE_API_PORT as string | undefined) ?? '8000'
let _apiBase = `http://${window.location.hostname}:${_apiPort}`

export function setApiBase(url: string) {
  _apiBase = url
}

export function getApiBase(): string {
  return _apiBase
}

// ── App mode (prod vs demo) ─────────────────────────────────────
// Build-time default from VITE_APP_MODE; can be overridden at runtime
// by the value reported by the backend's /health endpoint, so a single
// build can be deployed against either a prod or a demo backend.

export type AppMode = 'prod' | 'demo'

const buildMode = (import.meta.env.VITE_APP_MODE as AppMode | undefined) ?? 'prod'
let _appMode: AppMode = buildMode === 'demo' ? 'demo' : 'prod'

export function getAppMode(): AppMode {
  return _appMode
}

export function setAppMode(mode: AppMode) {
  _appMode = mode
}

export function isDemo(): boolean {
  return _appMode === 'demo'
}

/**
 * Normalize accession numbers: BS-26-D12345 → BS26-D12345
 * Removes the dash between the letter prefix and year digits.
 */
export function normalizeAccession(acc: string): string {
  return acc.trim().toUpperCase().replace(/^([A-Z]{2})-(\d{2})-/, '$1$2-')
}

// ── Authentication ──────────────────────────────────────────────

const AUTH_TOKEN_KEY = 'slidecap_auth_token'

export function getAuthToken(): string | null {
  return localStorage.getItem(AUTH_TOKEN_KEY)
}

export function setAuthToken(token: string) {
  localStorage.setItem(AUTH_TOKEN_KEY, token)
}

export function clearAuthToken() {
  localStorage.removeItem(AUTH_TOKEN_KEY)
}

/**
 * Install a global fetch interceptor that adds the auth token to all API requests.
 * Call once on app startup. Avoids modifying every fetch() call across all components.
 */
export function installAuthInterceptor() {
  const originalFetch = window.fetch.bind(window)
  window.fetch = function (input: RequestInfo | URL, init?: RequestInit) {
    const url = typeof input === 'string' ? input : input instanceof Request ? input.url : input.toString()
    if (url.startsWith(_apiBase)) {
      const token = getAuthToken()
      if (token) {
        const headers = new Headers(init?.headers)
        if (!headers.has('Authorization')) {
          headers.set('Authorization', `Bearer ${token}`)
        }
        return originalFetch(input, { ...init, headers })
      }
    }
    return originalFetch(input, init)
  } as typeof window.fetch
}
