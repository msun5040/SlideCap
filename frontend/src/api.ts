// Centralized API base URL - set dynamically by the launcher
let _apiBase = 'http://127.0.0.1:8000'

export function setApiBase(url: string) {
  _apiBase = url
}

export function getApiBase(): string {
  return _apiBase
}
