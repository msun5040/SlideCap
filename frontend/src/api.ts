// Centralized API base URL - uses current hostname so it works over the network
let _apiBase = `http://${window.location.hostname}:8000`

export function setApiBase(url: string) {
  _apiBase = url
}

export function getApiBase(): string {
  return _apiBase
}

/**
 * Normalize accession numbers: BS-26-D12345 → BS26-D12345
 * Removes the dash between the letter prefix and year digits.
 */
export function normalizeAccession(acc: string): string {
  return acc.trim().toUpperCase().replace(/^([A-Z]{2})-(\d{2})-/, '$1$2-')
}
