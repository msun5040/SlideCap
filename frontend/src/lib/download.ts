/**
 * Blob download helper.
 *
 * Revoking an object URL right after `a.click()` races the browser's own read of
 * the blob. On small files the read wins; on large ones (tens of MB) Chrome
 * shows the download parked at "49.6/49.6 MB" and never finalizes, because the
 * URL was pulled out from under it mid-transfer. Keeping the anchor in the DOM
 * and holding the URL alive for a while fixes it.
 */
export function saveBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.rel = 'noopener'
  a.style.display = 'none'
  document.body.appendChild(a)
  a.click()
  // Give the browser time to consume the blob before releasing it. Revoking
  // early is what stalls large downloads at 100%.
  window.setTimeout(() => {
    a.remove()
    URL.revokeObjectURL(url)
  }, 60_000)
}

/** Fetch a URL and save the response body as `filename`. Throws on non-2xx. */
export async function downloadUrl(url: string, filename: string, init?: RequestInit) {
  const res = await fetch(url, init)
  if (!res.ok) {
    const detail = await res.json().catch(() => null)
    throw new Error(detail?.detail || `Download failed (${res.status})`)
  }
  saveBlob(await res.blob(), filename)
}
