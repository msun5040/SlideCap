import { useEffect, useState } from 'react'
import { getApiBase } from '@/api'

/**
 * Patch crops from /slides/{hash}/region.jpeg, as object URLs.
 *
 * The endpoint needs auth and an <img src> can't carry a bearer token — it
 * bypasses the fetch interceptor — so crops are fetched as blobs and handed to
 * <img> as object URLs. One shared LRU serves the workspace's expanded patch and
 * the composition tile gallery alike. URLs are revoked only on eviction, never
 * while an <img> might still be reading one.
 */

export interface PatchRef {
  slide_hash: string
  x: number
  y: number
  size: number
}

const MAX_ENTRIES = 200
const cache = new Map<string, Promise<string>>()

function keyOf(p: PatchRef, out: number) {
  return `${p.slide_hash}/${p.x}/${p.y}/${p.size}/${out}`
}

export function fetchPatchUrl(p: PatchRef, out = 512): Promise<string> {
  const key = keyOf(p, out)
  const hit = cache.get(key)
  if (hit) {
    // Refresh recency.
    cache.delete(key); cache.set(key, hit)
    return hit
  }
  const url = `${getApiBase()}/slides/${p.slide_hash}/region.jpeg?x=${p.x}&y=${p.y}&size=${p.size}&out=${out}`
  const promise = fetch(url)
    .then(async res => {
      if (!res.ok) {
        const d = await res.json().catch(() => null)
        throw new Error(d?.detail || `Could not load patch image (${res.status})`)
      }
      return URL.createObjectURL(await res.blob())
    })
  promise.catch(() => cache.delete(key))  // don't cache failures
  cache.set(key, promise)
  while (cache.size > MAX_ENTRIES) {
    const oldest = cache.keys().next().value as string
    const dead = cache.get(oldest)
    cache.delete(oldest)
    dead?.then(u => URL.revokeObjectURL(u)).catch(() => {})
  }
  return promise
}

export function usePatchImage(p: PatchRef | null, out = 512): { url: string | null; error: string } {
  const [url, setUrl] = useState<string | null>(null)
  const [error, setError] = useState('')
  const key = p ? keyOf(p, out) : null
  useEffect(() => {
    if (!p) { setUrl(null); setError(''); return }
    let cancelled = false
    setError('')
    fetchPatchUrl(p, out)
      .then(u => { if (!cancelled) setUrl(u) })
      .catch(e => { if (!cancelled) { setError(e.message || 'Could not load patch'); setUrl(null) } })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key])
  return { url, error }
}
