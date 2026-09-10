import { useEffect, useState } from 'react'
import { getApiBase } from '@/api'

/**
 * The stain types actually present in the slide library.
 *
 * GET /stats derives these by parsing every indexed filename, so the list is
 * whatever is really on disk. Filters used to hardcode ['HE', 'IHC', 'Special'],
 * which offered "HE" long after the site moved to "HNE" — picking it returned
 * nothing, with no hint as to why.
 */
export function useStainTypes(): { stainTypes: string[]; loading: boolean } {
  const [stainTypes, setStainTypes] = useState<string[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    fetch(`${getApiBase()}/stats`)
      .then(res => (res.ok ? res.json() : null))
      .then(data => {
        if (cancelled || !data) return
        // Slides with no stain token parse to '' — a blank filter entry is
        // meaningless, so drop it.
        const types: string[] = (data.stain_types || [])
          .map((s: string) => (s || '').trim())
          .filter(Boolean)
        setStainTypes(Array.from(new Set<string>(types)).sort((a, b) => a.localeCompare(b)))
      })
      .catch(e => console.error('Failed to load stain types:', e))
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [])

  return { stainTypes, loading }
}
