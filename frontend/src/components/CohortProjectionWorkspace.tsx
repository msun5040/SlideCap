import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Loader2, X } from 'lucide-react'
import { getApiBase } from '@/api'
import { SlideViewerOSD } from '@/components/SlideViewerOSD'
import { CohortScatter, type ScatterColors } from '@/components/CohortScatter'
import { parseProjection, pointAtSlideXY, pointPatch, type ProjectionData } from '@/lib/projection'

/**
 * Full-window workspace: the cohort projection on one side, the slide the
 * selected patch came from on the other, and the patch itself blown up.
 *
 * The interaction that matters: clicking a point in the plot switches the
 * viewer to that patch's slide, boxes the patch in place on the WSI, and
 * animates the patch image out of that box to full size — so you can always see
 * both what the tissue looks like and where in the slide it sits. Clicking
 * tissue in the WSI does the same in reverse.
 */

interface GroupScheme {
  id: number
  name: string
  groups: { id: number; name: string; color?: string | null; slide_hashes: string[] }[]
}

interface Props {
  projectionId: number
  cohortId: number
  title?: string
  onClose: () => void
}

// Fallback palette for groups saved without an explicit colour.
const FALLBACK_COLORS = [
  '#ef4444', '#3b82f6', '#22c55e', '#eab308', '#8b5cf6',
  '#ec4899', '#14b8a6', '#f97316', '#6366f1', '#84cc16',
]

type ColorMode = { kind: 'none' } | { kind: 'slide' } | { kind: 'scheme'; schemeId: number }

export function CohortProjectionWorkspace({ projectionId, cohortId, title, onClose }: Props) {
  const [data, setData] = useState<ProjectionData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [schemes, setSchemes] = useState<GroupScheme[]>([])
  const [colorMode, setColorMode] = useState<ColorMode>({ kind: 'none' })

  const [selectedIdx, setSelectedIdx] = useState<number | null>(null)
  const [patchOpen, setPatchOpen] = useState(false)
  const [patchOrigin, setPatchOrigin] = useState<DOMRect | null>(null)
  const viewerWrapRef = useRef<HTMLDivElement | null>(null)

  // ── Load the artifact ────────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false
    setLoading(true); setError('')
    fetch(`${getApiBase()}/projections/${projectionId}/points`)
      .then(async res => {
        if (!res.ok) {
          const d = await res.json().catch(() => null)
          throw new Error(d?.detail || `Could not load projection (${res.status})`)
        }
        return res.arrayBuffer()
      })
      .then(buf => { if (!cancelled) setData(parseProjection(buf)) })
      .catch(e => { if (!cancelled) setError(e.message || 'Could not load projection') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [projectionId])

  // Group schemes are fetched separately and joined in purely for colour — the
  // projection's geometry was computed without them.
  useEffect(() => {
    fetch(`${getApiBase()}/cohorts/${cohortId}/group-schemes`)
      .then(r => (r.ok ? r.json() : []))
      .then(setSchemes)
      .catch(e => console.error('Failed to load group schemes:', e))
  }, [cohortId])

  // ── Colouring ────────────────────────────────────────────────────────
  const colors: ScatterColors | null = useMemo(() => {
    if (!data) return null

    if (colorMode.kind === 'slide') {
      const slides = data.header.slides
      const palette = slides.map((_, i) => FALLBACK_COLORS[i % FALLBACK_COLORS.length])
      const index = new Uint8Array(data.pointCount)
      for (let i = 0; i < data.pointCount; i++) index[i] = data.slideIdx[i] % 255
      return { index, palette, unassigned: '#94a3b8' }
    }

    if (colorMode.kind === 'scheme') {
      const scheme = schemes.find(s => s.id === colorMode.schemeId)
      if (!scheme) return null
      // slide_hash -> group ordinal, then fan out to points via slideIdx.
      const groupOfSlide = new Map<string, number>()
      scheme.groups.forEach((g, gi) => g.slide_hashes.forEach(h => groupOfSlide.set(h, gi)))
      const perSlide = data.header.slides.map(s => {
        const gi = groupOfSlide.get(s.slide_hash)
        return gi === undefined ? 255 : gi
      })
      const index = new Uint8Array(data.pointCount)
      for (let i = 0; i < data.pointCount; i++) index[i] = perSlide[data.slideIdx[i]]
      return {
        index,
        palette: scheme.groups.map((g, i) => g.color || FALLBACK_COLORS[i % FALLBACK_COLORS.length]),
        unassigned: '#cbd5e1',
      }
    }
    return null
  }, [data, colorMode, schemes])

  const legend = useMemo(() => {
    if (!data) return null
    if (colorMode.kind === 'scheme') {
      const scheme = schemes.find(s => s.id === colorMode.schemeId)
      if (!scheme) return null
      const counts = new Map<number, number>()
      const groupOfSlide = new Map<string, number>()
      scheme.groups.forEach((g, gi) => g.slide_hashes.forEach(h => groupOfSlide.set(h, gi)))
      let unassigned = 0
      for (const s of data.header.slides) {
        const gi = groupOfSlide.get(s.slide_hash)
        if (gi === undefined) unassigned += s.n_patches
        else counts.set(gi, (counts.get(gi) || 0) + s.n_patches)
      }
      const entries = scheme.groups.map((g, gi) => ({
        label: g.name,
        color: g.color || FALLBACK_COLORS[gi % FALLBACK_COLORS.length],
        count: counts.get(gi) || 0,
      }))
      if (unassigned > 0) entries.push({ label: 'Unassigned', color: '#cbd5e1', count: unassigned })
      return entries
    }
    if (colorMode.kind === 'slide') {
      return data.header.slides.slice(0, 12).map((s, i) => ({
        label: s.display_name || s.slide_hash.slice(0, 8),
        color: FALLBACK_COLORS[i % FALLBACK_COLORS.length],
        count: s.n_patches,
      }))
    }
    return null
  }, [data, colorMode, schemes])

  // ── Selection ────────────────────────────────────────────────────────
  const selected = useMemo(() => {
    if (!data || selectedIdx == null || selectedIdx < 0) return null
    return pointPatch(data, selectedIdx)
  }, [data, selectedIdx])

  // The patch endpoint requires auth, and an <img src> can't carry a bearer
  // token — it bypasses the fetch interceptor entirely (OSD gets around this
  // with loadTilesWithAjax/ajaxHeaders). So fetch it properly and hand the <img>
  // an object URL. A small LRU keeps re-selecting a patch instant; entries are
  // revoked only on eviction, never while an <img> might still be reading one.
  const patchCache = useRef<Map<string, string>>(new Map())
  const [patchUrl, setPatchUrl] = useState<string | null>(null)
  const [patchError, setPatchError] = useState('')

  const patchKey = useMemo(() => {
    if (!selected?.slide) return null
    const { slide, slide_x, slide_y, size } = selected
    return `${slide.slide_hash}/${slide_x}/${slide_y}/${size}`
  }, [selected])

  useEffect(() => {
    if (!patchKey || !selected?.slide) { setPatchUrl(null); return }
    const cached = patchCache.current.get(patchKey)
    if (cached) { setPatchUrl(cached); setPatchError(''); return }

    let cancelled = false
    const { slide, slide_x, slide_y, size } = selected
    const url = `${getApiBase()}/slides/${slide.slide_hash}/region.jpeg`
      + `?x=${slide_x}&y=${slide_y}&size=${size}&out=512`
    setPatchError('')
    fetch(url)
      .then(async res => {
        if (!res.ok) {
          const d = await res.json().catch(() => null)
          throw new Error(d?.detail || `Could not load patch image (${res.status})`)
        }
        return res.blob()
      })
      .then(blob => {
        if (cancelled) return
        const obj = URL.createObjectURL(blob)
        const cache = patchCache.current
        cache.set(patchKey, obj)
        while (cache.size > 32) {
          const oldest = cache.keys().next().value as string
          const dead = cache.get(oldest)
          cache.delete(oldest)
          if (dead) URL.revokeObjectURL(dead)
        }
        setPatchUrl(obj)
      })
      .catch(e => { if (!cancelled) { setPatchError(e.message); setPatchUrl(null) } })
    return () => { cancelled = true }
  }, [patchKey, selected])

  // Release every cached blob when the workspace closes.
  useEffect(() => () => {
    patchCache.current.forEach(u => URL.revokeObjectURL(u))
    patchCache.current.clear()
  }, [])

  const selectPoint = useCallback((idx: number) => {
    setSelectedIdx(idx)
    // Capture the highlight box's on-screen rect so the patch can visibly grow
    // out of its place on the slide rather than just appearing.
    const box = viewerWrapRef.current?.querySelector('[data-patch-box="1"]') as HTMLElement | null
    setPatchOrigin(box?.getBoundingClientRect() ?? null)
    setPatchOpen(true)
  }, [])

  const onSlideClick = useCallback((x: number, y: number) => {
    if (!data || !selected) return
    const slideIndex = data.header.slides.findIndex(s => s.slide_hash === selected.slide.slide_hash)
    if (slideIndex < 0) return
    const idx = pointAtSlideXY(data, slideIndex, x, y)
    if (idx >= 0) selectPoint(idx)
  }, [data, selected, selectPoint])

  // Escape is shared with SlideViewerOSD, which registers its own window-level
  // handler calling whatever onClose it was given (SlideViewerOSD.tsx:270). Both
  // fire for one press and the order isn't ours to control, so record when we've
  // consumed an Escape to collapse the patch and have the viewer's close ignore
  // that same press. Without this, dismissing the patch also tore down the whole
  // workspace and lost your place in the plot.
  const patchOpenRef = useRef(false)
  const escConsumedAt = useRef(0)
  useEffect(() => { patchOpenRef.current = patchOpen }, [patchOpen])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return
      if (patchOpenRef.current) {
        patchOpenRef.current = false
        escConsumedAt.current = Date.now()
        setPatchOpen(false)
      } else {
        onClose()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  /** The embedded viewer's close: its X button should close the workspace, but
   *  not an Escape we already used to collapse the patch. */
  const viewerClose = useCallback(() => {
    if (Date.now() - escConsumedAt.current > 100) onClose()
  }, [onClose])

  // Default the viewer to the first slide so there's always something shown.
  const activeSlide = selected?.slide ?? data?.header.slides[0] ?? null

  return (
    <div className="fixed inset-0 z-100 flex flex-col bg-neutral-950 text-neutral-100">
      {/* Header */}
      <div className="flex shrink-0 items-center gap-3 border-b border-neutral-800 px-4 py-2">
        <span className="text-sm font-medium">{title || 'Cohort projection'}</span>
        {data && (
          <span className="text-[11px] text-neutral-400">
            {data.pointCount.toLocaleString()} patches · {data.header.slides.length} slides ·{' '}
            {data.header.method.toUpperCase()}
          </span>
        )}

        <div className="ml-auto flex items-center gap-2">
          <label className="text-[11px] text-neutral-400">Colour by</label>
          <select
            className="rounded border border-neutral-700 bg-neutral-900 px-2 py-1 text-[12px]"
            value={colorMode.kind === 'scheme' ? `scheme:${colorMode.schemeId}` : colorMode.kind}
            onChange={e => {
              const v = e.target.value
              if (v.startsWith('scheme:')) setColorMode({ kind: 'scheme', schemeId: Number(v.slice(7)) })
              else setColorMode({ kind: v as 'none' | 'slide' })
            }}
          >
            <option value="none">Nothing</option>
            <option value="slide">Slide</option>
            {schemes.map(s => (
              <option key={s.id} value={`scheme:${s.id}`}>{s.name}</option>
            ))}
          </select>
          <button onClick={onClose} className="rounded p-1 hover:bg-neutral-800" title="Close (Esc)">
            <X className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Body */}
      <div className="flex min-h-0 flex-1">
        {/* Plot */}
        <div className="relative min-w-0 flex-1 border-r border-neutral-800 bg-neutral-900">
          {loading && (
            <div className="flex h-full items-center justify-center gap-2 text-sm text-neutral-400">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading projection…
            </div>
          )}
          {error && (
            <div className="flex h-full items-center justify-center px-6 text-center text-sm text-red-400">
              {error}
            </div>
          )}
          {data && !loading && !error && (
            <>
              <CohortScatter
                data={data}
                colors={colors}
                highlightIdx={selectedIdx}
                onSelectPoint={selectPoint}
              />
              {legend && (
                <div className="absolute left-3 top-3 max-h-[45%] overflow-auto rounded border border-neutral-700 bg-neutral-900/90 p-2 text-[11px]">
                  {legend.map(e => (
                    <div key={e.label} className="flex items-center gap-2 py-0.5">
                      <span className="h-2.5 w-2.5 shrink-0 rounded-[2px]"
                            style={{ backgroundColor: e.color }} />
                      <span className="truncate">{e.label}</span>
                      <span className="ml-auto pl-3 tabular-nums text-neutral-400">
                        {e.count.toLocaleString()}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </div>

        {/* Slide + patch */}
        <div ref={viewerWrapRef} className="relative w-[45%] min-w-[320px] shrink-0">
          {activeSlide ? (
            <SlideViewerOSD
              key={activeSlide.slide_hash}
              slideHash={activeSlide.slide_hash}
              slideName={activeSlide.display_name || activeSlide.slide_hash.slice(0, 12)}
              embedded
              highlightPatch={selected
                ? { slide_x: selected.slide_x, slide_y: selected.slide_y, size: selected.size }
                : null}
              onImageClick={onSlideClick}
              onClose={viewerClose}
            />
          ) : (
            <div className="flex h-full items-center justify-center text-sm text-neutral-500">
              Select a point to view its slide
            </div>
          )}

          {/* The patch, expanded. Animates from the highlight box's rect on the
              slide to this panel, so it reads as coming out of its location. */}
          {patchOpen && selected && (
            <div
              // Above the embedded SlideViewerOSD, which renders its own
              // `absolute inset-0 z-100` inside this same container.
              className="absolute inset-0 z-[120] flex items-center justify-center bg-neutral-950/80 backdrop-blur-sm"
              onClick={() => setPatchOpen(false)}
            >
              <div
                className="flex flex-col items-center gap-2"
                style={patchOrigin ? {
                  animation: 'sc-patch-grow 220ms cubic-bezier(0.2, 0.8, 0.2, 1)',
                } : undefined}
                onClick={e => e.stopPropagation()}
              >
                {patchUrl ? (
                  <img
                    src={patchUrl}
                    alt="Selected patch"
                    className="max-h-[70vh] max-w-[90%] rounded border-2 border-red-500 shadow-2xl"
                  />
                ) : patchError ? (
                  <div className="max-w-sm rounded border border-red-500 bg-neutral-900 p-4 text-center text-[12px] text-red-300">
                    {patchError}
                  </div>
                ) : (
                  <div className="flex h-40 w-40 items-center justify-center rounded border border-neutral-700 bg-neutral-900">
                    <Loader2 className="h-5 w-5 animate-spin text-neutral-500" />
                  </div>
                )}
                <div className="rounded bg-neutral-900/90 px-2 py-1 text-center text-[11px] text-neutral-300">
                  <div className="font-medium">
                    {selected.slide.display_name || selected.slide.slide_hash.slice(0, 12)}
                  </div>
                  <div className="text-neutral-400">
                    patch at ({selected.slide_x.toLocaleString()}, {selected.slide_y.toLocaleString()})
                    · {selected.size}px · click outside or press Esc to collapse
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      <style>{`
        @keyframes sc-patch-grow {
          from { transform: scale(0.25); opacity: 0; }
          to   { transform: scale(1);    opacity: 1; }
        }
      `}</style>
    </div>
  )
}
