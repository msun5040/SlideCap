import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Eye, EyeOff, Layers, List, Loader2, Play, Trash2, X } from 'lucide-react'
import { getApiBase } from '@/api'
import { SlideViewerOSD } from '@/components/SlideViewerOSD'
import { CohortScatter, type ScatterColors } from '@/components/CohortScatter'
import type { PatchMask } from '@/components/PatchClusterOverlay'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
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
 *
 * Clusterings are run from the control strip over the projection's reduced
 * matrix (server-side), and come back as a label per point. They colour the plot
 * like any other colour-by, and can be painted onto the slide as a patch mask.
 */

interface GroupScheme {
  id: number
  name: string
  groups: { id: number; name: string; color?: string | null; slide_hashes: string[] }[]
}

interface ProjectionInfo {
  id: number
  has_reduced?: boolean
  point_count?: number | null
}

interface ClusteringRow {
  id: number
  algorithm: Algorithm
  params: Record<string, unknown>
  status: string
  progress_pct: number
  progress_stage?: string | null
  error_message?: string | null
  n_clusters?: number | null
  n_noise?: number | null
  silhouette?: number | null
  approximate: boolean
  elapsed_seconds?: number | null
}

type Algorithm = 'kmeans' | 'hdbscan' | 'leiden' | 'agglomerative'

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

/** Distinct colours past the first ten: golden-angle hue steps. */
function clusterColor(i: number): string {
  if (i < FALLBACK_COLORS.length) return FALLBACK_COLORS[i]
  return `hsl(${Math.round((i * 137.508) % 360)} 65% 55%)`
}

const DIMMED = '#3f3f46'
const NOISE_COLOR = '#52525b'

// Timings measured on 1M patches x 50 PCA dims (8-core laptop), for a sense of
// scale; the server will differ.
const ALGORITHMS: { value: Algorithm; label: string; note: string }[] = [
  { value: 'kmeans', label: 'k-means',
    note: 'Fits every patch. Auto picks k by silhouette score (scored on a 10k sample). ~2s fixed k, ~15s auto per million patches.' },
  { value: 'hdbscan', label: 'HDBSCAN',
    note: 'Approximate at scale: fits a sample (default 50k patches), then assigns the rest to the nearest cluster, or noise if too far from any. ~40s per million patches.' },
  { value: 'leiden', label: 'Leiden',
    note: 'Fits every patch via a nearest-neighbour graph. Higher resolution gives more, smaller clusters. Slowest: ~3–4 min and ~3.5 GB RAM per million patches.' },
  { value: 'agglomerative', label: 'Agglomerative',
    note: 'Approximate at scale: Ward tree on a sample (default 15k patches), then nearest-cluster assignment for the rest. ~15s per million patches.' },
]
const ALG_LABEL: Record<Algorithm, string> = Object.fromEntries(
  ALGORITHMS.map(a => [a.value, a.label]),
) as Record<Algorithm, string>

type ColorMode =
  | { kind: 'none' }
  | { kind: 'slide' }
  | { kind: 'scheme'; schemeId: number }
  | { kind: 'cluster'; clusteringId: number }

function clusteringLabel(c: ClusteringRow): string {
  const k = c.n_clusters != null ? ` · ${c.n_clusters} cluster${c.n_clusters === 1 ? '' : 's'}` : ''
  // Imported runs carry a short variant label (e.g. "k = 8") so several k-means runs stay distinguishable.
  const variant = typeof c.params?.label === 'string' ? ` ${c.params.label}` : ''
  return `${ALG_LABEL[c.algorithm] ?? c.algorithm}${variant}${k} (#${c.id})`
}

function colorModeValue(m: ColorMode): string {
  if (m.kind === 'scheme') return `scheme:${m.schemeId}`
  if (m.kind === 'cluster') return `cluster:${m.clusteringId}`
  return m.kind
}

const inputCls = 'w-full rounded border border-neutral-700 bg-neutral-950 px-2 py-1 text-[12px] text-neutral-100 focus:border-neutral-500 focus:outline-none'

export function CohortProjectionWorkspace({ projectionId, cohortId, title, onClose }: Props) {
  const [data, setData] = useState<ProjectionData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [schemes, setSchemes] = useState<GroupScheme[]>([])
  const [colorMode, setColorModeRaw] = useState<ColorMode>({ kind: 'none' })
  const [focusIdx, setFocusIdx] = useState<number | null>(null)
  const [showLegend, setShowLegend] = useState(true)

  const [info, setInfo] = useState<ProjectionInfo | null>(null)
  const [clusterings, setClusterings] = useState<ClusteringRow[]>([])
  const [labels, setLabels] = useState<Map<number, Int16Array>>(new Map())
  const [labelsError, setLabelsError] = useState('')
  const [maskOn, setMaskOn] = useState(true)
  const [maskOpacity, setMaskOpacity] = useState(0.45)

  const [runOpen, setRunOpen] = useState(false)
  const [algorithm, setAlgorithm] = useState<Algorithm>('kmeans')
  const [kValue, setKValue] = useState('auto')
  const [kMax, setKMax] = useState('20')
  const [minClusterSize, setMinClusterSize] = useState('')
  const [fitSample, setFitSample] = useState('')
  const [resolution, setResolution] = useState('1.0')
  const [nNeighbors, setNNeighbors] = useState('15')
  const [starting, setStarting] = useState(false)
  const [runError, setRunError] = useState('')
  const [clusterFailure, setClusterFailure] = useState('')
  const autoSelectRef = useRef<number | null>(null)

  const [selectedIdx, setSelectedIdx] = useState<number | null>(null)
  const [patchOpen, setPatchOpen] = useState(false)
  const [patchOrigin, setPatchOrigin] = useState<DOMRect | null>(null)
  const viewerWrapRef = useRef<HTMLDivElement | null>(null)

  const setColorMode = useCallback((m: ColorMode) => {
    setColorModeRaw(m)
    setFocusIdx(null)
  }, [])

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

  // ── Clusterings: list, poll while busy, fetch labels on demand ───────
  const refreshClusterings = useCallback(async () => {
    try {
      const [rows, pinfo] = await Promise.all([
        fetch(`${getApiBase()}/projections/${projectionId}/clusterings`).then(r => (r.ok ? r.json() : null)),
        fetch(`${getApiBase()}/projections/${projectionId}`).then(r => (r.ok ? r.json() : null)),
      ])
      if (rows) setClusterings(rows)
      if (pinfo) setInfo(pinfo)
      return rows as ClusteringRow[] | null
    } catch (e) {
      console.error('Failed to load clusterings:', e)
      return null
    }
  }, [projectionId])

  useEffect(() => { refreshClusterings() }, [refreshClusterings])

  const busy = clusterings.some(c => c.status === 'pending' || c.status === 'running')
  useEffect(() => {
    if (!busy) return
    const t = setInterval(async () => {
      const rows = await refreshClusterings()
      // Colour by a run you just started as soon as it lands.
      const want = autoSelectRef.current
      const done = want != null ? rows?.find(c => c.id === want) : null
      if (done && done.status !== 'pending' && done.status !== 'running') {
        autoSelectRef.current = null
        if (done.status === 'completed') {
          setClusterFailure('')
          setColorMode({ kind: 'cluster', clusteringId: done.id })
        } else {
          // The popover is usually closed by now, so say it where it's visible.
          setClusterFailure(`${ALG_LABEL[done.algorithm] ?? done.algorithm} failed: ${done.error_message || 'unknown error'}`)
        }
      }
    }, 2000)
    return () => clearInterval(t)
  }, [busy, refreshClusterings, setColorMode])

  const activeClusterId = colorMode.kind === 'cluster' ? colorMode.clusteringId : null
  const activeClustering = clusterings.find(c => c.id === activeClusterId) ?? null

  useEffect(() => {
    if (activeClusterId == null || labels.has(activeClusterId) || !data) return
    let cancelled = false
    setLabelsError('')
    fetch(`${getApiBase()}/clusterings/${activeClusterId}/labels`)
      .then(async res => {
        if (!res.ok) {
          const d = await res.json().catch(() => null)
          throw new Error(d?.detail || `Could not load cluster labels (${res.status})`)
        }
        return res.arrayBuffer()
      })
      .then(buf => {
        if (cancelled) return
        const arr = new Int16Array(buf)
        // A label array that doesn't line up with the artifact would colour
        // plausibly and be entirely wrong — refuse it outright.
        if (arr.length !== data.pointCount) {
          throw new Error(`Cluster labels have ${arr.length.toLocaleString()} entries but the projection has ${data.pointCount.toLocaleString()} points.`)
        }
        setLabels(prev => new Map(prev).set(activeClusterId, arr))
      })
      .catch(e => { if (!cancelled) setLabelsError(e.message) })
    return () => { cancelled = true }
  }, [activeClusterId, labels, data])

  const startClustering = async () => {
    setStarting(true); setRunError('')
    const params: Record<string, unknown> = {}
    const num = (v: string) => (v.trim() === '' ? undefined : Number(v))
    if (algorithm === 'kmeans' || algorithm === 'agglomerative') {
      params.k = kValue.trim() === '' || kValue === 'auto' ? 'auto' : Number(kValue)
      if (params.k === 'auto' && num(kMax) !== undefined) params.k_max = num(kMax)
    }
    if (algorithm === 'hdbscan' && num(minClusterSize) !== undefined) params.min_cluster_size = num(minClusterSize)
    if ((algorithm === 'hdbscan' || algorithm === 'agglomerative') && num(fitSample) !== undefined) {
      params.fit_sample = num(fitSample)
    }
    if (algorithm === 'leiden') {
      if (num(resolution) !== undefined) params.resolution = num(resolution)
      if (num(nNeighbors) !== undefined) params.n_neighbors = num(nNeighbors)
    }
    if (Object.values(params).some(v => typeof v === 'number' && !Number.isFinite(v))) {
      setRunError('Parameters must be numbers.')
      setStarting(false)
      return
    }
    try {
      const res = await fetch(`${getApiBase()}/projections/${projectionId}/clusterings`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ algorithm, params }),
      })
      const d = await res.json().catch(() => null)
      if (!res.ok) throw new Error(d?.detail || `Could not start clustering (${res.status})`)
      autoSelectRef.current = d.id
      await refreshClusterings()
    } catch (e: any) {
      setRunError(e.message || 'Could not start clustering')
    } finally {
      setStarting(false)
    }
  }

  const deleteClustering = async (id: number) => {
    const res = await fetch(`${getApiBase()}/clusterings/${id}`, { method: 'DELETE' })
    if (!res.ok) return
    if (activeClusterId === id) setColorMode({ kind: 'none' })
    setLabels(prev => { const m = new Map(prev); m.delete(id); return m })
    refreshClusterings()
  }

  // ── Colouring ────────────────────────────────────────────────────────
  const baseColors: ScatterColors | null = useMemo(() => {
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

    if (colorMode.kind === 'cluster') {
      const lab = labels.get(colorMode.clusteringId)
      if (!lab) return null
      let maxLabel = -1
      const index = new Uint8Array(data.pointCount)
      for (let i = 0; i < data.pointCount; i++) {
        const l = lab[i]
        // The server caps results at 254 clusters, so 255 stays free for noise.
        index[i] = l < 0 ? 255 : l
        if (l > maxLabel) maxLabel = l
      }
      const palette = Array.from({ length: maxLabel + 1 }, (_, i) => clusterColor(i))
      return { index, palette, unassigned: NOISE_COLOR }
    }
    return null
  }, [data, colorMode, schemes, labels])

  // Legend focus is a palette swap: everything but the focused entry dims. No
  // per-point work, so it's instant even at a million points.
  const colors: ScatterColors | null = useMemo(() => {
    if (!baseColors || focusIdx == null) return baseColors
    return {
      index: baseColors.index,
      palette: baseColors.palette.map((c, i) => (i === focusIdx ? c : DIMMED)),
      unassigned: focusIdx === 255 ? baseColors.unassigned : DIMMED,
    }
  }, [baseColors, focusIdx])

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
        idx: gi,
        label: g.name,
        color: g.color || FALLBACK_COLORS[gi % FALLBACK_COLORS.length],
        count: counts.get(gi) || 0,
      }))
      if (unassigned > 0) entries.push({ idx: 255, label: 'Unassigned', color: '#cbd5e1', count: unassigned })
      return entries
    }
    if (colorMode.kind === 'slide') {
      return data.header.slides.slice(0, 12).map((s, i) => ({
        idx: i,
        label: s.display_name || s.slide_hash.slice(0, 8),
        color: FALLBACK_COLORS[i % FALLBACK_COLORS.length],
        count: s.n_patches,
      }))
    }
    if (colorMode.kind === 'cluster' && baseColors) {
      const counts = new Array(256).fill(0)
      const idx = baseColors.index
      for (let i = 0; i < idx.length; i++) counts[idx[i]]++
      const entries = baseColors.palette.map((color, i) => ({
        idx: i, label: `Cluster ${i + 1}`, color, count: counts[i],
      }))
      if (counts[255] > 0) entries.push({ idx: 255, label: 'Noise', color: NOISE_COLOR, count: counts[255] })
      return entries
    }
    return null
  }, [data, colorMode, schemes, baseColors])

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
  // workspace and lost your place in the plot. The run-clustering popover closes
  // itself on Escape via Radix's capture-phase document listener, which marks the
  // press consumed (onEscapeKeyDown below). A ref mirroring `runOpen` would not
  // work: React commits the close before this window listener runs.
  const patchOpenRef = useRef(false)
  const escConsumedAt = useRef(0)
  useEffect(() => { patchOpenRef.current = patchOpen }, [patchOpen])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return
      if (Date.now() - escConsumedAt.current < 100) return
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

  // ── Cluster mask for the slide in the viewer ─────────────────────────
  const patchMask: PatchMask | null = useMemo(() => {
    if (!data || !baseColors || colorMode.kind !== 'cluster' || !maskOn || !activeSlide) return null
    const { start, n_patches: n, patch_size } = activeSlide
    return {
      patchX: data.patchX.subarray(start, start + n),
      patchY: data.patchY.subarray(start, start + n),
      size: patch_size || 256,
      index: baseColors.index.subarray(start, start + n),
      // Focus shows only the focused cluster on tissue; noise is never painted
      // unless it's the focus, so unclustered tissue stays readable.
      palette: focusIdx == null
        ? baseColors.palette
        : baseColors.palette.map((c, i) => (i === focusIdx ? c : '')),
      unassigned: focusIdx === 255 ? NOISE_COLOR : null,
      opacity: maskOpacity,
    }
  }, [data, baseColors, colorMode, maskOn, activeSlide, focusIdx, maskOpacity])

  const running = clusterings.filter(c => c.status === 'pending' || c.status === 'running')
  const completed = clusterings.filter(c => c.status === 'completed')
  const algNote = ALGORITHMS.find(a => a.value === algorithm)?.note

  return (
    <div className="fixed inset-0 z-100 flex flex-col bg-neutral-950 text-neutral-100">
      {/* Header */}
      <div className="flex shrink-0 items-center gap-3 border-b border-neutral-800 px-4 py-2">
        <span className="text-sm font-medium">{title || 'Cohort projection'}</span>
        {data && (
          <span className="text-[11px] text-neutral-400">
            {data.pointCount.toLocaleString()} patches · {data.header.slides.length} slides ·{' '}
            {({ umap: 'UMAP', tsne: 't-SNE', pca: 'PCA' } as Record<string, string>)[data.header.method]
              ?? data.header.method.toUpperCase()}
            {typeof data.header.params?.fit_sample_n === 'number'
              && data.header.params.fit_sample_n < data.pointCount
              && ` · fit on ${data.header.params.fit_sample_n.toLocaleString()}, all patches placed`}
          </span>
        )}
        <button onClick={onClose} className="ml-auto rounded p-1 hover:bg-neutral-800" title="Close (Esc)">
          <X className="h-4 w-4" />
        </button>
      </div>

      {/* Control strip */}
      <div className="flex shrink-0 flex-wrap items-center gap-2 border-b border-neutral-800 bg-neutral-900/60 px-4 py-1.5 text-[12px]">
        <label className="text-[11px] text-neutral-400">Colour by</label>
        <select
          className="min-w-[180px] rounded border border-neutral-700 bg-neutral-900 px-2 py-1 text-[12px]"
          value={colorModeValue(colorMode)}
          onChange={e => {
            const v = e.target.value
            if (v.startsWith('scheme:')) setColorMode({ kind: 'scheme', schemeId: Number(v.slice(7)) })
            else if (v.startsWith('cluster:')) setColorMode({ kind: 'cluster', clusteringId: Number(v.slice(8)) })
            else setColorMode({ kind: v as 'none' | 'slide' })
          }}
        >
          <option value="none">Nothing</option>
          <option value="slide">Slide</option>
          {schemes.length > 0 && (
            <optgroup label="Label schemes">
              {schemes.map(s => <option key={s.id} value={`scheme:${s.id}`}>{s.name}</option>)}
            </optgroup>
          )}
          {completed.length > 0 && (
            <optgroup label="Clusterings">
              {completed.map(c => <option key={c.id} value={`cluster:${c.id}`}>{clusteringLabel(c)}</option>)}
            </optgroup>
          )}
        </select>

        <Popover open={runOpen} onOpenChange={setRunOpen}>
          <PopoverTrigger asChild>
            <button className="inline-flex items-center gap-1.5 rounded border border-neutral-700 px-2 py-1 hover:bg-neutral-800">
              <Layers className="h-3.5 w-3.5" /> Clustering…
            </button>
          </PopoverTrigger>
          <PopoverContent
            align="start"
            onEscapeKeyDown={() => { escConsumedAt.current = Date.now() }}
            // The popover portals to <body> with z-50; the workspace is a z-100
            // fixed layer, so force it above.
            className="!z-[130] w-[340px] border-neutral-700 bg-neutral-900 p-3 text-[12px] text-neutral-100"
          >
            <div className="mb-2 text-[13px] font-medium">Run clustering</div>
            <div className="space-y-2">
              <select className={inputCls} value={algorithm} onChange={e => setAlgorithm(e.target.value as Algorithm)}>
                {ALGORITHMS.map(a => <option key={a.value} value={a.value}>{a.label}</option>)}
              </select>

              {(algorithm === 'kmeans' || algorithm === 'agglomerative') && (
                <div className="grid grid-cols-2 gap-2">
                  <label className="space-y-0.5">
                    <span className="text-[11px] text-neutral-400">k (number or “auto”)</span>
                    <input className={inputCls} value={kValue} onChange={e => setKValue(e.target.value)} />
                  </label>
                  <label className="space-y-0.5">
                    <span className="text-[11px] text-neutral-400">Max k to try (auto)</span>
                    <input className={inputCls} value={kMax} disabled={kValue !== 'auto' && kValue.trim() !== ''}
                           onChange={e => setKMax(e.target.value)} />
                  </label>
                </div>
              )}
              {algorithm === 'hdbscan' && (
                <label className="block space-y-0.5">
                  <span className="text-[11px] text-neutral-400">Min cluster size (blank = 0.5% of sample)</span>
                  <input className={inputCls} value={minClusterSize} onChange={e => setMinClusterSize(e.target.value)} />
                </label>
              )}
              {(algorithm === 'hdbscan' || algorithm === 'agglomerative') && (
                <label className="block space-y-0.5">
                  <span className="text-[11px] text-neutral-400">
                    Fit sample (blank = {algorithm === 'hdbscan' ? '50,000' : '15,000; max 20,000'})
                  </span>
                  <input className={inputCls} value={fitSample} onChange={e => setFitSample(e.target.value)} />
                </label>
              )}
              {algorithm === 'leiden' && (
                <div className="grid grid-cols-2 gap-2">
                  <label className="space-y-0.5">
                    <span className="text-[11px] text-neutral-400">Resolution</span>
                    <input className={inputCls} value={resolution} onChange={e => setResolution(e.target.value)} />
                  </label>
                  <label className="space-y-0.5">
                    <span className="text-[11px] text-neutral-400">Neighbours</span>
                    <input className={inputCls} value={nNeighbors} onChange={e => setNNeighbors(e.target.value)} />
                  </label>
                </div>
              )}

              {algNote && <p className="text-[11px] leading-snug text-neutral-400">{algNote}</p>}
              {info && info.has_reduced === false && (
                <p className="text-[11px] leading-snug text-amber-400">
                  This projection predates saved PCA matrices, so the first clustering rebuilds it
                  from the feature files first — that adds roughly the PCA time of the original run.
                </p>
              )}
              {runError && <p className="text-[11px] text-red-400">{runError}</p>}

              <button
                onClick={startClustering}
                disabled={starting}
                className="inline-flex w-full items-center justify-center gap-1.5 rounded bg-neutral-100 px-2 py-1.5 font-medium text-neutral-900 hover:bg-white disabled:opacity-50"
              >
                {starting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                Run
              </button>
            </div>

            {clusterings.length > 0 && (
              <div className="mt-3 border-t border-neutral-800 pt-2">
                <div className="mb-1 text-[11px] text-neutral-400">
                  Runs on this projection — cluster numbers aren't comparable between runs
                </div>
                <div className="max-h-48 space-y-1 overflow-auto">
                  {clusterings.map(c => (
                    <div key={c.id} className="flex items-start gap-2 rounded px-1 py-0.5 hover:bg-neutral-800/60">
                      <div className="min-w-0 flex-1">
                        <button
                          className="truncate text-left disabled:cursor-default"
                          disabled={c.status !== 'completed'}
                          onClick={() => { setColorMode({ kind: 'cluster', clusteringId: c.id }); setRunOpen(false) }}
                        >
                          {clusteringLabel(c)}
                        </button>
                        <div className="text-[10px] text-neutral-500">
                          {c.status === 'completed' && (
                            <>
                              {c.silhouette != null && `silhouette ${c.silhouette.toFixed(2)} · `}
                              {c.n_noise ? `${c.n_noise.toLocaleString()} noise · ` : ''}
                              {c.approximate ? 'approximate · ' : ''}
                              {c.elapsed_seconds != null && `${Math.round(c.elapsed_seconds)}s`}
                            </>
                          )}
                          {(c.status === 'pending' || c.status === 'running') &&
                            `${c.progress_pct}% · ${c.progress_stage || c.status}`}
                          {c.status === 'failed' && <span className="text-red-400">{c.error_message || 'failed'}</span>}
                        </div>
                      </div>
                      {c.status !== 'pending' && c.status !== 'running' && (
                        <button onClick={() => deleteClustering(c.id)} title="Delete this clustering"
                                className="rounded p-0.5 text-neutral-500 hover:bg-neutral-800 hover:text-red-400">
                          <Trash2 className="h-3.5 w-3.5" />
                        </button>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </PopoverContent>
        </Popover>

        {running.map(c => (
          <span key={c.id} className="inline-flex items-center gap-1 rounded border border-neutral-700 px-1.5 py-0.5 text-[11px] text-neutral-300"
                title={c.progress_stage || undefined}>
            <Loader2 className="h-3 w-3 animate-spin" />
            {ALG_LABEL[c.algorithm]} {c.progress_pct}%
          </span>
        ))}

        {activeClustering && (
          <span className="text-[11px] text-neutral-400">
            {activeClustering.silhouette != null && `silhouette ${activeClustering.silhouette.toFixed(2)}`}
            {activeClustering.approximate && (
              <span className="ml-2 rounded bg-amber-500/15 px-1 py-0.5 text-amber-400"
                    title="Fit on a sample of patches; the rest were assigned to the nearest cluster.">
                approximate
              </span>
            )}
          </span>
        )}
        {labelsError && <span className="text-[11px] text-red-400">{labelsError}</span>}
        {clusterFailure && (
          <span className="inline-flex max-w-[480px] items-center gap-1 text-[11px] text-red-400" title={clusterFailure}>
            <span className="truncate">{clusterFailure}</span>
            <button onClick={() => setClusterFailure('')} className="rounded p-0.5 hover:bg-neutral-800" title="Dismiss">
              <X className="h-3 w-3" />
            </button>
          </span>
        )}

        <div className="ml-auto flex items-center gap-2">
          <button
            onClick={() => setShowLegend(v => !v)}
            disabled={!legend}
            className={`inline-flex items-center gap-1 rounded border px-2 py-1 disabled:opacity-40 ${showLegend && legend ? 'border-neutral-500 bg-neutral-800' : 'border-neutral-700 hover:bg-neutral-800'}`}
            title="Toggle legend"
          >
            <List className="h-3.5 w-3.5" /> Legend
          </button>
          <button
            onClick={() => setMaskOn(v => !v)}
            disabled={colorMode.kind !== 'cluster'}
            className={`inline-flex items-center gap-1 rounded border px-2 py-1 disabled:opacity-40 ${maskOn && colorMode.kind === 'cluster' ? 'border-neutral-500 bg-neutral-800' : 'border-neutral-700 hover:bg-neutral-800'}`}
            title={colorMode.kind === 'cluster' ? 'Paint cluster colours onto the slide' : 'Colour by a clustering to show a slide mask'}
          >
            {maskOn && colorMode.kind === 'cluster' ? <Eye className="h-3.5 w-3.5" /> : <EyeOff className="h-3.5 w-3.5" />}
            Slide mask
          </button>
          <input
            type="range" min={0.1} max={0.9} step={0.05}
            value={maskOpacity}
            onChange={e => setMaskOpacity(Number(e.target.value))}
            disabled={colorMode.kind !== 'cluster' || !maskOn}
            className="w-24 accent-neutral-300 disabled:opacity-40"
            title={`Mask opacity ${Math.round(maskOpacity * 100)}%`}
          />
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
              {colorMode.kind === 'cluster' && !baseColors && !labelsError && (
                <div className="absolute left-3 top-3 flex items-center gap-2 rounded border border-neutral-700 bg-neutral-900/90 px-2 py-1 text-[11px] text-neutral-400">
                  <Loader2 className="h-3 w-3 animate-spin" /> Loading cluster labels…
                </div>
              )}
              {legend && showLegend && (
                <div className="absolute left-3 top-3 max-h-[45%] min-w-[160px] overflow-auto rounded border border-neutral-700 bg-neutral-900/90 p-2 text-[11px]">
                  {legend.map(e => {
                    const dim = focusIdx != null && focusIdx !== e.idx
                    return (
                      <button
                        key={`${e.idx}-${e.label}`}
                        onClick={() => setFocusIdx(f => (f === e.idx ? null : e.idx))}
                        className={`flex w-full items-center gap-2 rounded px-1 py-0.5 text-left hover:bg-neutral-800 ${dim ? 'opacity-40' : ''}`}
                        title={focusIdx === e.idx ? 'Show all' : 'Show only this'}
                      >
                        <span className="h-2.5 w-2.5 shrink-0 rounded-[2px]"
                              style={{ backgroundColor: e.color }} />
                        <span className="truncate">{e.label}</span>
                        <span className="ml-auto pl-3 tabular-nums text-neutral-400">
                          {e.count.toLocaleString()}
                        </span>
                      </button>
                    )
                  })}
                  {focusIdx != null && (
                    <button onClick={() => setFocusIdx(null)}
                            className="mt-1 w-full rounded border border-neutral-700 px-1 py-0.5 text-neutral-300 hover:bg-neutral-800">
                      Show all
                    </button>
                  )}
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
              patchMask={patchMask}
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
                    · {selected.size}px
                    {colorMode.kind === 'cluster' && selectedIdx != null && baseColors && (
                      <> · {baseColors.index[selectedIdx] === 255 ? 'noise' : `cluster ${baseColors.index[selectedIdx] + 1}`}</>
                    )}
                    {' '}· click outside or press Esc to collapse
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
