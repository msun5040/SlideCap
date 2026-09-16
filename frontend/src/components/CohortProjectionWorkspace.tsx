import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { BarChart3, Eye, EyeOff, Layers, List, Loader2, Play, SquareStack, Trash2, X } from 'lucide-react'
import { getApiBase } from '@/api'
import { SlideViewerOSD } from '@/components/SlideViewerOSD'
import { CohortScatter, type PointSet, type ScatterColors } from '@/components/CohortScatter'
import type { PatchMask } from '@/components/PatchClusterOverlay'
import { CompositionPanel } from '@/components/CompositionPanel'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { parseProjection, pointAtSlideXY, pointPatch, type ProjectionData } from '@/lib/projection'
import { usePatchImage } from '@/lib/patchImages'

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
 *
 * Overlays place another cohort onto this projection without refitting it
 * (server: services/projection_overlay.py): its patches are drawn on top of the
 * map, assigned to the same k-means clusters, and can be compared group vs group
 * in the Composition panel.
 */

interface GroupScheme {
  id: number
  name: string
  groups: { id: number; name: string; color?: string | null; slide_hashes: string[] }[]
}

interface ProjectionInfo {
  id: number
  has_reduced?: boolean
  has_pca?: boolean
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

interface OverlayRow {
  id: number
  cohort_id: number
  cohort_name?: string | null
  include_held_out: boolean
  slide_count: number
  point_count?: number | null
  status: string
  progress_pct: number
  progress_stage?: string | null
  error_message?: string | null
  excluded: { slide_hash: string; reason: string }[]
  warnings: string[]
  report: {
    pca?: { relative_error?: number; fit_rows?: number; validate_rows?: number }
    clusterings?: Record<string, { agreement: number; n_clusters: number; far_share: number }>
  }
}

/** Legend rows in cluster mode also carry reference / overlay shares when an overlay is shown. */
type LegendPct = { refPct?: number; overlayPct?: number }

type OverlayColorBy = 'cluster' | 'slide' | 'single' | `scheme:${number}`
type RefShow = 'color' | 'grey' | 'hidden'

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

  // ── Overlay (another cohort placed on this projection) ───────────────
  const [overlays, setOverlays] = useState<OverlayRow[]>([])
  const [activeOverlayId, setActiveOverlayId] = useState<number | null>(null)
  const [overlayData, setOverlayData] = useState<ProjectionData | null>(null)
  const [overlayError, setOverlayError] = useState('')
  const [overlaySchemes, setOverlaySchemes] = useState<GroupScheme[]>([])
  const [overlayLabels, setOverlayLabels] = useState<Map<number, { labels: Int16Array; far: Uint8Array }>>(new Map())
  const [overlayColorBy, setOverlayColorBy] = useState<OverlayColorBy>('cluster')
  const [refShow, setRefShow] = useState<RefShow>('grey')
  const [hideFar, setHideFar] = useState(false)
  // Overlay groups hidden from the map (group ordinal in the colouring scheme; 255 = not in a group).
  const [overlayHiddenGroups, setOverlayHiddenGroups] = useState<Set<number>>(new Set())
  const [overlayOpen, setOverlayOpen] = useState(false)
  const [allCohorts, setAllCohorts] = useState<{ id: number; name: string }[]>([])
  const [newOverlayCohort, setNewOverlayCohort] = useState('')
  const [newOverlayHeldOut, setNewOverlayHeldOut] = useState(false)
  const [overlayStarting, setOverlayStarting] = useState(false)
  const [overlayRunError, setOverlayRunError] = useState('')
  const [compositionOpen, setCompositionOpen] = useState(false)

  const [selectedIdx, setSelectedIdx] = useState<number | null>(null)
  const [selectedSet, setSelectedSet] = useState<PointSet>('base')
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

  // ── Overlays: list, poll, load the active one ────────────────────────
  const refreshOverlays = useCallback(async () => {
    try {
      const rows: OverlayRow[] | null = await fetch(`${getApiBase()}/projections/${projectionId}/overlays`)
        .then(r => (r.ok ? r.json() : null))
      if (rows) setOverlays(rows)
      return rows
    } catch {
      return null
    }
  }, [projectionId])

  useEffect(() => { refreshOverlays() }, [refreshOverlays])

  const overlayBusy = overlays.some(o => o.status === 'pending' || o.status === 'running')
  const pendingOverlayRef = useRef<number | null>(null)
  useEffect(() => {
    if (!overlayBusy) return
    const t = setInterval(async () => {
      const rows = await refreshOverlays()
      const want = pendingOverlayRef.current
      const done = want != null ? rows?.find(o => o.id === want) : null
      if (done && done.status === 'completed') {
        pendingOverlayRef.current = null
        setActiveOverlayId(done.id)
        refreshClusterings()  // projection info: PCA now recovered
      } else if (done && done.status === 'failed') {
        pendingOverlayRef.current = null
      }
    }, 2000)
    return () => clearInterval(t)
  }, [overlayBusy, refreshOverlays])

  useEffect(() => {
    if (!overlayOpen || allCohorts.length) return
    fetch(`${getApiBase()}/cohorts`).then(r => (r.ok ? r.json() : [])).then(setAllCohorts).catch(() => {})
  }, [overlayOpen, allCohorts.length])

  const activeOverlay = overlays.find(o => o.id === activeOverlayId) ?? null

  useEffect(() => { setOverlayHiddenGroups(new Set()) }, [activeOverlayId, overlayColorBy])

  const toggleOverlayGroup = (idx: number) => setOverlayHiddenGroups(prev => {
    const next = new Set(prev)
    if (next.has(idx)) next.delete(idx); else next.add(idx)
    return next
  })

  useEffect(() => {
    setOverlayData(null); setOverlayLabels(new Map()); setOverlaySchemes([]); setOverlayError('')
    if (selectedSet === 'overlay') { setSelectedIdx(null); setSelectedSet('base'); setPatchOpen(false) }
    if (activeOverlayId == null) return
    let cancelled = false
    fetch(`${getApiBase()}/overlays/${activeOverlayId}/points`)
      .then(async res => {
        if (!res.ok) {
          const d = await res.json().catch(() => null)
          throw new Error(d?.detail || `Could not load overlay (${res.status})`)
        }
        return res.arrayBuffer()
      })
      .then(buf => { if (!cancelled) setOverlayData(parseProjection(buf)) })
      .catch(e => { if (!cancelled) setOverlayError(e.message || 'Could not load overlay') })
    const ov = overlays.find(o => o.id === activeOverlayId)
    if (ov) {
      fetch(`${getApiBase()}/cohorts/${ov.cohort_id}/group-schemes`)
        .then(r => (r.ok ? r.json() : [])).then(rows => { if (!cancelled) setOverlaySchemes(rows) }).catch(() => {})
    }
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeOverlayId])

  const startOverlay = async () => {
    if (!newOverlayCohort) return
    setOverlayStarting(true); setOverlayRunError('')
    try {
      const res = await fetch(`${getApiBase()}/projections/${projectionId}/overlays`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cohort_id: Number(newOverlayCohort), include_held_out: newOverlayHeldOut }),
      })
      const d = await res.json().catch(() => null)
      if (!res.ok) throw new Error(d?.detail || `Could not start overlay (${res.status})`)
      pendingOverlayRef.current = d.id
      await refreshOverlays()
    } catch (e: any) {
      setOverlayRunError(e.message || 'Could not start overlay')
    } finally {
      setOverlayStarting(false)
    }
  }

  const deleteOverlay = async (id: number) => {
    const res = await fetch(`${getApiBase()}/overlays/${id}`, { method: 'DELETE' })
    if (!res.ok) return
    if (activeOverlayId === id) setActiveOverlayId(null)
    refreshOverlays()
  }

  // Overlay assignments exist only for k-means clusterings (they need centroids).
  const overlayClusterable = activeClustering?.algorithm === 'kmeans'

  useEffect(() => {
    if (!overlayData || activeOverlayId == null || activeClusterId == null || !overlayClusterable) return
    if (overlayLabels.has(activeClusterId)) return
    let cancelled = false
    const get = (kind: 'labels' | 'far') =>
      fetch(`${getApiBase()}/overlays/${activeOverlayId}/${kind}?clustering_id=${activeClusterId}`).then(async res => {
        if (!res.ok) {
          const d = await res.json().catch(() => null)
          throw new Error(d?.detail || `Could not load overlay ${kind} (${res.status})`)
        }
        return res.arrayBuffer()
      })
    Promise.all([get('labels'), get('far')])
      .then(([lb, fb]) => {
        if (cancelled) return
        const labels = new Int16Array(lb), far = new Uint8Array(fb)
        if (labels.length !== overlayData.pointCount || far.length !== overlayData.pointCount) {
          throw new Error('Overlay cluster labels don\'t line up with the overlay\'s points.')
        }
        setOverlayLabels(prev => new Map(prev).set(activeClusterId, { labels, far }))
        refreshOverlays()  // picks up agreement / far share in the report
      })
      .catch(e => { if (!cancelled) setOverlayError(e.message) })
    return () => { cancelled = true }
  }, [overlayData, activeOverlayId, activeClusterId, overlayClusterable, overlayLabels, refreshOverlays])

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

  // With an overlay shown, the reference can step back to grey so the overlay reads.
  const refColors: ScatterColors | null = useMemo(() => {
    if (!overlayData || refShow !== 'grey' || !data) return colors
    return { index: colors?.index ?? new Uint8Array(data.pointCount), palette: ['#3f3f46'], unassigned: '#3f3f46' }
  }, [overlayData, refShow, colors, data])

  /** Overlay points' cluster colours (also used for its slide mask). */
  const overlayClusterColors: ScatterColors | null = useMemo(() => {
    if (!overlayData || activeClusterId == null || !baseColors || colorMode.kind !== 'cluster') return null
    const a = overlayLabels.get(activeClusterId)
    if (!a) return null
    const index = new Uint8Array(overlayData.pointCount)
    for (let i = 0; i < overlayData.pointCount; i++) {
      index[i] = hideFar && a.far[i] ? 255 : (a.labels[i] < 0 ? 255 : a.labels[i])
    }
    return { index, palette: baseColors.palette, unassigned: hideFar ? '' : NOISE_COLOR }
  }, [overlayData, activeClusterId, baseColors, colorMode, overlayLabels, hideFar])

  const overlayColors: ScatterColors | null = useMemo(() => {
    if (!overlayData) return null
    const withFocus = (c: ScatterColors) => focusIdx == null || overlayColorBy !== 'cluster' ? c : {
      index: c.index,
      palette: c.palette.map((col, i) => (i === focusIdx ? col : DIMMED)),
      unassigned: c.unassigned && focusIdx !== 255 ? DIMMED : c.unassigned,
    }
    if (overlayColorBy === 'cluster') return overlayClusterColors ? withFocus(overlayClusterColors) : null
    if (overlayColorBy === 'slide') {
      const index = new Uint8Array(overlayData.pointCount)
      for (let i = 0; i < overlayData.pointCount; i++) index[i] = overlayData.slideIdx[i] % 255
      return { index, palette: overlayData.header.slides.map((_, i) => FALLBACK_COLORS[i % FALLBACK_COLORS.length]), unassigned: '#94a3b8' }
    }
    if (overlayColorBy.startsWith('scheme:')) {
      const scheme = overlaySchemes.find(s => s.id === Number(overlayColorBy.slice(7)))
      if (!scheme) return null
      const groupOfSlide = new Map<string, number>()
      scheme.groups.forEach((g, gi) => g.slide_hashes.forEach(h => groupOfSlide.set(h, gi)))
      const perSlide = overlayData.header.slides.map(s => groupOfSlide.get(s.slide_hash) ?? 255)
      const index = new Uint8Array(overlayData.pointCount)
      for (let i = 0; i < overlayData.pointCount; i++) index[i] = perSlide[overlayData.slideIdx[i]]
      // Hidden groups draw with an empty colour, which the scatter skips.
      return {
        index,
        palette: scheme.groups.map((g, i) => overlayHiddenGroups.has(i) ? '' : (g.color || FALLBACK_COLORS[i % FALLBACK_COLORS.length])),
        unassigned: overlayHiddenGroups.has(255) ? '' : '#71717a',
      }
    }
    return null
  }, [overlayData, overlayColorBy, overlayClusterColors, overlaySchemes, focusIdx, overlayHiddenGroups])

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
      const oc = new Array(256).fill(0)
      let oTotal = 0
      if (overlayClusterColors) {
        const oi = overlayClusterColors.index
        for (let i = 0; i < oi.length; i++) oc[oi[i]]++
        oTotal = oi.length
      }
      const refTotal = idx.length || 1
      const entries: { idx: number; label: string; color: string; count: number; refPct?: number; overlayPct?: number }[] =
        baseColors.palette.map((color, i) => ({
          idx: i, label: `Cluster ${i + 1}`, color, count: counts[i],
          refPct: overlayClusterColors ? (100 * counts[i]) / refTotal : undefined,
          overlayPct: overlayClusterColors ? (100 * oc[i]) / (oTotal || 1) : undefined,
        }))
      if (counts[255] > 0) entries.push({ idx: 255, label: 'Noise', color: NOISE_COLOR, count: counts[255] })
      return entries
    }
    return null
  }, [data, colorMode, schemes, baseColors, overlayClusterColors])

  /** Overlay groups legend when the overlay is coloured by one of its schemes. */
  const overlayLegend = useMemo(() => {
    if (!overlayData || !overlayColorBy.startsWith('scheme:')) return null
    const scheme = overlaySchemes.find(s => s.id === Number(overlayColorBy.slice(7)))
    if (!scheme) return null
    const groupOfSlide = new Map<string, number>()
    scheme.groups.forEach((g, gi) => g.slide_hashes.forEach(h => groupOfSlide.set(h, gi)))
    const counts = new Map<number, number>()
    let none = 0
    for (const s of overlayData.header.slides) {
      const gi = groupOfSlide.get(s.slide_hash)
      if (gi === undefined) none += s.n_patches
      else counts.set(gi, (counts.get(gi) || 0) + s.n_patches)
    }
    const rows = scheme.groups.map((g, gi) => ({ idx: gi, label: g.name, color: g.color || FALLBACK_COLORS[gi % FALLBACK_COLORS.length], count: counts.get(gi) || 0 }))
    if (none) rows.push({ idx: 255, label: 'Not in a group', color: '#71717a', count: none })
    return { name: scheme.name, rows }
  }, [overlayData, overlayColorBy, overlaySchemes])

  // ── Selection ────────────────────────────────────────────────────────
  const selectedData = selectedSet === 'overlay' ? overlayData : data
  const selected = useMemo(() => {
    if (!selectedData || selectedIdx == null || selectedIdx < 0 || selectedIdx >= selectedData.pointCount) return null
    return pointPatch(selectedData, selectedIdx)
  }, [selectedData, selectedIdx])

  // Auth-carrying blob fetch with a shared LRU (lib/patchImages.ts).
  const { url: patchUrl, error: patchError } = usePatchImage(
    selected?.slide
      ? { slide_hash: selected.slide.slide_hash, x: selected.slide_x, y: selected.slide_y, size: selected.size }
      : null,
  )

  const selectPoint = useCallback((idx: number, set: PointSet = 'base') => {
    setSelectedIdx(idx)
    setSelectedSet(set)
    // Capture the highlight box's on-screen rect so the patch can visibly grow
    // out of its place on the slide rather than just appearing.
    const box = viewerWrapRef.current?.querySelector('[data-patch-box="1"]') as HTMLElement | null
    setPatchOrigin(box?.getBoundingClientRect() ?? null)
    setPatchOpen(true)
  }, [])

  const onSlideClick = useCallback((x: number, y: number) => {
    if (!selectedData || !selected) return
    const slideIndex = selectedData.header.slides.findIndex(s => s.slide_hash === selected.slide.slide_hash)
    if (slideIndex < 0) return
    const idx = pointAtSlideXY(selectedData, slideIndex, x, y)
    if (idx >= 0) selectPoint(idx, selectedSet)
  }, [selectedData, selected, selectPoint, selectedSet])

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
  const compositionOpenRef = useRef(false)
  const escConsumedAt = useRef(0)
  useEffect(() => { patchOpenRef.current = patchOpen }, [patchOpen])
  useEffect(() => { compositionOpenRef.current = compositionOpen }, [compositionOpen])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return
      if (Date.now() - escConsumedAt.current < 100) return
      if (patchOpenRef.current) {
        patchOpenRef.current = false
        escConsumedAt.current = Date.now()
        setPatchOpen(false)
      } else if (compositionOpenRef.current) {
        compositionOpenRef.current = false
        escConsumedAt.current = Date.now()
        setCompositionOpen(false)
      } else {
        onClose()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  /** The embedded viewer's close: its X button should close the workspace, but
   *  not an Escape meant for the expanded patch or the Composition panel. The
   *  viewer's own Escape listener can run before ours, so check what's open
   *  directly as well as whether we just consumed the press. */
  const viewerClose = useCallback(() => {
    if (patchOpenRef.current || compositionOpenRef.current) return
    if (Date.now() - escConsumedAt.current > 100) onClose()
  }, [onClose])

  // Default the viewer to the first slide so there's always something shown.
  const activeSlide = selected?.slide ?? data?.header.slides[0] ?? null

  // ── Cluster mask for the slide in the viewer ─────────────────────────
  const maskSource = selected && selectedSet === 'overlay'
    ? { d: overlayData, c: overlayClusterColors }
    : { d: data, c: baseColors }
  const patchMask: PatchMask | null = useMemo(() => {
    const { d, c } = maskSource
    if (!d || !c || !baseColors || colorMode.kind !== 'cluster' || !maskOn || !activeSlide) return null
    const { start, n_patches: n, patch_size } = activeSlide
    return {
      patchX: d.patchX.subarray(start, start + n),
      patchY: d.patchY.subarray(start, start + n),
      size: patch_size || 256,
      index: c.index.subarray(start, start + n),
      // Focus shows only the focused cluster on tissue; noise is never painted
      // unless it's the focus, so unclustered tissue stays readable.
      palette: focusIdx == null
        ? baseColors.palette
        : baseColors.palette.map((c, i) => (i === focusIdx ? c : '')),
      unassigned: focusIdx === 255 ? NOISE_COLOR : null,
      opacity: maskOpacity,
    }
  }, [maskSource.d, maskSource.c, baseColors, colorMode, maskOn, activeSlide, focusIdx, maskOpacity])

  const running = clusterings.filter(c => c.status === 'pending' || c.status === 'running')
  const completed = clusterings.filter(c => c.status === 'completed')
  const completedKmeans = completed.filter(c => c.algorithm === 'kmeans')
  const selectedLabelInfo = (() => {
    if (colorMode.kind !== 'cluster' || selectedIdx == null) return null
    if (selectedSet === 'overlay') {
      const a = activeClusterId != null ? overlayLabels.get(activeClusterId) : null
      if (!a || selectedIdx >= a.labels.length) return null
      return { cluster: a.labels[selectedIdx], far: !!a.far[selectedIdx] }
    }
    if (!baseColors) return null
    const ci = baseColors.index[selectedIdx]
    return { cluster: ci === 255 ? -1 : ci, far: false }
  })()
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

        <span className="mx-1 h-4 w-px bg-neutral-800" />
        <Popover open={overlayOpen} onOpenChange={setOverlayOpen}>
          <PopoverTrigger asChild>
            <button className={`inline-flex items-center gap-1.5 rounded border px-2 py-1 hover:bg-neutral-800 ${activeOverlay ? 'border-orange-500/70 text-orange-200' : 'border-neutral-700'}`}>
              <SquareStack className="h-3.5 w-3.5" />
              {activeOverlay ? `Overlay: ${activeOverlay.cohort_name ?? `cohort ${activeOverlay.cohort_id}`}` : 'Overlay…'}
            </button>
          </PopoverTrigger>
          <PopoverContent
            align="start"
            onEscapeKeyDown={() => { escConsumedAt.current = Date.now() }}
            className="!z-[130] w-[380px] border-neutral-700 bg-neutral-900 p-3 text-[12px] text-neutral-100"
          >
            <div className="mb-1 text-[13px] font-medium">Overlay another cohort</div>
            <p className="mb-2 text-[11px] leading-snug text-neutral-400">
              Places the cohort's patches onto this map without refitting anything. Each patch goes through this
              projection's 50-d PCA (the step the {data?.header.method === 'tsne' ? 't-SNE' : data?.header.method === 'umap' ? 'UMAP' : 'map'} and k-means were computed
              from), joins the nearest k-means cluster centre there, and is drawn at its nearest reference patch's
              position on the map. Slides already in this projection are left out.
            </p>
            <div className="space-y-2">
              <select className={inputCls} value={newOverlayCohort} onChange={e => setNewOverlayCohort(e.target.value)}>
                <option value="">Choose a cohort…</option>
                {allCohorts.filter(c => c.id !== cohortId).map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
              <label className="flex items-center gap-1.5 text-[11px] text-neutral-300">
                <input type="checkbox" checked={newOverlayHeldOut} onChange={e => setNewOverlayHeldOut(e.target.checked)} />
                Include cases held out of analysis
              </label>
              {info && info.has_pca === false && (
                <p className="text-[11px] leading-snug text-amber-400">
                  The first overlay on this projection also recovers its PCA from the reference slides' UNI files
                  (a few minutes on a large cohort).
                </p>
              )}
              {overlayRunError && <p className="text-[11px] text-red-400">{overlayRunError}</p>}
              <button onClick={startOverlay} disabled={!newOverlayCohort || overlayStarting}
                      className="inline-flex w-full items-center justify-center gap-1.5 rounded bg-neutral-100 px-2 py-1.5 font-medium text-neutral-900 hover:bg-white disabled:opacity-50">
                {overlayStarting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                Overlay
              </button>
            </div>

            {overlays.length > 0 && (
              <div className="mt-3 border-t border-neutral-800 pt-2">
                <div className="mb-1 text-[11px] text-neutral-400">Overlays on this projection</div>
                <div className="max-h-72 space-y-1.5 overflow-auto">
                  {overlays.map(o => {
                    const isActive = o.id === activeOverlayId
                    const agree = activeClusterId != null ? o.report?.clusterings?.[String(activeClusterId)] : undefined
                    const reasons = o.excluded.reduce<Record<string, number>>((m, e) => { m[e.reason] = (m[e.reason] || 0) + 1; return m }, {})
                    return (
                      <div key={o.id} className={`rounded border px-2 py-1.5 ${isActive ? 'border-orange-500/60 bg-orange-500/5' : 'border-neutral-800'}`}>
                        <div className="flex items-center gap-2">
                          <button className="min-w-0 flex-1 truncate text-left font-medium disabled:cursor-default"
                                  disabled={o.status !== 'completed'}
                                  onClick={() => { setActiveOverlayId(isActive ? null : o.id); setOverlayOpen(false) }}>
                            {o.cohort_name ?? `Cohort ${o.cohort_id}`}
                          </button>
                          {o.status === 'completed' && (
                            <span className="text-[10px] text-neutral-400">{isActive ? 'showing · click to hide' : 'click to show'}</span>
                          )}
                          {o.status !== 'pending' && o.status !== 'running' && (
                            <button onClick={() => deleteOverlay(o.id)} title="Delete this overlay"
                                    className="rounded p-0.5 text-neutral-500 hover:bg-neutral-800 hover:text-red-400">
                              <Trash2 className="h-3.5 w-3.5" />
                            </button>
                          )}
                        </div>
                        <div className="text-[10px] leading-snug text-neutral-500">
                          {(o.status === 'pending' || o.status === 'running') && `${o.progress_pct}% · ${o.progress_stage || o.status}`}
                          {o.status === 'failed' && <span className="text-red-400">{o.error_message || 'failed'}</span>}
                          {o.status === 'completed' && (
                            <>
                              {o.slide_count} slides · {(o.point_count ?? 0).toLocaleString()} patches
                              {o.report?.pca?.relative_error != null && ` · PCA recovery error ${o.report.pca.relative_error.toExponential(1)}`}
                              {agree && ` · agreement ${(agree.agreement * 100).toFixed(1)}% · far ${(agree.far_share * 100).toFixed(1)}%`}
                            </>
                          )}
                        </div>
                        {Object.keys(reasons).length > 0 && (
                          <div className="text-[10px] text-neutral-500">
                            Left out: {Object.entries(reasons).map(([r, n]) => `${n} ${r}`).join(' · ')}
                          </div>
                        )}
                        {o.warnings.map(w => <div key={w} className="text-[10px] text-amber-400">⚠ {w}</div>)}
                      </div>
                    )
                  })}
                </div>
              </div>
            )}
          </PopoverContent>
        </Popover>

        {overlays.filter(o => o.status === 'pending' || o.status === 'running').map(o => (
          <span key={o.id} className="inline-flex items-center gap-1 rounded border border-neutral-700 px-1.5 py-0.5 text-[11px] text-neutral-300"
                title={o.progress_stage || undefined}>
            <Loader2 className="h-3 w-3 animate-spin" /> overlay {o.progress_pct}%
          </span>
        ))}

        {activeOverlay && (
          <>
            <select className="rounded border border-neutral-700 bg-neutral-900 px-1.5 py-1 text-[12px]"
                    value={refShow} onChange={e => setRefShow(e.target.value as RefShow)} title="How the reference points are drawn">
              <option value="grey">Reference: grey</option>
              <option value="color">Reference: coloured</option>
              <option value="hidden">Reference: hidden</option>
            </select>
            <select className="rounded border border-neutral-700 bg-neutral-900 px-1.5 py-1 text-[12px]"
                    value={overlayColorBy} onChange={e => setOverlayColorBy(e.target.value as OverlayColorBy)} title="How the overlay points are coloured">
              <option value="cluster">Overlay: by cluster</option>
              {overlaySchemes.map(s => <option key={s.id} value={`scheme:${s.id}`}>Overlay: {s.name}</option>)}
              <option value="slide">Overlay: by slide</option>
              <option value="single">Overlay: one colour</option>
            </select>
            <label className="inline-flex items-center gap-1 text-[11px] text-neutral-300"
                   title="Hide overlay patches farther from their cluster centre than 99% of the reference's own patches">
              <input type="checkbox" checked={hideFar} onChange={e => setHideFar(e.target.checked)} /> hide far
            </label>
            <button onClick={() => setCompositionOpen(true)} disabled={completedKmeans.length === 0}
                    className="inline-flex items-center gap-1.5 rounded border border-neutral-700 px-2 py-1 hover:bg-neutral-800 disabled:opacity-40"
                    title={completedKmeans.length ? 'Compare cluster composition between two groups of the overlay cohort' : 'Needs a k-means clustering on this projection'}>
              <BarChart3 className="h-3.5 w-3.5" /> Composition
            </button>
          </>
        )}
        {activeOverlay && colorMode.kind === 'cluster' && overlayColorBy === 'cluster' && !overlayClusterable && (
          <span className="text-[11px] text-amber-400">overlay needs a k-means clustering</span>
        )}
        {overlayError && <span className="text-[11px] text-red-400">{overlayError}</span>}

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
                colors={refColors}
                highlightIdx={selectedSet === 'base' ? selectedIdx : null}
                overlay={overlayData ? { data: overlayData, colors: overlayColors } : null}
                baseHidden={!!overlayData && refShow === 'hidden'}
                overlayHighlightIdx={selectedSet === 'overlay' ? selectedIdx : null}
                onSelectPoint={selectPoint}
              />
              {overlayLegend && showLegend && (
                <div className="absolute right-14 top-3 max-h-[45%] min-w-[150px] overflow-auto rounded border border-orange-500/40 bg-neutral-900/90 p-2 text-[11px]">
                  <div className="mb-1 text-[10px] uppercase tracking-wide text-orange-300">Overlay · {overlayLegend.name}</div>
                  {overlayLegend.rows.map(r => {
                    const hidden = overlayHiddenGroups.has(r.idx)
                    return (
                      <button key={r.idx}
                              onClick={() => toggleOverlayGroup(r.idx)}
                              className={`flex w-full items-center gap-2 rounded px-1 py-0.5 text-left hover:bg-neutral-800 ${hidden ? 'opacity-40' : ''}`}
                              title={hidden ? 'Show this group' : 'Hide this group'}>
                        <span className="h-2.5 w-2.5 shrink-0 rounded-[2px]" style={{ backgroundColor: r.color }} />
                        <span className={`truncate ${hidden ? 'line-through' : ''}`}>{r.label}</span>
                        <span className="ml-auto pl-3 tabular-nums text-neutral-400">{r.count.toLocaleString()}</span>
                        {hidden ? <EyeOff className="h-3 w-3 shrink-0 text-neutral-500" /> : <Eye className="h-3 w-3 shrink-0 text-neutral-500" />}
                      </button>
                    )
                  })}
                  {overlayHiddenGroups.size > 0 && (
                    <button onClick={() => setOverlayHiddenGroups(new Set())}
                            className="mt-1 w-full rounded border border-neutral-700 px-1 py-0.5 text-neutral-300 hover:bg-neutral-800">
                      Show all
                    </button>
                  )}
                </div>
              )}
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
                        {(e as LegendPct).overlayPct != null ? (
                          <span className="ml-auto pl-3 tabular-nums text-neutral-400" title="Share of reference / overlay patches">
                            {(e as LegendPct).refPct!.toFixed(1)}% <span className="text-orange-300">{(e as LegendPct).overlayPct!.toFixed(1)}%</span>
                          </span>
                        ) : (
                          <span className="ml-auto pl-3 tabular-nums text-neutral-400">
                            {e.count.toLocaleString()}
                          </span>
                        )}
                      </button>
                    )
                  })}
                  {legend.some(e => (e as LegendPct).overlayPct != null) && (
                    <div className="mt-1 text-[10px] text-neutral-500">reference % · <span className="text-orange-300">overlay %</span></div>
                  )}
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
                    {selectedSet === 'overlay' && <> · <span className="text-orange-300">overlay</span></>}
                    {selectedLabelInfo && (
                      <> · {selectedLabelInfo.cluster < 0 ? 'noise' : `cluster ${selectedLabelInfo.cluster + 1}`}
                        {selectedLabelInfo.far && <span className="text-amber-400"> (far from its centre)</span>}</>
                    )}
                    {' '}· click outside or press Esc to collapse
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {compositionOpen && activeOverlay && (
        <CompositionPanel
          overlayId={activeOverlay.id}
          overlayCohortId={activeOverlay.cohort_id}
          projectionId={projectionId}
          clusterings={completedKmeans.map(c => ({
            id: c.id,
            label: typeof c.params?.label === 'string' ? c.params.label : `k-means k=${c.n_clusters ?? '?'} (#${c.id})`,
            n_clusters: c.n_clusters ?? 0,
          }))}
          clusterColor={clusterColor}
          onClose={() => setCompositionOpen(false)}
        />
      )}

      <style>{`
        @keyframes sc-patch-grow {
          from { transform: scale(0.25); opacity: 0; }
          to   { transform: scale(1);    opacity: 1; }
        }
      `}</style>
    </div>
  )
}
