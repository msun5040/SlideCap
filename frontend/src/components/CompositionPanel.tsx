import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { ArrowLeftRight, Download, Loader2, Play, X } from 'lucide-react'
import { getApiBase } from '@/api'
import { saveBlob } from '@/lib/download'
import { usePatchImage } from '@/lib/patchImages'
import { CompositionExportDialog } from './CompositionExportDialog'

/**
 * Cluster composition for an overlay cohort: how the share of each reference
 * cluster differs between group A and group B (e.g. Pre → Post), across several
 * clusterings at once. Two readings, switched by the Per patient / Group totals
 * toggle:
 *
 *   • Per patient — each patient's own before/after change, paired.
 *   • Group totals — every patch of a group pooled into one pile, so the
 *     percentages are of all tiles in that group.
 *
 * Statistics come from services/cluster_composition.py; this panel sets them up
 * and shows the results — heatmap across clusterings, slope chart or stacked
 * composition bars, the patches behind a cluster, and how clusters nest between
 * clusterings.
 */

interface SchemeRow {
  id: number
  name: string
  groups: { id: number; name: string; color?: string | null; slide_hashes: string[] }[]
}

export interface CompositionClustering {
  id: number
  label: string
  n_clusters: number
}

interface ClusterStat {
  cluster: number
  ref_pct: number
  a_median_pct?: number
  b_median_pct?: number
  median_delta_pp?: number
  n_up?: number
  n_down?: number
  p?: number | null
  q?: number | null
}

interface PatientRow {
  patient_id: string
  patient: string
  a_pct: number[]
  b_pct: number[]
  a_slides: number
  b_slides: number
  a_patches: number
  b_patches: number
}

interface PooledStat {
  cluster: number
  a_pct: number
  b_pct: number
  delta_pp: number
  log2_ratio: number | null
  ci_lo_pp: number | null
  ci_hi_pp: number | null
  a_patches: number
  b_patches: number
  p: number | null
  q: number | null
}

interface Pooled {
  n_blocks: number
  n_paired_blocks?: number
  n_slides_a?: number
  n_slides_b?: number
  n_units_a?: number
  n_units_b?: number
  pool_unit?: 'patch' | 'slide' | 'case' | 'patient'
  pool_cap_pct?: number | null
  n_patches_a?: number
  n_patches_b?: number
  dominance_a?: number
  dominance_b?: number
  tested: boolean
  global_p: number | null
  clusters: PooledStat[]
  warnings: string[]
}

interface ClusteringResult {
  clustering_id: number
  label: string
  n_clusters: number
  agreement: number
  n_pairs: number
  tested: boolean
  kept_clusters: number[]
  global_p: number | null
  clusters: ClusterStat[]
  pooled: Pooled
  patients: PatientRow[]
  unpaired: { patient: string; has: 'a' | 'b' }[]
  warnings: string[]
}

interface Tile {
  slide_hash: string
  display_name?: string | null
  x: number
  y: number
  size: number
  dist: number
}

interface Props {
  overlayId: number
  overlayCohortId: number
  projectionId: number
  clusterings: CompositionClustering[]
  clusterColor: (i: number) => string
  onClose: () => void
}

const selectCls = 'rounded border border-neutral-700 bg-neutral-900 px-2 py-1 text-[12px] text-neutral-100'

function fmtP(p: number | null | undefined) {
  if (p == null) return '—'
  return p < 0.001 ? p.toExponential(1) : p.toFixed(3)
}

/** Diverging blue (down) → neutral → red (up), for a Δ in percentage points. */
function deltaColor(d: number | undefined, maxAbs: number) {
  if (d == null || !Number.isFinite(d)) return '#27272a'
  const t = Math.max(-1, Math.min(1, d / (maxAbs || 1)))
  const a = Math.abs(t)
  const [r, g, b] = t >= 0 ? [239, 68, 68] : [59, 130, 246]
  const mix = (c: number) => Math.round(39 + (c - 39) * a)
  return `rgb(${mix(r)}, ${mix(g)}, ${mix(b)})`
}

function PatchThumb({ tile }: { tile: Tile }) {
  const { url, error } = usePatchImage(tile, 160)
  return (
    <div className="aspect-square overflow-hidden rounded border border-neutral-800 bg-neutral-900"
         title={`${tile.display_name || tile.slide_hash.slice(0, 10)} · (${tile.x}, ${tile.y}) · distance ${tile.dist.toFixed(2)}`}>
      {url ? <img src={url} alt="" className="h-full w-full object-cover" />
        : error ? <div className="p-1 text-[9px] text-red-400">{error}</div>
          : <div className="flex h-full items-center justify-center"><Loader2 className="h-3 w-3 animate-spin text-neutral-600" /></div>}
    </div>
  )
}

function SlopeChart({ result, clusterIdx, groupA, groupB }: {
  result: ClusteringResult; clusterIdx: number; groupA: string; groupB: string
}) {
  const col = result.kept_clusters.indexOf(clusterIdx)
  if (col < 0) return <p className="text-[11px] text-neutral-500">This cluster is excluded.</p>
  const pts = result.patients.map(p => ({ id: p.patient_id, label: p.patient, a: p.a_pct[col], b: p.b_pct[col] }))
  const W = 300, H = 220, padL = 40, padR = 70, padT = 12, padB = 26
  const max = Math.max(1, ...pts.flatMap(p => [p.a, p.b])) * 1.08
  const y = (v: number) => padT + (H - padT - padB) * (1 - v / max)
  const xA = padL + 20, xB = W - padR
  const ticks = [0, max / 2, max].map(v => Math.round(v * 10) / 10)
  const medA = result.clusters.find(c => c.cluster === clusterIdx)?.a_median_pct
  const medB = result.clusters.find(c => c.cluster === clusterIdx)?.b_median_pct
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full max-w-[340px]" role="img"
         aria-label={`Per-patient share of cluster ${clusterIdx + 1}, ${groupA} to ${groupB}`}>
      {ticks.map(t => (
        <g key={t}>
          <line x1={padL} x2={W - padR + 10} y1={y(t)} y2={y(t)} stroke="#27272a" />
          <text x={padL - 6} y={y(t) + 3} textAnchor="end" fontSize="9" fill="#71717a">{t}%</text>
        </g>
      ))}
      {pts.map(p => {
        const up = p.b > p.a
        return (
          <g key={p.id}>
            <title>{`${p.label}: ${p.a.toFixed(1)}% → ${p.b.toFixed(1)}%`}</title>
            <line x1={xA} x2={xB} y1={y(p.a)} y2={y(p.b)} stroke={up ? '#f87171' : '#60a5fa'} strokeOpacity={0.7} strokeWidth={1.5} />
            <circle cx={xA} cy={y(p.a)} r={2.5} fill="#a1a1aa" />
            <circle cx={xB} cy={y(p.b)} r={2.5} fill="#a1a1aa" />
          </g>
        )
      })}
      {medA != null && medB != null && (
        <line x1={xA} x2={xB} y1={y(medA)} y2={y(medB)} stroke="#fafafa" strokeWidth={2.5} strokeDasharray="4 3">
          <title>{`median: ${medA.toFixed(1)}% → ${medB.toFixed(1)}%`}</title>
        </line>
      )}
      <text x={xA} y={H - 8} textAnchor="middle" fontSize="10" fill="#d4d4d8">{groupA}</text>
      <text x={xB} y={H - 8} textAnchor="middle" fontSize="10" fill="#d4d4d8">{groupB}</text>
      <text x={xB + 8} y={padT + 8} fontSize="9" fill="#f87171">▲ up</text>
      <text x={xB + 8} y={padT + 20} fontSize="9" fill="#60a5fa">▼ down</text>
      <text x={xB + 8} y={padT + 32} fontSize="9" fill="#fafafa">- - median</text>
    </svg>
  )
}

/** Two stacked bars — the whole of group A and the whole of group B, split by cluster. */
function CompositionBars({ pooled, clusterColor, groupA, groupB, selected, onSelect }: {
  pooled: Pooled
  clusterColor: (i: number) => string
  groupA: string
  groupB: string
  selected: number | null
  onSelect: (c: number) => void
}) {
  const rows: { label: string; key: 'a' | 'b'; patches?: number; slides?: number }[] = [
    { label: groupA, key: 'a', patches: pooled.n_patches_a, slides: pooled.n_slides_a },
    { label: groupB, key: 'b', patches: pooled.n_patches_b, slides: pooled.n_slides_b },
  ]
  return (
    <div className="space-y-1.5">
      {rows.map(r => (
        <div key={r.key} className="flex items-center gap-2">
          <span className="w-24 shrink-0 truncate text-[11px] text-neutral-300" title={r.label}>{r.label}</span>
          <div className="flex h-6 flex-1 overflow-hidden rounded border border-neutral-800">
            {pooled.clusters.map(c => {
              const pct = r.key === 'a' ? c.a_pct : c.b_pct
              const isSel = selected === c.cluster
              return (
                <button key={c.cluster} onClick={() => onSelect(c.cluster)}
                        className="h-full border-r border-neutral-950/60 last:border-r-0"
                        style={{ width: `${pct}%`, backgroundColor: clusterColor(c.cluster),
                                 opacity: selected == null || isSel ? 1 : 0.35,
                                 outline: isSel ? '1px solid #fafafa' : undefined, outlineOffset: '-1px' }}
                        title={`Cluster ${c.cluster + 1}: ${pct.toFixed(1)}% of ${r.label} `
                               + `(${(r.key === 'a' ? c.a_patches : c.b_patches).toLocaleString()} patches)`} />
              )
            })}
          </div>
          <span className="w-28 shrink-0 text-right text-[10px] tabular-nums text-neutral-500">
            {r.patches?.toLocaleString() ?? '—'} patches
            {r.slides != null ? ` · ${r.slides} slides` : ''}
          </span>
        </div>
      ))}
    </div>
  )
}

export function CompositionPanel({ overlayId, overlayCohortId, projectionId, clusterings, clusterColor, onClose }: Props) {
  const [schemes, setSchemes] = useState<SchemeRow[]>([])
  const [schemeId, setSchemeId] = useState<number | null>(null)
  const [groupA, setGroupA] = useState<number | null>(null)
  const [groupB, setGroupB] = useState<number | null>(null)
  const [restrictId, setRestrictId] = useState<number | null>(null)
  const [weighting, setWeighting] = useState<'slide' | 'patch'>('slide')
  /** Which reading is on show: each patient's own change, or the two group piles. */
  const [mode, setMode] = useState<'patient' | 'group'>('patient')
  /** Group totals: what carries equal weight, and an optional cap on one case. */
  const [poolUnit, setPoolUnit] = useState<'patch' | 'slide' | 'case' | 'patient'>('patch')
  const [poolCap, setPoolCap] = useState<number | null>(null)
  const [excludeFar, setExcludeFar] = useState(false)
  const [chosen, setChosen] = useState<Set<number>>(() => new Set(clusterings.map(c => c.id)))
  const [excluded, setExcluded] = useState<Record<number, number[]>>({})

  const [resultState, setResultState] = useState<{ key: string; data: ClusteringResult[] } | null>(null)
  const [exportOpen, setExportOpen] = useState(false)
  const requestVersion = useRef(0)
  const requestParams = {
    clustering_ids: clusterings.filter(c => chosen.has(c.id)).map(c => c.id),
    scheme_id: schemeId, group_a_id: groupA, group_b_id: groupB,
    weighting, exclude_far: excludeFar, restrict_group_id: restrictId,
    pool_unit: poolUnit, pool_cap_pct: poolUnit === 'patch' ? poolCap : null,
    exclude_clusters: excluded,
  }
  const requestKey = JSON.stringify(requestParams)
  // A response is usable only for the exact inputs that produced it, even if
  // controls change while a request is in flight.
  const results = resultState?.key === requestKey ? resultState.data : null
  const [running, setRunning] = useState(false)
  const [error, setError] = useState('')
  const [sel, setSel] = useState<{ clusteringId: number; cluster: number } | null>(null)

  const [tiles, setTiles] = useState<{ a: Tile[]; b: Tile[]; ref: Tile[] } | null>(null)
  const [tilesLoading, setTilesLoading] = useState(false)
  const [cw, setCw] = useState<{ a: number | null; b: number | null }>({ a: null, b: null })
  const [cwData, setCwData] = useState<{ a: { label: string; n_clusters: number }; b: { label: string; n_clusters: number }; row_fraction: number[][] } | null>(null)

  // ── Schemes of the overlay cohort; guess Pre/Post ────────────────────
  useEffect(() => {
    fetch(`${getApiBase()}/cohorts/${overlayCohortId}/group-schemes`)
      .then(r => (r.ok ? r.json() : []))
      .then((rows: SchemeRow[]) => {
        setSchemes(rows)
        const s = rows.find(r => r.groups.length === 2) ?? rows.find(r => r.groups.length >= 2)
        if (!s) return
        setSchemeId(s.id)
        const pre = s.groups.find(g => /pre|before|baseline/i.test(g.name))
        const post = s.groups.find(g => /post|after|treat/i.test(g.name) && g.id !== pre?.id)
        setGroupA((pre ?? s.groups[0]).id)
        setGroupB((post ?? s.groups.find(g => g.id !== (pre ?? s.groups[0]).id) ?? s.groups[1]).id)
      })
      .catch(() => setSchemes([]))
  }, [overlayCohortId])

  const scheme = schemes.find(s => s.id === schemeId) ?? null
  const groupName = (id: number | null) =>
    schemes.flatMap(s => s.groups).find(g => g.id === id)?.name ?? '—'

  // Crosswalk defaults: coarsest vs finest chosen clustering.
  useEffect(() => {
    const byK = [...clusterings].sort((x, y) => x.n_clusters - y.n_clusters)
    if (byK.length >= 2) setCw({ a: byK[0].id, b: byK[byK.length - 1].id })
    else if (byK.length === 1) setCw({ a: byK[0].id, b: byK[0].id })
  }, [clusterings])

  const run = useCallback(async (excl = excluded) => {
    if (!schemeId || !groupA || !groupB) { setError('Choose a scheme and two groups to compare.'); return }
    if (chosen.size === 0) { setError('Choose at least one clustering.'); return }
    const version = ++requestVersion.current
    const body = JSON.stringify({ ...requestParams, exclude_clusters: excl })
    setRunning(true); setError('')
    try {
      const res = await fetch(`${getApiBase()}/overlays/${overlayId}/composition`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
      })
      const d = await res.json().catch(() => null)
      if (!res.ok) throw new Error(d?.detail || `Composition failed (${res.status})`)
      if (version !== requestVersion.current) return
      setResultState({ key: body, data: d.results })
      if (d.results.length && (!sel || !d.results.some((r: ClusteringResult) => r.clustering_id === sel.clusteringId))) {
        // Start on the largest shift in the first clustering.
        const r0: ClusteringResult = d.results[0]
        const best = [...r0.clusters].sort((x, y) => Math.abs(y.median_delta_pp ?? 0) - Math.abs(x.median_delta_pp ?? 0))[0]
        if (best) setSel({ clusteringId: r0.clustering_id, cluster: best.cluster })
      }
    } catch (e: any) {
      if (version === requestVersion.current) setError(e.message || 'Composition failed')
    } finally {
      if (version === requestVersion.current) setRunning(false)
    }
  }, [requestKey, overlayId, schemeId, groupA, groupB, weighting, excludeFar, restrictId, chosen, clusterings, excluded, sel,
      poolUnit, poolCap])

  // ── Tiles for the selected cluster ───────────────────────────────────
  useEffect(() => {
    if (!results || !sel || !groupA || !groupB) { setTiles(null); return }
    let cancelled = false
    setTilesLoading(true)
    const base = `${getApiBase()}/overlays/${overlayId}/tiles?clustering_id=${sel.clusteringId}&cluster=${sel.cluster}`
    const get = (q: string) => fetch(base + q).then(r => (r.ok ? r.json() : { tiles: [] })).then(d => d.tiles as Tile[])
    Promise.all([get(`&group_id=${groupA}&n=12`), get(`&group_id=${groupB}&n=12`), get(`&source=reference&n=6`)])
      .then(([a, b, ref]) => { if (!cancelled) setTiles({ a, b, ref }) })
      .catch(() => { if (!cancelled) setTiles(null) })
      .finally(() => { if (!cancelled) setTilesLoading(false) })
    return () => { cancelled = true }
  }, [sel, groupA, groupB, overlayId, results])

  // ── Crosswalk ────────────────────────────────────────────────────────
  useEffect(() => {
    if (cw.a == null || cw.b == null) { setCwData(null); return }
    fetch(`${getApiBase()}/projections/${projectionId}/crosswalk?a=${cw.a}&b=${cw.b}`)
      .then(r => (r.ok ? r.json() : null))
      .then(setCwData)
      .catch(() => setCwData(null))
  }, [cw, projectionId])

  const maxAbs = useMemo(() => {
    if (!results) return 1
    return Math.max(1, ...results.flatMap(r => mode === 'group'
      ? r.pooled.clusters.map(c => Math.abs(c.delta_pp))
      : r.clusters.map(c => Math.abs(c.median_delta_pp ?? 0))))
  }, [results, mode])

  const selResult = results?.find(r => r.clustering_id === sel?.clusteringId) ?? null
  const selStat = selResult?.clusters.find(c => c.cluster === sel?.cluster) ?? null
  const selPooled = selResult?.pooled.clusters.find(c => c.cluster === sel?.cluster) ?? null
  const warnings = useMemo(() => Array.from(new Set((results ?? []).flatMap(
    r => mode === 'group' ? [...r.warnings, ...(r.pooled.warnings ?? [])] : r.warnings))), [results, mode])
  const unpaired = results?.[0]?.unpaired ?? []

  const toggleExclude = (clusteringId: number, cluster: number) => {
    const cur = new Set(excluded[clusteringId] ?? [])
    if (cur.has(cluster)) cur.delete(cluster); else cur.add(cluster)
    const next = { ...excluded, [clusteringId]: [...cur] }
    setExcluded(next)
    run(next)
  }

  const exportCsv = (kind: 'patients' | 'stats' | 'groups') => {
    if (!results) return
    const a = groupName(groupA), b = groupName(groupB)
    const esc = (v: unknown) => {
      const s = v == null ? '' : String(v)
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
    }
    const rows: unknown[][] = []
    if (kind === 'patients') {
      rows.push(['clustering', 'cluster', 'patient_id', 'patient', 'group', 'share_pct', 'n_slides', 'n_patches', 'weighting'])
      for (const r of results) {
        r.kept_clusters.forEach((c, j) => {
          for (const p of r.patients) {
            rows.push([r.label, c + 1, p.patient_id, p.patient, a, p.a_pct[j], p.a_slides, p.a_patches, weighting])
            rows.push([r.label, c + 1, p.patient_id, p.patient, b, p.b_pct[j], p.b_slides, p.b_patches, weighting])
          }
        })
      }
    } else if (kind === 'groups') {
      rows.push(['clustering', 'cluster', 'ref_pct', `${a}_pct_of_group`, `${b}_pct_of_group`, 'delta_pp',
                 'ci_lo_pp', 'ci_hi_pp', 'log2_ratio', `${a}_patches`, `${b}_patches`, 'permutation_p', 'bh_q',
                 'global_permutation_p', 'n_patients', 'weighting', 'cap_pct_per_case', 'excluded_far', 'restricted_to'])
      for (const r of results) {
        const ref = new Map(r.clusters.map(c => [c.cluster, c.ref_pct]))
        for (const c of r.pooled.clusters) {
          rows.push([r.label, c.cluster + 1, ref.get(c.cluster), c.a_pct, c.b_pct, c.delta_pp,
                     c.ci_lo_pp, c.ci_hi_pp, c.log2_ratio, c.a_patches, c.b_patches, c.p, c.q,
                     r.pooled.global_p, r.pooled.n_blocks,
                     r.pooled.pool_unit === 'patch' ? 'every patch' : `each ${r.pooled.pool_unit} equally`,
                     r.pooled.pool_cap_pct ?? '', excludeFar, restrictId ? groupName(restrictId) : ''])
        }
      }
    } else {
      rows.push(['clustering', 'cluster', 'ref_pct', `${a}_median_pct`, `${b}_median_pct`, 'median_delta_pp',
                 'n_up', 'n_down', 'n_pairs', 'wilcoxon_p', 'bh_q', 'global_permutation_p', 'excluded_far', 'restricted_to'])
      for (const r of results) {
        for (const c of r.clusters) {
          rows.push([r.label, c.cluster + 1, c.ref_pct, c.a_median_pct, c.b_median_pct, c.median_delta_pp,
                     c.n_up, c.n_down, r.n_pairs, c.p, c.q, r.global_p, excludeFar, restrictId ? groupName(restrictId) : ''])
        }
      }
    }
    const csv = rows.map(r => r.map(esc).join(',')).join('\n')
    saveBlob(new Blob([csv], { type: 'text/csv' }), `composition-${kind}-${a}-vs-${b}.csv`.replace(/\s+/g, '_'))
  }

  const allGroups = schemes.flatMap(s => s.groups.map(g => ({ ...g, scheme: s.name })))

  return (
    <div className="absolute inset-y-0 right-0 z-[125] flex w-[min(1120px,94vw)] flex-col border-l border-neutral-800 bg-neutral-950 text-neutral-100 shadow-2xl">
      {/* Header */}
      <div className="flex shrink-0 items-center gap-3 border-b border-neutral-800 px-4 py-2">
        <span className="text-sm font-medium">Composition</span>
        <span className="text-[11px] text-neutral-400">
          {mode === 'group'
            ? <>share of all patches in each group, {groupName(groupA)} vs {groupName(groupB)}</>
            : <>patient-paired shift in reference-cluster shares, {groupName(groupA)} → {groupName(groupB)}</>}
        </span>
        <div className="ml-auto flex items-center gap-2">
          <button onClick={() => exportCsv('groups')} disabled={!results}
                  className="inline-flex items-center gap-1 rounded border border-neutral-700 px-2 py-1 text-[12px] hover:bg-neutral-800 disabled:opacity-40">
            <Download className="h-3.5 w-3.5" /> Group CSV
          </button>
          <button onClick={() => exportCsv('stats')} disabled={!results}
                  className="inline-flex items-center gap-1 rounded border border-neutral-700 px-2 py-1 text-[12px] hover:bg-neutral-800 disabled:opacity-40">
            <Download className="h-3.5 w-3.5" /> Stats CSV
          </button>
          <button onClick={() => setExportOpen(true)} disabled={clusterings.length === 0}
                  className="inline-flex items-center gap-1 rounded border border-neutral-700 px-2 py-1 text-[12px] hover:bg-neutral-800 disabled:opacity-40">
            <Download className="h-3.5 w-3.5" /> Export data
          </button>
          <button onClick={onClose} className="rounded p-1 hover:bg-neutral-800" title="Close composition">
            <X className="h-4 w-4" />
          </button>
        </div>
      </div>

      <CompositionExportDialog open={exportOpen} onOpenChange={setExportOpen}
        overlayId={overlayId} clusterings={clusterings} groupA={groupA} groupB={groupB}
        groupAName={groupName(groupA)} groupBName={groupName(groupB)} schemeId={schemeId}
        groups={allGroups} excluded={excluded}
        onExportPaired={results ? () => exportCsv('patients') : undefined} />

      {/* Settings */}
      <div className="flex shrink-0 flex-wrap items-center gap-2 border-b border-neutral-800 bg-neutral-900/60 px-4 py-2 text-[12px]">
        {schemes.length === 0 ? (
          <span className="text-amber-400">
            The overlay cohort has no label schemes. Create one with two groups (e.g. Timepoint: Pre / Post) in the
            Analysis Workspace, assign slides, then reopen this panel.
          </span>
        ) : (
          <>
            <select className={selectCls} value={schemeId ?? ''} onChange={e => {
              const s = schemes.find(x => x.id === Number(e.target.value))
              setSchemeId(s?.id ?? null); setGroupA(s?.groups[0]?.id ?? null); setGroupB(s?.groups[1]?.id ?? null)
            }}>
              {schemes.map(s => <option key={s.id} value={s.id}>{s.name}</option>)}
            </select>
            <select className={selectCls} value={groupA ?? ''} onChange={e => setGroupA(Number(e.target.value))}>
              {scheme?.groups.map(g => <option key={g.id} value={g.id}>{g.name}</option>)}
            </select>
            <button className="rounded p-1 hover:bg-neutral-800" title="Swap groups"
                    onClick={() => { setGroupA(groupB); setGroupB(groupA) }}>
              <ArrowLeftRight className="h-3.5 w-3.5" />
            </button>
            <select className={selectCls} value={groupB ?? ''} onChange={e => setGroupB(Number(e.target.value))}>
              {scheme?.groups.map(g => <option key={g.id} value={g.id}>{g.name}</option>)}
            </select>
            <span className="mx-1 h-4 w-px bg-neutral-800" />
            <select className={selectCls} value={weighting} onChange={e => setWeighting(e.target.value as 'slide' | 'patch')}
                    title="How a patient's slides in one group combine — per-patient reading only; group totals always pool every patch">
              <option value="slide">Each slide weighted equally</option>
              <option value="patch">Pool patches</option>
            </select>
            <select className={selectCls} value={restrictId ?? ''} onChange={e => setRestrictId(e.target.value ? Number(e.target.value) : null)}
                    title="Sensitivity check: only use slides in this group (e.g. resections)">
              <option value="">All slides</option>
              {allGroups.map(g => <option key={g.id} value={g.id}>Only {g.scheme}: {g.name}</option>)}
            </select>
            <label className="inline-flex items-center gap-1.5" title="Leave out patches farther from their cluster than the reference's own 99th percentile">
              <input type="checkbox" checked={excludeFar} onChange={e => setExcludeFar(e.target.checked)} />
              Exclude far patches
            </label>
          </>
        )}
        <div className="flex w-full flex-wrap items-center gap-1.5 pt-1">
          <div className="mr-1 inline-flex overflow-hidden rounded border border-neutral-700">
            {([['patient', 'Per patient'], ['group', 'Group totals']] as const).map(([k, label]) => (
              <button key={k} onClick={() => setMode(k)}
                      title={k === 'patient'
                        ? 'Each patient paired with themselves; the test is on patients'
                        : 'All patches of a group pooled — percentages are of every tile in that group'}
                      className={`px-2 py-0.5 text-[11px] ${mode === k ? 'bg-neutral-100 text-neutral-900' : 'hover:bg-neutral-800'}`}>
                {label}
              </button>
            ))}
          </div>
          {mode === 'group' && (
            <>
              <select className={selectCls} value={poolUnit}
                      onChange={e => setPoolUnit(e.target.value as typeof poolUnit)}
                      title="What carries equal weight in the group totals. Nothing is ever discarded — a slide that counts for less still contributes all of its patches.">
                <option value="patch">Every patch counts once</option>
                <option value="slide">Each slide counts once</option>
                <option value="case">Each case counts once</option>
                <option value="patient">Each patient counts once</option>
              </select>
              {poolUnit === 'patch' && (
                <select className={selectCls} value={poolCap ?? ''}
                        onChange={e => setPoolCap(e.target.value ? Number(e.target.value) : null)}
                        title="Scale down any case that would otherwise carry more than this share of its group. Its patches all still count, just for less each.">
                  <option value="">No cap per case</option>
                  <option value="30">Cap a case at 30%</option>
                  <option value="20">Cap a case at 20%</option>
                  <option value="10">Cap a case at 10%</option>
                  <option value="5">Cap a case at 5%</option>
                </select>
              )}
            </>
          )}
          <span className="text-[11px] text-neutral-400">Clusterings</span>
          {clusterings.map(c => (
            <label key={c.id} className={`inline-flex items-center gap-1 rounded border px-1.5 py-0.5 ${chosen.has(c.id) ? 'border-neutral-500 bg-neutral-800' : 'border-neutral-800 text-neutral-500'}`}>
              <input type="checkbox" checked={chosen.has(c.id)} onChange={() => setChosen(prev => {
                const n = new Set(prev); if (n.has(c.id)) n.delete(c.id); else n.add(c.id); return n
              })} />
              {c.label}
            </label>
          ))}
          <button onClick={() => run()} disabled={running || !schemeId}
                  className="ml-auto inline-flex items-center gap-1.5 rounded bg-neutral-100 px-3 py-1 font-medium text-neutral-900 hover:bg-white disabled:opacity-50">
            {running ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
            {results ? 'Re-run' : 'Run'}
          </button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-auto px-4 py-3 text-[12px]">
        {error && <div className="mb-3 rounded border border-red-900 bg-red-950/40 p-2 text-red-300">{error}</div>}
        {resultState && !results && !running && !error && (
          <p className="mb-2 text-amber-300">Settings changed. Run the comparison again to update results and summary exports.</p>
        )}
        {!results && !running && !error && (
          <p className="text-neutral-400">
            Choose the two groups to compare and the clusterings to test, then Run. You get both readings: per patient
            (each patient paired with themselves — they need slides in both groups) and group totals (every patch of a
            group pooled, so percentages are of all tiles in that group).
          </p>
        )}

        {results && (
          <div className="space-y-4">
            {(warnings.length > 0 || (mode === 'patient' && unpaired.length > 0)) && (
              <div className="rounded border border-amber-900/60 bg-amber-950/30 p-2 text-[11px] text-amber-300">
                {warnings.map(w => <div key={w}>• {w}</div>)}
                {mode === 'patient' && unpaired.length > 0 && (
                  <div>• {unpaired.length} patient(s) only have slides in one group and aren't in the paired test
                    ({unpaired.slice(0, 8).map(u => u.patient).join(', ')}{unpaired.length > 8 ? '…' : ''}).</div>
                )}
              </div>
            )}

            {/* Heatmap across clusterings */}
            <div>
              <div className="mb-1.5 flex items-baseline gap-2">
                <span className="font-medium">
                  {mode === 'group' ? 'Change in group share per cluster' : 'Median change per cluster'}
                </span>
                <span className="text-[11px] text-neutral-500">
                  percentage points, {groupName(groupA)} → {groupName(groupB)} · ● BH q &lt; 0.05 · click a cell for detail ·
                  cluster numbers aren't comparable between clusterings
                </span>
              </div>
              <div className="space-y-1.5">
                {results.map(r => (
                  <div key={r.clustering_id} className="flex items-center gap-2">
                    <div className="w-40 shrink-0">
                      <div className="font-medium">{r.label}</div>
                      <div className="text-[10px] text-neutral-500">
                        {mode === 'group'
                          ? <>{r.pooled.n_blocks} patients · global p {fmtP(r.pooled.global_p)}</>
                          : <>{r.n_pairs} pairs · global p {fmtP(r.global_p)}</>}
                        {' · '}agreement {(r.agreement * 100).toFixed(1)}%
                      </div>
                    </div>
                    <div className="flex flex-wrap gap-1">
                      {Array.from({ length: r.n_clusters }, (_, c) => {
                        const st = r.clusters.find(x => x.cluster === c)
                        const pl = r.pooled.clusters.find(x => x.cluster === c)
                        const shown = mode === 'group' ? pl : st
                        const delta = mode === 'group' ? pl?.delta_pp : st?.median_delta_pp
                        const q = mode === 'group' ? pl?.q : st?.q
                        const isSel = sel?.clusteringId === r.clustering_id && sel.cluster === c
                        const sig = q != null && q < 0.05
                        return (
                          <button key={c}
                                  onClick={() => setSel({ clusteringId: r.clustering_id, cluster: c })}
                                  className={`relative flex h-11 w-12 flex-col items-center justify-center rounded border text-[10px] ${isSel ? 'border-white' : 'border-neutral-800'}`}
                                  style={{ backgroundColor: shown ? deltaColor(delta, maxAbs) : 'transparent',
                                           backgroundImage: shown ? undefined : 'repeating-linear-gradient(45deg,#27272a 0 4px,transparent 4px 8px)' }}
                                  title={!shown ? `Cluster ${c + 1}: excluded`
                                    : mode === 'group' && pl
                                      ? `Cluster ${c + 1}: ${pl.a_pct.toFixed(1)}% → ${pl.b_pct.toFixed(1)}% of all patches `
                                        + `(${pl.delta_pp > 0 ? '+' : ''}${pl.delta_pp.toFixed(1)} pp), p ${fmtP(pl.p)}, q ${fmtP(pl.q)}`
                                      : `Cluster ${c + 1}: ${st?.median_delta_pp?.toFixed(1)} pp (${st?.n_up}↑ ${st?.n_down}↓), p ${fmtP(st?.p)}, q ${fmtP(st?.q)}`}>
                            <span className="absolute left-1 top-1 h-1.5 w-1.5 rounded-full" style={{ backgroundColor: clusterColor(c) }} />
                            <span className="font-medium">{c + 1}{sig ? ' ●' : ''}</span>
                            <span className="tabular-nums">
                              {delta != null ? `${delta > 0 ? '+' : ''}${delta.toFixed(1)}` : '—'}
                            </span>
                          </button>
                        )
                      })}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Detail for the selected cluster */}
            {selResult && sel && (
              <div className="rounded border border-neutral-800 p-3">
                <div className="mb-2 flex flex-wrap items-center gap-2">
                  <span className="h-3 w-3 rounded-[2px]" style={{ backgroundColor: clusterColor(sel.cluster) }} />
                  <span className="text-[13px] font-medium">{selResult.label} · cluster {sel.cluster + 1}</span>
                  {mode === 'group' ? selPooled && (
                    <span className="text-[11px] text-neutral-400">
                      reference {selStat?.ref_pct.toFixed(1)}% · {groupName(groupA)} {selPooled.a_pct.toFixed(1)}%
                      ({selPooled.a_patches.toLocaleString()} patches) · {groupName(groupB)} {selPooled.b_pct.toFixed(1)}%
                      ({selPooled.b_patches.toLocaleString()} patches) · Δ {selPooled.delta_pp > 0 ? '+' : ''}
                      {selPooled.delta_pp.toFixed(1)} pp
                      {selPooled.ci_lo_pp != null && selPooled.ci_hi_pp != null &&
                        ` (95% CI ${selPooled.ci_lo_pp.toFixed(1)} to ${selPooled.ci_hi_pp.toFixed(1)})`}
                      {selPooled.log2_ratio != null && ` · ×${Math.pow(2, selPooled.log2_ratio).toFixed(2)}`}
                      {' '}· p {fmtP(selPooled.p)} · q {fmtP(selPooled.q)}
                    </span>
                  ) : selStat && (
                    <span className="text-[11px] text-neutral-400">
                      reference {selStat.ref_pct.toFixed(1)}% · {groupName(groupA)} median {selStat.a_median_pct?.toFixed(1)}%
                      · {groupName(groupB)} median {selStat.b_median_pct?.toFixed(1)}%
                      · Δ {selStat.median_delta_pp?.toFixed(1)} pp · {selStat.n_up}↑ {selStat.n_down}↓ of {selResult.n_pairs}
                      · p {fmtP(selStat.p)} · q {fmtP(selStat.q)}
                    </span>
                  )}
                  <button onClick={() => toggleExclude(selResult.clustering_id, sel.cluster)}
                          className="ml-auto rounded border border-neutral-700 px-2 py-0.5 text-[11px] hover:bg-neutral-800"
                          title="E.g. background / glass clusters. Shares are recomputed without it.">
                    {(excluded[selResult.clustering_id] ?? []).includes(sel.cluster) ? 'Include cluster again' : 'Exclude cluster & re-run'}
                  </button>
                </div>
                {mode === 'group' ? !selResult.pooled.tested && (
                  <p className="mb-2 text-[11px] text-amber-400">
                    Fewer than 4 patients in the two groups — percentages only, no test.
                  </p>
                ) : !selResult.tested && (
                  <p className="mb-2 text-[11px] text-amber-400">
                    Fewer than 6 paired patients — effect sizes only, no tests.
                  </p>
                )}
                {mode === 'group' && (
                  <div className="mb-3 space-y-2">
                    <div className="text-[11px] text-neutral-500">
                      {selResult.pooled.pool_unit === 'patch'
                        ? (selResult.pooled.pool_cap_pct
                            ? `Every patch counts once, with no case above ${selResult.pooled.pool_cap_pct}% of its group.`
                            : 'Every patch counts once — a bigger slide has more say.')
                        : `Each ${selResult.pooled.pool_unit} counts once: `
                          + `${selResult.pooled.n_units_a ?? 0} in ${groupName(groupA)}, `
                          + `${selResult.pooled.n_units_b ?? 0} in ${groupName(groupB)}. Every patch still counts.`}
                      {selResult.pooled.dominance_a != null && selResult.pooled.dominance_b != null &&
                        ` Biggest single contributor: ${(selResult.pooled.dominance_a * 100).toFixed(0)}% of `
                        + `${groupName(groupA)}, ${(selResult.pooled.dominance_b * 100).toFixed(0)}% of ${groupName(groupB)}.`}
                    </div>
                    <CompositionBars pooled={selResult.pooled} clusterColor={clusterColor}
                                     groupA={groupName(groupA)} groupB={groupName(groupB)}
                                     selected={sel.cluster}
                                     onSelect={c => setSel({ clusteringId: selResult.clustering_id, cluster: c })} />
                    <div className="overflow-auto">
                      <table className="w-full border-collapse text-[11px] tabular-nums">
                        <thead className="text-neutral-500">
                          <tr className="border-b border-neutral-800 text-left">
                            <th className="py-1 pr-2 font-normal">Cluster</th>
                            <th className="py-1 pr-2 font-normal">Reference</th>
                            <th className="py-1 pr-2 font-normal">{groupName(groupA)}</th>
                            <th className="py-1 pr-2 font-normal">{groupName(groupB)}</th>
                            <th className="py-1 pr-2 font-normal">Δ pp</th>
                            <th className="py-1 pr-2 font-normal">95% CI</th>
                            <th className="py-1 pr-2 font-normal">Fold</th>
                            <th className="py-1 pr-2 font-normal">p</th>
                            <th className="py-1 pr-2 font-normal">q</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selResult.pooled.clusters.map(c => {
                            const ref = selResult.clusters.find(x => x.cluster === c.cluster)?.ref_pct
                            const isSel = c.cluster === sel.cluster
                            return (
                              <tr key={c.cluster}
                                  onClick={() => setSel({ clusteringId: selResult.clustering_id, cluster: c.cluster })}
                                  className={`cursor-pointer border-b border-neutral-900 ${isSel ? 'bg-neutral-800/60' : 'hover:bg-neutral-900'}`}>
                                <td className="py-1 pr-2">
                                  <span className="mr-1 inline-block h-2 w-2 rounded-[2px] align-middle"
                                        style={{ backgroundColor: clusterColor(c.cluster) }} />
                                  {c.cluster + 1}
                                </td>
                                <td className="py-1 pr-2 text-neutral-500">{ref?.toFixed(1)}%</td>
                                <td className="py-1 pr-2">{c.a_pct.toFixed(1)}%</td>
                                <td className="py-1 pr-2">{c.b_pct.toFixed(1)}%</td>
                                <td className="py-1 pr-2" style={{ color: c.delta_pp > 0 ? '#f87171' : '#60a5fa' }}>
                                  {c.delta_pp > 0 ? '+' : ''}{c.delta_pp.toFixed(1)}
                                </td>
                                <td className="py-1 pr-2 text-neutral-500">
                                  {c.ci_lo_pp != null && c.ci_hi_pp != null
                                    ? `${c.ci_lo_pp.toFixed(1)} … ${c.ci_hi_pp.toFixed(1)}` : '—'}
                                </td>
                                <td className="py-1 pr-2 text-neutral-400">
                                  {c.log2_ratio == null ? '—' : `×${Math.pow(2, c.log2_ratio).toFixed(2)}`}
                                </td>
                                <td className="py-1 pr-2">{fmtP(c.p)}</td>
                                <td className="py-1 pr-2">{c.q != null && c.q < 0.05 ? <b>{fmtP(c.q)}</b> : fmtP(c.q)}</td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
                <div className={`grid gap-4 ${mode === 'group' ? '' : 'lg:grid-cols-[340px_1fr]'}`}>
                  {mode === 'patient' && (
                    <SlopeChart result={selResult} clusterIdx={sel.cluster}
                                groupA={groupName(groupA)} groupB={groupName(groupB)} />
                  )}
                  <div className="space-y-2">
                    {tilesLoading && <div className="flex items-center gap-2 text-neutral-500"><Loader2 className="h-3 w-3 animate-spin" /> Loading patches…</div>}
                    {tiles && ([['a', groupName(groupA)], ['b', groupName(groupB)], ['ref', 'Reference cohort']] as const).map(([k, label]) => (
                      <div key={k}>
                        <div className="mb-1 text-[11px] text-neutral-400">
                          {label} — patches closest to the cluster centre{tiles[k].length === 0 ? ': none' : ''}
                        </div>
                        <div className="grid grid-cols-6 gap-1 sm:grid-cols-12">
                          {tiles[k].map(t => <PatchThumb key={`${t.slide_hash}-${t.x}-${t.y}`} tile={t} />)}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Crosswalk */}
            {clusterings.length >= 1 && (
              <div className="rounded border border-neutral-800 p-3">
                <div className="mb-2 flex flex-wrap items-center gap-2">
                  <span className="font-medium">How clusters nest</span>
                  <select className={selectCls} value={cw.a ?? ''} onChange={e => setCw(v => ({ ...v, a: Number(e.target.value) }))}>
                    {clusterings.map(c => <option key={c.id} value={c.id}>{c.label}</option>)}
                  </select>
                  <span className="text-neutral-500">→</span>
                  <select className={selectCls} value={cw.b ?? ''} onChange={e => setCw(v => ({ ...v, b: Number(e.target.value) }))}>
                    {clusterings.map(c => <option key={c.id} value={c.id}>{c.label}</option>)}
                  </select>
                  <span className="text-[11px] text-neutral-500">share of each row's reference patches landing in each column</span>
                </div>
                {cwData && (
                  <div className="overflow-auto">
                    <table className="border-collapse text-[10px] tabular-nums">
                      <thead>
                        <tr>
                          <th className="px-1 text-left text-neutral-500">{cwData.a.label} ↓ / {cwData.b.label} →</th>
                          {Array.from({ length: cwData.b.n_clusters }, (_, j) => (
                            <th key={j} className="px-1 font-normal text-neutral-400">
                              <span className="inline-block h-1.5 w-1.5 rounded-full align-middle" style={{ backgroundColor: clusterColor(j) }} /> {j + 1}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {cwData.row_fraction.map((row, i) => (
                          <tr key={i}>
                            <td className="px-1 text-neutral-400">
                              <span className="inline-block h-1.5 w-1.5 rounded-full align-middle" style={{ backgroundColor: clusterColor(i) }} /> {i + 1}
                            </td>
                            {row.map((f, j) => (
                              <td key={j} className="h-6 w-9 border border-neutral-900 text-center"
                                  style={{ backgroundColor: `rgba(250,250,250,${Math.min(0.85, f)})`, color: f > 0.45 ? '#09090b' : '#a1a1aa' }}>
                                {f >= 0.01 ? Math.round(f * 100) : ''}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            <p className="text-[10px] text-neutral-500">
              {mode === 'group' ? (
                <>
                  Group totals pool every patch in the group, so by default a patient with several large resections
                  weighs more than one small biopsy; the weighting control makes each slide, case or patient count once
                  instead, or caps how much of a group one case can carry. Either way every patch is counted — a
                  down-weighted slide contributes all of its patches, just for less each. Patches within a patient aren't
                  independent, so the p-values come from randomising the group label in whole patients (a patient's own
                  slides swap sides; a patient present in only one group moves wholesale) and rescoring the pooled
                  difference, Benjamini–Hochberg within each clustering. The 95% intervals resample patients.
                </>
              ) : (
                <>
                  Shares per slide use a 0.5-patch pseudocount; tests use centred log-ratios (Wilcoxon signed-rank on
                  patient differences, Benjamini–Hochberg within each clustering; global test = within-patient sign-flip
                  permutation).
                </>
              )}
              {' '}With few patients treat results as hypotheses. Assignments are nearest reference centroid in PCA space
              and don't depend on positions in the map.
            </p>
          </div>
        )}
      </div>
    </div>
  )
}
