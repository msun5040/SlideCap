import { useEffect, useMemo, useState } from 'react'
import { ArrowDownUp, Download, Eye, EyeOff, Loader2, X } from 'lucide-react'
import { getApiBase } from '@/api'
import { saveBlob } from '@/lib/download'

/**
 * How much of each slide is each cluster.
 *
 * One stacked bar per slide, segments in cluster order, full width — so the
 * question "is this slide mostly cluster 3?" is answered by looking, and
 * "do these two slides have the same mix?" by comparing two rows. Sorting by a
 * cluster's share is the one control, because the useful follow-up is always
 * "which slides have the most of THIS one".
 *
 * Percentages are of each slide's CLASSIFIED patches, so every bar fills. Slides
 * with unclassified patches (HDBSCAN noise) say so on the right rather than
 * having that silently absorbed into a cluster.
 */

interface Row {
  slide_hash: string
  display_name: string
  n_patches: number
  n_noise: number
  counts: number[]
  pct: number[]
}

interface Props {
  clusteringId: number
  title: string
  clusterColor: (i: number) => string
  onClose: () => void
  /** Open this slide in the workspace viewer. */
  onPick?: (slideHash: string) => void
  /** Whether this run is published to the slide details dialog. */
  showOnSlides?: boolean
  /** Publish or unpublish this run; the parent owns the request. */
  onShowOnSlidesChange?: (next: boolean) => void
}

export function ClusterComposition({
  clusteringId, title, clusterColor, onClose, onPick,
  showOnSlides = false, onShowOnSlidesChange,
}: Props) {
  const [rows, setRows] = useState<Row[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [sortBy, setSortBy] = useState<number | 'name'>('name')

  useEffect(() => {
    let cancelled = false
    setRows(null); setError(null)
    fetch(`${getApiBase()}/clusterings/${clusteringId}/composition`)
      .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then(d => { if (!cancelled) setRows(d.slides) })
      .catch(e => { if (!cancelled) setError(e.message) })
    return () => { cancelled = true }
  }, [clusteringId])

  const sorted = useMemo(() => {
    if (!rows) return []
    const r = [...rows]
    if (sortBy === 'name') {
      return r.sort((a, b) => (a.display_name || a.slide_hash).localeCompare(b.display_name || b.slide_hash))
    }
    return r.sort((a, b) => (b.pct[sortBy] ?? 0) - (a.pct[sortBy] ?? 0))
  }, [rows, sortBy])

  const k = rows?.[0]?.pct.length ?? 0

  const download = () => {
    if (!rows) return
    // Counts as well as percentages: percentages are rounded for display, and
    // anything downstream that weights slides or pools them needs the raw
    // numbers back. Rows follow the current sort, so sorting by a cluster and
    // exporting gives a file already ranked by it.
    const head = ['slide', 'slide_hash', 'patches', 'unclassified',
                  ...Array.from({ length: k }, (_, i) => `cluster_${i + 1}_n`),
                  ...Array.from({ length: k }, (_, i) => `cluster_${i + 1}_pct`)]
    const body = sorted.map(r => [
      JSON.stringify(r.display_name || r.slide_hash.slice(0, 12)),
      r.slide_hash, r.n_patches, r.n_noise, ...r.counts, ...r.pct,
    ].join(','))
    saveBlob(new Blob([[head.join(','), ...body].join('\n')], { type: 'text/csv' }),
             `cluster-composition-${clusteringId}.csv`)
  }

  return (
    <div className="fixed inset-0 z-110 flex flex-col bg-neutral-950/97 text-neutral-100">
      <div className="flex shrink-0 items-center gap-3 border-b border-neutral-800 px-4 py-2">
        <span className="text-sm font-medium">Cluster composition</span>
        <span className="text-[11px] text-neutral-400">{title} · share of each slide</span>

        <div className="ml-auto flex items-center gap-2">
          <label className="flex items-center gap-1.5 text-[11px] text-neutral-400">
            <ArrowDownUp className="h-3 w-3" />
            sort by
            <select
              value={String(sortBy)}
              onChange={e => setSortBy(e.target.value === 'name' ? 'name' : Number(e.target.value))}
              className="rounded border border-neutral-700 bg-neutral-900 px-1.5 py-1 text-[12px] text-neutral-100"
            >
              <option value="name">slide</option>
              {Array.from({ length: k }, (_, i) => (
                <option key={i} value={i}>cluster {i + 1}</option>
              ))}
            </select>
          </label>
          {onShowOnSlidesChange && (
            <button
              onClick={() => onShowOnSlidesChange(!showOnSlides)}
              className={`flex items-center gap-1.5 rounded border px-2 py-1 text-[11px] ${
                showOnSlides
                  ? 'border-emerald-700 bg-emerald-950/60 text-emerald-300 hover:bg-emerald-900/60'
                  : 'border-neutral-700 text-neutral-300 hover:bg-neutral-800'}`}
              title={showOnSlides
                ? 'This run appears under Cluster composition in slide details. Click to stop showing it.'
                : 'Show this run under Cluster composition when a slide is opened anywhere in the app'}
            >
              {showOnSlides ? <Eye className="h-3 w-3" /> : <EyeOff className="h-3 w-3" />}
              {showOnSlides ? 'on slides' : 'not on slides'}
            </button>
          )}
          <button onClick={download} disabled={!rows}
                  className="flex items-center gap-1.5 rounded border border-neutral-700 px-2 py-1
                             text-[11px] text-neutral-300 hover:bg-neutral-800 disabled:opacity-50">
            <Download className="h-3 w-3" /> CSV
          </button>
          <button onClick={onClose} className="rounded p-1 hover:bg-neutral-800" title="Close (Esc)">
            <X className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Legend — the only key the bars need. */}
      {k > 0 && (
        <div className="flex shrink-0 flex-wrap items-center gap-3 border-b border-neutral-800 px-4 py-1.5">
          {Array.from({ length: k }, (_, i) => (
            <span key={i} className="flex items-center gap-1.5 text-[11px] text-neutral-400">
              <span className="h-2.5 w-2.5 rounded-sm" style={{ background: clusterColor(i) }} />
              Cluster {i + 1}
            </span>
          ))}
        </div>
      )}

      <div className="min-h-0 flex-1 overflow-auto px-4 py-2">
        {error ? <div className="p-4 text-sm text-red-400">Could not load composition: {error}</div>
          : !rows ? <div className="flex items-center gap-2 p-4 text-sm text-neutral-400">
                      <Loader2 className="h-4 w-4 animate-spin" /> loading…
                    </div>
            : sorted.map(r => (
              <button
                key={r.slide_hash}
                type="button"
                onClick={() => onPick?.(r.slide_hash)}
                disabled={!onPick}
                className="flex w-full items-center gap-3 rounded px-1 py-[3px] text-left
                           enabled:hover:bg-neutral-900 disabled:cursor-default"
              >
                <span className="w-44 shrink-0 truncate font-mono text-[11px] text-neutral-300"
                      title={r.display_name || r.slide_hash}>
                  {r.display_name || r.slide_hash.slice(0, 12)}
                </span>

                <span className="flex h-4 flex-1 overflow-hidden rounded-sm bg-neutral-900">
                  {r.pct.map((p, i) => p > 0 && (
                    <span key={i} style={{ width: `${p}%`, background: clusterColor(i) }}
                          title={`Cluster ${i + 1}: ${p.toFixed(1)}% (${r.counts[i].toLocaleString()} patches)`} />
                  ))}
                </span>

                <span className="w-28 shrink-0 text-right text-[10px] tabular-nums text-neutral-500">
                  {r.n_patches.toLocaleString()}
                  {r.n_noise > 0 && (
                    <span className="ml-1 text-amber-500/80"
                          title={`${r.n_noise.toLocaleString()} patches unclassified`}>
                      ·{Math.round(100 * r.n_noise / r.n_patches)}% n/c
                    </span>
                  )}
                </span>
              </button>
            ))}
      </div>
    </div>
  )
}
