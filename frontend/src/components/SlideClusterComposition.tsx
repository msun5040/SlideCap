import { useEffect, useState } from 'react'
import { Loader2 } from 'lucide-react'
import { getApiBase } from '@/api'
import { clusterColor } from '@/lib/clusterColors'

/**
 * This slide's cluster mix, under the clusterings published to slide details.
 *
 * The slide-centric view of what the workspace shows cohort-wide: open a slide
 * anywhere in the app and see what is in it, without first finding the right
 * projection. One stacked bar per clustering, so a slide published under
 * several reads as several rows.
 *
 * Shows only runs someone published from the workspace (the eye toggle on a
 * run). A projection collects several — a UMAP at k=60, a t-SNE at k=50, plus
 * whatever was tried along the way — and this dialog opens everywhere, so none
 * of them appear here until they are chosen.
 *
 * Renders nothing at all when no published clustering covers the slide, so it
 * costs the dialog no space until it has something to say.
 */

interface Row {
  clustering_id: number
  cohort_name: string | null
  method: string
  algorithm: string
  label: string
  n_clusters: number
  n_patches: number
  n_noise: number
  counts: number[]
  pct: number[]
}

export function SlideClusterComposition({ slideHash }: { slideHash: string }) {
  const [rows, setRows] = useState<Row[] | null>(null)

  useEffect(() => {
    let cancelled = false
    setRows(null)
    fetch(`${getApiBase()}/slides/${slideHash}/cluster-composition`)
      .then(r => (r.ok ? r.json() : null))
      .then(d => { if (!cancelled) setRows(d?.clusterings ?? []) })
      .catch(() => { if (!cancelled) setRows([]) })
    return () => { cancelled = true }
  }, [slideHash])

  if (rows === null) {
    return (
      <div className="col-span-2 border-t pt-3">
        <label className="text-sm text-muted-foreground">Cluster composition</label>
        <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
          <Loader2 className="h-3 w-3 animate-spin" /> loading…
        </div>
      </div>
    )
  }
  if (rows.length === 0) return null

  return (
    <div className="col-span-2 border-t pt-3">
      <label className="text-sm text-muted-foreground">Cluster composition</label>
      <div className="mt-1 space-y-2">
        {rows.map(r => (
          <div key={r.clustering_id}>
            <div className="flex items-baseline justify-between text-xs">
              <span className="truncate">
                {r.label || `${r.algorithm} k=${r.n_clusters}`}
                {r.cohort_name && (
                  <span className="ml-1.5 text-muted-foreground">
                    {r.cohort_name} · {r.method.toUpperCase()}
                  </span>
                )}
              </span>
              <span className="shrink-0 tabular-nums text-muted-foreground">
                {r.n_patches.toLocaleString()} patches
                {r.n_noise > 0 && ` · ${Math.round(100 * r.n_noise / r.n_patches)}% unclassified`}
              </span>
            </div>

            <div className="mt-1 flex h-3.5 overflow-hidden rounded-sm bg-muted">
              {r.pct.map((p, i) => p > 0 && (
                <div key={i} style={{ width: `${p}%`, background: clusterColor(i) }}
                     title={`Cluster ${i + 1}: ${p.toFixed(1)}% (${r.counts[i].toLocaleString()} patches)`} />
              ))}
            </div>

            {/* The two or three that actually matter, named. The rest are in the
                bar and its tooltips — listing all of them would bury the point. */}
            <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[11px] text-muted-foreground">
              {r.pct.map((p, i) => ({ p, i }))
                .sort((a, b) => b.p - a.p)
                .slice(0, 3)
                .filter(({ p }) => p > 0)
                .map(({ p, i }) => (
                  <span key={i} className="flex items-center gap-1">
                    <span className="h-2 w-2 rounded-[2px]" style={{ background: clusterColor(i) }} />
                    Cluster {i + 1} {p.toFixed(0)}%
                  </span>
                ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
