import { useEffect, useState } from 'react'
import { Loader2, X } from 'lucide-react'
import { getApiBase } from '@/api'
import { usePatchImage } from '@/lib/patchImages'

/**
 * What each cluster actually looks like.
 *
 * For every cluster of the selected k-means run, the patches nearest its centroid
 * (GET /clusterings/{id}/tiles), spread across slides so a row isn't eight crops of one
 * slide. That is the quickest way to give a cluster a name: read the row, write it down.
 *
 * Clusters are numbered 1-based here, as everywhere else in the UI; the label files
 * themselves are 0-based.
 */

interface Tile {
  slide_hash: string
  display_name?: string | null
  x: number
  y: number
  size: number
  dist: number
}

type Row = Tile[] | 'loading' | 'error'

interface Props {
  clusteringId: number
  title: string
  nClusters: number
  clusterColor: (i: number) => string
  onClose: () => void
  /** Show this patch in the workspace viewer (click a tile). */
  onPick?: (t: Tile) => void
}

const PER_CLUSTER = [6, 8, 10, 12, 16]

function Thumb({ tile, onPick }: { tile: Tile; onPick?: (t: Tile) => void }) {
  const { url, error } = usePatchImage(tile, 160)
  const title = `${tile.display_name || tile.slide_hash.slice(0, 10)} · (${tile.x}, ${tile.y})`
    + ` · distance to centre ${tile.dist.toFixed(2)}`
  return (
    <button type="button" title={title} onClick={() => onPick?.(tile)} disabled={!onPick}
            className="aspect-square overflow-hidden rounded border border-neutral-800 bg-neutral-900
                       enabled:hover:border-neutral-500 disabled:cursor-default">
      {url ? <img src={url} alt="" className="h-full w-full object-cover" />
        : error ? <div className="p-1 text-[9px] text-red-400">{error}</div>
          : <div className="flex h-full items-center justify-center"><Loader2 className="h-3 w-3 animate-spin text-neutral-600" /></div>}
    </button>
  )
}

export function ClusterGallery({ clusteringId, title, nClusters, clusterColor, onClose, onPick }: Props) {
  const [perCluster, setPerCluster] = useState(10)
  const [rows, setRows] = useState<Record<number, Row>>({})

  useEffect(() => {
    let cancelled = false
    setRows({})
    ;(async () => {
      // One cluster at a time: every tile is a crop read from a slide file, and firing all
      // of them at once makes the first row wait for the last.
      for (let c = 0; c < nClusters; c++) {
        if (cancelled) return
        setRows(r => ({ ...r, [c]: 'loading' }))
        try {
          const res = await fetch(`${getApiBase()}/clusterings/${clusteringId}/tiles?cluster=${c}&n=${perCluster}`)
          const data = res.ok ? await res.json() : null
          if (!cancelled) setRows(r => ({ ...r, [c]: (data?.tiles as Tile[]) ?? 'error' }))
        } catch {
          if (!cancelled) setRows(r => ({ ...r, [c]: 'error' }))
        }
      }
    })()
    return () => { cancelled = true }
  }, [clusteringId, nClusters, perCluster])

  return (
    <div className="fixed inset-0 z-110 flex flex-col bg-neutral-950/97 text-neutral-100">
      <div className="flex shrink-0 items-center gap-3 border-b border-neutral-800 px-4 py-2">
        <span className="text-sm font-medium">Representative patches</span>
        <span className="text-[11px] text-neutral-400">{title} · nearest each cluster centre, spread across slides</span>
        <label className="ml-auto flex items-center gap-1.5 text-[11px] text-neutral-400">
          per cluster
          <select value={perCluster} onChange={e => setPerCluster(Number(e.target.value))}
                  className="rounded border border-neutral-700 bg-neutral-900 px-1.5 py-1 text-[12px] text-neutral-100">
            {PER_CLUSTER.map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </label>
        <button onClick={onClose} className="rounded p-1 hover:bg-neutral-800" title="Close (Esc)">
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="min-h-0 flex-1 overflow-auto px-4 py-3">
        {Array.from({ length: nClusters }, (_, c) => {
          const row = rows[c]
          return (
            <div key={c} className="mb-3 flex items-start gap-3">
              <div className="flex w-28 shrink-0 items-center gap-2 pt-1">
                <span className="h-3.5 w-3.5 shrink-0 rounded" style={{ background: clusterColor(c) }} />
                <span className="text-[13px] font-medium">Cluster {c + 1}</span>
              </div>
              <div className="grid flex-1 gap-1.5"
                   style={{ gridTemplateColumns: `repeat(${perCluster}, minmax(0, 1fr))` }}>
                {Array.isArray(row)
                  ? (row.length
                    ? row.map((t, i) => <Thumb key={`${t.slide_hash}-${t.x}-${t.y}-${i}`} tile={t} onPick={onPick} />)
                    : <span className="text-[11px] text-neutral-500">no patches</span>)
                  : row === 'error'
                    ? <span className="text-[11px] text-red-400">could not load patches</span>
                    : <span className="flex items-center gap-1.5 text-[11px] text-neutral-500">
                        <Loader2 className="h-3 w-3 animate-spin" /> loading…
                      </span>}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
