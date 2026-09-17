import { useEffect, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { Download, Loader2, X } from 'lucide-react'
import { getApiBase } from '@/api'
import { saveBlob } from '@/lib/download'
import type { CompositionClustering } from './CompositionPanel'

interface Props {
  open: boolean
  onOpenChange: (open: boolean) => void
  overlayId: number
  clusterings: CompositionClustering[]
  groupA: number | null
  groupB: number | null
  groupAName: string
  groupBName: string
  schemeId: number | null
  groups: { id: number; name: string; scheme: string }[]
  excluded: Record<number, number[]>
  onExportPaired?: () => void
}

const selectCls = 'w-full rounded border border-neutral-700 bg-neutral-900 px-2 py-2 text-sm text-neutral-100'

export function CompositionExportDialog(p: Props) {
  const [level, setLevel] = useState('slide')
  const [layout, setLayout] = useState('wide')
  const [scope, setScope] = useState('all')
  const [chosen, setChosen] = useState<Set<number>>(new Set())
  const [excludeFar, setExcludeFar] = useState(false)
  const [applyExclusions, setApplyExclusions] = useState(false)
  const [restrictId, setRestrictId] = useState<number | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const canGroup = p.groupA != null && p.groupB != null && p.groupA !== p.groupB
  useEffect(() => {
    if (p.open) { setChosen(new Set(p.clusterings.map(c => c.id))); setError('') }
    // Initialize the selection on opening, not on each parent render.
  }, [p.open])

  async function download() {
    setBusy(true); setError('')
    try {
      const response = await fetch(`${getApiBase()}/overlays/${p.overlayId}/composition/export`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          clustering_ids: p.clusterings.filter(c => chosen.has(c.id)).map(c => c.id),
          level, layout, scope, scheme_id: p.schemeId,
          group_a_id: canGroup ? p.groupA : null, group_b_id: canGroup ? p.groupB : null,
          exclude_far: excludeFar, exclude_clusters: applyExclusions ? p.excluded : {},
          restrict_group_id: restrictId,
        }),
      })
      if (!response.ok) {
        const data = await response.json().catch(() => null)
        throw new Error(data?.detail || `Export failed (${response.status})`)
      }
      saveBlob(await response.blob(), `composition-per-${level}-${layout}.csv`)
      p.onOpenChange(false)
    } catch (e) { setError(e instanceof Error ? e.message : 'Export failed') }
    finally { setBusy(false) }
  }

  return (
    <Dialog.Root open={p.open} onOpenChange={open => { if (!busy) p.onOpenChange(open) }}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-[150] bg-black/70" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-[151] max-h-[90vh] w-[min(520px,94vw)] -translate-x-1/2 -translate-y-1/2 overflow-auto rounded-lg border border-neutral-700 bg-neutral-950 p-5 text-neutral-100 shadow-2xl">
          <Dialog.Title className="text-base font-semibold">Download cluster data</Dialog.Title>
          <Dialog.Description className="mt-2 text-sm text-neutral-400">
            Raw patch counts and percentages for every cluster. Includes unpaired patients; no comparison run is required.
          </Dialog.Description>
          <Dialog.Close disabled={busy} aria-label="Close export window" className="absolute right-3 top-3 p-1"><X className="h-4 w-4" /></Dialog.Close>
          <fieldset disabled={busy} className="mt-4 space-y-4 disabled:opacity-60">
            <label className="block space-y-1 text-sm">Rows grouped by
              <select className={selectCls} value={level} onChange={e => setLevel(e.target.value)}>
                <option value="slide">Slide</option><option value="case">Case</option><option value="patient">Patient</option>
              </select>
            </label>
            <label className="block space-y-1 text-sm">Include
              <select className={selectCls} value={scope} onChange={e => setScope(e.target.value)}>
                <option value="all">Every slide in this overlay</option>
                <option value="groups" disabled={!canGroup}>Selected groups: {p.groupAName} / {p.groupBName}</option>
              </select>
            </label>
            <label className="block space-y-1 text-sm">CSV layout
              <select className={selectCls} value={layout} onChange={e => setLayout(e.target.value)}>
                <option value="wide">Wide — count and percentage columns for each cluster</option>
                <option value="long">Long — one row per cluster</option>
              </select>
            </label>
            <fieldset className="space-y-1">
              <legend className="mb-1 text-sm">Clusterings</legend>
              {p.clusterings.map(c => <label key={c.id} className="flex items-center gap-2 text-sm">
                <input type="checkbox" checked={chosen.has(c.id)} onChange={() => setChosen(prev => {
                  const next = new Set(prev); if (next.has(c.id)) next.delete(c.id); else next.add(c.id); return next
                })} />{c.label}
              </label>)}
            </fieldset>
            <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={excludeFar} onChange={e => setExcludeFar(e.target.checked)} />Exclude far patches</label>
            <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={applyExclusions} onChange={e => setApplyExclusions(e.target.checked)} />Apply the panel’s excluded clusters</label>
            <label className="block space-y-1 text-sm">Restrict slides
              <select className={selectCls} value={restrictId ?? ''} onChange={e => setRestrictId(e.target.value ? Number(e.target.value) : null)}>
                <option value="">No restriction</option>
                {p.groups.map(g => <option key={g.id} value={g.id}>{g.scheme}: {g.name}</option>)}
              </select>
            </label>
          </fieldset>
          <p className="mt-4 text-xs text-neutral-400">
            Percentages are cluster count ÷ retained patch count, without pseudocounts or weighting.
            Case and patient counts are pooled within each group. Unassigned patients stay separate by case.
            Each clustering has its own rows. Blank percentages mean no retained patches; blank cluster columns mean unavailable or excluded.
            Only slides with data in this overlay are included.
          </p>
          {error && <p role="alert" className="mt-3 text-sm text-red-400">{error}</p>}
          <div className="mt-4 flex flex-wrap items-center justify-end gap-3">
            {p.onExportPaired && <button disabled={busy} onClick={p.onExportPaired} className="mr-auto text-xs text-neutral-400 underline">Paired summary CSV</button>}
            <button disabled={busy || chosen.size === 0 || (scope === 'groups' && !canGroup)} onClick={download}
              className="inline-flex items-center gap-2 rounded bg-neutral-100 px-3 py-2 text-sm font-medium text-neutral-900 disabled:opacity-40">
              {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />} Download CSV
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
