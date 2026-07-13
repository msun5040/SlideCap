import { useEffect, useMemo, useState } from 'react'
import { ChevronDown, ChevronRight, Folder, FileText, Loader2 } from 'lucide-react'
import { Checkbox } from '@/components/ui/checkbox'
import { getApiBase } from '@/api'

// One selected slide that has the chosen analysis. `key` is `${job_id}:${slide_hash}`.
export interface PickTarget {
  key: string
  slide_hash: string
  job_id: number
  label: string
}

interface FileTreeNode {
  name: string
  type: 'file' | 'dir'
  path: string
  size?: number
  is_image?: boolean
  children?: FileTreeNode[]
}

interface Props {
  /** Selected slides (with the chosen analysis) whose outputs to browse. */
  targets: PickTarget[]
  /** slideKey → set of selected relpaths. Controlled by the parent. */
  value: Record<string, string[]>
  onChange: (next: Record<string, string[]>) => void
}

function formatBytes(bytes?: number): string {
  if (!bytes) return ''
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

// Coarse "file type" used for the pattern chips. Prefers known compound
// suffixes (a UNI geojson is often .geojson.snappy) over the last dot.
export function fileExt(name: string): string {
  const lower = name.toLowerCase()
  for (const d of ['.geojson.snappy', '.geojson.gz', '.json.gz', '.tar.gz']) {
    if (lower.endsWith(d)) return d
  }
  const i = lower.lastIndexOf('.')
  return i >= 0 ? lower.slice(i) : '(no ext)'
}

function flattenFiles(nodes: FileTreeNode[]): FileTreeNode[] {
  const out: FileTreeNode[] = []
  const walk = (ns: FileTreeNode[]) => {
    for (const n of ns) {
      if (n.type === 'dir') walk(n.children || [])
      else out.push(n)
    }
  }
  walk(nodes)
  return out
}

/**
 * Batch file picker for extracting analysis outputs across a cohort.
 *
 * Fetches the output tree for every target slide (one round trip via
 * /analyses/pull-inspect), then offers two ways to choose files, per the
 * "both" requirement:
 *   1. Type/pattern chips — the union of file extensions across the cohort
 *      (".h5", ".geojson.snappy", …). Toggling a chip selects/clears that
 *      type on every slide at once — the "grab all UNI .h5" path.
 *   2. Per-slide trees — expand a slide and tick individual files.
 *
 * Selection is reported up as slideKey → relpaths[]; the parent turns that
 * into export items.
 */
export function AnalysisFilePicker({ targets, value, onChange }: Props) {
  const [trees, setTrees] = useState<Record<string, FileTreeNode[]>>({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [expandedSlides, setExpandedSlides] = useState<Set<string>>(new Set())

  const targetsKey = targets.map(t => t.key).sort().join('|')

  useEffect(() => {
    if (targets.length === 0) {
      setTrees({})
      return
    }
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    fetch(`${getApiBase()}/analyses/pull-inspect`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: targets.map(t => ({ slide_hash: t.slide_hash, job_id: t.job_id })) }),
      signal: ac.signal,
    })
      .then(async r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data: { trees: Record<string, FileTreeNode[]> }) => {
        setTrees(data.trees || {})
        setLoading(false)
      })
      .catch(e => {
        if (e.name === 'AbortError') return
        setError(e.message || 'Failed to inspect analysis outputs')
        setLoading(false)
      })
    return () => ac.abort()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [targetsKey])

  // Per-target flattened files, and the union of file types across the cohort.
  const flat = useMemo(() => {
    const m: Record<string, FileTreeNode[]> = {}
    for (const t of targets) m[t.key] = flattenFiles(trees[t.key] || [])
    return m
  }, [trees, targetsKey])

  const extStats = useMemo(() => {
    const counts = new Map<string, number>()
    for (const t of targets) {
      for (const f of flat[t.key] || []) {
        const e = fileExt(f.name)
        counts.set(e, (counts.get(e) || 0) + 1)
      }
    }
    return Array.from(counts.entries()).sort((a, b) => b[1] - a[1])
  }, [flat, targetsKey])

  const selectedCount = Object.values(value).reduce((n, arr) => n + arr.length, 0)

  // Is every file of this extension (across all targets) currently selected?
  const extActive = (ext: string): boolean => {
    let any = false
    for (const t of targets) {
      const sel = new Set(value[t.key] || [])
      for (const f of flat[t.key] || []) {
        if (fileExt(f.name) === ext) {
          any = true
          if (!sel.has(f.path)) return false
        }
      }
    }
    return any
  }

  const toggleExt = (ext: string) => {
    const active = extActive(ext)
    const next: Record<string, string[]> = { ...value }
    for (const t of targets) {
      const sel = new Set(next[t.key] || [])
      for (const f of flat[t.key] || []) {
        if (fileExt(f.name) === ext) {
          if (active) sel.delete(f.path)
          else sel.add(f.path)
        }
      }
      next[t.key] = Array.from(sel)
    }
    onChange(next)
  }

  const toggleFile = (key: string, path: string) => {
    const sel = new Set(value[key] || [])
    if (sel.has(path)) sel.delete(path)
    else sel.add(path)
    onChange({ ...value, [key]: Array.from(sel) })
  }

  const toggleSlideExpand = (key: string) => {
    setExpandedSlides(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  if (targets.length === 0) {
    return <p className="text-[12px] text-muted-foreground">No selected slides have this analysis.</p>
  }
  if (loading) {
    return (
      <div className="flex items-center gap-2 text-[12px] text-muted-foreground py-3">
        <Loader2 className="h-3.5 w-3.5 animate-spin" /> Inspecting outputs for {targets.length} slide{targets.length === 1 ? '' : 's'}…
      </div>
    )
  }
  if (error) {
    return <p className="text-[12px] text-red-600 py-2">Could not inspect outputs: {error}</p>
  }

  return (
    <div className="space-y-2">
      {/* Type/pattern chips — select a file type across the whole cohort */}
      <div>
        <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide mb-1">File types</p>
        {extStats.length === 0 ? (
          <p className="text-[12px] text-muted-foreground">No output files found on disk for these slides.</p>
        ) : (
          <div className="flex flex-wrap gap-1.5">
            {extStats.map(([ext, count]) => {
              const active = extActive(ext)
              return (
                <button
                  key={ext}
                  type="button"
                  onClick={() => toggleExt(ext)}
                  className={`text-[12px] px-2 py-1 rounded border font-mono transition-colors ${
                    active
                      ? 'bg-primary/10 text-primary border-primary/40'
                      : 'bg-background text-muted-foreground border-gray-300 hover:bg-muted/30'
                  }`}
                  title={active ? `Deselect all ${ext} files` : `Select all ${ext} files`}
                >
                  {ext} <span className="opacity-60">({count})</span>
                </button>
              )
            })}
          </div>
        )}
      </div>

      {/* Per-slide hand-pick */}
      <div className="border border-gray-300 rounded-md divide-y divide-gray-100 max-h-64 overflow-y-auto">
        {targets.map(t => {
          const files = flat[t.key] || []
          const sel = new Set(value[t.key] || [])
          const open = expandedSlides.has(t.key)
          const nSel = files.filter(f => sel.has(f.path)).length
          return (
            <div key={t.key}>
              <button
                type="button"
                onClick={() => toggleSlideExpand(t.key)}
                className="w-full flex items-center gap-1.5 px-2 py-1.5 text-left hover:bg-muted/30"
              >
                {open ? <ChevronDown className="h-3.5 w-3.5 shrink-0" /> : <ChevronRight className="h-3.5 w-3.5 shrink-0" />}
                <Folder className="h-3.5 w-3.5 text-amber-500 shrink-0" />
                <span className="text-[12px] font-mono truncate flex-1" title={t.label}>{t.label}</span>
                <span className="text-[11px] text-muted-foreground shrink-0">
                  {nSel > 0 ? `${nSel}/${files.length}` : `${files.length} files`}
                </span>
              </button>
              {open && (
                files.length === 0 ? (
                  <p className="text-[11px] text-muted-foreground pl-8 pb-1.5">No files on disk.</p>
                ) : (
                  <div className="pb-1">
                    {files.map(f => (
                      <label
                        key={f.path}
                        className="flex items-center gap-2 pl-8 pr-2 py-0.5 hover:bg-muted/20 cursor-pointer"
                      >
                        <Checkbox
                          checked={sel.has(f.path)}
                          onCheckedChange={() => toggleFile(t.key, f.path)}
                        />
                        <FileText className="h-3 w-3 text-muted-foreground shrink-0" />
                        <span className="text-[11px] font-mono truncate flex-1" title={f.path}>{f.path}</span>
                        <span className="text-[10px] text-muted-foreground shrink-0">{formatBytes(f.size)}</span>
                      </label>
                    ))}
                  </div>
                )
              )}
            </div>
          )
        })}
      </div>

      <p className="text-[11px] text-muted-foreground">
        {selectedCount === 0
          ? 'No files selected yet — pick a type above or expand a slide.'
          : `${selectedCount} file${selectedCount === 1 ? '' : 's'} selected across ${targets.length} slide${targets.length === 1 ? '' : 's'}.`}
      </p>
    </div>
  )
}
