import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  ChartScatter, Check, EyeOff, Loader2, Play, Plus, RefreshCw, Tags, Trash2, X,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
import { SearchableSelect } from '@/components/ui/searchable-select'
import { CohortProjectionWorkspace } from '@/components/CohortProjectionWorkspace'
import { getApiBase } from '@/api'
import type { CohortDetail } from '@/types/slide'

/**
 * Cohort-level analysis workspace.
 *
 * Kept out of the Cohorts tab on purpose — cohort building is about membership,
 * this is about what you do with a cohort once it exists. You import a cohort
 * here (the same shape as Data Pull), label its slides, and run projections.
 *
 * The labels are only ever used to colour a plot; a projection is computed from
 * embeddings alone, so re-labelling never invalidates one.
 */

const PRESET_COLORS = [
  '#ef4444', '#3b82f6', '#22c55e', '#eab308', '#8b5cf6',
  '#ec4899', '#14b8a6', '#f97316', '#6366f1', '#84cc16',
]

interface CohortRow { id: number; name: string; slide_count?: number }

interface GroupRow {
  id: number
  name: string
  color?: string | null
  sort_order: number
  slide_count: number
  slide_hashes: string[]
}
interface SchemeRow {
  id: number
  name: string
  description?: string | null
  sort_order: number
  groups: GroupRow[]
}

interface ProjectionRow {
  id: number
  method: string
  status: string
  progress_pct: number
  progress_stage?: string | null
  error_message?: string | null
  point_count?: number | null
  slide_count: number
  analysis_name?: string | null
  created_at?: string | null
  params?: Record<string, unknown>
}

/** " · fit on N" when the embedding was fit on a subsample and the rest placed onto it (e.g. imported runs). */
function fitSampleNote(params: Record<string, unknown> | undefined, pointCount?: number | null): string {
  const n = params?.fit_sample_n
  return typeof n === 'number' && pointCount && n < pointCount ? ` · fit on ${n.toLocaleString()}` : ''
}

interface AnalysisStatusEntry {
  status: string
  job_id: number
  analysis_id?: number | null
  analysis_name?: string | null
}

// slide_hash → analysis key → best status across that slide's jobs
type AnalysisStatus = Record<string, Record<string, AnalysisStatusEntry>>

function analysisChipClass(status: string): string {
  if (status === 'completed') return 'border-transparent bg-emerald-600/15 text-emerald-700 dark:text-emerald-400'
  if (status === 'failed') return 'border-red-500/40 text-red-600'
  // pending / running / transferring
  return 'border-dashed border-muted-foreground/40 text-muted-foreground'
}

export function AnalysisWorkspace() {
  const [cohorts, setCohorts] = useState<CohortRow[]>([])
  const [cohortId, setCohortId] = useState<string>('')
  const [cohort, setCohort] = useState<CohortDetail | null>(null)
  const [analysisStatus, setAnalysisStatus] = useState<AnalysisStatus>({})
  const [loading, setLoading] = useState(false)

  const [schemes, setSchemes] = useState<SchemeRow[]>([])
  const [newSchemeName, setNewSchemeName] = useState('')
  const [newGroupName, setNewGroupName] = useState<Record<number, string>>({})
  const [activeSchemeId, setActiveSchemeId] = useState<number | null>(null)
  const [selectedHashes, setSelectedHashes] = useState<Set<string>>(new Set())

  const [projections, setProjections] = useState<ProjectionRow[]>([])
  const [method, setMethod] = useState<'umap' | 'tsne' | 'pca'>('umap')
  const [starting, setStarting] = useState(false)
  const [runError, setRunError] = useState('')
  const [openProjection, setOpenProjection] = useState<number | null>(null)
  // Held-out cases (set in Cohorts) are left out of runs unless included here.
  const [includeHeldOut, setIncludeHeldOut] = useState(false)

  useEffect(() => {
    fetch(`${getApiBase()}/cohorts`)
      .then(r => (r.ok ? r.json() : []))
      .then(setCohorts)
      .catch(e => console.error('Failed to load cohorts:', e))
  }, [])

  const numericCohortId = cohortId ? Number(cohortId) : null

  const loadCohort = useCallback(async (id: number) => {
    setLoading(true)
    try {
      const [detail, status, sch, projs] = await Promise.all([
        fetch(`${getApiBase()}/cohorts/${id}`).then(r => (r.ok ? r.json() : null)),
        fetch(`${getApiBase()}/cohorts/${id}/analysis-status`).then(r => (r.ok ? r.json() : { slides: {} })),
        fetch(`${getApiBase()}/cohorts/${id}/group-schemes`).then(r => (r.ok ? r.json() : [])),
        fetch(`${getApiBase()}/cohorts/${id}/projections`).then(r => (r.ok ? r.json() : [])),
      ])
      setCohort(detail)
      setAnalysisStatus(status?.slides || {})
      setSchemes(sch)
      setProjections(projs)
      setActiveSchemeId(sch.length > 0 ? sch[0].id : null)
      setSelectedHashes(new Set())
      setIncludeHeldOut(false)
    } catch (e) {
      console.error('Failed to load cohort:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    if (numericCohortId) loadCohort(numericCohortId)
  }, [numericCohortId, loadCohort])

  // Poll while anything is still computing.
  useEffect(() => {
    const busy = projections.some(p => p.status === 'pending' || p.status === 'running')
    if (!busy || !numericCohortId) return
    const t = setInterval(() => {
      fetch(`${getApiBase()}/cohorts/${numericCohortId}/projections`)
        .then(r => (r.ok ? r.json() : null))
        .then(rows => { if (rows) setProjections(rows) })
        .catch(() => {})
    }, 2000)
    return () => clearInterval(t)
  }, [projections, numericCohortId])

  // ── Readiness: which slides actually have output to project ──────────
  // case_hash → reason for cases held out of analysis in the Cohorts section.
  const heldOut = useMemo(
    () => new Map((cohort?.held_out_cases ?? []).map(h => [h.case_hash, h.reason ?? null] as const)),
    [cohort],
  )

  const readiness = useMemo(() => {
    const slides = cohort?.slides || []
    const ready: string[] = []
    const notReady: string[] = []
    const heldSlides: string[] = []
    const heldCases = new Set<string>()
    for (const s of slides) {
      if (s.case_hash && heldOut.has(s.case_hash)) {
        heldSlides.push(s.slide_hash)
        heldCases.add(s.case_hash)
        if (!includeHeldOut) continue
      }
      const entries = Object.values(analysisStatus[s.slide_hash] || {})
      if (entries.some(e => e.status === 'completed')) ready.push(s.slide_hash)
      else notReady.push(s.slide_hash)
    }
    return {
      ready, notReady,
      total: ready.length + notReady.length,
      heldSlideCount: heldSlides.length,
      heldCaseCount: heldCases.size,
    }
  }, [cohort, analysisStatus, heldOut, includeHeldOut])

  // ── Scheme / group mutations ─────────────────────────────────────────
  const refreshSchemes = useCallback(async (selectId?: number) => {
    if (!numericCohortId) return
    const r = await fetch(`${getApiBase()}/cohorts/${numericCohortId}/group-schemes`)
    if (!r.ok) return
    const rows: SchemeRow[] = await r.json()
    setSchemes(rows)
    // Keep a scheme selected, otherwise the group editor silently has nothing
    // to attach to and creating the first scheme looks like it did nothing.
    setActiveSchemeId(prev => {
      if (selectId != null) return selectId
      if (prev != null && rows.some(s => s.id === prev)) return prev
      return rows.length > 0 ? rows[0].id : null
    })
  }, [numericCohortId])

  const createScheme = async () => {
    if (!numericCohortId || !newSchemeName.trim()) return
    const res = await fetch(`${getApiBase()}/cohorts/${numericCohortId}/group-schemes`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: newSchemeName.trim() }),
    })
    setNewSchemeName('')
    const created = res.ok ? await res.json().catch(() => null) : null
    await refreshSchemes(created?.id)
  }

  const deleteScheme = async (id: number) => {
    if (!numericCohortId) return
    await fetch(`${getApiBase()}/cohorts/${numericCohortId}/group-schemes/${id}`, { method: 'DELETE' })
    if (activeSchemeId === id) setActiveSchemeId(null)
    await refreshSchemes()
  }

  const createGroup = async (schemeId: number) => {
    const name = (newGroupName[schemeId] || '').trim()
    if (!numericCohortId || !name) return
    const scheme = schemes.find(s => s.id === schemeId)
    const color = PRESET_COLORS[(scheme?.groups.length || 0) % PRESET_COLORS.length]
    await fetch(`${getApiBase()}/cohorts/${numericCohortId}/group-schemes/${schemeId}/groups`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, color }),
    })
    setNewGroupName(p => ({ ...p, [schemeId]: '' }))
    await refreshSchemes()
  }

  const deleteGroup = async (groupId: number) => {
    if (!numericCohortId) return
    await fetch(`${getApiBase()}/cohorts/${numericCohortId}/groups/${groupId}`, { method: 'DELETE' })
    await refreshSchemes()
  }

  const assignSelected = async (groupId: number) => {
    if (!numericCohortId || selectedHashes.size === 0) return
    await fetch(`${getApiBase()}/cohorts/${numericCohortId}/groups/${groupId}/slides`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ add_slide_hashes: Array.from(selectedHashes) }),
    })
    setSelectedHashes(new Set())
    await refreshSchemes()
  }

  const activeScheme = schemes.find(s => s.id === activeSchemeId) || null

  /** slide_hash -> group in the active scheme, for the per-slide chips. */
  const groupOfSlide = useMemo(() => {
    const m = new Map<string, GroupRow>()
    activeScheme?.groups.forEach(g => g.slide_hashes.forEach(h => m.set(h, g)))
    return m
  }, [activeScheme])

  const toggleSlide = (hash: string) => {
    setSelectedHashes(prev => {
      const next = new Set(prev)
      if (next.has(hash)) next.delete(hash); else next.add(hash)
      return next
    })
  }

  const selectCase = (caseHash: string | null | undefined) => {
    if (!cohort) return
    const hashes = cohort.slides.filter(s => s.case_hash === caseHash).map(s => s.slide_hash)
    setSelectedHashes(prev => {
      const next = new Set(prev)
      const allIn = hashes.every(h => next.has(h))
      hashes.forEach(h => (allIn ? next.delete(h) : next.add(h)))
      return next
    })
  }

  // ── Run a projection ─────────────────────────────────────────────────
  const runProjection = async () => {
    if (!numericCohortId) return
    setRunError(''); setStarting(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${numericCohortId}/projections`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ method, include_held_out: includeHeldOut }),
      })
      if (!res.ok) {
        const d = await res.json().catch(() => null)
        throw new Error(d?.detail || `Could not start projection (${res.status})`)
      }
      const row = await res.json()
      setProjections(p => [row, ...p])
    } catch (e: any) {
      setRunError(e.message || 'Could not start projection')
    } finally {
      setStarting(false)
    }
  }

  const deleteProjection = async (id: number) => {
    await fetch(`${getApiBase()}/projections/${id}`, { method: 'DELETE' })
    setProjections(p => p.filter(x => x.id !== id))
  }

  // Group cohort slides by case for the labelling list.
  const casesList = useMemo(() => {
    type CohortSlideRow = CohortDetail['slides'][number]
    const byCase = new Map<string, CohortSlideRow[]>()
    for (const s of cohort?.slides || []) {
      const k = s.case_hash || 'unknown'
      if (!byCase.has(k)) byCase.set(k, [])
      byCase.get(k)!.push(s)
    }
    return Array.from(byCase.entries()).sort((a, b) => {
      const an = a[1][0]?.accession_number || ''
      const bn = b[1][0]?.accession_number || ''
      return an.localeCompare(bn)
    })
  }, [cohort])

  return (
    <div className="flex h-full min-h-0 flex-col gap-4">
      <div>
        <h1 className="mb-1 text-2xl font-semibold">Analysis Workspace</h1>
        <p className="text-muted-foreground">
          Label a cohort and project every patch across its slides into 2D.
        </p>
      </div>

      {/* 1. Import */}
      <div className="flex flex-wrap items-end gap-3 rounded-lg border p-3">
        <div className="space-y-1.5">
          <label className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            Cohort
          </label>
          <SearchableSelect
            className="h-10 w-72"
            value={cohortId}
            onChange={setCohortId}
            placeholder="Import a cohort…"
            searchPlaceholder="Search cohorts…"
            emptyText="No cohorts yet"
            options={cohorts.map(c => ({
              value: String(c.id),
              label: c.name,
              hint: c.slide_count != null ? `${c.slide_count} slides` : undefined,
            }))}
          />
        </div>

        {cohort && (
          <>
            <div className="text-[12px]">
              <div className="font-medium">{readiness.ready.length} of {readiness.total} slides ready</div>
              <div className="text-muted-foreground">
                {readiness.notReady.length > 0
                  ? `${readiness.notReady.length} have no completed analysis and will be skipped`
                  : 'every slide has completed analysis output'}
              </div>
            </div>
            {readiness.heldCaseCount > 0 && (
              <label className="flex cursor-pointer items-center gap-2 rounded-md border border-dashed px-2 py-1.5 text-[12px]">
                <Checkbox
                  checked={includeHeldOut}
                  onCheckedChange={v => setIncludeHeldOut(v === true)}
                />
                <span>
                  <span className="font-medium">
                    {includeHeldOut ? 'Including' : 'Excluding'} {readiness.heldCaseCount} held-out
                    case{readiness.heldCaseCount === 1 ? '' : 's'}
                  </span>
                  <span className="block text-[11px] text-muted-foreground">
                    {readiness.heldSlideCount} slide{readiness.heldSlideCount === 1 ? '' : 's'} · held out in Cohorts ·
                    tick to include them in runs
                  </span>
                </span>
              </label>
            )}
            <Button variant="outline" size="sm"
                    onClick={() => numericCohortId && loadCohort(numericCohortId)}>
              <RefreshCw className="mr-1 h-3.5 w-3.5" />Refresh
            </Button>
          </>
        )}
        {loading && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
      </div>

      {cohort && (
        <div className="flex min-h-0 flex-1 gap-4">
          {/* 2. Labels */}
          <div className="flex min-h-0 w-[45%] flex-col gap-3 rounded-lg border p-3">
            <div className="flex items-center gap-2">
              <Tags className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm font-medium">Labels</span>
              <span className="text-[11px] text-muted-foreground">
                used only to colour the plot
              </span>
            </div>

            <div className="flex gap-2">
              <Input
                value={newSchemeName}
                onChange={e => setNewSchemeName(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && createScheme()}
                placeholder="New scheme, e.g. Treatment arm"
                className="h-8 text-[13px]"
              />
              <Button size="sm" className="h-8" onClick={createScheme} disabled={!newSchemeName.trim()}>
                <Plus className="h-3.5 w-3.5" />
              </Button>
            </div>

            {schemes.length === 0 && (
              <p className="text-[12px] text-muted-foreground">
                No schemes yet. A scheme is one axis of labelling — "Timepoint", "Treatment arm" —
                and its groups are the values.
              </p>
            )}

            {/* Scheme tabs */}
            {schemes.length > 0 && (
              <div className="flex flex-wrap gap-1">
                {schemes.map(s => (
                  <button
                    key={s.id}
                    onClick={() => setActiveSchemeId(s.id)}
                    className={`rounded border px-2 py-1 text-[12px] ${
                      activeSchemeId === s.id
                        ? 'border-primary bg-primary/10 text-primary'
                        : 'border-input hover:bg-muted/40'
                    }`}
                  >
                    {s.name}
                  </button>
                ))}
              </div>
            )}

            {activeScheme && (
              <div className="space-y-2">
                <div className="flex flex-wrap items-center gap-1.5">
                  {activeScheme.groups.map(g => (
                    <span key={g.id}
                          className="inline-flex items-center gap-1 rounded border px-2 py-0.5 text-[11px]">
                      <span className="h-2 w-2 rounded-[2px]"
                            style={{ backgroundColor: g.color || '#94a3b8' }} />
                      {g.name}
                      <span className="text-muted-foreground">{g.slide_count}</span>
                      <button onClick={() => deleteGroup(g.id)} className="ml-0.5 hover:text-red-600">
                        <X className="h-3 w-3" />
                      </button>
                    </span>
                  ))}
                </div>
                <div className="flex gap-2">
                  <Input
                    value={newGroupName[activeScheme.id] || ''}
                    onChange={e => setNewGroupName(p => ({ ...p, [activeScheme.id]: e.target.value }))}
                    onKeyDown={e => e.key === 'Enter' && createGroup(activeScheme.id)}
                    placeholder="Add a group, e.g. Pre-treatment"
                    className="h-8 text-[13px]"
                  />
                  <Button size="sm" variant="outline" className="h-8"
                          onClick={() => createGroup(activeScheme.id)}>
                    <Plus className="h-3.5 w-3.5" />
                  </Button>
                  <Button size="sm" variant="ghost" className="h-8 text-red-600"
                          onClick={() => deleteScheme(activeScheme.id)} title="Delete scheme">
                    <Trash2 className="h-3.5 w-3.5" />
                  </Button>
                </div>

                {selectedHashes.size > 0 && (
                  <div className="flex flex-wrap items-center gap-1.5 rounded-md border bg-muted/30 p-2">
                    <span className="text-[11px] text-muted-foreground">
                      {selectedHashes.size} slide{selectedHashes.size === 1 ? '' : 's'} → assign to
                    </span>
                    {activeScheme.groups.map(g => (
                      <button key={g.id} onClick={() => assignSelected(g.id)}
                              className="inline-flex items-center gap-1 rounded border px-2 py-0.5 text-[11px] hover:bg-background">
                        <span className="h-2 w-2 rounded-[2px]"
                              style={{ backgroundColor: g.color || '#94a3b8' }} />
                        {g.name}
                      </button>
                    ))}
                    <button onClick={() => setSelectedHashes(new Set())}
                            className="ml-auto text-[11px] text-muted-foreground hover:text-foreground">
                      Clear
                    </button>
                  </div>
                )}
              </div>
            )}

            {/* Slide list */}
            <div className="min-h-0 flex-1 overflow-auto rounded border">
              {casesList.map(([caseHash, slides]) => (
                <div key={caseHash}
                     className={`border-b last:border-b-0 ${heldOut.has(caseHash) && !includeHeldOut ? 'opacity-60' : ''}`}>
                  <button
                    onClick={() => selectCase(caseHash === 'unknown' ? null : caseHash)}
                    className="flex w-full items-center gap-2 bg-muted/40 px-2 py-1 text-left text-[12px] font-medium hover:bg-muted"
                  >
                    {slides[0]?.accession_number || caseHash.slice(0, 10)}
                    <span className="text-[11px] font-normal text-muted-foreground">
                      {slides.length} slide{slides.length === 1 ? '' : 's'}
                    </span>
                    {heldOut.has(caseHash) && (
                      <span
                        className="ml-auto inline-flex items-center gap-1 rounded-full border border-gray-300 bg-gray-100 px-1.5 py-0.5 text-[10px] font-normal text-gray-600"
                        title={includeHeldOut
                          ? 'Held out in Cohorts, but included in runs because "include held-out" is ticked'
                          : 'Held out in Cohorts — excluded from projections'}
                      >
                        <EyeOff className="h-2.5 w-2.5" />
                        held out{heldOut.get(caseHash) ? ` · ${heldOut.get(caseHash)}` : ''}
                        {includeHeldOut ? ' (included)' : ''}
                      </span>
                    )}
                  </button>
                  {slides.map(s => {
                    const g = groupOfSlide.get(s.slide_hash)
                    const analyses = Object.entries(analysisStatus[s.slide_hash] || {})
                      .sort(([a], [b]) => a.localeCompare(b))
                    return (
                      <label key={s.slide_hash}
                             className="flex cursor-pointer items-center gap-2 px-2 py-1 text-[12px] hover:bg-muted/30">
                        <Checkbox
                          checked={selectedHashes.has(s.slide_hash)}
                          onCheckedChange={() => toggleSlide(s.slide_hash)}
                        />
                        <span className="truncate">{s.block_id}-{s.slide_number} {s.stain_type}</span>
                        {/* One right-aligned group, so the group chip and the
                            analysis chips never compete for the ml-auto slot. */}
                        <span className="ml-auto flex shrink-0 items-center gap-1">
                          {analyses.length === 0 ? (
                            <span className="text-[10px] text-amber-600">no analysis</span>
                          ) : analyses.map(([key, e]) => (
                            <span key={key}
                                  title={`${e.analysis_name || key}: ${e.status} (job ${e.job_id})`}
                                  className={`rounded border px-1.5 py-0.5 text-[10px] ${analysisChipClass(e.status)}`}>
                              {e.analysis_name || key}
                            </span>
                          ))}
                          {g && (
                            <span className="inline-flex items-center gap-1 rounded border px-1.5 py-0.5 text-[10px]">
                              <span className="h-2 w-2 rounded-[2px]"
                                    style={{ backgroundColor: g.color || '#94a3b8' }} />
                              {g.name}
                            </span>
                          )}
                        </span>
                      </label>
                    )
                  })}
                </div>
              ))}
            </div>
          </div>

          {/* 3. Projections */}
          <div className="flex min-h-0 flex-1 flex-col gap-3 rounded-lg border p-3">
            <div className="flex items-center gap-2">
              <ChartScatter className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm font-medium">Projections</span>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <select
                value={method}
                onChange={e => setMethod(e.target.value as 'umap' | 'tsne' | 'pca')}
                className="h-8 rounded-md border border-input bg-background px-2 text-[13px]"
              >
                <option value="umap">UMAP</option>
                <option value="tsne">t-SNE (slower)</option>
                <option value="pca">PCA (fast)</option>
              </select>
              <Button size="sm" className="h-8" onClick={runProjection}
                      disabled={starting || readiness.ready.length === 0}>
                {starting ? <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                          : <Play className="mr-1 h-3.5 w-3.5" />}
                Run on {readiness.ready.length} slide{readiness.ready.length === 1 ? '' : 's'}
              </Button>
              <span className="text-[11px] text-muted-foreground">
                every patch of every ready slide
                {method !== 'pca' && ' · starts with a PCA pre-reduction step'}
              </span>
            </div>

            {runError && (
              <div className="rounded-md border border-red-300 bg-red-50 p-2.5 text-[12px] text-red-700">
                {runError}
              </div>
            )}

            <div className="min-h-0 flex-1 space-y-2 overflow-auto">
              {projections.length === 0 && (
                <p className="text-[12px] text-muted-foreground">
                  No projections yet. PCA is quick and a good way to confirm the data reads
                  correctly before committing to a UMAP run.
                </p>
              )}
              {projections.map(p => (
                <div key={p.id} className="rounded-md border p-2.5">
                  <div className="flex items-center gap-2">
                    <span className="text-[13px] font-medium">
                      {({ umap: 'UMAP', tsne: 't-SNE', pca: 'PCA' } as Record<string, string>)[p.method] ?? p.method.toUpperCase()}
                    </span>
                    <span className="text-[11px] text-muted-foreground"
                          title={typeof p.params?.fit_note === 'string' ? p.params.fit_note : undefined}>
                      {p.slide_count} slides
                      {p.point_count ? ` · ${p.point_count.toLocaleString()} patches` : ''}
                      {fitSampleNote(p.params, p.point_count)}
                    </span>
                    {p.params?.source === 'imported' && (
                      <span className="rounded bg-violet-50 px-1.5 py-0.5 text-[10px] font-medium text-violet-700">
                        imported
                      </span>
                    )}
                    <span className={`ml-auto rounded px-1.5 py-0.5 text-[10px] font-medium ${
                      p.status === 'completed' ? 'bg-emerald-50 text-emerald-700'
                      : p.status === 'failed' ? 'bg-red-50 text-red-700'
                      : 'bg-blue-50 text-blue-700'
                    }`}>
                      {p.status}
                    </span>
                  </div>

                  {(p.status === 'running' || p.status === 'pending') && (
                    <div className="mt-1.5">
                      <div className="h-1.5 w-full overflow-hidden rounded bg-muted">
                        <div className="h-full bg-primary transition-all"
                             style={{ width: `${p.progress_pct || 0}%` }} />
                      </div>
                      <p className="mt-1 text-[11px] text-muted-foreground">
                        {p.progress_stage || 'Working…'}
                      </p>
                    </div>
                  )}

                  {p.status === 'failed' && p.error_message && (
                    <p className="mt-1 text-[11px] text-red-700">{p.error_message}</p>
                  )}

                  <div className="mt-2 flex gap-2">
                    <Button size="sm" variant="outline" className="h-7 text-[12px]"
                            disabled={p.status !== 'completed'}
                            onClick={() => setOpenProjection(p.id)}>
                      <Check className="mr-1 h-3 w-3" />Open workspace
                    </Button>
                    <Button size="sm" variant="ghost" className="h-7 text-[12px] text-red-600"
                            onClick={() => deleteProjection(p.id)}>
                      <Trash2 className="h-3 w-3" />
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {openProjection != null && numericCohortId != null && (
        <CohortProjectionWorkspace
          projectionId={openProjection}
          cohortId={numericCohortId}
          title={cohort?.name}
          onClose={() => setOpenProjection(null)}
        />
      )}
    </div>
  )
}
