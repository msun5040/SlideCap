import { useState, useEffect, useRef, useMemo } from 'react'
import {
  Loader2, Clock, AlertTriangle, CheckCircle2, X, RefreshCw, RotateCw,
  Upload, Cpu, Sparkles, PackageCheck, Check, Layers,
} from 'lucide-react'
import { signalClusterDisconnected } from '@/components/ClusterConnect'
import { getApiBase, isDemo } from '@/api'
import type { AnalysisJob } from '@/types/slide'

// ── Detail-slide shape (richer than the list JobSlide) ─────────────────
interface DetailSlide {
  id: number
  slide_id?: string
  accession_number?: string | null
  block_id?: string | null
  stain_type?: string | null
  gpu_index?: number | null
  status: string
  error_message?: string | null
}
type JobDetail = Omit<AnalysisJob, 'slides'> & {
  throughput_per_min?: number | null
  eta_seconds?: number | null
  gpus_in_use?: number[]
  slides?: DetailSlide[]
}

type Filter = 'all' | 'active' | 'attention' | 'completed'

// ── Status → color mapping (mirrors the design's semantic palette) ─────
const DOT: Record<string, string> = {
  pending: 'bg-gray-400', queued: 'bg-gray-400',
  transferring: 'bg-blue-500', running: 'bg-amber-500',
  completed: 'bg-emerald-600', failed: 'bg-red-600',
}
const TEXT: Record<string, string> = {
  pending: 'text-gray-500', queued: 'text-gray-500',
  transferring: 'text-blue-600', running: 'text-amber-600',
  completed: 'text-emerald-700', failed: 'text-red-700',
}
const FILL: Record<string, string> = {
  pending: 'bg-gray-300', queued: 'bg-gray-300',
  transferring: 'bg-blue-500', running: 'bg-amber-500',
  completed: 'bg-emerald-600', failed: 'bg-red-600',
}
const CELL: Record<string, string> = {
  completed: 'bg-emerald-600', running: 'bg-amber-500 animate-pulse',
  failed: 'bg-red-600', transferring: 'bg-blue-500 animate-pulse',
  queued: 'bg-gray-300', pending: 'bg-gray-300',
}

// Stable-ish color swatch per model name (purely cosmetic)
const SWATCHES = ['#2c8a7d', '#bf9020', '#7a52b3', '#c0568f', '#b4452f', '#36577a']
const swatchFor = (s: string) => {
  let h = 0
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0
  return SWATCHES[Math.abs(h) % SWATCHES.length]
}

const ago = (iso?: string): string => {
  if (!iso) return ''
  const s = (Date.now() - new Date(iso).getTime()) / 1000
  if (s < 60) return `${Math.round(s)}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m} min ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h} h ago`
  return `${Math.floor(h / 24)} d ago`
}
const etaFmt = (sec?: number | null): string | null => {
  if (!sec || sec <= 0) return null
  if (sec < 60) return `~${Math.round(sec)}s`
  const m = Math.round(sec / 60)
  if (m < 60) return `~${m} min`
  return `~${Math.floor(m / 60)}h ${m % 60}m`
}
const stageLabel = (s: string) =>
  s === 'transferring' ? 'Transfer' : s === 'completed' ? 'Complete' :
  s === 'queued' || s === 'pending' ? 'Queued' : s === 'failed' ? 'Failed' : 'Inference'

const isActive = (j: { status: string }) =>
  ['running', 'transferring', 'pending', 'queued'].includes(j.status)

// ── Pipeline stepper ───────────────────────────────────────────────────
const STEPS = [
  { label: 'Transfer', icon: Upload },
  { label: 'Queue', icon: Clock },
  { label: 'Inference', icon: Cpu },
  { label: 'Post-process', icon: Sparkles },
  { label: 'Export', icon: PackageCheck },
]
const stageIndex = (status: string): number =>
  status === 'transferring' ? 0 :
  status === 'pending' || status === 'queued' ? 1 :
  status === 'running' ? 2 :
  status === 'completed' ? 5 : /* failed */ 2

function Stepper({ status }: { status: string }) {
  const active = stageIndex(status)
  const failed = status === 'failed'
  return (
    <div className="flex items-center px-1 py-1">
      {STEPS.map((step, i) => {
        const done = i < active
        const isCur = i === active
        const Icon = done ? Check : step.icon
        const err = failed && isCur
        return (
          <div key={step.label} className="flex items-center">
            <div className="flex items-center gap-2">
              <span className={`grid h-6 w-6 place-items-center rounded-full border ${
                err ? 'border-red-600 bg-red-600 text-white' :
                done ? 'border-emerald-600 bg-emerald-600 text-white' :
                isCur ? 'border-primary bg-primary text-primary-foreground' :
                'border-border bg-background text-muted-foreground/60'}`}>
                <Icon className="h-3 w-3" />
              </span>
              <span className={`text-xs font-semibold whitespace-nowrap ${
                err ? 'text-red-700' : done || isCur ? 'text-foreground' : 'text-muted-foreground/60'}`}>
                {step.label}
              </span>
            </div>
            {i < STEPS.length - 1 && (
              <span className={`mx-3 h-0.5 w-10 ${i < active ? 'bg-emerald-600' : 'bg-border'}`} />
            )}
          </div>
        )
      })}
    </div>
  )
}

export function AnalysisInstrument() {
  const [jobs, setJobs] = useState<AnalysisJob[]>([])
  const [filter, setFilter] = useState<Filter>('all')
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [detail, setDetail] = useState<JobDetail | null>(null)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [retrying, setRetrying] = useState(false)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // ── Fetching ─────────────────────────────────────────────────────────
  const fetchJobs = async () => {
    try {
      const res = await fetch(`${getApiBase()}/jobs?limit=200`)
      if (res.ok) setJobs(await res.json())
    } catch (e) { console.error('Failed to fetch jobs:', e) }
  }
  const fetchDetail = async (id: number) => {
    try {
      const res = await fetch(`${getApiBase()}/jobs/${id}`)
      if (res.ok) setDetail(await res.json())
    } catch (e) { console.error('Failed to fetch job detail:', e) }
  }

  useEffect(() => { fetchJobs() }, [])

  // Auto-select first job (prefer an active one)
  useEffect(() => {
    if (selectedId == null && jobs.length > 0) {
      const first = jobs.find(isActive) || jobs[0]
      setSelectedId(first.id)
    }
  }, [jobs, selectedId])

  useEffect(() => { if (selectedId != null) fetchDetail(selectedId) }, [selectedId])

  // Poll while any job is active
  useEffect(() => {
    const hasActive = jobs.some(isActive)
    if (hasActive) {
      intervalRef.current = setInterval(() => {
        fetchJobs()
        if (selectedId != null) fetchDetail(selectedId)
      }, isDemo() ? 1500 : 15000)
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current) }
  }, [jobs, selectedId])

  // ── Actions ──────────────────────────────────────────────────────────
  const handleRefresh = async () => {
    setIsRefreshing(true)
    try {
      const res = await fetch(`${getApiBase()}/jobs/refresh`, { method: 'POST' })
      if (res.status === 503) { signalClusterDisconnected(); return }
      await fetchJobs()
      if (selectedId != null) await fetchDetail(selectedId)
    } catch (e) { console.error('Refresh failed:', e) }
    finally { setIsRefreshing(false) }
  }
  const handleCancel = async (id: number) => {
    if (!confirm('Cancel this job and all its slides?')) return
    try {
      await fetch(`${getApiBase()}/jobs/cancel`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_ids: [id] }),
      })
      await fetchJobs()
      if (selectedId === id) await fetchDetail(id)
    } catch (e) { console.error('Cancel failed:', e) }
  }
  const handleRetry = async (id: number) => {
    setRetrying(true)
    try {
      const res = await fetch(`${getApiBase()}/jobs/${id}/retry-failed`, { method: 'POST' })
      if (res.status === 503) { signalClusterDisconnected(); return }
      if (res.ok) {
        const data = await res.json()
        await fetchJobs()
        if (data.job_id) setSelectedId(data.job_id)
        else await fetchDetail(id)
      }
    } catch (e) { console.error('Retry failed:', e) }
    finally { setRetrying(false) }
  }

  // ── Derived ──────────────────────────────────────────────────────────
  const counts = useMemo(() => {
    const todayStr = new Date().toDateString()
    return {
      running: jobs.filter(j => j.status === 'running').length,
      queued: jobs.filter(j => j.status === 'pending' || j.status === 'queued' || j.status === 'transferring').length,
      attention: jobs.filter(j => j.status === 'failed' || j.failed_count > 0).length,
      completedToday: jobs.filter(j => j.status === 'completed' && j.completed_at && new Date(j.completed_at).toDateString() === todayStr).length,
    }
  }, [jobs])

  const filtered = useMemo(() => jobs.filter(j =>
    filter === 'all' ? true :
    filter === 'active' ? isActive(j) :
    filter === 'attention' ? (j.status === 'failed' || j.failed_count > 0) :
    j.status === 'completed'
  ), [jobs, filter])

  const tiles: { key: Filter; label: string; value: number; icon: typeof Loader2; tone: string }[] = [
    { key: 'active', label: 'Running', value: counts.running, icon: Loader2, tone: 'bg-amber-50 text-amber-600' },
    { key: 'active', label: 'Queued', value: counts.queued, icon: Clock, tone: 'bg-gray-100 text-gray-600' },
    { key: 'attention', label: 'Needs attention', value: counts.attention, icon: AlertTriangle, tone: 'bg-red-50 text-red-600' },
    { key: 'completed', label: 'Completed today', value: counts.completedToday, icon: CheckCircle2, tone: 'bg-emerald-50 text-emerald-600' },
  ]

  const failedSlides = (detail?.slides || []).filter(s => s.status === 'failed')
  const progressPct = detail && detail.slide_count > 0
    ? Math.round((detail.completed_count / detail.slide_count) * 100) : 0

  return (
    <div className="space-y-3">
      {/* ── Status tiles ── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-2.5">
        {tiles.map((t, i) => {
          const cur = filter === t.key
          return (
            <button
              key={i}
              onClick={() => setFilter(t.key)}
              className={`flex items-center gap-3 rounded-md border bg-card px-3.5 py-2.5 text-left transition-colors ${
                cur ? 'border-primary ring-1 ring-primary/40 bg-primary/5' : 'hover:bg-muted/40'}`}
            >
              <span className={`grid h-8 w-8 place-items-center rounded-md shrink-0 ${t.tone}`}>
                <t.icon className={`h-4 w-4 ${t.label === 'Running' && t.value > 0 ? 'animate-spin' : ''}`} />
              </span>
              <span>
                <span className="block text-2xl font-bold leading-none tabular-nums">{t.value}</span>
                <span className="block text-[11px] uppercase tracking-wide text-muted-foreground mt-1">{t.label}</span>
              </span>
            </button>
          )
        })}
      </div>

      {/* ── Master / detail split ── */}
      <div className="flex rounded-lg border overflow-hidden h-[600px] bg-card">
        {/* Master */}
        <div className="w-[360px] shrink-0 border-r flex flex-col">
          <div className="flex items-center gap-1.5 px-3 py-2.5 border-b">
            {(['all', 'active', 'attention', 'completed'] as Filter[]).map(f => (
              <button
                key={f}
                onClick={() => setFilter(f)}
                className={`text-[11.5px] font-semibold px-2.5 py-1 rounded-full border transition-colors ${
                  filter === f ? 'bg-primary/10 text-primary border-primary/40' : 'text-muted-foreground border-border hover:bg-muted/40'}`}
              >
                {f === 'all' ? 'All' : f === 'active' ? 'Active' : f === 'attention' ? 'Needs attention' : 'Completed'}
              </button>
            ))}
            <button onClick={handleRefresh} disabled={isRefreshing} className="ml-auto text-muted-foreground hover:text-foreground" title="Refresh">
              <RefreshCw className={`h-3.5 w-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
            </button>
          </div>

          <div className="flex-1 overflow-auto p-2 space-y-1.5">
            {filtered.length === 0 ? (
              <p className="text-sm text-muted-foreground text-center py-10">No jobs.</p>
            ) : filtered.map(j => {
              const cur = selectedId === j.id
              const pct = j.slide_count > 0 ? Math.round((j.completed_count / j.slide_count) * 100) : 0
              return (
                <button
                  key={j.id}
                  onClick={() => setSelectedId(j.id)}
                  className={`w-full text-left rounded-md border px-3 py-2.5 border-l-[3px] transition-colors ${
                    cur ? 'bg-primary/5 border-primary border-l-primary' : 'border-l-transparent hover:bg-muted/40'}`}
                >
                  <div className="flex items-center gap-2">
                    <span className="text-[13px] font-bold tracking-tight">{j.model_name}</span>
                    {j.model_version && <span className="font-mono text-[10px] text-muted-foreground">v{j.model_version}</span>}
                    <span className={`ml-auto inline-flex items-center gap-1.5 text-[11px] font-semibold ${TEXT[j.status] || ''}`}>
                      <span className={`h-[7px] w-[7px] rounded-full ${DOT[j.status] || 'bg-gray-400'}`} />
                      {j.status}
                    </span>
                  </div>
                  <div className="flex items-center gap-1.5 text-[11.5px] text-muted-foreground my-1.5">
                    <span className="h-2 w-2 rounded-sm shrink-0" style={{ background: swatchFor(j.model_name) }} />
                    <span className="truncate">{j.submitted_by || `job #${j.id}`}</span>
                    <span className="font-mono text-[10.5px] shrink-0">· {j.slide_count} slides</span>
                  </div>
                  <div className="h-[5px] rounded-full bg-muted overflow-hidden">
                    <span className={`block h-full rounded-full ${FILL[j.status] || 'bg-gray-300'}`} style={{ width: `${j.status === 'transferring' ? 55 : pct}%` }} />
                  </div>
                  <div className="flex items-center justify-between mt-1.5 text-[11px] text-muted-foreground">
                    <span>{stageLabel(j.status)} <span className="font-mono text-[10.5px] text-foreground">{j.completed_count}/{j.slide_count}</span></span>
                    <span className="truncate ml-2">
                      {j.gpu_index != null ? `gpu-${j.gpu_index} · ` : ''}{ago(j.submitted_at)}
                    </span>
                  </div>
                </button>
              )
            })}
          </div>
        </div>

        {/* Detail */}
        <div className="flex-1 overflow-auto min-w-0">
          {!detail ? (
            <div className="h-full grid place-items-center text-sm text-muted-foreground">
              Select a job to inspect.
            </div>
          ) : (
            <>
              {/* header */}
              <div className="px-6 py-4 border-b">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <h3 className="flex items-center gap-2 text-lg font-bold tracking-tight">
                      <Layers className="h-[18px] w-[18px] text-primary" />
                      {detail.model_name}
                      {detail.model_version && (
                        <span className="font-mono text-[11px] font-medium text-muted-foreground bg-muted border rounded px-1.5 py-0.5">
                          v{detail.model_version}
                        </span>
                      )}
                    </h3>
                    <div className="flex flex-wrap gap-x-3.5 gap-y-1 mt-1.5 text-xs text-muted-foreground">
                      {detail.submitted_by && <span>Submitted by <b className="text-foreground font-semibold">{detail.submitted_by}</b></span>}
                      <span>{ago(detail.submitted_at)}</span>
                      <span className="font-mono">job #{detail.id}</span>
                      {detail.gpus_in_use && detail.gpus_in_use.length > 0 && (
                        <span className="font-mono">gpu {detail.gpus_in_use.join(', ')}</span>
                      )}
                    </div>
                  </div>
                  <div className="flex gap-2 shrink-0">
                    {isActive(detail) && (
                      <button onClick={() => handleCancel(detail.id)}
                        className="inline-flex items-center gap-1.5 h-8 px-3 text-xs font-semibold rounded border border-red-200 text-red-600 hover:bg-red-50">
                        <X className="h-3.5 w-3.5" />Cancel
                      </button>
                    )}
                    {detail.failed_count > 0 && !isActive(detail) && (
                      <button onClick={() => handleRetry(detail.id)} disabled={retrying}
                        className="inline-flex items-center gap-1.5 h-8 px-3 text-xs font-semibold rounded border hover:bg-muted">
                        {retrying ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RotateCw className="h-3.5 w-3.5" />}
                        Retry failed
                      </button>
                    )}
                  </div>
                </div>
              </div>

              {/* stepper */}
              <div className="px-6 py-4">
                <Stepper status={detail.status} />
              </div>

              {/* stats */}
              <div className="grid grid-cols-4 mx-6 rounded-md border overflow-hidden">
                {[
                  { l: 'Progress', v: <>{progressPct}<small className="text-xs font-medium text-muted-foreground">%</small></> },
                  { l: 'Slides', v: <><span className="font-mono">{detail.completed_count}</span><small className="text-xs font-medium text-muted-foreground"> / {detail.slide_count}</small></> },
                  { l: 'Throughput', v: detail.throughput_per_min != null ? <><span className="font-mono">{detail.throughput_per_min}</span><small className="text-xs font-medium text-muted-foreground"> /min</small></> : <span className="text-muted-foreground">—</span> },
                  { l: 'Est. remaining', v: etaFmt(detail.eta_seconds) ? <>{etaFmt(detail.eta_seconds)}</> : <span className="text-muted-foreground">—</span> },
                ].map((s, i) => (
                  <div key={i} className={`px-4 py-3 ${i < 3 ? 'border-r' : ''}`}>
                    <div className="text-[10px] uppercase tracking-wide text-muted-foreground">{s.l}</div>
                    <div className="text-[19px] font-bold tracking-tight mt-1 tabular-nums">{s.v}</div>
                  </div>
                ))}
              </div>

              {/* per-slide heatmap */}
              <div className="px-6 pt-5">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-[13px] font-bold tracking-tight">
                    Per-slide progress · {detail.slide_count} slides
                  </span>
                  <span className="flex gap-3.5 text-[11px] text-muted-foreground">
                    <Legend cls="bg-emerald-600" label={`Done ${detail.completed_count}`} />
                    <Legend cls="bg-amber-500" label={`Running ${(detail.slides || []).filter(s => s.status === 'running').length}`} />
                    <Legend cls="bg-red-600" label={`Failed ${detail.failed_count}`} />
                    <Legend cls="bg-gray-300" label={`Queued ${(detail.slides || []).filter(s => ['queued', 'pending', 'transferring'].includes(s.status)).length}`} />
                  </span>
                </div>
                <div className="flex flex-wrap gap-1">
                  {(detail.slides || []).map(s => (
                    <span
                      key={s.id}
                      className={`h-[13px] w-[13px] rounded-sm ${CELL[s.status] || 'bg-gray-300'}`}
                      title={`${s.slide_id || s.accession_number || ''}${s.block_id ? ` · ${s.block_id}` : ''} — ${s.status}${s.gpu_index != null ? ` (gpu-${s.gpu_index})` : ''}${s.error_message ? `: ${s.error_message}` : ''}`}
                    />
                  ))}
                </div>
              </div>

              {/* failed list */}
              {failedSlides.length > 0 && (
                <div className="m-6 rounded-md border border-red-200 bg-red-50/50 overflow-hidden">
                  <div className="flex items-center gap-2 px-4 py-2.5 text-xs font-bold text-red-700 border-b border-red-200">
                    <AlertTriangle className="h-4 w-4" />
                    {failedSlides.length} slide{failedSlides.length !== 1 ? 's' : ''} failed
                    {!isActive(detail) && (
                      <button onClick={() => handleRetry(detail.id)} disabled={retrying}
                        className="ml-auto inline-flex items-center gap-1.5 h-6 px-2.5 text-[11px] font-semibold rounded border border-red-300 bg-white text-red-600 hover:bg-red-50">
                        {retrying ? <Loader2 className="h-3 w-3 animate-spin" /> : <RotateCw className="h-3 w-3" />}
                        Retry failed slides
                      </button>
                    )}
                  </div>
                  {failedSlides.map(s => (
                    <div key={s.id} className="flex items-center gap-3 px-4 py-2 text-xs border-b border-red-100 last:border-0">
                      <span className="font-mono">{s.slide_id || '—'}</span>
                      <span className="font-mono text-muted-foreground">
                        {s.accession_number || '—'}{s.block_id ? ` · ${s.block_id}` : ''}
                      </span>
                      <span className="text-muted-foreground truncate ml-auto text-right">{s.error_message || 'failed'}</span>
                    </div>
                  ))}
                </div>
              )}
              <div className="h-4" />
            </>
          )}
        </div>
      </div>
    </div>
  )
}

function Legend({ cls, label }: { cls: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <i className={`h-[9px] w-[9px] rounded-sm ${cls}`} />{label}
    </span>
  )
}
