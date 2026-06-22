import { useState, useEffect, useMemo } from 'react'
import {
  Search, Users, Tag as TagIcon, ShieldCheck, Eye, Loader2, Send,
  CheckCircle, XCircle, AlertTriangle, Microscope,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@/components/ui/select'
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table'
import { SlideViewerOSD } from '@/components/SlideViewerOSD'
import { signalClusterDisconnected } from '@/components/ClusterConnect'
import type { Analysis, Cohort, CohortDetail, CohortSlide, Slide, GpuInfo } from '@/types/slide'
import { getApiBase, normalizeAccession } from '@/api'
import { displaySlide } from '@/lib/display'

interface AnalysisQCProps {
  clusterConnected?: boolean
}

interface QCResult {
  status: 'pass' | 'warn' | 'fail' | 'unchecked'
  manual_status?: string | null
  metrics?: { tissue_pct?: number | null; mpp?: number | null; width?: number; height?: number; magnification?: number | null } | null
  checks?: { name: string; status: string; detail: string }[] | null
}

// Common slide shape across the three sources.
type QCSlide = Pick<CohortSlide, 'slide_hash' | 'accession_number' | 'block_id' | 'slide_number' | 'stain_type' | 'year' | 'case_hash'> & { slide_id?: string }

interface TagInfo { id: number; name: string; color?: string; slide_count?: number }

type Step = 1 | 2 | 3
type Source = 'cohort' | 'search' | 'tag'

const qcTone = (s?: string) =>
  s === 'pass' ? { dot: 'bg-emerald-500', text: 'text-emerald-700', label: 'Pass' } :
  s === 'warn' ? { dot: 'bg-amber-500', text: 'text-amber-700', label: 'Warn' } :
  s === 'fail' ? { dot: 'bg-red-500', text: 'text-red-700', label: 'Fail' } :
  { dot: 'bg-gray-300', text: 'text-muted-foreground', label: 'Unchecked' }

export function AnalysisQC({ clusterConnected = false }: AnalysisQCProps) {
  const [step, setStep] = useState<Step>(1)

  // ── Step 1: source + selection ───────────────────────────────────────
  const [source, setSource] = useState<Source>('cohort')
  const [cohorts, setCohorts] = useState<Cohort[]>([])
  const [tags, setTags] = useState<TagInfo[]>([])
  const [selectedCohortId, setSelectedCohortId] = useState<number | null>(null)
  const [selectedTagName, setSelectedTagName] = useState('')
  const [searchQuery, setSearchQuery] = useState('')
  const [slides, setSlides] = useState<QCSlide[]>([])
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [loadingSlides, setLoadingSlides] = useState(false)

  // ── QC state ─────────────────────────────────────────────────────────
  const [qcStatus, setQcStatus] = useState<Record<string, QCResult>>({})
  const [runningQc, setRunningQc] = useState(false)
  const [included, setIncluded] = useState<Set<string>>(new Set())  // step-2 inclusion
  const [viewer, setViewer] = useState<{ hash: string; name: string } | null>(null)

  // ── Step 3: submit config ────────────────────────────────────────────
  const [analyses, setAnalyses] = useState<Analysis[]>([])
  const [analysisId, setAnalysisId] = useState<number | null>(null)
  const [gpus, setGpus] = useState<GpuInfo[]>([])
  const [gpu, setGpu] = useState(0)
  const [wsiDir, setWsiDir] = useState('/ligonlab/Prem/slidecap_wsi')
  const [outDir, setOutDir] = useState('/ligonlab/Prem/slidecap_output')
  const [parameters, setParameters] = useState('')
  const [submittedBy, setSubmittedBy] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [submitResult, setSubmitResult] = useState<{ ok: boolean; msg: string } | null>(null)

  useEffect(() => {
    fetch(`${getApiBase()}/cohorts`).then(r => r.ok ? r.json() : []).then(setCohorts).catch(() => {})
    fetch(`${getApiBase()}/tags`).then(r => r.ok ? r.json() : []).then(setTags).catch(() => {})
    fetch(`${getApiBase()}/analyses?active_only=true`).then(r => r.ok ? r.json() : []).then(setAnalyses).catch(() => {})
  }, [])

  useEffect(() => {
    if (step === 3 && clusterConnected) {
      fetch(`${getApiBase()}/cluster/gpus`).then(r => r.ok ? r.json() : []).then(setGpus).catch(() => {})
    }
  }, [step, clusterConnected])

  // ── Load slides from the chosen source ───────────────────────────────
  const setWorkingSet = (list: QCSlide[]) => {
    setSlides(list)
    setSelected(new Set(list.map(s => s.slide_hash)))  // default all selected
  }

  const loadCohort = async (id: number) => {
    setSelectedCohortId(id)
    setLoadingSlides(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${id}`)
      if (res.ok) { const d: CohortDetail = await res.json(); setWorkingSet(d.slides) }
    } catch (e) { console.error(e) } finally { setLoadingSlides(false) }
  }

  const loadTag = async (name: string) => {
    setSelectedTagName(name)
    setLoadingSlides(true)
    try {
      const res = await fetch(`${getApiBase()}/tags/${encodeURIComponent(name)}/slides`)
      if (res.ok) { const data = await res.json(); setWorkingSet((data.slides || data) as QCSlide[]) }
    } catch (e) { console.error(e) } finally { setLoadingSlides(false) }
  }

  const handleSearch = async () => {
    if (!searchQuery.trim()) return
    setLoadingSlides(true)
    try {
      const q = normalizeAccession(searchQuery.trim())
      const res = await fetch(`${getApiBase()}/search?q=${encodeURIComponent(q)}&limit=500`)
      if (res.ok) { const data = await res.json(); setWorkingSet((data.results || []) as QCSlide[]) }
    } catch (e) { console.error(e) } finally { setLoadingSlides(false) }
  }

  const toggleSelect = (h: string) => setSelected(prev => {
    const n = new Set(prev); n.has(h) ? n.delete(h) : n.add(h); return n
  })
  const toggleInclude = (h: string) => setIncluded(prev => {
    const n = new Set(prev); n.has(h) ? n.delete(h) : n.add(h); return n
  })

  // ── QC ───────────────────────────────────────────────────────────────
  const selectedSlides = useMemo(() => slides.filter(s => selected.has(s.slide_hash)), [slides, selected])

  const fetchQc = async (hashes: string[]) => {
    const merged: Record<string, QCResult> = {}
    for (let i = 0; i < hashes.length; i += 100) {
      const batch = hashes.slice(i, i + 100)
      try {
        const res = await fetch(`${getApiBase()}/qc?slide_hashes=${batch.map(encodeURIComponent).join(',')}`)
        if (res.ok) Object.assign(merged, (await res.json()).results || {})
      } catch (e) { console.error(e) }
    }
    return merged
  }

  // Entering step 2: load any cached QC for the selected slides.
  const goToQc = async () => {
    setStep(2)
    setIncluded(new Set(selectedSlides.map(s => s.slide_hash)))
    setQcStatus(await fetchQc(selectedSlides.map(s => s.slide_hash)))
  }

  const runQc = async () => {
    if (runningQc) return
    setRunningQc(true)
    try {
      const hashes = selectedSlides.map(s => s.slide_hash)
      for (let i = 0; i < hashes.length; i += 100) {
        await fetch(`${getApiBase()}/qc/run`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ slide_hashes: hashes.slice(i, i + 100) }),
        })
      }
      setQcStatus(await fetchQc(hashes))
    } catch (e) { console.error(e) } finally { setRunningQc(false) }
  }

  const setManual = async (hash: string, status: 'pass' | 'fail' | null) => {
    try {
      const res = await fetch(`${getApiBase()}/qc/manual`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hash: hash, status }),
      })
      if (res.ok) { const u: QCResult = await res.json(); setQcStatus(prev => ({ ...prev, [hash]: u })) }
    } catch (e) { console.error(e) }
  }

  const includedSlides = useMemo(() => selectedSlides.filter(s => included.has(s.slide_hash)), [selectedSlides, included])
  const failIncluded = includedSlides.filter(s => qcStatus[s.slide_hash]?.status === 'fail').length

  // ── Submit ───────────────────────────────────────────────────────────
  const handleSubmit = async () => {
    if (!analysisId) return
    setSubmitting(true); setSubmitResult(null)
    try {
      const res = await fetch(`${getApiBase()}/jobs/submit`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          analysis_id: analysisId,
          slide_hashes: includedSlides.map(s => s.slide_hash),
          gpu_index: gpu,
          remote_wsi_dir: wsiDir,
          remote_output_dir: outDir,
          parameters: parameters || undefined,
          submitted_by: submittedBy || undefined,
        }),
      })
      if (res.status === 503) { signalClusterDisconnected(); setSubmitResult({ ok: false, msg: 'Not connected to cluster.' }); return }
      if (res.ok) {
        const d = await res.json()
        setSubmitResult({ ok: true, msg: `Job #${d.job_id} submitted with ${d.slides_created} slide(s). Track it in the Jobs tab.` })
      } else {
        const e = await res.json(); setSubmitResult({ ok: false, msg: e.detail || 'Submission failed' })
      }
    } catch { setSubmitResult({ ok: false, msg: 'Network error' }) }
    finally { setSubmitting(false) }
  }

  // ── Render ───────────────────────────────────────────────────────────
  const StepPill = ({ n, label }: { n: Step; label: string }) => (
    <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium ${step === n ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'}`}>
      {n}. {label}
    </div>
  )

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold mb-2">QC &amp; Submit</h1>
        <p className="text-muted-foreground">Select slides, quality-check them (open the viewer to inspect), then submit for analysis.</p>
      </div>

      <div className="flex items-center gap-3">
        <StepPill n={1} label="Select slides" />
        <div className="h-px w-6 bg-border" />
        <StepPill n={2} label="QC review" />
        <div className="h-px w-6 bg-border" />
        <StepPill n={3} label="Configure & submit" />
      </div>

      {/* ── STEP 1: SELECT ── */}
      {step === 1 && (
        <div className="space-y-4">
          <div className="flex gap-2">
            <Button variant={source === 'cohort' ? 'default' : 'outline'} size="sm" onClick={() => setSource('cohort')}><Users className="mr-2 h-4 w-4" />Cohort</Button>
            <Button variant={source === 'search' ? 'default' : 'outline'} size="sm" onClick={() => setSource('search')}><Search className="mr-2 h-4 w-4" />Search</Button>
            <Button variant={source === 'tag' ? 'default' : 'outline'} size="sm" onClick={() => setSource('tag')}><TagIcon className="mr-2 h-4 w-4" />By Tag</Button>
          </div>

          {source === 'cohort' && (
            <Select value={selectedCohortId?.toString() ?? ''} onValueChange={v => loadCohort(Number(v))}>
              <SelectTrigger className="max-w-sm"><SelectValue placeholder="Choose a cohort…" /></SelectTrigger>
              <SelectContent>
                {cohorts.map(c => <SelectItem key={c.id} value={c.id.toString()}>{c.name} ({c.slide_count} slides)</SelectItem>)}
              </SelectContent>
            </Select>
          )}
          {source === 'search' && (
            <div className="flex gap-2 max-w-lg">
              <Input placeholder="Accession, slide ID, block…" value={searchQuery} onChange={e => setSearchQuery(e.target.value)} onKeyDown={e => e.key === 'Enter' && handleSearch()} />
              <Button onClick={handleSearch}>Search</Button>
            </div>
          )}
          {source === 'tag' && (
            <Select value={selectedTagName} onValueChange={loadTag}>
              <SelectTrigger className="max-w-sm"><SelectValue placeholder="Choose a tag…" /></SelectTrigger>
              <SelectContent>
                {tags.filter(t => (t.slide_count ?? 0) > 0).map(t => (
                  <SelectItem key={t.id} value={t.name}>
                    <span className="inline-flex items-center gap-2">
                      {t.color && <span className="h-2 w-2 rounded-[2px]" style={{ background: t.color }} />}
                      {t.name} ({t.slide_count})
                    </span>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          )}

          {loadingSlides ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading…</div>
          ) : slides.length > 0 && (
            <>
              <div className="rounded-lg border max-h-[420px] overflow-auto">
                <Table>
                  <TableHeader className="sticky top-0 z-10 [&_th]:bg-muted/95">
                    <TableRow>
                      <TableHead className="w-10">
                        <Checkbox checked={selected.size === slides.length && slides.length > 0} onCheckedChange={() => setSelected(selected.size === slides.length ? new Set() : new Set(slides.map(s => s.slide_hash)))} />
                      </TableHead>
                      <TableHead>Slide</TableHead><TableHead>Block</TableHead><TableHead>Stain</TableHead><TableHead>Year</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {slides.map(s => (
                      <TableRow key={s.slide_hash} className="cursor-pointer" onClick={() => toggleSelect(s.slide_hash)}>
                        <TableCell onClick={e => e.stopPropagation()}><Checkbox checked={selected.has(s.slide_hash)} onCheckedChange={() => toggleSelect(s.slide_hash)} /></TableCell>
                        <TableCell className="font-mono text-sm">{displaySlide(s as Slide)}</TableCell>
                        <TableCell>{s.block_id}</TableCell>
                        <TableCell>{s.stain_type}</TableCell>
                        <TableCell>{s.year ?? '-'}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">{selected.size} of {slides.length} selected</span>
                <Button onClick={goToQc} disabled={selected.size === 0}>Next: QC review ({selected.size})</Button>
              </div>
            </>
          )}
        </div>
      )}

      {/* ── STEP 2: QC ── */}
      {step === 2 && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <Button variant="ghost" size="sm" onClick={() => setStep(1)}>← Back to selection</Button>
            <Button variant="outline" size="sm" onClick={runQc} disabled={runningQc}>
              {runningQc ? <><Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> Running QC…</> : <><ShieldCheck className="mr-2 h-3.5 w-3.5" /> Run QC on {selectedSlides.length}</>}
            </Button>
          </div>

          <div className="rounded-lg border max-h-[460px] overflow-auto">
            <Table>
              <TableHeader className="sticky top-0 z-10 [&_th]:bg-muted/95">
                <TableRow>
                  <TableHead className="w-10">Use</TableHead>
                  <TableHead>Slide</TableHead><TableHead>Stain</TableHead>
                  <TableHead>QC</TableHead><TableHead>Tissue</TableHead><TableHead>MPP</TableHead>
                  <TableHead>Review</TableHead><TableHead className="w-32">Flag</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {selectedSlides.map(s => {
                  const q = qcStatus[s.slide_hash]
                  const tone = qcTone(q?.status)
                  return (
                    <TableRow key={s.slide_hash}>
                      <TableCell><Checkbox checked={included.has(s.slide_hash)} onCheckedChange={() => toggleInclude(s.slide_hash)} /></TableCell>
                      <TableCell className="font-mono text-sm">{displaySlide(s as Slide)}</TableCell>
                      <TableCell>{s.stain_type}</TableCell>
                      <TableCell>
                        <span className={`inline-flex items-center gap-1.5 text-xs font-medium ${tone.text}`} title={(q?.checks || []).filter(c => c.status !== 'pass').map(c => `${c.name}: ${c.detail}`).join(' · ') || undefined}>
                          <span className={`h-2 w-2 rounded-full ${tone.dot}`} />
                          {q?.manual_status ? `${tone.label}*` : tone.label}
                        </span>
                      </TableCell>
                      <TableCell className="text-sm tabular-nums">{q?.metrics?.tissue_pct != null ? `${q.metrics.tissue_pct}%` : '—'}</TableCell>
                      <TableCell className="text-sm tabular-nums">{q?.metrics?.mpp ?? '—'}</TableCell>
                      <TableCell>
                        <Button variant="ghost" size="icon" className="h-7 w-7" title="Open viewer to inspect"
                          onClick={() => setViewer({ hash: s.slide_hash, name: displaySlide(s as Slide) })}>
                          <Eye className="h-4 w-4" />
                        </Button>
                      </TableCell>
                      <TableCell>
                        <div className="flex gap-1">
                          <button className="text-[11px] px-1.5 py-0.5 rounded border border-emerald-300 text-emerald-700 hover:bg-emerald-50" onClick={() => setManual(s.slide_hash, 'pass')}>Pass</button>
                          <button className="text-[11px] px-1.5 py-0.5 rounded border border-red-300 text-red-700 hover:bg-red-50" onClick={() => setManual(s.slide_hash, 'fail')}>Fail</button>
                          {q?.manual_status && <button className="text-[11px] px-1.5 py-0.5 rounded border text-muted-foreground hover:bg-muted" onClick={() => setManual(s.slide_hash, null)} title="Clear override">✕</button>}
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </div>

          {failIncluded > 0 && (
            <div className="flex items-center gap-2 p-3 rounded-lg bg-red-500/10 text-red-700 border border-red-300 text-sm">
              <AlertTriangle className="h-4 w-4 shrink-0" />
              {failIncluded} included slide{failIncluded !== 1 ? 's' : ''} failed QC — uncheck them, or proceed anyway.
            </div>
          )}

          <div className="flex items-center justify-between">
            <span className="text-sm text-muted-foreground">{included.size} slide{included.size !== 1 ? 's' : ''} will be submitted</span>
            <Button onClick={() => setStep(3)} disabled={included.size === 0}>Next: Configure &amp; submit ({included.size})</Button>
          </div>
        </div>
      )}

      {/* ── STEP 3: SUBMIT ── */}
      {step === 3 && (
        <div className="space-y-4">
          <Button variant="ghost" size="sm" onClick={() => setStep(2)}>← Back to QC</Button>

          {!clusterConnected && (
            <div className="flex items-center gap-2 p-4 rounded-lg bg-yellow-500/10 text-yellow-700 border border-yellow-300 text-sm">
              <AlertTriangle className="h-5 w-5 shrink-0" /> Connect to the cluster first to submit jobs.
            </div>
          )}
          {submitResult && (
            <div className={`p-4 rounded-lg ${submitResult.ok ? 'bg-green-500/10 text-green-700' : 'bg-red-500/10 text-red-700'}`}>
              {submitResult.ok ? <CheckCircle className="inline h-4 w-4 mr-2" /> : <XCircle className="inline h-4 w-4 mr-2" />}
              {submitResult.msg}
            </div>
          )}

          <div className="space-y-2">
            <label className="text-sm font-medium">Analysis Pipeline *</label>
            <div className="grid gap-2">
              {analyses.length === 0 ? <p className="text-sm text-muted-foreground">No active analyses. Register one in the Registry tab.</p> :
                analyses.map(a => (
                  <label key={a.id} className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${analysisId === a.id ? 'border-primary bg-primary/5' : 'hover:bg-muted/50'}`}>
                    <input type="radio" name="qc-analysis" checked={analysisId === a.id} onChange={() => { setAnalysisId(a.id); setParameters(a.default_parameters || '') }} className="h-4 w-4" />
                    <div className="flex-1">
                      <div className="flex items-center gap-2"><span className="font-medium">{a.name}</span><Badge variant="secondary">v{a.version}</Badge>{a.gpu_required && <Badge>GPU</Badge>}</div>
                      {a.description && <p className="text-sm text-muted-foreground">{a.description}</p>}
                    </div>
                  </label>
                ))}
            </div>
          </div>

          {clusterConnected && gpus.length > 0 && (
            <div className="space-y-2">
              <label className="text-sm font-medium">GPU</label>
              <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                {gpus.map(g => (
                  <label key={g.index} className={`flex items-start gap-2 p-3 rounded-lg border cursor-pointer ${gpu === g.index ? 'border-primary bg-primary/5' : 'hover:bg-muted/50'}`}>
                    <input type="radio" name="qc-gpu" checked={gpu === g.index} onChange={() => setGpu(g.index)} className="h-4 w-4 mt-0.5" />
                    <div className="text-xs"><p className="font-medium">GPU {g.index}</p><p className="text-muted-foreground truncate">{g.name}</p><p>{g.memory_used_mb}/{g.memory_total_mb} MB</p></div>
                  </label>
                ))}
              </div>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2"><label className="text-sm font-medium">Remote WSI Directory</label><Input value={wsiDir} onChange={e => setWsiDir(e.target.value)} className="font-mono text-sm" /></div>
            <div className="space-y-2"><label className="text-sm font-medium">Remote Output Directory</label><Input value={outDir} onChange={e => setOutDir(e.target.value)} className="font-mono text-sm" /></div>
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium">Parameters (JSON, optional)</label>
            <textarea className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[70px]" placeholder='{"batch_size": 4}' value={parameters} onChange={e => setParameters(e.target.value)} />
          </div>
          <div className="space-y-2"><label className="text-sm font-medium">Submitted By (optional)</label><Input value={submittedBy} onChange={e => setSubmittedBy(e.target.value)} /></div>

          <Button onClick={handleSubmit} disabled={submitting || !analysisId || includedSlides.length === 0 || !clusterConnected} className="w-full">
            <Send className="mr-2 h-4 w-4" />
            {submitting ? 'Submitting…' : !clusterConnected ? 'Connect to cluster first' : `Submit Job (${includedSlides.length} slides)`}
          </Button>
        </div>
      )}

      {/* Slide viewer overlay (opened from the QC table) */}
      {viewer && (
        <SlideViewerOSD slideHash={viewer.hash} slideName={viewer.name} onClose={() => setViewer(null)} />
      )}
    </div>
  )
}
