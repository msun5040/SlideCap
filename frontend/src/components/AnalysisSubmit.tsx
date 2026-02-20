import { useState, useEffect, useRef } from 'react'
import { Search, Send, Users, AlertTriangle, Loader2, CheckCircle, XCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import type { Analysis, AnalysisJob, Cohort, Slide, GpuInfo } from '@/types/slide'

const API_BASE = 'http://localhost:8000'

interface AnalysisSubmitProps {
  clusterConnected?: boolean
}

export function AnalysisSubmit({ clusterConnected = false }: AnalysisSubmitProps) {
  const [step, setStep] = useState<1 | 2>(1)
  const [mode, setMode] = useState<'search' | 'cohort'>('search')

  // Step 1 — slide selection
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState<Slide[]>([])
  const [selectedHashes, setSelectedHashes] = useState<Set<string>>(new Set())
  const [cohorts, setCohorts] = useState<Cohort[]>([])
  const [selectedCohortId, setSelectedCohortId] = useState<number | null>(null)

  // Step 2 — analysis + cluster config
  const [analyses, setAnalyses] = useState<Analysis[]>([])
  const [selectedAnalysisId, setSelectedAnalysisId] = useState<number | null>(null)
  const [gpus, setGpus] = useState<GpuInfo[]>([])
  const [selectedGpu, setSelectedGpu] = useState(0)
  const [remoteWsiDir, setRemoteWsiDir] = useState('/ligonlab/Prem/slidecap_wsi')
  const [remoteOutputDir, setRemoteOutputDir] = useState('/ligonlab/Prem/slidecap_output')
  const [parameters, setParameters] = useState('')
  const [submittedBy, setSubmittedBy] = useState('')
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [submitResult, setSubmitResult] = useState<{ success: boolean; message: string } | null>(null)

  // Progress tracking
  const [trackedJobId, setTrackedJobId] = useState<number | null>(null)
  const [trackedJob, setTrackedJob] = useState<AnalysisJob | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    fetchCohorts()
    fetchAnalyses()
  }, [])

  // Poll job progress
  useEffect(() => {
    if (!trackedJobId) return
    const poll = async () => {
      try {
        // Trigger backend to refresh statuses from cluster before fetching
        await fetch(`${API_BASE}/jobs/refresh`, { method: 'POST' }).catch(() => {})
        const res = await fetch(`${API_BASE}/jobs/${trackedJobId}`)
        if (res.ok) {
          const data: AnalysisJob = await res.json()
          setTrackedJob(data)
          if (data.status === 'completed' || data.status === 'failed') {
            if (pollRef.current) clearInterval(pollRef.current)
            pollRef.current = null
          }
        }
      } catch (e) {
        console.error('Poll failed:', e)
      }
    }
    poll()
    pollRef.current = setInterval(poll, 10000)
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
      pollRef.current = null
    }
  }, [trackedJobId])

  // Fetch GPUs when entering step 2 and connected
  useEffect(() => {
    if (step === 2 && clusterConnected) {
      fetchGpus()
    }
  }, [step, clusterConnected])

  const fetchCohorts = async () => {
    try {
      const res = await fetch(`${API_BASE}/cohorts`)
      if (res.ok) setCohorts(await res.json())
    } catch (e) { console.error(e) }
  }

  const fetchAnalyses = async () => {
    try {
      const res = await fetch(`${API_BASE}/analyses?active_only=true`)
      if (res.ok) setAnalyses(await res.json())
    } catch (e) { console.error(e) }
  }

  const fetchGpus = async () => {
    try {
      const res = await fetch(`${API_BASE}/cluster/gpus`)
      if (res.ok) setGpus(await res.json())
    } catch { /* ignore */ }
  }

  const handleSearch = async () => {
    if (!searchQuery.trim()) return
    try {
      const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(searchQuery)}&limit=100`)
      if (res.ok) {
        const data = await res.json()
        setSearchResults(data.results)
      }
    } catch (e) { console.error(e) }
  }

  const toggleSlide = (hash: string) => {
    setSelectedHashes((prev) => {
      const next = new Set(prev)
      if (next.has(hash)) next.delete(hash)
      else next.add(hash)
      return next
    })
  }

  const selectAll = () => {
    if (selectedHashes.size === searchResults.length) {
      setSelectedHashes(new Set())
    } else {
      setSelectedHashes(new Set(searchResults.map((s) => s.slide_hash)))
    }
  }

  const handleSubmit = async () => {
    if (!selectedAnalysisId) return
    setIsSubmitting(true)
    setSubmitResult(null)

    try {
      let url: string
      let body: Record<string, unknown>

      if (mode === 'cohort' && selectedCohortId) {
        url = `${API_BASE}/jobs/submit-cohort/${selectedCohortId}`
        body = {
          analysis_id: selectedAnalysisId,
          gpu_index: selectedGpu,
          remote_wsi_dir: remoteWsiDir,
          remote_output_dir: remoteOutputDir,
          parameters: parameters || undefined,
          submitted_by: submittedBy || undefined,
        }
      } else {
        url = `${API_BASE}/jobs/submit`
        body = {
          analysis_id: selectedAnalysisId,
          slide_hashes: Array.from(selectedHashes),
          gpu_index: selectedGpu,
          remote_wsi_dir: remoteWsiDir,
          remote_output_dir: remoteOutputDir,
          parameters: parameters || undefined,
          submitted_by: submittedBy || undefined,
        }
      }

      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })

      if (res.ok) {
        const data = await res.json()
        const errMsg = data.errors?.length ? ` (${data.errors.length} errors)` : ''
        const slidesMsg = data.slides_created != null ? `${data.slides_created} slide(s)` : ''
        const jobMsg = data.job_id != null ? `Job #${data.job_id}` : 'Job'
        setSubmitResult({
          success: true,
          message: `${jobMsg} submitted with ${slidesMsg}${errMsg}. Transferring slides...`,
        })
        if (data.job_id) {
          setTrackedJobId(data.job_id)
          setTrackedJob(null)
        }
      } else {
        const err = await res.json()
        setSubmitResult({ success: false, message: err.detail || 'Submission failed' })
      }
    } catch {
      setSubmitResult({ success: false, message: 'Network error' })
    } finally {
      setIsSubmitting(false)
    }
  }

  const slideCount = mode === 'cohort'
    ? cohorts.find((c) => c.id === selectedCohortId)?.slide_count ?? 0
    : selectedHashes.size

  return (
    <div className="space-y-6">
      {!clusterConnected && (
        <div className="flex items-center gap-2 p-4 rounded-lg bg-yellow-500/10 text-yellow-700 border border-yellow-300">
          <AlertTriangle className="h-5 w-5 shrink-0" />
          <p className="text-sm">
            Connect to the cluster first to submit jobs. Use the connection panel above.
          </p>
        </div>
      )}

      {submitResult && (
        <div
          className={`p-4 rounded-lg ${
            submitResult.success ? 'bg-green-500/10 text-green-700' : 'bg-red-500/10 text-red-700'
          }`}
        >
          {submitResult.message}
          <button onClick={() => setSubmitResult(null)} className="ml-2 underline">
            Dismiss
          </button>
        </div>
      )}

      {/* Job progress tracker */}
      {trackedJob && (
        <div className="rounded-lg border p-4 space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              {(trackedJob.status === 'pending' || trackedJob.status === 'transferring' || trackedJob.status === 'running') ? (
                <Loader2 className="h-4 w-4 animate-spin text-blue-600" />
              ) : trackedJob.status === 'completed' ? (
                <CheckCircle className="h-4 w-4 text-green-600" />
              ) : (
                <XCircle className="h-4 w-4 text-red-600" />
              )}
              <span className="text-sm font-medium">
                Job #{trackedJob.id} — {trackedJob.status === 'transferring' ? 'Transferring slides' : trackedJob.status === 'running' ? 'Running analysis' : trackedJob.status}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">
                {trackedJob.completed_count + trackedJob.failed_count} / {trackedJob.slide_count} done
              </span>
              {(trackedJob.status === 'completed' || trackedJob.status === 'failed') && (
                <Button variant="ghost" size="sm" onClick={() => { setTrackedJobId(null); setTrackedJob(null) }}>
                  Dismiss
                </Button>
              )}
            </div>
          </div>

          {/* Overall progress bar */}
          <div className="h-3 bg-gray-200 rounded-full overflow-hidden">
            <div className="h-full flex transition-all duration-500">
              {trackedJob.completed_count > 0 && (
                <div
                  className="bg-green-500 h-full"
                  style={{ width: `${(trackedJob.completed_count / trackedJob.slide_count) * 100}%` }}
                />
              )}
              {trackedJob.failed_count > 0 && (
                <div
                  className="bg-red-500 h-full"
                  style={{ width: `${(trackedJob.failed_count / trackedJob.slide_count) * 100}%` }}
                />
              )}
              {trackedJob.status === 'transferring' && (
                <div
                  className="bg-purple-400 h-full animate-pulse"
                  style={{ width: `${Math.max(5, ((trackedJob.slide_count - trackedJob.completed_count - trackedJob.failed_count) / trackedJob.slide_count) * 20)}%` }}
                />
              )}
              {trackedJob.status === 'running' && trackedJob.completed_count === 0 && trackedJob.failed_count === 0 && (
                <div className="bg-blue-400 h-full animate-pulse" style={{ width: '10%' }} />
              )}
            </div>
          </div>

          {/* Per-slide breakdown */}
          {trackedJob.slides && trackedJob.slides.length > 0 && (
            <div className="grid gap-1 max-h-[200px] overflow-auto">
              {trackedJob.slides.map((s) => (
                <div key={s.id} className="flex items-center gap-2 text-xs">
                  <span className="font-mono w-28 truncate">{s.slide_hash ? s.slide_hash.slice(0, 12) + '...' : '-'}</span>
                  <Badge
                    variant="outline"
                    className={`text-[10px] px-1.5 py-0 ${
                      s.status === 'completed' ? 'bg-green-500/10 text-green-700' :
                      s.status === 'failed' ? 'bg-red-500/10 text-red-700' :
                      s.status === 'transferring' ? 'bg-purple-500/10 text-purple-700' :
                      s.status === 'running' ? 'bg-blue-500/10 text-blue-700' :
                      'bg-gray-500/10 text-gray-600'
                    }`}
                  >
                    {s.status}
                  </Badge>
                  {s.error_message && (
                    <span className="text-red-600 truncate">{s.error_message}</span>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Step indicator */}
      <div className="flex items-center gap-4">
        <div
          className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium ${
            step === 1 ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'
          }`}
        >
          1. Select Slides
        </div>
        <div className="h-px w-8 bg-border" />
        <div
          className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium ${
            step === 2 ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'
          }`}
        >
          2. Configure & Submit
        </div>
      </div>

      {step === 1 && (
        <div className="space-y-4">
          {/* Mode toggle */}
          <div className="flex gap-2">
            <Button
              variant={mode === 'search' ? 'default' : 'outline'}
              size="sm"
              onClick={() => setMode('search')}
            >
              <Search className="mr-2 h-4 w-4" />
              Search Slides
            </Button>
            <Button
              variant={mode === 'cohort' ? 'default' : 'outline'}
              size="sm"
              onClick={() => setMode('cohort')}
            >
              <Users className="mr-2 h-4 w-4" />
              Pick Cohort
            </Button>
          </div>

          {mode === 'search' && (
            <div className="space-y-4">
              <div className="flex gap-2">
                <Input
                  placeholder="Search by accession number..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
                />
                <Button onClick={handleSearch}>Search</Button>
              </div>

              {searchResults.length > 0 && (
                <div className="rounded-lg border max-h-[400px] overflow-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-10">
                          <input
                            type="checkbox"
                            checked={selectedHashes.size === searchResults.length && searchResults.length > 0}
                            onChange={selectAll}
                            className="h-4 w-4"
                          />
                        </TableHead>
                        <TableHead>Accession</TableHead>
                        <TableHead>Block</TableHead>
                        <TableHead>Stain</TableHead>
                        <TableHead>Year</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {searchResults.map((s) => (
                        <TableRow
                          key={s.slide_hash}
                          className="cursor-pointer"
                          onClick={() => toggleSlide(s.slide_hash)}
                        >
                          <TableCell>
                            <input
                              type="checkbox"
                              checked={selectedHashes.has(s.slide_hash)}
                              onChange={() => toggleSlide(s.slide_hash)}
                              className="h-4 w-4"
                            />
                          </TableCell>
                          <TableCell className="font-mono text-sm">{s.accession_number}</TableCell>
                          <TableCell>{s.block_id}</TableCell>
                          <TableCell>{s.stain_type}</TableCell>
                          <TableCell>{s.year}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}

              <p className="text-sm text-muted-foreground">
                {selectedHashes.size} slide(s) selected
              </p>
            </div>
          )}

          {mode === 'cohort' && (
            <div className="space-y-4">
              <div className="rounded-lg border max-h-[400px] overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-10" />
                      <TableHead>Name</TableHead>
                      <TableHead>Slides</TableHead>
                      <TableHead>Cases</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {cohorts.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={4} className="h-16 text-center text-muted-foreground">
                          No cohorts available
                        </TableCell>
                      </TableRow>
                    ) : (
                      cohorts.map((c) => (
                        <TableRow
                          key={c.id}
                          className="cursor-pointer"
                          onClick={() => setSelectedCohortId(c.id)}
                        >
                          <TableCell>
                            <input
                              type="radio"
                              name="cohort"
                              checked={selectedCohortId === c.id}
                              onChange={() => setSelectedCohortId(c.id)}
                              className="h-4 w-4"
                            />
                          </TableCell>
                          <TableCell className="font-medium">{c.name}</TableCell>
                          <TableCell>{c.slide_count}</TableCell>
                          <TableCell>{c.case_count}</TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </div>
          )}

          <Button
            onClick={() => setStep(2)}
            disabled={slideCount === 0}
          >
            Next: Configure Analysis ({slideCount} slides)
          </Button>
        </div>
      )}

      {step === 2 && (
        <div className="space-y-4">
          <Button variant="ghost" size="sm" onClick={() => setStep(1)}>
            &larr; Back to slide selection
          </Button>

          <div className="space-y-2">
            <label className="text-sm font-medium">Analysis Pipeline *</label>
            <div className="grid gap-2">
              {analyses.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No active analyses. Register one in the Registry tab first.
                </p>
              ) : (
                analyses.map((a) => (
                  <label
                    key={a.id}
                    className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
                      selectedAnalysisId === a.id
                        ? 'border-primary bg-primary/5'
                        : 'hover:bg-muted/50'
                    }`}
                  >
                    <input
                      type="radio"
                      name="analysis"
                      checked={selectedAnalysisId === a.id}
                      onChange={() => {
                        setSelectedAnalysisId(a.id)
                        setParameters(a.default_parameters || '')
                      }}
                      className="h-4 w-4"
                    />
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium">{a.name}</span>
                        <Badge variant="secondary">v{a.version}</Badge>
                        {a.gpu_required && <Badge>GPU</Badge>}
                      </div>
                      {a.description && (
                        <p className="text-sm text-muted-foreground">{a.description}</p>
                      )}
                    </div>
                    <span className="text-xs text-muted-foreground">~{a.estimated_runtime_minutes} min</span>
                  </label>
                ))
              )}
            </div>
          </div>

          {/* GPU Selection */}
          {clusterConnected && gpus.length > 0 && (
            <div className="space-y-2">
              <label className="text-sm font-medium">GPU Selection</label>
              <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                {gpus.map((gpu) => {
                  const memPct = Math.round((gpu.memory_used_mb / gpu.memory_total_mb) * 100)
                  const isBusy = gpu.utilization_pct > 50 || memPct > 70
                  return (
                    <label
                      key={gpu.index}
                      className={`flex items-start gap-2 p-3 rounded-lg border cursor-pointer transition-colors ${
                        selectedGpu === gpu.index
                          ? 'border-primary bg-primary/5'
                          : isBusy
                            ? 'border-yellow-300 bg-yellow-500/5'
                            : 'hover:bg-muted/50'
                      }`}
                    >
                      <input
                        type="radio"
                        name="gpu"
                        checked={selectedGpu === gpu.index}
                        onChange={() => setSelectedGpu(gpu.index)}
                        className="h-4 w-4 mt-0.5"
                      />
                      <div className="text-xs space-y-0.5">
                        <p className="font-medium">GPU {gpu.index}</p>
                        <p className="text-muted-foreground truncate">{gpu.name}</p>
                        <p>Mem: {gpu.memory_used_mb}/{gpu.memory_total_mb} MB ({memPct}%)</p>
                        <p>Util: {gpu.utilization_pct}%</p>
                      </div>
                    </label>
                  )
                })}
              </div>
            </div>
          )}

          {/* Remote paths */}
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Remote WSI Directory</label>
              <Input
                placeholder="/path/to/wsi/on/cluster"
                value={remoteWsiDir}
                onChange={(e) => setRemoteWsiDir(e.target.value)}
                className="font-mono text-sm"
              />
              <p className="text-xs text-muted-foreground">Slides will be rsynced here</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Remote Output Directory</label>
              <Input
                placeholder="/path/to/output/on/cluster"
                value={remoteOutputDir}
                onChange={(e) => setRemoteOutputDir(e.target.value)}
                className="font-mono text-sm"
              />
              <p className="text-xs text-muted-foreground">Results will be written here</p>
            </div>
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium">Parameters (JSON, optional)</label>
            <textarea
              className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[80px]"
              placeholder='{"batch_size": 4}'
              value={parameters}
              onChange={(e) => setParameters(e.target.value)}
            />
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium">Submitted By (optional)</label>
            <Input
              placeholder="Your name"
              value={submittedBy}
              onChange={(e) => setSubmittedBy(e.target.value)}
            />
          </div>

          <Button
            onClick={handleSubmit}
            disabled={isSubmitting || !selectedAnalysisId || slideCount === 0 || !clusterConnected}
            className="w-full"
          >
            <Send className="mr-2 h-4 w-4" />
            {isSubmitting
              ? 'Submitting (rsync + tmux)...'
              : !clusterConnected
                ? 'Connect to cluster first'
                : `Submit Job (${slideCount} slides)`}
          </Button>
        </div>
      )}
    </div>
  )
}
