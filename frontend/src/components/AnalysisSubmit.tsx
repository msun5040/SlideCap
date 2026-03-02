import { useState, useEffect, useRef, useMemo } from 'react'
import {
  Search, Send, Users, AlertTriangle, Loader2, CheckCircle, XCircle,
  ChevronDown, ChevronRight, Tag, Hash, Stethoscope,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import type { Analysis, AnalysisJob, Cohort, CohortDetail, CohortPatient, Slide, GpuInfo } from '@/types/slide'
import { signalClusterDisconnected } from '@/components/ClusterConnect'

const API_BASE = 'http://localhost:8000'

interface AnalysisSubmitProps {
  clusterConnected?: boolean
}

interface TagInfo {
  id: number
  name: string
  color?: string
  slide_count?: number
}

export function AnalysisSubmit({ clusterConnected = false }: AnalysisSubmitProps) {
  const [step, setStep] = useState<1 | 2>(1)
  const [mode, setMode] = useState<'search' | 'cohort' | 'tag'>('search')

  // ── Search mode ──────────────────────────────────────────────────────
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState<Slide[]>([])
  const [selectedHashes, setSelectedHashes] = useState<Set<string>>(new Set())
  const [showHashes, setShowHashes] = useState(false)

  // ── Cohort mode ──────────────────────────────────────────────────────
  const [cohorts, setCohorts] = useState<Cohort[]>([])
  const [selectedCohortId, setSelectedCohortId] = useState<number | null>(null)
  const [cohortDetail, setCohortDetail] = useState<CohortDetail | null>(null)
  const [cohortPatients, setCohortPatients] = useState<CohortPatient[]>([])
  const [loadingCohortDetail, setLoadingCohortDetail] = useState(false)
  const [patientSelectMode, setPatientSelectMode] = useState<'all' | 'specific'>('all')
  const [selectedPatientIds, setSelectedPatientIds] = useState<Set<number>>(new Set())
  const [includeUnassigned, setIncludeUnassigned] = useState(true)
  const [expandedPatients, setExpandedPatients] = useState<Set<number>>(new Set())

  // ── Tag/Flag mode ────────────────────────────────────────────────────
  const [tags, setTags] = useState<TagInfo[]>([])
  const [selectedTagName, setSelectedTagName] = useState<string>('')
  const [tagSlides, setTagSlides] = useState<Slide[]>([])
  const [tagSelectedHashes, setTagSelectedHashes] = useState<Set<string>>(new Set())
  const [loadingTagSlides, setLoadingTagSlides] = useState(false)

  // ── Step 2 — analysis + cluster config ──────────────────────────────
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

  // ── Initial fetches ──────────────────────────────────────────────────
  useEffect(() => {
    fetchCohorts()
    fetchAnalyses()
    fetchTags()
  }, [])

  // Poll job progress
  useEffect(() => {
    if (!trackedJobId) return
    const poll = async () => {
      try {
        const res = await fetch(`${API_BASE}/jobs/${trackedJobId}`)
        if (res.ok) {
          const data: AnalysisJob = await res.json()
          setTrackedJob(data)
          if (data.status === 'completed' || data.status === 'failed') {
            if (pollRef.current) clearInterval(pollRef.current)
            pollRef.current = null
          }
        }
      } catch (e) { console.error('Poll failed:', e) }
    }
    poll()
    pollRef.current = setInterval(poll, 10000)
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
      pollRef.current = null
    }
  }, [trackedJobId])

  // Fetch GPUs on step 2
  useEffect(() => {
    if (step === 2 && clusterConnected) fetchGpus()
  }, [step, clusterConnected])

  // Fetch cohort detail + patients when cohort selected
  useEffect(() => {
    if (!selectedCohortId) {
      setCohortDetail(null)
      setCohortPatients([])
      return
    }
    const load = async () => {
      setLoadingCohortDetail(true)
      try {
        const [detailRes, patientsRes] = await Promise.all([
          fetch(`${API_BASE}/cohorts/${selectedCohortId}`),
          fetch(`${API_BASE}/cohorts/${selectedCohortId}/patients`),
        ])
        if (detailRes.ok) setCohortDetail(await detailRes.json())
        if (patientsRes.ok) {
          const pts: CohortPatient[] = await patientsRes.json()
          setCohortPatients(pts)
          // Default: select all patients
          setSelectedPatientIds(new Set(pts.map(p => p.id)))
        }
      } catch (e) { console.error(e) }
      finally { setLoadingCohortDetail(false) }
    }
    load()
    setPatientSelectMode('all')
    setExpandedPatients(new Set())
  }, [selectedCohortId])

  // Fetch tag slides when tag selected
  useEffect(() => {
    if (!selectedTagName || mode !== 'tag') return
    const load = async () => {
      setLoadingTagSlides(true)
      try {
        const res = await fetch(`${API_BASE}/tags/${encodeURIComponent(selectedTagName)}/slides`)
        if (res.ok) {
          const data = await res.json()
          const slides: Slide[] = data.slides || data
          setTagSlides(slides)
          setTagSelectedHashes(new Set(slides.map((s: Slide) => s.slide_hash)))
        }
      } catch (e) { console.error(e) }
      finally { setLoadingTagSlides(false) }
    }
    load()
  }, [selectedTagName, mode])

  // ── Fetch functions ──────────────────────────────────────────────────
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

  const fetchTags = async () => {
    try {
      const res = await fetch(`${API_BASE}/tags`)
      if (res.ok) setTags(await res.json())
    } catch (e) { console.error(e) }
  }

  const fetchGpus = async () => {
    try {
      const res = await fetch(`${API_BASE}/cluster/gpus`)
      if (res.ok) setGpus(await res.json())
    } catch { /* ignore */ }
  }

  // ── Derived state ────────────────────────────────────────────────────
  const cohortCaseGroups = useMemo(() => {
    if (!cohortDetail) return []
    const map = new Map<string, { case_hash: string; accession_number: string | null; slide_count: number }>()
    for (const slide of cohortDetail.slides) {
      const key = slide.case_hash || slide.slide_hash
      if (!map.has(key)) map.set(key, { case_hash: key, accession_number: slide.accession_number, slide_count: 0 })
      map.get(key)!.slide_count++
    }
    return Array.from(map.values())
  }, [cohortDetail])

  const assignedCaseHashes = useMemo(() => {
    const set = new Set<string>()
    for (const p of cohortPatients) {
      for (const s of p.surgeries) set.add(s.case_hash)
    }
    return set
  }, [cohortPatients])

  const unassignedCases = useMemo(
    () => cohortCaseGroups.filter(g => !assignedCaseHashes.has(g.case_hash)),
    [cohortCaseGroups, assignedCaseHashes]
  )

  // ── Slide count for "Next" button ────────────────────────────────────
  const slideCount = useMemo(() => {
    if (mode === 'search') return selectedHashes.size
    if (mode === 'tag') return tagSelectedHashes.size
    if (mode === 'cohort') {
      if (!selectedCohortId) return 0
      if (patientSelectMode === 'all') {
        return cohorts.find(c => c.id === selectedCohortId)?.slide_count ?? 0
      }
      // Count from selected patients + unassigned if included
      let count = 0
      for (const p of cohortPatients) {
        if (selectedPatientIds.has(p.id)) {
          count += p.surgeries.reduce((sum, s) => sum + s.slide_count, 0)
        }
      }
      if (includeUnassigned) {
        count += unassignedCases.reduce((sum, c) => sum + c.slide_count, 0)
      }
      return count
    }
    return 0
  }, [mode, selectedHashes, tagSelectedHashes, selectedCohortId, cohorts, patientSelectMode, cohortPatients, selectedPatientIds, includeUnassigned, unassignedCases])

  const getSelectedCaseHashes = (): string[] | undefined => {
    if (patientSelectMode === 'all') return undefined
    const hashes: string[] = []
    for (const p of cohortPatients) {
      if (selectedPatientIds.has(p.id)) {
        for (const s of p.surgeries) hashes.push(s.case_hash)
      }
    }
    if (includeUnassigned) {
      for (const c of unassignedCases) hashes.push(c.case_hash)
    }
    return hashes
  }

  // ── Search ───────────────────────────────────────────────────────────
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
    setSelectedHashes(prev => {
      const next = new Set(prev)
      if (next.has(hash)) next.delete(hash)
      else next.add(hash)
      return next
    })
  }

  const toggleAllSearchSlides = () => {
    if (selectedHashes.size >= searchResults.length) setSelectedHashes(new Set())
    else setSelectedHashes(new Set(searchResults.map(s => s.slide_hash)))
  }

  // ── Tag slide toggle ─────────────────────────────────────────────────
  const toggleTagSlide = (hash: string) => {
    setTagSelectedHashes(prev => {
      const next = new Set(prev)
      if (next.has(hash)) next.delete(hash)
      else next.add(hash)
      return next
    })
  }

  const toggleAllTagSlides = () => {
    if (tagSelectedHashes.size >= tagSlides.length) setTagSelectedHashes(new Set())
    else setTagSelectedHashes(new Set(tagSlides.map(s => s.slide_hash)))
  }

  // ── Cohort patient selection ─────────────────────────────────────────
  const togglePatient = (patientId: number) => {
    setSelectedPatientIds(prev => {
      const next = new Set(prev)
      if (next.has(patientId)) next.delete(patientId)
      else next.add(patientId)
      return next
    })
  }

  const toggleExpandPatient = (patientId: number) => {
    setExpandedPatients(prev => {
      const next = new Set(prev)
      if (next.has(patientId)) next.delete(patientId)
      else next.add(patientId)
      return next
    })
  }

  const toggleAllPatients = () => {
    if (selectedPatientIds.size >= cohortPatients.length) setSelectedPatientIds(new Set())
    else setSelectedPatientIds(new Set(cohortPatients.map(p => p.id)))
  }

  // ── Slide name helper ────────────────────────────────────────────────
  const slideName = (s: Slide) =>
    s.accession_number
      ? `${s.accession_number}${s.block_id ? ` ${s.block_id}` : ''}${s.stain_type ? ` ${s.stain_type}` : ''}`
      : s.slide_hash.slice(0, 12) + '…'

  // ── Submit ───────────────────────────────────────────────────────────
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
        const caseHashes = getSelectedCaseHashes()
        if (caseHashes) body.case_hashes = caseHashes
      } else {
        const hashes = mode === 'tag'
          ? Array.from(tagSelectedHashes)
          : Array.from(selectedHashes)
        url = `${API_BASE}/jobs/submit`
        body = {
          analysis_id: selectedAnalysisId,
          slide_hashes: hashes,
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

      if (res.status === 503) {
        signalClusterDisconnected()
        setSubmitResult({ success: false, message: 'Not connected to cluster.' })
      } else if (res.ok) {
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

  // ── Render ───────────────────────────────────────────────────────────
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
        <div className={`p-4 rounded-lg ${submitResult.success ? 'bg-green-500/10 text-green-700' : 'bg-red-500/10 text-red-700'}`}>
          {submitResult.message}
          <button onClick={() => setSubmitResult(null)} className="ml-2 underline">Dismiss</button>
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
                Job #{trackedJob.id} — {
                  trackedJob.status === 'transferring' ? 'Transferring slides' :
                  trackedJob.status === 'running' ? 'Running analysis' :
                  trackedJob.status
                }
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

          <div className="h-3 bg-gray-200 rounded-full overflow-hidden">
            <div className="h-full flex transition-all duration-500">
              {trackedJob.completed_count > 0 && (
                <div className="bg-green-500 h-full" style={{ width: `${(trackedJob.completed_count / trackedJob.slide_count) * 100}%` }} />
              )}
              {trackedJob.failed_count > 0 && (
                <div className="bg-red-500 h-full" style={{ width: `${(trackedJob.failed_count / trackedJob.slide_count) * 100}%` }} />
              )}
              {trackedJob.status === 'transferring' && (
                <div className="bg-purple-400 h-full animate-pulse" style={{ width: `${Math.max(5, ((trackedJob.slide_count - trackedJob.completed_count - trackedJob.failed_count) / trackedJob.slide_count) * 20)}%` }} />
              )}
              {trackedJob.status === 'running' && trackedJob.completed_count === 0 && trackedJob.failed_count === 0 && (
                <div className="bg-blue-400 h-full animate-pulse" style={{ width: '10%' }} />
              )}
            </div>
          </div>

          {trackedJob.slides && trackedJob.slides.length > 0 && (
            <div className="grid gap-1 max-h-50 overflow-auto">
              {trackedJob.slides.map((s) => (
                <div key={s.id} className="flex items-center gap-2 text-xs">
                  <span className="font-mono truncate flex-1 text-muted-foreground">
                    {(s as any).accession_number
                      ? `${(s as any).accession_number}${(s as any).block_id ? ` ${(s as any).block_id}` : ''}${(s as any).stain_type ? ` ${(s as any).stain_type}` : ''}`
                      : s.slide_hash ? s.slide_hash.slice(0, 16) + '…' : '-'}
                  </span>
                  <Badge
                    variant="outline"
                    className={`text-[10px] px-1.5 py-0 shrink-0 ${
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
        <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium ${step === 1 ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'}`}>
          1. Select Slides
        </div>
        <div className="h-px w-8 bg-border" />
        <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium ${step === 2 ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'}`}>
          2. Configure & Submit
        </div>
      </div>

      {/* ── STEP 1: Select slides ── */}
      {step === 1 && (
        <div className="space-y-4">
          {/* Mode toggle */}
          <div className="flex gap-2 flex-wrap">
            <Button variant={mode === 'search' ? 'default' : 'outline'} size="sm" onClick={() => setMode('search')}>
              <Search className="mr-2 h-4 w-4" />
              Search Slides
            </Button>
            <Button variant={mode === 'cohort' ? 'default' : 'outline'} size="sm" onClick={() => setMode('cohort')}>
              <Users className="mr-2 h-4 w-4" />
              Pick Cohort
            </Button>
            <Button variant={mode === 'tag' ? 'default' : 'outline'} size="sm" onClick={() => setMode('tag')}>
              <Tag className="mr-2 h-4 w-4" />
              By Flag/Tag
            </Button>
          </div>

          {/* ── Search mode ── */}
          {mode === 'search' && (
            <div className="space-y-4">
              <div className="flex gap-2">
                <Input
                  placeholder="Search by accession number..."
                  value={searchQuery}
                  onChange={e => setSearchQuery(e.target.value)}
                  onKeyDown={e => e.key === 'Enter' && handleSearch()}
                />
                <Button onClick={handleSearch}>Search</Button>
              </div>

              {searchResults.length > 0 && (
                <div className="rounded-lg border max-h-100 overflow-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-10">
                          <input
                            type="checkbox"
                            checked={selectedHashes.size === searchResults.length && searchResults.length > 0}
                            onChange={toggleAllSearchSlides}
                            className="h-4 w-4"
                          />
                        </TableHead>
                        <TableHead>
                          <div className="flex items-center gap-2">
                            Slide
                            <button
                              className={`text-xs px-1.5 py-0.5 rounded border transition-colors ${showHashes ? 'bg-muted text-foreground border-border' : 'text-muted-foreground border-transparent hover:border-border'}`}
                              onClick={() => setShowHashes(v => !v)}
                              title="Toggle hash display"
                            >
                              <Hash className="h-3 w-3 inline mr-0.5" />
                              {showHashes ? 'Hash' : 'Name'}
                            </button>
                          </div>
                        </TableHead>
                        <TableHead>Block</TableHead>
                        <TableHead>Stain</TableHead>
                        <TableHead>Year</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {searchResults.map(s => (
                        <TableRow key={s.slide_hash} className="cursor-pointer" onClick={() => toggleSlide(s.slide_hash)}>
                          <TableCell>
                            <input
                              type="checkbox"
                              checked={selectedHashes.has(s.slide_hash)}
                              onChange={() => toggleSlide(s.slide_hash)}
                              onClick={e => e.stopPropagation()}
                              className="h-4 w-4"
                            />
                          </TableCell>
                          <TableCell className="font-mono text-sm">
                            {showHashes ? s.slide_hash.slice(0, 16) + '…' : (s.accession_number || s.slide_hash.slice(0, 12) + '…')}
                          </TableCell>
                          <TableCell>{s.block_id}</TableCell>
                          <TableCell>{s.stain_type}</TableCell>
                          <TableCell>{s.year}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
              <p className="text-sm text-muted-foreground">{selectedHashes.size} slide(s) selected</p>
            </div>
          )}

          {/* ── Cohort mode ── */}
          {mode === 'cohort' && (
            <div className="space-y-4">
              {/* Cohort list */}
              <div className="rounded-lg border max-h-56 overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-10" />
                      <TableHead>Cohort</TableHead>
                      <TableHead>Slides</TableHead>
                      <TableHead>Cases</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {cohorts.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={4} className="h-16 text-center text-muted-foreground">No cohorts available</TableCell>
                      </TableRow>
                    ) : (
                      cohorts.map(c => (
                        <TableRow key={c.id} className="cursor-pointer" onClick={() => setSelectedCohortId(c.id)}>
                          <TableCell>
                            <input type="radio" name="cohort" checked={selectedCohortId === c.id} onChange={() => setSelectedCohortId(c.id)} className="h-4 w-4" />
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

              {/* Patient/surgery breakdown */}
              {selectedCohortId && (
                <div className="space-y-3">
                  {loadingCohortDetail ? (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" /> Loading patients...
                    </div>
                  ) : (
                    <>
                      {/* Select mode toggle */}
                      <div className="flex items-center gap-3">
                        <label className="text-sm font-medium">Scope:</label>
                        <div className="flex gap-2">
                          <button
                            className={`text-sm px-3 py-1 rounded-md border transition-colors ${patientSelectMode === 'all' ? 'bg-primary text-primary-foreground border-primary' : 'border-input hover:bg-muted'}`}
                            onClick={() => setPatientSelectMode('all')}
                          >
                            All slides
                          </button>
                          <button
                            className={`text-sm px-3 py-1 rounded-md border transition-colors ${patientSelectMode === 'specific' ? 'bg-primary text-primary-foreground border-primary' : 'border-input hover:bg-muted'}`}
                            onClick={() => {
                              setPatientSelectMode('specific')
                              setSelectedPatientIds(new Set(cohortPatients.map(p => p.id)))
                            }}
                          >
                            Select patients
                          </button>
                        </div>
                      </div>

                      {patientSelectMode === 'specific' && (
                        <div className="rounded-lg border divide-y max-h-72 overflow-auto">
                          {/* Select all header */}
                          <div
                            className="flex items-center gap-3 px-4 py-2.5 bg-muted/30 cursor-pointer sticky top-0"
                            onClick={toggleAllPatients}
                          >
                            <Checkbox
                              checked={selectedPatientIds.size === cohortPatients.length && cohortPatients.length > 0}
                              onCheckedChange={toggleAllPatients}
                            />
                            <span className="text-sm font-medium">
                              {selectedPatientIds.size === cohortPatients.length
                                ? `All ${cohortPatients.length} patients`
                                : `${selectedPatientIds.size} / ${cohortPatients.length} patients`}
                            </span>
                          </div>

                          {/* Assigned patients with surgeries */}
                          {cohortPatients.map(patient => {
                            const isExpanded = expandedPatients.has(patient.id)
                            const checked = selectedPatientIds.has(patient.id)
                            return (
                              <div key={patient.id}>
                                <div className="flex items-center gap-3 px-4 py-2 hover:bg-muted/30 transition-colors">
                                  <Checkbox
                                    checked={checked}
                                    onCheckedChange={() => togglePatient(patient.id)}
                                    onClick={e => e.stopPropagation()}
                                  />
                                  <button
                                    className="flex-1 flex items-center gap-2 text-left"
                                    onClick={() => toggleExpandPatient(patient.id)}
                                  >
                                    {isExpanded
                                      ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                                      : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                                    }
                                    <span className="text-sm font-medium">{patient.label}</span>
                                    <span className="text-xs text-muted-foreground">
                                      {patient.surgeries.length} surger{patient.surgeries.length !== 1 ? 'ies' : 'y'}
                                      {' · '}
                                      {patient.surgeries.reduce((s, sg) => s + sg.slide_count, 0)} slides
                                    </span>
                                  </button>
                                </div>
                                {isExpanded && patient.surgeries.length > 0 && (
                                  <div className="pb-1 bg-muted/5">
                                    {patient.surgeries.map(surgery => (
                                      <div key={surgery.id} className="flex items-center gap-3 pl-10 pr-4 py-1.5 text-sm text-muted-foreground">
                                        <Stethoscope className="h-3.5 w-3.5 shrink-0" />
                                        <span className="font-medium text-foreground">{surgery.surgery_label}</span>
                                        <span>{surgery.accession_number || surgery.case_hash.slice(0, 10) + '…'}</span>
                                        <span className="ml-auto">{surgery.slide_count} slides</span>
                                      </div>
                                    ))}
                                  </div>
                                )}
                              </div>
                            )
                          })}

                          {/* Unassigned cases */}
                          {unassignedCases.length > 0 && (
                            <div
                              className="flex items-center gap-3 px-4 py-2 hover:bg-muted/30 cursor-pointer transition-colors"
                              onClick={() => setIncludeUnassigned(v => !v)}
                            >
                              <Checkbox
                                checked={includeUnassigned}
                                onCheckedChange={() => setIncludeUnassigned(v => !v)}
                              />
                              <span className="text-sm text-muted-foreground italic">
                                Unassigned cases ({unassignedCases.length})
                              </span>
                              <span className="text-xs text-muted-foreground ml-auto">
                                {unassignedCases.reduce((s, c) => s + c.slide_count, 0)} slides
                              </span>
                            </div>
                          )}
                        </div>
                      )}

                      {patientSelectMode === 'all' && cohortDetail && (
                        <p className="text-sm text-muted-foreground">
                          All {cohortDetail.slides.length} slides across {cohortCaseGroups.length} cases will be submitted.
                        </p>
                      )}
                    </>
                  )}
                </div>
              )}
            </div>
          )}

          {/* ── Tag/Flag mode ── */}
          {mode === 'tag' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Select flag/tag</label>
                <Select value={selectedTagName} onValueChange={setSelectedTagName}>
                  <SelectTrigger className="max-w-xs">
                    <SelectValue placeholder="Choose a flag or tag..." />
                  </SelectTrigger>
                  <SelectContent>
                    {tags.filter(t => (t.slide_count ?? 0) > 0).map(tag => (
                      <SelectItem key={tag.id} value={tag.name}>
                        <div className="flex items-center gap-2">
                          {tag.color && <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: tag.color }} />}
                          {tag.name}
                          <span className="text-xs text-muted-foreground">({tag.slide_count} slides)</span>
                        </div>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  Flag slides from the Cohort view using the flag icon on each slide, then choose the <code className="text-[11px] bg-muted px-1 rounded">flagged</code> tag here.
                </p>
              </div>

              {selectedTagName && (
                <>
                  {loadingTagSlides ? (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" /> Loading slides...
                    </div>
                  ) : tagSlides.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No slides found with this flag.</p>
                  ) : (
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-muted-foreground">{tagSlides.length} slide{tagSlides.length !== 1 ? 's' : ''} with flag "{selectedTagName}"</span>
                        <button className="text-xs text-primary hover:underline" onClick={toggleAllTagSlides}>
                          {tagSelectedHashes.size >= tagSlides.length ? 'Deselect all' : 'Select all'}
                        </button>
                      </div>
                      <div className="rounded-lg border max-h-[350px] overflow-auto">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead className="w-10">
                                <input type="checkbox" checked={tagSelectedHashes.size === tagSlides.length && tagSlides.length > 0} onChange={toggleAllTagSlides} className="h-4 w-4" />
                              </TableHead>
                              <TableHead>Slide</TableHead>
                              <TableHead>Block</TableHead>
                              <TableHead>Stain</TableHead>
                              <TableHead>Year</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {tagSlides.map(s => (
                              <TableRow key={s.slide_hash} className="cursor-pointer" onClick={() => toggleTagSlide(s.slide_hash)}>
                                <TableCell>
                                  <input type="checkbox" checked={tagSelectedHashes.has(s.slide_hash)} onChange={() => toggleTagSlide(s.slide_hash)} className="h-4 w-4" />
                                </TableCell>
                                <TableCell className="font-mono text-sm">{slideName(s)}</TableCell>
                                <TableCell>{s.block_id}</TableCell>
                                <TableCell>{s.stain_type}</TableCell>
                                <TableCell>{s.year}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          )}

          <Button onClick={() => setStep(2)} disabled={slideCount === 0}>
            Next: Configure Analysis ({slideCount} slides)
          </Button>
        </div>
      )}

      {/* ── STEP 2: Configure & submit ── */}
      {step === 2 && (
        <div className="space-y-4">
          <Button variant="ghost" size="sm" onClick={() => setStep(1)}>
            ← Back to slide selection
          </Button>

          {/* Analysis selection */}
          <div className="space-y-2">
            <label className="text-sm font-medium">Analysis Pipeline *</label>
            <div className="grid gap-2">
              {analyses.length === 0 ? (
                <p className="text-sm text-muted-foreground">No active analyses. Register one in the Registry tab first.</p>
              ) : (
                analyses.map(a => (
                  <label
                    key={a.id}
                    className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${selectedAnalysisId === a.id ? 'border-primary bg-primary/5' : 'hover:bg-muted/50'}`}
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
                      {a.description && <p className="text-sm text-muted-foreground">{a.description}</p>}
                    </div>
                    <span className="text-xs text-muted-foreground">~{a.estimated_runtime_minutes} min</span>
                  </label>
                ))
              )}
            </div>
          </div>

          {/* GPU selection */}
          {clusterConnected && gpus.length > 0 && (
            <div className="space-y-2">
              <label className="text-sm font-medium">GPU Selection</label>
              <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                {gpus.map(gpu => {
                  const memPct = Math.round((gpu.memory_used_mb / gpu.memory_total_mb) * 100)
                  const isBusy = gpu.utilization_pct > 50 || memPct > 70
                  return (
                    <label
                      key={gpu.index}
                      className={`flex items-start gap-2 p-3 rounded-lg border cursor-pointer transition-colors ${selectedGpu === gpu.index ? 'border-primary bg-primary/5' : isBusy ? 'border-yellow-300 bg-yellow-500/5' : 'hover:bg-muted/50'}`}
                    >
                      <input type="radio" name="gpu" checked={selectedGpu === gpu.index} onChange={() => setSelectedGpu(gpu.index)} className="h-4 w-4 mt-0.5" />
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
              <Input placeholder="/path/to/wsi/on/cluster" value={remoteWsiDir} onChange={e => setRemoteWsiDir(e.target.value)} className="font-mono text-sm" />
              <p className="text-xs text-muted-foreground">Slides will be rsynced here</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Remote Output Directory</label>
              <Input placeholder="/path/to/output/on/cluster" value={remoteOutputDir} onChange={e => setRemoteOutputDir(e.target.value)} className="font-mono text-sm" />
              <p className="text-xs text-muted-foreground">Results will be written here</p>
            </div>
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium">Parameters (JSON, optional)</label>
            <textarea
              className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[80px]"
              placeholder='{"batch_size": 4}'
              value={parameters}
              onChange={e => setParameters(e.target.value)}
            />
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium">Submitted By (optional)</label>
            <Input placeholder="Your name" value={submittedBy} onChange={e => setSubmittedBy(e.target.value)} />
          </div>

          <Button
            onClick={handleSubmit}
            disabled={isSubmitting || !selectedAnalysisId || slideCount === 0 || !clusterConnected}
            className="w-full"
          >
            <Send className="mr-2 h-4 w-4" />
            {isSubmitting ? 'Submitting (rsync + tmux)...' :
             !clusterConnected ? 'Connect to cluster first' :
             `Submit Job (${slideCount} slides)`}
          </Button>
        </div>
      )}
    </div>
  )
}
