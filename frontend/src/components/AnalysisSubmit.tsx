import { useState, useEffect, useRef, useMemo } from 'react'
import {
  Search, Send, Users, AlertTriangle, Loader2, CheckCircle, XCircle,
  ChevronDown, ChevronRight, Tag, Hash, Stethoscope, FolderOpen, Microscope,
  FileText, Play,
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
import type { Analysis, AnalysisJob, Cohort, CohortDetail, CohortPatient, CohortSlide, Slide, GpuInfo, Study, StudyDetail, StudySlide } from '@/types/slide'
import { signalClusterDisconnected } from '@/components/ClusterConnect'

import { getApiBase, normalizeAccession, isDemo } from '@/api'
import { displaySlide, displayCase } from '@/lib/display'
import { SortableHeader } from '@/components/SortableHeader'
import { useSortable } from '@/hooks/useSortable'

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
  const [mode, setMode] = useState<'search' | 'cohort' | 'tag' | 'study'>('search')

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
  // Slide-level selection used in "specific" scope — lets users drill patient → case → slide
  const [cohortSelectedHashes, setCohortSelectedHashes] = useState<Set<string>>(new Set())
  const [expandedPatients, setExpandedPatients] = useState<Set<number>>(new Set())
  const [expandedCases, setExpandedCases] = useState<Set<string>>(new Set())

  // ── Tag/Flag mode ────────────────────────────────────────────────────
  const [tags, setTags] = useState<TagInfo[]>([])
  const [selectedTagName, setSelectedTagName] = useState<string>('')
  const [tagSlides, setTagSlides] = useState<Slide[]>([])
  const [tagSelectedHashes, setTagSelectedHashes] = useState<Set<string>>(new Set())
  const [loadingTagSlides, setLoadingTagSlides] = useState(false)

  // ── Study mode ───────────────────────────────────────────────────────
  const [studies, setStudies] = useState<Study[]>([])
  const [selectedStudyId, setSelectedStudyId] = useState<number | null>(null)
  const [studyDetail, setStudyDetail] = useState<StudyDetail | null>(null)
  const [loadingStudy, setLoadingStudy] = useState(false)
  const [studySelectedHashes, setStudySelectedHashes] = useState<Set<string>>(new Set())

  const { sorted: sortedSearchResults, sortConfig: searchSortConfig, handleSort: handleSearchSort } = useSortable(searchResults)
  const { sorted: sortedTagSlides, sortConfig: tagSortConfig, handleSort: handleTagSort } = useSortable(tagSlides)

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

  // Output files (shown after job completes)
  type OutputGroup = { slide_hash: string; label: string; files: string[]; annotation_count: number; is_local: boolean }
  const [outputGroups, setOutputGroups] = useState<OutputGroup[] | null>(null)
  const [loadingOutputs, setLoadingOutputs] = useState(false)

  // ── Initial fetches ──────────────────────────────────────────────────
  useEffect(() => {
    fetchCohorts()
    fetchAnalyses()
    fetchTags()
    fetchStudies()
  }, [])

  // Poll job progress
  useEffect(() => {
    if (!trackedJobId) return
    const poll = async () => {
      try {
        const res = await fetch(`${getApiBase()}/jobs/${trackedJobId}`)
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
    pollRef.current = setInterval(poll, isDemo() ? 1500 : 10000)
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
          fetch(`${getApiBase()}/cohorts/${selectedCohortId}`),
          fetch(`${getApiBase()}/cohorts/${selectedCohortId}/patients`),
        ])
        if (detailRes.ok) {
          const detail: CohortDetail = await detailRes.json()
          setCohortDetail(detail)
          // Default: every slide selected (so "specific" scope starts equivalent to "all")
          setCohortSelectedHashes(new Set(detail.slides.map(s => s.slide_hash)))
        }
        if (patientsRes.ok) {
          const pts: CohortPatient[] = await patientsRes.json()
          setCohortPatients(pts)
        }
      } catch (e) { console.error(e) }
      finally { setLoadingCohortDetail(false) }
    }
    load()
    setPatientSelectMode('all')
    setExpandedPatients(new Set())
    setExpandedCases(new Set())
  }, [selectedCohortId])

  // Fetch tag slides when tag selected
  useEffect(() => {
    if (!selectedTagName || mode !== 'tag') return
    const load = async () => {
      setLoadingTagSlides(true)
      try {
        const res = await fetch(`${getApiBase()}/tags/${encodeURIComponent(selectedTagName)}/slides`)
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

  // Fetch study detail when study selected
  useEffect(() => {
    if (!selectedStudyId || mode !== 'study') {
      setStudyDetail(null)
      return
    }
    const load = async () => {
      setLoadingStudy(true)
      try {
        const res = await fetch(`${getApiBase()}/studies/${selectedStudyId}`)
        if (res.ok) {
          const data: StudyDetail = await res.json()
          setStudyDetail(data)
          setStudySelectedHashes(new Set(data.slides.map(s => s.slide_hash)))
        }
      } catch (e) { console.error(e) }
      finally { setLoadingStudy(false) }
    }
    load()
  }, [selectedStudyId, mode])

  // ── Fetch functions ──────────────────────────────────────────────────
  const fetchCohorts = async () => {
    try {
      const res = await fetch(`${getApiBase()}/cohorts`)
      if (res.ok) setCohorts(await res.json())
    } catch (e) { console.error(e) }
  }

  const fetchAnalyses = async () => {
    try {
      const res = await fetch(`${getApiBase()}/analyses?active_only=true`)
      if (res.ok) setAnalyses(await res.json())
    } catch (e) { console.error(e) }
  }

  const fetchTags = async () => {
    try {
      const res = await fetch(`${getApiBase()}/tags`)
      if (res.ok) setTags(await res.json())
    } catch (e) { console.error(e) }
  }

  const fetchStudies = async () => {
    try {
      const res = await fetch(`${getApiBase()}/studies`)
      if (res.ok) setStudies(await res.json())
    } catch (e) { console.error(e) }
  }

  const fetchGpus = async () => {
    try {
      const res = await fetch(`${getApiBase()}/cluster/gpus`)
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

  // slide rows grouped by case_hash — lets us drill from a patient/case down to slides
  const slidesByCase = useMemo(() => {
    const m = new Map<string, CohortSlide[]>()
    if (cohortDetail) {
      for (const s of cohortDetail.slides) {
        const key = s.case_hash || s.slide_hash
        if (!m.has(key)) m.set(key, [])
        m.get(key)!.push(s)
      }
    }
    return m
  }, [cohortDetail])

  const caseHashesForPatient = (p: CohortPatient) => p.surgeries.map(s => s.case_hash)
  const slideHashesForCase = (caseHash: string) =>
    (slidesByCase.get(caseHash) || []).map(s => s.slide_hash)
  const slideHashesForPatient = (p: CohortPatient) =>
    caseHashesForPatient(p).flatMap(slideHashesForCase)

  // tri-state for a group checkbox given the slide hashes it covers
  const groupCheckState = (hashes: string[]): boolean | 'indeterminate' => {
    if (hashes.length === 0) return false
    const selected = hashes.reduce((n, h) => n + (cohortSelectedHashes.has(h) ? 1 : 0), 0)
    if (selected === 0) return false
    if (selected === hashes.length) return true
    return 'indeterminate'
  }

  // ── Slide count for "Next" button ────────────────────────────────────
  const slideCount = useMemo(() => {
    if (mode === 'search') return selectedHashes.size
    if (mode === 'tag') return tagSelectedHashes.size
    if (mode === 'study') return studySelectedHashes.size
    if (mode === 'cohort') {
      if (!selectedCohortId) return 0
      if (patientSelectMode === 'all') {
        return cohorts.find(c => c.id === selectedCohortId)?.slide_count ?? 0
      }
      // Specific scope tracks an explicit set of slide hashes
      return cohortSelectedHashes.size
    }
    return 0
  }, [mode, selectedHashes, tagSelectedHashes, studySelectedHashes, selectedCohortId, cohorts, patientSelectMode, cohortSelectedHashes])

  // ── Search ───────────────────────────────────────────────────────────
  const handleSearch = async () => {
    if (!searchQuery.trim()) return
    try {
      const queries = searchQuery.includes(',')
        ? searchQuery.split(',').map(s => s.trim()).filter(Boolean)
        : [searchQuery.trim()]

      if (queries.length <= 1) {
        const q = normalizeAccession(queries[0])
        const res = await fetch(`${getApiBase()}/search?q=${encodeURIComponent(q)}&limit=100`)
        if (res.ok) {
          const data = await res.json()
          setSearchResults(data.results)
        }
      } else {
        const allResults: Slide[] = []
        const seen = new Set<string>()
        for (const raw of queries) {
          const q = normalizeAccession(raw)
          const res = await fetch(`${getApiBase()}/search?q=${encodeURIComponent(q)}&limit=100`)
          if (res.ok) {
            const data = await res.json()
            for (const slide of data.results || []) {
              if (!seen.has(slide.slide_hash)) {
                seen.add(slide.slide_hash)
                allResults.push(slide)
              }
            }
          }
        }
        setSearchResults(allResults)
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

  // ── Study slide toggle ──────────────────────────────────────────────
  const toggleStudySlide = (hash: string) => {
    setStudySelectedHashes(prev => {
      const next = new Set(prev)
      if (next.has(hash)) next.delete(hash)
      else next.add(hash)
      return next
    })
  }

  const toggleAllStudySlides = () => {
    if (!studyDetail) return
    if (studySelectedHashes.size >= studyDetail.slides.length) setStudySelectedHashes(new Set())
    else setStudySelectedHashes(new Set(studyDetail.slides.map(s => s.slide_hash)))
  }

  // ── Cohort slide selection (specific scope) ──────────────────────────
  // Add/remove a batch of slide hashes from the selection.
  const setHashesSelected = (hashes: string[], on: boolean) => {
    setCohortSelectedHashes(prev => {
      const next = new Set(prev)
      if (on) hashes.forEach(h => next.add(h))
      else hashes.forEach(h => next.delete(h))
      return next
    })
  }

  // Toggle a group (patient/case): if not fully selected, select all; else clear all.
  const toggleGroup = (hashes: string[]) => {
    const state = groupCheckState(hashes)
    setHashesSelected(hashes, state !== true)
  }

  const toggleCohortSlide = (hash: string) => {
    setCohortSelectedHashes(prev => {
      const next = new Set(prev)
      if (next.has(hash)) next.delete(hash)
      else next.add(hash)
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

  const toggleExpandCase = (caseHash: string) => {
    setExpandedCases(prev => {
      const next = new Set(prev)
      if (next.has(caseHash)) next.delete(caseHash)
      else next.add(caseHash)
      return next
    })
  }

  const allCohortHashes = cohortDetail ? cohortDetail.slides.map(s => s.slide_hash) : []
  const toggleAllCohortSlides = () => {
    if (cohortSelectedHashes.size >= allCohortHashes.length) setCohortSelectedHashes(new Set())
    else setCohortSelectedHashes(new Set(allCohortHashes))
  }

  // Quick-run a single slide: select just that slide and jump to configure/submit.
  const runSingleSlide = (hash: string) => {
    setCohortSelectedHashes(new Set([hash]))
    setStep(2)
  }

  // A single slide row inside the cohort drill-down (deepest level).
  const renderCohortSlideRow = (s: CohortSlide) => (
    <div key={s.slide_hash} className="group flex items-center gap-3 pl-16 pr-4 py-1 hover:bg-muted/30 transition-colors">
      <Checkbox
        checked={cohortSelectedHashes.has(s.slide_hash)}
        onCheckedChange={() => toggleCohortSlide(s.slide_hash)}
      />
      <Microscope className="h-3 w-3 shrink-0 text-muted-foreground" />
      <span className="text-xs font-mono flex-1 truncate">{displaySlide(s)}</span>
      <span className="text-[10px] text-muted-foreground shrink-0">{s.stain_type}</span>
      <Button
        variant="ghost"
        size="sm"
        className="h-6 px-2 text-xs opacity-0 group-hover:opacity-100 transition-opacity"
        onClick={() => runSingleSlide(s.slide_hash)}
        title="Configure & run analysis on just this slide"
      >
        <Play className="h-3 w-3 mr-1" /> Run
      </Button>
    </div>
  )

  // ── Slide name helper ────────────────────────────────────────────────
  const slideName = (s: Slide) => displaySlide(s)

  // ── Output file fetch ────────────────────────────────────────────────
  const fetchOutputFiles = async () => {
    if (!trackedJobId) return
    setLoadingOutputs(true)
    try {
      const res = await fetch(`${getApiBase()}/jobs/${trackedJobId}/output-filenames`)
      if (res.ok) {
        const data = await res.json()
        setOutputGroups(data)
      }
    } catch (e) { console.error('Failed to load output files:', e) }
    finally { setLoadingOutputs(false) }
  }

  // ── Submit ───────────────────────────────────────────────────────────
  const handleSubmit = async () => {
    if (!selectedAnalysisId) return
    setIsSubmitting(true)
    setSubmitResult(null)

    try {
      let url: string
      let body: Record<string, unknown>

      if (mode === 'cohort' && selectedCohortId && patientSelectMode === 'all') {
        url = `${getApiBase()}/jobs/submit-cohort/${selectedCohortId}`
        body = {
          analysis_id: selectedAnalysisId,
          gpu_index: selectedGpu,
          remote_wsi_dir: remoteWsiDir,
          remote_output_dir: remoteOutputDir,
          parameters: parameters || undefined,
          submitted_by: submittedBy || undefined,
        }
      } else {
        const hashes = mode === 'tag'
          ? Array.from(tagSelectedHashes)
          : mode === 'study'
            ? Array.from(studySelectedHashes)
            : mode === 'cohort'
              ? Array.from(cohortSelectedHashes)
              : Array.from(selectedHashes)
        url = `${getApiBase()}/jobs/submit`
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
          setOutputGroups(null)
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
                    {displaySlide(s as any)}
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

          {trackedJob.status === 'completed' && (
            <div className="pt-2 border-t space-y-2">
              {!outputGroups ? (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={fetchOutputFiles}
                  disabled={loadingOutputs}
                >
                  {loadingOutputs ? (
                    <><Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> Loading…</>
                  ) : (
                    <><FileText className="mr-2 h-3.5 w-3.5" /> View Output Files</>
                  )}
                </Button>
              ) : (
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-medium text-muted-foreground">Output files</span>
                    <Button variant="ghost" size="sm" onClick={() => setOutputGroups(null)} className="h-6 text-xs">
                      Hide
                    </Button>
                  </div>
                  {outputGroups.length === 0 ? (
                    <p className="text-xs text-muted-foreground">No output files found.</p>
                  ) : (
                    <div className="space-y-2">
                      {outputGroups.map((g) => (
                        <div key={g.slide_hash} className="rounded-md border bg-muted/30 p-2 space-y-1">
                          <div className="flex items-center gap-2">
                            <span className="text-xs font-mono font-medium">{g.label}</span>
                            <span className="text-[10px] text-muted-foreground">{g.files.length} file{g.files.length !== 1 ? 's' : ''}</span>
                          </div>
                          {g.files.length > 0 && (
                            <ul className="space-y-0.5 pl-1">
                              {g.files.map((f) => (
                                <li key={f} className="flex items-center gap-2 text-[11px] font-mono text-muted-foreground">
                                  <FileText className="h-3 w-3 shrink-0" />
                                  {f}
                                </li>
                              ))}
                            </ul>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
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
            <Button variant={mode === 'study' ? 'default' : 'outline'} size="sm" onClick={() => setMode('study')}>
              <FolderOpen className="mr-2 h-4 w-4" />
              From Study
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
                        <TableHead><SortableHeader label="Block" sortKey="block_id" sortConfig={searchSortConfig} onSort={handleSearchSort} /></TableHead>
                        <TableHead><SortableHeader label="Stain" sortKey="stain_type" sortConfig={searchSortConfig} onSort={handleSearchSort} /></TableHead>
                        <TableHead><SortableHeader label="Year" sortKey="year" sortConfig={searchSortConfig} onSort={handleSearchSort} /></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {sortedSearchResults.map(s => (
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
                            {showHashes ? s.slide_hash.slice(0, 16) + '…' : displaySlide(s)}
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
                            onClick={() => setPatientSelectMode('specific')}
                          >
                            Select slides
                          </button>
                        </div>
                      </div>

                      {patientSelectMode === 'specific' && (
                        <div className="rounded-lg border divide-y max-h-96 overflow-auto">
                          {/* Select all header */}
                          <div
                            className="flex items-center gap-3 px-4 py-2.5 bg-muted/30 cursor-pointer sticky top-0 z-10"
                            onClick={toggleAllCohortSlides}
                          >
                            <Checkbox
                              checked={groupCheckState(allCohortHashes)}
                              onCheckedChange={toggleAllCohortSlides}
                            />
                            <span className="text-sm font-medium">
                              {cohortSelectedHashes.size} / {allCohortHashes.length} slides selected
                            </span>
                            <span className="text-xs text-muted-foreground ml-auto">
                              Expand a patient → case to pick individual slides
                            </span>
                          </div>

                          {/* Assigned patients → surgeries (cases) → slides */}
                          {cohortPatients.map(patient => {
                            const pHashes = slideHashesForPatient(patient)
                            const isExpanded = expandedPatients.has(patient.id)
                            return (
                              <div key={patient.id}>
                                <div className="flex items-center gap-3 px-4 py-2 hover:bg-muted/30 transition-colors">
                                  <Checkbox
                                    checked={groupCheckState(pHashes)}
                                    onCheckedChange={() => toggleGroup(pHashes)}
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
                                      {pHashes.length} slides
                                    </span>
                                  </button>
                                </div>
                                {isExpanded && patient.surgeries.map(surgery => {
                                  const cHashes = slideHashesForCase(surgery.case_hash)
                                  const caseSlides = slidesByCase.get(surgery.case_hash) || []
                                  const caseExpanded = expandedCases.has(surgery.case_hash)
                                  return (
                                    <div key={surgery.id} className="bg-muted/5">
                                      <div className="flex items-center gap-3 pl-9 pr-4 py-1.5 hover:bg-muted/30 transition-colors">
                                        <Checkbox
                                          checked={groupCheckState(cHashes)}
                                          onCheckedChange={() => toggleGroup(cHashes)}
                                          onClick={e => e.stopPropagation()}
                                        />
                                        <button
                                          className="flex-1 flex items-center gap-2 text-left"
                                          onClick={() => caseSlides.length > 0 && toggleExpandCase(surgery.case_hash)}
                                        >
                                          {caseSlides.length > 0
                                            ? (caseExpanded
                                              ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                                              : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />)
                                            : <span className="w-3.5 shrink-0" />
                                          }
                                          <Stethoscope className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                                          <span className="text-sm font-medium">{surgery.surgery_label}</span>
                                          <span className="text-xs text-muted-foreground">{displayCase(surgery)}</span>
                                          <span className="ml-auto text-xs text-muted-foreground">{cHashes.length} slides</span>
                                        </button>
                                      </div>
                                      {caseExpanded && caseSlides.map(renderCohortSlideRow)}
                                    </div>
                                  )
                                })}
                              </div>
                            )
                          })}

                          {/* Unassigned cases → slides */}
                          {unassignedCases.map(c => {
                            const cHashes = slideHashesForCase(c.case_hash)
                            const caseSlides = slidesByCase.get(c.case_hash) || []
                            const caseExpanded = expandedCases.has(c.case_hash)
                            return (
                              <div key={c.case_hash}>
                                <div className="flex items-center gap-3 px-4 py-2 hover:bg-muted/30 transition-colors">
                                  <Checkbox
                                    checked={groupCheckState(cHashes)}
                                    onCheckedChange={() => toggleGroup(cHashes)}
                                    onClick={e => e.stopPropagation()}
                                  />
                                  <button
                                    className="flex-1 flex items-center gap-2 text-left"
                                    onClick={() => caseSlides.length > 0 && toggleExpandCase(c.case_hash)}
                                  >
                                    {caseSlides.length > 0
                                      ? (caseExpanded
                                        ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                                        : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />)
                                      : <span className="w-3.5 shrink-0" />
                                    }
                                    <span className="text-sm text-muted-foreground italic">{displayCase(c)}</span>
                                    <span className="text-[10px] text-muted-foreground">unassigned</span>
                                    <span className="ml-auto text-xs text-muted-foreground">{cHashes.length} slides</span>
                                  </button>
                                </div>
                                {caseExpanded && caseSlides.map(renderCohortSlideRow)}
                              </div>
                            )
                          })}
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
                              <TableHead><SortableHeader label="Block" sortKey="block_id" sortConfig={tagSortConfig} onSort={handleTagSort} /></TableHead>
                              <TableHead><SortableHeader label="Stain" sortKey="stain_type" sortConfig={tagSortConfig} onSort={handleTagSort} /></TableHead>
                              <TableHead><SortableHeader label="Year" sortKey="year" sortConfig={tagSortConfig} onSort={handleTagSort} /></TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {sortedTagSlides.map(s => (
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

          {/* ── Study mode ── */}
          {mode === 'study' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Select study</label>
                <Select value={selectedStudyId?.toString() ?? ''} onValueChange={v => setSelectedStudyId(Number(v))}>
                  <SelectTrigger className="max-w-xs">
                    <SelectValue placeholder="Choose a study..." />
                  </SelectTrigger>
                  <SelectContent>
                    {studies.map(s => (
                      <SelectItem key={s.id} value={s.id.toString()}>
                        <div className="flex items-center gap-2">
                          <FolderOpen className="h-3.5 w-3.5 text-muted-foreground" />
                          {s.name}
                          <span className="text-xs text-muted-foreground">({s.slide_count} slides)</span>
                        </div>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  Select slides from a study folder, including external/research slides.
                </p>
              </div>

              {selectedStudyId && (
                <>
                  {loadingStudy ? (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" /> Loading study...
                    </div>
                  ) : !studyDetail || studyDetail.slides.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No slides in this study. Import files via the Studies tab first.</p>
                  ) : (
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-muted-foreground">{studyDetail.slides.length} slide{studyDetail.slides.length !== 1 ? 's' : ''} in study</span>
                        <button className="text-xs text-primary hover:underline" onClick={toggleAllStudySlides}>
                          {studySelectedHashes.size >= studyDetail.slides.length ? 'Deselect all' : 'Select all'}
                        </button>
                      </div>

                      {/* Group filter chips */}
                      {studyDetail.groups.length > 0 && (
                        <div className="flex items-center gap-1.5 flex-wrap">
                          <span className="text-xs text-muted-foreground">Groups:</span>
                          {studyDetail.groups.map(g => {
                            const groupSlideHashes = g.slide_hashes
                            const allInGroup = groupSlideHashes.every(h => studySelectedHashes.has(h))
                            return (
                              <button
                                key={g.id}
                                className={`text-[10px] font-medium px-2 py-1 rounded border transition-colors ${
                                  allInGroup ? 'border-primary bg-primary/10 text-primary' : 'border-border hover:border-primary/40'
                                }`}
                                style={g.color ? { borderColor: allInGroup ? undefined : g.color + '60' } : undefined}
                                onClick={() => {
                                  setStudySelectedHashes(prev => {
                                    const next = new Set(prev)
                                    if (allInGroup) {
                                      groupSlideHashes.forEach(h => next.delete(h))
                                    } else {
                                      groupSlideHashes.forEach(h => next.add(h))
                                    }
                                    return next
                                  })
                                }}
                              >
                                {g.color && <span className="inline-block w-2 h-2 rounded-full mr-1" style={{ backgroundColor: g.color }} />}
                                {g.label || g.name} ({groupSlideHashes.length})
                              </button>
                            )
                          })}
                        </div>
                      )}

                      <div className="rounded-lg border max-h-[350px] overflow-auto">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead className="w-10">
                                <input type="checkbox" checked={studySelectedHashes.size === studyDetail.slides.length && studyDetail.slides.length > 0} onChange={toggleAllStudySlides} className="h-4 w-4" />
                              </TableHead>
                              <TableHead>Slide</TableHead>
                              <TableHead>Block</TableHead>
                              <TableHead>Stain</TableHead>
                              <TableHead>Size</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {studyDetail.slides.map(s => (
                              <TableRow key={s.slide_hash} className="cursor-pointer" onClick={() => toggleStudySlide(s.slide_hash)}>
                                <TableCell>
                                  <input type="checkbox" checked={studySelectedHashes.has(s.slide_hash)} onChange={() => toggleStudySlide(s.slide_hash)} className="h-4 w-4" />
                                </TableCell>
                                <TableCell className="font-mono text-sm">{displaySlide(s)}</TableCell>
                                <TableCell>{s.block_id || '—'}</TableCell>
                                <TableCell>{s.stain_type || '—'}</TableCell>
                                <TableCell className="text-xs text-muted-foreground tabular-nums">
                                  {s.file_size_bytes ? `${(s.file_size_bytes / (1024*1024)).toFixed(0)} MB` : '—'}
                                </TableCell>
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
