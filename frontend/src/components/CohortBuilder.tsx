import { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import {
  ArrowLeft, Search, Filter, X, Plus, ChevronDown, ChevronRight,
  Check, Download, FolderArchive, AlertTriangle, Users, FileText,
  CheckCircle2, Clock, XCircle, Loader2, Flag, BarChart2, Trash2, Stethoscope,
  Microscope, ClipboardList, Link2,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { Switch } from '@/components/ui/switch'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { Slide, CohortSlide, CohortDetail, CaseGroup, CohortFlag, CohortPatient } from '@/types/slide'
import { PatientTracker } from '@/components/PatientTracker'
import { CohortFromPasteDialog } from '@/components/CohortFromPasteDialog'

import { getApiBase, normalizeAccession, isDemo } from '@/api'
import { displaySlide, displayCase } from '@/lib/display'
import { SortableHeader } from '@/components/SortableHeader'
import { useSlideDetails } from '@/components/SlideDetailsContext'
import { useSortable } from '@/hooks/useSortable'
const SLIDE_FLAG_TAG = 'flagged'

// Above this slide count, the per-slide stain dots are replaced by a compact
// fixed-width proportional stacked bar so long cases don't overflow the row.
const STAIN_DOT_LIMIT = 12

// Visual hierarchy: patient accent colors (left border + header tint)
const PATIENT_COLORS = [
  { border: '#3b82f6', bg: 'rgba(59,130,246,0.06)', text: '#2563eb', badge: 'bg-blue-100 text-blue-700' },
  { border: '#8b5cf6', bg: 'rgba(139,92,246,0.06)', text: '#7c3aed', badge: 'bg-violet-100 text-violet-700' },
  { border: '#06b6d4', bg: 'rgba(6,182,212,0.06)', text: '#0891b2', badge: 'bg-cyan-100 text-cyan-700' },
  { border: '#f59e0b', bg: 'rgba(245,158,11,0.06)', text: '#d97706', badge: 'bg-amber-100 text-amber-700' },
  { border: '#10b981', bg: 'rgba(16,185,129,0.06)', text: '#059669', badge: 'bg-emerald-100 text-emerald-700' },
  { border: '#ec4899', bg: 'rgba(236,72,153,0.06)', text: '#db2777', badge: 'bg-pink-100 text-pink-700' },
  { border: '#f97316', bg: 'rgba(249,115,22,0.06)', text: '#ea580c', badge: 'bg-orange-100 text-orange-700' },
  { border: '#6366f1', bg: 'rgba(99,102,241,0.06)', text: '#4f46e5', badge: 'bg-indigo-100 text-indigo-700' },
]

// Stain type → dot color for at-a-glance case composition
const STAIN_DOT_COLORS: Record<string, string> = {
  'HE': '#ec4899',      // pink
  'IHC': '#3b82f6',     // blue
  'Special': '#f59e0b', // amber
}
function getStainDotColor(stain: string): string {
  if (stain === 'HE') return STAIN_DOT_COLORS['HE']
  if (stain.startsWith('IHC')) return STAIN_DOT_COLORS['IHC']
  if (stain === 'Special') return STAIN_DOT_COLORS['Special']
  return '#94a3b8' // slate fallback
}

interface CohortBuilderProps {
  cohortId: number
  onBack: () => void
}

interface SlideAnalysisEntry {
  status: 'pending' | 'transferring' | 'running' | 'completed' | 'failed'
  job_id: number
  analysis_id?: number
  analysis_name: string
}

function StatusIcon({ status, title }: { status: string; title?: string }) {
  const icon =
    status === 'completed' ? (
      <CheckCircle2 className="h-3.5 w-3.5 text-green-600" />
    ) : status === 'running' || status === 'transferring' ? (
      <Loader2 className="h-3.5 w-3.5 text-blue-500 animate-spin" />
    ) : status === 'pending' ? (
      <Clock className="h-3.5 w-3.5 text-amber-500" />
    ) : status === 'failed' ? (
      <XCircle className="h-3.5 w-3.5 text-red-500" />
    ) : status === 'partial' ? (
      <CheckCircle2 className="h-3.5 w-3.5 text-amber-500" />
    ) : null

  if (!icon) return null

  return <span title={title}>{icon}</span>
}

export function CohortBuilder({ cohortId, onBack }: CohortBuilderProps) {
  // Imperative trigger for the global slide-details dialog — used by per-slide
  // rows below so users can quick-look any slide in the cohort.
  const { openSlideDetails } = useSlideDetails()

  // ── Cohort data ──────────────────────────────────────────────────────
  const [cohort, setCohort] = useState<CohortDetail | null>(null)
  const [cohortLoading, setCohortLoading] = useState(true)

  // ── Tab state ────────────────────────────────────────────────────────
  const [activeTab, setActiveTab] = useState<'cases' | 'patients'>('cases')

  // ── Add Slides drawer ────────────────────────────────────────────────
  const [showAddSlides, setShowAddSlides] = useState(false)
  const [showPasteAdd, setShowPasteAdd] = useState(false)
  const [searchResults, setSearchResults] = useState<Slide[]>([])
  const [searchLoading, setSearchLoading] = useState(false)
  const [searchTerm, setSearchTerm] = useState('')
  const [yearFilter, setYearFilter] = useState<string>('all')
  const [stainFilter, setStainFilter] = useState<string>('all')
  const [tagFilter, setTagFilter] = useState<string>('all')
  // Include external (non-clinical) slides by default so they can be added to
  // cohorts too. exclude = clinical only, only = external only.
  const [externalFilter, setExternalFilter] = useState<'exclude' | 'include' | 'only'>('include')
  const [availableTags, setAvailableTags] = useState<{ id: number; name: string; color?: string; slide_count?: number }[]>([])
  const [resultsTruncated, setResultsTruncated] = useState(false)
  const [selectedHashes, setSelectedHashes] = useState<Set<string>>(new Set())
  const { sorted: sortedSearchResults, sortConfig: cbSearchSortConfig, handleSort: handleCbSearchSort } = useSortable(searchResults)

  // ── Cases tab: multi-select ──────────────────────────────────────────
  const [collapsedCases, setCollapsedCases] = useState<Set<string>>(new Set())
  const [collapsedPatientGroups, setCollapsedPatientGroups] = useState<Set<number>>(new Set())
  const [selectedCaseHashes, setSelectedCaseHashes] = useState<Set<string>>(new Set())
  const lastClickedCase = useRef<string | null>(null)
  // Cases tab opens with every case card collapsed; the user expands the ones
  // they want. Only applied once on first load so adding slides later doesn't
  // re-collapse cards the user has opened.
  const didInitCollapse = useRef(false)

  // ── Cohort-specific flags ────────────────────────────────────────────
  const [cohortFlags, setCohortFlags] = useState<CohortFlag[]>([])
  const [loadingFlags, setLoadingFlags] = useState(false)
  const [flagToolbarMode, setFlagToolbarMode] = useState<'idle' | 'apply' | 'new'>('idle')
  const [flagDropdownValue, setFlagDropdownValue] = useState<string>('')
  const [newFlagName, setNewFlagName] = useState('')
  const [flagApplying, setFlagApplying] = useState(false)

  // ── Per-slide analysis status ────────────────────────────────────────
  const [slideAnalysisStatus, setSlideAnalysisStatus] = useState<
    Record<string, Record<string, SlideAnalysisEntry>>
  >({})

  // ── Patient/surgery assignments ───────────────────────────────────────
  const [cohortPatients, setCohortPatients] = useState<CohortPatient[]>([])
  const [patientsLoading, setPatientsLoading] = useState(false)

  // ── Export dialog ────────────────────────────────────────────────────
  const [isExportDialogOpen, setIsExportDialogOpen] = useState(false)
  const [isExporting, setIsExporting] = useState(false)

  // ── Auto-add result summary (what turning the toggle on pulled in) ─────
  const [autoAddSummary, setAutoAddSummary] = useState<
    { totalSlides: number; cases: { label: string; count: number }[] } | null
  >(null)

  // ── Derived state ────────────────────────────────────────────────────
  const cohortHashSet = useMemo(() => {
    if (!cohort) return new Set<string>()
    return new Set(cohort.slides.map(s => s.slide_hash))
  }, [cohort])

  // Cases explicitly followed by this cohort. When the cohort-wide auto-add is
  // on, every case is followed implicitly, so treat all as followed.
  const followedCaseHashes = useMemo(
    () => new Set(cohort?.followed_case_hashes ?? []),
    [cohort],
  )
  const isCaseFollowed = useCallback(
    (caseHash: string | null | undefined) =>
      !!cohort?.auto_add_cases || (!!caseHash && followedCaseHashes.has(caseHash)),
    [cohort, followedCaseHashes],
  )

  const caseGroups = useMemo((): CaseGroup[] => {
    if (!cohort) return []
    const groupMap = new Map<string, CaseGroup>()
    for (const slide of cohort.slides) {
      const key = slide.case_hash || slide.slide_hash
      if (!groupMap.has(key)) {
        groupMap.set(key, {
          case_hash: slide.case_hash || key,
          accession_number: slide.accession_number,
          year: slide.year,
          slides: [],
        })
      }
      groupMap.get(key)!.slides.push(slide)
    }
    const groups = Array.from(groupMap.values())
    groups.sort((a, b) => (a.accession_number || a.case_hash).localeCompare(b.accession_number || b.case_hash))
    return groups
  }, [cohort])

  const stats = useMemo(() => {
    if (!cohort) return { slides: 0, cases: 0, stains: {} as Record<string, number> }
    const stains: Record<string, number> = {}
    for (const s of cohort.slides) {
      const cat = s.stain_type === 'HE' ? 'HE' : s.stain_type.startsWith('IHC') ? 'IHC' : s.stain_type
      stains[cat] = (stains[cat] || 0) + 1
    }
    return { slides: cohort.slides.length, cases: caseGroups.length, stains }
  }, [cohort, caseGroups])

  const groupedCasesByPatient = useMemo(() => {
    if (!caseGroups.length || !cohortPatients.length) {
      return { patientGroups: [] as {
        patientId: number
        patientLabel: string
        surgeries: {
          surgeryId: number
          surgeryLabel: string
          cases: CaseGroup[]
        }[]
      }[], unassigned: caseGroups }
    }

    const caseToMeta = new Map<string, { patientId: number; patientLabel: string; surgeryId: number; surgeryLabel: string }>()
    for (const patient of cohortPatients) {
      for (const surgery of patient.surgeries) {
        caseToMeta.set(surgery.case_hash, {
          patientId: patient.id,
          patientLabel: patient.label,
          surgeryId: surgery.id,
          surgeryLabel: surgery.surgery_label,
        })
      }
    }

    const unassigned: CaseGroup[] = []
    const patientMap = new Map<number, {
      patientLabel: string
      surgeries: Map<number, { surgeryLabel: string; cases: CaseGroup[] }>
    }>()

    for (const group of caseGroups) {
      const meta = group.case_hash ? caseToMeta.get(group.case_hash) : undefined
      if (!meta) {
        unassigned.push(group)
        continue
      }
      let pEntry = patientMap.get(meta.patientId)
      if (!pEntry) {
        pEntry = { patientLabel: meta.patientLabel, surgeries: new Map() }
        patientMap.set(meta.patientId, pEntry)
      }
      let sEntry = pEntry.surgeries.get(meta.surgeryId)
      if (!sEntry) {
        sEntry = { surgeryLabel: meta.surgeryLabel, cases: [] }
        pEntry.surgeries.set(meta.surgeryId, sEntry)
      }
      sEntry.cases.push(group)
    }

    const patientGroups = Array.from(patientMap.entries()).map(([patientId, p]) => ({
      patientId,
      patientLabel: p.patientLabel,
      surgeries: Array.from(p.surgeries.entries()).map(([surgeryId, s]) => ({
        surgeryId,
        surgeryLabel: s.surgeryLabel,
        cases: s.cases,
      })),
    }))

    // Honor the manual patient order from the Patients tab: cohortPatients
    // arrives sorted by display_order, so sort groups by each patient's index
    // in that list (unknown ids fall to the end).
    const patientOrder = new Map(cohortPatients.map((p, i) => [p.id, i]))
    patientGroups.sort((a, b) =>
      (patientOrder.get(a.patientId) ?? Infinity) - (patientOrder.get(b.patientId) ?? Infinity))
    for (const pg of patientGroups) {
      pg.surgeries.sort((a, b) => a.surgeryLabel.localeCompare(b.surgeryLabel))
    }

    return { patientGroups, unassigned }
  }, [caseGroups, cohortPatients])

  const exportInfo = useMemo(() => {
    if (!cohort) return { totalBytes: 0, knownCount: 0, unknownCount: 0 }
    let totalBytes = 0, knownCount = 0, unknownCount = 0
    for (const s of cohort.slides) {
      if (s.file_size_bytes) { totalBytes += s.file_size_bytes; knownCount++ }
      else unknownCount++
    }
    return { totalBytes, knownCount, unknownCount }
  }, [cohort])

  const addableCount = useMemo(() => {
    let count = 0
    for (const hash of selectedHashes) {
      if (!cohortHashSet.has(hash)) count++
    }
    return count
  }, [selectedHashes, cohortHashSet])

  // ── Derived: overview panel ──────────────────────────────────────────
  const yearDistribution = useMemo((): [number, number][] => {
    const m = new Map<number, number>()
    for (const g of caseGroups) {
      if (g.year) m.set(g.year, (m.get(g.year) || 0) + 1)
    }
    return Array.from(m.entries()).sort(([a], [b]) => b - a)
  }, [caseGroups])

  const analysisHistory = useMemo(() => {
    const jobMap = new Map<number, { analysis_name: string; statuses: string[]; slideCount: number }>()
    for (const slideEntries of Object.values(slideAnalysisStatus)) {
      for (const entry of Object.values(slideEntries)) {
        if (!jobMap.has(entry.job_id)) {
          jobMap.set(entry.job_id, { analysis_name: entry.analysis_name, statuses: [], slideCount: 0 })
        }
        const j = jobMap.get(entry.job_id)!
        j.statuses.push(entry.status)
        j.slideCount++
      }
    }
    return Array.from(jobMap.entries()).map(([job_id, info]) => {
      const completed = info.statuses.filter(s => s === 'completed').length
      const failed = info.statuses.filter(s => s === 'failed').length
      const running = info.statuses.filter(s => s === 'running' || s === 'transferring').length
      const overall = failed > 0 && completed === 0 ? 'failed' : running > 0 ? 'running' : completed === info.slideCount ? 'completed' : 'partial'
      return { job_id, analysis_name: info.analysis_name, completed, failed, running, total: info.slideCount, status: overall }
    })
  }, [slideAnalysisStatus])

  // Per-case aggregate analysis status (for case header badges)
  const getCaseAnalysisStatuses = useCallback((slides: CohortSlide[]) => {
    const analyses: Record<string, { completed: number; total: number; running: number; failed: number }> = {}
    for (const s of slides) {
      const status = slideAnalysisStatus[s.slide_hash]
      if (!status) continue
      for (const [name, entry] of Object.entries(status)) {
        if (!analyses[name]) analyses[name] = { completed: 0, total: 0, running: 0, failed: 0 }
        analyses[name].total++
        if (entry.status === 'completed') analyses[name].completed++
        else if (entry.status === 'running' || entry.status === 'transferring') analyses[name].running++
        else if (entry.status === 'failed') analyses[name].failed++
      }
    }
    return Object.entries(analyses).map(([name, s]) => {
      const status = s.running > 0 ? 'running' : s.completed === s.total ? 'completed' : s.failed > 0 ? 'partial' : 'pending'
      return { name, status, completed: s.completed, total: s.total }
    })
  }, [slideAnalysisStatus])

  // Flags that a case belongs to
  const getCaseFlagNames = useCallback((caseHash: string): string[] => {
    return cohortFlags.filter(f => f.case_hashes.includes(caseHash)).map(f => f.name)
  }, [cohortFlags])

  // ── Data fetching ────────────────────────────────────────────────────
  const fetchCohort = useCallback(async () => {
    setCohortLoading(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}`)
      if (res.ok) setCohort(await res.json())
    } catch (e) {
      console.error('Failed to fetch cohort:', e)
    } finally {
      setCohortLoading(false)
    }
  }, [cohortId])

  const fetchFlags = useCallback(async () => {
    setLoadingFlags(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/flags`)
      if (res.ok) setCohortFlags(await res.json())
    } catch (e) {
      console.error('Failed to fetch flags:', e)
    } finally {
      setLoadingFlags(false)
    }
  }, [cohortId])

  const fetchAnalysisStatus = useCallback(async () => {
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/analysis-status`)
      if (res.ok) {
        const data = await res.json()
        setSlideAnalysisStatus(data.slides || {})
      }
    } catch (e) {
      console.error('Failed to fetch analysis status:', e)
    }
  }, [cohortId])

  const fetchCohortPatients = useCallback(async () => {
    setPatientsLoading(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients`)
      if (res.ok) {
        const data: CohortPatient[] = await res.json()
        setCohortPatients(data)
      }
    } catch (e) {
      console.error('Failed to fetch cohort patients:', e)
    } finally {
      setPatientsLoading(false)
    }
  }, [cohortId])

  // Collapse all case cards on first load (default view = everything closed).
  useEffect(() => {
    if (!didInitCollapse.current && caseGroups.length > 0) {
      setCollapsedCases(new Set(caseGroups.map(g => g.case_hash)))
      didInitCollapse.current = true
    }
  }, [caseGroups])

  useEffect(() => { fetchCohort() }, [fetchCohort])
  useEffect(() => { fetchFlags() }, [fetchFlags])
  useEffect(() => { fetchAnalysisStatus() }, [fetchAnalysisStatus])
  useEffect(() => { fetchCohortPatients() }, [fetchCohortPatients])

  useEffect(() => {
    const fetchTags = async () => {
      try {
        const res = await fetch(`${getApiBase()}/tags`)
        if (res.ok) setAvailableTags(await res.json())
      } catch (e) { console.error(e) }
    }
    fetchTags()
  }, [])

  // ── Flag operations ──────────────────────────────────────────────────
  const applyFlagToSelected = async (flagId: number) => {
    if (selectedCaseHashes.size === 0 || flagApplying) return
    setFlagApplying(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/flags/${flagId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ add_case_hashes: Array.from(selectedCaseHashes), remove_case_hashes: [] }),
      })
      if (res.ok) {
        const updated = await res.json()
        setCohortFlags(prev => prev.map(f => f.id === flagId ? updated : f))
      }
    } catch (e) { console.error(e) }
    finally {
      setFlagApplying(false)
      setFlagToolbarMode('idle')
      setFlagDropdownValue('')
    }
  }

  const createAndApplyFlag = async (name: string) => {
    const trimmed = name.trim()
    if (!trimmed || flagApplying) return
    setFlagApplying(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/flags`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: trimmed, case_hashes: Array.from(selectedCaseHashes) }),
      })
      if (res.ok) {
        const newFlag = await res.json()
        setCohortFlags(prev => [...prev, newFlag])
        setNewFlagName('')
        setFlagToolbarMode('idle')
      }
    } catch (e) { console.error(e) }
    finally { setFlagApplying(false) }
  }

  const removeCaseFromFlag = async (flagId: number, caseHash: string) => {
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/flags/${flagId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ add_case_hashes: [], remove_case_hashes: [caseHash] }),
      })
      if (res.ok) {
        const updated = await res.json()
        setCohortFlags(prev => prev.map(f => f.id === flagId ? updated : f))
      }
    } catch (e) { console.error(e) }
  }

  const deleteFlag = async (flagId: number) => {
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/flags/${flagId}`, { method: 'DELETE' })
      if (res.ok) setCohortFlags(prev => prev.filter(f => f.id !== flagId))
    } catch (e) { console.error(e) }
  }

  // ── Slide-level flagging (via global tag) ──────────────────────────────
  const isSlideFlagged = (slide: CohortSlide) =>
    slide.tags?.includes(SLIDE_FLAG_TAG)

  const updateSlideTagsInState = (slideHash: string, updater: (tags: string[]) => string[]) => {
    setCohort(prev => {
      if (!prev) return prev
      return {
        ...prev,
        slides: prev.slides.map(s =>
          s.slide_hash === slideHash
            ? { ...s, tags: updater(s.tags || []) }
            : s
        ),
      }
    })
  }

  const toggleSlideFlag = async (slide: CohortSlide) => {
    const currentlyFlagged = isSlideFlagged(slide)
    const slideHash = slide.slide_hash

    // Optimistic UI update
    updateSlideTagsInState(slideHash, tags => {
      const set = new Set(tags)
      if (currentlyFlagged) set.delete(SLIDE_FLAG_TAG)
      else set.add(SLIDE_FLAG_TAG)
      return Array.from(set)
    })

    try {
      if (currentlyFlagged) {
        await fetch(`${getApiBase()}/slides/${encodeURIComponent(slideHash)}/tags/${encodeURIComponent(SLIDE_FLAG_TAG)}`, {
          method: 'DELETE',
        })
      } else {
        await fetch(`${getApiBase()}/slides/${encodeURIComponent(slideHash)}/tags`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: SLIDE_FLAG_TAG, color: '#F59E0B' }), // amber
        })
      }
    } catch (e) {
      console.error('Failed to toggle slide flag', e)
      // On error, refresh cohort to resync tags
      fetchCohort()
    }
  }

  // ── Case-level flagging (flag all slides in case) ─────────────────────
  const toggleCaseFlag = async (group: CaseGroup) => {
    const slideHashes = group.slides.map(s => s.slide_hash)
    if (slideHashes.length === 0) return

    const allFlagged = group.slides.every(s => isSlideFlagged(s))

    // Optimistic update
    setCohort(prev => {
      if (!prev) return prev
      return {
        ...prev,
        slides: prev.slides.map(s => {
          if (!slideHashes.includes(s.slide_hash)) return s
          const currentTags = s.tags || []
          const set = new Set(currentTags)
          if (allFlagged) {
            set.delete(SLIDE_FLAG_TAG)
          } else {
            set.add(SLIDE_FLAG_TAG)
          }
          return { ...s, tags: Array.from(set) }
        }),
      }
    })

    try {
      const url = `${getApiBase()}/slides/bulk/tags/${allFlagged ? 'remove' : 'add'}`
      const body: any = {
        slide_hashes: slideHashes,
        tags: [SLIDE_FLAG_TAG],
      }
      if (!allFlagged) {
        body.color = '#F59E0B'
      }
      await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
    } catch (e) {
      console.error('Failed to toggle case flag', e)
      fetchCohort()
    }
  }

  // ── Multi-select helpers ─────────────────────────────────────────────
  const toggleCaseSelect = (caseHash: string, e: React.MouseEvent) => {
    const next = new Set(selectedCaseHashes)
    if (e.shiftKey && lastClickedCase.current) {
      // Range select
      const hashes = caseGroups.map(g => g.case_hash)
      const a = hashes.indexOf(lastClickedCase.current)
      const b = hashes.indexOf(caseHash)
      const [lo, hi] = a < b ? [a, b] : [b, a]
      for (let i = lo; i <= hi; i++) next.add(hashes[i])
    } else {
      if (next.has(caseHash)) next.delete(caseHash)
      else next.add(caseHash)
    }
    lastClickedCase.current = caseHash
    setSelectedCaseHashes(next)
  }

  const toggleSelectAll = () => {
    if (selectedCaseHashes.size === caseGroups.length) setSelectedCaseHashes(new Set())
    else setSelectedCaseHashes(new Set(caseGroups.map(g => g.case_hash)))
  }

  // ── Search ───────────────────────────────────────────────────────────
  const handleSearch = async () => {
    setSearchLoading(true)
    setSelectedHashes(new Set())
    try {
      const params = new URLSearchParams()
      if (searchTerm.trim()) params.append('q', normalizeAccession(searchTerm))
      if (yearFilter !== 'all') params.append('year', yearFilter)
      if (stainFilter !== 'all') params.append('stain', stainFilter)
      if (tagFilter !== 'all') params.append('tag', tagFilter)
      params.append('external', externalFilter)
      const res = await fetch(`${getApiBase()}/search?${params}`)
      if (res.ok) {
        const data = await res.json()
        setSearchResults(data.results)
        setResultsTruncated(data.truncated || false)
      }
    } catch (e) { console.error(e) }
    finally { setSearchLoading(false) }
  }

  // ── Slide mutations ──────────────────────────────────────────────────
  const searchSlideToCohortSlide = (s: Slide): CohortSlide => ({
    slide_hash: s.slide_hash,
    accession_number: s.accession_number || null,
    block_id: s.block_id,
    slide_number: s.slide_number || null,
    stain_type: s.stain_type,
    random_id: s.random_id,
    year: s.year || null,
    case_hash: s.case_hash || null,
    tags: s.slide_tags || [],
    file_size_bytes: s.file_size_bytes,
  })

  const addSlides = async (hashes: string[]) => {
    if (hashes.length === 0 || !cohort) return
    const newSlides: CohortSlide[] = []
    for (const hash of hashes) {
      if (cohortHashSet.has(hash)) continue
      const s = searchResults.find(r => r.slide_hash === hash)
      if (s) newSlides.push(searchSlideToCohortSlide(s))
    }
    const prev = cohort
    if (newSlides.length > 0) setCohort({ ...cohort, slides: [...cohort.slides, ...newSlides] })
    setSelectedHashes(new Set())
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/slides`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: hashes }),
      })
      if (res.ok) {
        const data = await res.json()
        if (data.not_found?.length > 0) {
          const nf = new Set(data.not_found as string[])
          setCohort(p => p ? { ...p, slides: p.slides.filter(s => !nf.has(s.slide_hash)), slide_count: data.total_slides, case_count: data.total_cases } : p)
        } else {
          setCohort(p => p ? { ...p, slide_count: data.total_slides, case_count: data.total_cases } : p)
        }
      } else setCohort(prev)
    } catch { setCohort(prev) }
  }

  // Toggle the "follow cases" setting. Turning it on back-fills existing
  // sibling slides server-side; we refetch to show them and report exactly which
  // cases/slides were pulled in (diffed against the pre-toggle slide set).
  const toggleAutoAddCases = async (next: boolean) => {
    if (!cohort) return
    const beforeHashes = new Set(cohort.slides.map(s => s.slide_hash))
    setCohort({ ...cohort, auto_add_cases: next })  // optimistic
    if (!next) setAutoAddSummary(null)              // clear summary when turning off
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ auto_add_cases: next }),
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      if (next && data.slides_added > 0) {
        // Refetch the full cohort, then diff to find the newly-added slides.
        const freshRes = await fetch(`${getApiBase()}/cohorts/${cohortId}`)
        if (freshRes.ok) {
          const fresh: CohortDetail = await freshRes.json()
          setCohort(fresh)
          const added = fresh.slides.filter(s => !beforeHashes.has(s.slide_hash))
          // Group by case, keeping a redaction-safe display label (demo → case ID).
          const byCase = new Map<string, { label: string; count: number }>()
          for (const s of added) {
            const key = s.case_hash || s.slide_hash
            const label = displayCase({ accession_number: s.accession_number, case_hash: s.case_hash })
            const entry = byCase.get(key)
            if (entry) entry.count += 1
            else byCase.set(key, { label, count: 1 })
          }
          setAutoAddSummary({
            totalSlides: added.length,
            cases: Array.from(byCase.values()).sort((a, b) => b.count - a.count),
          })
        }
        fetchAnalysisStatus()
      } else if (next) {
        setAutoAddSummary({ totalSlides: 0, cases: [] })
      }
    } catch (e) {
      console.error('Failed to update auto-add setting:', e)
      setCohort(c => c ? { ...c, auto_add_cases: !next } : c)  // revert
      setAutoAddSummary(null)
    }
  }

  // Follow/unfollow one or more whole cases. Following links each case: existing
  // slides back-fill server-side (we refetch to show them) and future onboarded
  // slides auto-join. Unfollowing only stops future auto-add; present slides stay.
  const followCases = async (caseHashes: string[], next: boolean) => {
    if (!cohort) return
    const hashes = caseHashes.filter(Boolean)
    if (hashes.length === 0) return
    const prevFollowed = cohort.followed_case_hashes ?? []
    const optimistic = next
      ? Array.from(new Set([...prevFollowed, ...hashes]))
      : prevFollowed.filter(h => !hashes.includes(h))
    setCohort({ ...cohort, followed_case_hashes: optimistic })
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/followed-cases`, {
        method: next ? 'POST' : 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ case_hashes: hashes }),
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      // Sync followed set from server; pull in back-filled slides if any.
      setCohort(c => c ? { ...c, followed_case_hashes: data.followed_case_hashes } : c)
      if (next && data.slides_added > 0) {
        fetchCohort()
        fetchAnalysisStatus()
      }
    } catch (e) {
      console.error('Failed to update case-follow setting:', e)
      setCohort(c => c ? { ...c, followed_case_hashes: prevFollowed } : c)  // revert
    }
  }

  const toggleFollowCase = (caseHash: string, next: boolean) => followCases([caseHash], next)

  const removeSlide = async (slideHash: string) => {
    if (!cohort) return
    const prev = cohort
    setCohort({ ...cohort, slides: cohort.slides.filter(s => s.slide_hash !== slideHash) })
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/slides`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: [slideHash] }),
      })
      if (res.ok) {
        const data = await res.json()
        setCohort(p => p ? { ...p, slide_count: data.total_slides, case_count: data.total_cases } : p)
      } else {
        setCohort(prev)
        const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }))
        console.error('Remove slide failed:', err)
      }
    } catch (e) { setCohort(prev); console.error('Remove slide error:', e) }
  }

  const removeCase = async (caseSlides: CohortSlide[]) => {
    if (!cohort || caseSlides.length === 0) return
    // If this case is explicitly followed, unfollow first so its slides don't
    // get re-added on the next index. (Cohort-wide auto-add is left untouched.)
    const caseHash = caseSlides[0].case_hash
    if (caseHash && followedCaseHashes.has(caseHash)) {
      await toggleFollowCase(caseHash, false)
    }
    const hashes = caseSlides.map(s => s.slide_hash)
    const hashSet = new Set(hashes)
    const prev = cohort
    setCohort({ ...cohort, slides: cohort.slides.filter(s => !hashSet.has(s.slide_hash)) })
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/slides`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: hashes }),
      })
      if (res.ok) {
        const data = await res.json()
        setCohort(p => p ? { ...p, slide_count: data.total_slides, case_count: data.total_cases } : p)
      } else {
        setCohort(prev)
        const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }))
        console.error('Remove case failed:', err)
      }
    } catch (e) { setCohort(prev); console.error('Remove case error:', e) }
  }

  // ── Toggle helpers ───────────────────────────────────────────────────
  const toggleCaseCollapse = (caseHash: string) => {
    const next = new Set(collapsedCases)
    if (next.has(caseHash)) next.delete(caseHash)
    else next.add(caseHash)
    setCollapsedCases(next)
  }

  const toggleSlideSelection = (hash: string) => {
    const next = new Set(selectedHashes)
    if (next.has(hash)) next.delete(hash)
    else next.add(hash)
    setSelectedHashes(next)
  }

  const toggleSelectAllSearch = () => {
    if (selectedHashes.size === searchResults.length) setSelectedHashes(new Set())
    else setSelectedHashes(new Set(searchResults.map(s => s.slide_hash)))
  }

  // ── Helpers ──────────────────────────────────────────────────────────
  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B'
    const units = ['B', 'KB', 'MB', 'GB', 'TB']
    const i = Math.floor(Math.log(bytes) / Math.log(1024))
    return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`
  }

  const handleExport = () => {
    setIsExporting(true)
    window.location.href = `${getApiBase()}/cohorts/${cohortId}/export`
    setTimeout(() => { setIsExporting(false); setIsExportDialogOpen(false) }, 1500)
  }

  const years = ['2024', '2023', '2022', '2021', '2020']
  const stainTypes = ['HE', 'IHC', 'Special']

  // ── Guards ───────────────────────────────────────────────────────────
  if (cohortLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <p className="text-muted-foreground">Loading cohort...</p>
      </div>
    )
  }

  if (!cohort) {
    return (
      <div className="space-y-4">
        <Button variant="ghost" onClick={onBack}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Cohorts
        </Button>
        <p className="text-muted-foreground">Cohort not found.</p>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-[calc(100vh-10rem)]">

      {/* ── Header ── */}
      <div className="flex items-start gap-3 pb-3 border-b border-gray-300 shrink-0">
        <Button variant="ghost" size="sm" onClick={onBack} className="mt-0.5 shrink-0">
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex-1 min-w-0">
          <h1 className="text-xl font-semibold truncate">{cohort.name}</h1>
          <p className="text-sm text-muted-foreground mt-0.5">
            <span className="font-medium text-foreground">{stats.slides}</span> slides
            {' · '}
            <span className="font-medium text-foreground">{stats.cases}</span> cases
            {Object.entries(stats.stains).map(([s, n]) => (
              <span key={s}> · {s}: {n}</span>
            ))}
          </p>
          <div className="mt-1.5">
            <div
              className="inline-flex items-center gap-2 text-xs text-muted-foreground"
              title="When on, ALL cases in this cohort are followed: any newly-onboarded slide whose case is already here is added automatically, and existing siblings are pulled in now. To follow only specific cases instead, leave this off and use the link icon on each case."
            >
              <Switch
                checked={!!cohort.auto_add_cases}
                onCheckedChange={(c) => toggleAutoAddCases(c)}
                aria-label="Auto-add new slides for all cases"
              />
              <span
                className="cursor-pointer select-none"
                onClick={() => toggleAutoAddCases(!cohort.auto_add_cases)}
              >
                Auto-add new slides for all cases
              </span>
              <Link2 className="h-3 w-3 opacity-60" />
            </div>

            {autoAddSummary && (
              <div className="mt-1.5 flex items-start gap-2 rounded-md border border-blue-200 bg-blue-50/70 px-2.5 py-1.5 text-xs text-blue-900 max-w-xl">
                <Link2 className="h-3.5 w-3.5 mt-0.5 shrink-0 text-blue-500" />
                <div className="min-w-0">
                  {autoAddSummary.totalSlides > 0 ? (
                    <>
                      <span className="font-medium">
                        Pulled in {autoAddSummary.totalSlides} slide{autoAddSummary.totalSlides !== 1 ? 's' : ''} across {autoAddSummary.cases.length} case{autoAddSummary.cases.length !== 1 ? 's' : ''}
                      </span>
                      <div className="mt-1 flex flex-wrap gap-1">
                        {autoAddSummary.cases.map(c => (
                          <span
                            key={c.label}
                            className="inline-flex items-center gap-1 rounded bg-white/70 border border-blue-200 px-1.5 py-0.5"
                          >
                            <span className="font-mono">{c.label}</span>
                            <span className="text-blue-500">+{c.count}</span>
                          </span>
                        ))}
                      </div>
                    </>
                  ) : (
                    <span>Auto-add on — no additional slides to pull in right now.</span>
                  )}
                </div>
                <button
                  className="ml-auto shrink-0 text-blue-400 hover:text-blue-700"
                  onClick={() => setAutoAddSummary(null)}
                  title="Dismiss"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0 mt-0.5">
          <Button
            variant={showAddSlides ? 'secondary' : 'outline'}
            size="sm"
            onClick={() => setShowAddSlides(!showAddSlides)}
          >
            <Plus className="mr-1.5 h-4 w-4" />
            Add Slides
          </Button>
          {stats.slides > 0 && (
            <>
              <a
                href={`${getApiBase()}/cohorts/${cohortId}/export.csv`}
                download
                className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-input bg-background hover:bg-accent hover:text-accent-foreground transition-colors"
                title="Export metadata as CSV"
              >
                <FileText className="h-4 w-4" />
                CSV
              </a>
              <Button variant="outline" size="sm" onClick={() => setIsExportDialogOpen(true)}>
                <Download className="mr-1.5 h-4 w-4" />
                Export ZIP
              </Button>
            </>
          )}
        </div>
      </div>

      {/* ── Tab bar ── */}
      <div className="flex items-center border-b border-gray-300 shrink-0">
        {(
          [
            { key: 'cases', label: 'Cases', icon: <FileText className="h-3.5 w-3.5" />, count: stats.cases },
            { key: 'patients', label: 'Patients', icon: <Users className="h-3.5 w-3.5" />, count: null },
          ] as const
        ).map(({ key, label, icon, count }) => (
          <button
            key={key}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-sm border-b-[3px] rounded-t-sm transition-colors ${
              activeTab === key
                ? 'font-semibold bg-selected-bg text-selected-foreground'
                : 'font-medium border-transparent text-muted-foreground hover:text-foreground'
            }`}
            style={activeTab === key ? { borderBottomColor: 'var(--selected-accent)' } : undefined}
            onClick={() => setActiveTab(key)}
          >
            {icon}
            {label}
            {count !== null && (
              <span className="text-xs text-muted-foreground font-normal">({count})</span>
            )}
          </button>
        ))}
      </div>

      {/* ── Body ── */}
      <div className="flex flex-1 min-h-0">

        {/* Left: Cases / Patients */}
        <div className={`flex flex-col min-w-0 border-r border-gray-300 ${showAddSlides ? 'w-[52%]' : 'w-[58%]'}`}>

          {/* ── Cases tab ── */}
          {activeTab === 'cases' && (
            <div className="flex-1 overflow-hidden flex flex-col">
              {caseGroups.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-full gap-3 text-center p-8">
                  <FileText className="h-10 w-10 text-muted-foreground/40" />
                  <div>
                    <p className="text-sm font-medium">No slides in this cohort</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Click "Add Slides" above to search and add slides.
                    </p>
                  </div>
                </div>
              ) : (
                <>
                  {/* Select-all + bulk flag toolbar */}
                  <div className="flex items-center gap-2 px-3 py-2 border-b border-gray-300 bg-muted/20 shrink-0">
                    <Checkbox
                      checked={selectedCaseHashes.size === caseGroups.length && caseGroups.length > 0}
                      onCheckedChange={toggleSelectAll}
                      className="shrink-0"
                    />
                    {selectedCaseHashes.size === 0 ? (
                      <span className="text-xs text-muted-foreground">
                        {caseGroups.length} case{caseGroups.length !== 1 ? 's' : ''}
                        {' · shift+click to range select'}
                      </span>
                    ) : (
                      <>
                        <span className="text-xs font-medium">{selectedCaseHashes.size} selected</span>

                        {flagToolbarMode === 'idle' && (
                          <>
                            {!cohort.auto_add_cases && (() => {
                              const selected = Array.from(selectedCaseHashes)
                              const allFollowed = selected.length > 0 &&
                                selected.every(h => followedCaseHashes.has(h))
                              return (
                                <Button
                                  size="sm"
                                  variant="outline"
                                  className="h-6 px-2 text-xs"
                                  onClick={() => followCases(selected, !allFollowed)}
                                  title={allFollowed
                                    ? 'Stop auto-adding future slides for the selected cases'
                                    : 'Follow the selected cases — back-fill their slides now and auto-add future ones'}
                                >
                                  <Link2 className="h-3 w-3 mr-1" />
                                  {allFollowed ? 'Unfollow' : 'Follow'}
                                </Button>
                              )
                            })()}
                            {cohortFlags.length > 0 && (
                              <Button
                                size="sm"
                                variant="outline"
                                className="h-6 px-2 text-xs"
                                onClick={() => setFlagToolbarMode('apply')}
                              >
                                <Flag className="h-3 w-3 mr-1" />
                                Add to flag
                              </Button>
                            )}
                            <Button
                              size="sm"
                              variant="outline"
                              className="h-6 px-2 text-xs"
                              onClick={() => setFlagToolbarMode('new')}
                            >
                              <Plus className="h-3 w-3 mr-1" />
                              New flag
                            </Button>
                          </>
                        )}

                        {flagToolbarMode === 'apply' && (
                          <div className="flex items-center gap-1.5">
                            <Select value={flagDropdownValue} onValueChange={(val) => {
                              setFlagDropdownValue(val)
                              applyFlagToSelected(parseInt(val))
                            }}>
                              <SelectTrigger className="h-6 text-xs w-44">
                                <SelectValue placeholder="Choose flag…" />
                              </SelectTrigger>
                              <SelectContent>
                                {cohortFlags.map(f => (
                                  <SelectItem key={f.id} value={String(f.id)}>
                                    <div className="flex items-center gap-1.5">
                                      <Flag className="h-3 w-3 text-amber-500" />
                                      {f.name}
                                      <span className="text-muted-foreground text-xs">({f.case_hashes.length})</span>
                                    </div>
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                            {flagApplying && <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />}
                            <button className="text-muted-foreground hover:text-foreground" onClick={() => setFlagToolbarMode('idle')}>
                              <X className="h-3.5 w-3.5" />
                            </button>
                          </div>
                        )}

                        {flagToolbarMode === 'new' && (
                          <div className="flex items-center gap-1.5">
                            <Input
                              autoFocus
                              placeholder="Flag name…"
                              value={newFlagName}
                              onChange={e => setNewFlagName(e.target.value)}
                              onKeyDown={e => e.key === 'Enter' && createAndApplyFlag(newFlagName)}
                              className="h-6 text-xs w-44"
                            />
                            <Button
                              size="sm"
                              className="h-6 px-2 text-xs"
                              onClick={() => createAndApplyFlag(newFlagName)}
                              disabled={!newFlagName.trim() || flagApplying}
                            >
                              {flagApplying ? <Loader2 className="h-3 w-3 animate-spin" /> : 'Create'}
                            </Button>
                            <button className="text-muted-foreground hover:text-foreground" onClick={() => setFlagToolbarMode('idle')}>
                              <X className="h-3.5 w-3.5" />
                            </button>
                          </div>
                        )}

                        <button
                          className="text-muted-foreground hover:text-foreground ml-auto"
                          onClick={() => setSelectedCaseHashes(new Set())}
                        >
                          <X className="h-3.5 w-3.5" />
                        </button>
                      </>
                    )}
                  </div>

                  {/* Case list */}
                  <div className="flex-1 overflow-auto">
                    <div className="p-2 space-y-1">
                      {(() => {
                        const { patientGroups, unassigned } = groupedCasesByPatient

                        // ── Case card: compact inline card with stain dots ──
                        const renderCaseCard = (group: CaseGroup, indent: boolean = false) => {
                          const isExpanded = !collapsedCases.has(group.case_hash)
                          const caseStatuses = getCaseAnalysisStatuses(group.slides)
                          const caseFlagNames = getCaseFlagNames(group.case_hash)
                          const isSelected = selectedCaseHashes.has(group.case_hash)
                          const allSlidesFlagged = group.slides.length > 0 && group.slides.every(s => isSlideFlagged(s))
                          const anySlidesFlagged = group.slides.some(s => isSlideFlagged(s))
                          const followed = isCaseFollowed(group.case_hash)
                          // Cohort-wide auto-add makes every case followed implicitly; the
                          // per-case toggle is disabled in that mode (nothing to opt out of).
                          const followLockedByCohort = !!cohort.auto_add_cases

                          // Group slides by stain for the dot / bar summary
                          const stainCounts: Record<string, number> = {}
                          for (const s of group.slides) {
                            const key = s.stain_type === 'HE' ? 'HE' : s.stain_type.startsWith('IHC') ? 'IHC' : s.stain_type
                            stainCounts[key] = (stainCounts[key] || 0) + 1
                          }
                          // Stable category order for the stacked bar (HE, IHC, Special, then rest).
                          const stainRank = (k: string) => (k === 'HE' ? 0 : k === 'IHC' ? 1 : k === 'Special' ? 2 : 3)
                          const orderedStains = Object.entries(stainCounts)
                            .sort((a, b) => stainRank(a[0]) - stainRank(b[0]) || a[0].localeCompare(b[0]))
                          const stainTitle = orderedStains.map(([s, n]) => `${s}: ${n}`).join(', ')
                          const useStainBar = group.slides.length > STAIN_DOT_LIMIT

                          return (
                            <div
                              key={group.case_hash}
                              className={`rounded-lg border transition-all ${
                                isSelected
                                  ? 'border-blue-300 bg-blue-50/50 shadow-sm'
                                  : 'border-gray-200 bg-white hover:border-gray-300 hover:shadow-sm'
                              } ${indent ? 'ml-4' : ''}`}
                            >
                              {/* Case header */}
                              <div className="flex items-center gap-2 px-3 py-2 group/case">
                                {/* Checkbox */}
                                <div
                                  className="shrink-0 cursor-pointer"
                                  onClick={e => { e.stopPropagation(); toggleCaseSelect(group.case_hash, e) }}
                                >
                                  <Checkbox
                                    checked={isSelected}
                                    onCheckedChange={() => {}}
                                    className="pointer-events-none"
                                  />
                                </div>

                                {/* Expand toggle + accession */}
                                <button
                                  className="flex items-center gap-2 min-w-0 flex-1 text-left"
                                  onClick={() => toggleCaseCollapse(group.case_hash)}
                                >
                                  <Microscope className="h-3.5 w-3.5 text-muted-foreground/60 shrink-0" />
                                  <span className="text-sm font-semibold truncate">
                                    {displayCase(group)}
                                  </span>
                                  {group.year && (
                                    <span className="text-[11px] text-muted-foreground bg-muted/60 rounded px-1.5 py-0.5 shrink-0">
                                      {group.year}
                                    </span>
                                  )}
                                </button>

                                {/* Stain composition — dots for small cases, a compact
                                    proportional stacked bar for long ones (fixed width). */}
                                {useStainBar ? (
                                  <div
                                    className="flex h-2.5 w-24 rounded-full overflow-hidden shrink-0 ring-1 ring-black/5"
                                    title={stainTitle}
                                  >
                                    {orderedStains.map(([cat, n]) => (
                                      <div
                                        key={cat}
                                        style={{ width: `${(n / group.slides.length) * 100}%`, backgroundColor: getStainDotColor(cat) }}
                                        title={`${cat}: ${n}`}
                                      />
                                    ))}
                                  </div>
                                ) : (
                                  <div className="flex items-center gap-1 shrink-0" title={stainTitle}>
                                    {group.slides.map((s, i) => (
                                      <span
                                        key={i}
                                        className="inline-block h-2 w-2 rounded-full shrink-0"
                                        style={{ backgroundColor: getStainDotColor(s.stain_type) }}
                                        title={`${s.block_id} · ${s.stain_type}`}
                                      />
                                    ))}
                                  </div>
                                )}

                                {/* Slide count */}
                                <span className="text-[11px] text-muted-foreground shrink-0 tabular-nums">
                                  {group.slides.length}
                                </span>

                                {/* Analysis status indicators */}
                                {caseStatuses.length > 0 && (
                                  <div className="flex items-center gap-1 shrink-0">
                                    {caseStatuses.map(cs => (
                                      <StatusIcon key={cs.name} status={cs.status} title={`${cs.name}: ${cs.completed}/${cs.total}`} />
                                    ))}
                                  </div>
                                )}

                                {/* Follow-case toggle — link the whole case so future
                                    onboarded slides auto-join the cohort */}
                                <button
                                  className={`shrink-0 inline-flex items-center justify-center h-6 w-6 rounded-full transition-colors ${
                                    followed
                                      ? 'text-blue-600'
                                      : 'text-muted-foreground/40 hover:text-blue-500'
                                  } ${followLockedByCohort ? 'cursor-default' : ''}`}
                                  onClick={e => {
                                    e.stopPropagation()
                                    if (followLockedByCohort) return
                                    toggleFollowCase(group.case_hash, !followed)
                                  }}
                                  title={
                                    followLockedByCohort
                                      ? 'All cases are followed (cohort-wide auto-add is on)'
                                      : followed
                                        ? 'Following — future slides for this case auto-join. Click to unfollow.'
                                        : 'Follow case — auto-add future slides for this case'
                                  }
                                >
                                  <Link2 className={`h-3 w-3 ${followed ? 'stroke-[2.5]' : ''}`} />
                                </button>

                                {/* Flag button */}
                                <button
                                  className={`shrink-0 inline-flex items-center justify-center h-6 w-6 rounded-full transition-colors ${
                                    allSlidesFlagged
                                      ? 'text-red-500'
                                      : anySlidesFlagged
                                        ? 'text-red-400'
                                        : 'text-muted-foreground/40 hover:text-red-400'
                                  }`}
                                  onClick={e => { e.stopPropagation(); toggleCaseFlag(group) }}
                                  title={allSlidesFlagged ? 'Unflag all slides' : 'Flag all slides'}
                                >
                                  <Flag className={`h-3 w-3 ${allSlidesFlagged || anySlidesFlagged ? 'fill-current' : ''}`} />
                                </button>

                                {/* Cohort flag chips */}
                                {caseFlagNames.length > 0 && (
                                  <div className="flex items-center gap-1 shrink-0">
                                    {caseFlagNames.slice(0, 2).map(name => {
                                      const flag = cohortFlags.find(f => f.name === name)!
                                      return (
                                        <span
                                          key={name}
                                          className="inline-flex items-center gap-0.5 bg-amber-50 text-amber-700 text-[10px] rounded-full pl-1.5 pr-1 py-0.5 border border-amber-200"
                                        >
                                          <Flag className="h-2 w-2" />
                                          {name}
                                          <button
                                            className="hover:text-destructive ml-0.5 opacity-0 group-hover/case:opacity-100"
                                            onClick={e => { e.stopPropagation(); removeCaseFromFlag(flag.id, group.case_hash) }}
                                          >
                                            <X className="h-2.5 w-2.5" />
                                          </button>
                                        </span>
                                      )
                                    })}
                                    {caseFlagNames.length > 2 && (
                                      <span className="text-[10px] text-muted-foreground">+{caseFlagNames.length - 2}</span>
                                    )}
                                  </div>
                                )}

                                {/* Expand indicator */}
                                <button
                                  className="shrink-0 text-muted-foreground/40 hover:text-muted-foreground"
                                  onClick={() => toggleCaseCollapse(group.case_hash)}
                                >
                                  {isExpanded
                                    ? <ChevronDown className="h-3.5 w-3.5" />
                                    : <ChevronRight className="h-3.5 w-3.5" />
                                  }
                                </button>

                                {/* Remove */}
                                <button
                                  className="shrink-0 opacity-0 group-hover/case:opacity-100 text-muted-foreground hover:text-destructive transition-opacity"
                                  onClick={() => removeCase(group.slides)}
                                  title="Remove case from cohort"
                                >
                                  <X className="h-3.5 w-3.5" />
                                </button>
                              </div>

                              {/* Expanded slide detail */}
                              {isExpanded && (
                                <div className="border-t border-gray-100 bg-gray-50/50 rounded-b-lg">
                                  {group.slides.map((slide) => {
                                    const slideStatus = slideAnalysisStatus[slide.slide_hash]
                                    const entries = slideStatus ? Object.values(slideStatus) : []
                                    const flagged = isSlideFlagged(slide)
                                    return (
                                      <div
                                        key={slide.slide_hash}
                                        className="flex items-center gap-2.5 px-3 py-1.5 group/slide hover:bg-gray-100/60 transition-colors first:pt-2 last:pb-2 cursor-pointer"
                                        onClick={() => openSlideDetails({
                                          // CohortSlide shape → Slide-compatible seed. Full data
                                          // streams in from /slides/{hash} once the dialog opens.
                                          slide_hash: slide.slide_hash,
                                          accession_number: slide.accession_number || '',
                                          block_id: slide.block_id,
                                          slide_number: slide.slide_number || '',
                                          stain_type: slide.stain_type,
                                          year: slide.year ?? undefined,
                                          random_id: slide.random_id,
                                          case_hash: slide.case_hash ?? undefined,
                                          slide_tags: slide.tags,
                                          file_size_bytes: slide.file_size_bytes,
                                        })}
                                        title="View slide details"
                                      >
                                        {/* Stain dot */}
                                        <span
                                          className="inline-block h-2.5 w-2.5 rounded-full shrink-0 ring-1 ring-black/5"
                                          style={{ backgroundColor: getStainDotColor(slide.stain_type) }}
                                        />
                                        {/* Block ID */}
                                        <span className="text-xs font-mono text-muted-foreground w-16 truncate">{slide.block_id}</span>
                                        {/* Stain badge */}
                                        <Badge variant="outline" className="text-[10px] h-5 px-1.5 font-medium">{slide.stain_type}</Badge>
                                        {/* Analysis badges */}
                                        {entries.length > 0 && (
                                          <div className="flex items-center gap-1 flex-wrap">
                                            {entries.map(entry => (
                                              <span
                                                key={entry.analysis_name}
                                                className={`inline-flex items-center gap-0.5 rounded-full border text-[10px] h-5 px-1.5 ${
                                                  entry.status === 'completed'
                                                    ? 'bg-green-50 border-green-200 text-green-700'
                                                    : entry.status === 'failed'
                                                      ? 'bg-red-50 border-red-200 text-red-600'
                                                      : entry.status === 'running' || entry.status === 'transferring'
                                                        ? 'bg-yellow-50 border-yellow-200 text-yellow-700'
                                                        : 'bg-muted/60 border-border text-muted-foreground'
                                                }`}
                                                title={`${entry.analysis_name}: ${entry.status}`}
                                              >
                                                <StatusIcon status={entry.status} />
                                                {entry.analysis_name.length > 12 ? entry.analysis_name.slice(0, 12) + '…' : entry.analysis_name}
                                              </span>
                                            ))}
                                          </div>
                                        )}
                                        <div className="flex-1" />
                                        {/* Slide flag */}
                                        <button
                                          className={`shrink-0 inline-flex items-center justify-center h-5 w-5 rounded-full transition-colors ${
                                            flagged ? 'text-red-500' : 'text-muted-foreground/30 opacity-0 group-hover/slide:opacity-100 hover:text-red-400'
                                          }`}
                                          onClick={(e) => { e.stopPropagation(); toggleSlideFlag(slide) }}
                                          title={flagged ? 'Unflag slide' : 'Flag slide'}
                                        >
                                          <Flag className={`h-2.5 w-2.5 ${flagged ? 'fill-current' : ''}`} />
                                        </button>
                                        {/* Remove slide */}
                                        <button
                                          className="shrink-0 opacity-0 group-hover/slide:opacity-100 text-muted-foreground hover:text-destructive transition-opacity"
                                          onClick={(e) => { e.stopPropagation(); removeSlide(slide.slide_hash) }}
                                          title="Remove slide"
                                        >
                                          <X className="h-3 w-3" />
                                        </button>
                                      </div>
                                    )
                                  })}
                                </div>
                              )}
                            </div>
                          )
                        }

                        // ── No patients assigned: flat case list ──
                        if (groupedCasesByPatient.patientGroups.length === 0) {
                          return <div className="space-y-1">{caseGroups.map(g => renderCaseCard(g))}</div>
                        }

                        // ── Patient → Surgery → Case hierarchy ──
                        return (
                          <div className="space-y-3">
                            {patientGroups.map((pg, pgIndex) => {
                              const color = PATIENT_COLORS[pgIndex % PATIENT_COLORS.length]
                              const isPatientCollapsed = collapsedPatientGroups.has(pg.patientId)
                              const totalSlides = pg.surgeries.reduce((n, sg) => n + sg.cases.reduce((m, c) => m + c.slides.length, 0), 0)
                              const totalCases = pg.surgeries.reduce((n, sg) => n + sg.cases.length, 0)

                              return (
                                <div
                                  key={`patient-${pg.patientId}`}
                                  className="rounded-xl overflow-hidden border border-gray-200"
                                  style={{ borderLeftWidth: '4px', borderLeftColor: color.border }}
                                >
                                  {/* Patient header */}
                                  <button
                                    className="w-full flex items-center gap-3 px-4 py-3 transition-colors cursor-pointer select-none"
                                    style={{ backgroundColor: color.bg }}
                                    onClick={() =>
                                      setCollapsedPatientGroups(prev => {
                                        const next = new Set(prev)
                                        if (next.has(pg.patientId)) next.delete(pg.patientId)
                                        else next.add(pg.patientId)
                                        return next
                                      })
                                    }
                                  >
                                    {/* Patient avatar */}
                                    <div
                                      className="h-8 w-8 rounded-full flex items-center justify-center text-white text-xs font-bold shrink-0"
                                      style={{ backgroundColor: color.border }}
                                    >
                                      {pg.patientLabel.slice(0, 2).toUpperCase()}
                                    </div>
                                    <div className="flex-1 text-left min-w-0">
                                      <div className="text-sm font-semibold" style={{ color: color.text }}>
                                        {pg.patientLabel}
                                      </div>
                                      <div className="text-[11px] text-muted-foreground">
                                        {pg.surgeries.length} {pg.surgeries.length === 1 ? 'surgery' : 'surgeries'} · {totalCases} {totalCases === 1 ? 'case' : 'cases'} · {totalSlides} slides
                                      </div>
                                    </div>
                                    {isPatientCollapsed
                                      ? <ChevronRight className="h-4 w-4 text-muted-foreground shrink-0" />
                                      : <ChevronDown className="h-4 w-4 text-muted-foreground shrink-0" />
                                    }
                                  </button>

                                  {/* Surgery sections — always visible when patient expanded */}
                                  {!isPatientCollapsed && (
                                    <div className="bg-white">
                                      {pg.surgeries.map(sg => (
                                        <div key={`surgery-${sg.surgeryId}`}>
                                          {/* Surgery divider */}
                                          <div className="flex items-center gap-2 px-4 py-1.5 bg-gray-50 border-t border-gray-100">
                                            <Stethoscope className="h-3 w-3 text-muted-foreground/60" />
                                            <span
                                              className={`inline-flex items-center text-[11px] font-semibold rounded-md px-2 py-0.5 ${color.badge}`}
                                            >
                                              {sg.surgeryLabel}
                                            </span>
                                            <span className="text-[11px] text-muted-foreground">
                                              {sg.cases.length} {sg.cases.length === 1 ? 'case' : 'cases'} · {sg.cases.reduce((n, c) => n + c.slides.length, 0)} slides
                                            </span>
                                          </div>
                                          {/* Cases within this surgery */}
                                          <div className="p-2 space-y-1">
                                            {sg.cases.map(cg => renderCaseCard(cg))}
                                          </div>
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              )
                            })}

                            {/* Unassigned cases */}
                            {unassigned.length > 0 && (
                              <div className="rounded-xl overflow-hidden border border-dashed border-gray-300">
                                <div className="flex items-center gap-2 px-4 py-2.5 bg-gray-50/50">
                                  <AlertTriangle className="h-3.5 w-3.5 text-muted-foreground/50" />
                                  <span className="text-xs font-medium text-muted-foreground">
                                    Unassigned
                                  </span>
                                  <span className="text-[11px] text-muted-foreground">
                                    {unassigned.length} {unassigned.length === 1 ? 'case' : 'cases'}
                                  </span>
                                </div>
                                <div className="p-2 space-y-1 bg-white">
                                  {unassigned.map(g => renderCaseCard(g))}
                                </div>
                              </div>
                            )}
                          </div>
                        )
                      })()}
                    </div>
                  </div>
                </>
              )}
            </div>
          )}

          {/* ── Patients tab ── */}
          {activeTab === 'patients' && (
            <div className="flex-1 overflow-hidden flex flex-col">
              <PatientTracker cohortId={cohortId} caseGroups={caseGroups} onPatientsChanged={fetchCohortPatients} />
            </div>
          )}

        </div>

        {/* ── Right panel: Cohort Overview OR Add Slides ── */}
        <div className="flex-1 flex flex-col overflow-hidden bg-background">
          {showAddSlides ? (
            <>
              {/* Add Slides drawer header */}
              <div className="flex items-center justify-between px-4 py-3 border-b border-gray-300 shrink-0">
                <h3 className="text-sm font-semibold">Add Slides</h3>
                <div className="flex items-center gap-1">
                  <Button variant="outline" size="sm" onClick={() => setShowPasteAdd(true)}>
                    <ClipboardList className="h-3.5 w-3.5 mr-1.5" />
                    Paste list
                  </Button>
                  <Button variant="ghost" size="sm" onClick={() => setShowAddSlides(false)}>
                    <X className="h-4 w-4" />
                  </Button>
                </div>
              </div>

              {/* Search controls */}
              <div className="p-3 space-y-2 border-b border-gray-300 shrink-0">
                <div className="flex gap-2">
                  <div className="relative flex-1">
                    <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                    <Input
                      placeholder="Search accession..."
                      value={searchTerm}
                      onChange={e => setSearchTerm(e.target.value)}
                      onKeyDown={e => e.key === 'Enter' && handleSearch()}
                      className="pl-10"
                    />
                  </div>
                  <Button onClick={handleSearch} disabled={searchLoading} size="default">
                    {searchLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Search'}
                  </Button>
                </div>
                <div className="flex flex-wrap gap-1.5">
                  <Select value={yearFilter} onValueChange={setYearFilter}>
                    <SelectTrigger className="w-28 h-8 text-xs">
                      <SelectValue placeholder="Year" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Years</SelectItem>
                      {years.map(y => <SelectItem key={y} value={y}>{y}</SelectItem>)}
                    </SelectContent>
                  </Select>
                  <Select value={stainFilter} onValueChange={setStainFilter}>
                    <SelectTrigger className="w-28 h-8 text-xs">
                      <Filter className="mr-1.5 h-3 w-3" />
                      <SelectValue placeholder="Stain" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Stains</SelectItem>
                      {stainTypes.map(s => <SelectItem key={s} value={s}>{s}</SelectItem>)}
                    </SelectContent>
                  </Select>
                  <Select value={tagFilter} onValueChange={setTagFilter}>
                    <SelectTrigger className="w-28 h-8 text-xs">
                      <SelectValue placeholder="Tag" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Tags</SelectItem>
                      {availableTags.filter(t => (t.slide_count ?? 0) > 0).map(tag => (
                        <SelectItem key={tag.id} value={tag.name}>
                          <div className="flex items-center gap-2">
                            {tag.color && <span className="w-2 h-2 rounded-[2px]" style={{ backgroundColor: tag.color }} />}
                            {tag.name}
                          </div>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Select value={externalFilter} onValueChange={(v) => setExternalFilter(v as 'exclude' | 'include' | 'only')}>
                    <SelectTrigger className="w-36 h-8 text-xs">
                      <SelectValue placeholder="Slide type" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="include">Clinical + external</SelectItem>
                      <SelectItem value="exclude">Clinical only</SelectItem>
                      <SelectItem value="only">External only</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* Bulk add bar */}
              {selectedHashes.size > 0 && (
                <div className="flex items-center gap-2 px-3 py-2 bg-muted/50 border-b border-gray-300 shrink-0">
                  <span className="text-xs text-muted-foreground">{selectedHashes.size} selected</span>
                  <Button
                    size="sm"
                    className="h-7"
                    onClick={() => addSlides(Array.from(selectedHashes).filter(h => !cohortHashSet.has(h)))}
                    disabled={addableCount === 0}
                  >
                    <Plus className="mr-1 h-3 w-3" />
                    Add {addableCount > 0 ? addableCount : ''}
                  </Button>
                  <Button variant="ghost" size="sm" className="h-7" onClick={() => setSelectedHashes(new Set())}>
                    Clear
                  </Button>
                </div>
              )}

              {/* Results info */}
              {searchResults.length > 0 && (
                <div className="px-3 py-1.5 text-xs text-muted-foreground border-b border-gray-300 shrink-0">
                  {searchResults.length} result{searchResults.length !== 1 ? 's' : ''}
                  {resultsTruncated && <span className="text-orange-600 ml-1">(limit reached)</span>}
                </div>
              )}

              {/* Results table */}
              <div className="flex-1 overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-10">
                        <Checkbox
                          checked={searchResults.length > 0 && selectedHashes.size === searchResults.length}
                          onCheckedChange={toggleSelectAllSearch}
                        />
                      </TableHead>
                      <TableHead><SortableHeader label="Accession" sortKey="accession_number" sortConfig={cbSearchSortConfig} onSort={handleCbSearchSort} /></TableHead>
                      <TableHead><SortableHeader label="Block" sortKey="block_id" sortConfig={cbSearchSortConfig} onSort={handleCbSearchSort} /></TableHead>
                      <TableHead className="w-20"></TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {sortedSearchResults.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={4} className="h-24 text-center text-muted-foreground text-sm">
                          {searchLoading ? 'Searching…' : 'Search to find slides to add.'}
                        </TableCell>
                      </TableRow>
                    ) : (
                      sortedSearchResults.map((slide) => {
                        const inCohort = cohortHashSet.has(slide.slide_hash)
                        return (
                          <TableRow
                            key={slide.slide_hash}
                            className={`${selectedHashes.has(slide.slide_hash) ? 'bg-muted/30' : ''} ${inCohort ? 'opacity-60' : ''}`}
                          >
                            <TableCell onClick={e => e.stopPropagation()}>
                              <Checkbox
                                checked={selectedHashes.has(slide.slide_hash)}
                                onCheckedChange={() => toggleSlideSelection(slide.slide_hash)}
                              />
                            </TableCell>
                            <TableCell className="text-sm font-medium">
                              <span className="inline-flex items-center gap-1.5">
                                {displaySlide(slide)}
                                {slide.is_external && (
                                  <Badge variant="outline" className="text-[10px] h-4 px-1 font-medium text-amber-700 border-amber-300 bg-amber-50">
                                    Ext
                                  </Badge>
                                )}
                              </span>
                            </TableCell>
                            <TableCell className="text-sm text-muted-foreground">{slide.block_id}</TableCell>
                            <TableCell>
                              {inCohort ? (
                                <Badge className="bg-green-500/10 text-green-700 text-xs gap-1 border-0">
                                  <Check className="h-3 w-3" />
                                  Added
                                </Badge>
                              ) : (
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-6 px-2 text-xs"
                                  onClick={e => { e.stopPropagation(); addSlides([slide.slide_hash]) }}
                                >
                                  <Plus className="h-3 w-3 mr-1" />
                                  Add
                                </Button>
                              )}
                            </TableCell>
                          </TableRow>
                        )
                      })
                    )}
                  </TableBody>
                </Table>
              </div>
            </>
          ) : (
            /* ── Cohort Overview panel ── */
            <div className="flex flex-col h-full overflow-auto">
              <div className="flex items-center gap-2 px-4 py-3 border-b border-gray-300 shrink-0">
                <BarChart2 className="h-4 w-4 text-muted-foreground" />
                <h3 className="text-sm font-semibold">Cohort Overview</h3>
              </div>

              <div className="flex-1 overflow-auto p-4 space-y-5">

                {/* Stain composition */}
                {Object.keys(stats.stains).length > 0 && (
                  <div>
                    <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2.5">
                      Stain Composition
                    </p>
                    <div className="space-y-2">
                      {Object.entries(stats.stains).sort(([, a], [, b]) => b - a).map(([stain, count]) => {
                        const pct = Math.round((count / stats.slides) * 100)
                        return (
                          <div key={stain} className="flex items-center gap-2.5">
                            <span className="text-xs text-muted-foreground w-10 text-right shrink-0">{stain}</span>
                            <div className="flex-1 bg-muted rounded-full h-2">
                              <div className="bg-primary/70 rounded-full h-2 transition-all" style={{ width: `${pct}%` }} />
                            </div>
                            <span className="text-xs text-muted-foreground w-5 text-right shrink-0">{count}</span>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )}

                {/* Year distribution */}
                {yearDistribution.length > 0 && (
                  <div>
                    <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2.5">
                      Year Distribution
                    </p>
                    <div className="space-y-2">
                      {yearDistribution.map(([year, count]) => {
                        const pct = Math.round((count / stats.cases) * 100)
                        return (
                          <div key={year} className="flex items-center gap-2.5">
                            <span className="text-xs text-muted-foreground w-10 text-right shrink-0">{year}</span>
                            <div className="flex-1 bg-muted rounded-full h-2">
                              <div className="bg-blue-500/60 rounded-full h-2 transition-all" style={{ width: `${pct}%` }} />
                            </div>
                            <span className="text-xs text-muted-foreground w-5 text-right shrink-0">{count}</span>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )}

                {/* Cohort flags */}
                <div>
                  <div className="flex items-center justify-between mb-2.5">
                    <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                      Flags
                    </p>
                    <button
                      className="text-xs text-primary hover:underline flex items-center gap-1"
                      onClick={() => {
                        setSelectedCaseHashes(new Set(caseGroups.map(g => g.case_hash)))
                        setFlagToolbarMode('new')
                      }}
                    >
                      <Plus className="h-3 w-3" />
                      New flag
                    </button>
                  </div>
                  {loadingFlags ? (
                    <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                  ) : cohortFlags.length === 0 ? (
                    <p className="text-xs text-muted-foreground">
                      No flags yet. Select cases then use "New flag" to mark subsets for analysis.
                    </p>
                  ) : (
                    <div className="space-y-1.5">
                      {cohortFlags.map(flag => (
                        <div key={flag.id} className="flex items-center justify-between gap-2 group/flag">
                          <div className="flex items-center gap-1.5 min-w-0">
                            <Flag className="h-3 w-3 text-amber-500 shrink-0" />
                            <span className="text-xs truncate font-medium">{flag.name}</span>
                            <span className="text-xs text-muted-foreground shrink-0">
                              {flag.case_hashes.length} case{flag.case_hashes.length !== 1 ? 's' : ''}
                            </span>
                          </div>
                          <div className="flex items-center gap-1 opacity-0 group-hover/flag:opacity-100 transition-opacity shrink-0">
                            <button
                              className="text-xs text-primary hover:underline"
                              title="Select these cases"
                              onClick={() => setSelectedCaseHashes(new Set(flag.case_hashes))}
                            >
                              Select
                            </button>
                            <button
                              className="text-muted-foreground hover:text-destructive"
                              title="Delete flag"
                              onClick={() => deleteFlag(flag.id)}
                            >
                              <Trash2 className="h-3 w-3" />
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                {/* Analysis history */}
                {analysisHistory.length > 0 && (
                  <div>
                    <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2.5">
                      Analysis History
                    </p>
                    <div className="space-y-2">
                      {analysisHistory.map(job => (
                        <div key={job.job_id} className="flex items-center justify-between gap-2">
                          <div className="flex items-center gap-2 min-w-0">
                            <StatusIcon status={job.status} />
                            <span className="text-xs font-medium truncate">{job.analysis_name}</span>
                          </div>
                          <span className="text-xs text-muted-foreground shrink-0">
                            {job.completed}/{job.total} slides
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {stats.slides === 0 && (
                  <div className="flex flex-col items-center justify-center h-32 gap-2 text-center">
                    <BarChart2 className="h-8 w-8 text-muted-foreground/30" />
                    <p className="text-xs text-muted-foreground">Add slides to see cohort overview</p>
                  </div>
                )}

              </div>
            </div>
          )}
        </div>
      </div>

      {/* ── Export Dialog ── */}
      <Dialog open={isExportDialogOpen} onOpenChange={setIsExportDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FolderArchive className="h-5 w-5" />
              Export Cohort
            </DialogTitle>
            <DialogDescription>
              Download all slides as a ZIP file organized by accession number.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            <div className="rounded-lg border p-4 space-y-3">
              <div className="flex justify-between text-sm">
                <span className="text-muted-foreground">Slides</span>
                <span className="font-medium">{stats.slides}</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-muted-foreground">Cases</span>
                <span className="font-medium">{stats.cases}</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-muted-foreground">Estimated size</span>
                <span className="font-medium">
                  {exportInfo.totalBytes > 0 ? formatBytes(exportInfo.totalBytes) : 'Unknown'}
                  {exportInfo.unknownCount > 0 && (
                    <span className="text-muted-foreground font-normal ml-1">
                      ({exportInfo.unknownCount} unknown)
                    </span>
                  )}
                </span>
              </div>
              {Object.keys(stats.stains).length > 0 && (
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">Stains</span>
                  <span className="font-medium">
                    {Object.entries(stats.stains).map(([s, n]) => `${s}: ${n}`).join(', ')}
                  </span>
                </div>
              )}
            </div>

            <div className="rounded-lg border p-4">
              <p className="text-xs font-medium text-muted-foreground mb-2">Folder structure</p>
              <div className="text-xs font-mono text-muted-foreground space-y-0.5">
                <p>{cohort?.name.replace(/\s+/g, '_')}.zip</p>
                {caseGroups.slice(0, 3).map((g) => (
                  <div key={g.case_hash} className="pl-4">
                    <p>{displayCase(g)}/</p>
                    {g.slides.slice(0, 2).map((s) => (
                      <p key={s.slide_hash} className="pl-4 truncate">
                        {isDemo()
                          ? `${s.slide_hash.slice(0, 10)}.svs`
                          : `${s.accession_number}_${s.block_id}_${s.stain_type}.svs`}
                      </p>
                    ))}
                    {g.slides.length > 2 && <p className="pl-4">… +{g.slides.length - 2} more</p>}
                  </div>
                ))}
                {caseGroups.length > 3 && <p className="pl-4">… +{caseGroups.length - 3} more cases</p>}
              </div>
            </div>

            {exportInfo.totalBytes > 10 * 1024 * 1024 * 1024 && (
              <div className="flex items-start gap-2 rounded-lg bg-orange-500/10 p-3 text-sm text-orange-700">
                <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                <span>
                  This export is over {formatBytes(exportInfo.totalBytes)}. Large downloads may take a while.
                </span>
              </div>
            )}
          </div>

          <div className="flex justify-end gap-2 pt-2">
            <Button variant="outline" onClick={() => setIsExportDialogOpen(false)}>Cancel</Button>
            <Button onClick={handleExport} disabled={isExporting}>
              <Download className="mr-2 h-4 w-4" />
              {isExporting ? 'Starting…' : 'Download ZIP'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Paste-flow dialog in "add" mode: resolve a pasted accession list and
          add the chosen slides to this cohort (same UX as cohort creation). */}
      <CohortFromPasteDialog
        open={showPasteAdd}
        onOpenChange={setShowPasteAdd}
        targetCohortId={cohortId}
        onAdded={() => { fetchCohort(); fetchAnalysisStatus() }}
      />

    </div>
  )
}
