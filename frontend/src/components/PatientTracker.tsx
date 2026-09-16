import { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import {
  Plus,
  X,
  ChevronDown,
  ChevronRight,
  ArrowUp,
  ArrowDown,
  ArrowDownAZ,
  UserCircle2,
  Stethoscope,
  CircleDashed,
  GripVertical,
  ListOrdered,
  Search,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import type { CaseGroup, CohortPatient, PatientSurgery, CohortPlaceholder } from '@/types/slide'

import { getApiBase } from '@/api'
import { displayCase } from '@/lib/display'

interface PatientTrackerProps {
  cohortId: number
  caseGroups: CaseGroup[]
  /** All cohort placeholders; those pinned to a patient render as pastel-red
   *  "needs attention" timepoints within that patient. */
  placeholders?: CohortPlaceholder[]
  /** Fired after any patient/surgery mutation so the parent can refresh the
   *  case→patient grouping shown on the Cases tab. */
  onPatientsChanged?: () => void
  /** Fired after adding/removing a placeholder so the parent can refetch. */
  onPlaceholdersChanged?: () => void
}

/** What's being dragged. Kept in a ref rather than dataTransfer, which can't be
 *  read during dragover and so can't drive drop highlighting. */
type DragPayload =
  | { kind: 'case'; caseHash: string }
  | { kind: 'surgery'; patientId: number; caseHash: string }
  | { kind: 'placeholder'; patientId: number; placeholderId: number }
  | { kind: 'patient'; patientId: number }

const MAX_RANGE = 1000

export function PatientTracker({ cohortId, caseGroups, placeholders = [], onPatientsChanged, onPlaceholdersChanged }: PatientTrackerProps) {
  const [patients, setPatients] = useState<CohortPatient[]>([])
  const [loading, setLoading] = useState(true)

  // Expand / collapse patient cards
  const [expandedPatients, setExpandedPatients] = useState<Set<number>>(new Set())

  // Inline patient-label editing
  const [editingPatientId, setEditingPatientId] = useState<number | null>(null)
  const [editingLabel, setEditingLabel] = useState('')

  // Inline surgery-label editing
  const [editingSurgery, setEditingSurgery] = useState<{ patientId: number; caseHash: string } | null>(null)
  const [editingSurgeryLabel, setEditingSurgeryLabel] = useState('')

  // New-patient inline form (top of list)
  const [showNewPatient, setShowNewPatient] = useState(false)
  const [newPatientLabel, setNewPatientLabel] = useState('')

  // Range of patients (prefix + numbers), e.g. CCNU_1 … CCNU_50
  const [showRange, setShowRange] = useState(false)
  const [rangePrefix, setRangePrefix] = useState('P')
  const [rangeStart, setRangeStart] = useState('1')
  const [rangeEnd, setRangeEnd] = useState('10')
  const [rangeSuffix, setRangeSuffix] = useState('')
  const [rangePad, setRangePad] = useState(false)
  const [rangeBusy, setRangeBusy] = useState(false)
  const [rangeMsg, setRangeMsg] = useState('')

  // Unassigned cases: filter + type-a-patient-label quick assign
  const [caseFilter, setCaseFilter] = useState('')
  const [quickLabel, setQuickLabel] = useState<Record<string, string>>({})
  const [quickBusy, setQuickBusy] = useState<string | null>(null)

  // Add-surgery-to-patient form
  const [addSurgeryPatientId, setAddSurgeryPatientId] = useState<number | null>(null)
  const [addSurgeryCaseHash, setAddSurgeryCaseHash] = useState('')
  const [addSurgeryLabel, setAddSurgeryLabel] = useState('S1')

  // Add-placeholder-timepoint form (per patient)
  const [addPhPatientId, setAddPhPatientId] = useState<number | null>(null)
  const [addPhSurgeryLabel, setAddPhSurgeryLabel] = useState('S1')
  const [addPhLabel, setAddPhLabel] = useState('')
  const [addPhExpected, setAddPhExpected] = useState('')

  // Drag and drop
  const dragRef = useRef<DragPayload | null>(null)
  const [dragKind, setDragKind] = useState<DragPayload['kind'] | null>(null)
  const [dropPatientId, setDropPatientId] = useState<number | null>(null)
  const [timelineDrop, setTimelineDrop] = useState<{ patientId: number; index: number; after: boolean } | null>(null)
  const [patientDrop, setPatientDrop] = useState<{ index: number; after: boolean } | null>(null)

  // ── Fetch ────────────────────────────────────────────────────────────────

  // silent=true skips the full-panel "Loading…" swap — used for refetches after
  // a mutation (reorder/add) so expanded patients don't flash/collapse.
  const fetchPatients = useCallback(async (opts?: { silent?: boolean }) => {
    if (!opts?.silent) setLoading(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients`)
      if (res.ok) {
        const data: CohortPatient[] = await res.json()
        setPatients(data)
        // Patients start collapsed; the user expands the ones they want.
      }
    } catch { /* ignore */ }
    if (!opts?.silent) setLoading(false)
  }, [cohortId])

  useEffect(() => { fetchPatients() }, [fetchPatients])

  // ── Derived state ────────────────────────────────────────────────────────

  const assignedCaseHashes = useMemo(() => {
    const s = new Set<string>()
    for (const p of patients)
      for (const surg of p.surgeries)
        s.add(surg.case_hash)
    return s
  }, [patients])

  const unassignedCases = useMemo(
    () => caseGroups.filter((g) => !assignedCaseHashes.has(g.case_hash)),
    [caseGroups, assignedCaseHashes],
  )

  const visibleUnassigned = useMemo(() => {
    const q = caseFilter.trim().toLowerCase()
    if (!q) return unassignedCases
    return unassignedCases.filter(c => `${displayCase(c)} ${c.year ?? ''}`.toLowerCase().includes(q))
  }, [unassignedCases, caseFilter])

  const patientByLabel = useMemo(() => {
    const m = new Map<string, CohortPatient>()
    for (const p of patients) m.set(p.label.trim().toLowerCase(), p)
    return m
  }, [patients])

  // Placeholders pinned to each patient (id → list), for the pastel-red timepoints.
  const placeholdersByPatient = useMemo(() => {
    const m = new Map<number, CohortPlaceholder[]>()
    for (const p of placeholders) {
      if (p.patient_id == null) continue
      const arr = m.get(p.patient_id) ?? []
      arr.push(p)
      m.set(p.patient_id, arr)
    }
    return m
  }, [placeholders])

  const rangePreview = useMemo(() => {
    const a = Number(rangeStart), b = Number(rangeEnd)
    if (!Number.isInteger(a) || !Number.isInteger(b) || a < 0 || b < 0) return { labels: [] as string[], error: 'Start and end must be whole numbers.' }
    if (b < a) return { labels: [] as string[], error: 'End must be at least start.' }
    if (b - a + 1 > MAX_RANGE) return { labels: [] as string[], error: `At most ${MAX_RANGE} patients at once.` }
    const width = rangePad ? String(b).length : 0
    const labels: string[] = []
    for (let i = a; i <= b; i++) labels.push(`${rangePrefix}${String(i).padStart(width, '0')}${rangeSuffix}`)
    return { labels, error: '' }
  }, [rangePrefix, rangeStart, rangeEnd, rangeSuffix, rangePad])
  const rangeExisting = rangePreview.labels.filter(l => patientByLabel.has(l.trim().toLowerCase())).length

  // ── Helpers ──────────────────────────────────────────────────────────────

  const togglePatient = (id: number) =>
    setExpandedPatients((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })

  const expandPatient = (id: number) => setExpandedPatients(prev => new Set(prev).add(id))

  /** The next free timepoint label for a patient: S{n+1}, skipping any in use. */
  const nextSurgeryLabel = (patient: CohortPatient) => {
    const used = new Set([
      ...patient.surgeries.map(s => s.surgery_label.toLowerCase()),
      ...(placeholdersByPatient.get(patient.id) ?? []).map(p => (p.surgery_label ?? '').toLowerCase()),
    ])
    let n = patient.surgeries.length + (placeholdersByPatient.get(patient.id)?.length ?? 0) + 1
    while (used.has(`s${n}`)) n++
    return `S${n}`
  }

  // ── CRUD ─────────────────────────────────────────────────────────────────

  const createPatient = async () => {
    if (!newPatientLabel.trim()) return
    const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label: newPatientLabel.trim() }),
    })
    if (res.ok) {
      const p: CohortPatient = await res.json()
      setPatients((prev) => [...prev, p])
      onPatientsChanged?.()
    }
    setNewPatientLabel('')
    setShowNewPatient(false)
  }

  const createRange = async () => {
    if (rangePreview.error || rangePreview.labels.length === 0) return
    setRangeBusy(true); setRangeMsg('')
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients/bulk`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ labels: rangePreview.labels }),
      })
      const d = await res.json().catch(() => null)
      if (!res.ok) throw new Error(d?.detail || `HTTP ${res.status}`)
      await fetchPatients({ silent: true })
      onPatientsChanged?.()
      const skipped = d.skipped?.length ?? 0
      if (skipped === 0) {
        setShowRange(false)
      } else {
        setRangeMsg(`Created ${d.created.length}; skipped ${skipped} that already existed.`)
      }
    } catch (e: any) {
      setRangeMsg(`Couldn't create patients: ${e.message}`)
    } finally {
      setRangeBusy(false)
    }
  }

  const deletePatient = async (patientId: number) => {
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients/${patientId}`, { method: 'DELETE' })
      if (res.ok) {
        setPatients((prev) => prev.filter((p) => p.id !== patientId))
        onPatientsChanged?.()
      } else {
        const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }))
        console.error('Delete patient failed:', err)
        alert(`Failed to delete patient: ${err.detail || res.status}`)
      }
    } catch (e) {
      console.error('Delete patient error:', e)
      alert('Failed to delete patient: network error')
    }
  }

  const savePatientLabel = async (patientId: number) => {
    const trimmed = editingLabel.trim()
    if (!trimmed) { setEditingPatientId(null); return }
    const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients/${patientId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label: trimmed }),
    })
    if (res.ok) {
      setPatients((prev) => prev.map((p) => p.id === patientId ? { ...p, label: trimmed } : p))
      onPatientsChanged?.()
    }
    setEditingPatientId(null)
  }

  const removeSurgery = async (patientId: number, caseHash: string) => {
    const res = await fetch(
      `${getApiBase()}/cohorts/${cohortId}/patients/${patientId}/cases/${caseHash}`,
      { method: 'DELETE' },
    )
    if (res.ok) {
      setPatients((prev) => prev.map((p) =>
        p.id === patientId ? { ...p, surgeries: p.surgeries.filter((s) => s.case_hash !== caseHash) } : p,
      ))
      onPatientsChanged?.()
    }
  }

  const saveSurgeryLabel = async (patientId: number, caseHash: string) => {
    const trimmed = editingSurgeryLabel.trim()
    if (!trimmed) { setEditingSurgery(null); return }
    const res = await fetch(
      `${getApiBase()}/cohorts/${cohortId}/patients/${patientId}/cases/${caseHash}`,
      {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ surgery_label: trimmed }),
      },
    )
    if (res.ok) {
      setPatients((prev) => prev.map((p) =>
        p.id === patientId
          ? { ...p, surgeries: p.surgeries.map((s) => s.case_hash === caseHash ? { ...s, surgery_label: trimmed } : s) }
          : p,
      ))
      onPatientsChanged?.()
    }
    setEditingSurgery(null)
  }

  /** Assign (or move) a case to a patient. The server moves it if another patient had it. */
  const assignCaseTo = async (patientId: number, caseHash: string, surgeryLabel: string) => {
    if (!caseHash || !surgeryLabel.trim()) return false
    const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients/${patientId}/cases`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ case_hash: caseHash, surgery_label: surgeryLabel.trim() }),
    })
    if (res.ok) {
      await fetchPatients({ silent: true })
      onPatientsChanged?.()
    }
    return res.ok
  }

  const submitAddSurgery = async () => {
    if (!addSurgeryPatientId || !addSurgeryCaseHash) return
    await assignCaseTo(addSurgeryPatientId, addSurgeryCaseHash, addSurgeryLabel)
    setAddSurgeryPatientId(null)
    setAddSurgeryCaseHash('')
    setAddSurgeryLabel('S1')
  }

  /** Type a patient label next to an unassigned case: an existing patient gets the
   *  case; an unknown label creates the patient first. */
  const quickAssign = async (caseHash: string) => {
    const label = (quickLabel[caseHash] ?? '').trim()
    if (!label) return
    setQuickBusy(caseHash)
    try {
      let patient = patientByLabel.get(label.toLowerCase())
      if (!patient) {
        const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ label }),
        })
        if (!res.ok) return
        patient = await res.json() as CohortPatient
      }
      if (await assignCaseTo(patient.id, caseHash, nextSurgeryLabel(patient))) {
        setQuickLabel(prev => { const n = { ...prev }; delete n[caseHash]; return n })
        expandPatient(patient.id)
      }
    } finally {
      setQuickBusy(null)
    }
  }

  // Persist a patient order. The Cases tab reads this same order.
  const persistPatientOrder = async (reordered: CohortPatient[]) => {
    setPatients(reordered)  // optimistic
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients/reorder`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ patient_ids: reordered.map((p) => p.id) }),
      })
      if (res.ok) onPatientsChanged?.()
      else fetchPatients()  // resync on failure
    } catch { fetchPatients() }
  }

  const movePatient = (index: number, dir: -1 | 1) => {
    const target = index + dir
    if (target < 0 || target >= patients.length) return
    const reordered = [...patients]
    const [moved] = reordered.splice(index, 1)
    reordered.splice(target, 0, moved)
    persistPatientOrder(reordered)
  }

  // Return to the default alphabetical (A–Z) ordering, clearing the manual order.
  const resetOrder = async () => {
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients/reset-order`, {
        method: 'POST',
      })
      if (res.ok) {
        await fetchPatients()   // re-pull in alphabetical order
        onPatientsChanged?.()   // re-sort the Cases tab too
      }
    } catch { fetchPatients() }
  }

  // A patient's timeline = real surgeries + pinned placeholders, ordered together.
  // Manually-ordered items (display_order >= 1) first; the rest fall back to
  // natural-alphabetical by label — so a placeholder labelled S2 sits between S1
  // and S3 by default, and any item can be moved across the whole list.
  type TimelineEntry =
    | { kind: 'surgery'; order: number; label: string; surgery: PatientSurgery }
    | { kind: 'placeholder'; order: number; label: string; placeholder: CohortPlaceholder }
  const buildTimeline = useCallback((patient: CohortPatient): TimelineEntry[] => {
    const entries: TimelineEntry[] = [
      ...patient.surgeries.map((s) => ({
        kind: 'surgery' as const, order: s.display_order ?? 0, label: s.surgery_label ?? '', surgery: s,
      })),
      ...(placeholdersByPatient.get(patient.id) ?? []).map((p) => ({
        kind: 'placeholder' as const, order: p.display_order ?? 0, label: p.surgery_label ?? '', placeholder: p,
      })),
    ]
    entries.sort((a, b) => {
      const ao = a.order && a.order > 0 ? a.order : 1e9
      const bo = b.order && b.order > 0 ? b.order : 1e9
      if (ao !== bo) return ao - bo
      return a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: 'base' })
    })
    return entries
  }, [placeholdersByPatient])

  const persistTimeline = async (patient: CohortPatient, reordered: TimelineEntry[]) => {
    const items = reordered.map((e) =>
      e.kind === 'surgery'
        ? { kind: 'surgery', ref: e.surgery.case_hash }
        : { kind: 'placeholder', ref: String(e.placeholder.id) })
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients/${patient.id}/timeline/reorder`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items }),
      })
      if (res.ok) {
        fetchPatients({ silent: true })  // refresh surgery order without a flash
        onPatientsChanged?.()            // re-sort the Cases tab
        onPlaceholdersChanged?.()        // refresh placeholder order (prop)
      } else fetchPatients({ silent: true })
    } catch { fetchPatients({ silent: true }) }
  }

  // Move a timeline entry (surgery or placeholder) up/down and persist the order.
  const moveTimelineItem = (patient: CohortPatient, timeline: TimelineEntry[], index: number, dir: -1 | 1) => {
    const target = index + dir
    if (target < 0 || target >= timeline.length) return
    const reordered = [...timeline]
    const [moved] = reordered.splice(index, 1)
    reordered.splice(target, 0, moved)
    persistTimeline(patient, reordered)
  }

  const submitAddPlaceholder = async () => {
    if (!addPhPatientId || !addPhLabel.trim()) return
    const parsed = parseInt(addPhExpected, 10)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/placeholders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          label: addPhLabel.trim(),
          surgery_label: addPhSurgeryLabel.trim() || null,
          patient_id: addPhPatientId,
          expected_slides: Number.isFinite(parsed) && parsed > 0 ? parsed : null,
        }),
      })
      if (res.ok) {
        setAddPhPatientId(null); setAddPhLabel(''); setAddPhExpected(''); setAddPhSurgeryLabel('S1')
        onPlaceholdersChanged?.()
      }
    } catch (e) { console.error('Failed to add placeholder:', e) }
  }

  const deletePlaceholder = async (id: number) => {
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/placeholders/${id}`, { method: 'DELETE' })
      if (res.ok) onPlaceholdersChanged?.()
    } catch (e) { console.error('Failed to delete placeholder:', e) }
  }

  // ── Drag and drop ────────────────────────────────────────────────────────

  const startDrag = (e: React.DragEvent, payload: DragPayload, image?: Element | null) => {
    dragRef.current = payload
    setDragKind(payload.kind)
    e.dataTransfer.effectAllowed = 'move'
    // Firefox won't start a drag without some data.
    e.dataTransfer.setData('text/plain', payload.kind)
    if (image) e.dataTransfer.setDragImage(image, 16, 16)
  }

  const endDrag = () => {
    dragRef.current = null
    setDragKind(null)
    setDropPatientId(null)
    setTimelineDrop(null)
    setPatientDrop(null)
  }

  useEffect(() => {
    // A drop outside any target still ends the drag.
    window.addEventListener('dragend', endDrag)
    return () => window.removeEventListener('dragend', endDrag)
  }, [])

  /** Cases (unassigned, or another patient's surgery) can be dropped on a patient. */
  const acceptsCase = (patientId: number) => {
    const d = dragRef.current
    return !!d && (d.kind === 'case' || (d.kind === 'surgery' && d.patientId !== patientId))
  }

  const onPatientDragOver = (e: React.DragEvent, patientId: number) => {
    if (!acceptsCase(patientId)) return
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
    if (dropPatientId !== patientId) setDropPatientId(patientId)
  }

  const onPatientDrop = async (e: React.DragEvent, patient: CohortPatient) => {
    const d = dragRef.current
    if (!acceptsCase(patient.id) || !d) return
    e.preventDefault()
    endDrag()
    const caseHash = d.kind === 'case' || d.kind === 'surgery' ? d.caseHash : ''
    if (await assignCaseTo(patient.id, caseHash, nextSurgeryLabel(patient))) expandPatient(patient.id)
  }

  /** Reorder patients: drop a dragged patient before/after another's header. */
  const onPatientHeaderDragOver = (e: React.DragEvent, index: number) => {
    if (dragRef.current?.kind !== 'patient') return
    e.preventDefault()
    const r = (e.currentTarget as HTMLElement).getBoundingClientRect()
    const after = e.clientY > r.top + r.height / 2
    if (patientDrop?.index !== index || patientDrop.after !== after) setPatientDrop({ index, after })
  }

  const onPatientHeaderDrop = (e: React.DragEvent, index: number) => {
    const d = dragRef.current
    if (d?.kind !== 'patient') return
    e.preventDefault()
    const after = patientDrop?.index === index ? patientDrop.after : false
    endDrag()
    const from = patients.findIndex(p => p.id === d.patientId)
    if (from < 0) return
    let to = index + (after ? 1 : 0)
    if (from < to) to--
    if (to === from) return
    const reordered = [...patients]
    const [moved] = reordered.splice(from, 1)
    reordered.splice(to, 0, moved)
    persistPatientOrder(reordered)
  }

  /** Reorder within a patient: drop a timepoint before/after another. */
  const isOwnTimelineDrag = (patientId: number) => {
    const d = dragRef.current
    return !!d && (d.kind === 'surgery' || d.kind === 'placeholder') && d.patientId === patientId
  }

  const onTimelineDragOver = (e: React.DragEvent, patientId: number, index: number) => {
    if (!isOwnTimelineDrag(patientId)) return   // other drags bubble up to the patient
    e.preventDefault()
    e.stopPropagation()
    const r = (e.currentTarget as HTMLElement).getBoundingClientRect()
    const after = e.clientY > r.top + r.height / 2
    if (timelineDrop?.patientId !== patientId || timelineDrop.index !== index || timelineDrop.after !== after) {
      setTimelineDrop({ patientId, index, after })
    }
  }

  const onTimelineDrop = (e: React.DragEvent, patient: CohortPatient, timeline: TimelineEntry[], index: number) => {
    const d = dragRef.current
    if (!isOwnTimelineDrag(patient.id) || !d) return
    e.preventDefault()
    e.stopPropagation()
    const after = timelineDrop?.patientId === patient.id && timelineDrop.index === index ? timelineDrop.after : false
    endDrag()
    const from = timeline.findIndex(t =>
      (d.kind === 'surgery' && t.kind === 'surgery' && t.surgery.case_hash === d.caseHash) ||
      (d.kind === 'placeholder' && t.kind === 'placeholder' && t.placeholder.id === d.placeholderId))
    if (from < 0) return
    let to = index + (after ? 1 : 0)
    if (from < to) to--
    if (to === from) return
    const reordered = [...timeline]
    const [moved] = reordered.splice(from, 1)
    reordered.splice(to, 0, moved)
    persistTimeline(patient, reordered)
  }

  // ── Render ───────────────────────────────────────────────────────────────

  if (loading)
    return <div className="flex items-center justify-center h-full text-sm text-muted-foreground p-8">Loading...</div>

  const dropLine = 'pointer-events-none absolute left-3 right-3 h-0.5 rounded bg-primary'

  return (
    <div className="@container flex flex-col h-full">
      {/* Panel header */}
      <div className="flex flex-wrap items-center justify-between gap-2 px-4 py-2.5 border-b bg-muted/30 shrink-0">
        <div className="whitespace-nowrap">
          <span className="text-sm font-semibold">Patients</span>
          <span className="text-xs text-muted-foreground ml-2">
            {patients.length} patient{patients.length !== 1 ? 's' : ''}
            {assignedCaseHashes.size > 0 && ` · ${assignedCaseHashes.size} assigned`}
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          {patients.length > 1 && (
            <Button
              size="sm"
              variant="ghost"
              className="h-7 text-xs text-muted-foreground"
              onClick={resetOrder}
              title="Reset to alphabetical order (A–Z). Drag a patient's handle (or use the arrows) to set a custom order."
            >
              <ArrowDownAZ className="h-3.5 w-3.5 mr-1" />
              Sort A–Z
            </Button>
          )}
          <Button
            size="sm"
            variant="outline"
            className="h-7 text-xs"
            onClick={() => { setShowRange(v => !v); setShowNewPatient(false); setRangeMsg('') }}
            title="Create a numbered series of patients, e.g. CCNU_1 … CCNU_50"
          >
            <ListOrdered className="h-3.5 w-3.5 mr-1" />
            Add range
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="h-7 text-xs"
            onClick={() => { setShowNewPatient(true); setShowRange(false); setNewPatientLabel('') }}
          >
            <Plus className="h-3.5 w-3.5 mr-1" />
            New Patient
          </Button>
        </div>
      </div>

      {/* Range form */}
      {showRange && (
        <div className="shrink-0 space-y-2 border-b bg-primary/5 px-4 py-2.5">
          <div className="flex flex-wrap items-end gap-2">
            <label className="space-y-0.5">
              <span className="block text-[11px] text-muted-foreground">Prefix</span>
              <Input value={rangePrefix} onChange={e => setRangePrefix(e.target.value)} className="h-7 w-28 text-sm" placeholder="CCNU_" autoFocus />
            </label>
            <label className="space-y-0.5">
              <span className="block text-[11px] text-muted-foreground">From</span>
              <Input type="number" min={0} value={rangeStart} onChange={e => setRangeStart(e.target.value)} className="h-7 w-20 text-sm" />
            </label>
            <label className="space-y-0.5">
              <span className="block text-[11px] text-muted-foreground">To</span>
              <Input type="number" min={0} value={rangeEnd} onChange={e => setRangeEnd(e.target.value)}
                     onKeyDown={e => { if (e.key === 'Enter') createRange() }} className="h-7 w-20 text-sm" />
            </label>
            <label className="space-y-0.5">
              <span className="block text-[11px] text-muted-foreground">Suffix (optional)</span>
              <Input value={rangeSuffix} onChange={e => setRangeSuffix(e.target.value)} className="h-7 w-24 text-sm" />
            </label>
            <label className="flex h-7 items-center gap-1.5 text-xs" title="Pad numbers to the same width, e.g. CCNU_01 … CCNU_50">
              <input type="checkbox" checked={rangePad} onChange={e => setRangePad(e.target.checked)} />
              Zero-pad
            </label>
            <Button size="sm" className="h-7" onClick={createRange}
                    disabled={rangeBusy || !!rangePreview.error || rangePreview.labels.length === rangeExisting}>
              Create {Math.max(0, rangePreview.labels.length - rangeExisting)}
            </Button>
            <button onClick={() => setShowRange(false)} className="mb-1 text-muted-foreground hover:text-foreground">
              <X className="h-4 w-4" />
            </button>
          </div>
          <p className="text-xs text-muted-foreground">
            {rangePreview.error ? <span className="text-red-600">{rangePreview.error}</span> : (
              <>
                <span className="font-mono">
                  {rangePreview.labels.slice(0, 3).join(', ')}
                  {rangePreview.labels.length > 4 ? ' … ' : rangePreview.labels.length === 4 ? ', ' : ''}
                  {rangePreview.labels.length > 3 ? rangePreview.labels[rangePreview.labels.length - 1] : ''}
                </span>
                {' '}· {rangePreview.labels.length} patient{rangePreview.labels.length === 1 ? '' : 's'}
                {rangeExisting > 0 && ` · ${rangeExisting} already exist and will be skipped`}
              </>
            )}
          </p>
          {rangeMsg && <p className="text-xs text-amber-700">{rangeMsg}</p>}
        </div>
      )}

      {/* Side by side when the panel is wide enough; stacked otherwise. */}
      <div className="flex min-h-0 flex-1 flex-col @2xl:flex-row">
        {/* ── Patients ── */}
        <div className="min-h-0 flex-1 overflow-auto divide-y">

          {/* New patient inline form */}
          {showNewPatient && (
            <div className="flex items-center gap-2 px-4 py-2 bg-primary/5">
              <UserCircle2 className="h-4 w-4 text-muted-foreground shrink-0" />
              <Input
                autoFocus
                placeholder="Label, e.g. P001"
                value={newPatientLabel}
                onChange={(e) => setNewPatientLabel(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') createPatient()
                  if (e.key === 'Escape') setShowNewPatient(false)
                }}
                className="h-7 text-sm flex-1"
              />
              <Button size="sm" className="h-7" onClick={createPatient} disabled={!newPatientLabel.trim()}>
                Create
              </Button>
              <button onClick={() => setShowNewPatient(false)} className="text-muted-foreground hover:text-foreground">
                <X className="h-4 w-4" />
              </button>
            </div>
          )}

          {/* Empty state */}
          {patients.length === 0 && !showNewPatient && (
            <div className="flex flex-col items-center justify-center py-12 px-4 text-center">
              <UserCircle2 className="h-10 w-10 text-muted-foreground/30 mb-3" />
              <p className="text-sm font-medium text-muted-foreground">No patients yet</p>
              <p className="text-xs text-muted-foreground mt-1">
                Create patients (or a numbered range), then drag cases onto them
              </p>
            </div>
          )}

          {/* Patient cards */}
          {patients.map((patient, index) => {
            const isExpanded = expandedPatients.has(patient.id)
            const isEditingLabel = editingPatientId === patient.id
            const isAddingSurgery = addSurgeryPatientId === patient.id
            const timeline = buildTimeline(patient)
            const isCaseDropTarget = dropPatientId === patient.id && (dragKind === 'case' || dragKind === 'surgery')

            return (
              <div
                key={patient.id}
                className={`group/patient relative transition-colors ${isCaseDropTarget ? 'bg-primary/10 ring-2 ring-inset ring-primary' : ''}`}
                onDragOver={(e) => onPatientDragOver(e, patient.id)}
                onDragLeave={(e) => {
                  if (!(e.currentTarget as HTMLElement).contains(e.relatedTarget as Node)) {
                    setDropPatientId(prev => (prev === patient.id ? null : prev))
                  }
                }}
                onDrop={(e) => onPatientDrop(e, patient)}
              >
                {/* Patient header row */}
                <div
                  data-patient-row
                  className="relative flex items-center gap-2 px-4 py-2.5 hover:bg-muted/30 transition-colors"
                  onDragOver={(e) => onPatientHeaderDragOver(e, index)}
                  onDrop={(e) => onPatientHeaderDrop(e, index)}
                >
                  {patientDrop?.index === index && dragKind === 'patient' && (
                    <div className={dropLine} style={patientDrop.after ? { bottom: -1 } : { top: -1 }} />
                  )}
                  <span
                    draggable
                    onDragStart={(e) => startDrag(e, { kind: 'patient', patientId: patient.id },
                      (e.currentTarget as HTMLElement).closest('[data-patient-row]'))}
                    onDragEnd={endDrag}
                    className="cursor-grab text-muted-foreground/40 hover:text-muted-foreground active:cursor-grabbing"
                    title="Drag to reorder patients"
                  >
                    <GripVertical className="h-3.5 w-3.5" />
                  </span>
                  <button
                    className="text-muted-foreground shrink-0"
                    onClick={() => togglePatient(patient.id)}
                  >
                    {isExpanded
                      ? <ChevronDown className="h-3.5 w-3.5" />
                      : <ChevronRight className="h-3.5 w-3.5" />}
                  </button>

                  <UserCircle2 className="h-4 w-4 text-blue-500 shrink-0" />

                  {isEditingLabel ? (
                    <Input
                      autoFocus
                      value={editingLabel}
                      onChange={(e) => setEditingLabel(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') savePatientLabel(patient.id)
                        if (e.key === 'Escape') setEditingPatientId(null)
                      }}
                      onBlur={() => savePatientLabel(patient.id)}
                      className="h-6 text-sm flex-1 max-w-36 py-0"
                    />
                  ) : (
                    <button
                      className="text-sm font-medium hover:text-primary transition-colors text-left"
                      onClick={() => { setEditingPatientId(patient.id); setEditingLabel(patient.label) }}
                      title="Click to rename"
                    >
                      {patient.label}
                    </button>
                  )}

                  {isCaseDropTarget && (
                    <span className="text-xs font-medium text-primary">Drop to add as {nextSurgeryLabel(patient)}</span>
                  )}

                  <span className="text-xs text-muted-foreground ml-auto mr-2">
                    {patient.surgeries.length} {patient.surgeries.length === 1 ? 'surgery' : 'surgeries'}
                    {(placeholdersByPatient.get(patient.id)?.length ?? 0) > 0 && (
                      <span className="text-red-500"> · {placeholdersByPatient.get(patient.id)!.length} pending</span>
                    )}
                  </span>

                  <div className="flex items-center opacity-0 group-hover/patient:opacity-100 transition-all">
                    <button
                      className="text-muted-foreground hover:text-foreground disabled:opacity-25 disabled:hover:text-muted-foreground"
                      onClick={() => movePatient(index, -1)}
                      disabled={index === 0}
                      title="Move up"
                    >
                      <ArrowUp className="h-3.5 w-3.5" />
                    </button>
                    <button
                      className="text-muted-foreground hover:text-foreground disabled:opacity-25 disabled:hover:text-muted-foreground"
                      onClick={() => movePatient(index, 1)}
                      disabled={index === patients.length - 1}
                      title="Move down"
                    >
                      <ArrowDown className="h-3.5 w-3.5" />
                    </button>
                  </div>

                  <button
                    className="opacity-0 group-hover/patient:opacity-100 text-muted-foreground hover:text-destructive transition-all ml-1"
                    onClick={() => deletePatient(patient.id)}
                    title="Delete patient"
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>

                {/* Expanded content */}
                {isExpanded && (
                  <div className="bg-muted/5">
                    {timeline.length === 0 && (
                      <p className="text-xs text-muted-foreground pl-12 py-1.5 italic">
                        No timepoints yet — drag a case here, or use Add surgery
                      </p>
                    )}

                    {timeline.map((entry, tIndex) => {
                      const canUp = tIndex > 0
                      const canDown = tIndex < timeline.length - 1
                      const OrderButtons = (
                        <>
                          <button
                            className="text-muted-foreground hover:text-foreground disabled:opacity-25 disabled:hover:text-muted-foreground"
                            onClick={() => moveTimelineItem(patient, timeline, tIndex, -1)}
                            disabled={!canUp}
                            title="Move up"
                          >
                            <ArrowUp className="h-3 w-3" />
                          </button>
                          <button
                            className="text-muted-foreground hover:text-foreground disabled:opacity-25 disabled:hover:text-muted-foreground"
                            onClick={() => moveTimelineItem(patient, timeline, tIndex, 1)}
                            disabled={!canDown}
                            title="Move down"
                          >
                            <ArrowDown className="h-3 w-3" />
                          </button>
                        </>
                      )
                      const indicator = timelineDrop?.patientId === patient.id && timelineDrop.index === tIndex && (
                        <div className={dropLine} style={timelineDrop.after ? { bottom: -1 } : { top: -1 }} />
                      )
                      const rowDnD = {
                        onDragOver: (e: React.DragEvent) => onTimelineDragOver(e, patient.id, tIndex),
                        onDrop: (e: React.DragEvent) => onTimelineDrop(e, patient, timeline, tIndex),
                        onDragEnd: endDrag,
                      }

                      if (entry.kind === 'surgery') {
                        const surgery = entry.surgery
                        const isEditingSurg =
                          editingSurgery?.patientId === patient.id &&
                          editingSurgery?.caseHash === surgery.case_hash
                        return (
                          <div
                            key={`s-${surgery.case_hash}`}
                            draggable={!isEditingSurg}
                            onDragStart={(e) => startDrag(e, { kind: 'surgery', patientId: patient.id, caseHash: surgery.case_hash })}
                            {...rowDnD}
                            className="relative flex items-center gap-2 pl-6 pr-4 py-1.5 group/surgery hover:bg-muted/20 cursor-grab active:cursor-grabbing"
                            title="Drag to reorder, or onto another patient to move this case"
                          >
                            {indicator}
                            <GripVertical className="h-3 w-3 text-muted-foreground/30 group-hover/surgery:text-muted-foreground shrink-0" />
                            <Stethoscope className="h-3 w-3 text-muted-foreground shrink-0 ml-2" />

                            {isEditingSurg ? (
                              <Input
                                autoFocus
                                value={editingSurgeryLabel}
                                onChange={(e) => setEditingSurgeryLabel(e.target.value)}
                                onKeyDown={(e) => {
                                  if (e.key === 'Enter') saveSurgeryLabel(patient.id, surgery.case_hash)
                                  if (e.key === 'Escape') setEditingSurgery(null)
                                }}
                                onBlur={() => saveSurgeryLabel(patient.id, surgery.case_hash)}
                                className="h-5 text-xs w-14 py-0"
                              />
                            ) : (
                              <Badge
                                variant="secondary"
                                className="text-xs cursor-pointer hover:bg-primary/10 transition-colors px-1.5 h-5 shrink-0"
                                onClick={() => {
                                  setEditingSurgery({ patientId: patient.id, caseHash: surgery.case_hash })
                                  setEditingSurgeryLabel(surgery.surgery_label)
                                }}
                                title="Click to edit label"
                              >
                                {surgery.surgery_label}
                              </Badge>
                            )}

                            <span className="text-xs font-mono text-foreground truncate">
                              {displayCase(surgery)}
                            </span>

                            {surgery.year && (
                              <span className="text-xs text-muted-foreground shrink-0">{surgery.year}</span>
                            )}

                            <span className="text-xs text-muted-foreground shrink-0">
                              · {surgery.slide_count} slide{surgery.slide_count !== 1 ? 's' : ''}
                            </span>

                            <div className="ml-auto flex items-center opacity-0 group-hover/surgery:opacity-100 transition-all">
                              {OrderButtons}
                              <button
                                className="text-muted-foreground hover:text-destructive transition-colors ml-0.5"
                                onClick={() => removeSurgery(patient.id, surgery.case_hash)}
                                title="Remove surgery"
                              >
                                <X className="h-3 w-3" />
                              </button>
                            </div>
                          </div>
                        )
                      }

                      // Placeholder timepoint — pastel red "needs attention"
                      const ph = entry.placeholder
                      return (
                        <div
                          key={`ph-${ph.id}`}
                          draggable
                          onDragStart={(e) => startDrag(e, { kind: 'placeholder', patientId: patient.id, placeholderId: ph.id })}
                          {...rowDnD}
                          className="relative flex items-center gap-2 pl-6 pr-4 py-1.5 group/ph bg-red-50/70 hover:bg-red-50 border-l-2 border-red-300 cursor-grab active:cursor-grabbing"
                          title={ph.note || 'Slides still to be found & scanned — drag to reorder'}
                        >
                          {indicator}
                          <GripVertical className="h-3 w-3 text-red-300 shrink-0" />
                          <CircleDashed className="h-3 w-3 text-red-400 shrink-0 ml-2" />
                          {ph.surgery_label && (
                            <Badge
                              variant="secondary"
                              className="text-xs px-1.5 h-5 shrink-0 bg-red-100 text-red-700 hover:bg-red-100 border border-red-200"
                            >
                              {ph.surgery_label}
                            </Badge>
                          )}
                          <span className="text-xs font-mono text-red-700 truncate">{ph.label}</span>
                          {ph.expected_slides ? (
                            <span className="text-xs text-red-500 shrink-0">
                              · ~{ph.expected_slides} slide{ph.expected_slides !== 1 ? 's' : ''}
                            </span>
                          ) : null}
                          <span className="text-[10px] uppercase tracking-wide text-red-500 shrink-0 ml-1">
                            needs scan
                          </span>
                          <div className="ml-auto flex items-center opacity-0 group-hover/ph:opacity-100 transition-all">
                            {OrderButtons}
                            <button
                              className="text-muted-foreground hover:text-destructive transition-colors ml-0.5"
                              onClick={() => deletePlaceholder(ph.id)}
                              title="Remove placeholder"
                            >
                              <X className="h-3 w-3" />
                            </button>
                          </div>
                        </div>
                      )
                    })}

                    {/* Add placeholder inline form */}
                    {addPhPatientId === patient.id && (
                      <div className="flex items-center gap-1.5 pl-11 pr-4 py-2 bg-red-50/60 border-t border-red-100 flex-wrap">
                        <Input
                          placeholder="S1"
                          value={addPhSurgeryLabel}
                          onChange={(e) => setAddPhSurgeryLabel(e.target.value)}
                          onKeyDown={(e) => { if (e.key === 'Escape') setAddPhPatientId(null) }}
                          className="h-7 text-xs w-12"
                          title="Timepoint label"
                        />
                        <Input
                          autoFocus
                          placeholder="Accession / what to find…"
                          value={addPhLabel}
                          onChange={(e) => setAddPhLabel(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') submitAddPlaceholder()
                            if (e.key === 'Escape') setAddPhPatientId(null)
                          }}
                          className="h-7 text-xs flex-1 min-w-28"
                        />
                        <Input
                          type="number"
                          min="1"
                          placeholder="#"
                          value={addPhExpected}
                          onChange={(e) => setAddPhExpected(e.target.value)}
                          onKeyDown={(e) => { if (e.key === 'Enter') submitAddPlaceholder() }}
                          className="h-7 text-xs w-14"
                          title="Expected # of slides"
                        />
                        <Button
                          size="sm"
                          className="h-7 text-xs bg-red-600 hover:bg-red-700"
                          onClick={submitAddPlaceholder}
                          disabled={!addPhLabel.trim()}
                        >
                          Add
                        </Button>
                        <button className="text-muted-foreground hover:text-foreground" onClick={() => setAddPhPatientId(null)}>
                          <X className="h-3.5 w-3.5" />
                        </button>
                      </div>
                    )}

                    {/* Add surgery row */}
                    {isAddingSurgery ? (
                      <div className="flex items-center gap-1.5 pl-11 pr-4 py-2 bg-primary/5 border-t flex-wrap">
                        <Select value={addSurgeryCaseHash} onValueChange={setAddSurgeryCaseHash}>
                          <SelectTrigger className="h-7 text-xs flex-1 min-w-28">
                            <SelectValue placeholder="Select case…" />
                          </SelectTrigger>
                          <SelectContent>
                            {unassignedCases.length === 0
                              ? <SelectItem value="_none" disabled>No unassigned cases</SelectItem>
                              : unassignedCases.map((c) => (
                                <SelectItem key={c.case_hash} value={c.case_hash}>
                                  {displayCase(c)}
                                  {c.year ? ` (${c.year})` : ''}
                                </SelectItem>
                              ))}
                          </SelectContent>
                        </Select>

                        <Input
                          placeholder="S1"
                          value={addSurgeryLabel}
                          onChange={(e) => setAddSurgeryLabel(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') submitAddSurgery()
                            if (e.key === 'Escape') setAddSurgeryPatientId(null)
                          }}
                          className="h-7 text-xs w-14"
                        />

                        <Button
                          size="sm"
                          className="h-7 text-xs"
                          onClick={submitAddSurgery}
                          disabled={!addSurgeryCaseHash || addSurgeryCaseHash === '_none' || !addSurgeryLabel.trim()}
                        >
                          Add
                        </Button>

                        <button
                          className="text-muted-foreground hover:text-foreground"
                          onClick={() => setAddSurgeryPatientId(null)}
                        >
                          <X className="h-3.5 w-3.5" />
                        </button>
                      </div>
                    ) : addPhPatientId === patient.id ? null : (
                      <div className="flex items-center gap-4 pl-11 pr-4 py-1.5">
                        <button
                          className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-primary transition-colors"
                          onClick={() => {
                            setAddSurgeryPatientId(patient.id)
                            setAddSurgeryCaseHash('')
                            setAddSurgeryLabel(nextSurgeryLabel(patient))
                          }}
                        >
                          <Plus className="h-3 w-3" />
                          Add surgery
                        </button>
                        <button
                          className="flex items-center gap-1.5 text-xs text-red-500 hover:text-red-700 transition-colors"
                          onClick={() => {
                            setAddPhPatientId(patient.id)
                            setAddPhLabel('')
                            setAddPhExpected('')
                            setAddPhSurgeryLabel(nextSurgeryLabel(patient))
                          }}
                          title="Add a placeholder timepoint for slides still to be found & scanned"
                        >
                          <CircleDashed className="h-3 w-3" />
                          Add placeholder
                        </button>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )
          })}
        </div>

        {/* ── Unassigned cases: drag onto a patient, or type a patient label ── */}
        {unassignedCases.length > 0 && (
          <div className="flex min-h-0 max-h-[50%] flex-col border-t @2xl:max-h-none @2xl:w-[22rem] @2xl:shrink-0 @2xl:border-l @2xl:border-t-0">
            <div className="shrink-0 space-y-1.5 border-b bg-muted/20 px-3 py-2">
              <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Unassigned · {unassignedCases.length} {unassignedCases.length === 1 ? 'case' : 'cases'}
              </p>
              <p className="text-[11px] text-muted-foreground">
                Drag a case onto a patient, or type a patient label and press Enter — a new label creates that patient.
              </p>
              {unassignedCases.length > 8 && (
                <div className="relative">
                  <Search className="pointer-events-none absolute left-2 top-1/2 h-3 w-3 -translate-y-1/2 text-muted-foreground" />
                  <Input value={caseFilter} onChange={e => setCaseFilter(e.target.value)} placeholder="Filter cases…" className="h-7 pl-6 text-xs" />
                </div>
              )}
            </div>
            <datalist id={`patient-labels-${cohortId}`}>
              {patients.map(p => <option key={p.id} value={p.label} />)}
            </datalist>
            <div className="min-h-0 flex-1 overflow-auto divide-y">
              {visibleUnassigned.map((caseGroup) => {
                const typed = quickLabel[caseGroup.case_hash] ?? ''
                const match = typed.trim() ? patientByLabel.get(typed.trim().toLowerCase()) : undefined
                return (
                  <div
                    key={caseGroup.case_hash}
                    className="group/case flex flex-col gap-1 px-3 py-2 hover:bg-muted/20"
                  >
                    {/* Only this line is the drag handle, so the label input below stays selectable. */}
                    <div
                      draggable
                      onDragStart={(e) => startDrag(e, { kind: 'case', caseHash: caseGroup.case_hash },
                        (e.currentTarget as HTMLElement).parentElement)}
                      onDragEnd={endDrag}
                      className="flex min-w-0 cursor-grab items-center gap-2 active:cursor-grabbing"
                      title="Drag onto a patient"
                    >
                      <GripVertical className="h-3.5 w-3.5 shrink-0 text-muted-foreground/40 group-hover/case:text-muted-foreground" />
                      <span className="truncate font-mono text-xs text-muted-foreground">{displayCase(caseGroup)}</span>
                      {caseGroup.year && <span className="shrink-0 text-xs text-muted-foreground">{caseGroup.year}</span>}
                      <span className="shrink-0 text-xs text-muted-foreground">
                        · {caseGroup.slides.length} slide{caseGroup.slides.length !== 1 ? 's' : ''}
                      </span>
                    </div>
                    <div className="flex items-center gap-1.5 pl-5">
                      <Input
                        list={`patient-labels-${cohortId}`}
                        value={typed}
                        onChange={e => setQuickLabel(prev => ({ ...prev, [caseGroup.case_hash]: e.target.value }))}
                        onKeyDown={e => { if (e.key === 'Enter') quickAssign(caseGroup.case_hash) }}
                        placeholder="Patient label…"
                        className="h-6 flex-1 text-xs"
                      />
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-6 px-2 text-xs"
                        disabled={!typed.trim() || quickBusy === caseGroup.case_hash}
                        onClick={() => quickAssign(caseGroup.case_hash)}
                        title={match ? `Add to ${match.label} as ${nextSurgeryLabel(match)}` : typed.trim() ? `Create patient “${typed.trim()}” and add this case` : undefined}
                      >
                        {typed.trim() && !match ? 'Create & add' : 'Add'}
                      </Button>
                    </div>
                  </div>
                )
              })}
              {visibleUnassigned.length === 0 && (
                <p className="px-3 py-4 text-center text-xs text-muted-foreground">No cases match.</p>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
