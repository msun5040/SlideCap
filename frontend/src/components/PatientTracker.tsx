import { useState, useEffect, useCallback, useMemo } from 'react'
import {
  Plus,
  X,
  ChevronDown,
  ChevronRight,
  UserCircle2,
  Stethoscope,
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
import type { CaseGroup, CohortPatient, PatientSurgery } from '@/types/slide'

import { getApiBase } from '@/api'

interface PatientTrackerProps {
  cohortId: number
  caseGroups: CaseGroup[]
}

export function PatientTracker({ cohortId, caseGroups }: PatientTrackerProps) {
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

  // Assign-unassigned-case form
  const [assigningCase, setAssigningCase] = useState<string | null>(null)
  const [assignToPatient, setAssignToPatient] = useState<string>('')
  const [assignNewPatientLabel, setAssignNewPatientLabel] = useState('')
  const [assignSurgeryLabel, setAssignSurgeryLabel] = useState('S1')

  // Add-surgery-to-patient form
  const [addSurgeryPatientId, setAddSurgeryPatientId] = useState<number | null>(null)
  const [addSurgeryCaseHash, setAddSurgeryCaseHash] = useState('')
  const [addSurgeryLabel, setAddSurgeryLabel] = useState('S1')

  // ── Fetch ────────────────────────────────────────────────────────────────

  const fetchPatients = useCallback(async () => {
    setLoading(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients`)
      if (res.ok) {
        const data: CohortPatient[] = await res.json()
        setPatients(data)
        setExpandedPatients(new Set(data.map((p) => p.id)))
      }
    } catch { /* ignore */ }
    setLoading(false)
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

  // ── Helpers ──────────────────────────────────────────────────────────────

  const togglePatient = (id: number) =>
    setExpandedPatients((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })

  const surgeryFromCaseGroup = (caseHash: string, label: string, tempId = Date.now()): PatientSurgery => {
    const cg = caseGroups.find((g) => g.case_hash === caseHash)
    return {
      id: tempId,
      surgery_label: label,
      case_hash: caseHash,
      accession_number: cg?.accession_number ?? null,
      year: cg?.year ?? null,
      slide_count: cg?.slides.length ?? 0,
    }
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
      setExpandedPatients((prev) => new Set(prev).add(p.id))
    }
    setNewPatientLabel('')
    setShowNewPatient(false)
  }

  const deletePatient = async (patientId: number) => {
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients/${patientId}`, { method: 'DELETE' })
      if (res.ok) {
        setPatients((prev) => prev.filter((p) => p.id !== patientId))
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
    if (res.ok)
      setPatients((prev) => prev.map((p) => p.id === patientId ? { ...p, label: trimmed } : p))
    setEditingPatientId(null)
  }

  const removeSurgery = async (patientId: number, caseHash: string) => {
    const res = await fetch(
      `${getApiBase()}/cohorts/${cohortId}/patients/${patientId}/cases/${caseHash}`,
      { method: 'DELETE' },
    )
    if (res.ok)
      setPatients((prev) => prev.map((p) =>
        p.id === patientId ? { ...p, surgeries: p.surgeries.filter((s) => s.case_hash !== caseHash) } : p,
      ))
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
    if (res.ok)
      setPatients((prev) => prev.map((p) =>
        p.id === patientId
          ? { ...p, surgeries: p.surgeries.map((s) => s.case_hash === caseHash ? { ...s, surgery_label: trimmed } : s) }
          : p,
      ))
    setEditingSurgery(null)
  }

  const addSurgery = async (patientId: number, caseHash: string, surgeryLabel: string) => {
    if (!caseHash || !surgeryLabel.trim()) return
    const res = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients/${patientId}/cases`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ case_hash: caseHash, surgery_label: surgeryLabel.trim() }),
    })
    if (res.ok) {
      const newSurg = surgeryFromCaseGroup(caseHash, surgeryLabel.trim())
      setPatients((prev) => prev.map((p) =>
        p.id === patientId
          ? {
            ...p,
            surgeries: [...p.surgeries, newSurg]
              .sort((a, b) => a.surgery_label.localeCompare(b.surgery_label)),
          }
          : p,
      ))
    }
  }

  const assignCase = async () => {
    if (!assigningCase || !assignToPatient) return
    let patientId: number

    if (assignToPatient === 'new') {
      if (!assignNewPatientLabel.trim()) return
      const createRes = await fetch(`${getApiBase()}/cohorts/${cohortId}/patients`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ label: assignNewPatientLabel.trim() }),
      })
      if (!createRes.ok) return
      const newPatient: CohortPatient = await createRes.json()
      patientId = newPatient.id
      setPatients((prev) => [...prev, newPatient])
      setExpandedPatients((prev) => new Set(prev).add(patientId))
    } else {
      patientId = parseInt(assignToPatient)
    }

    await addSurgery(patientId, assigningCase, assignSurgeryLabel)
    setAssigningCase(null)
    setAssignToPatient('')
    setAssignNewPatientLabel('')
    setAssignSurgeryLabel('S1')
  }

  const submitAddSurgery = async () => {
    if (!addSurgeryPatientId || !addSurgeryCaseHash) return
    await addSurgery(addSurgeryPatientId, addSurgeryCaseHash, addSurgeryLabel)
    setAddSurgeryPatientId(null)
    setAddSurgeryCaseHash('')
    setAddSurgeryLabel('S1')
  }

  // ── Render ───────────────────────────────────────────────────────────────

  if (loading)
    return <div className="flex items-center justify-center h-full text-sm text-muted-foreground p-8">Loading...</div>

  return (
    <div className="flex flex-col h-full">
      {/* Panel header */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b bg-muted/30 shrink-0">
        <div>
          <span className="text-sm font-semibold">Patients</span>
          <span className="text-xs text-muted-foreground ml-2">
            {patients.length} patient{patients.length !== 1 ? 's' : ''}
            {assignedCaseHashes.size > 0 && ` · ${assignedCaseHashes.size} assigned`}
          </span>
        </div>
        <Button
          size="sm"
          variant="outline"
          className="h-7 text-xs"
          onClick={() => { setShowNewPatient(true); setNewPatientLabel('') }}
        >
          <Plus className="h-3.5 w-3.5 mr-1" />
          New Patient
        </Button>
      </div>

      <div className="flex-1 overflow-auto divide-y">

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
              Create a patient and assign their surgeries
            </p>
          </div>
        )}

        {/* Patient cards */}
        {patients.map((patient) => {
          const isExpanded = expandedPatients.has(patient.id)
          const isEditingLabel = editingPatientId === patient.id
          const isAddingSurgery = addSurgeryPatientId === patient.id

          return (
            <div key={patient.id} className="group/patient">
              {/* Patient header row */}
              <div className="flex items-center gap-2 px-4 py-2.5 hover:bg-muted/30 transition-colors">
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

                <span className="text-xs text-muted-foreground ml-auto mr-2">
                  {patient.surgeries.length} {patient.surgeries.length === 1 ? 'surgery' : 'surgeries'}
                </span>

                <button
                  className="opacity-0 group-hover/patient:opacity-100 text-muted-foreground hover:text-destructive transition-all"
                  onClick={() => deletePatient(patient.id)}
                  title="Delete patient"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>

              {/* Expanded content */}
              {isExpanded && (
                <div className="bg-muted/5">
                  {patient.surgeries.length === 0 && (
                    <p className="text-xs text-muted-foreground pl-12 py-1.5 italic">No surgeries yet</p>
                  )}

                  {patient.surgeries.map((surgery) => {
                    const isEditingSurg =
                      editingSurgery?.patientId === patient.id &&
                      editingSurgery?.caseHash === surgery.case_hash

                    return (
                      <div
                        key={surgery.case_hash}
                        className="flex items-center gap-2 pl-11 pr-4 py-1.5 group/surgery hover:bg-muted/20"
                      >
                        <Stethoscope className="h-3 w-3 text-muted-foreground shrink-0" />

                        {/* Surgery label — editable on click */}
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
                          {surgery.accession_number ?? surgery.case_hash.slice(0, 8) + '…'}
                        </span>

                        {surgery.year && (
                          <span className="text-xs text-muted-foreground shrink-0">{surgery.year}</span>
                        )}

                        <span className="text-xs text-muted-foreground shrink-0">
                          · {surgery.slide_count} slide{surgery.slide_count !== 1 ? 's' : ''}
                        </span>

                        <button
                          className="ml-auto opacity-0 group-hover/surgery:opacity-100 text-muted-foreground hover:text-destructive transition-all"
                          onClick={() => removeSurgery(patient.id, surgery.case_hash)}
                          title="Remove surgery"
                        >
                          <X className="h-3 w-3" />
                        </button>
                      </div>
                    )
                  })}

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
                                {c.accession_number ?? c.case_hash.slice(0, 8) + '…'}
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
                  ) : (
                    <button
                      className="flex items-center gap-1.5 pl-11 pr-4 py-1.5 text-xs text-muted-foreground hover:text-primary transition-colors w-full"
                      onClick={() => {
                        setAddSurgeryPatientId(patient.id)
                        setAddSurgeryCaseHash('')
                        setAddSurgeryLabel('S1')
                      }}
                    >
                      <Plus className="h-3 w-3" />
                      Add surgery
                    </button>
                  )}
                </div>
              )}
            </div>
          )
        })}

        {/* Unassigned cases section */}
        {unassignedCases.length > 0 && (
          <div>
            <div className="px-4 py-2 bg-muted/20 sticky top-0">
              <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
                Unassigned · {unassignedCases.length} {unassignedCases.length === 1 ? 'case' : 'cases'}
              </p>
            </div>

            {unassignedCases.map((caseGroup) => {
              const isAssigning = assigningCase === caseGroup.case_hash

              return (
                <div key={caseGroup.case_hash} className="border-t first:border-t-0">
                  {/* Case row */}
                  <div className="flex items-center gap-2 px-4 py-2 hover:bg-muted/20">
                    <div className="flex-1 flex items-center gap-2 text-sm min-w-0">
                      <span className="font-mono text-xs truncate text-muted-foreground">
                        {caseGroup.accession_number ?? caseGroup.case_hash.slice(0, 8) + '…'}
                      </span>
                      {caseGroup.year && (
                        <span className="text-xs text-muted-foreground shrink-0">{caseGroup.year}</span>
                      )}
                      <span className="text-xs text-muted-foreground shrink-0">
                        · {caseGroup.slides.length} slide{caseGroup.slides.length !== 1 ? 's' : ''}
                      </span>
                    </div>

                    {!isAssigning && (
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-6 text-xs px-2 shrink-0"
                        onClick={() => {
                          setAssigningCase(caseGroup.case_hash)
                          setAssignToPatient(patients.length > 0 ? String(patients[0].id) : 'new')
                          setAssignSurgeryLabel('S1')
                          setAssignNewPatientLabel('')
                        }}
                      >
                        Assign
                      </Button>
                    )}
                  </div>

                  {/* Assign form */}
                  {isAssigning && (
                    <div className="flex items-center gap-1.5 px-4 py-2 bg-primary/5 border-t flex-wrap">
                      <Select value={assignToPatient} onValueChange={setAssignToPatient}>
                        <SelectTrigger className="h-7 text-xs w-32">
                          <SelectValue placeholder="Patient…" />
                        </SelectTrigger>
                        <SelectContent>
                          {patients.map((p) => (
                            <SelectItem key={p.id} value={String(p.id)}>{p.label}</SelectItem>
                          ))}
                          <SelectItem value="new">
                            <span className="text-primary">+ New patient</span>
                          </SelectItem>
                        </SelectContent>
                      </Select>

                      {assignToPatient === 'new' && (
                        <Input
                          autoFocus
                          placeholder="Patient label"
                          value={assignNewPatientLabel}
                          onChange={(e) => setAssignNewPatientLabel(e.target.value)}
                          className="h-7 text-xs w-28"
                        />
                      )}

                      <Input
                        placeholder="S1"
                        value={assignSurgeryLabel}
                        onChange={(e) => setAssignSurgeryLabel(e.target.value)}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') assignCase()
                          if (e.key === 'Escape') setAssigningCase(null)
                        }}
                        className="h-7 text-xs w-14"
                      />

                      <Button
                        size="sm"
                        className="h-7 text-xs"
                        onClick={assignCase}
                        disabled={
                          !assignToPatient ||
                          (assignToPatient === 'new' && !assignNewPatientLabel.trim()) ||
                          !assignSurgeryLabel.trim()
                        }
                      >
                        Save
                      </Button>

                      <button
                        className="text-muted-foreground hover:text-foreground"
                        onClick={() => setAssigningCase(null)}
                      >
                        <X className="h-3.5 w-3.5" />
                      </button>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
