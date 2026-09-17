import { useEffect, useState } from 'react'
import { Loader2, Plus, Users } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { SearchableSelect } from '@/components/ui/searchable-select'
import { getApiBase } from '@/api'

/** One selectable set of slides, e.g. "This slide" vs "Whole case". */
export interface CohortScope {
  key: string
  label: string
  hashes: string[]
}

interface Props {
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Slides to add. Ignored when `scopes` is provided. */
  slideHashes?: string[]
  /** Optional radio choices; the first one is the default. */
  scopes?: CohortScope[]
  /** Line under the title. Defaults to "<n> slides selected." */
  description?: string
  /** Fired after slides land in a cohort. */
  onAdded?: (info: { cohortId: number; cohortName: string; added: number; hashes: string[] }) => void
}

interface CohortOption {
  id: number
  name: string
  slide_count?: number
}

/** Last cohort added to, so repeat adds are one click. */
const LAST_COHORT_KEY = 'slidecap_last_cohort_id'

function readLastCohort(): string {
  try {
    return localStorage.getItem(LAST_COHORT_KEY) || ''
  } catch {
    return ''
  }
}

/**
 * "Add to cohort" — shared by the Slide Library bulk bar, its per-row quick add
 * and the bulk case lookup. Creates a cohort or appends to an existing one; the
 * last cohort used is remembered so building one up over several searches is a
 * couple of clicks.
 */
export function AddToCohortDialog({ open, onOpenChange, slideHashes, scopes, description, onAdded }: Props) {
  const [mode, setMode] = useState<'new' | 'existing'>('new')
  const [name, setName] = useState('')
  const [desc, setDesc] = useState('')
  const [cohorts, setCohorts] = useState<CohortOption[]>([])
  const [cohortId, setCohortId] = useState('')
  const [scopeKey, setScopeKey] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState('')

  const hashes = scopes
    ? (scopes.find(s => s.key === scopeKey) || scopes[0])?.hashes || []
    : slideHashes || []

  // Load cohorts each time the dialog opens; counts drift as slides are added.
  useEffect(() => {
    if (!open) return
    setError('')
    setResult('')
    setName('')
    setDesc('')
    setBusy(false)
    // Empty means "whatever the first scope is" — the scope list can widen
    // after the caller finishes loading the rest of the case.
    setScopeKey('')

    const remembered = readLastCohort()
    let cancelled = false
    ;(async () => {
      try {
        const res = await fetch(`${getApiBase()}/cohorts`)
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data: CohortOption[] = await res.json()
        if (cancelled) return
        setCohorts(data)
        // Default to the last cohort used when it still exists — the common
        // case is adding to the same cohort again.
        const stillThere = data.some(c => String(c.id) === remembered)
        setCohortId(stillThere ? remembered : '')
        setMode(stillThere ? 'existing' : 'new')
      } catch (e) {
        if (cancelled) return
        console.error('Failed to load cohorts:', e)
        setError('Could not load existing cohorts.')
        setMode('new')
      }
    })()
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  const submit = async () => {
    if (hashes.length === 0) return
    setError('')
    setResult('')
    setBusy(true)
    try {
      let targetId: number
      let label: string

      if (mode === 'new') {
        if (!name.trim()) {
          setError('Cohort name is required.')
          return
        }
        const res = await fetch(`${getApiBase()}/cohorts`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: name.trim(),
            description: desc.trim() || null,
          }),
        })
        if (!res.ok) {
          const detail = await res.json().catch(() => null)
          throw new Error(detail?.detail || `Could not create cohort (${res.status})`)
        }
        const created = await res.json()
        targetId = created.id
        label = created.name
      } else {
        if (!cohortId) {
          setError('Pick a cohort to add to.')
          return
        }
        targetId = Number(cohortId)
        label = cohorts.find(c => c.id === targetId)?.name || 'cohort'
      }

      const res = await fetch(`${getApiBase()}/cohorts/${targetId}/slides`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: hashes }),
      })
      if (!res.ok) {
        const detail = await res.json().catch(() => null)
        throw new Error(detail?.detail || `Could not add slides (${res.status})`)
      }
      const data = await res.json()

      // The endpoint returns `added` as a count and skips slides already in the
      // cohort, so report what actually landed rather than how many were picked.
      const added: number = data.added ?? 0
      const notFound: number = (data.not_found || []).length
      const skipped = hashes.length - added - notFound
      setResult(
        `Added ${added} slide${added === 1 ? '' : 's'} to "${label}"` +
        (skipped > 0 ? ` — ${skipped} already in it` : '') +
        (notFound > 0 ? ` — ${notFound} not found` : '') + '.'
      )
      try { localStorage.setItem(LAST_COHORT_KEY, String(targetId)) } catch { /* private mode */ }
      onAdded?.({ cohortId: targetId, cohortName: label, added, hashes })
    } catch (e: any) {
      console.error('Add to cohort failed:', e)
      setError(e.message || 'Could not add slides to the cohort.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Add to Cohort</DialogTitle>
          <DialogDescription>
            {description || `${hashes.length} slide${hashes.length === 1 ? '' : 's'} selected.`}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          {scopes && scopes.length > 1 && (
            <div className="flex flex-wrap gap-2">
              {scopes.map(s => (
                <button
                  key={s.key}
                  type="button"
                  onClick={() => { setScopeKey(s.key); setResult('') }}
                  disabled={busy}
                  className={`rounded-md border px-2.5 py-1.5 text-[12px] transition-colors ${
                    (scopeKey || scopes[0].key) === s.key
                      ? 'border-primary bg-primary/5 font-medium'
                      : 'border-input hover:bg-muted/40'
                  }`}
                >
                  {s.label}
                  <span className="ml-1.5 text-muted-foreground">({s.hashes.length})</span>
                </button>
              ))}
            </div>
          )}

          <div className="flex gap-2">
            <button
              type="button"
              onClick={() => { setMode('new'); setError(''); setResult('') }}
              className={`flex-1 rounded-md border p-2.5 text-left transition-colors ${
                mode === 'new' ? 'border-primary bg-primary/5' : 'border-gray-300 hover:bg-muted/30'
              }`}
            >
              <div className="flex items-center gap-1.5 text-[13px] font-medium">
                <Plus className="h-3.5 w-3.5" />New cohort
              </div>
              <p className="mt-0.5 text-[11px] text-muted-foreground">Create one from this selection.</p>
            </button>
            <button
              type="button"
              onClick={() => { setMode('existing'); setError(''); setResult('') }}
              className={`flex-1 rounded-md border p-2.5 text-left transition-colors ${
                mode === 'existing' ? 'border-primary bg-primary/5' : 'border-gray-300 hover:bg-muted/30'
              }`}
            >
              <div className="flex items-center gap-1.5 text-[13px] font-medium">
                <Users className="h-3.5 w-3.5" />Existing cohort
              </div>
              <p className="mt-0.5 text-[11px] text-muted-foreground">Add to one you already have.</p>
            </button>
          </div>

          {mode === 'new' ? (
            <div className="space-y-3">
              <div className="space-y-1.5">
                <label className="text-sm font-medium">Cohort name</label>
                <Input
                  value={name}
                  onChange={e => setName(e.target.value)}
                  placeholder="e.g. GBM recurrence 2026"
                  disabled={busy}
                />
              </div>
              <div className="space-y-1.5">
                <label className="text-sm font-medium">Description (optional)</label>
                <Input
                  value={desc}
                  onChange={e => setDesc(e.target.value)}
                  placeholder="What is this cohort for?"
                  disabled={busy}
                />
              </div>
            </div>
          ) : (
            <div className="space-y-1.5">
              <label className="text-sm font-medium">Cohort</label>
              <SearchableSelect
                className="w-full h-10"
                value={cohortId}
                onChange={setCohortId}
                placeholder="Choose a cohort..."
                searchPlaceholder="Search cohorts..."
                emptyText="No cohorts yet"
                disabled={busy}
                options={cohorts.map(c => ({
                  value: String(c.id),
                  label: c.name,
                  hint: c.slide_count != null ? `${c.slide_count} slides` : undefined,
                }))}
              />
            </div>
          )}

          {error && (
            <div className="rounded-md border border-red-300 bg-red-50 p-2.5 text-[12px] text-red-700">{error}</div>
          )}
          {result && (
            <div className="rounded-md border border-emerald-300 bg-emerald-50 p-2.5 text-[12px] text-emerald-800">{result}</div>
          )}
        </div>

        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            {result ? 'Close' : 'Cancel'}
          </Button>
          <Button
            onClick={submit}
            disabled={busy || hashes.length === 0 || (mode === 'new' ? !name.trim() : !cohortId)}
          >
            {busy && <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />}
            {mode === 'new' ? 'Create & Add' : 'Add to Cohort'}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}
