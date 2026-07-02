import { useEffect, useMemo, useState } from 'react'
import { Copy, Loader2, Search } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { Slide } from '@/types/slide'
import { getApiBase, normalizeAccession } from '@/api'
import { displaySlide } from '@/lib/display'
import { copyToClipboard } from '@/lib/clipboard'

interface Props {
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Create mode (default): fired with the new cohort's id after successful
   *  create + slide-add. */
  onCreated?: (cohortId: number) => void
  /** When set, the dialog runs in "add" mode: resolved slides are added to this
   *  existing cohort instead of creating a new one. The name/description step is
   *  hidden. */
  targetCohortId?: number
  /** Add mode: fired with the number of slides submitted after a successful add. */
  onAdded?: (addedCount: number) => void
}

interface ResolveResult {
  /** Every exact-match slide returned by /search for at least one pasted accession. */
  slides: Slide[]
  /** Accessions that returned zero exact matches — useful for "what's still missing". */
  notFound: string[]
  /** Total distinct accessions parsed from the paste box. */
  totalAccessions: number
}

const STAIN_HE_VARIANTS = new Set(['he', 'h&e', 'hne', 'h_e'])

/**
 * Build a cohort from a pasted list of accession numbers, with live filtering
 * by year / stain / "only-scanned" before commit. Mirrors the SlidePull paste
 * pattern but lands in a cohort instead of a pull list.
 *
 * Resolve runs one /search per accession (same as SlidePull) — N small calls
 * is fine for the typical cohort size (≤ a few hundred accessions). For
 * substantially bigger lists a batch-resolve endpoint would be worth adding.
 */
export function CohortFromPasteDialog({ open, onOpenChange, onCreated, targetCohortId, onAdded }: Props) {
  const addMode = targetCohortId != null
  // ── Paste + resolve state ────────────────────────────────────────
  const [pasteText, setPasteText] = useState('')
  const [resolving, setResolving] = useState(false)
  const [resolveProgress, setResolveProgress] = useState<{ done: number; total: number } | null>(null)
  const [resolved, setResolved] = useState<ResolveResult | null>(null)

  // ── Filters ──────────────────────────────────────────────────────
  const [filterYears, setFilterYears] = useState<Set<number>>(new Set())
  const [filterStains, setFilterStains] = useState<Set<string>>(new Set())
  const [onlyScanned, setOnlyScanned] = useState(true)
  const [hneOnly, setHneOnly] = useState(false)
  /** "First slide per case" common cohort-building pattern. When ON, after
   *  filters narrow down the list, we auto-select just one slide per case
   *  (the first by block_id+slide_number). User can still toggle individual
   *  rows. */
  const [onePerCase, setOnePerCase] = useState(false)

  // ── Selection + commit ───────────────────────────────────────────
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [cohortName, setCohortName] = useState('')
  const [cohortDesc, setCohortDesc] = useState('')
  const [creating, setCreating] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Reset everything when the dialog closes
  useEffect(() => {
    if (!open) {
      setPasteText(''); setResolving(false); setResolveProgress(null); setResolved(null)
      setFilterYears(new Set()); setFilterStains(new Set())
      setOnlyScanned(true); setHneOnly(false); setOnePerCase(false)
      setSelected(new Set()); setCohortName(''); setCohortDesc('')
      setCreating(false); setError(null)
    }
  }, [open])

  // ── Resolve ──────────────────────────────────────────────────────
  const handleResolve = async () => {
    const lines = pasteText.split(/[\n,;]+/).map(s => s.trim()).filter(Boolean)
    const accessions = Array.from(new Set(lines.map(normalizeAccession)))
    if (accessions.length === 0) return
    setResolving(true)
    setError(null)
    setResolved(null)
    setResolveProgress({ done: 0, total: accessions.length })

    const allSlides: Slide[] = []
    const notFound: string[] = []
    for (let i = 0; i < accessions.length; i++) {
      const q = accessions[i]
      try {
        const res = await fetch(`${getApiBase()}/search?q=${encodeURIComponent(q)}&limit=100`)
        if (res.ok) {
          const data = await res.json()
          const exact = (data.results || []).filter(
            (s: Slide) => normalizeAccession(s.accession_number || '') === q,
          )
          if (exact.length > 0) {
            allSlides.push(...exact)
          } else {
            notFound.push(q)
          }
        } else {
          notFound.push(q)
        }
      } catch {
        notFound.push(q)
      }
      setResolveProgress({ done: i + 1, total: accessions.length })
    }
    setResolved({ slides: allSlides, notFound, totalAccessions: accessions.length })
    setResolving(false)
    setResolveProgress(null)
  }

  // ── Derived: available filter values + filtered slide list ──────
  const availableYears = useMemo(() => {
    if (!resolved) return []
    const ys = new Set<number>()
    for (const s of resolved.slides) if (s.year) ys.add(s.year)
    return Array.from(ys).sort((a, b) => b - a)
  }, [resolved])

  const availableStains = useMemo(() => {
    if (!resolved) return []
    const ss = new Set<string>()
    for (const s of resolved.slides) if (s.stain_type) ss.add(s.stain_type)
    return Array.from(ss).sort()
  }, [resolved])

  const filteredSlides = useMemo(() => {
    if (!resolved) return [] as Slide[]
    let out = resolved.slides
    if (filterYears.size > 0) out = out.filter(s => s.year != null && filterYears.has(s.year))
    if (filterStains.size > 0) out = out.filter(s => filterStains.has(s.stain_type))
    if (hneOnly) out = out.filter(s => STAIN_HE_VARIANTS.has(s.stain_type.toLowerCase()))
    return out
  }, [resolved, filterYears, filterStains, hneOnly])

  // Whenever the filtered list changes, default selection = all filtered
  // (or one-per-case if that toggle is on). User edits override per-row.
  useEffect(() => {
    if (filteredSlides.length === 0) { setSelected(new Set()); return }
    if (onePerCase) {
      const seen = new Set<string>()
      const pick = new Set<string>()
      // Sort by case + block + slide# for deterministic "first" pick
      const sorted = [...filteredSlides].sort((a, b) => {
        const ca = a.case_hash || a.accession_number || ''
        const cb = b.case_hash || b.accession_number || ''
        if (ca !== cb) return ca.localeCompare(cb)
        if (a.block_id !== b.block_id) return a.block_id.localeCompare(b.block_id)
        return (a.slide_number || '').localeCompare(b.slide_number || '')
      })
      for (const s of sorted) {
        const k = s.case_hash || s.accession_number || ''
        if (!seen.has(k)) { seen.add(k); pick.add(s.slide_hash) }
      }
      setSelected(pick)
    } else {
      setSelected(new Set(filteredSlides.map(s => s.slide_hash)))
    }
  }, [filteredSlides, onePerCase])

  const toggleSlide = (hash: string) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(hash)) next.delete(hash); else next.add(hash)
      return next
    })
  }

  const toggleYear = (y: number) => {
    setFilterYears(prev => {
      const next = new Set(prev)
      if (next.has(y)) next.delete(y); else next.add(y)
      return next
    })
  }
  const toggleStain = (st: string) => {
    setFilterStains(prev => {
      const next = new Set(prev)
      if (next.has(st)) next.delete(st); else next.add(st)
      return next
    })
  }

  // ── Group filtered slides by case for the preview table ─────────
  type Group = { caseKey: string; label: string; year?: number; slides: Slide[] }
  const groups: Group[] = useMemo(() => {
    const byCase = new Map<string, Group>()
    for (const s of filteredSlides) {
      const k = s.case_hash || s.accession_number || s.slide_hash
      if (!byCase.has(k)) {
        byCase.set(k, {
          caseKey: k,
          label: s.case_id || s.accession_number || s.slide_hash.slice(0, 12),
          year: s.year,
          slides: [],
        })
      }
      byCase.get(k)!.slides.push(s)
    }
    return Array.from(byCase.values()).sort((a, b) => a.label.localeCompare(b.label))
  }, [filteredSlides])

  // ── Copy unresolved → clipboard ─────────────────────────────────
  const copyUnresolved = async () => {
    if (!resolved?.notFound?.length) return
    await copyToClipboard(resolved.notFound.join('\n'))
  }

  // ── Commit: add to existing cohort, or create cohort + add slides ──
  const handleCommit = async () => {
    if (selected.size === 0) return

    // Add mode: just push the selected slides into the existing cohort.
    if (addMode) {
      setCreating(true)
      setError(null)
      try {
        const addRes = await fetch(`${getApiBase()}/cohorts/${targetCohortId}/slides`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ slide_hashes: Array.from(selected) }),
        })
        if (!addRes.ok) {
          const detail = await addRes.json().catch(() => ({}))
          throw new Error(detail.detail || `HTTP ${addRes.status}`)
        }
        onAdded?.(selected.size)
        onOpenChange(false)
      } catch (e: any) {
        setError(e.message || 'Failed to add slides')
      } finally {
        setCreating(false)
      }
      return
    }

    if (!cohortName.trim()) return
    setCreating(true)
    setError(null)
    try {
      const createRes = await fetch(`${getApiBase()}/cohorts`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: cohortName.trim(),
          description: cohortDesc.trim() || undefined,
          source_type: 'manual',
          source_details: JSON.stringify({
            built_from: 'paste',
            pasted_accessions: resolved?.totalAccessions,
            not_found: resolved?.notFound?.length ?? 0,
            filters: {
              years: Array.from(filterYears),
              stains: Array.from(filterStains),
              only_scanned: onlyScanned,
              hne_only: hneOnly,
              one_per_case: onePerCase,
            },
          }),
        }),
      })
      if (!createRes.ok) {
        const detail = await createRes.json().catch(() => ({}))
        throw new Error(detail.detail || `HTTP ${createRes.status}`)
      }
      const newCohort = await createRes.json()
      const cohortId = newCohort.id

      const addRes = await fetch(`${getApiBase()}/cohorts/${cohortId}/slides`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: Array.from(selected) }),
      })
      if (!addRes.ok) {
        const detail = await addRes.json().catch(() => ({}))
        throw new Error(detail.detail || `HTTP ${addRes.status}`)
      }

      onCreated?.(cohortId)
      onOpenChange(false)
    } catch (e: any) {
      setError(e.message || 'Failed to create cohort')
    } finally {
      setCreating(false)
    }
  }

  // ── Render ──────────────────────────────────────────────────────
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[88vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>
            {addMode ? 'Add slides from accession list' : 'Create cohort from accession list'}
          </DialogTitle>
          <DialogDescription>
            Paste a list of accession numbers (one per line; commas / semicolons also OK),
            resolve to slides, then filter and pick which to {addMode ? 'add' : 'include'}.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto space-y-4 pr-1">
          {/* Step 1 — paste box */}
          {!resolved && (
            <div className="space-y-2">
              <label className="text-sm font-medium">Accessions</label>
              <textarea
                className="w-full min-h-[140px] rounded-md border bg-background px-3 py-2 text-sm font-mono"
                placeholder={'BS22-D76390\nBS23-W44115\nBS20-E02024'}
                value={pasteText}
                onChange={(e) => setPasteText(e.target.value)}
                disabled={resolving}
              />
              <div className="flex items-center justify-between">
                <span className="text-xs text-muted-foreground">
                  {pasteText.split(/[\n,;]+/).filter(s => s.trim()).length} entries
                </span>
                <Button onClick={handleResolve} disabled={resolving || !pasteText.trim()}>
                  {resolving ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      Resolving {resolveProgress ? `(${resolveProgress.done}/${resolveProgress.total})` : '…'}
                    </>
                  ) : (
                    <><Search className="mr-2 h-4 w-4" /> Resolve</>
                  )}
                </Button>
              </div>
            </div>
          )}

          {/* Step 2 — filters + preview */}
          {resolved && (
            <>
              <div className="rounded-md border bg-muted/30 px-3 py-2 flex flex-wrap items-center gap-3 text-xs">
                <span><span className="font-medium">{resolved.totalAccessions}</span> pasted</span>
                <span className="text-muted-foreground">·</span>
                <span><span className="font-medium">{resolved.totalAccessions - resolved.notFound.length}</span> in system</span>
                <span className="text-muted-foreground">·</span>
                <span><span className="font-medium">{resolved.slides.length}</span> slides total</span>
                <span className="text-muted-foreground">·</span>
                <span className="text-green-700"><span className="font-medium">{filteredSlides.length}</span> match filters</span>
                <span className="text-muted-foreground">·</span>
                <span><span className="font-medium">{selected.size}</span> selected</span>
                <Button variant="ghost" size="sm" className="ml-auto h-7 text-xs"
                  onClick={() => { setResolved(null); setPasteText('') }}>
                  Start over
                </Button>
              </div>

              {/* Filter row */}
              <div className="space-y-2">
                {availableYears.length > 0 && (
                  <div className="flex flex-wrap items-center gap-1.5">
                    <span className="text-xs text-muted-foreground mr-1">Year:</span>
                    {availableYears.map(y => (
                      <button
                        key={y}
                        type="button"
                        onClick={() => toggleYear(y)}
                        className={`text-xs rounded px-2 py-0.5 border transition-colors ${
                          filterYears.has(y)
                            ? 'border-foreground bg-foreground text-background'
                            : 'border-input hover:bg-muted'
                        }`}
                      >
                        {y}
                      </button>
                    ))}
                  </div>
                )}
                {availableStains.length > 0 && (
                  <div className="flex flex-wrap items-center gap-1.5">
                    <span className="text-xs text-muted-foreground mr-1">Stain:</span>
                    {availableStains.map(st => (
                      <button
                        key={st}
                        type="button"
                        onClick={() => toggleStain(st)}
                        className={`text-xs rounded px-2 py-0.5 border transition-colors ${
                          filterStains.has(st)
                            ? 'border-foreground bg-foreground text-background'
                            : 'border-input hover:bg-muted'
                        }`}
                      >
                        {st}
                      </button>
                    ))}
                  </div>
                )}
                <div className="flex flex-wrap items-center gap-4 pt-1">
                  <label className="flex items-center gap-2 text-xs">
                    <Checkbox checked={onlyScanned} onCheckedChange={(c) => setOnlyScanned(!!c)} />
                    <span title="When off, the unresolved accessions are shown below for reference. Slides in the system are always the only ones added to the cohort.">
                      Only scanned cases
                    </span>
                  </label>
                  <label className="flex items-center gap-2 text-xs">
                    <Checkbox checked={hneOnly} onCheckedChange={(c) => setHneOnly(!!c)} />
                    <span>H&amp;E only</span>
                  </label>
                  <label className="flex items-center gap-2 text-xs">
                    <Checkbox checked={onePerCase} onCheckedChange={(c) => setOnePerCase(!!c)} />
                    <span>1 slide per case</span>
                  </label>
                </div>
              </div>

              {/* Unresolved accessions (only when toggle is off) */}
              {!onlyScanned && resolved.notFound.length > 0 && (
                <div className="rounded-md border border-orange-200 bg-orange-50 dark:border-orange-900 dark:bg-orange-950 px-3 py-2 space-y-1">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-medium text-orange-800 dark:text-orange-300">
                      {resolved.notFound.length} not in system
                    </span>
                    <Button variant="ghost" size="sm" className="h-6 text-xs" onClick={copyUnresolved}>
                      <Copy className="h-3 w-3 mr-1" /> Copy
                    </Button>
                  </div>
                  <p className="text-[11px] font-mono text-orange-700 dark:text-orange-400 break-all">
                    {resolved.notFound.slice(0, 12).join(', ')}
                    {resolved.notFound.length > 12 ? ` … +${resolved.notFound.length - 12} more` : ''}
                  </p>
                </div>
              )}

              {/* Preview list — grouped by case */}
              <div className="border rounded-md max-h-[40vh] overflow-y-auto divide-y">
                {groups.length === 0 ? (
                  <p className="text-sm text-muted-foreground text-center py-8">
                    No slides match the current filters.
                  </p>
                ) : (
                  groups.map(g => (
                    <div key={g.caseKey} className="px-3 py-2">
                      <div className="text-xs font-medium mb-1">
                        {g.label} {g.year && <span className="text-muted-foreground">({g.year})</span>}
                        <span className="ml-2 text-muted-foreground">
                          {g.slides.filter(s => selected.has(s.slide_hash)).length}/{g.slides.length}
                        </span>
                      </div>
                      <div className="space-y-0.5">
                        {g.slides.map(s => (
                          <label key={s.slide_hash}
                            className="flex items-center gap-2 text-xs pl-4 py-0.5 hover:bg-muted/40 cursor-pointer rounded">
                            <Checkbox
                              checked={selected.has(s.slide_hash)}
                              onCheckedChange={() => toggleSlide(s.slide_hash)}
                            />
                            <span className="font-mono text-muted-foreground w-12 shrink-0">{s.block_id}</span>
                            <Badge variant="outline" className="text-[10px] h-5 px-1.5">{s.stain_type}</Badge>
                            <span className="text-muted-foreground">{displaySlide(s)}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  ))
                )}
              </div>

              {/* Cohort name + commit */}
              <div className="space-y-2 pt-2 border-t">
                {!addMode && (
                <div className="grid grid-cols-2 gap-3">
                  <div className="space-y-1">
                    <label className="text-xs font-medium">Cohort name</label>
                    <Input
                      value={cohortName}
                      onChange={(e) => setCohortName(e.target.value)}
                      placeholder="e.g. Q1 paste batch"
                    />
                  </div>
                  <div className="space-y-1">
                    <label className="text-xs font-medium">Description (optional)</label>
                    <Input
                      value={cohortDesc}
                      onChange={(e) => setCohortDesc(e.target.value)}
                      placeholder="What is this cohort?"
                    />
                  </div>
                </div>
                )}
                {error && <p className="text-xs text-red-600">{error}</p>}
              </div>
            </>
          )}
        </div>

        {/* Footer */}
        {resolved && (
          <div className="flex justify-end gap-2 pt-3 border-t">
            <Button variant="ghost" onClick={() => onOpenChange(false)} disabled={creating}>
              Cancel
            </Button>
            <Button
              onClick={handleCommit}
              disabled={creating || selected.size === 0 || (!addMode && !cohortName.trim())}
            >
              {creating ? (
                <><Loader2 className="mr-2 h-4 w-4 animate-spin" /> {addMode ? 'Adding…' : 'Creating…'}</>
              ) : addMode ? (
                <>Add {selected.size} slide{selected.size === 1 ? '' : 's'} to cohort</>
              ) : (
                <>Create cohort with {selected.size} slide{selected.size === 1 ? '' : 's'}</>
              )}
            </Button>
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}
