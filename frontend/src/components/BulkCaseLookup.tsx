import { useEffect, useMemo, useRef, useState } from 'react'
import {
  ChevronDown, ChevronRight, Loader2, Search, Tags as TagsIcon, Users, FileDown, XCircle, CheckCircle2,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { TagChip } from '@/components/ui/TagChip'
import { CopyButton } from '@/components/ui/CopyButton'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { AddToCohortDialog } from '@/components/AddToCohortDialog'
import type { Slide, Tag } from '@/types/slide'
import { getApiBase, normalizeAccession } from '@/api'
import { displayCase, displaySlide } from '@/lib/display'
import { saveBlob } from '@/lib/download'

interface Props {
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Fired after slides land in a cohort, so the caller can refresh/navigate. */
  onAddedToCohort?: (cohortId: number, cohortName: string) => void
}

/** One pasted accession and whatever the index has for it. */
interface LookupCase {
  accession: string
  slides: Slide[]
  /** The lookup itself failed (network/server), so "not scanned" isn't known. */
  errored: boolean
}

const STAIN_HE_VARIANTS = new Set(['he', 'h&e', 'hne', 'h_e'])

const PRESET_COLORS = [
  '#EF4444', '#F97316', '#EAB308', '#22C55E', '#14B8A6',
  '#3B82F6', '#8B5CF6', '#EC4899', '#6B7280',
]

/** Run `fn` over `items` with at most `limit` in flight, preserving order. */
async function mapPool<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length)
  let next = 0
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const i = next++
      if (i >= items.length) return
      out[i] = await fn(items[i])
    }
  })
  await Promise.all(workers)
  return out
}

/**
 * Bulk case lookup: paste a list of accessions and see which ones actually have
 * slides scanned into SlideCap, then act on the hits — tag them, drop them into
 * an existing cohort, or create a new one. The misses stay listed so the list
 * can be handed back to whoever needs to pull those cases.
 *
 * Resolution is one /search per accession (the index is in memory, so these are
 * cheap) with a small concurrency pool; a batch endpoint would only be worth it
 * for lists in the thousands.
 */
export function BulkCaseLookup({ open, onOpenChange, onAddedToCohort }: Props) {
  const [pasteText, setPasteText] = useState('')
  const [resolving, setResolving] = useState(false)
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null)
  const [cases, setCases] = useState<LookupCase[] | null>(null)
  const cancelRef = useRef(false)

  // View + filters
  const [view, setView] = useState<'all' | 'found' | 'missing'>('all')
  const [filterText, setFilterText] = useState('')
  const [hneOnly, setHneOnly] = useState(false)
  const [expanded, setExpanded] = useState<Set<string>>(new Set())

  // Selection (slide level; case rows tick all their slides)
  const [selected, setSelected] = useState<Set<string>>(new Set())

  // Actions
  const [cohortOpen, setCohortOpen] = useState(false)
  const [tagOpen, setTagOpen] = useState(false)
  const [tagName, setTagName] = useState('')
  const [tagColor, setTagColor] = useState(PRESET_COLORS[0])
  const [allTags, setAllTags] = useState<Tag[]>([])
  const [tagging, setTagging] = useState(false)
  const [actionMsg, setActionMsg] = useState('')
  const [actionError, setActionError] = useState('')

  useEffect(() => {
    if (open) return
    // Reset on close so the next open starts clean.
    cancelRef.current = true
    setPasteText(''); setResolving(false); setProgress(null); setCases(null)
    setView('all'); setFilterText(''); setHneOnly(false); setExpanded(new Set())
    setSelected(new Set()); setActionMsg(''); setActionError('')
    setTagOpen(false); setTagName('')
  }, [open])

  useEffect(() => {
    if (!open) return
    fetch(`${getApiBase()}/tags`)
      .then(r => (r.ok ? r.json() : []))
      .then(setAllTags)
      .catch(() => setAllTags([]))
  }, [open])

  // ── Resolve ───────────────────────────────────────────────────────
  const pastedCount = pasteText.split(/[\n,;\t]+/).filter(s => s.trim()).length

  const handleResolve = async () => {
    const lines = pasteText.split(/[\n,;\t]+/).map(s => s.trim()).filter(Boolean)
    const accessions = Array.from(new Set(lines.map(normalizeAccession)))
    if (accessions.length === 0) return
    cancelRef.current = false
    setResolving(true)
    setCases(null)
    setActionMsg(''); setActionError('')
    setProgress({ done: 0, total: accessions.length })

    let done = 0
    const results = await mapPool(accessions, 6, async (q): Promise<LookupCase> => {
      if (cancelRef.current) return { accession: q, slides: [], errored: true }
      let slides: Slide[] = []
      let errored = false
      try {
        const res = await fetch(
          `${getApiBase()}/search?q=${encodeURIComponent(q)}&limit=500&external=include`,
        )
        if (res.ok) {
          const data = await res.json()
          slides = (data.results || []).filter(
            (s: Slide) => normalizeAccession(s.accession_number || '') === q,
          )
        } else {
          errored = true
        }
      } catch {
        errored = true
      }
      done += 1
      setProgress({ done, total: accessions.length })
      return { accession: q, slides, errored }
    })

    if (cancelRef.current) { setResolving(false); setProgress(null); return }
    setCases(results)
    setResolving(false)
    setProgress(null)
    // Start with every scanned slide ticked — the usual next step is to act on
    // all the hits.
    setSelected(new Set(results.flatMap(c => c.slides.map(s => s.slide_hash))))
  }

  // ── Derived views ─────────────────────────────────────────────────
  const stats = useMemo(() => {
    if (!cases) return null
    const found = cases.filter(c => c.slides.length > 0)
    const errored = cases.filter(c => c.errored)
    return {
      total: cases.length,
      found: found.length,
      missing: cases.length - found.length - errored.length,
      errored: errored.length,
      slides: found.reduce((n, c) => n + c.slides.length, 0),
    }
  }, [cases])

  /** Slides shown for a case, after the H&E filter. */
  const visibleSlides = (c: LookupCase) =>
    hneOnly ? c.slides.filter(s => STAIN_HE_VARIANTS.has((s.stain_type || '').toLowerCase())) : c.slides

  const shown = useMemo(() => {
    if (!cases) return [] as LookupCase[]
    const needle = filterText.trim().toUpperCase()
    return cases.filter(c => {
      const has = visibleSlides(c).length > 0
      if (view === 'found' && !has) return false
      if (view === 'missing' && has) return false
      if (needle && !c.accession.toUpperCase().includes(needle)) return false
      return true
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cases, view, filterText, hneOnly])

  const shownSlideHashes = useMemo(
    () => shown.flatMap(c => visibleSlides(c).map(s => s.slide_hash)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [shown, hneOnly],
  )
  const selectedShown = shownSlideHashes.filter(h => selected.has(h)).length
  const allShownSelected = shownSlideHashes.length > 0 && selectedShown === shownSlideHashes.length

  const missingList = useMemo(
    () => (cases || []).filter(c => c.slides.length === 0 && !c.errored).map(c => c.accession),
    [cases],
  )

  const toggleSlide = (hash: string) => setSelected(prev => {
    const next = new Set(prev)
    if (next.has(hash)) next.delete(hash); else next.add(hash)
    return next
  })

  const toggleCase = (c: LookupCase) => setSelected(prev => {
    const next = new Set(prev)
    const slides = visibleSlides(c)
    const all = slides.length > 0 && slides.every(s => next.has(s.slide_hash))
    slides.forEach(s => (all ? next.delete(s.slide_hash) : next.add(s.slide_hash)))
    return next
  })

  const toggleAllShown = () => setSelected(prev => {
    const next = new Set(prev)
    if (allShownSelected) shownSlideHashes.forEach(h => next.delete(h))
    else shownSlideHashes.forEach(h => next.add(h))
    return next
  })

  /** Keep one slide per case — the first by block + slide number. */
  const pickOnePerCase = () => {
    const pick = new Set<string>()
    for (const c of shown) {
      const slides = [...visibleSlides(c)].sort((a, b) =>
        (a.block_id || '').localeCompare(b.block_id || '') ||
        (a.slide_number || '').localeCompare(b.slide_number || ''),
      )
      if (slides[0]) pick.add(slides[0].slide_hash)
    }
    setSelected(pick)
  }

  const toggleExpand = (accession: string) => setExpanded(prev => {
    const next = new Set(prev)
    if (next.has(accession)) next.delete(accession); else next.add(accession)
    return next
  })

  // ── Bulk tag ──────────────────────────────────────────────────────
  const applyTag = async (name: string, color: string) => {
    const hashes = Array.from(selected)
    if (!name.trim() || hashes.length === 0) return
    setTagging(true)
    setActionError(''); setActionMsg('')
    try {
      const res = await fetch(`${getApiBase()}/slides/bulk/tags/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: hashes, tags: [name.trim()], color }),
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      // Reflect the new tag locally so the table matches without a re-resolve.
      setCases(prev => prev && prev.map(c => ({
        ...c,
        slides: c.slides.map(s =>
          selected.has(s.slide_hash) && !(s.slide_tags || []).includes(name.trim())
            ? { ...s, slide_tags: [...(s.slide_tags || []), name.trim()] }
            : s,
        ),
      })))
      setActionMsg(`Tagged ${data.updated ?? hashes.length} slide${(data.updated ?? hashes.length) === 1 ? '' : 's'} with "${name.trim()}".`)
      setTagOpen(false)
      setTagName('')
      fetch(`${getApiBase()}/tags`).then(r => (r.ok ? r.json() : [])).then(setAllTags).catch(() => {})
    } catch (e: any) {
      console.error('Bulk tag failed:', e)
      setActionError('Could not tag the selected slides.')
    } finally {
      setTagging(false)
    }
  }

  // ── CSV ───────────────────────────────────────────────────────────
  const exportCsv = () => {
    if (!cases) return
    const rows = [['case', 'status', 'slides', 'stains', 'year']]
    for (const c of cases) {
      const slides = c.slides
      const status = c.errored ? 'lookup failed' : slides.length > 0 ? 'scanned' : 'not in SlideCap'
      const stains = Array.from(new Set(slides.map(s => s.stain_type).filter(Boolean))).join(' ')
      const year = slides.find(s => s.year)?.year ?? ''
      rows.push([
        slides[0] ? displayCase({ ...slides[0], case_id: slides[0].case_id }) : c.accession,
        status,
        String(slides.length),
        stains,
        String(year),
      ])
    }
    const csv = rows.map(r => r.map(v => (/[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v)).join(',')).join('\n')
    saveBlob(new Blob([csv], { type: 'text/csv' }), 'bulk-case-lookup.csv')
  }

  const tagSuggestions = allTags
    .filter(t => tagName.trim() && t.name.toLowerCase().includes(tagName.trim().toLowerCase()))
    .slice(0, 6)

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="max-w-4xl max-h-[88vh] overflow-hidden flex flex-col">
          <DialogHeader>
            <DialogTitle>Bulk case lookup</DialogTitle>
            <DialogDescription>
              Paste a list of accessions to see which cases have slides scanned into SlideCap,
              then tag the hits or add them to a cohort.
            </DialogDescription>
          </DialogHeader>

          <div className="flex-1 overflow-y-auto space-y-4 pr-1">
            {/* Step 1 — paste */}
            {!cases && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Accessions</label>
                <textarea
                  className="w-full min-h-[160px] rounded-md border bg-background px-3 py-2 text-sm font-mono"
                  placeholder={'One per line — commas, semicolons and tabs also work'}
                  value={pasteText}
                  onChange={e => setPasteText(e.target.value)}
                  disabled={resolving}
                />
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">{pastedCount} entries</span>
                  <Button onClick={handleResolve} disabled={resolving || !pasteText.trim()}>
                    {resolving ? (
                      <>
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        Looking up {progress ? `(${progress.done}/${progress.total})` : '…'}
                      </>
                    ) : (
                      <><Search className="mr-2 h-4 w-4" /> Look up</>
                    )}
                  </Button>
                </div>
              </div>
            )}

            {/* Step 2 — results */}
            {cases && stats && (
              <>
                <div className="rounded-md border bg-muted/30 px-3 py-2 flex flex-wrap items-center gap-3 text-xs">
                  <span><span className="font-medium">{stats.total}</span> cases</span>
                  <span className="text-muted-foreground">·</span>
                  <span className="text-emerald-700 dark:text-emerald-400">
                    <span className="font-medium">{stats.found}</span> scanned ({stats.slides} slides)
                  </span>
                  <span className="text-muted-foreground">·</span>
                  <span className="text-orange-700 dark:text-orange-400">
                    <span className="font-medium">{stats.missing}</span> not in SlideCap
                  </span>
                  {stats.errored > 0 && (
                    <>
                      <span className="text-muted-foreground">·</span>
                      <span className="text-red-600"><span className="font-medium">{stats.errored}</span> lookup failed</span>
                    </>
                  )}
                  <Button variant="ghost" size="sm" className="ml-auto h-7 text-xs"
                    onClick={() => { setCases(null); setSelected(new Set()); setActionMsg(''); setActionError('') }}>
                    Start over
                  </Button>
                </div>

                {/* View + filters */}
                <div className="flex flex-wrap items-center gap-2">
                  <div className="flex rounded-md border overflow-hidden">
                    {([['all', 'All'], ['found', 'Scanned'], ['missing', 'Not found']] as const).map(([k, label]) => (
                      <button
                        key={k}
                        type="button"
                        onClick={() => setView(k)}
                        className={`px-2.5 py-1 text-xs transition-colors ${
                          view === k ? 'bg-foreground text-background' : 'hover:bg-muted'
                        }`}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                  <Input
                    value={filterText}
                    onChange={e => setFilterText(e.target.value)}
                    placeholder="Filter this list…"
                    className="h-8 w-48 text-xs"
                  />
                  <label className="flex items-center gap-2 text-xs">
                    <Checkbox checked={hneOnly} onCheckedChange={c => setHneOnly(!!c)} />
                    <span>H&amp;E only</span>
                  </label>
                  <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={pickOnePerCase}>
                    Pick 1 per case
                  </Button>
                  {missingList.length > 0 && (
                    <CopyButton
                      value={missingList.join('\n')}
                      className="h-7 rounded-md border px-2 text-xs hover:bg-muted"
                      iconClassName="h-3 w-3"
                      label={`Copy ${missingList.length} not found`}
                    />
                  )}
                  <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={exportCsv}>
                    <FileDown className="mr-1 h-3.5 w-3.5" /> CSV
                  </Button>
                </div>

                {actionMsg && (
                  <div className="rounded-md border border-emerald-300 bg-emerald-50 px-3 py-2 text-[12px] text-emerald-800">{actionMsg}</div>
                )}
                {actionError && (
                  <div className="rounded-md border border-red-300 bg-red-50 px-3 py-2 text-[12px] text-red-700">{actionError}</div>
                )}

                {/* Case list */}
                <div className="border rounded-md max-h-[42vh] overflow-y-auto divide-y">
                  <label className="sticky top-0 z-10 flex items-center gap-2 bg-muted/60 backdrop-blur px-3 py-1.5 text-xs font-medium cursor-pointer">
                    <Checkbox
                      checked={allShownSelected ? true : selectedShown > 0 ? 'indeterminate' : false}
                      onCheckedChange={toggleAllShown}
                      disabled={shownSlideHashes.length === 0}
                    />
                    <span>Select all shown</span>
                    <span className="ml-auto font-normal text-muted-foreground">
                      {selected.size} slide{selected.size === 1 ? '' : 's'} selected
                    </span>
                  </label>

                  {shown.length === 0 ? (
                    <p className="text-sm text-muted-foreground text-center py-8">Nothing matches this view.</p>
                  ) : shown.map(c => {
                    const slides = visibleSlides(c)
                    const picked = slides.filter(s => selected.has(s.slide_hash)).length
                    const isOpen = expanded.has(c.accession)
                    const stains = Array.from(new Set(slides.map(s => s.stain_type).filter(Boolean)))
                    const tags = Array.from(new Set(slides.flatMap(s => s.slide_tags || [])))
                    return (
                      <div key={c.accession} className="px-3 py-1.5">
                        <div className="flex items-center gap-2 text-xs">
                          <Checkbox
                            checked={picked > 0 && picked === slides.length ? true : picked > 0 ? 'indeterminate' : false}
                            onCheckedChange={() => toggleCase(c)}
                            disabled={slides.length === 0}
                          />
                          {slides.length > 0 ? (
                            <button type="button" onClick={() => toggleExpand(c.accession)} className="text-muted-foreground hover:text-foreground">
                              {isOpen ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                            </button>
                          ) : (
                            <span className="w-3.5" />
                          )}
                          <span className="font-medium">
                            {slides[0] ? displayCase({ ...slides[0] }) : c.accession}
                          </span>
                          {slides[0]?.year && <span className="text-muted-foreground">({slides[0].year})</span>}

                          {c.errored ? (
                            <Badge variant="outline" className="h-5 px-1.5 text-[10px] border-red-300 text-red-700">
                              lookup failed
                            </Badge>
                          ) : slides.length > 0 ? (
                            <span className="inline-flex items-center gap-1 text-emerald-700 dark:text-emerald-400">
                              <CheckCircle2 className="h-3.5 w-3.5" />
                              {slides.length} slide{slides.length === 1 ? '' : 's'}
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 text-orange-700 dark:text-orange-400">
                              <XCircle className="h-3.5 w-3.5" />
                              {c.slides.length > 0 ? 'no H&E' : 'not in SlideCap'}
                            </span>
                          )}

                          <div className="ml-auto flex items-center gap-1">
                            {stains.slice(0, 4).map(st => (
                              <Badge key={st} variant="outline" className="h-5 px-1.5 text-[10px]">{st}</Badge>
                            ))}
                            {stains.length > 4 && <span className="text-muted-foreground">+{stains.length - 4}</span>}
                            {tags.slice(0, 2).map(t => (
                              <TagChip key={t} name={t} color={allTags.find(at => at.name === t)?.color || undefined} />
                            ))}
                          </div>
                        </div>

                        {isOpen && slides.length > 0 && (
                          <div className="space-y-0.5 pt-1">
                            {slides.map(s => (
                              <label key={s.slide_hash}
                                className="flex items-center gap-2 text-xs pl-8 py-0.5 hover:bg-muted/40 cursor-pointer rounded">
                                <Checkbox
                                  checked={selected.has(s.slide_hash)}
                                  onCheckedChange={() => toggleSlide(s.slide_hash)}
                                />
                                <span className="font-mono text-muted-foreground w-12 shrink-0">{s.block_id}</span>
                                <Badge variant="outline" className="text-[10px] h-5 px-1.5">{s.stain_type}</Badge>
                                <span className="text-muted-foreground">{displaySlide(s)}</span>
                                {s.completed_analyses && s.completed_analyses.length > 0 && (
                                  <span className="ml-auto text-[10px] text-muted-foreground">
                                    {s.completed_analyses.join(', ')}
                                  </span>
                                )}
                              </label>
                            ))}
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </>
            )}
          </div>

          {/* Footer actions */}
          {cases && (
            <div className="flex items-center gap-2 pt-3 border-t">
              <span className="text-xs text-muted-foreground">
                {selected.size} slide{selected.size === 1 ? '' : 's'} selected
              </span>
              <div className="ml-auto flex items-center gap-2">
                <Button variant="ghost" onClick={() => onOpenChange(false)}>Close</Button>
                <Button
                  variant="outline"
                  disabled={selected.size === 0}
                  onClick={() => { setTagOpen(true); setActionMsg(''); setActionError('') }}
                >
                  <TagsIcon className="mr-1.5 h-4 w-4" /> Add tag
                </Button>
                <Button
                  disabled={selected.size === 0}
                  onClick={() => { setCohortOpen(true); setActionMsg(''); setActionError('') }}
                >
                  <Users className="mr-1.5 h-4 w-4" /> Add to cohort
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>

      {/* Tag the selection */}
      <Dialog open={tagOpen} onOpenChange={setTagOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Add tag</DialogTitle>
            <DialogDescription>
              {selected.size} slide{selected.size === 1 ? '' : 's'} selected.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-1">
            <Input
              autoFocus
              value={tagName}
              onChange={e => setTagName(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter' && tagName.trim()) applyTag(tagName, tagColor) }}
              placeholder="Tag name"
              disabled={tagging}
            />
            {tagSuggestions.length > 0 && (
              <div className="flex flex-wrap gap-1.5">
                {tagSuggestions.map(t => (
                  <button key={t.id ?? t.name} type="button" onClick={() => applyTag(t.name, t.color || tagColor)}>
                    <TagChip name={t.name} color={t.color || undefined} />
                  </button>
                ))}
              </div>
            )}
            <div className="flex items-center gap-1.5">
              <span className="text-xs text-muted-foreground mr-1">Colour</span>
              {PRESET_COLORS.map(c => (
                <button
                  key={c}
                  type="button"
                  onClick={() => setTagColor(c)}
                  className={`h-5 w-5 rounded-full border-2 ${tagColor === c ? 'border-foreground' : 'border-transparent'}`}
                  style={{ backgroundColor: c }}
                  aria-label={`Colour ${c}`}
                />
              ))}
            </div>
          </div>
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={() => setTagOpen(false)}>Cancel</Button>
            <Button onClick={() => applyTag(tagName, tagColor)} disabled={tagging || !tagName.trim()}>
              {tagging && <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />}
              Add tag
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      <AddToCohortDialog
        open={cohortOpen}
        onOpenChange={setCohortOpen}
        slideHashes={Array.from(selected)}
        onAdded={({ cohortId, cohortName, added }) => {
          setActionMsg(`Added ${added} slide${added === 1 ? '' : 's'} to "${cohortName}".`)
          onAddedToCohort?.(cohortId, cohortName)
        }}
      />
    </>
  )
}
