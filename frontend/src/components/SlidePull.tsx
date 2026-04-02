import { useState, useEffect, useCallback } from 'react'
import {
  Search,
  Plus,
  Trash2,
  ChevronRight,
  ChevronDown,
  Check,
  X,
  Download,
  Upload,
  FileText,
  Package,
  Microscope,
  Circle,
  Copy,
  ClipboardCheck,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { getApiBase, normalizeAccession } from '@/api'
import type { Slide, Cohort, CohortDetail, RequestSheet, RequestSheetDetail } from '@/types/slide'

// ── Types ───────────────────────────────────────────────────────
interface PullCase {
  accession: string
  slides: PullSlide[]
  expanded: boolean
}

interface PullSlide {
  slide_hash: string
  block_id: string
  stain_type: string
  slide_number: string
  file_size_bytes?: number
  selected: boolean
}

function formatBytes(bytes?: number): string {
  if (!bytes) return '—'
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

// ── Main Component ──────────────────────────────────────────────
export function SlidePull() {
  const [cases, setCases] = useState<PullCase[]>([])
  const [loading, setLoading] = useState(false)

  // Source dialogs
  const [isSearchOpen, setIsSearchOpen] = useState(false)
  const [isCohortOpen, setIsCohortOpen] = useState(false)
  const [isSheetOpen, setIsSheetOpen] = useState(false)
  const [isPasteOpen, setIsPasteOpen] = useState(false)

  // Search
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState<Slide[]>([])
  const [searching, setSearching] = useState(false)

  // Cohort import
  const [cohorts, setCohorts] = useState<Cohort[]>([])
  const [selectedCohortId, setSelectedCohortId] = useState('')
  const [cohortDetail, setCohortDetail] = useState<CohortDetail | null>(null)
  const [loadingCohort, setLoadingCohort] = useState(false)

  // Sheet import
  const [sheets, setSheets] = useState<RequestSheet[]>([])
  const [selectedSheetId, setSelectedSheetId] = useState('')
  const [sheetDetail, setSheetDetail] = useState<RequestSheetDetail | null>(null)
  const [loadingSheet, setLoadingSheet] = useState(false)

  // Paste
  const [pasteText, setPasteText] = useState('')
  const [pasteLoading, setPasteLoading] = useState(false)

  // Export
  const [copied, setCopied] = useState(false)
  const [downloading, setDownloading] = useState(false)
  const [downloadError, setDownloadError] = useState('')

  // ── Add slides from search results (grouped by accession) ─────
  const addSlidesFromResults = useCallback((slides: Slide[]) => {
    setCases(prev => {
      const next = [...prev]
      const caseMap = new Map(next.map((c, i) => [c.accession, i]))

      for (const slide of slides) {
        const acc = normalizeAccession(slide.accession_number)
        const pullSlide: PullSlide = {
          slide_hash: slide.slide_hash,
          block_id: slide.block_id,
          stain_type: slide.stain_type,
          slide_number: slide.slide_number,
          file_size_bytes: slide.file_size_bytes,
          selected: true,
        }

        const idx = caseMap.get(acc)
        if (idx !== undefined) {
          // Don't add duplicate slides
          if (!next[idx].slides.some(s => s.slide_hash === slide.slide_hash)) {
            next[idx].slides.push(pullSlide)
          }
        } else {
          const newCase: PullCase = { accession: acc, slides: [pullSlide], expanded: true }
          caseMap.set(acc, next.length)
          next.push(newCase)
        }
      }
      return next
    })
  }, [])

  // ── Search handler (supports comma-separated accessions) ─────
  const handleSearch = async () => {
    if (!searchQuery.trim()) return
    setSearching(true)
    try {
      const queries = searchQuery.split(/[,;]+/).map(s => s.trim()).filter(Boolean)
      const allResults: Slide[] = []
      const seen = new Set<string>()
      for (const raw of queries) {
        const q = normalizeAccession(raw)
        const res = await fetch(`${getApiBase()}/search?q=${encodeURIComponent(q)}&limit=100`)
        if (res.ok) {
          const data = await res.json()
          for (const slide of (data.results || []) as Slide[]) {
            if (!seen.has(slide.slide_hash)) {
              seen.add(slide.slide_hash)
              allResults.push(slide)
            }
          }
        }
      }
      setSearchResults(allResults)
    } catch (e) {
      console.error('Search failed:', e)
    } finally {
      setSearching(false)
    }
  }

  const addSearchResults = () => {
    addSlidesFromResults(searchResults)
    setIsSearchOpen(false)
    setSearchQuery('')
    setSearchResults([])
  }

  // ── Cohort import ─────────────────────────────────────────────
  const openCohortImport = async () => {
    setIsCohortOpen(true)
    setSelectedCohortId('')
    setCohortDetail(null)
    try {
      const res = await fetch(`${getApiBase()}/cohorts`)
      if (res.ok) setCohorts(await res.json())
    } catch {}
  }

  const loadCohortDetail = async (id: string) => {
    setSelectedCohortId(id)
    setLoadingCohort(true)
    try {
      const res = await fetch(`${getApiBase()}/cohorts/${id}`)
      if (res.ok) setCohortDetail(await res.json())
    } catch {}
    setLoadingCohort(false)
  }

  const importCohort = () => {
    if (!cohortDetail) return
    const slides: Slide[] = cohortDetail.slides.map(s => ({
      slide_hash: s.slide_hash,
      accession_number: s.accession_number || 'Unknown',
      block_id: s.block_id,
      stain_type: s.stain_type,
      slide_number: s.slide_number || '',
      file_size_bytes: s.file_size_bytes,
    }))
    addSlidesFromResults(slides)
    setIsCohortOpen(false)
  }

  // ── Request sheet import ──────────────────────────────────────
  const openSheetImport = async () => {
    setIsSheetOpen(true)
    setSelectedSheetId('')
    setSheetDetail(null)
    try {
      const res = await fetch(`${getApiBase()}/request-sheets`)
      if (res.ok) setSheets(await res.json())
    } catch {}
  }

  const loadSheetDetail = async (id: string) => {
    setSelectedSheetId(id)
    setLoadingSheet(true)
    try {
      const res = await fetch(`${getApiBase()}/request-sheets/${id}`)
      if (res.ok) setSheetDetail(await res.json())
    } catch {}
    setLoadingSheet(false)
  }

  const importSheet = async () => {
    if (!sheetDetail) return
    setLoading(true)
    // For each accession in the sheet, search for its slides
    for (const row of sheetDetail.rows) {
      try {
        const q = normalizeAccession(row.accession_number)
        const res = await fetch(`${getApiBase()}/search?q=${encodeURIComponent(q)}&limit=100`)
        if (res.ok) {
          const data = await res.json()
          // Only add exact accession matches
          const exact = (data.results || []).filter((s: Slide) =>
            normalizeAccession(s.accession_number) === q
          )
          if (exact.length > 0) addSlidesFromResults(exact)
        }
      } catch {}
    }
    setLoading(false)
    setIsSheetOpen(false)
  }

  // ── Paste import ──────────────────────────────────────────────
  const handlePasteImport = async () => {
    const lines = pasteText.split(/[\n,;]+/).map(l => l.trim()).filter(Boolean)
    if (lines.length === 0) return
    setPasteLoading(true)
    for (const raw of lines) {
      const q = normalizeAccession(raw)
      try {
        const res = await fetch(`${getApiBase()}/search?q=${encodeURIComponent(q)}&limit=100`)
        if (res.ok) {
          const data = await res.json()
          const exact = (data.results || []).filter((s: Slide) =>
            normalizeAccession(s.accession_number) === q
          )
          if (exact.length > 0) addSlidesFromResults(exact)
        }
      } catch {}
    }
    setPasteLoading(false)
    setIsPasteOpen(false)
    setPasteText('')
  }

  // ── Case/slide operations ─────────────────────────────────────
  const toggleCase = (accession: string) => {
    setCases(prev => prev.map(c =>
      c.accession === accession ? { ...c, expanded: !c.expanded } : c
    ))
  }

  const toggleSlide = (accession: string, slideHash: string) => {
    setCases(prev => prev.map(c =>
      c.accession === accession
        ? { ...c, slides: c.slides.map(s => s.slide_hash === slideHash ? { ...s, selected: !s.selected } : s) }
        : c
    ))
  }

  const toggleAllInCase = (accession: string) => {
    setCases(prev => prev.map(c => {
      if (c.accession !== accession) return c
      const allSelected = c.slides.every(s => s.selected)
      return { ...c, slides: c.slides.map(s => ({ ...s, selected: !allSelected })) }
    }))
  }

  const removeCase = (accession: string) => {
    setCases(prev => prev.filter(c => c.accession !== accession))
  }

  const selectAll = () => {
    setCases(prev => prev.map(c => ({ ...c, slides: c.slides.map(s => ({ ...s, selected: true })) })))
  }

  const deselectAll = () => {
    setCases(prev => prev.map(c => ({ ...c, slides: c.slides.map(s => ({ ...s, selected: false })) })))
  }

  // ── Stats ─────────────────────────────────────────────────────
  const totalSlides = cases.reduce((sum, c) => sum + c.slides.length, 0)
  const selectedSlides = cases.reduce((sum, c) => sum + c.slides.filter(s => s.selected).length, 0)
  const totalSize = cases.reduce((sum, c) => sum + c.slides.filter(s => s.selected).reduce((ss, s) => ss + (s.file_size_bytes || 0), 0), 0)
  const selectedCases = cases.filter(c => c.slides.some(s => s.selected)).length

  // ── Export pull list ──────────────────────────────────────────
  const generatePullList = (): string => {
    const lines: string[] = ['Accession,Block,Stain,Slide#']
    for (const c of cases) {
      for (const s of c.slides) {
        if (s.selected) {
          lines.push(`${c.accession},${s.block_id},${s.stain_type},${s.slide_number}`)
        }
      }
    }
    return lines.join('\n')
  }

  const copyPullList = () => {
    navigator.clipboard.writeText(generatePullList()).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  const downloadPullList = () => {
    const csv = generatePullList()
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `slide-pull-${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const downloadFiles = async () => {
    const hashes = cases.flatMap(c => c.slides.filter(s => s.selected).map(s => s.slide_hash))
    if (hashes.length === 0) return
    setDownloading(true)
    setDownloadError('')
    try {
      const res = await fetch(`${getApiBase()}/slides/pull-download`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: hashes }),
      })
      if (!res.ok) {
        const detail = await res.json().catch(() => null)
        throw new Error(detail?.detail || `Download failed (${res.status})`)
      }
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `slide-pull-${new Date().toISOString().slice(0, 10)}.zip`
      a.click()
      URL.revokeObjectURL(url)
    } catch (e: any) {
      console.error('Download failed:', e)
      setDownloadError(e.message || 'Download failed')
    } finally {
      setDownloading(false)
    }
  }

  // ── Empty state ───────────────────────────────────────────────
  if (cases.length === 0 && !loading) {
    return (
      <div className="h-full flex flex-col">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-lg font-semibold">WSI Pull</h2>
            <p className="text-[13px] text-muted-foreground">Select slides from cases to create a pull request</p>
          </div>
        </div>

        <div className="flex-1 flex items-center justify-center">
          <div className="text-center max-w-md space-y-5">
            <div className="mx-auto w-14 h-14 rounded-lg bg-primary/10 flex items-center justify-center">
              <Package className="h-7 w-7 text-primary" />
            </div>
            <div>
              <p className="text-[14px] font-medium mb-1">Start a WSI Pull</p>
              <p className="text-[13px] text-muted-foreground">Add cases to select which slides you need pulled</p>
            </div>
            <div className="grid grid-cols-2 gap-2 max-w-xs mx-auto">
              <Button variant="outline" size="sm" className="h-9 text-[13px]" onClick={() => setIsPasteOpen(true)}>
                <FileText className="h-3.5 w-3.5 mr-1.5" />Paste Cases
              </Button>
              <Button variant="outline" size="sm" className="h-9 text-[13px]" onClick={() => setIsSearchOpen(true)}>
                <Search className="h-3.5 w-3.5 mr-1.5" />Search
              </Button>
              <Button variant="outline" size="sm" className="h-9 text-[13px]" onClick={openCohortImport}>
                <Upload className="h-3.5 w-3.5 mr-1.5" />From Cohort
              </Button>
              <Button variant="outline" size="sm" className="h-9 text-[13px]" onClick={openSheetImport}>
                <FileText className="h-3.5 w-3.5 mr-1.5" />From Request
              </Button>
            </div>
          </div>
        </div>

        {/* Dialogs rendered at bottom */}
        {renderDialogs()}
      </div>
    )
  }

  // ── Render dialogs ────────────────────────────────────────────
  function renderDialogs() {
    return (
      <>
        {/* Search dialog */}
        <Dialog open={isSearchOpen} onOpenChange={setIsSearchOpen}>
          <DialogContent className="sm:max-w-lg">
            <DialogHeader>
              <DialogTitle>Search for Slides</DialogTitle>
              <DialogDescription>Search by accession number to find slides</DialogDescription>
            </DialogHeader>
            <div className="space-y-3 py-2">
              <div className="flex gap-2">
                <Input
                  placeholder="e.g. BS26-D12345, BS08-E31645"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
                  className="text-[13px] font-mono"
                  autoFocus
                />
                <Button size="sm" onClick={handleSearch} disabled={searching || !searchQuery.trim()}>
                  {searching ? 'Searching...' : 'Search'}
                </Button>
              </div>
              {searchResults.length > 0 && (
                <div className="border border-gray-300 rounded-md max-h-[300px] overflow-y-auto">
                  <div className="px-3 py-1.5 bg-muted/30 border-b border-gray-200 text-[11px] font-medium text-muted-foreground sticky top-0">
                    {searchResults.length} slides found
                  </div>
                  {searchResults.map(s => (
                    <div key={s.slide_hash} className="flex items-center gap-2 px-3 py-1.5 border-b border-gray-100 text-[12px] last:border-b-0">
                      <span className="font-mono font-medium text-foreground">{s.accession_number}</span>
                      <span className="text-muted-foreground">{s.block_id}</span>
                      <span className="rounded bg-muted px-1 py-0.5 text-[11px]">{s.stain_type}</span>
                      <span className="ml-auto text-muted-foreground tabular-nums">{formatBytes(s.file_size_bytes)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={() => setIsSearchOpen(false)}>Cancel</Button>
              {searchResults.length > 0 && (
                <Button size="sm" onClick={addSearchResults}>Add {searchResults.length} Slides</Button>
              )}
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* Paste dialog */}
        <Dialog open={isPasteOpen} onOpenChange={setIsPasteOpen}>
          <DialogContent className="sm:max-w-lg">
            <DialogHeader>
              <DialogTitle>Paste Case List</DialogTitle>
              <DialogDescription>Paste accession numbers. All slides for each case will be loaded.</DialogDescription>
            </DialogHeader>
            <div className="space-y-3 py-2">
              <textarea
                value={pasteText}
                onChange={(e) => setPasteText(e.target.value)}
                placeholder={"BS26-D12345\nBS08-E31645\nBS24-001234"}
                rows={6}
                className="w-full rounded-md border border-gray-300 bg-transparent px-3 py-2 text-[13px] font-mono shadow-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring resize-y"
                autoFocus
              />
              {pasteText && (
                <p className="text-[11px] text-muted-foreground">
                  {pasteText.split(/[\n,;]+/).filter(l => l.trim()).length} cases
                </p>
              )}
            </div>
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={() => setIsPasteOpen(false)}>Cancel</Button>
              <Button size="sm" onClick={handlePasteImport} disabled={!pasteText.trim() || pasteLoading}>
                {pasteLoading ? 'Loading...' : 'Load Slides'}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* Cohort import dialog */}
        <Dialog open={isCohortOpen} onOpenChange={setIsCohortOpen}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Import from Cohort</DialogTitle>
              <DialogDescription>Load all slides from a cohort</DialogDescription>
            </DialogHeader>
            <div className="space-y-3 py-2">
              <Select value={selectedCohortId} onValueChange={loadCohortDetail}>
                <SelectTrigger className="text-[13px]">
                  <SelectValue placeholder="Select a cohort..." />
                </SelectTrigger>
                <SelectContent>
                  {cohorts.map(c => (
                    <SelectItem key={c.id} value={String(c.id)} className="text-[13px]">
                      {c.name} ({c.case_count} cases, {c.slide_count} slides)
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {loadingCohort && <p className="text-[12px] text-muted-foreground">Loading cohort...</p>}
              {cohortDetail && (
                <div className="rounded-md bg-muted/30 border border-gray-300 p-2.5 text-[12px]">
                  <span className="font-medium">{cohortDetail.name}</span>
                  <span className="text-muted-foreground"> — {cohortDetail.case_count} cases, {cohortDetail.slide_count} slides</span>
                </div>
              )}
            </div>
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={() => setIsCohortOpen(false)}>Cancel</Button>
              <Button size="sm" onClick={importCohort} disabled={!cohortDetail}>Import</Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* Request sheet import dialog */}
        <Dialog open={isSheetOpen} onOpenChange={setIsSheetOpen}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Import from Request Sheet</DialogTitle>
              <DialogDescription>Load slides for cases tracked in a request sheet</DialogDescription>
            </DialogHeader>
            <div className="space-y-3 py-2">
              <Select value={selectedSheetId} onValueChange={loadSheetDetail}>
                <SelectTrigger className="text-[13px]">
                  <SelectValue placeholder="Select a request sheet..." />
                </SelectTrigger>
                <SelectContent>
                  {sheets.map(s => (
                    <SelectItem key={s.id} value={String(s.id)} className="text-[13px]">
                      {s.name} ({s.case_count} cases)
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {loadingSheet && <p className="text-[12px] text-muted-foreground">Loading sheet...</p>}
              {sheetDetail && (
                <div className="rounded-md bg-muted/30 border border-gray-300 p-2.5 text-[12px]">
                  <span className="font-medium">{sheetDetail.name}</span>
                  <span className="text-muted-foreground"> — {sheetDetail.rows.length} cases</span>
                </div>
              )}
            </div>
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={() => setIsSheetOpen(false)}>Cancel</Button>
              <Button size="sm" onClick={importSheet} disabled={!sheetDetail || loading}>
                {loading ? 'Loading slides...' : 'Import'}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </>
    )
  }

  // ── Main layout ───────────────────────────────────────────────
  return (
    <div className="flex flex-col h-full" style={{ minHeight: 0 }}>
      {/* Header row */}
      <div className="flex items-center justify-between mb-3">
        <div>
          <h2 className="text-lg font-semibold">WSI Pull</h2>
          <p className="text-[12px] text-muted-foreground">{cases.length} cases · {selectedSlides} of {totalSlides} slides selected · {formatBytes(totalSize)}</p>
        </div>
        <div className="flex items-center gap-1.5">
          <Button size="sm" variant="outline" className="h-7 text-[12px]" onClick={() => setIsPasteOpen(true)}>
            <FileText className="h-3 w-3 mr-1" />Paste
          </Button>
          <Button size="sm" variant="outline" className="h-7 text-[12px]" onClick={() => setIsSearchOpen(true)}>
            <Search className="h-3 w-3 mr-1" />Search
          </Button>
          <Button size="sm" variant="outline" className="h-7 text-[12px]" onClick={openCohortImport}>
            <Upload className="h-3 w-3 mr-1" />Cohort
          </Button>
          <Button size="sm" variant="outline" className="h-7 text-[12px]" onClick={openSheetImport}>
            <FileText className="h-3 w-3 mr-1" />Request
          </Button>
          <div className="w-px h-5 bg-border mx-0.5" />
          <Button size="sm" variant="ghost" className="h-7 text-[12px]" onClick={selectAll}>Select All</Button>
          <Button size="sm" variant="ghost" className="h-7 text-[12px]" onClick={deselectAll}>Clear</Button>
        </div>
      </div>

      {/* Content: case tree + summary sidebar */}
      <div className="flex gap-3 flex-1 min-h-0">
        {/* Case tree (main area) */}
        <div className="flex-1 min-w-0 border border-gray-300 rounded-md shadow-sm overflow-hidden flex flex-col bg-background">
          {/* Column headers */}
          <div className="flex items-center gap-2 px-3 py-1.5 bg-muted/30 border-b border-gray-300 text-[11px] font-medium text-muted-foreground shrink-0">
            <div className="w-5" />
            <div className="w-5" />
            <div className="flex-1">Case / Slide</div>
            <div className="w-16 text-center">Block</div>
            <div className="w-20 text-center">Stain</div>
            <div className="w-16 text-right">Size</div>
            <div className="w-8" />
          </div>

          {/* Scrollable tree */}
          <div className="flex-1 overflow-y-auto">
            {cases.map(c => {
              const caseSelected = c.slides.filter(s => s.selected).length
              const allSelected = c.slides.every(s => s.selected)
              const someSelected = caseSelected > 0 && !allSelected

              return (
                <div key={c.accession}>
                  {/* Case row */}
                  <div className="flex items-center gap-2 px-3 py-1.5 border-b border-gray-100 bg-muted/10 hover:bg-muted/20 transition-colors group">
                    <button onClick={() => toggleCase(c.accession)} className="w-5 flex items-center justify-center text-muted-foreground">
                      {c.expanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                    </button>
                    <div className="w-5 flex items-center justify-center">
                      <Checkbox
                        checked={allSelected ? true : someSelected ? 'indeterminate' : false}
                        onCheckedChange={() => toggleAllInCase(c.accession)}
                      />
                    </div>
                    <div className="flex-1 min-w-0">
                      <span className="font-mono text-[13px] font-semibold tracking-tight">{c.accession}</span>
                      <span className="text-[11px] text-muted-foreground ml-2">{caseSelected}/{c.slides.length} slides</span>
                    </div>
                    <div className="w-16" />
                    <div className="w-20" />
                    <div className="w-16 text-right text-[11px] text-muted-foreground tabular-nums">
                      {formatBytes(c.slides.filter(s => s.selected).reduce((sum, s) => sum + (s.file_size_bytes || 0), 0))}
                    </div>
                    <div className="w-8 flex items-center justify-center">
                      <button
                        onClick={() => removeCase(c.accession)}
                        className="opacity-0 group-hover:opacity-60 hover:!opacity-100 transition-opacity"
                      >
                        <Trash2 className="h-3 w-3 text-muted-foreground" />
                      </button>
                    </div>
                  </div>

                  {/* Slide rows */}
                  {c.expanded && c.slides.map(s => (
                    <div
                      key={s.slide_hash}
                      className={`flex items-center gap-2 px-3 py-1 border-b border-gray-50 transition-colors ${
                        s.selected ? 'bg-primary/[0.03]' : 'opacity-50'
                      } hover:bg-muted/10`}
                    >
                      <div className="w-5" />
                      <div className="w-5 flex items-center justify-center">
                        <Checkbox
                          checked={s.selected}
                          onCheckedChange={() => toggleSlide(c.accession, s.slide_hash)}
                        />
                      </div>
                      <div className="flex-1 min-w-0 flex items-center gap-1.5">
                        <Microscope className="h-3 w-3 text-muted-foreground/50 shrink-0" />
                        <span className="text-[12px] font-mono text-muted-foreground truncate">{s.slide_hash.slice(0, 10)}...</span>
                      </div>
                      <div className="w-16 text-center">
                        <span className="inline-flex rounded bg-gray-100 border border-gray-300 px-1.5 py-0.5 text-[11px] font-mono">{s.block_id || '—'}</span>
                      </div>
                      <div className="w-20 text-center">
                        <span className={`inline-flex rounded px-1.5 py-0.5 text-[11px] font-medium ${
                          s.stain_type === 'HE' ? 'bg-rose-50 text-rose-700 border border-rose-200' :
                          s.stain_type?.startsWith('IHC') ? 'bg-blue-50 text-blue-700 border border-blue-200' :
                          'bg-gray-50 text-gray-600 border border-gray-200'
                        }`}>{s.stain_type || '—'}</span>
                      </div>
                      <div className="w-16 text-right text-[11px] text-muted-foreground tabular-nums">
                        {formatBytes(s.file_size_bytes)}
                      </div>
                      <div className="w-8" />
                    </div>
                  ))}
                </div>
              )
            })}
          </div>
        </div>

        {/* Right sidebar: pull summary */}
        <div className="w-[220px] shrink-0 flex flex-col gap-2">
          {/* Stats */}
          <div className="border border-gray-300 rounded-md shadow-sm p-3 bg-background space-y-3">
            <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Pull Summary</p>
            <div className="space-y-2">
              <div className="flex justify-between text-[13px]">
                <span className="text-muted-foreground">Cases</span>
                <span className="font-semibold tabular-nums">{selectedCases}</span>
              </div>
              <div className="flex justify-between text-[13px]">
                <span className="text-muted-foreground">Slides</span>
                <span className="font-semibold tabular-nums">{selectedSlides}</span>
              </div>
              <div className="h-px bg-border" />
              <div className="flex justify-between text-[13px]">
                <span className="text-muted-foreground">Total Size</span>
                <span className="font-semibold tabular-nums">{formatBytes(totalSize)}</span>
              </div>
            </div>
          </div>

          {/* Stain breakdown */}
          {selectedSlides > 0 && (
            <div className="border border-gray-300 rounded-md shadow-sm p-3 bg-background space-y-2">
              <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">By Stain</p>
              {(() => {
                const stainCounts: Record<string, number> = {}
                for (const c of cases) {
                  for (const s of c.slides) {
                    if (s.selected) {
                      const key = s.stain_type || 'Unknown'
                      stainCounts[key] = (stainCounts[key] || 0) + 1
                    }
                  }
                }
                return Object.entries(stainCounts).sort((a, b) => b[1] - a[1]).map(([stain, count]) => (
                  <div key={stain} className="flex justify-between text-[12px]">
                    <span className="text-muted-foreground">{stain}</span>
                    <span className="tabular-nums font-medium">{count}</span>
                  </div>
                ))
              })()}
            </div>
          )}

          {/* Export actions */}
          <div className="border border-gray-300 rounded-md shadow-sm p-3 bg-background space-y-2">
            <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Export</p>
            <Button
              size="sm"
              className="w-full h-8 text-[12px]"
              onClick={downloadFiles}
              disabled={selectedSlides === 0 || downloading}
            >
              <Package className="h-3 w-3 mr-1.5" />
              {downloading ? 'Preparing ZIP...' : 'Download Files'}
            </Button>
            {downloadError && (
              <p className="text-[11px] text-red-600">{downloadError}</p>
            )}
            <Button
              size="sm"
              variant="outline"
              className="w-full h-8 text-[12px]"
              onClick={downloadPullList}
              disabled={selectedSlides === 0}
            >
              <Download className="h-3 w-3 mr-1.5" />Download CSV
            </Button>
            <Button
              size="sm"
              variant="outline"
              className="w-full h-8 text-[12px]"
              onClick={copyPullList}
              disabled={selectedSlides === 0}
            >
              {copied ? <ClipboardCheck className="h-3 w-3 mr-1.5 text-emerald-600" /> : <Copy className="h-3 w-3 mr-1.5" />}
              {copied ? 'Copied!' : 'Copy to Clipboard'}
            </Button>
          </div>

          {/* Clear all */}
          <Button
            size="sm"
            variant="ghost"
            className="h-7 text-[12px] text-muted-foreground"
            onClick={() => setCases([])}
          >
            <Trash2 className="h-3 w-3 mr-1" />Clear All
          </Button>
        </div>
      </div>

      {loading && (
        <div className="fixed inset-0 bg-background/50 flex items-center justify-center z-50">
          <div className="bg-background border border-gray-300 shadow-lg rounded-md px-6 py-4 text-[13px] text-muted-foreground">
            Loading slides...
          </div>
        </div>
      )}

      {renderDialogs()}
    </div>
  )
}
