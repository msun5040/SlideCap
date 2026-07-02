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
  FolderOutput,
  History,
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
import { copyToClipboard } from '@/lib/clipboard'
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

// Classify a stain into a coarse bucket used by the stain-filter UI.
type StainBucket = 'H&E' | 'IHC' | 'Other'
function stainBucket(stain?: string): StainBucket {
  const s = (stain || '').trim().toUpperCase()
  // H&E may be recorded as HE, HNE, H&E, or H E depending on the site's naming.
  if (s === 'HE' || s === 'HNE' || s === 'H&E' || s === 'H E') return 'H&E'
  if (s.startsWith('IHC')) return 'IHC'
  return 'Other'
}
function slideMatchesStainFilter(stain: string | undefined, buckets: Set<StainBucket>): boolean {
  if (buckets.size === 0) return true // empty filter = all
  return buckets.has(stainBucket(stain))
}

interface ExportReport {
  output_dir: string
  preferred_method: 'symlink' | 'hardlink' | 'copy'
  skip_existing: boolean
  bin_size: number
  bin_count: number
  total_requested: number
  total_exported: number
  total_skipped: number
  missing_count: number
  failure_count: number
  bins: { bin: string; path: string; slides: number; skipped: number; failures: number }[]
  summary_path: string
}

interface PullHistoryEntry {
  id: string
  exported_at: string
  output_dir: string
  preferred_method: 'symlink' | 'hardlink' | 'copy'
  method_requested: string
  skip_existing: boolean
  bin_size: number
  bin_count: number
  total_requested: number
  total_exported: number
  total_skipped: number
  missing_count: number
  failure_count: number
  case_count: number
  accessions: string[]
  bins: { bin: string; slides: number; skipped: number; failures: number }[]
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
  const [pasteStainFilter, setPasteStainFilter] = useState<Set<StainBucket>>(new Set(['H&E']))

  // Export
  const [copied, setCopied] = useState(false)
  const [downloading, setDownloading] = useState(false)
  const [downloadError, setDownloadError] = useState('')

  // Export to directory
  const [isExportOpen, setIsExportOpen] = useState(false)
  const [exportDir, setExportDir] = useState('')
  const [exportBinSize, setExportBinSize] = useState(100)
  const [exportMethod, setExportMethod] = useState<'hardlink' | 'copy' | 'symlink'>('hardlink')
  const [exportSkipExisting, setExportSkipExisting] = useState(true)
  const [exporting, setExporting] = useState(false)
  const [exportError, setExportError] = useState('')
  const [exportReport, setExportReport] = useState<ExportReport | null>(null)

  // Pull history
  const [isHistoryOpen, setIsHistoryOpen] = useState(false)
  const [history, setHistory] = useState<PullHistoryEntry[]>([])
  const [historyLoading, setHistoryLoading] = useState(false)
  const [historyError, setHistoryError] = useState('')
  const [expandedHistoryId, setExpandedHistoryId] = useState<string | null>(null)

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
            && slideMatchesStainFilter(s.stain_type, pasteStainFilter)
          )
          if (exact.length > 0) addSlidesFromResults(exact)
        }
      } catch {}
    }
    setPasteLoading(false)
    setIsPasteOpen(false)
    setPasteText('')
  }

  const togglePasteStain = (b: StainBucket) => {
    setPasteStainFilter(prev => {
      const next = new Set(prev)
      if (next.has(b)) next.delete(b)
      else next.add(b)
      return next
    })
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
    copyToClipboard(generatePullList()).then((ok) => {
      if (!ok) return
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

  const openExportDialog = () => {
    setExportError('')
    setExportReport(null)
    setIsExportOpen(true)
  }

  const runExportToDirectory = async () => {
    const hashes = cases.flatMap(c => c.slides.filter(s => s.selected).map(s => s.slide_hash))
    if (hashes.length === 0) return
    if (!exportDir.trim()) {
      setExportError('Output directory is required')
      return
    }
    setExporting(true)
    setExportError('')
    setExportReport(null)
    try {
      const res = await fetch(`${getApiBase()}/slides/pull-export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          slide_hashes: hashes,
          output_dir: exportDir.trim(),
          bin_size: Math.max(1, exportBinSize | 0),
          method: exportMethod,
          skip_existing: exportSkipExisting,
        }),
      })
      if (!res.ok) {
        const detail = await res.json().catch(() => null)
        throw new Error(detail?.detail || `Export failed (${res.status})`)
      }
      const report = (await res.json()) as ExportReport
      setExportReport(report)
    } catch (e: any) {
      console.error('Export failed:', e)
      setExportError(e.message || 'Export failed')
    } finally {
      setExporting(false)
    }
  }

  const openHistory = async () => {
    setIsHistoryOpen(true)
    setHistoryError('')
    setHistoryLoading(true)
    setExpandedHistoryId(null)
    try {
      const res = await fetch(`${getApiBase()}/slides/pull-history?limit=100`)
      if (!res.ok) {
        const detail = await res.json().catch(() => null)
        throw new Error(detail?.detail || `Failed to load history (${res.status})`)
      }
      const data = await res.json()
      setHistory((data.entries || []) as PullHistoryEntry[])
    } catch (e: any) {
      console.error('Pull history failed:', e)
      setHistoryError(e.message || 'Failed to load history')
    } finally {
      setHistoryLoading(false)
    }
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
              <div className="space-y-1.5">
                <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Stain filter</p>
                <div className="flex gap-1.5">
                  {(['H&E', 'IHC', 'Other'] as StainBucket[]).map(b => {
                    const active = pasteStainFilter.has(b)
                    return (
                      <button
                        key={b}
                        type="button"
                        onClick={() => togglePasteStain(b)}
                        className={`text-[12px] px-2.5 py-1 rounded border transition-colors ${
                          active
                            ? b === 'H&E'
                              ? 'bg-rose-50 text-rose-700 border-rose-300'
                              : b === 'IHC'
                                ? 'bg-blue-50 text-blue-700 border-blue-300'
                                : 'bg-gray-100 text-gray-700 border-gray-300'
                            : 'bg-background text-muted-foreground border-gray-300 hover:bg-muted/30'
                        }`}
                      >
                        {b}
                      </button>
                    )
                  })}
                </div>
                <p className="text-[11px] text-muted-foreground">
                  {pasteStainFilter.size === 0 ? 'No filter — all stains will be loaded' : `Only ${Array.from(pasteStainFilter).join(', ')} slides will be loaded`}
                </p>
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={() => setIsPasteOpen(false)}>Cancel</Button>
              <Button size="sm" onClick={handlePasteImport} disabled={!pasteText.trim() || pasteLoading}>
                {pasteLoading ? 'Loading...' : 'Load Slides'}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* Export to directory dialog */}
        <Dialog open={isExportOpen} onOpenChange={setIsExportOpen}>
          <DialogContent className="sm:max-w-lg">
            <DialogHeader>
              <DialogTitle>Export to Directory</DialogTitle>
              <DialogDescription>
                Create a directory of slide files for Halo. Splits into {exportBinSize}-slide bins.
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-3 py-2">
              <div className="space-y-1.5">
                <label className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Output directory</label>
                <Input
                  value={exportDir}
                  onChange={(e) => setExportDir(e.target.value)}
                  placeholder="e.g. /Volumes/halo-share/pulls/2026-06-02-mycase"
                  className="text-[13px] font-mono"
                  autoFocus
                  disabled={exporting}
                />
                <p className="text-[11px] text-muted-foreground">Absolute path on the SlideCap host. Will be created if it doesn't exist.</p>
              </div>
              <div className="space-y-1.5">
                <label className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Method</label>
                <Select value={exportMethod} onValueChange={(v) => setExportMethod(v as 'hardlink' | 'copy' | 'symlink')} disabled={exporting}>
                  <SelectTrigger className="text-[13px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="hardlink" className="text-[13px]">Hardlink (recommended)</SelectItem>
                    <SelectItem value="copy" className="text-[13px]">Copy</SelectItem>
                    <SelectItem value="symlink" className="text-[13px]">Symlink</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-[11px] text-muted-foreground">
                  {exportMethod === 'hardlink' && 'Instant. Looks like a real file to Halo. No extra disk space. Requires same volume as the WSI files.'}
                  {exportMethod === 'copy' && 'Slowest but always works. Duplicates bytes on disk.'}
                  {exportMethod === 'symlink' && 'Instant, but Halo may not follow symlinks. Use only if you have verified it works.'}
                  {' '}Falls back to copy if the preferred method fails.
                </p>
              </div>
              <div className="space-y-1.5">
                <label className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Slides per bin</label>
                <Input
                  type="number"
                  min={1}
                  max={1000}
                  value={exportBinSize}
                  onChange={(e) => setExportBinSize(parseInt(e.target.value || '100', 10))}
                  className="text-[13px] w-24"
                  disabled={exporting}
                />
                <p className="text-[11px] text-muted-foreground">
                  {selectedSlides > 0
                    ? `${selectedSlides} selected → ${Math.ceil(selectedSlides / Math.max(1, exportBinSize))} bin${Math.ceil(selectedSlides / Math.max(1, exportBinSize)) === 1 ? '' : 's'}`
                    : 'Select slides first'}
                </p>
              </div>

              <div className="flex items-start gap-2 pt-0.5">
                <Checkbox
                  id="skip-existing"
                  checked={exportSkipExisting}
                  onCheckedChange={(v) => setExportSkipExisting(v === true)}
                  disabled={exporting}
                  className="mt-0.5"
                />
                <label htmlFor="skip-existing" className="text-[12px] leading-tight cursor-pointer select-none">
                  <span className="font-medium">Skip slides already in the target</span>
                  <span className="block text-[11px] text-muted-foreground">
                    Resumes an interrupted pull — re-pasting the same list only links what's missing. Uncheck to re-materialize everything.
                  </span>
                </label>
              </div>

              {exportError && (
                <div className="rounded-md border border-red-300 bg-red-50 p-2.5 text-[12px] text-red-700">
                  {exportError}
                </div>
              )}

              {exportReport && (
                <div className="rounded-md border border-emerald-300 bg-emerald-50 p-3 text-[12px] space-y-2">
                  <div className="flex items-center gap-2">
                    <Check className="h-3.5 w-3.5 text-success-ink" />
                    <span className="font-medium text-success-ink">Export complete</span>
                    <span className="ml-auto rounded-sm bg-success-soft text-success-ink px-1.5 py-0.5 text-[11px] font-medium">
                      {exportReport.preferred_method}
                    </span>
                  </div>
                  <div className="font-mono text-[11px] text-emerald-900 break-all">{exportReport.output_dir}</div>
                  <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-[12px]">
                    <span className="text-emerald-900/70">Bins created</span><span className="tabular-nums text-right">{exportReport.bin_count}</span>
                    <span className="text-emerald-900/70">In target</span><span className="tabular-nums text-right">{exportReport.total_exported}</span>
                    {exportReport.total_skipped > 0 && (
                      <>
                        <span className="text-emerald-900/70">Skipped (already present)</span><span className="tabular-nums text-right">{exportReport.total_skipped}</span>
                      </>
                    )}
                    <span className="text-emerald-900/70">Missing</span><span className="tabular-nums text-right">{exportReport.missing_count}</span>
                    <span className="text-emerald-900/70">Failed</span><span className="tabular-nums text-right">{exportReport.failure_count}</span>
                  </div>
                  {exportReport.bins.length > 0 && (
                    <div className="border-t border-emerald-200 pt-1.5 space-y-0.5 max-h-32 overflow-y-auto">
                      {exportReport.bins.map(b => (
                        <div key={b.bin} className="flex justify-between text-[11px] font-mono">
                          <span>{b.bin}</span>
                          <span className="tabular-nums">{b.slides} slides{b.skipped ? ` · ${b.skipped} skipped` : ''}{b.failures ? ` · ${b.failures} failed` : ''}</span>
                        </div>
                      ))}
                    </div>
                  )}
                  {exportReport.preferred_method === 'copy' && exportMethod !== 'copy' && (
                    <p className="text-[11px] text-amber-700 border-t border-emerald-200 pt-1.5">
                      Preferred method ({exportMethod}) failed on this target — fell back to copy. SlideCap dedup is preserved on the source side only.
                    </p>
                  )}
                </div>
              )}
            </div>
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={() => setIsExportOpen(false)}>
                {exportReport ? 'Close' : 'Cancel'}
              </Button>
              {!exportReport && (
                <Button size="sm" onClick={runExportToDirectory} disabled={selectedSlides === 0 || !exportDir.trim() || exporting}>
                  {exporting ? 'Exporting...' : `Export ${selectedSlides} slide${selectedSlides === 1 ? '' : 's'}`}
                </Button>
              )}
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* Pull history dialog */}
        <Dialog open={isHistoryOpen} onOpenChange={setIsHistoryOpen}>
          <DialogContent className="sm:max-w-2xl">
            <DialogHeader>
              <DialogTitle>Pull History</DialogTitle>
              <DialogDescription>
                Past Export-to-Directory pulls and the cases they touched. Newest first.
              </DialogDescription>
            </DialogHeader>
            <div className="py-2 max-h-[60vh] overflow-y-auto">
              {historyLoading && <p className="text-[12px] text-muted-foreground">Loading history...</p>}
              {historyError && (
                <div className="rounded-md border border-red-300 bg-red-50 p-2.5 text-[12px] text-red-700">{historyError}</div>
              )}
              {!historyLoading && !historyError && history.length === 0 && (
                <p className="text-[12px] text-muted-foreground">No pulls recorded yet.</p>
              )}
              <div className="space-y-2">
                {history.map((h) => {
                  const open = expandedHistoryId === h.id
                  return (
                    <div key={h.id} className="rounded-md border border-gray-300 bg-background">
                      <button
                        className="w-full flex items-center gap-2 p-2.5 text-left hover:bg-muted/40"
                        onClick={() => setExpandedHistoryId(open ? null : h.id)}
                      >
                        {open ? <ChevronDown className="h-3.5 w-3.5 shrink-0" /> : <ChevronRight className="h-3.5 w-3.5 shrink-0" />}
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2">
                            <span className="text-[12px] font-medium tabular-nums">{h.exported_at}</span>
                            <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide">{h.preferred_method}</span>
                            {h.skip_existing && <span className="rounded bg-blue-100 text-blue-700 px-1.5 py-0.5 text-[10px] font-medium">skip-existing</span>}
                          </div>
                          <div className="font-mono text-[11px] text-muted-foreground truncate">{h.output_dir}</div>
                        </div>
                        <div className="text-[11px] text-muted-foreground tabular-nums shrink-0 text-right">
                          {h.case_count} case{h.case_count === 1 ? '' : 's'}<br />
                          {h.total_exported} slide{h.total_exported === 1 ? '' : 's'}
                        </div>
                      </button>
                      {open && (
                        <div className="border-t border-gray-200 p-2.5 space-y-2 text-[12px]">
                          <div className="grid grid-cols-3 gap-x-3 gap-y-0.5">
                            <span className="text-muted-foreground">Requested</span><span className="col-span-2 tabular-nums">{h.total_requested}</span>
                            <span className="text-muted-foreground">In target</span><span className="col-span-2 tabular-nums">{h.total_exported}</span>
                            <span className="text-muted-foreground">Skipped</span><span className="col-span-2 tabular-nums">{h.total_skipped}</span>
                            <span className="text-muted-foreground">Missing</span><span className="col-span-2 tabular-nums">{h.missing_count}</span>
                            <span className="text-muted-foreground">Failed</span><span className="col-span-2 tabular-nums">{h.failure_count}</span>
                            <span className="text-muted-foreground">Bins</span><span className="col-span-2 tabular-nums">{h.bin_count} × {h.bin_size}</span>
                          </div>
                          {h.bins.length > 0 && (
                            <div className="border-t border-gray-200 pt-1.5 space-y-0.5 max-h-28 overflow-y-auto">
                              {h.bins.map((b) => (
                                <div key={b.bin} className="flex justify-between text-[11px] font-mono">
                                  <span>{b.bin}</span>
                                  <span className="tabular-nums">{b.slides} slides{b.skipped ? ` · ${b.skipped} skipped` : ''}{b.failures ? ` · ${b.failures} failed` : ''}</span>
                                </div>
                              ))}
                            </div>
                          )}
                          <div className="border-t border-gray-200 pt-1.5">
                            <p className="text-[11px] text-muted-foreground mb-1">Cases ({h.case_count})</p>
                            <p className="text-[11px] font-mono break-words leading-relaxed">{h.accessions.join(', ') || '—'}</p>
                          </div>
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={() => setIsHistoryOpen(false)}>Close</Button>
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
                          stainBucket(s.stain_type) === 'H&E' ? 'bg-rose-50 text-rose-700 border border-rose-200' :
                          stainBucket(s.stain_type) === 'IHC' ? 'bg-blue-50 text-blue-700 border border-blue-200' :
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
              onClick={openExportDialog}
              disabled={selectedSlides === 0}
            >
              <FolderOutput className="h-3 w-3 mr-1.5" />
              Export to Directory
            </Button>
            <Button
              size="sm"
              variant="outline"
              className="w-full h-8 text-[12px]"
              onClick={openHistory}
            >
              <History className="h-3 w-3 mr-1.5" />
              Pull History
            </Button>
            <Button
              size="sm"
              variant="outline"
              className="w-full h-8 text-[12px]"
              onClick={downloadFiles}
              disabled={selectedSlides === 0 || downloading}
            >
              <Package className="h-3 w-3 mr-1.5" />
              {downloading ? 'Preparing ZIP...' : 'Download ZIP'}
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
