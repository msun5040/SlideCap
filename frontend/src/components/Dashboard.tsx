import { useState, useEffect, useCallback, useRef } from 'react'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Badge } from '@/components/ui/badge'
import {
  Microscope,
  FolderOpen,
  HardDrive,
  Inbox,
  ScanSearch,
  ArrowRightLeft,
  AlertTriangle,
  CheckCircle,
  XCircle,
  Clock,
  Loader2,
  Trash2,
  FileSpreadsheet,
  Upload,
  Download,
  Tags,
  RefreshCw,
  Database,
  X,
  Plus,
  Tag as TagIcon,
} from 'lucide-react'

import { getApiBase } from '@/api'
import { Input } from '@/components/ui/input'
import { SortableHeader } from '@/components/SortableHeader'
import { useSortable } from '@/hooks/useSortable'
import type { Tag } from '@/types/slide'

// Preset colors for tags (matches SlideLibrary)
const PRESET_COLORS = [
  '#EF4444', '#F97316', '#EAB308', '#22C55E', '#14B8A6',
  '#3B82F6', '#8B5CF6', '#EC4899', '#6B7280',
]

interface StagingFile {
  filename: string
  size_bytes: number
  parsed: boolean
  accession: string | null
  block_id: string | null
  slide_number: string | null
  stain_type: string | null
  year: number | null
  destination: string | null
  conflict: boolean
  conflict_reason: string | null
}

interface SortStatus {
  running: boolean
  done: boolean
  total: number
  current: number
  current_file: string
  sorted: number
  skipped: number
  errors: string[]
}

interface DashboardSummary {
  library: {
    total_slides: number
    total_cases: number
    years: Record<string, number>
  }
  staging: {
    count: number
    total_size_bytes: number
  }
  recent_jobs: {
    id: number
    model_name: string
    status: string
    slide_count: number
    submitted_at: string | null
    completed_at: string | null
  }[]
  storage: {
    network_root: string
    slides_size_mb: number
    analyses_size_mb: number
    staging_size_mb: number
  }
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(i > 1 ? 1 : 0)} ${units[i]}`
}

function formatMB(mb: number): string {
  if (mb >= 1024) return `${(mb / 1024).toFixed(1)} GB`
  return `${mb} MB`
}

function timeAgo(dateStr: string | null): string {
  if (!dateStr) return ''
  const diff = Date.now() - new Date(dateStr).getTime()
  const mins = Math.floor(diff / 60000)
  if (mins < 1) return 'just now'
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  const days = Math.floor(hrs / 24)
  return `${days}d ago`
}

function StatusBadge({ status }: { status: string }) {
  const variants: Record<string, { class: string; icon: React.ReactNode }> = {
    completed: { class: 'bg-green-100 text-green-800', icon: <CheckCircle className="h-3 w-3" /> },
    failed: { class: 'bg-red-100 text-red-800', icon: <XCircle className="h-3 w-3" /> },
    running: { class: 'bg-blue-100 text-blue-800', icon: <Loader2 className="h-3 w-3 animate-spin" /> },
    pending: { class: 'bg-yellow-100 text-yellow-800', icon: <Clock className="h-3 w-3" /> },
    transferring: { class: 'bg-purple-100 text-purple-800', icon: <ArrowRightLeft className="h-3 w-3" /> },
  }
  const v = variants[status] || variants.pending
  return (
    <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${v.class}`}>
      {v.icon} {status}
    </span>
  )
}

interface DashboardProps {
  onSortStarted: () => void
  sortStatus: SortStatus | null
}

export function Dashboard({ onSortStarted, sortStatus }: DashboardProps) {
  const [summary, setSummary] = useState<DashboardSummary | null>(null)
  const [stagingFiles, setStagingFiles] = useState<StagingFile[] | null>(null)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [scanning, setScanning] = useState(false)
  const [sorting, setSorting] = useState(false)

  // Staging tag state
  const [stagingTags, setStagingTags] = useState<{ name: string; color: string }[]>([])
  const [stagingTagInput, setStagingTagInput] = useState('')
  const [stagingTagColor, setStagingTagColor] = useState(PRESET_COLORS[0])
  const [showStagingTagInput, setShowStagingTagInput] = useState(false)
  const [stagingTagSuggestions, setStagingTagSuggestions] = useState<Tag[]>([])
  const [showStagingTagSuggestions, setShowStagingTagSuggestions] = useState(false)
  const stagingTagInputRef = useRef<HTMLInputElement>(null)
  const stagingTagSuggestionsRef = useRef<HTMLDivElement>(null)

  // Indexing state
  const [indexing, setIndexing] = useState(false)
  const [indexResult, setIndexResult] = useState<{
    type: 'full' | 'incremental'
    new_slides: number
    skipped: number
    skipped_files: string[]
    errors: string[]
  } | null>(null)

  // CSV import state
  const [importFile, setImportFile] = useState<File | null>(null)
  const [importMode, setImportMode] = useState<'merge' | 'replace'>('merge')
  const [importPreview, setImportPreview] = useState<{ headers: string[]; rows: string[][]; totalRows: number } | null>(null)
  const [importing, setImporting] = useState(false)
  const [importResult, setImportResult] = useState<{ matched: number; unmatched: number; tags_added: number; mode: string } | null>(null)
  const [dragOver, setDragOver] = useState(false)

  const fetchSummary = useCallback(async () => {
    try {
      const res = await fetch(`${getApiBase()}/dashboard/summary`)
      if (res.ok) setSummary(await res.json())
    } catch { /* ignore */ }
  }, [])

  useEffect(() => {
    fetchSummary()
  }, [fetchSummary])

  const handleScan = async () => {
    setScanning(true)
    try {
      const res = await fetch(`${getApiBase()}/staging/scan`)
      if (res.ok) {
        const files: StagingFile[] = await res.json()
        setStagingFiles(files)
        setSelected(new Set())
      }
    } catch { /* ignore */ }
    setScanning(false)
  }

  const handleSort = async (filenames?: string[]) => {
    setSorting(true)
    try {
      const body: Record<string, unknown> = filenames ? { filenames } : { filenames: [] }
      if (stagingTags.length > 0) {
        body.tags = stagingTags.map(t => t.name)
        body.tag_color = stagingTags[0].color
      }
      const res = await fetch(`${getApiBase()}/staging/sort`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (res.ok) {
        onSortStarted()
      }
    } catch { /* ignore */ }
    setSorting(false)
  }

  // Staging tag helpers
  const fetchStagingTagSuggestions = async (query: string) => {
    if (!query.trim()) { setStagingTagSuggestions([]); return }
    try {
      const res = await fetch(`${getApiBase()}/tags/search?q=${encodeURIComponent(query)}`)
      if (res.ok) setStagingTagSuggestions(await res.json())
    } catch { /* ignore */ }
  }

  const addStagingTag = (name: string, color?: string) => {
    const trimmed = name.trim()
    if (!trimmed) return
    if (stagingTags.some(t => t.name.toLowerCase() === trimmed.toLowerCase())) return
    setStagingTags(prev => [...prev, { name: trimmed, color: color || stagingTagColor }])
    setStagingTagInput('')
    setStagingTagSuggestions([])
    setShowStagingTagSuggestions(false)
  }

  const removeStagingTag = (name: string) => {
    setStagingTags(prev => prev.filter(t => t.name !== name))
  }

  const handleDeleteStaging = async (filename: string) => {
    try {
      const res = await fetch(`${getApiBase()}/staging/file/${encodeURIComponent(filename)}`, { method: 'DELETE' })
      if (res.ok) {
        setStagingFiles(prev => prev?.filter(f => f.filename !== filename) ?? null)
      } else {
        const err = await res.json().catch(() => ({ detail: `Failed (${res.status})` }))
        alert(`Could not delete ${filename}: ${err.detail}`)
      }
    } catch (e) {
      alert(`Could not delete ${filename}: ${e}`)
    }
  }

  const handleDeleteAllConflicts = async () => {
    const conflicts = stagingFiles?.filter(f => f.conflict) ?? []
    for (const f of conflicts) {
      await handleDeleteStaging(f.filename)
    }
  }

  const handleIndex = async (type: 'full' | 'incremental') => {
    setIndexing(true)
    setIndexResult(null)
    try {
      const endpoint = type === 'full' ? '/index/full' : '/index/incremental'
      const res = await fetch(`${getApiBase()}${endpoint}`, { method: 'POST' })
      if (res.ok) {
        const data = await res.json()
        setIndexResult({
          type,
          new_slides: type === 'full' ? (data.slides_indexed ?? 0) : (data.new_slides_indexed ?? 0),
          skipped: data.files_skipped ?? 0,
          skipped_files: data.skipped_files ?? [],
          errors: data.errors ?? [],
        })
        fetchSummary()
      }
    } catch { /* ignore */ }
    setIndexing(false)
  }

  const parseCSVPreview = (file: File) => {
    const reader = new FileReader()
    reader.onload = (e) => {
      const text = (e.target?.result as string).replace(/^\uFEFF/, '') // strip BOM
      const lines = text.split('\n').map(l => l.trim()).filter(Boolean)
      if (lines.length < 2) return
      const headers = lines[0].split(',').map(h => h.replace(/^"|"$/g, '').trim())
      const rows = lines.slice(1, 6).map(l =>
        l.split(',').map(c => c.replace(/^"|"$/g, '').trim())
      )
      setImportPreview({ headers, rows, totalRows: lines.length - 1 })
    }
    reader.readAsText(file)
  }

  const handleImportFile = (file: File) => {
    setImportFile(file)
    setImportResult(null)
    parseCSVPreview(file)
  }

  const handleImportApply = async () => {
    if (!importFile) return
    setImporting(true)
    setImportResult(null)
    try {
      const form = new FormData()
      form.append('file', importFile)
      const res = await fetch(`${getApiBase()}/import/slides-csv?mode=${importMode}`, {
        method: 'POST',
        body: form,
      })
      if (res.ok) {
        const result = await res.json()
        setImportResult(result)
        setImportFile(null)
        setImportPreview(null)
      }
    } catch { /* ignore */ }
    setImporting(false)
  }

  // Re-scan and refresh when sort completes
  useEffect(() => {
    if (sortStatus?.done && !sortStatus.running) {
      handleScan()
      fetchSummary()
    }
  }, [sortStatus?.done, sortStatus?.running])

  const { sorted: sortedStagingFiles, sortConfig: stagingSortConfig, handleSort: handleStagingSort } = useSortable(stagingFiles ?? [])
  const selectableFiles = stagingFiles?.filter(f => f.parsed && !f.conflict) || []
  const allSelected = selectableFiles.length > 0 && selectableFiles.every(f => selected.has(f.filename))

  const toggleSelect = (filename: string) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(filename)) next.delete(filename)
      else next.add(filename)
      return next
    })
  }

  const toggleAll = () => {
    if (allSelected) {
      setSelected(new Set())
    } else {
      setSelected(new Set(selectableFiles.map(f => f.filename)))
    }
  }

  const totalStorageMB = summary
    ? summary.storage.slides_size_mb + summary.storage.analyses_size_mb + summary.storage.staging_size_mb
    : 0

  // Year breakdown chart
  const yearEntries = summary
    ? Object.entries(summary.library.years)
        .map(([y, c]) => ({ year: y, count: c }))
        .sort((a, b) => b.year.localeCompare(a.year))
    : []
  const maxYearCount = Math.max(...yearEntries.map(e => e.count), 1)

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Dashboard</h1>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="rounded-lg border bg-card p-5">
          <div className="flex items-center gap-3 mb-2">
            <div className="rounded-md bg-blue-100 p-2"><Microscope className="h-5 w-5 text-blue-600" /></div>
            <span className="text-sm text-muted-foreground">Total Slides</span>
          </div>
          <p className="text-3xl font-bold">{summary?.library.total_slides.toLocaleString() ?? '—'}</p>
        </div>
        <div className="rounded-lg border bg-card p-5">
          <div className="flex items-center gap-3 mb-2">
            <div className="rounded-md bg-green-100 p-2"><FolderOpen className="h-5 w-5 text-green-600" /></div>
            <span className="text-sm text-muted-foreground">Total Cases</span>
          </div>
          <p className="text-3xl font-bold">{summary?.library.total_cases.toLocaleString() ?? '—'}</p>
        </div>
        <div className="rounded-lg border bg-card p-5">
          <div className="flex items-center gap-3 mb-2">
            <div className="rounded-md bg-orange-100 p-2"><Inbox className="h-5 w-5 text-orange-600" /></div>
            <span className="text-sm text-muted-foreground">Staging</span>
          </div>
          <p className="text-3xl font-bold">{summary?.staging.count ?? '—'}</p>
          {summary && summary.staging.count > 0 && (
            <p className="text-xs text-muted-foreground mt-1">{formatBytes(summary.staging.total_size_bytes)} ready</p>
          )}
        </div>
        <div className="rounded-lg border bg-card p-5">
          <div className="flex items-center gap-3 mb-2">
            <div className="rounded-md bg-purple-100 p-2"><HardDrive className="h-5 w-5 text-purple-600" /></div>
            <span className="text-sm text-muted-foreground">Storage</span>
          </div>
          <p className="text-3xl font-bold">{summary ? formatMB(totalStorageMB) : '—'}</p>
          {summary && (
            <p className="text-xs text-muted-foreground mt-1">
              Slides: {formatMB(summary.storage.slides_size_mb)} &middot; Analyses: {formatMB(summary.storage.analyses_size_mb)}
            </p>
          )}
        </div>
      </div>

      {/* Staging Section */}
      <div className="rounded-lg border bg-card">
        <div className="flex items-center justify-between p-4 border-b">
          <h2 className="text-lg font-semibold">Slide Staging</h2>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={handleScan} disabled={scanning}>
              {scanning ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <ScanSearch className="h-4 w-4 mr-1" />}
              Scan Staging Folder
            </Button>
            {stagingFiles && stagingFiles.some(f => f.conflict) && (
              <Button variant="outline" size="sm" onClick={handleDeleteAllConflicts} className="text-red-600 hover:text-red-700">
                <Trash2 className="h-4 w-4 mr-1" />
                Remove Conflicts ({stagingFiles.filter(f => f.conflict).length})
              </Button>
            )}
            {stagingFiles && selectableFiles.length > 0 && (
              <Button
                size="sm"
                onClick={() => handleSort()}
                disabled={sorting || sortStatus?.running}
              >
                {sorting ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <ArrowRightLeft className="h-4 w-4 mr-1" />}
                Sort All ({selectableFiles.length}){stagingTags.length > 0 ? ` + ${stagingTags.length} tag${stagingTags.length > 1 ? 's' : ''}` : ''}
              </Button>
            )}
          </div>
        </div>

        {/* Staging Tags Bar */}
        {stagingFiles && stagingFiles.length > 0 && (
          <div className="px-4 py-3 border-b bg-muted/30">
            <div className="flex items-center gap-2 flex-wrap">
              <div className="flex items-center gap-1.5 text-sm text-muted-foreground shrink-0">
                <TagIcon className="h-4 w-4" />
                <span className="font-medium">Tags on sort:</span>
              </div>
              {stagingTags.map(t => (
                <span
                  key={t.name}
                  className="inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium text-white"
                  style={{ backgroundColor: t.color }}
                >
                  {t.name}
                  <button onClick={() => removeStagingTag(t.name)} className="hover:opacity-70">
                    <X className="h-3 w-3" />
                  </button>
                </span>
              ))}
              {showStagingTagInput ? (
                <div className="relative">
                  <div className="flex items-center gap-1">
                    <div className="flex items-center gap-1 border rounded-md px-1 bg-background">
                      {/* Color picker */}
                      <div className="flex items-center gap-0.5 pr-1 border-r">
                        {PRESET_COLORS.map(c => (
                          <button
                            key={c}
                            className={`w-4 h-4 rounded-full border-2 transition-all ${stagingTagColor === c ? 'border-foreground scale-110' : 'border-transparent hover:border-muted-foreground/50'}`}
                            style={{ backgroundColor: c }}
                            onClick={() => setStagingTagColor(c)}
                          />
                        ))}
                      </div>
                      <input
                        ref={stagingTagInputRef}
                        type="text"
                        placeholder="Tag name..."
                        className="border-0 bg-transparent text-sm py-1 px-1.5 outline-none w-32"
                        value={stagingTagInput}
                        onChange={e => {
                          setStagingTagInput(e.target.value)
                          fetchStagingTagSuggestions(e.target.value)
                          setShowStagingTagSuggestions(true)
                        }}
                        onKeyDown={e => {
                          if (e.key === 'Enter') {
                            e.preventDefault()
                            addStagingTag(stagingTagInput, stagingTagColor)
                          } else if (e.key === 'Escape') {
                            setShowStagingTagInput(false)
                            setStagingTagInput('')
                            setShowStagingTagSuggestions(false)
                          }
                        }}
                        onFocus={() => { if (stagingTagInput) setShowStagingTagSuggestions(true) }}
                        onBlur={() => setTimeout(() => setShowStagingTagSuggestions(false), 150)}
                        autoFocus
                      />
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 w-7 p-0"
                      onClick={() => { setShowStagingTagInput(false); setStagingTagInput(''); setShowStagingTagSuggestions(false) }}
                    >
                      <X className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                  {/* Suggestions dropdown */}
                  {showStagingTagSuggestions && stagingTagSuggestions.length > 0 && (
                    <div
                      ref={stagingTagSuggestionsRef}
                      className="absolute top-full left-0 mt-1 w-56 bg-popover border rounded-md shadow-md z-20 py-1"
                    >
                      {stagingTagSuggestions.map(tag => (
                        <button
                          key={tag.id}
                          className="w-full text-left px-3 py-1.5 text-sm hover:bg-muted flex items-center gap-2"
                          onMouseDown={e => { e.preventDefault(); addStagingTag(tag.name, tag.color || stagingTagColor) }}
                        >
                          <span className="w-3 h-3 rounded-full shrink-0" style={{ backgroundColor: tag.color || '#6B7280' }} />
                          {tag.name}
                          {tag.slide_count != null && (
                            <span className="text-xs text-muted-foreground ml-auto">{tag.slide_count} slides</span>
                          )}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              ) : (
                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 text-xs"
                  onClick={() => setShowStagingTagInput(true)}
                >
                  <Plus className="h-3 w-3 mr-1" />
                  Add Tag
                </Button>
              )}
              {stagingTags.length > 0 && (
                <span className="text-xs text-muted-foreground ml-1">
                  Applied to all slides after sorting
                </span>
              )}
            </div>
          </div>
        )}

        {stagingFiles === null ? (
          <div className="p-8 text-center text-muted-foreground text-sm">
            Click "Scan Staging Folder" to check for new slides
          </div>
        ) : stagingFiles.length === 0 ? (
          <div className="p-8 text-center text-muted-foreground text-sm">
            No .svs files found in staging folder
          </div>
        ) : (
          <div className="p-4">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-muted-foreground border-b">
                  <th className="pb-2 pr-3 w-8">
                    <Checkbox
                      checked={allSelected}
                      onCheckedChange={toggleAll}
                      disabled={selectableFiles.length === 0}
                    />
                  </th>
                  <th className="pb-2 pr-3"><SortableHeader label="Filename" sortKey="filename" sortConfig={stagingSortConfig} onSort={handleStagingSort} /></th>
                  <th className="pb-2 pr-3 w-16"><SortableHeader label="Year" sortKey="year" sortConfig={stagingSortConfig} onSort={handleStagingSort} /></th>
                  <th className="pb-2 pr-3 w-16"><SortableHeader label="Stain" sortKey="stain_type" sortConfig={stagingSortConfig} onSort={handleStagingSort} /></th>
                  <th className="pb-2 pr-3 w-20 text-right"><SortableHeader label="Size" sortKey="size_bytes" sortConfig={stagingSortConfig} onSort={handleStagingSort} /></th>
                  <th className="pb-2 w-48">Destination</th>
                </tr>
              </thead>
              <tbody>
                {sortedStagingFiles.map(f => {
                  const disabled = !f.parsed || f.conflict
                  return (
                    <tr key={f.filename} className={`border-b last:border-0 ${disabled ? 'opacity-50' : ''}`}>
                      <td className="py-2 pr-3">
                        <Checkbox
                          checked={selected.has(f.filename)}
                          onCheckedChange={() => toggleSelect(f.filename)}
                          disabled={disabled}
                        />
                      </td>
                      <td className="py-2 pr-3 font-mono text-xs truncate max-w-75">
                        {!f.parsed && <AlertTriangle className="h-3.5 w-3.5 text-yellow-500 inline mr-1" />}
                        {f.conflict && <AlertTriangle className="h-3.5 w-3.5 text-red-500 inline mr-1" />}
                        {f.filename}
                      </td>
                      <td className="py-2 pr-3">{f.year ?? '—'}</td>
                      <td className="py-2 pr-3">
                        {f.stain_type ? <Badge variant="outline">{f.stain_type}</Badge> : '—'}
                      </td>
                      <td className="py-2 pr-3 text-right text-muted-foreground">{formatBytes(f.size_bytes)}</td>
                      <td className="py-2 text-xs text-muted-foreground">
                        {f.conflict ? (
                          <div className="flex items-center gap-2">
                            <span className="text-red-500">Conflict: {f.conflict_reason || 'file exists'}</span>
                            <button
                              className="p-0.5 rounded text-muted-foreground hover:text-red-600 hover:bg-red-50 transition-colors"
                              title="Remove from staging"
                              onClick={() => handleDeleteStaging(f.filename)}
                            >
                              <Trash2 className="h-3.5 w-3.5" />
                            </button>
                          </div>
                        ) : f.destination ? (
                          <span>&rarr; {f.destination}</span>
                        ) : (
                          <span className="text-yellow-600">Cannot parse filename</span>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>

            {selected.size > 0 && (
              <div className="mt-3 flex items-center justify-between">
                <div className="flex items-center gap-1.5 flex-wrap">
                  {stagingTags.length > 0 && (
                    <>
                      <TagIcon className="h-3.5 w-3.5 text-muted-foreground" />
                      {stagingTags.map(t => (
                        <span
                          key={t.name}
                          className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium text-white"
                          style={{ backgroundColor: t.color }}
                        >
                          {t.name}
                        </span>
                      ))}
                    </>
                  )}
                </div>
                <Button size="sm" onClick={() => handleSort([...selected])} disabled={sorting || sortStatus?.running}>
                  {sorting ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <ArrowRightLeft className="h-4 w-4 mr-1" />}
                  Sort Selected ({selected.size}){stagingTags.length > 0 ? ` + ${stagingTags.length} tag${stagingTags.length > 1 ? 's' : ''}` : ''}
                </Button>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Bottom Row: Year Breakdown + Recent Activity */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Year Breakdown */}
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-lg font-semibold mb-4">Slides by Year</h2>
          {yearEntries.length === 0 ? (
            <p className="text-sm text-muted-foreground">No data</p>
          ) : (
            <div className="space-y-2">
              {yearEntries.map(({ year, count }) => (
                <div key={year} className="flex items-center gap-3">
                  <span className="w-12 text-sm font-medium text-right">{year}</span>
                  <div className="flex-1 h-6 bg-muted rounded overflow-hidden">
                    <div
                      className="h-full bg-blue-500 rounded transition-all"
                      style={{ width: `${(count / maxYearCount) * 100}%` }}
                    />
                  </div>
                  <span className="w-14 text-sm text-muted-foreground text-right">{count.toLocaleString()}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Recent Activity */}
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-lg font-semibold mb-4">Recent Activity</h2>
          {!summary || summary.recent_jobs.length === 0 ? (
            <p className="text-sm text-muted-foreground">No recent jobs</p>
          ) : (
            <div className="space-y-3">
              {summary.recent_jobs.map(job => (
                <div key={job.id} className="flex items-center justify-between py-2 border-b last:border-0">
                  <div className="flex items-center gap-3">
                    <StatusBadge status={job.status} />
                    <div>
                      <p className="text-sm font-medium">Job #{job.id} &middot; {job.model_name}</p>
                      <p className="text-xs text-muted-foreground">{job.slide_count} slide{job.slide_count !== 1 ? 's' : ''}</p>
                    </div>
                  </div>
                  <span className="text-xs text-muted-foreground">
                    {timeAgo(job.completed_at || job.submitted_at)}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Library Index */}
      <div className="rounded-lg border bg-card">
        <div className="flex items-center gap-2 p-4 border-b">
          <Database className="h-5 w-5 text-muted-foreground" />
          <h2 className="text-lg font-semibold">Library Index</h2>
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-0 divide-y lg:divide-y-0 lg:divide-x">

          {/* Incremental */}
          <div className="p-6 flex flex-col gap-4">
            <div className="flex items-start gap-3">
              <div className="rounded-md bg-blue-100 p-2 shrink-0">
                <RefreshCw className="h-5 w-5 text-blue-600" />
              </div>
              <div>
                <h3 className="font-medium">Incremental Index</h3>
                <p className="text-sm text-muted-foreground mt-0.5">
                  Scans for new slides only — much faster. Run this after sorting new files from staging.
                </p>
              </div>
            </div>
            <Button
              size="sm"
              variant="outline"
              onClick={() => handleIndex('incremental')}
              disabled={indexing}
              className="w-fit"
            >
              {indexing ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-1" />}
              Run Incremental Index
            </Button>
          </div>

          {/* Full */}
          <div className="p-6 flex flex-col gap-4">
            <div className="flex items-start gap-3">
              <div className="rounded-md bg-orange-100 p-2 shrink-0">
                <Database className="h-5 w-5 text-orange-600" />
              </div>
              <div>
                <h3 className="font-medium">Full Index</h3>
                <p className="text-sm text-muted-foreground mt-0.5">
                  Rescans the entire slide library. Marks missing files and adds any slides not yet in the database. Takes longer on large collections.
                </p>
              </div>
            </div>
            <Button
              size="sm"
              variant="outline"
              onClick={() => handleIndex('full')}
              disabled={indexing}
              className="w-fit"
            >
              {indexing ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <Database className="h-4 w-4 mr-1" />}
              Run Full Index
            </Button>
          </div>

        </div>

        {/* Result */}
        {indexResult && (
          <div className="border-t p-4">
            <div className="rounded-lg border border-green-200 bg-green-50 p-4 space-y-1">
              <p className="text-sm font-medium text-green-800 flex items-center gap-1.5">
                <CheckCircle className="h-4 w-4" />
                {indexResult.type === 'full' ? 'Full index' : 'Incremental index'} complete
              </p>
              <div className="flex gap-6 pt-1">
                <div>
                  <p className="text-xl font-bold text-green-700">{indexResult.new_slides.toLocaleString()}</p>
                  <p className="text-xs text-green-600">new slides added</p>
                </div>
                <div>
                  <p className={`text-xl font-bold ${indexResult.skipped > 0 ? 'text-yellow-600' : 'text-green-700'}`}>
                    {indexResult.skipped.toLocaleString()}
                  </p>
                  <p className={`text-xs ${indexResult.skipped > 0 ? 'text-yellow-600' : 'text-green-600'}`}>skipped</p>
                </div>
                {indexResult.errors.length > 0 && (
                  <div>
                    <p className="text-xl font-bold text-red-600">{indexResult.errors.length}</p>
                    <p className="text-xs text-red-500">errors</p>
                  </div>
                )}
              </div>
              {indexResult.errors.length > 0 && (
                <div className="mt-2 space-y-0.5">
                  {indexResult.errors.slice(0, 5).map((e, i) => (
                    <p key={i} className="text-xs text-red-600 font-mono">{typeof e === 'string' ? e : JSON.stringify(e)}</p>
                  ))}
                  {indexResult.errors.length > 5 && (
                    <p className="text-xs text-red-500">+{indexResult.errors.length - 5} more errors</p>
                  )}
                </div>
              )}
              {indexResult.skipped_files.length > 0 && (
                <div className="mt-2 space-y-0.5">
                  <p className="text-xs font-medium text-yellow-700">Skipped files (unrecognized filename format):</p>
                  {indexResult.skipped_files.slice(0, 10).map((f, i) => (
                    <p key={i} className="text-xs text-yellow-600 font-mono">{f}</p>
                  ))}
                  {indexResult.skipped_files.length > 10 && (
                    <p className="text-xs text-yellow-500">+{indexResult.skipped_files.length - 10} more skipped</p>
                  )}
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Data Management */}
      <div className="rounded-lg border bg-card">
        <div className="flex items-center gap-2 p-4 border-b">
          <FileSpreadsheet className="h-5 w-5 text-muted-foreground" />
          <h2 className="text-lg font-semibold">Data Management</h2>
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-0 divide-y lg:divide-y-0 lg:divide-x">

          {/* Export */}
          <div className="p-6 flex flex-col gap-4">
            <div className="flex items-start gap-3">
              <div className="rounded-md bg-green-100 p-2 shrink-0">
                <Download className="h-5 w-5 text-green-600" />
              </div>
              <div>
                <h3 className="font-medium">Export Slide Data</h3>
                <p className="text-sm text-muted-foreground mt-0.5">
                  Download a CSV with all slides, metadata, and tags. Use it as a template for bulk edits.
                </p>
              </div>
            </div>
            {summary && (
              <p className="text-xs text-muted-foreground">
                {summary.library.total_slides.toLocaleString()} slides across {summary.library.total_cases.toLocaleString()} cases
              </p>
            )}
            <a
              href={`${getApiBase()}/export/slides.csv`}
              download="slides_export.csv"
              className="inline-flex items-center gap-2 rounded-md bg-green-600 hover:bg-green-700 text-white px-4 py-2 text-sm font-medium transition-colors w-fit"
            >
              <Download className="h-4 w-4" />
              Download CSV
            </a>
          </div>

          {/* Import */}
          <div className="p-6 flex flex-col gap-4">
            <div className="flex items-start gap-3">
              <div className="rounded-md bg-blue-100 p-2 shrink-0">
                <Upload className="h-5 w-5 text-blue-600" />
              </div>
              <div>
                <h3 className="font-medium">Import / Bulk Tag</h3>
                <p className="text-sm text-muted-foreground mt-0.5">
                  Upload a modified CSV to bulk-apply tags. Slides are matched by <code className="text-xs bg-muted px-1 rounded">slide_hash</code>. Tags use semicolons as separators.
                </p>
              </div>
            </div>

            {/* Drag-and-drop zone */}
            {!importFile && (
              <div
                onDragOver={e => { e.preventDefault(); setDragOver(true) }}
                onDragLeave={() => setDragOver(false)}
                onDrop={e => {
                  e.preventDefault()
                  setDragOver(false)
                  const f = e.dataTransfer.files[0]
                  if (f && f.name.endsWith('.csv')) handleImportFile(f)
                }}
                className={`relative flex flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed p-8 text-center transition-colors cursor-pointer ${
                  dragOver ? 'border-blue-400 bg-blue-50' : 'border-muted hover:border-muted-foreground/40'
                }`}
                onClick={() => document.getElementById('csv-file-input')?.click()}
              >
                <Upload className="h-8 w-8 text-muted-foreground" />
                <p className="text-sm text-muted-foreground">Drop a CSV here or <span className="text-blue-600 underline">browse</span></p>
                <input
                  id="csv-file-input"
                  type="file"
                  accept=".csv"
                  className="sr-only"
                  onChange={e => {
                    const f = e.target.files?.[0]
                    if (f) handleImportFile(f)
                    e.target.value = ''
                  }}
                />
              </div>
            )}

            {/* Preview */}
            {importFile && importPreview && (
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2 text-sm">
                    <FileSpreadsheet className="h-4 w-4 text-muted-foreground" />
                    <span className="font-medium truncate max-w-50">{importFile.name}</span>
                    <span className="text-muted-foreground">({importPreview.totalRows.toLocaleString()} rows)</span>
                  </div>
                  <button
                    className="text-xs text-muted-foreground hover:text-foreground underline"
                    onClick={() => { setImportFile(null); setImportPreview(null); setImportResult(null) }}
                  >
                    Clear
                  </button>
                </div>

                {/* Preview table */}
                <div className="overflow-x-auto rounded-md border text-xs">
                  <table className="w-full">
                    <thead className="bg-muted">
                      <tr>
                        {importPreview.headers.map(h => (
                          <th key={h} className="px-2 py-1.5 text-left font-medium text-muted-foreground whitespace-nowrap">{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {importPreview.rows.map((row, i) => (
                        <tr key={i} className="border-t">
                          {row.map((cell, j) => (
                            <td key={j} className="px-2 py-1.5 max-w-40 truncate text-muted-foreground">{cell}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {importPreview.totalRows > 5 && (
                    <p className="text-center py-1.5 text-muted-foreground text-xs border-t">
                      +{(importPreview.totalRows - 5).toLocaleString()} more rows
                    </p>
                  )}
                </div>

                {/* Mode toggle */}
                <div className="flex items-center gap-1 rounded-lg bg-muted p-1 w-fit">
                  <button
                    className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${importMode === 'merge' ? 'bg-background shadow text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
                    onClick={() => setImportMode('merge')}
                  >
                    <Tags className="h-3 w-3 inline mr-1" />
                    Merge tags
                  </button>
                  <button
                    className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${importMode === 'replace' ? 'bg-background shadow text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
                    onClick={() => setImportMode('replace')}
                  >
                    <RefreshCw className="h-3 w-3 inline mr-1" />
                    Replace tags
                  </button>
                </div>
                <p className="text-xs text-muted-foreground -mt-1">
                  {importMode === 'merge'
                    ? 'Adds new tags without removing existing ones.'
                    : 'Clears all existing tags and sets exactly what is in the CSV.'}
                </p>

                <Button size="sm" onClick={handleImportApply} disabled={importing}>
                  {importing ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <Upload className="h-4 w-4 mr-1" />}
                  Apply Import
                </Button>
              </div>
            )}

            {/* Result */}
            {importResult && (
              <div className="rounded-lg border border-green-200 bg-green-50 p-4 space-y-1">
                <p className="text-sm font-medium text-green-800 flex items-center gap-1.5">
                  <CheckCircle className="h-4 w-4" /> Import complete
                </p>
                <div className="grid grid-cols-3 gap-2 pt-1">
                  <div className="text-center">
                    <p className="text-xl font-bold text-green-700">{importResult.matched}</p>
                    <p className="text-xs text-green-600">matched</p>
                  </div>
                  <div className="text-center">
                    <p className="text-xl font-bold text-green-700">{importResult.tags_added}</p>
                    <p className="text-xs text-green-600">tags applied</p>
                  </div>
                  <div className="text-center">
                    <p className={`text-xl font-bold ${importResult.unmatched > 0 ? 'text-yellow-600' : 'text-green-700'}`}>
                      {importResult.unmatched}
                    </p>
                    <p className={`text-xs ${importResult.unmatched > 0 ? 'text-yellow-600' : 'text-green-600'}`}>unmatched</p>
                  </div>
                </div>
                <p className="text-xs text-green-600 pt-1">Mode: {importResult.mode}</p>
              </div>
            )}
          </div>

        </div>
      </div>
    </div>
  )
}
