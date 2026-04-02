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

    </div>
  )
}
