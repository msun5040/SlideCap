import { useState, useEffect, useMemo } from 'react'
import { Download, FileDown, Image, Loader2, Search, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'

const API_BASE = 'http://localhost:8000'

interface DownloadModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  slideHashes: string[]
  jobId: number
}

export function DownloadModal({ open, onOpenChange, slideHashes, jobId }: DownloadModalProps) {
  const [filenames, setFilenames] = useState<string[]>([])
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [includeWsi, setIncludeWsi] = useState(false)
  const [loading, setLoading] = useState(false)
  const [downloading, setDownloading] = useState(false)

  // Filter state
  const [filterInput, setFilterInput] = useState('')
  const [activeFilters, setActiveFilters] = useState<string[]>([])

  // Fetch available filenames when modal opens
  useEffect(() => {
    if (!open || !jobId) return
    setLoading(true)
    setFilterInput('')
    setActiveFilters([])
    const hashParam = slideHashes.join(',')
    fetch(`${API_BASE}/jobs/${jobId}/output-filenames?slide_hashes=${encodeURIComponent(hashParam)}`)
      .then((res) => (res.ok ? res.json() : []))
      .then((names: string[]) => {
        setFilenames(names)
        // Pre-select geojson files by default
        const defaults = new Set(
          names.filter(
            (n) => n.endsWith('.geojson.snappy') || n.endsWith('.geojson')
          )
        )
        setSelected(defaults)
      })
      .catch(() => setFilenames([]))
      .finally(() => setLoading(false))
  }, [open, jobId])

  // Derive unique extensions from filenames
  const extensions = useMemo(() => {
    const exts = new Set<string>()
    for (const name of filenames) {
      // Extract compound extension: e.g. "cells.geojson.snappy" → ".geojson.snappy"
      const dotIdx = name.indexOf('.')
      if (dotIdx !== -1) exts.add(name.substring(dotIdx))
    }
    return Array.from(exts).sort()
  }, [filenames])

  // Files visible after applying active filters
  const visibleFiles = useMemo(() => {
    if (activeFilters.length === 0) return filenames
    return filenames.filter((name) =>
      activeFilters.some((f) => name.includes(f))
    )
  }, [filenames, activeFilters])

  // Filter suggestions based on input
  const filterSuggestions = useMemo(() => {
    if (!filterInput.trim()) return extensions
    const q = filterInput.toLowerCase()
    return extensions.filter((ext) => ext.toLowerCase().includes(q))
  }, [extensions, filterInput])

  const addFilter = (filter: string) => {
    if (activeFilters.includes(filter)) return
    const newFilters = [...activeFilters, filter]
    setActiveFilters(newFilters)
    setFilterInput('')
    // Auto-select files matching the new filter set
    const matching = filenames.filter((name) =>
      newFilters.some((f) => name.includes(f))
    )
    setSelected(new Set(matching))
  }

  const removeFilter = (filter: string) => {
    const newFilters = activeFilters.filter((f) => f !== filter)
    setActiveFilters(newFilters)
    if (newFilters.length === 0) {
      // No filters — keep current selection as-is
      return
    }
    // Re-select files matching remaining filters
    const matching = filenames.filter((name) =>
      newFilters.some((f) => name.includes(f))
    )
    setSelected(new Set(matching))
  }

  const addFilterFromInput = () => {
    const val = filterInput.trim()
    if (!val) return
    addFilter(val)
  }

  const toggleFile = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  const selectAllVisible = () => {
    setSelected((prev) => {
      const next = new Set(prev)
      for (const name of visibleFiles) next.add(name)
      return next
    })
  }

  const selectNoneVisible = () => {
    setSelected((prev) => {
      const next = new Set(prev)
      for (const name of visibleFiles) next.delete(name)
      return next
    })
  }

  const doDownload = async () => {
    if (selected.size === 0 && !includeWsi) return
    setDownloading(true)
    try {
      const res = await fetch(`${API_BASE}/download-bundle`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          slide_hashes: slideHashes,
          job_id: jobId,
          include_filenames: Array.from(selected),
          include_wsi: includeWsi,
        }),
      })
      if (res.ok) {
        const blob = await res.blob()
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `job_${jobId}_bundle.zip`
        a.click()
        URL.revokeObjectURL(url)
        onOpenChange(false)
      } else {
        const err = await res.json().catch(() => ({ detail: 'Download failed' }))
        alert(err.detail || 'Download failed')
      }
    } catch (e) {
      console.error('Bundle download failed:', e)
      alert('Download failed')
    } finally {
      setDownloading(false)
    }
  }

  const formatName = (name: string) => {
    if (name.endsWith('.snappy')) return name.replace(/\.snappy$/, '') + ' *'
    return name
  }

  const isImage = (name: string) =>
    /\.(png|jpg|jpeg|tif|tiff|bmp|gif|svg)$/i.test(name)

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Download className="h-5 w-5" />
            Download Bundle
          </DialogTitle>
          <DialogDescription>
            {slideHashes.length} slide{slideHashes.length !== 1 ? 's' : ''} selected
            {' \u00b7 '}Job #{jobId}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          {/* WSI toggle */}
          <label className="flex items-center gap-3 rounded-md border p-3 cursor-pointer hover:bg-muted/50">
            <Checkbox
              checked={includeWsi}
              onCheckedChange={(v) => setIncludeWsi(v === true)}
            />
            <Image className="h-4 w-4 text-blue-500 shrink-0" />
            <div>
              <p className="text-sm font-medium">Include original H&E slide (.svs)</p>
              <p className="text-xs text-muted-foreground">Large files — may take a while</p>
            </div>
          </label>

          {/* File filter + list */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <p className="text-sm font-medium">Analysis output files</p>
              <div className="flex gap-2">
                <button
                  className="text-xs text-primary hover:underline"
                  onClick={selectAllVisible}
                >
                  All{activeFilters.length > 0 ? ' visible' : ''}
                </button>
                <button
                  className="text-xs text-muted-foreground hover:underline"
                  onClick={selectNoneVisible}
                >
                  None
                </button>
              </div>
            </div>

            {/* Filter input */}
            {!loading && filenames.length > 0 && (
              <div className="mb-2 space-y-2">
                <div className="relative">
                  <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                  <Input
                    placeholder="Filter by extension (e.g. .geojson, .pt, cells)..."
                    value={filterInput}
                    onChange={(e) => setFilterInput(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault()
                        // If there's exactly one suggestion, use it; otherwise use raw input
                        if (filterSuggestions.length === 1) {
                          addFilter(filterSuggestions[0])
                        } else {
                          addFilterFromInput()
                        }
                      }
                    }}
                    className="pl-8 h-8 text-sm"
                  />
                </div>

                {/* Extension quick-pick chips */}
                {filterInput && filterSuggestions.length > 0 && (
                  <div className="flex flex-wrap gap-1">
                    {filterSuggestions.map((ext) => (
                      <button
                        key={ext}
                        className="inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-mono hover:bg-muted/50 transition-colors"
                        onClick={() => addFilter(ext)}
                      >
                        {ext}
                      </button>
                    ))}
                  </div>
                )}

                {/* Active filters */}
                {activeFilters.length > 0 && (
                  <div className="flex flex-wrap gap-1">
                    {activeFilters.map((f) => (
                      <Badge
                        key={f}
                        variant="secondary"
                        className="font-mono text-xs gap-1 cursor-pointer hover:bg-destructive/10"
                        onClick={() => removeFilter(f)}
                      >
                        {f}
                        <X className="h-3 w-3" />
                      </Badge>
                    ))}
                    <button
                      className="text-xs text-muted-foreground hover:underline ml-1"
                      onClick={() => {
                        setActiveFilters([])
                      }}
                    >
                      Clear filters
                    </button>
                  </div>
                )}
              </div>
            )}

            {loading ? (
              <div className="flex items-center gap-2 py-4 justify-center text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                <span className="text-sm">Loading file list...</span>
              </div>
            ) : filenames.length === 0 ? (
              <p className="text-sm text-muted-foreground py-2">
                No output files found for this job.
              </p>
            ) : (
              <div className="space-y-1 max-h-52 overflow-y-auto">
                {visibleFiles.map((name) => (
                  <label
                    key={name}
                    className="flex items-center gap-3 rounded px-2 py-1.5 cursor-pointer hover:bg-muted/50 min-w-0 overflow-hidden"
                  >
                    <Checkbox
                      checked={selected.has(name)}
                      onCheckedChange={() => toggleFile(name)}
                      className="shrink-0"
                    />
                    {isImage(name) ? (
                      <Image className="h-3.5 w-3.5 text-blue-500 shrink-0" />
                    ) : (
                      <FileDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                    )}
                    <span className="text-sm font-mono truncate min-w-0" title={name}>
                      {formatName(name)}
                    </span>
                  </label>
                ))}
                {activeFilters.length > 0 && visibleFiles.length === 0 && (
                  <p className="text-sm text-muted-foreground py-2 text-center">
                    No files match the current filters.
                  </p>
                )}
              </div>
            )}

            {/* Hidden selected count when filtering */}
            {activeFilters.length > 0 && selected.size > 0 && (
              <p className="text-xs text-muted-foreground mt-1">
                {selected.size} file{selected.size !== 1 ? 's' : ''} selected total
                {visibleFiles.length < filenames.length &&
                  ` (showing ${visibleFiles.length} of ${filenames.length})`}
              </p>
            )}

            {filenames.some((n) => n.endsWith('.snappy')) && (
              <p className="text-xs text-muted-foreground mt-2">
                * .snappy files will be decompressed automatically
              </p>
            )}
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            onClick={doDownload}
            disabled={downloading || (selected.size === 0 && !includeWsi)}
          >
            {downloading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Downloading...
              </>
            ) : (
              <>
                <Download className="mr-2 h-4 w-4" />
                Download ZIP
                {selected.size > 0 && ` (${selected.size})`}
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
