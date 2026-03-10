import { useState, useEffect, useMemo } from 'react'
import { Download, FileDown, Image, Loader2, Search, X, ChevronDown, ChevronRight, FileText, Wand2 } from 'lucide-react'
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

import { getApiBase } from '@/api'

interface SlideGroup {
  slide_hash: string
  label: string
  files: string[]
  annotation_count: number
  is_local: boolean
}

interface DownloadModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  slideHashes: string[]
  jobId: number
  postprocessAvailable?: boolean
}

export function DownloadModal({ open, onOpenChange, slideHashes, jobId, postprocessAvailable }: DownloadModalProps) {
  const [groups, setGroups] = useState<SlideGroup[]>([])
  const [selected, setSelected] = useState<Set<string>>(new Set()) // "slideHash:filename"
  const [includeWsi, setIncludeWsi] = useState(false)
  const [includeAnnotations, setIncludeAnnotations] = useState(false)
  const [applyPostprocess, setApplyPostprocess] = useState(false)
  const [loading, setLoading] = useState(false)
  const [downloading, setDownloading] = useState(false)

  // Filter state
  const [filterInput, setFilterInput] = useState('')
  const [activeFilters, setActiveFilters] = useState<string[]>([])

  // Collapsed slide groups
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set())

  // Fetch grouped filenames when modal opens
  useEffect(() => {
    if (!open || !jobId) return
    setLoading(true)
    setFilterInput('')
    setActiveFilters([])
    setCollapsedGroups(new Set())
    const hashParam = slideHashes.join(',')
    fetch(`${getApiBase()}/jobs/${jobId}/output-filenames?slide_hashes=${encodeURIComponent(hashParam)}`)
      .then((res) => (res.ok ? res.json() : []))
      .then((data: SlideGroup[]) => {
        setGroups(data)
        // Pre-select geojson files
        const defaults = new Set<string>()
        for (const g of data) {
          for (const f of g.files) {
            if (f.endsWith('.geojson.snappy') || f.endsWith('.geojson')) {
              defaults.add(`${g.slide_hash}:${f}`)
            }
          }
        }
        setSelected(defaults)
      })
      .catch(() => setGroups([]))
      .finally(() => setLoading(false))
  }, [open, jobId])

  // All filenames across all groups (for deriving extensions)
  const allFiles = useMemo(() => {
    const files: { slideHash: string; name: string }[] = []
    for (const g of groups) {
      for (const f of g.files) {
        files.push({ slideHash: g.slide_hash, name: f })
      }
    }
    return files
  }, [groups])

  // Unique extensions
  const extensions = useMemo(() => {
    const exts = new Set<string>()
    for (const { name } of allFiles) {
      const dotIdx = name.indexOf('.')
      if (dotIdx !== -1) exts.add(name.substring(dotIdx))
    }
    return Array.from(exts).sort()
  }, [allFiles])

  // Visible files after filters
  const visibleKeys = useMemo(() => {
    const keys = new Set<string>()
    for (const { slideHash, name } of allFiles) {
      if (activeFilters.length === 0 || activeFilters.some((f) => name.includes(f))) {
        keys.add(`${slideHash}:${name}`)
      }
    }
    return keys
  }, [allFiles, activeFilters])

  // Filter suggestions
  const filterSuggestions = useMemo(() => {
    if (!filterInput.trim()) return extensions
    const q = filterInput.toLowerCase()
    return extensions.filter((ext) => ext.toLowerCase().includes(q))
  }, [extensions, filterInput])

  const totalAnnotations = useMemo(
    () => groups.reduce((sum, g) => sum + g.annotation_count, 0),
    [groups]
  )

  // --- Filter helpers ---

  const addFilter = (filter: string) => {
    if (activeFilters.includes(filter)) return
    const newFilters = [...activeFilters, filter]
    setActiveFilters(newFilters)
    setFilterInput('')
    // Auto-select matching
    const matching = new Set<string>()
    for (const { slideHash, name } of allFiles) {
      if (newFilters.some((f) => name.includes(f))) {
        matching.add(`${slideHash}:${name}`)
      }
    }
    setSelected(matching)
  }

  const removeFilter = (filter: string) => {
    const newFilters = activeFilters.filter((f) => f !== filter)
    setActiveFilters(newFilters)
    if (newFilters.length === 0) return
    const matching = new Set<string>()
    for (const { slideHash, name } of allFiles) {
      if (newFilters.some((f) => name.includes(f))) {
        matching.add(`${slideHash}:${name}`)
      }
    }
    setSelected(matching)
  }

  const addFilterFromInput = () => {
    const val = filterInput.trim()
    if (!val) return
    addFilter(val)
  }

  // --- Selection helpers ---

  const toggleFile = (slideHash: string, filename: string) => {
    const key = `${slideHash}:${filename}`
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const selectAllVisible = () => {
    setSelected((prev) => {
      const next = new Set(prev)
      for (const key of visibleKeys) next.add(key)
      return next
    })
  }

  const selectNoneVisible = () => {
    setSelected((prev) => {
      const next = new Set(prev)
      for (const key of visibleKeys) next.delete(key)
      return next
    })
  }

  const toggleGroup = (slideHash: string) => {
    setCollapsedGroups((prev) => {
      const next = new Set(prev)
      if (next.has(slideHash)) next.delete(slideHash)
      else next.add(slideHash)
      return next
    })
  }

  const toggleGroupSelection = (group: SlideGroup) => {
    const groupKeys = group.files
      .filter((f) => visibleKeys.has(`${group.slide_hash}:${f}`))
      .map((f) => `${group.slide_hash}:${f}`)
    const allSelected = groupKeys.every((k) => selected.has(k))

    setSelected((prev) => {
      const next = new Set(prev)
      for (const k of groupKeys) {
        if (allSelected) next.delete(k)
        else next.add(k)
      }
      return next
    })
  }

  // --- Download ---

  const doDownload = async () => {
    // Build per-slide file map: slideHash -> [filenames]
    const slideFileMap = new Map<string, string[]>()
    for (const key of selected) {
      const colonIdx = key.indexOf(':')
      const hash = key.substring(0, colonIdx)
      const name = key.substring(colonIdx + 1)
      if (!slideFileMap.has(hash)) slideFileMap.set(hash, [])
      slideFileMap.get(hash)!.push(name)
    }

    const filenameSet = new Set<string>()
    for (const files of slideFileMap.values()) {
      for (const f of files) filenameSet.add(f)
    }

    // Only include WSI for slides that have at least one file selected.
    // If no files are selected at all (WSI-only download), include for all slides.
    const slidesWithFiles = Array.from(slideFileMap.keys())
    const wsiSlideHashes = includeWsi
      ? (slidesWithFiles.length > 0 ? slidesWithFiles : slideHashes)
      : []

    // slide_hashes: union of slides needing files + slides needing WSI
    const requestedHashes = Array.from(
      new Set([...slidesWithFiles, ...wsiSlideHashes])
    )

    if (requestedHashes.length === 0 && !includeAnnotations) return

    setDownloading(true)
    try {
      const res = await fetch(`${getApiBase()}/download-bundle`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          slide_hashes: requestedHashes,
          job_id: jobId,
          include_filenames: Array.from(filenameSet),
          wsi_slide_hashes: wsiSlideHashes,
          include_annotations: includeAnnotations,
          apply_postprocess: applyPostprocess,
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

  const isImage = (name: string) =>
    /\.(png|jpg|jpeg|tif|tiff|bmp|gif|svg)$/i.test(name)

  const selectedCount = selected.size + (includeWsi ? slideHashes.length : 0)

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl max-h-[85vh] flex flex-col">
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

        <div className="space-y-4 py-2 overflow-y-auto flex-1 min-h-0">
          {/* Include toggles */}
          <div className="flex gap-3 flex-wrap">
            <label className="flex items-center gap-3 rounded-md border p-3 cursor-pointer hover:bg-muted/50 flex-1">
              <Checkbox
                checked={includeWsi}
                onCheckedChange={(v) => setIncludeWsi(v === true)}
              />
              <Image className="h-4 w-4 text-blue-500 shrink-0" />
              <div className="min-w-0">
                <p className="text-sm font-medium">Original H&E (.svs)</p>
                <p className="text-xs text-muted-foreground">Large files</p>
              </div>
            </label>
            {totalAnnotations > 0 && (
              <label className="flex items-center gap-3 rounded-md border p-3 cursor-pointer hover:bg-muted/50 flex-1">
                <Checkbox
                  checked={includeAnnotations}
                  onCheckedChange={(v) => setIncludeAnnotations(v === true)}
                />
                <FileText className="h-4 w-4 text-orange-500 shrink-0" />
                <div className="min-w-0">
                  <p className="text-sm font-medium">Imported Annotations</p>
                  <p className="text-xs text-muted-foreground">{totalAnnotations} file{totalAnnotations !== 1 ? 's' : ''}</p>
                </div>
              </label>
            )}
            {postprocessAvailable && (
              <label className="flex items-center gap-3 rounded-md border p-3 cursor-pointer hover:bg-muted/50 flex-1">
                <Checkbox
                  checked={applyPostprocess}
                  onCheckedChange={(v) => setApplyPostprocess(v === true)}
                />
                <Wand2 className="h-4 w-4 text-purple-500 shrink-0" />
                <div className="min-w-0">
                  <p className="text-sm font-medium">Apply post-processing</p>
                  <p className="text-xs text-muted-foreground">Run pipeline script on download</p>
                </div>
              </label>
            )}
          </div>

          {/* File filter + grouped list */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <p className="text-sm font-medium">Analysis output files</p>
              <div className="flex gap-2">
                <button className="text-xs text-primary hover:underline" onClick={selectAllVisible}>
                  All{activeFilters.length > 0 ? ' visible' : ''}
                </button>
                <button className="text-xs text-muted-foreground hover:underline" onClick={selectNoneVisible}>
                  None
                </button>
              </div>
            </div>

            {/* Filter input */}
            {!loading && allFiles.length > 0 && (
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
                        if (filterSuggestions.length === 1) addFilter(filterSuggestions[0])
                        else addFilterFromInput()
                      }
                    }}
                    className="pl-8 h-8 text-sm"
                  />
                </div>

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
                      onClick={() => setActiveFilters([])}
                    >
                      Clear filters
                    </button>
                  </div>
                )}
              </div>
            )}

            {/* Grouped file list */}
            {loading ? (
              <div className="flex items-center gap-2 py-4 justify-center text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                <span className="text-sm">Loading file list...</span>
              </div>
            ) : groups.length === 0 ? (
              <p className="text-sm text-muted-foreground py-2">
                No output files found for this job.
              </p>
            ) : (
              <div className="space-y-1 max-h-64 overflow-y-auto border rounded-md">
                {groups.map((group) => {
                  const visibleGroupFiles = group.files.filter((f) =>
                    visibleKeys.has(`${group.slide_hash}:${f}`)
                  )
                  if (visibleGroupFiles.length === 0 && activeFilters.length > 0) return null

                  const isCollapsed = collapsedGroups.has(group.slide_hash)
                  const groupSelectedCount = visibleGroupFiles.filter((f) =>
                    selected.has(`${group.slide_hash}:${f}`)
                  ).length
                  const allGroupSelected = groupSelectedCount === visibleGroupFiles.length && visibleGroupFiles.length > 0

                  return (
                    <div key={group.slide_hash}>
                      {/* Group header */}
                      <div
                        className="flex items-center gap-2 px-3 py-2 bg-muted/40 sticky top-0 cursor-pointer hover:bg-muted/60 border-b"
                        onClick={() => toggleGroup(group.slide_hash)}
                      >
                        <Checkbox
                          checked={allGroupSelected}
                          onCheckedChange={() => toggleGroupSelection(group)}
                          onClick={(e) => e.stopPropagation()}
                          className="shrink-0"
                        />
                        {isCollapsed ? (
                          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                        ) : (
                          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                        )}
                        <span className="text-sm font-medium truncate">{group.label}</span>
                        <span className={`text-xs px-1.5 py-0.5 rounded border shrink-0 ${
                          group.is_local
                            ? 'bg-green-500/10 text-green-700 border-green-300'
                            : 'bg-amber-500/10 text-amber-700 border-amber-300'
                        }`}>
                          {group.is_local ? 'On drive' : 'On cluster'}
                        </span>
                        <span className="text-xs text-muted-foreground ml-auto shrink-0">
                          {groupSelectedCount}/{visibleGroupFiles.length}
                        </span>
                      </div>

                      {/* Files */}
                      {!isCollapsed && (
                        <div className="py-0.5">
                          {visibleGroupFiles.map((name) => {
                            const key = `${group.slide_hash}:${name}`
                            return (
                              <label
                                key={key}
                                className="flex items-center gap-3 rounded px-3 py-1 cursor-pointer hover:bg-muted/30 min-w-0 overflow-hidden ml-4"
                              >
                                <Checkbox
                                  checked={selected.has(key)}
                                  onCheckedChange={() => toggleFile(group.slide_hash, name)}
                                  className="shrink-0"
                                />
                                {isImage(name) ? (
                                  <Image className="h-3.5 w-3.5 text-blue-500 shrink-0" />
                                ) : (
                                  <FileDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                                )}
                                <span className="text-xs font-mono truncate min-w-0" title={name}>
                                  {name}
                                </span>
                              </label>
                            )
                          })}
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}

            {selected.size > 0 && (
              <p className="text-xs text-muted-foreground mt-1">
                {selected.size} file{selected.size !== 1 ? 's' : ''} selected
                {activeFilters.length > 0 && allFiles.length !== visibleKeys.size &&
                  ` (filtered from ${allFiles.length})`}
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
            disabled={downloading || (selected.size === 0 && !includeWsi && !includeAnnotations)}
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
                {selectedCount > 0 && ` (${selected.size})`}
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
