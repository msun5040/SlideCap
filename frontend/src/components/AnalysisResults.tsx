import { useState, useEffect, useCallback } from 'react'
import {
  Search,
  FileDown,
  Image,
  FileText,
  ChevronRight,
  ChevronDown,
  Trash2,
  Download,
  ArrowLeft,
  Package,
  ShoppingCart,
  X,
  Hash,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { DownloadModal } from '@/components/DownloadModal'
import { CopyableText } from '@/components/CopyableText'

const API_BASE = 'http://localhost:8000'

// ---------- Types ----------

interface JobSummary {
  id: number
  analysis_id: number
  model_name: string
  model_version: string | null
  parameters: string | null
  gpu_index: number | null
  status: string
  submitted_by: string | null
  submitted_at: string | null
  started_at: string | null
  completed_at: string | null
  error_message: string | null
  slide_count: number
  completed_count: number
  failed_count: number
}

interface JobSlideDetail {
  id: number
  slide_hash: string | null
  filename: string | null
  cluster_job_id: string | null
  status: string
  started_at: string | null
  completed_at: string | null
  error_message: string | null
  log_tail: string | null
  remote_output_path: string | null
  cell_stats: Record<string, number> | null
}

interface JobDetail extends JobSummary {
  slides: JobSlideDetail[]
}

interface ResultFile {
  name: string
  size: number
  is_image: boolean
}

interface SlideResult {
  job_id: number
  job_slide_id: number
  analysis_name: string
  version: string
  status: string
  completed_at?: string
  output_path?: string
  cell_stats?: Record<string, number> | null
}

interface SlideWithResults {
  slide_hash: string
  accession_number: string
  block_id: string
  stain_type: string
  year?: number
  results: SlideResult[]
}

// ---------- Helpers ----------

function formatSize(bytes: number) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function formatDate(iso: string | null) {
  if (!iso) return ''
  const d = new Date(iso)
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

function formatCount(n: number) {
  return n.toLocaleString()
}

function CellStats({ stats }: { stats: Record<string, number> }) {
  const entries = Object.entries(stats)
  if (entries.length === 0) return null
  return (
    <div className="flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-muted-foreground pl-12 pb-1">
      {entries.map(([name, count]) => (
        <span key={name}>
          <span className="font-medium text-foreground/70">{name}:</span>{' '}
          {formatCount(count)}
        </span>
      ))}
    </div>
  )
}

// ---------- Component ----------

export function AnalysisResults() {
  // View mode: 'jobs' (default) or 'search'
  const [view, setView] = useState<'jobs' | 'search'>('jobs')

  // Job list state
  const [jobs, setJobs] = useState<JobSummary[]>([])
  const [loadingJobs, setLoadingJobs] = useState(false)
  const [expandedJobs, setExpandedJobs] = useState<Set<number>>(new Set())
  const [jobDetails, setJobDetails] = useState<Record<number, JobDetail>>({})

  // Slide display options
  const [showHashes, setShowHashes] = useState(false)

  // Slide expansion within jobs
  const [expandedSlides, setExpandedSlides] = useState<Set<string>>(new Set()) // key: "jobId:slideHash"
  const [slideFiles, setSlideFiles] = useState<Record<string, ResultFile[]>>({}) // key: "jobId:slideHash"
  const [loadingSlideFiles, setLoadingSlideFiles] = useState<Set<string>>(new Set())

  // Search state
  const [query, setQuery] = useState('')
  const [searchResults, setSearchResults] = useState<SlideWithResults[]>([])
  const [searchDone, setSearchDone] = useState(false)
  const [searchLoading, setSearchLoading] = useState(false)

  // Search mode: slide expansion for file browsing
  const [searchExpandedSlides, setSearchExpandedSlides] = useState<Set<string>>(new Set())
  const [searchSlideFiles, setSearchSlideFiles] = useState<Record<string, ResultFile[]>>({})
  const [searchLoadingFiles, setSearchLoadingFiles] = useState<Set<string>>(new Set())

  // Transfer state
  const [transferringJobs, setTransferringJobs] = useState<Set<number>>(new Set())

  // Cart state
  const [cart, setCart] = useState<Set<string>>(new Set()) // "jobId:slideHash:filename"

  // Slide selection within expanded jobs (for bundle download)
  const [selectedJobSlides, setSelectedJobSlides] = useState<Record<number, Set<string>>>({}) // jobId -> Set<slideHash>

  // Bundle modal state
  const [bundleModal, setBundleModal] = useState<{ open: boolean; jobId: number; slideHashes: string[] }>({
    open: false,
    jobId: 0,
    slideHashes: [],
  })

  // Preview
  const [previewUrl, setPreviewUrl] = useState<string | null>(null)

  // Delete confirmation
  const [deleteConfirm, setDeleteConfirm] = useState<{
    open: boolean
    type: 'file' | 'job'
    label: string
    jobId?: number
    onConfirm: (deleteFiles?: boolean) => Promise<void>
  }>({ open: false, type: 'file', label: '', onConfirm: async () => {} })

  // ---------- Fetch jobs ----------

  const fetchJobs = useCallback(async () => {
    setLoadingJobs(true)
    try {
      const res = await fetch(`${API_BASE}/jobs?limit=100`)
      if (res.ok) {
        setJobs(await res.json())
      }
    } catch (e) {
      console.error('Failed to fetch jobs:', e)
    } finally {
      setLoadingJobs(false)
    }
  }, [])

  useEffect(() => {
    fetchJobs()
  }, [fetchJobs])

  // ---------- Expand/collapse job ----------

  const toggleJob = async (jobId: number) => {
    const next = new Set(expandedJobs)
    if (next.has(jobId)) {
      next.delete(jobId)
    } else {
      next.add(jobId)
      // Fetch detail if not already loaded
      if (!jobDetails[jobId]) {
        try {
          const res = await fetch(`${API_BASE}/jobs/${jobId}`)
          if (res.ok) {
            const detail: JobDetail = await res.json()
            setJobDetails((prev) => ({ ...prev, [jobId]: detail }))
          }
        } catch (e) {
          console.error('Failed to fetch job detail:', e)
        }
      }
    }
    setExpandedJobs(next)
  }

  // ---------- Expand/collapse slide (within job) ----------

  const slideKey = (jobId: number, slideHash: string) => `${jobId}:${slideHash}`

  const toggleSlide = async (jobId: number, slideHash: string) => {
    const key = slideKey(jobId, slideHash)
    const next = new Set(expandedSlides)
    if (next.has(key)) {
      next.delete(key)
    } else {
      next.add(key)
      if (!slideFiles[key]) {
        setLoadingSlideFiles((prev) => new Set(prev).add(key))
        try {
          const res = await fetch(
            `${API_BASE}/results/${jobId}/files?slide_hash=${encodeURIComponent(slideHash)}`
          )
          if (res.ok) {
            const data = await res.json()
            setSlideFiles((prev) => ({ ...prev, [key]: data }))
          } else {
            setSlideFiles((prev) => ({ ...prev, [key]: [] }))
          }
        } catch (e) {
          console.error(e)
          setSlideFiles((prev) => ({ ...prev, [key]: [] }))
        } finally {
          setLoadingSlideFiles((prev) => {
            const s = new Set(prev)
            s.delete(key)
            return s
          })
        }
      }
    }
    setExpandedSlides(next)
  }

  // ---------- Search ----------

  const doSearch = async () => {
    if (!query.trim()) return
    setSearchLoading(true)
    setSearchDone(false)
    setSearchExpandedSlides(new Set())
    setSearchSlideFiles({})
    try {
      const res = await fetch(
        `${API_BASE}/results/search?q=${encodeURIComponent(query.trim())}`
      )
      if (res.ok) {
        const data = await res.json()
        setSearchResults(data.results)
      } else {
        setSearchResults([])
      }
    } catch (e) {
      console.error(e)
      setSearchResults([])
    } finally {
      setSearchLoading(false)
      setSearchDone(true)
      setView('search')
    }
  }

  const backToJobs = () => {
    setView('jobs')
    setQuery('')
    setSearchResults([])
    setSearchDone(false)
  }

  // ---------- Search mode: expand slide to files ----------

  const toggleSearchSlide = async (slideHash: string, jobId: number) => {
    const key = `${jobId}:${slideHash}`
    const next = new Set(searchExpandedSlides)
    if (next.has(key)) {
      next.delete(key)
    } else {
      next.add(key)
      if (!searchSlideFiles[key]) {
        setSearchLoadingFiles((prev) => new Set(prev).add(key))
        try {
          const res = await fetch(
            `${API_BASE}/results/${jobId}/files?slide_hash=${encodeURIComponent(slideHash)}`
          )
          if (res.ok) {
            const data = await res.json()
            setSearchSlideFiles((prev) => ({ ...prev, [key]: data }))
          } else {
            setSearchSlideFiles((prev) => ({ ...prev, [key]: [] }))
          }
        } catch (e) {
          setSearchSlideFiles((prev) => ({ ...prev, [key]: [] }))
        } finally {
          setSearchLoadingFiles((prev) => {
            const s = new Set(prev)
            s.delete(key)
            return s
          })
        }
      }
    }
    setSearchExpandedSlides(next)
  }

  // ---------- Transfer ----------

  const transferAll = async (jobId: number) => {
    setTransferringJobs((prev) => new Set(prev).add(jobId))
    try {
      const res = await fetch(`${API_BASE}/jobs/${jobId}/transfer-results`, { method: 'POST' })
      if (res.ok) {
        const data = await res.json()
        if (data.transferred > 0) {
          // Re-fetch job detail to update slide statuses
          const detailRes = await fetch(`${API_BASE}/jobs/${jobId}`)
          if (detailRes.ok) {
            const detail: JobDetail = await detailRes.json()
            setJobDetails((prev) => ({ ...prev, [jobId]: detail }))
          }
          // Clear cached files so they reload on next expand
          setSlideFiles((prev) => {
            const next = { ...prev }
            for (const key of Object.keys(next)) {
              if (key.startsWith(`${jobId}:`)) delete next[key]
            }
            return next
          })
        } else {
          alert(
            data.errors?.length
              ? `Transfer issues: ${data.errors.join(', ')}`
              : 'No files transferred. Check cluster connection.'
          )
        }
      } else {
        const err = await res.json()
        alert(err.detail || 'Transfer failed')
      }
    } catch (e) {
      console.error('Transfer failed:', e)
      alert('Transfer failed — check cluster connection')
    } finally {
      setTransferringJobs((prev) => {
        const s = new Set(prev)
        s.delete(jobId)
        return s
      })
    }
  }

  // ---------- Delete job ----------

  const confirmDeleteJob = (jobId: number) => {
    setDeleteConfirm({
      open: true,
      type: 'job',
      label: `Job #${jobId}`,
      jobId,
      onConfirm: async (deleteFiles?: boolean) => {
        try {
          const url = deleteFiles
            ? `${API_BASE}/jobs/${jobId}?delete_files=true`
            : `${API_BASE}/jobs/${jobId}`
          const res = await fetch(url, { method: 'DELETE' })
          if (res.ok) {
            setJobs((prev) => prev.filter((j) => j.id !== jobId))
            setExpandedJobs((prev) => {
              const s = new Set(prev)
              s.delete(jobId)
              return s
            })
          } else {
            const err = await res.json()
            alert(err.detail || 'Failed to delete')
          }
        } catch (e) {
          console.error('Delete failed:', e)
        }
      },
    })
  }

  const confirmDeleteFile = (jobId: number, slideHash: string, filename: string) => {
    setDeleteConfirm({
      open: true,
      type: 'file',
      label: filename,
      onConfirm: async () => {
        try {
          const res = await fetch(
            `${API_BASE}/results/${jobId}/file/${encodeURIComponent(filename)}?slide_hash=${encodeURIComponent(slideHash)}`,
            { method: 'DELETE' }
          )
          if (res.ok) {
            // Remove from cached file list
            const key = slideKey(jobId, slideHash)
            setSlideFiles((prev) => ({
              ...prev,
              [key]: (prev[key] || []).filter((f) => f.name !== filename),
            }))
            // Also remove from search file cache
            setSearchSlideFiles((prev) => ({
              ...prev,
              [key]: (prev[key] || []).filter((f) => f.name !== filename),
            }))
            // Remove from cart
            const ck = cartKey(jobId, slideHash, filename)
            setCart((prev) => {
              const next = new Set(prev)
              next.delete(ck)
              return next
            })
          } else {
            const err = await res.json()
            alert(err.detail || 'Failed to delete file')
          }
        } catch (e) {
          console.error('Delete file failed:', e)
        }
      },
    })
  }

  // ---------- File download ----------

  const downloadFile = (jobId: number, slideHash: string, filename: string) => {
    const url = `${API_BASE}/results/${jobId}/file/${encodeURIComponent(filename)}?slide_hash=${encodeURIComponent(slideHash)}`
    const a = document.createElement('a')
    a.href = url
    a.download = filename.replace(/\.snappy$/, '')
    a.click()
  }

  const downloadJobZip = (jobId: number) => {
    const a = document.createElement('a')
    a.href = `${API_BASE}/jobs/${jobId}/download-zip`
    a.download = `job_${jobId}_results.zip`
    a.click()
  }

  const previewFile = (jobId: number, slideHash: string, filename: string) => {
    setPreviewUrl(
      `${API_BASE}/results/${jobId}/file/${encodeURIComponent(filename)}?slide_hash=${encodeURIComponent(slideHash)}`
    )
  }

  // ---------- Cart ----------

  const cartKey = (jobId: number, slideHash: string, filename: string) =>
    `${jobId}:${slideHash}:${filename}`

  const toggleCart = (jobId: number, slideHash: string, filename: string) => {
    const key = cartKey(jobId, slideHash, filename)
    setCart((prev) => {
      const next = new Set(prev)
      if (next.has(key)) {
        next.delete(key)
      } else {
        next.add(key)
      }
      return next
    })
  }

  const clearCart = () => setCart(new Set())

  const downloadCart = async () => {
    const items = Array.from(cart).map((key) => {
      const [jobId, slideHash, ...rest] = key.split(':')
      return { job_id: parseInt(jobId), slide_hash: slideHash, filename: rest.join(':') }
    })

    try {
      const res = await fetch(`${API_BASE}/results/download-cart`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items }),
      })
      if (res.ok) {
        const blob = await res.blob()
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = 'selected_results.zip'
        a.click()
        URL.revokeObjectURL(url)
      } else {
        alert('Download failed')
      }
    } catch (e) {
      console.error('Cart download failed:', e)
      alert('Download failed')
    }
  }

  // ---------- Slide selection for bundle download ----------

  const toggleJobSlide = (jobId: number, slideHash: string) => {
    setSelectedJobSlides((prev) => {
      const current = prev[jobId] ?? new Set<string>()
      const next = new Set(current)
      if (next.has(slideHash)) next.delete(slideHash)
      else next.add(slideHash)
      return { ...prev, [jobId]: next }
    })
  }

  const selectAllJobSlides = (jobId: number) => {
    const detail = jobDetails[jobId]
    if (!detail) return
    const all = new Set(detail.slides.filter((s) => s.slide_hash).map((s) => s.slide_hash!))
    setSelectedJobSlides((prev) => ({ ...prev, [jobId]: all }))
  }

  const clearJobSlideSelection = (jobId: number) => {
    setSelectedJobSlides((prev) => ({ ...prev, [jobId]: new Set() }))
  }

  const openBundleModal = (jobId: number, slideHashes?: string[]) => {
    const hashes = slideHashes ?? Array.from(selectedJobSlides[jobId] ?? new Set())
    if (hashes.length === 0) {
      // Default to all slides in the job
      const detail = jobDetails[jobId]
      if (detail) {
        const all = detail.slides.filter((s) => s.slide_hash).map((s) => s.slide_hash!)
        setBundleModal({ open: true, jobId, slideHashes: all })
        return
      }
    }
    setBundleModal({ open: true, jobId, slideHashes: hashes })
  }

  // ---------- Status badge helper ----------

  const statusBadge = (status: string) => {
    const colors: Record<string, string> = {
      completed: 'bg-green-500/10 text-green-700',
      running: 'bg-blue-500/10 text-blue-700',
      pending: 'bg-yellow-500/10 text-yellow-700',
      failed: 'bg-red-500/10 text-red-700',
      submitted: 'bg-purple-500/10 text-purple-700',
      transferring: 'bg-cyan-500/10 text-cyan-700',
    }
    return (
      <Badge variant="outline" className={colors[status] || ''}>
        {status}
      </Badge>
    )
  }

  // ---------- Render file row ----------

  const renderFileRow = (
    file: ResultFile,
    jobId: number,
    slideHash: string,
  ) => {
    const key = cartKey(jobId, slideHash, file.name)
    const inCart = cart.has(key)

    return (
      <div key={file.name} className="flex items-center justify-between py-1 pl-12 pr-3">
        <div className="flex items-center gap-2">
          <Checkbox
            checked={inCart}
            onCheckedChange={() => toggleCart(jobId, slideHash, file.name)}
          />
          {file.is_image ? (
            <Image className="h-3.5 w-3.5 text-blue-500" />
          ) : (
            <FileText className="h-3.5 w-3.5 text-muted-foreground" />
          )}
          <span className="text-sm font-mono">{file.name}</span>
          <span className="text-xs text-muted-foreground">{formatSize(file.size)}</span>
        </div>
        <div className="flex gap-1">
          {file.is_image && (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 text-xs"
              onClick={() => previewFile(jobId, slideHash, file.name)}
            >
              Preview
            </Button>
          )}
          <Button
            variant="ghost"
            size="sm"
            className="h-7"
            onClick={() => downloadFile(jobId, slideHash, file.name)}
          >
            <FileDown className="h-3.5 w-3.5" />
          </Button>
          <button
            className="p-1 rounded hover:bg-destructive/10 text-muted-foreground hover:text-destructive transition-colors"
            title="Delete file from disk"
            onClick={() => confirmDeleteFile(jobId, slideHash, file.name)}
          >
            <Trash2 className="h-3 w-3" />
          </button>
        </div>
      </div>
    )
  }

  // ---------- Render: Job list mode ----------

  const renderJobList = () => (
    <div className="space-y-1">
      {loadingJobs && jobs.length === 0 && (
        <p className="text-sm text-muted-foreground py-4">Loading jobs...</p>
      )}
      {!loadingJobs && jobs.length === 0 && (
        <p className="text-sm text-muted-foreground py-4">No analysis jobs found.</p>
      )}
      {jobs.map((job) => {
        const isExpanded = expandedJobs.has(job.id)
        const detail = jobDetails[job.id]

        return (
          <div key={job.id} className="rounded-lg border">
            {/* Job header row */}
            <div
              className="flex items-center gap-3 px-3 py-2.5 cursor-pointer hover:bg-muted/50 transition-colors"
              onClick={() => toggleJob(job.id)}
            >
              {isExpanded ? (
                <ChevronDown className="h-4 w-4 text-muted-foreground shrink-0" />
              ) : (
                <ChevronRight className="h-4 w-4 text-muted-foreground shrink-0" />
              )}
              <div className="flex-1 flex items-center gap-2 min-w-0">
                <span className="font-medium text-sm">Job #{job.id}</span>
                <Badge variant="secondary" className="text-xs">
                  {job.model_name} {job.model_version || ''}
                </Badge>
                {statusBadge(job.status)}
                <span className="text-xs text-muted-foreground">
                  {job.completed_count}/{job.slide_count}
                  {job.failed_count > 0 && (
                    <span className="text-red-500 ml-1">({job.failed_count} failed)</span>
                  )}
                </span>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <span className="text-xs text-muted-foreground">
                  {formatDate(job.completed_at || job.submitted_at)}
                </span>
                <button
                  className="p-1 rounded hover:bg-destructive/10 text-muted-foreground hover:text-destructive transition-colors"
                  title="Delete this job"
                  onClick={(e) => {
                    e.stopPropagation()
                    confirmDeleteJob(job.id)
                  }}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>

            {/* Expanded: slides list */}
            {isExpanded && (
              <div className="border-t">
                {/* Job-level buttons */}
                <div className="flex items-center gap-2 px-4 py-2 bg-muted/20 flex-wrap">
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 text-xs"
                    disabled={transferringJobs.has(job.id)}
                    onClick={(e) => {
                      e.stopPropagation()
                      transferAll(job.id)
                    }}
                  >
                    <Download className="mr-1 h-3.5 w-3.5" />
                    {transferringJobs.has(job.id) ? 'Transferring...' : 'Transfer All'}
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 text-xs"
                    onClick={(e) => {
                      e.stopPropagation()
                      downloadJobZip(job.id)
                    }}
                  >
                    <Package className="mr-1 h-3.5 w-3.5" />
                    Download ZIP
                  </Button>
                  <Button
                    size="sm"
                    className="h-7 text-xs"
                    onClick={(e) => {
                      e.stopPropagation()
                      openBundleModal(job.id)
                    }}
                  >
                    <FileDown className="mr-1 h-3.5 w-3.5" />
                    Download Bundle
                    {(selectedJobSlides[job.id]?.size ?? 0) > 0 &&
                      ` (${selectedJobSlides[job.id].size})`}
                  </Button>
                  {detail && (
                    <div className="flex items-center gap-1 ml-auto text-xs">
                      <button
                        className="text-primary hover:underline"
                        onClick={(e) => {
                          e.stopPropagation()
                          selectAllJobSlides(job.id)
                        }}
                      >
                        Select all
                      </button>
                      <span className="text-muted-foreground">|</span>
                      <button
                        className="text-muted-foreground hover:underline"
                        onClick={(e) => {
                          e.stopPropagation()
                          clearJobSlideSelection(job.id)
                        }}
                      >
                        Clear
                      </button>
                    </div>
                  )}
                </div>

                {!detail ? (
                  <p className="text-sm text-muted-foreground px-4 py-3">Loading slides...</p>
                ) : detail.slides.length === 0 ? (
                  <p className="text-sm text-muted-foreground px-4 py-3">No slides in this job.</p>
                ) : (
                  <div className="divide-y">
                    {detail.slides.map((js) => {
                      if (!js.slide_hash) return null
                      const sKey = slideKey(job.id, js.slide_hash)
                      const isSlideExpanded = expandedSlides.has(sKey)
                      const files = slideFiles[sKey]
                      const isLoadingFiles = loadingSlideFiles.has(sKey)

                      return (
                        <div key={js.id}>
                          {/* Slide row */}
                          <div
                            className={`flex items-center gap-2 px-4 py-2 cursor-pointer transition-colors ${
                              isSlideExpanded ? 'bg-primary/5' : 'hover:bg-muted/30'
                            }`}
                            onClick={() => toggleSlide(job.id, js.slide_hash!)}
                          >
                            <Checkbox
                              checked={selectedJobSlides[job.id]?.has(js.slide_hash!) ?? false}
                              onCheckedChange={(e) => {
                                // Don't toggle expansion when clicking checkbox
                                toggleJobSlide(job.id, js.slide_hash!)
                              }}
                              onClick={(e) => e.stopPropagation()}
                            />
                            <span className="text-muted-foreground">
                              {isSlideExpanded ? '└' : '├'}
                            </span>
                            {isSlideExpanded ? (
                              <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
                            ) : (
                              <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
                            )}
                            <CopyableText
                              className="text-sm max-w-[300px]"
                              text={showHashes
                                ? `${js.slide_hash!.substring(0, 12)}...`
                                : js.filename || `${js.slide_hash!.substring(0, 12)}...`}
                              copyValue={showHashes ? js.slide_hash! : (js.filename || js.slide_hash!)}
                            />
                            {statusBadge(js.status)}
                            {js.error_message && (
                              <span className="text-xs text-red-500 truncate max-w-[200px]">
                                {js.error_message}
                              </span>
                            )}
                          </div>

                          {/* Cell stats (shown even when not expanded) */}
                          {js.cell_stats && (
                            <CellStats stats={js.cell_stats} />
                          )}

                          {/* Expanded: file list */}
                          {isSlideExpanded && (
                            <div className="bg-muted/10 border-t py-1">
                              {isLoadingFiles ? (
                                <p className="text-sm text-muted-foreground pl-12 py-2">
                                  Loading files...
                                </p>
                              ) : !files || files.length === 0 ? (
                                <p className="text-sm text-muted-foreground pl-12 py-2">
                                  No output files found locally. Use "Transfer All" above.
                                </p>
                              ) : (
                                files.map((f) =>
                                  renderFileRow(f, job.id, js.slide_hash!)
                                )
                              )}
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                )}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )

  // ---------- Render: Search mode ----------

  const renderSearchResults = () => (
    <div className="space-y-3">
      <Button variant="ghost" size="sm" onClick={backToJobs} className="mb-2">
        <ArrowLeft className="mr-1 h-4 w-4" />
        Back to all jobs
      </Button>

      {searchDone && searchResults.length === 0 && (
        <p className="text-sm text-muted-foreground">
          No slides with completed analyses found for this query.
        </p>
      )}

      {searchResults.length > 0 && (
        <>
          <h3 className="text-sm font-medium">
            {searchResults.length} slide{searchResults.length !== 1 ? 's' : ''} with results
          </h3>
          {searchResults.map((slide) => (
            <div key={slide.slide_hash} className="rounded-lg border">
              <div className="flex items-center gap-3 p-3 bg-muted/30">
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <span className="font-medium font-mono text-sm">
                      {slide.accession_number}
                    </span>
                    <Badge variant="secondary">{slide.block_id}</Badge>
                    <Badge variant="outline">{slide.stain_type}</Badge>
                    {slide.year && (
                      <span className="text-xs text-muted-foreground">{slide.year}</span>
                    )}
                  </div>
                  <CopyableText
                    className="text-xs text-muted-foreground mt-0.5"
                    text={`${slide.slide_hash.substring(0, 16)}...`}
                    copyValue={slide.slide_hash}
                  />
                </div>
                <Badge variant="secondary">{slide.results.length} analyses</Badge>
              </div>

              <div className="divide-y">
                {slide.results.map((r) => {
                  const sKey = `${r.job_id}:${slide.slide_hash}`
                  const isExpanded = searchExpandedSlides.has(sKey)
                  const files = searchSlideFiles[sKey]
                  const isLoading = searchLoadingFiles.has(sKey)

                  return (
                    <div key={`${r.job_id}-${r.job_slide_id}`}>
                      <div
                        className={`flex items-center justify-between px-3 py-2 cursor-pointer transition-colors ${
                          isExpanded ? 'bg-primary/5' : 'hover:bg-muted/50'
                        }`}
                        onClick={() => toggleSearchSlide(slide.slide_hash, r.job_id)}
                      >
                        <div className="flex items-center gap-2">
                          {isExpanded ? (
                            <ChevronDown className="h-4 w-4 text-muted-foreground" />
                          ) : (
                            <ChevronRight className="h-4 w-4 text-muted-foreground" />
                          )}
                          <span className="text-sm font-medium">{r.analysis_name}</span>
                          <Badge variant="secondary" className="text-xs">
                            v{r.version}
                          </Badge>
                        </div>
                        <div className="flex items-center gap-2">
                          {r.completed_at && (
                            <span className="text-xs text-muted-foreground">
                              {new Date(r.completed_at).toLocaleDateString()}
                            </span>
                          )}
                          {statusBadge(r.status)}
                        </div>
                      </div>

                      {/* Cell stats */}
                      {r.cell_stats && (
                        <CellStats stats={r.cell_stats} />
                      )}

                      {isExpanded && (
                        <div className="bg-muted/10 border-t py-1">
                          {isLoading ? (
                            <p className="text-sm text-muted-foreground pl-8 py-2">
                              Loading files...
                            </p>
                          ) : !files || files.length === 0 ? (
                            <p className="text-sm text-muted-foreground pl-8 py-2">
                              No output files found locally.
                            </p>
                          ) : (
                            files.map((f) =>
                              renderFileRow(f, r.job_id, slide.slide_hash)
                            )
                          )}
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            </div>
          ))}
        </>
      )}
    </div>
  )

  // ---------- Main render ----------

  return (
    <div className="space-y-4">
      {/* Search bar — always visible */}
      <div className="flex gap-2 items-center">
        <Input
          placeholder="Search by accession number (e.g. S24-12345)..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && doSearch()}
          className="max-w-md"
        />
        <Button onClick={doSearch} disabled={searchLoading}>
          <Search className="mr-2 h-4 w-4" />
          {searchLoading ? 'Searching...' : 'Search'}
        </Button>
        <Button
          variant={showHashes ? 'secondary' : 'ghost'}
          size="sm"
          className="h-9 text-xs gap-1"
          onClick={() => setShowHashes((v) => !v)}
          title={showHashes ? 'Showing slide hashes — click to show filenames' : 'Showing filenames — click to show slide hashes'}
        >
          <Hash className="h-3.5 w-3.5" />
          {showHashes ? 'Hashes' : 'Filenames'}
        </Button>
      </div>

      {/* Content */}
      {view === 'jobs' ? renderJobList() : renderSearchResults()}

      {/* Image preview */}
      {previewUrl && (
        <div className="rounded-lg border p-4">
          <div className="flex justify-between mb-2">
            <h4 className="text-sm font-medium">Preview</h4>
            <Button variant="ghost" size="sm" onClick={() => setPreviewUrl(null)}>
              Close
            </Button>
          </div>
          <img
            src={previewUrl}
            alt="Result preview"
            className="max-w-full max-h-[500px] rounded"
          />
        </div>
      )}

      {/* Cart bar */}
      {cart.size > 0 && (
        <div className="fixed bottom-0 left-0 right-0 bg-background border-t shadow-lg px-6 py-3 flex items-center justify-between z-50">
          <div className="flex items-center gap-3">
            <ShoppingCart className="h-5 w-5 text-primary" />
            <span className="text-sm font-medium">{cart.size} file{cart.size !== 1 ? 's' : ''} selected</span>
          </div>
          <div className="flex gap-2">
            <Button variant="ghost" size="sm" onClick={clearCart}>
              <X className="mr-1 h-4 w-4" />
              Clear
            </Button>
            <Button size="sm" onClick={downloadCart}>
              <FileDown className="mr-1 h-4 w-4" />
              Download Cart ({cart.size} files)
            </Button>
          </div>
        </div>
      )}

      {/* Bundle download modal */}
      <DownloadModal
        open={bundleModal.open}
        onOpenChange={(open) => setBundleModal((prev) => ({ ...prev, open }))}
        slideHashes={bundleModal.slideHashes}
        jobId={bundleModal.jobId}
      />

      {/* Delete confirmation dialog */}
      <Dialog open={deleteConfirm.open} onOpenChange={(open) => setDeleteConfirm((prev) => ({ ...prev, open }))}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Confirm Deletion</DialogTitle>
            <DialogDescription>
              {deleteConfirm.type === 'file' ? (
                <>
                  Are you sure you want to permanently delete{' '}
                  <span className="font-medium text-foreground">{deleteConfirm.label}</span>?
                  This will remove the file from the network drive.
                </>
              ) : (
                <>
                  How would you like to delete{' '}
                  <span className="font-medium text-foreground">{deleteConfirm.label}</span>?
                </>
              )}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteConfirm((prev) => ({ ...prev, open: false }))}>
              Cancel
            </Button>
            {deleteConfirm.type === 'job' ? (
              <>
                <Button
                  variant="secondary"
                  onClick={async () => {
                    await deleteConfirm.onConfirm(false)
                    setDeleteConfirm((prev) => ({ ...prev, open: false }))
                  }}
                >
                  Record Only
                </Button>
                <Button
                  variant="destructive"
                  onClick={async () => {
                    await deleteConfirm.onConfirm(true)
                    setDeleteConfirm((prev) => ({ ...prev, open: false }))
                  }}
                >
                  Delete Everything
                </Button>
              </>
            ) : (
              <Button
                variant="destructive"
                onClick={async () => {
                  await deleteConfirm.onConfirm()
                  setDeleteConfirm((prev) => ({ ...prev, open: false }))
                }}
              >
                Delete
              </Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
