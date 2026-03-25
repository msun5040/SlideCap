import { useState, useEffect, useRef } from 'react'
import { RefreshCw, XCircle, ChevronDown, ChevronRight, RotateCcw } from 'lucide-react'
import { signalClusterDisconnected } from '@/components/ClusterConnect'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import type { AnalysisJob, JobSlide } from '@/types/slide'

import { getApiBase } from '@/api'
import { SortableHeader } from '@/components/SortableHeader'
import { useSortable } from '@/hooks/useSortable'

const STATUS_COLORS: Record<string, string> = {
  pending: 'bg-gray-500/10 text-gray-600 border-gray-300',
  transferring: 'bg-purple-500/10 text-purple-700 border-purple-300',
  queued: 'bg-yellow-500/10 text-yellow-700 border-yellow-300',
  running: 'bg-blue-500/10 text-blue-700 border-blue-300',
  completed: 'bg-green-500/10 text-green-700 border-green-300',
  failed: 'bg-red-500/10 text-red-700 border-red-300',
}

export function AnalysisJobs() {
  const [jobs, setJobs] = useState<AnalysisJob[]>([])
  const [loading, setLoading] = useState(true)
  const [statusFilter, setStatusFilter] = useState<string>('')
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [retryingJobs, setRetryingJobs] = useState<Set<number>>(new Set())
  const [expandedJobId, setExpandedJobId] = useState<number | null>(null)
  const [expandedSlides, setExpandedSlides] = useState<JobSlide[]>([])
  const [loadingSlides, setLoadingSlides] = useState(false)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const { sorted: sortedJobs, sortConfig: jobsSortConfig, handleSort: handleJobsSort } = useSortable(jobs, { key: 'id', direction: 'desc' })

  useEffect(() => {
    fetchJobs()
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [statusFilter])

  // Auto-refresh when active jobs exist
  useEffect(() => {
    const hasActive = jobs.some((j) =>
      j.status === 'running' || j.status === 'transferring' || j.status === 'pending'
    )
    if (hasActive) {
      intervalRef.current = setInterval(fetchJobs, 15000)
    } else if (intervalRef.current) {
      clearInterval(intervalRef.current)
      intervalRef.current = null
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [jobs])

  const fetchJobs = async () => {
    try {
      const params = new URLSearchParams({ limit: '200' })
      if (statusFilter) params.set('status', statusFilter)
      const res = await fetch(`${getApiBase()}/jobs?${params}`)
      if (res.ok) setJobs(await res.json())
    } catch (e) {
      console.error('Failed to fetch jobs:', e)
    } finally {
      setLoading(false)
    }
  }

  const fetchJobDetail = async (jobId: number) => {
    setLoadingSlides(true)
    try {
      const res = await fetch(`${getApiBase()}/jobs/${jobId}`)
      if (res.ok) {
        const data = await res.json()
        setExpandedSlides(data.slides || [])
      }
    } catch (e) {
      console.error('Failed to fetch job detail:', e)
    } finally {
      setLoadingSlides(false)
    }
  }

  const handleRefresh = async () => {
    setIsRefreshing(true)
    try {
      const res = await fetch(`${getApiBase()}/jobs/refresh`, { method: 'POST' })
      if (res.status === 503) { signalClusterDisconnected(); return }
      await fetchJobs()
      if (expandedJobId) await fetchJobDetail(expandedJobId)
    } catch (e) {
      console.error('Refresh failed:', e)
    } finally {
      setIsRefreshing(false)
    }
  }

  const handleRetry = async (jobId: number) => {
    setRetryingJobs((prev) => new Set(prev).add(jobId))
    try {
      const res = await fetch(`${getApiBase()}/jobs/${jobId}/retry`, { method: 'POST' })
      if (res.status === 503) { signalClusterDisconnected(); return }
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        alert(err.detail || 'Retry failed')
      }
      await fetchJobs()
      if (expandedJobId === jobId) await fetchJobDetail(jobId)
    } catch (e) {
      console.error('Retry failed:', e)
    } finally {
      setRetryingJobs((prev) => { const s = new Set(prev); s.delete(jobId); return s })
    }
  }

  const handleCancel = async (jobId: number) => {
    if (!confirm('Cancel this job and all its slides?')) return
    try {
      await fetch(`${getApiBase()}/jobs/cancel`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_ids: [jobId] }),
      })
      fetchJobs()
      if (expandedJobId === jobId) {
        setExpandedJobId(null)
        setExpandedSlides([])
      }
    } catch (e) {
      console.error('Cancel failed:', e)
    }
  }

  const formatDuration = (job: AnalysisJob) => {
    if (!job.started_at) return '-'
    const start = new Date(job.started_at).getTime()
    const end = job.completed_at ? new Date(job.completed_at).getTime() : Date.now()
    const minutes = Math.round((end - start) / 60000)
    if (minutes < 1) return '<1 min'
    if (minutes < 60) return `${minutes} min`
    return `${Math.floor(minutes / 60)}h ${minutes % 60}m`
  }

  const toggleExpand = async (jobId: number) => {
    if (expandedJobId === jobId) {
      setExpandedJobId(null)
      setExpandedSlides([])
    } else {
      setExpandedJobId(jobId)
      await fetchJobDetail(jobId)
    }
  }

  const ProgressBar = ({ job }: { job: AnalysisJob }) => {
    if (job.slide_count === 0) return null
    const completedPct = (job.completed_count / job.slide_count) * 100
    const failedPct = (job.failed_count / job.slide_count) * 100
    return (
      <div className="flex items-center gap-2">
        <div className="flex-1 h-2 bg-gray-200 rounded-full overflow-hidden min-w-[60px]">
          <div className="h-full flex">
            {completedPct > 0 && (
              <div className="bg-green-500 h-full" style={{ width: `${completedPct}%` }} />
            )}
            {failedPct > 0 && (
              <div className="bg-red-500 h-full" style={{ width: `${failedPct}%` }} />
            )}
          </div>
        </div>
        <span className="text-xs text-muted-foreground whitespace-nowrap">
          {job.completed_count}/{job.slide_count}
        </span>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <select
            className="rounded-md border bg-background px-3 py-1.5 text-sm"
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
          >
            <option value="">All Statuses</option>
            <option value="pending">Pending</option>
            <option value="transferring">Transferring</option>
            <option value="running">Running</option>
            <option value="completed">Completed</option>
            <option value="failed">Failed</option>
          </select>
          <span className="text-sm text-muted-foreground">{sortedJobs.length} jobs</span>
        </div>
        <Button variant="outline" size="sm" onClick={handleRefresh} disabled={isRefreshing}>
          <RefreshCw className={`mr-2 h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-8" />
              <TableHead><SortableHeader label="ID" sortKey="id" sortConfig={jobsSortConfig} onSort={handleJobsSort} /></TableHead>
              <TableHead><SortableHeader label="Analysis" sortKey="model_name" sortConfig={jobsSortConfig} onSort={handleJobsSort} /></TableHead>
              <TableHead><SortableHeader label="Status" sortKey="status" sortConfig={jobsSortConfig} onSort={handleJobsSort} /></TableHead>
              <TableHead>Progress</TableHead>
              <TableHead><SortableHeader label="GPU" sortKey="gpu_index" sortConfig={jobsSortConfig} onSort={handleJobsSort} /></TableHead>
              <TableHead><SortableHeader label="Submitted" sortKey="submitted_at" sortConfig={jobsSortConfig} onSort={handleJobsSort} /></TableHead>
              <TableHead>Runtime</TableHead>
              <TableHead className="w-16">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow>
                <TableCell colSpan={9} className="h-24 text-center">
                  Loading jobs...
                </TableCell>
              </TableRow>
            ) : sortedJobs.length === 0 ? (
              <TableRow>
                <TableCell colSpan={9} className="h-24 text-center text-muted-foreground">
                  No jobs found. Submit an analysis from the Submit tab.
                </TableCell>
              </TableRow>
            ) : (
              sortedJobs.map((job) => (
                <>
                  <TableRow key={job.id} className="cursor-pointer" onClick={() => toggleExpand(job.id)}>
                    <TableCell>
                      <button className="p-0.5">
                        {expandedJobId === job.id ? (
                          <ChevronDown className="h-4 w-4" />
                        ) : (
                          <ChevronRight className="h-4 w-4" />
                        )}
                      </button>
                    </TableCell>
                    <TableCell className="font-mono text-sm">{job.id}</TableCell>
                    <TableCell>
                      <div>
                        <span className="font-medium">{job.model_name}</span>
                        {job.model_version && (
                          <span className="text-xs text-muted-foreground ml-1">v{job.model_version}</span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline" className={STATUS_COLORS[job.status] || ''}>
                        {job.status}
                      </Badge>
                    </TableCell>
                    <TableCell className="min-w-[140px]">
                      <ProgressBar job={job} />
                    </TableCell>
                    <TableCell className="text-sm">
                      {job.gpu_index != null ? `GPU ${job.gpu_index}` : '-'}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {job.submitted_at
                        ? new Date(job.submitted_at).toLocaleString()
                        : '-'}
                    </TableCell>
                    <TableCell className="text-sm">{formatDuration(job)}</TableCell>
                    <TableCell>
                      <div className="flex items-center gap-1">
                        {(job.status === 'running' || job.status === 'transferring' || job.status === 'pending') && (
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={(e) => { e.stopPropagation(); handleCancel(job.id) }}
                            title="Cancel job"
                          >
                            <XCircle className="h-4 w-4 text-destructive" />
                          </Button>
                        )}
                        {job.failed_count > 0 && job.status !== 'running' && job.status !== 'transferring' && (
                          <Button
                            variant="ghost"
                            size="icon"
                            disabled={retryingJobs.has(job.id)}
                            onClick={(e) => { e.stopPropagation(); handleRetry(job.id) }}
                            title={`Retry ${job.failed_count} failed slide(s) — skips already-uploaded files`}
                          >
                            <RotateCcw className={`h-4 w-4 text-amber-600 ${retryingJobs.has(job.id) ? 'animate-spin' : ''}`} />
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                  {expandedJobId === job.id && (
                    <TableRow key={`${job.id}-detail`}>
                      <TableCell colSpan={9} className="bg-muted/50 p-0">
                        <div className="p-4 space-y-3">
                          <p className="text-xs font-medium text-muted-foreground">
                            Slides ({job.slide_count})
                          </p>
                          {loadingSlides ? (
                            <p className="text-sm text-muted-foreground">Loading slides...</p>
                          ) : expandedSlides.length === 0 ? (
                            <p className="text-sm text-muted-foreground">No slides in this job.</p>
                          ) : (
                            <div className="rounded border bg-background">
                              <Table>
                                <TableHeader>
                                  <TableRow>
                                    <TableHead>Slide Hash</TableHead>
                                    <TableHead>Status</TableHead>
                                    <TableHead>tmux Session</TableHead>
                                    <TableHead>Error</TableHead>
                                    <TableHead>Log</TableHead>
                                  </TableRow>
                                </TableHeader>
                                <TableBody>
                                  {expandedSlides.map((js) => (
                                    <SlideRow key={js.id} js={js} />
                                  ))}
                                </TableBody>
                              </Table>
                            </div>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  )}
                </>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {jobs.some((j) => j.status === 'failed' && j.error_message) && (
        <div className="space-y-2">
          <h3 className="text-sm font-medium">Recent Errors</h3>
          {jobs
            .filter((j) => j.status === 'failed' && j.error_message)
            .slice(0, 5)
            .map((j) => (
              <div key={j.id} className="text-xs bg-red-500/5 border border-red-200 rounded p-2">
                <span className="font-medium">Job {j.id}:</span> {j.error_message}
              </div>
            ))}
        </div>
      )}
    </div>
  )
}

function SlideRow({ js }: { js: JobSlide }) {
  const [showLog, setShowLog] = useState(false)

  return (
    <>
      <TableRow>
        <TableCell className="font-mono text-xs">
          {js.slide_hash ? js.slide_hash.slice(0, 12) + '...' : '-'}
        </TableCell>
        <TableCell>
          <Badge variant="outline" className={STATUS_COLORS[js.status] || ''}>
            {js.status}
          </Badge>
        </TableCell>
        <TableCell className="font-mono text-xs">
          {js.cluster_job_id || '-'}
        </TableCell>
        <TableCell className="text-xs text-red-600 max-w-[200px] truncate">
          {js.error_message || '-'}
        </TableCell>
        <TableCell>
          {js.log_tail && (
            <button
              onClick={() => setShowLog(!showLog)}
              className="text-xs text-blue-600 hover:underline"
            >
              {showLog ? 'Hide' : 'View'}
            </button>
          )}
        </TableCell>
      </TableRow>
      {showLog && js.log_tail && (
        <TableRow>
          <TableCell colSpan={5} className="p-0">
            <pre className="text-xs font-mono bg-black/80 text-green-400 rounded m-2 p-3 overflow-x-auto max-h-[200px] overflow-y-auto whitespace-pre-wrap">
              {js.log_tail}
            </pre>
          </TableCell>
        </TableRow>
      )}
    </>
  )
}
