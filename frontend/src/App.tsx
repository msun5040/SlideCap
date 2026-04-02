import { useState, useEffect, useRef, useCallback } from 'react'
import {
  LayoutDashboard,
  Microscope,
  Users,
  FlaskConical,
  ClipboardList,
  Package,
  PanelLeftClose,
  PanelLeft,
  X,
  Circle,
  FlaskRound,
  RefreshCw,
  Database,
  Download,
  Upload,
  Trash2,
  Search,
  FileDown,
  HardDriveDownload,
  Ghost,
  Loader2,
} from 'lucide-react'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Dashboard } from '@/components/Dashboard'
import { SlideLibrary } from '@/components/SlideLibrary'
import { CohortDashboard } from '@/components/CohortDashboard'
import { AnalysisDashboard } from '@/components/AnalysisDashboard'
import { RequestTracker } from '@/components/RequestTracker'
import { SlidePull } from '@/components/SlidePull'
import { StudyManager } from '@/components/StudyManager'
import { LauncherScreen } from '@/components/LauncherScreen'
import { LoadingSpinner } from '@/components/LoadingSpinner'
import { setApiBase, getApiBase } from '@/api'

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

type View = 'dashboard' | 'slides' | 'cohorts' | 'studies' | 'requests' | 'pull' | 'analysis'

// Re-export for backward compat
export { getApiBase as getAPI } from '@/api'

export default function App() {
  const [launched, setLaunched] = useState(false)
  const [currentView, setCurrentView] = useState<View>('slides')
  const [isSidebarOpen, setIsSidebarOpen] = useState(true)
  const [sortStatus, setSortStatus] = useState<SortStatus | null>(null)
  const [backendStatus, setBackendStatus] = useState<'connected' | 'disconnected'>('connected')
  const [isLoading, setIsLoading] = useState(true)
  const [menuAction, setMenuAction] = useState<{ type: string; status: 'running' | 'done' | 'error'; message: string } | null>(null)
  const [ghostPreview, setGhostPreview] = useState<any>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const healthPollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    const isElectron = !!(window as any).electronAPI?.isElectron
    if (!isElectron) {
      fetch('http://127.0.0.1:8000/health', { signal: AbortSignal.timeout(3000) })
        .then((res) => {
          if (res.ok) {
            setApiBase('http://127.0.0.1:8000')
            setLaunched(true)
          }
        })
        .catch(() => {})
    }
  }, [])

  const handleLaunchReady = useCallback((apiBase: string) => {
    setApiBase(apiBase)
    setLaunched(true)
  }, [])

  const startPolling = useCallback(() => {
    if (pollRef.current) return
    pollRef.current = setInterval(async () => {
      try {
        const res = await fetch(`${getApiBase()}/staging/sort/status`)
        if (!res.ok) return
        const status: SortStatus = await res.json()
        setSortStatus(status)
        if (!status.running) {
          clearInterval(pollRef.current!)
          pollRef.current = null
        }
      } catch {}
    }, 800)
  }, [])

  useEffect(() => {
    if (!launched) return
    setIsLoading(true)
    fetch(`${getApiBase()}/staging/sort/status`)
      .then(r => r.ok ? r.json() : null)
      .then((status: SortStatus | null) => {
        if (status) {
          setSortStatus(status)
          if (status.running) startPolling()
        }
      })
      .catch(() => {})
      .finally(() => setIsLoading(false))
  }, [launched, startPolling])

  useEffect(() => {
    if (!launched) return
    const checkHealth = async () => {
      try {
        const res = await fetch(`${getApiBase()}/health`, { signal: AbortSignal.timeout(5000) })
        setBackendStatus(res.ok ? 'connected' : 'disconnected')
      } catch {
        setBackendStatus('disconnected')
      }
    }
    checkHealth()
    healthPollRef.current = setInterval(checkHealth, 20000)
    return () => {
      if (healthPollRef.current) clearInterval(healthPollRef.current)
    }
  }, [launched])

  const handleSortStarted = useCallback(() => {
    startPolling()
  }, [startPolling])

  const runMenuAction = useCallback(async (label: string, endpoint: string, method = 'POST') => {
    setMenuAction({ type: label, status: 'running', message: `${label}...` })
    try {
      const res = await fetch(`${getApiBase()}${endpoint}`, { method })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      const summary = data.new_slides_indexed != null
        ? `${data.new_slides_indexed} new slides indexed`
        : data.cached_slides != null
        ? `${data.cached_slides} slides cached`
        : data.slides_to_remove != null
        ? `${data.slides_to_remove} ghost slides removed, ${data.cases_to_remove} empty cases removed`
        : 'Done'
      setMenuAction({ type: label, status: 'done', message: summary })
      setTimeout(() => setMenuAction(null), 4000)
    } catch (e: any) {
      setMenuAction({ type: label, status: 'error', message: e.message || 'Failed' })
      setTimeout(() => setMenuAction(null), 5000)
    }
  }, [])

  const previewGhosts = useCallback(async () => {
    setMenuAction({ type: 'Ghost scan', status: 'running', message: 'Scanning for ghost slides...' })
    try {
      const res = await fetch(`${getApiBase()}/index/ghost-slides`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      setGhostPreview(data)
      setMenuAction({ type: 'Ghost scan', status: 'done', message: `${data.ghost_count} ghost slides found` })
      setTimeout(() => setMenuAction(null), 3000)
    } catch (e: any) {
      setMenuAction({ type: 'Ghost scan', status: 'error', message: e.message })
      setTimeout(() => setMenuAction(null), 5000)
    }
  }, [])

  const exportSlidesCsv = useCallback(() => {
    window.open(`${getApiBase()}/export/slides.csv`, '_blank')
  }, [])

  if (!launched) {
    return <LauncherScreen onReady={handleLaunchReady} />
  }

  const navigationItems = [
    { id: 'dashboard' as View, label: 'Dashboard', icon: LayoutDashboard },
    { id: 'slides' as View, label: 'Slide Library', icon: Microscope },
    { id: 'cohorts' as View, label: 'Cohorts', icon: Users },
    { id: 'studies' as View, label: 'Studies', icon: FlaskRound },
    { id: 'requests' as View, label: 'Requests', icon: ClipboardList },
    { id: 'pull' as View, label: 'WSI Pull', icon: Package },
    { id: 'analysis' as View, label: 'Analysis', icon: FlaskConical },
  ]

  const viewClass = (view: View) =>
    currentView === view ? '' : 'hidden'

  return (
    <div className="flex h-screen bg-background">
      {/* macOS traffic light spacer */}
      <div className="fixed top-0 left-0 right-0 h-8 z-50" style={{ WebkitAppRegion: 'drag' } as React.CSSProperties} />

      {/* Sidebar — dark, minimal */}
      <aside
        className={`${
          isSidebarOpen ? 'w-56' : 'w-0'
        } shrink-0 transition-all duration-200 ease-out overflow-hidden`}
        style={{ backgroundColor: 'var(--sidebar-bg)' }}
      >
        <div className="flex h-full flex-col pt-8">
          {/* Brand */}
          <div className="flex items-center gap-2.5 px-5 h-14">
            <div className="flex h-7 w-7 items-center justify-center rounded-sm" style={{ backgroundColor: 'var(--sidebar-accent)' }}>
              <Microscope className="h-4 w-4 text-white" />
            </div>
            <div>
              <span className="text-sm font-semibold text-white tracking-tight">SlideCap</span>
            </div>
          </div>

          {/* Navigation */}
          <nav className="flex-1 px-3 mt-2 space-y-0.5">
            {navigationItems.map((item) => {
              const Icon = item.icon
              const isActive = currentView === item.id
              return (
                <button
                  key={item.id}
                  onClick={() => setCurrentView(item.id)}
                  className={`flex w-full items-center gap-2.5 rounded-sm px-2.5 py-2 text-[13px] transition-all duration-150 ${
                    isActive
                      ? 'text-white bg-white/10'
                      : 'hover:bg-white/5'
                  }`}
                  style={{ color: isActive ? 'var(--sidebar-active)' : 'var(--sidebar-foreground)' }}
                >
                  <Icon className="h-4 w-4" style={isActive ? { color: 'var(--sidebar-accent)' } : {}} />
                  {item.label}
                </button>
              )
            })}
          </nav>

          {/* Status footer */}
          <div className="px-4 py-3 border-t border-white/10">
            <div className="flex items-center gap-2 text-[11px]" style={{ color: 'var(--sidebar-foreground)' }}>
              <Circle
                className={`h-2 w-2 fill-current ${backendStatus === 'connected' ? 'text-emerald-400' : 'text-red-400'}`}
              />
              {backendStatus === 'connected' ? 'Connected' : 'Offline'}
            </div>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <div className="flex flex-1 flex-col overflow-hidden pt-8">
        {/* Header — menu bar */}
        <header className="flex h-11 items-center gap-1 border-b px-3 bg-background shrink-0">
          <button
            onClick={() => setIsSidebarOpen(!isSidebarOpen)}
            className="text-muted-foreground hover:text-foreground transition-colors duration-150 p-1.5 rounded-sm hover:bg-muted"
          >
            {isSidebarOpen ? (
              <PanelLeftClose className="h-4 w-4" />
            ) : (
              <PanelLeft className="h-4 w-4" />
            )}
          </button>
          <div className="h-4 w-px bg-border mx-1" />

          {/* Menu bar */}
          <DropdownMenu>
            <DropdownMenuTrigger className="px-2.5 py-1 text-[13px] text-muted-foreground hover:text-foreground hover:bg-muted rounded-sm transition-colors outline-none">
              Index
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="w-56">
              <DropdownMenuLabel className="text-xs">Indexing</DropdownMenuLabel>
              <DropdownMenuItem onClick={() => runMenuAction('Incremental index', '/index/incremental')}>
                <RefreshCw className="h-4 w-4 mr-2" />
                Incremental Index
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => runMenuAction('Full index', '/index/full')}>
                <Database className="h-4 w-4 mr-2" />
                Full Reindex
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => runMenuAction('Refresh cache', '/index/refresh-cache')}>
                <Search className="h-4 w-4 mr-2" />
                Refresh Path Cache
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuLabel className="text-xs">Cleanup</DropdownMenuLabel>
              <DropdownMenuItem onClick={previewGhosts}>
                <Ghost className="h-4 w-4 mr-2" />
                Scan Ghost Slides
              </DropdownMenuItem>
              <DropdownMenuItem
                onClick={() => runMenuAction('Cleanup', '/index/cleanup?dry_run=false')}
                className="text-red-600 focus:text-red-600"
              >
                <Trash2 className="h-4 w-4 mr-2" />
                Remove Ghost Slides
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>

          <DropdownMenu>
            <DropdownMenuTrigger className="px-2.5 py-1 text-[13px] text-muted-foreground hover:text-foreground hover:bg-muted rounded-sm transition-colors outline-none">
              Data
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="w-56">
              <DropdownMenuLabel className="text-xs">Export</DropdownMenuLabel>
              <DropdownMenuItem onClick={exportSlidesCsv}>
                <FileDown className="h-4 w-4 mr-2" />
                Export Slide Library (CSV)
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuLabel className="text-xs">Import</DropdownMenuLabel>
              <DropdownMenuItem onClick={() => {
                const input = document.createElement('input')
                input.type = 'file'
                input.accept = '.csv'
                input.onchange = async (e) => {
                  const file = (e.target as HTMLInputElement).files?.[0]
                  if (!file) return
                  setMenuAction({ type: 'Import patients', status: 'running', message: 'Importing...' })
                  const formData = new FormData()
                  formData.append('file', file)
                  try {
                    const res = await fetch(`${getApiBase()}/patients/import-csv`, { method: 'POST', body: formData })
                    if (!res.ok) throw new Error(`HTTP ${res.status}`)
                    const data = await res.json()
                    setMenuAction({ type: 'Import patients', status: 'done', message: `${data.patients_created} patients, ${data.cases_assigned} cases assigned` })
                    setTimeout(() => setMenuAction(null), 4000)
                  } catch (err: any) {
                    setMenuAction({ type: 'Import patients', status: 'error', message: err.message })
                    setTimeout(() => setMenuAction(null), 5000)
                  }
                }
                input.click()
              }}>
                <Upload className="h-4 w-4 mr-2" />
                Import Patient Mapping (CSV)
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => {
                const input = document.createElement('input')
                input.type = 'file'
                input.accept = '.csv'
                input.onchange = async (e) => {
                  const file = (e.target as HTMLInputElement).files?.[0]
                  if (!file) return
                  setMenuAction({ type: 'Import slides', status: 'running', message: 'Importing...' })
                  const formData = new FormData()
                  formData.append('file', file)
                  try {
                    const res = await fetch(`${getApiBase()}/import/slides-csv`, { method: 'POST', body: formData })
                    if (!res.ok) throw new Error(`HTTP ${res.status}`)
                    const data = await res.json()
                    setMenuAction({ type: 'Import slides', status: 'done', message: `${data.matched || 0} slides matched` })
                    setTimeout(() => setMenuAction(null), 4000)
                  } catch (err: any) {
                    setMenuAction({ type: 'Import slides', status: 'error', message: err.message })
                    setTimeout(() => setMenuAction(null), 5000)
                  }
                }
                input.click()
              }}>
                <HardDriveDownload className="h-4 w-4 mr-2" />
                Import Slide List (CSV)
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>

          {/* Spacer + current view label */}
          <div className="h-4 w-px bg-border mx-1" />
          <span className="text-[13px] font-medium text-foreground">
            {navigationItems.find((item) => item.id === currentView)?.label}
          </span>

          {/* Menu action status — right side */}
          <div className="ml-auto flex items-center gap-2">
            {menuAction && (
              <div className={`flex items-center gap-1.5 text-[12px] px-2.5 py-1 rounded-sm ${
                menuAction.status === 'running' ? 'bg-blue-50 text-blue-700 dark:bg-blue-950 dark:text-blue-300'
                : menuAction.status === 'error' ? 'bg-red-50 text-red-700 dark:bg-red-950 dark:text-red-300'
                : 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300'
              }`}>
                {menuAction.status === 'running' && <Loader2 className="h-3 w-3 animate-spin" />}
                <span>{menuAction.message}</span>
                {menuAction.status !== 'running' && (
                  <button onClick={() => setMenuAction(null)} className="ml-1 hover:opacity-70">
                    <X className="h-3 w-3" />
                  </button>
                )}
              </div>
            )}
          </div>
        </header>

        {/* Backend status banner */}
        {backendStatus === 'disconnected' && (
          <div className="border-b bg-red-50 px-5 py-2">
            <div className="flex items-center gap-2 text-[12px] text-red-700">
              <Circle className="h-1.5 w-1.5 fill-red-600 text-red-600" />
              <span className="font-medium">Backend offline — reconnecting...</span>
            </div>
          </div>
        )}

        {/* Sort progress banner */}
        {sortStatus && (sortStatus.running || sortStatus.done) && (
          <div className="border-b px-5 py-2 bg-secondary/50">
            {sortStatus.running ? (
              <div className="space-y-1.5">
                <div className="flex items-center justify-between text-[12px] text-muted-foreground">
                  <span className="font-medium text-foreground">Sorting slides</span>
                  <span className="tabular-nums">{sortStatus.current} / {sortStatus.total}</span>
                </div>
                <div className="h-1 w-full overflow-hidden bg-border">
                  <div
                    className="h-full bg-primary transition-all duration-300 ease-out"
                    style={{ width: sortStatus.total > 0 ? `${(sortStatus.current / sortStatus.total) * 100}%` : '0%' }}
                  />
                </div>
                {sortStatus.current_file && (
                  <p className="truncate text-[11px] text-muted-foreground font-mono">{sortStatus.current_file}</p>
                )}
              </div>
            ) : (
              <div className="space-y-1.5">
                <div className="flex items-center justify-between text-[12px]">
                  <span className="text-emerald-700 font-medium">
                    Sort complete — {sortStatus.sorted} moved
                    {sortStatus.skipped > 0 && `, ${sortStatus.skipped} skipped`}
                  </span>
                  <button
                    className="text-muted-foreground hover:text-foreground transition-colors"
                    onClick={() => setSortStatus(null)}
                  >
                    <X className="h-3 w-3" />
                  </button>
                </div>
                {sortStatus.errors.length > 0 && (
                  <div className="space-y-0.5">
                    {sortStatus.errors.slice(0, 5).map((err, i) => (
                      <p key={i} className="text-[11px] font-mono text-yellow-700">{err}</p>
                    ))}
                    {sortStatus.errors.length > 5 && (
                      <p className="text-[11px] text-yellow-600">+{sortStatus.errors.length - 5} more</p>
                    )}
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Content Area */}
        <main className="flex-1 overflow-hidden p-5">
          <div className="mx-auto max-w-[1600px] h-full overflow-y-auto">
            {isLoading ? (
              <div className="flex items-center justify-center h-64">
                <LoadingSpinner size="lg" label="Loading..." />
              </div>
            ) : (
              <>
                <div className={viewClass('dashboard')}>
                  <Dashboard onSortStarted={handleSortStarted} sortStatus={sortStatus} />
                </div>
                <div className={viewClass('slides')}>
                  <SlideLibrary />
                </div>
                <div className={viewClass('cohorts')}>
                  <CohortDashboard />
                </div>
                <div className={viewClass('studies')}>
                  <StudyManager />
                </div>
                <div className={`${viewClass('requests')} h-full`}>
                  <RequestTracker />
                </div>
                <div className={viewClass('pull')}>
                  <SlidePull />
                </div>
                <div className={viewClass('analysis')}>
                  <AnalysisDashboard />
                </div>
              </>
            )}
          </div>
        </main>
      </div>

      {/* Ghost slides preview dialog */}
      <Dialog open={ghostPreview !== null} onOpenChange={(open) => { if (!open) setGhostPreview(null) }}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-hidden flex flex-col">
          <DialogHeader>
            <DialogTitle>Ghost Slides</DialogTitle>
            <DialogDescription>
              {ghostPreview?.ghost_count ?? 0} slide records with no matching file on disk
              ({ghostPreview?.cached_slides ?? 0} of {ghostPreview?.total_slides_in_db ?? 0} slides have files)
            </DialogDescription>
          </DialogHeader>
          <div className="flex-1 overflow-y-auto space-y-3 pr-1">
            {ghostPreview?.by_case && Object.entries(ghostPreview.by_case).map(([caseId, caseData]: [string, any]) => (
              <div key={caseId} className="border rounded-md p-3">
                <div className="flex items-center gap-2 mb-2">
                  <span className="font-mono text-sm font-medium">{caseId}</span>
                  <Badge variant="outline" className="text-xs">{caseData.year}</Badge>
                  <span className="text-xs text-muted-foreground">{caseData.ghost_slides.length} ghost slides</span>
                </div>
                <div className="space-y-1">
                  {caseData.ghost_slides.map((g: any) => (
                    <div key={g.slidecap_id} className="flex items-center gap-3 text-xs text-muted-foreground">
                      <span className="font-mono w-16">{g.slidecap_id}</span>
                      <span className="w-10">{g.block_id}</span>
                      <Badge variant="secondary" className="text-[10px]">{g.stain_type}</Badge>
                      {g.tags.length > 0 && (
                        <span className="text-[10px]">{g.tags.join(', ')}</span>
                      )}
                      {g.has_active_jobs && (
                        <Badge variant="destructive" className="text-[10px]">active job</Badge>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            ))}
            {ghostPreview?.ghost_count === 0 && (
              <p className="text-sm text-muted-foreground text-center py-8">No ghost slides found. All records match files on disk.</p>
            )}
          </div>
          {ghostPreview?.ghost_count > 0 && (
            <div className="flex items-center justify-between pt-3 border-t">
              <p className="text-xs text-muted-foreground">
                Removing will delete these records from the database. Files are not affected.
              </p>
              <Button
                variant="destructive"
                size="sm"
                onClick={async () => {
                  await runMenuAction('Cleanup', '/index/cleanup?dry_run=false')
                  setGhostPreview(null)
                }}
              >
                <Trash2 className="h-3.5 w-3.5 mr-1.5" />
                Remove {ghostPreview.ghost_count} ghosts
              </Button>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
