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
} from 'lucide-react'
import { Dashboard } from '@/components/Dashboard'
import { SlideLibrary } from '@/components/SlideLibrary'
import { CohortDashboard } from '@/components/CohortDashboard'
import { AnalysisDashboard } from '@/components/AnalysisDashboard'
import { RequestTracker } from '@/components/RequestTracker'
import { SlidePull } from '@/components/SlidePull'
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

type View = 'dashboard' | 'slides' | 'cohorts' | 'requests' | 'pull' | 'analysis'

// Re-export for backward compat
export { getApiBase as getAPI } from '@/api'

export default function App() {
  const [launched, setLaunched] = useState(false)
  const [currentView, setCurrentView] = useState<View>('slides')
  const [isSidebarOpen, setIsSidebarOpen] = useState(true)
  const [sortStatus, setSortStatus] = useState<SortStatus | null>(null)
  const [backendStatus, setBackendStatus] = useState<'connected' | 'disconnected'>('connected')
  const [isLoading, setIsLoading] = useState(true)
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

  if (!launched) {
    return <LauncherScreen onReady={handleLaunchReady} />
  }

  const navigationItems = [
    { id: 'dashboard' as View, label: 'Dashboard', icon: LayoutDashboard },
    { id: 'slides' as View, label: 'Slide Library', icon: Microscope },
    { id: 'cohorts' as View, label: 'Cohorts', icon: Users },
    { id: 'requests' as View, label: 'Requests', icon: ClipboardList },
    { id: 'pull' as View, label: 'Slide Pull', icon: Package },
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
        {/* Header — lightweight */}
        <header className="flex h-11 items-center gap-3 border-b px-5 bg-background shrink-0">
          <button
            onClick={() => setIsSidebarOpen(!isSidebarOpen)}
            className="text-muted-foreground hover:text-foreground transition-colors duration-150 p-0.5"
          >
            {isSidebarOpen ? (
              <PanelLeftClose className="h-4 w-4" />
            ) : (
              <PanelLeft className="h-4 w-4" />
            )}
          </button>
          <div className="h-4 w-px bg-border" />
          <span className="text-[13px] font-medium text-foreground">
            {navigationItems.find((item) => item.id === currentView)?.label}
          </span>
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
        <main className="flex-1 overflow-y-auto p-5">
          <div className="mx-auto max-w-[1600px]">
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
                <div className={viewClass('requests')}>
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
    </div>
  )
}
