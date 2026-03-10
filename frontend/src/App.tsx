import { useState, useEffect, useRef, useCallback } from 'react'
import {
  LayoutDashboard,
  Microscope,
  Users,
  FlaskConical,
  Menu,
  X,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Dashboard } from '@/components/Dashboard'
import { SlideLibrary } from '@/components/SlideLibrary'
import { CohortDashboard } from '@/components/CohortDashboard'
import { AnalysisDashboard } from '@/components/AnalysisDashboard'
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

type View = 'dashboard' | 'slides' | 'cohorts' | 'analysis'

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

  // Auto-launch in dev/browser mode if no Electron
  useEffect(() => {
    const isElectron = !!(window as any).electronAPI?.isElectron
    if (!isElectron) {
      // In browser mode, try connecting directly
      fetch('http://127.0.0.1:8000/health', { signal: AbortSignal.timeout(3000) })
        .then((res) => {
          if (res.ok) {
            setApiBase('http://127.0.0.1:8000')
            setLaunched(true)
          }
        })
        .catch(() => {
          // Show launcher so user can configure
        })
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
      } catch { /* network error, keep polling */ }
    }, 800)
  }, [])

  // On mount: check if a sort is already running (e.g. page reload mid-sort)
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

  // Health check polling - check backend every 20 seconds
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

    // Check immediately on mount
    checkHealth()

    // Then poll every 20 seconds
    healthPollRef.current = setInterval(checkHealth, 20000)

    return () => {
      if (healthPollRef.current) clearInterval(healthPollRef.current)
    }
  }, [launched])

  const handleSortStarted = useCallback(() => {
    startPolling()
  }, [startPolling])

  // Show launcher if not yet connected
  if (!launched) {
    return <LauncherScreen onReady={handleLaunchReady} />
  }

  const navigationItems = [
    { id: 'dashboard' as View, label: 'Dashboard', icon: LayoutDashboard },
    { id: 'slides' as View, label: 'Slide Library', icon: Microscope },
    { id: 'cohorts' as View, label: 'Cohorts', icon: Users },
    { id: 'analysis' as View, label: 'Analysis', icon: FlaskConical },
  ]

  // All views stay mounted so data persists across navigation
  const viewClass = (view: View) =>
    currentView === view ? '' : 'hidden'

  return (
    <div className="flex h-screen bg-background">
      {/* macOS traffic light spacer — draggable titlebar area */}
      <div className="fixed top-0 left-0 right-0 h-8 z-50" style={{ WebkitAppRegion: 'drag' } as React.CSSProperties} />

      {/* Sidebar */}
      <aside
        className={`${
          isSidebarOpen ? 'w-64' : 'w-0'
        } shrink-0 border-r bg-card transition-all duration-300 overflow-hidden`}
      >
        <div className="flex h-full flex-col pt-8">
          {/* Logo */}
          <div className="flex h-16 items-center gap-3 border-b px-6">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary text-primary-foreground">
              <Microscope className="h-6 w-6" />
            </div>
            <div>
              <h1 className="font-semibold">SlideCap</h1>
              <p className="text-xs text-muted-foreground">Slide Management</p>
            </div>
          </div>

          {/* Navigation */}
          <nav className="flex-1 space-y-1 p-4">
            {navigationItems.map((item) => {
              const Icon = item.icon
              const isActive = currentView === item.id
              return (
                <button
                  key={item.id}
                  onClick={() => setCurrentView(item.id)}
                  className={`flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-sm transition-colors ${
                    isActive
                      ? 'bg-primary text-primary-foreground'
                      : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                  }`}
                >
                  <Icon className="h-5 w-5" />
                  {item.label}
                </button>
              )
            })}
          </nav>

          {/* Footer */}
          <div className="border-t p-4">
            <div className="rounded-lg bg-muted p-3">
              <p className="text-xs font-medium">SlideCap</p>
              <p className="text-xs text-muted-foreground mt-1">
                Organize and search pathology slides
              </p>
            </div>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <div className="flex flex-1 flex-col overflow-hidden pt-8">
        {/* Header */}
        <header className="flex h-16 items-center gap-4 border-b bg-card px-6">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setIsSidebarOpen(!isSidebarOpen)}
          >
            {isSidebarOpen ? (
              <X className="h-5 w-5" />
            ) : (
              <Menu className="h-5 w-5" />
            )}
          </Button>
          <div className="flex-1">
            <h2 className="font-semibold">
              {navigationItems.find((item) => item.id === currentView)?.label}
            </h2>
            <p className="text-sm text-muted-foreground">
              SlideCap Management System
            </p>
          </div>
          {/* Connection status dot */}
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <div className={`h-2 w-2 rounded-full ${backendStatus === 'connected' ? 'bg-green-500' : 'bg-red-500'}`} />
            {backendStatus === 'connected' ? 'Connected' : 'Offline'}
          </div>
        </header>

        {/* Backend status banner */}
        {backendStatus === 'disconnected' && (
          <div className="border-b bg-red-50 px-6 py-3">
            <div className="flex items-center gap-2 text-sm text-red-700">
              <div className="h-2 w-2 rounded-full bg-red-600" />
              <span className="font-medium">Backend offline — reconnecting...</span>
            </div>
          </div>
        )}

        {/* Sort progress banner — persists across navigation */}
        {sortStatus && (sortStatus.running || sortStatus.done) && (
          <div className="border-b bg-muted/40 px-6 py-2">
            {sortStatus.running ? (
              <div className="space-y-1">
                <div className="flex items-center justify-between text-xs text-muted-foreground">
                  <span className="font-medium text-foreground">Sorting slides…</span>
                  <span>{sortStatus.current} / {sortStatus.total}</span>
                </div>
                <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
                  <div
                    className="h-full bg-primary transition-all duration-300"
                    style={{ width: sortStatus.total > 0 ? `${(sortStatus.current / sortStatus.total) * 100}%` : '0%' }}
                  />
                </div>
                {sortStatus.current_file && (
                  <p className="truncate text-xs text-muted-foreground">{sortStatus.current_file}</p>
                )}
              </div>
            ) : (
              <div className="flex items-center justify-between text-xs">
                <span className="text-green-700 font-medium">
                  Sort complete — {sortStatus.sorted} moved
                  {sortStatus.skipped > 0 && `, ${sortStatus.skipped} skipped`}
                </span>
                <button
                  className="text-muted-foreground hover:text-foreground"
                  onClick={() => setSortStatus(null)}
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            )}
          </div>
        )}

        {/* Content Area */}
        <main className="flex-1 overflow-y-auto p-6">
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
