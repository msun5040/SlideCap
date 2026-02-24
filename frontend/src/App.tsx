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

const API = 'http://127.0.0.1:8000'

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

export default function App() {
  const [currentView, setCurrentView] = useState<View>('slides')
  const [isSidebarOpen, setIsSidebarOpen] = useState(true)
  const [sortStatus, setSortStatus] = useState<SortStatus | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const startPolling = useCallback(() => {
    if (pollRef.current) return
    pollRef.current = setInterval(async () => {
      try {
        const res = await fetch(`${API}/staging/sort/status`)
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
    fetch(`${API}/staging/sort/status`)
      .then(r => r.ok ? r.json() : null)
      .then((status: SortStatus | null) => {
        if (status) {
          setSortStatus(status)
          if (status.running) startPolling()
        }
      })
      .catch(() => {})
  }, [startPolling])

  const handleSortStarted = useCallback(() => {
    startPolling()
  }, [startPolling])

  const navigationItems = [
    { id: 'dashboard' as View, label: 'Dashboard', icon: LayoutDashboard },
    { id: 'slides' as View, label: 'Slide Library', icon: Microscope },
    { id: 'cohorts' as View, label: 'Cohorts', icon: Users },
    { id: 'analysis' as View, label: 'Analysis', icon: FlaskConical },
  ]

  const renderContent = () => {
    switch (currentView) {
      case 'dashboard':
        return <Dashboard onSortStarted={handleSortStarted} sortStatus={sortStatus} />
      case 'slides':
        return <SlideLibrary />
      case 'cohorts':
        return <CohortDashboard />
      case 'analysis':
        return <AnalysisDashboard />
      default:
        return <SlideLibrary />
    }
  }

  return (
    <div className="flex h-screen bg-background">
      {/* Sidebar */}
      <aside
        className={`${
          isSidebarOpen ? 'w-64' : 'w-0'
        } shrink-0 border-r bg-card transition-all duration-300 overflow-hidden`}
      >
        <div className="flex h-full flex-col">
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
      <div className="flex flex-1 flex-col overflow-hidden">
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
        </header>

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
            {renderContent()}
          </div>
        </main>
      </div>
    </div>
  )
}
