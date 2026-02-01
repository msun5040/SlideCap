import { useState } from 'react'
import {
  LayoutDashboard,
  Microscope,
  Menu,
  X,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { SlideLibrary } from '@/components/SlideLibrary'

type View = 'dashboard' | 'slides'

export default function App() {
  const [currentView, setCurrentView] = useState<View>('slides')
  const [isSidebarOpen, setIsSidebarOpen] = useState(true)

  const navigationItems = [
    { id: 'dashboard' as View, label: 'Dashboard', icon: LayoutDashboard },
    { id: 'slides' as View, label: 'Slide Library', icon: Microscope },
  ]

  const renderContent = () => {
    switch (currentView) {
      case 'dashboard':
        return (
          <div className="space-y-6">
            <h1 className="text-2xl font-semibold">Dashboard</h1>
            <p className="text-muted-foreground">Welcome to SlideCap</p>
          </div>
        )
      case 'slides':
        return <SlideLibrary />
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
        } flex-shrink-0 border-r bg-card transition-all duration-300 overflow-hidden`}
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
