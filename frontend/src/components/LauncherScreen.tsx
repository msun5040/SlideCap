import { useState, useEffect, useRef } from 'react'
import { Microscope, FolderOpen, Loader2, CheckCircle, XCircle, HardDrive } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { LoadingSpinner } from './LoadingSpinner'

type LaunchPhase = 'select' | 'starting' | 'ready' | 'error'

interface ElectronAPI {
  selectDirectory: () => Promise<string | null>
  startBackend: (networkRoot: string) => Promise<{ success: boolean; port?: number; error?: string }>
  onBackendLog: (callback: (msg: string) => void) => () => void
  onBackendError: (callback: (msg: string) => void) => () => void
  isElectron: boolean
}

declare global {
  interface Window {
    electronAPI?: ElectronAPI
  }
}

interface LauncherScreenProps {
  onReady: (apiBase: string) => void
}

export function LauncherScreen({ onReady }: LauncherScreenProps) {
  const [phase, setPhase] = useState<LaunchPhase>('select')
  const [selectedPath, setSelectedPath] = useState<string>('')
  const [error, setError] = useState<string>('')
  const [logs, setLogs] = useState<string[]>([])
  const [manualPath, setManualPath] = useState('')
  const logsEndRef = useRef<HTMLDivElement>(null)
  const isElectron = !!window.electronAPI?.isElectron

  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  // Listen for backend logs
  useEffect(() => {
    if (!isElectron) return
    const cleanupLog = window.electronAPI!.onBackendLog((msg) => {
      setLogs((prev) => [...prev.slice(-100), msg.trim()])
    })
    const cleanupErr = window.electronAPI!.onBackendError((msg) => {
      setLogs((prev) => [...prev.slice(-100), `ERROR: ${msg}`])
    })
    return () => {
      cleanupLog()
      cleanupErr()
    }
  }, [isElectron])

  const handleSelectDirectory = async () => {
    if (isElectron) {
      const dir = await window.electronAPI!.selectDirectory()
      if (dir) {
        setSelectedPath(dir)
        startBackendWithPath(dir)
      }
    }
  }

  const handleManualConnect = () => {
    if (!manualPath.trim()) return
    setSelectedPath(manualPath.trim())
    startBackendWithPath(manualPath.trim())
  }

  const startBackendWithPath = async (networkRoot: string) => {
    setPhase('starting')
    setLogs([])
    setError('')

    if (isElectron) {
      const result = await window.electronAPI!.startBackend(networkRoot)
      if (result.success) {
        setPhase('ready')
        setTimeout(() => {
          onReady(`http://127.0.0.1:${result.port}`)
        }, 800)
      } else {
        setPhase('error')
        setError(result.error || 'Failed to start backend')
      }
    } else {
      // Browser mode - just connect to existing backend
      try {
        const res = await fetch('http://127.0.0.1:8000/health', {
          signal: AbortSignal.timeout(5000),
        })
        if (res.ok) {
          setPhase('ready')
          setTimeout(() => onReady('http://127.0.0.1:8000'), 500)
        } else {
          setPhase('error')
          setError('Backend responded but is not healthy')
        }
      } catch {
        setPhase('error')
        setError('Cannot connect to backend at http://127.0.0.1:8000')
      }
    }
  }

  const handleRetry = () => {
    setPhase('select')
    setError('')
    setLogs([])
    setSelectedPath('')
  }

  return (
    <div className="flex h-screen items-center justify-center bg-gradient-to-br from-gray-50 to-gray-100">
      <div className="w-full max-w-lg mx-4">
        {/* Logo + Title */}
        <div className="text-center mb-8">
          <div className="inline-flex h-20 w-20 items-center justify-center rounded-2xl bg-primary text-primary-foreground mb-4 shadow-lg">
            <Microscope className="h-10 w-10" />
          </div>
          <h1 className="text-3xl font-bold tracking-tight">SlideCap</h1>
          <p className="text-muted-foreground mt-1">Pathology Slide Management</p>
        </div>

        {/* Main Card */}
        <div className="bg-white rounded-2xl shadow-xl border p-8">
          {phase === 'select' && (
            <div className="space-y-6">
              <div className="text-center">
                <h2 className="text-lg font-semibold mb-1">Select Network Root</h2>
                <p className="text-sm text-muted-foreground">
                  Choose the root directory where your slide folders are located
                </p>
              </div>

              {isElectron ? (
                <Button
                  onClick={handleSelectDirectory}
                  className="w-full h-24 text-base flex-col gap-2 rounded-xl border-2 border-dashed border-muted-foreground/20 bg-muted/30 hover:bg-muted/50 hover:border-primary/30 text-foreground"
                  variant="ghost"
                >
                  <FolderOpen className="h-8 w-8 text-muted-foreground" />
                  <span>Browse for Directory</span>
                </Button>
              ) : (
                <div className="space-y-3">
                  <div className="flex items-center gap-2 p-3 rounded-lg bg-blue-50 text-blue-700 text-sm">
                    <HardDrive className="h-4 w-4 shrink-0" />
                    <span>Running in browser mode. Make sure the backend is started separately.</span>
                  </div>
                  <div className="flex gap-2">
                    <input
                      type="text"
                      value={manualPath}
                      onChange={(e) => setManualPath(e.target.value)}
                      placeholder="/Volumes/... or Z:\slides"
                      className="flex-1 rounded-lg border px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/20"
                      onKeyDown={(e) => e.key === 'Enter' && handleManualConnect()}
                    />
                    <Button onClick={handleManualConnect} disabled={!manualPath.trim()}>
                      Connect
                    </Button>
                  </div>
                </div>
              )}

              <div className="text-xs text-muted-foreground text-center space-y-1">
                <p>This directory should contain your <code className="bg-muted px-1 rounded">slides/</code> folder</p>
                <p className="text-muted-foreground/60">
                  {isElectron ? 'macOS: /Volumes/... • Windows: Z:\\...' : 'Backend must be running on port 8000'}
                </p>
              </div>
            </div>
          )}

          {phase === 'starting' && (
            <div className="space-y-6">
              <div className="text-center">
                <LoadingSpinner size="lg" className="mb-4" />
                <h2 className="text-lg font-semibold">Starting SlideCap</h2>
                <p className="text-sm text-muted-foreground mt-1">
                  Initializing backend and indexing slides...
                </p>
              </div>

              {selectedPath && (
                <div className="flex items-center gap-2 p-3 rounded-lg bg-muted text-sm">
                  <FolderOpen className="h-4 w-4 shrink-0 text-muted-foreground" />
                  <span className="truncate font-mono text-xs">{selectedPath}</span>
                </div>
              )}

              {/* Log output */}
              {logs.length > 0 && (
                <div className="bg-gray-900 rounded-lg p-3 max-h-40 overflow-y-auto">
                  {logs.map((log, i) => (
                    <p key={i} className={`text-xs font-mono ${log.startsWith('ERROR') ? 'text-red-400' : 'text-gray-300'}`}>
                      {log}
                    </p>
                  ))}
                  <div ref={logsEndRef} />
                </div>
              )}
            </div>
          )}

          {phase === 'ready' && (
            <div className="text-center space-y-4">
              <div className="inline-flex h-16 w-16 items-center justify-center rounded-full bg-green-100">
                <CheckCircle className="h-8 w-8 text-green-600" />
              </div>
              <div>
                <h2 className="text-lg font-semibold">Ready!</h2>
                <p className="text-sm text-muted-foreground mt-1">
                  Loading SlideCap...
                </p>
              </div>
              <LoadingSpinner size="sm" />
            </div>
          )}

          {phase === 'error' && (
            <div className="space-y-6">
              <div className="text-center">
                <div className="inline-flex h-16 w-16 items-center justify-center rounded-full bg-red-100 mb-4">
                  <XCircle className="h-8 w-8 text-red-600" />
                </div>
                <h2 className="text-lg font-semibold">Connection Failed</h2>
                <p className="text-sm text-red-600 mt-2">{error}</p>
              </div>

              {/* Log output on error */}
              {logs.length > 0 && (
                <div className="bg-gray-900 rounded-lg p-3 max-h-32 overflow-y-auto">
                  {logs.map((log, i) => (
                    <p key={i} className={`text-xs font-mono ${log.startsWith('ERROR') ? 'text-red-400' : 'text-gray-300'}`}>
                      {log}
                    </p>
                  ))}
                </div>
              )}

              <Button onClick={handleRetry} className="w-full" variant="outline">
                Try Again
              </Button>
            </div>
          )}
        </div>

        {/* Version */}
        <p className="text-center text-xs text-muted-foreground mt-4">
          SlideCap v0.1.0
        </p>
      </div>
    </div>
  )
}
