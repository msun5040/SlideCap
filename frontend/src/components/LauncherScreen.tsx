import { useState, useEffect, useRef } from 'react'
import { Microscope, FolderOpen, CheckCircle, XCircle, HardDrive } from 'lucide-react'
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
    <div className="flex h-screen items-center justify-center" style={{ backgroundColor: '#111' }}>
      <div className="w-full max-w-md mx-4">
        {/* Brand */}
        <div className="text-center mb-10">
          <div className="inline-flex h-12 w-12 items-center justify-center rounded-sm bg-primary mb-4">
            <Microscope className="h-6 w-6 text-white" />
          </div>
          <h1 className="text-2xl font-semibold text-white tracking-tight">SlideCap</h1>
          <p className="text-[13px] text-neutral-500 mt-1">Pathology Slide Management</p>
        </div>

        {/* Card */}
        <div className="bg-neutral-900 border border-neutral-800 rounded-sm p-6">
          {phase === 'select' && (
            <div className="space-y-5">
              <div>
                <h2 className="text-[14px] font-medium text-white mb-1">Network Root</h2>
                <p className="text-[12px] text-neutral-500">
                  Select the directory containing your slides/ folder
                </p>
              </div>

              {isElectron ? (
                <button
                  onClick={handleSelectDirectory}
                  className="w-full h-20 flex flex-col items-center justify-center gap-2 border border-dashed border-neutral-700 hover:border-primary/50 hover:bg-white/[0.02] transition-all duration-150 rounded-sm cursor-pointer"
                >
                  <FolderOpen className="h-5 w-5 text-neutral-500" />
                  <span className="text-[13px] text-neutral-400">Browse for Directory</span>
                </button>
              ) : (
                <div className="space-y-3">
                  <div className="flex items-center gap-2 p-2.5 border border-blue-900/50 bg-blue-950/30 text-[12px] text-blue-400 rounded-sm">
                    <HardDrive className="h-3.5 w-3.5 shrink-0" />
                    <span>Browser mode — start backend separately</span>
                  </div>
                  <div className="flex gap-2">
                    <input
                      type="text"
                      value={manualPath}
                      onChange={(e) => setManualPath(e.target.value)}
                      placeholder="/Volumes/... or Z:\slides"
                      className="flex-1 bg-neutral-800 border border-neutral-700 text-white rounded-sm px-2.5 py-1.5 text-[13px] font-mono focus:outline-none focus:ring-2 focus:ring-primary placeholder:text-neutral-600"
                      onKeyDown={(e) => e.key === 'Enter' && handleManualConnect()}
                    />
                    <Button onClick={handleManualConnect} disabled={!manualPath.trim()} size="sm">
                      Connect
                    </Button>
                  </div>
                </div>
              )}

              <p className="text-[11px] text-neutral-600 text-center">
                {isElectron ? 'macOS: /Volumes/... \u00b7 Windows: Z:\\...' : 'Backend must be running on port 8000'}
              </p>
            </div>
          )}

          {phase === 'starting' && (
            <div className="space-y-5">
              <div className="text-center py-2">
                <LoadingSpinner size="md" className="mb-3" />
                <h2 className="text-[14px] font-medium text-white">Starting SlideCap</h2>
                <p className="text-[12px] text-neutral-500 mt-1">
                  Initializing backend...
                </p>
              </div>

              {selectedPath && (
                <div className="flex items-center gap-2 p-2 bg-neutral-800 text-[12px] rounded-sm">
                  <FolderOpen className="h-3.5 w-3.5 shrink-0 text-neutral-500" />
                  <span className="truncate font-mono text-neutral-400">{selectedPath}</span>
                </div>
              )}

              {logs.length > 0 && (
                <div className="bg-black rounded-sm p-2.5 max-h-36 overflow-y-auto border border-neutral-800">
                  {logs.map((log, i) => (
                    <p key={i} className={`text-[11px] font-mono leading-relaxed ${log.startsWith('ERROR') ? 'text-red-400' : 'text-neutral-500'}`}>
                      {log}
                    </p>
                  ))}
                  <div ref={logsEndRef} />
                </div>
              )}
            </div>
          )}

          {phase === 'ready' && (
            <div className="text-center py-4 space-y-3">
              <CheckCircle className="h-8 w-8 text-emerald-400 mx-auto" />
              <div>
                <h2 className="text-[14px] font-medium text-white">Ready</h2>
                <p className="text-[12px] text-neutral-500 mt-1">Loading interface...</p>
              </div>
              <LoadingSpinner size="sm" />
            </div>
          )}

          {phase === 'error' && (
            <div className="space-y-5">
              <div className="text-center py-2">
                <XCircle className="h-8 w-8 text-red-400 mx-auto mb-3" />
                <h2 className="text-[14px] font-medium text-white">Connection Failed</h2>
                <p className="text-[12px] text-red-400 mt-2">{error}</p>
              </div>

              {logs.length > 0 && (
                <div className="bg-black rounded-sm p-2.5 max-h-28 overflow-y-auto border border-neutral-800">
                  {logs.map((log, i) => (
                    <p key={i} className={`text-[11px] font-mono leading-relaxed ${log.startsWith('ERROR') ? 'text-red-400' : 'text-neutral-500'}`}>
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

        <p className="text-center text-[11px] text-neutral-600 mt-5">v0.1.0</p>
      </div>
    </div>
  )
}
