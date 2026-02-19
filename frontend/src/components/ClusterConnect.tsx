import { useState, useEffect, useCallback } from 'react'
import { Wifi, WifiOff, Cpu, RefreshCw } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import type { ClusterStatus, GpuInfo } from '@/types/slide'

const API_BASE = 'http://localhost:8000'

interface ClusterConnectProps {
  onStatusChange?: (connected: boolean) => void
}

export function ClusterConnect({ onStatusChange }: ClusterConnectProps) {
  const [status, setStatus] = useState<ClusterStatus>({ connected: false })
  const [host, setHost] = useState('cetus.dfci.harvard.edu')
  const [port, setPort] = useState('22')
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [isConnecting, setIsConnecting] = useState(false)
  const [error, setError] = useState('')
  const [showForm, setShowForm] = useState(false)

  const fetchStatus = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/cluster/status`)
      if (res.ok) {
        const data = await res.json()
        setStatus(data)
        onStatusChange?.(data.connected)
      }
    } catch {
      // ignore
    }
  }, [onStatusChange])

  useEffect(() => {
    fetchStatus()
  }, [fetchStatus])

  const handleConnect = async () => {
    if (!host || !username || !password) return
    setIsConnecting(true)
    setError('')

    try {
      const res = await fetch(`${API_BASE}/cluster/connect`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          host,
          port: parseInt(port) || 22,
          username,
          password,
        }),
      })

      if (res.ok) {
        const data = await res.json()
        setStatus({ connected: true, host, username, gpus: data.gpus })
        onStatusChange?.(true)
        setShowForm(false)
        setPassword('')
      } else {
        const err = await res.json()
        setError(err.detail || 'Connection failed')
      }
    } catch {
      setError('Network error')
    } finally {
      setIsConnecting(false)
    }
  }

  const handleDisconnect = async () => {
    try {
      await fetch(`${API_BASE}/cluster/disconnect`, { method: 'POST' })
      setStatus({ connected: false })
      onStatusChange?.(false)
    } catch {
      // ignore
    }
  }

  const refreshGpus = async () => {
    try {
      const res = await fetch(`${API_BASE}/cluster/gpus`)
      if (res.ok) {
        const gpus = await res.json()
        setStatus((prev) => ({ ...prev, gpus }))
      }
    } catch {
      // ignore
    }
  }

  if (status.connected && !showForm) {
    return (
      <div className="rounded-lg border bg-card p-4 space-y-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="h-2.5 w-2.5 rounded-full bg-green-500" />
            <span className="text-sm font-medium">
              Connected to {status.host} as {status.username}
            </span>
          </div>
          <div className="flex gap-2">
            <Button variant="ghost" size="sm" onClick={refreshGpus}>
              <RefreshCw className="mr-1 h-3.5 w-3.5" />
              Refresh GPUs
            </Button>
            <Button variant="outline" size="sm" onClick={handleDisconnect}>
              <WifiOff className="mr-1 h-3.5 w-3.5" />
              Disconnect
            </Button>
          </div>
        </div>

        {status.gpus && status.gpus.length > 0 && (
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
            {status.gpus.map((gpu) => (
              <GpuCard key={gpu.index} gpu={gpu} />
            ))}
          </div>
        )}
      </div>
    )
  }

  return (
    <div className="rounded-lg border bg-card p-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="h-2.5 w-2.5 rounded-full bg-gray-400" />
          <span className="text-sm text-muted-foreground">Not connected to cluster</span>
        </div>
        {!showForm && (
          <Button size="sm" onClick={() => setShowForm(true)}>
            <Wifi className="mr-1 h-3.5 w-3.5" />
            Connect
          </Button>
        )}
      </div>

      {showForm && (
        <div className="space-y-3 pt-2">
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <div className="space-y-1">
              <label className="text-xs font-medium">Host</label>
              <Input
                placeholder="cetus.dfci.harvard.edu"
                value={host}
                onChange={(e) => setHost(e.target.value)}
                className="h-8 text-sm"
              />
            </div>
            <div className="space-y-1">
              <label className="text-xs font-medium">Port</label>
              <Input
                placeholder="22"
                value={port}
                onChange={(e) => setPort(e.target.value)}
                className="h-8 text-sm"
              />
            </div>
            <div className="space-y-1">
              <label className="text-xs font-medium">Username</label>
              <Input
                placeholder="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="h-8 text-sm"
              />
            </div>
            <div className="space-y-1">
              <label className="text-xs font-medium">Password</label>
              <Input
                type="password"
                placeholder="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleConnect()}
                className="h-8 text-sm"
              />
            </div>
          </div>

          {error && (
            <p className="text-sm text-red-600">{error}</p>
          )}

          <div className="flex gap-2">
            <Button size="sm" onClick={handleConnect} disabled={isConnecting || !host || !username || !password}>
              {isConnecting ? 'Connecting...' : 'Connect'}
            </Button>
            <Button variant="ghost" size="sm" onClick={() => setShowForm(false)}>
              Cancel
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}

function GpuCard({ gpu }: { gpu: GpuInfo }) {
  const memPct = Math.round((gpu.memory_used_mb / gpu.memory_total_mb) * 100)
  const isBusy = gpu.utilization_pct > 50 || memPct > 70

  return (
    <div className={`rounded-md border p-3 space-y-2 ${isBusy ? 'border-yellow-300 bg-yellow-500/5' : 'border-green-300 bg-green-500/5'}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1.5">
          <Cpu className="h-3.5 w-3.5 text-muted-foreground" />
          <span className="text-xs font-medium">GPU {gpu.index}</span>
        </div>
        <span className={`text-xs font-medium ${isBusy ? 'text-yellow-700' : 'text-green-700'}`}>
          {isBusy ? 'Busy' : 'Free'}
        </span>
      </div>
      <p className="text-xs text-muted-foreground truncate">{gpu.name}</p>
      <div className="space-y-1">
        <div className="flex justify-between text-xs">
          <span>Memory</span>
          <span>{gpu.memory_used_mb}/{gpu.memory_total_mb} MB</span>
        </div>
        <div className="h-1.5 rounded-full bg-muted overflow-hidden">
          <div
            className={`h-full rounded-full ${memPct > 70 ? 'bg-yellow-500' : 'bg-green-500'}`}
            style={{ width: `${memPct}%` }}
          />
        </div>
        <div className="flex justify-between text-xs">
          <span>Utilization</span>
          <span>{gpu.utilization_pct}%</span>
        </div>
        <div className="h-1.5 rounded-full bg-muted overflow-hidden">
          <div
            className={`h-full rounded-full ${gpu.utilization_pct > 50 ? 'bg-yellow-500' : 'bg-green-500'}`}
            style={{ width: `${gpu.utilization_pct}%` }}
          />
        </div>
      </div>
    </div>
  )
}
