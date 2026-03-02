import { useState, useEffect, useRef } from 'react'
import { ClusterConnect } from '@/components/ClusterConnect'
import { AnalysisRegistry } from '@/components/AnalysisRegistry'
import { AnalysisSubmit } from '@/components/AnalysisSubmit'
import { AnalysisJobs } from '@/components/AnalysisJobs'
import { AnalysisResults } from '@/components/AnalysisResults'

type SubView = 'registry' | 'submit' | 'jobs' | 'results'

const tabs: { id: SubView; label: string }[] = [
  { id: 'registry', label: 'Registry' },
  { id: 'submit', label: 'Submit' },
  { id: 'jobs', label: 'Jobs' },
  { id: 'results', label: 'Results' },
]

export function AnalysisDashboard() {
  const [subView, setSubView] = useState<SubView>('registry')
  const [clusterConnected, setClusterConnected] = useState(false)
  const [showDisconnectBanner, setShowDisconnectBanner] = useState(false)
  const prevConnected = useRef(false)

  const handleStatusChange = (connected: boolean) => {
    if (prevConnected.current && !connected) {
      setShowDisconnectBanner(true)
    }
    if (connected) {
      setShowDisconnectBanner(false)
    }
    prevConnected.current = connected
    setClusterConnected(connected)
  }

  // Auto-dismiss banner after 6 seconds
  useEffect(() => {
    if (!showDisconnectBanner) return
    const id = setTimeout(() => setShowDisconnectBanner(false), 6000)
    return () => clearTimeout(id)
  }, [showDisconnectBanner])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold mb-2">AI Analysis</h1>
        <p className="text-muted-foreground">
          Manage analysis pipelines, submit jobs, and browse results
        </p>
      </div>

      {/* Disconnection banner */}
      {showDisconnectBanner && (
        <div className="fixed top-4 left-1/2 -translate-x-1/2 z-50 flex items-center gap-3 rounded-lg border border-amber-400 bg-amber-50 px-4 py-3 shadow-lg text-sm text-amber-800">
          <span className="h-2 w-2 rounded-full bg-amber-500 shrink-0" />
          <span>Lost connection to cluster. Reconnect to submit jobs or refresh status.</span>
          <button
            className="ml-2 text-amber-600 hover:text-amber-900 font-medium"
            onClick={() => setShowDisconnectBanner(false)}
          >
            ✕
          </button>
        </div>
      )}

      {/* Cluster connection bar */}
      <ClusterConnect onStatusChange={handleStatusChange} />

      {/* Tab bar */}
      <div className="flex gap-1 border-b">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setSubView(tab.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              subView === tab.id
                ? 'border-primary text-foreground'
                : 'border-transparent text-muted-foreground hover:text-foreground hover:border-muted-foreground/30'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {subView === 'registry' && <AnalysisRegistry />}
      {subView === 'submit' && <AnalysisSubmit clusterConnected={clusterConnected} />}
      {subView === 'jobs' && <AnalysisJobs />}
      {subView === 'results' && <AnalysisResults />}
    </div>
  )
}
