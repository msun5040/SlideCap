import { useState } from 'react'
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

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold mb-2">AI Analysis</h1>
        <p className="text-muted-foreground">
          Manage analysis pipelines, submit jobs, and browse results
        </p>
      </div>

      {/* Cluster connection bar */}
      <ClusterConnect onStatusChange={setClusterConnected} />

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
