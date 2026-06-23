import { useState, useEffect } from 'react'
import { Plus, Pencil, Power, PowerOff } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { Analysis, AnalysisKind } from '@/types/slide'
import { TransformsEditor } from '@/components/TransformsEditor'

import { getApiBase } from '@/api'

export function AnalysisRegistry() {
  const [analyses, setAnalyses] = useState<Analysis[]>([])
  const [kinds, setKinds] = useState<AnalysisKind[]>([])
  const [loading, setLoading] = useState(true)
  const [isDialogOpen, setIsDialogOpen] = useState(false)
  const [editingAnalysis, setEditingAnalysis] = useState<Analysis | null>(null)

  // Form state
  const [formName, setFormName] = useState('')
  const [formVersion, setFormVersion] = useState('1.0')
  const [formDescription, setFormDescription] = useState('')
  const [formKind, setFormKind] = useState('cellvit')
  const [formScriptPath, setFormScriptPath] = useState('')
  const [formWorkingDir, setFormWorkingDir] = useState('')
  const [formEnvSetup, setFormEnvSetup] = useState('')
  const [formCommandTemplate, setFormCommandTemplate] = useState('')
  const [formExecutionMode, setFormExecutionMode] = useState<'batch' | 'sharded'>('batch')
  const [formDoneGlob, setFormDoneGlob] = useState('')
  const [formMaxGpus, setFormMaxGpus] = useState(0)
  const [formParamsSchema, setFormParamsSchema] = useState('')
  const [formDefaultParams, setFormDefaultParams] = useState('')
  const [formGpuRequired, setFormGpuRequired] = useState(true)
  const [formEstRuntime, setFormEstRuntime] = useState(60)
  const [formTransforms, setFormTransforms] = useState('')
  const [isSaving, setIsSaving] = useState(false)

  useEffect(() => {
    fetchAnalyses()
    fetch(`${getApiBase()}/analyses/kinds`)
      .then(r => r.ok ? r.json() : [])
      .then(setKinds)
      .catch(() => setKinds([]))
  }, [])

  const selectedKind = kinds.find(k => k.id === formKind)

  const fetchAnalyses = async () => {
    setLoading(true)
    try {
      const response = await fetch(`${getApiBase()}/analyses`)
      if (response.ok) {
        setAnalyses(await response.json())
      }
    } catch (error) {
      console.error('Failed to fetch analyses:', error)
    } finally {
      setLoading(false)
    }
  }

  const resetForm = () => {
    setFormName('')
    setFormVersion('1.0')
    setFormDescription('')
    setFormKind('cellvit')
    setFormScriptPath('')
    setFormWorkingDir('')
    setFormEnvSetup('')
    setFormCommandTemplate('')
    setFormExecutionMode('batch')
    setFormDoneGlob('')
    setFormMaxGpus(0)
    setFormParamsSchema('')
    setFormDefaultParams('')
    setFormGpuRequired(true)
    setFormEstRuntime(60)
    setFormTransforms('')
    setEditingAnalysis(null)
  }

  const openCreateDialog = () => {
    resetForm()
    setIsDialogOpen(true)
  }

  const openEditDialog = (analysis: Analysis) => {
    setEditingAnalysis(analysis)
    setFormName(analysis.name)
    setFormVersion(analysis.version)
    setFormDescription(analysis.description || '')
    setFormKind(analysis.kind || 'cellvit')
    setFormScriptPath(analysis.script_path || '')
    setFormWorkingDir(analysis.working_directory || '')
    setFormEnvSetup(analysis.env_setup || '')
    setFormCommandTemplate(analysis.command_template || '')
    setFormExecutionMode(analysis.execution_mode === 'sharded' ? 'sharded' : 'batch')
    setFormDoneGlob(analysis.done_glob || '')
    setFormMaxGpus(analysis.max_parallel_gpus || 0)
    setFormParamsSchema(analysis.parameters_schema || '')
    setFormDefaultParams(analysis.default_parameters || '')
    setFormGpuRequired(analysis.gpu_required)
    setFormEstRuntime(analysis.estimated_runtime_minutes)
    setFormTransforms(analysis.transforms || '')
    setIsDialogOpen(true)
  }

  const handleSave = async () => {
    if (!formName.trim()) return
    setIsSaving(true)

    try {
      const body: Record<string, unknown> = {
        name: formName,
        version: formVersion,
        description: formDescription || undefined,
        kind: formKind,
        script_path: formScriptPath || undefined,
        working_directory: formWorkingDir || undefined,
        env_setup: formEnvSetup || undefined,
        command_template: formCommandTemplate || undefined,
        execution_mode: formExecutionMode,
        done_glob: formDoneGlob || undefined,
        max_parallel_gpus: formMaxGpus,
        parameters_schema: formParamsSchema || undefined,
        default_parameters: formDefaultParams || undefined,
        gpu_required: formGpuRequired,
        estimated_runtime_minutes: formEstRuntime,
        transforms: formTransforms || undefined,
      }

      const url = editingAnalysis
        ? `${getApiBase()}/analyses/${editingAnalysis.id}`
        : `${getApiBase()}/analyses`
      const method = editingAnalysis ? 'PATCH' : 'POST'

      const response = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })

      if (response.ok) {
        setIsDialogOpen(false)
        resetForm()
        fetchAnalyses()
      } else {
        const error = await response.json()
        alert(error.detail || 'Failed to save analysis')
      }
    } catch (error) {
      console.error('Failed to save analysis:', error)
    } finally {
      setIsSaving(false)
    }
  }

  const toggleActive = async (analysis: Analysis) => {
    try {
      const response = await fetch(`${getApiBase()}/analyses/${analysis.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ active: !analysis.active }),
      })
      if (response.ok) {
        fetchAnalyses()
      }
    } catch (error) {
      console.error('Failed to toggle analysis:', error)
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          Registered analysis pipelines available for job submission
        </p>
        <Button onClick={openCreateDialog}>
          <Plus className="mr-2 h-4 w-4" />
          Add Analysis
        </Button>
      </div>

      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Version</TableHead>
              <TableHead>Script</TableHead>
              <TableHead>GPU</TableHead>
              <TableHead>Est. Runtime</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Jobs</TableHead>
              <TableHead className="w-24">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow>
                <TableCell colSpan={8} className="h-24 text-center">
                  Loading analyses...
                </TableCell>
              </TableRow>
            ) : analyses.length === 0 ? (
              <TableRow>
                <TableCell colSpan={8} className="h-24 text-center">
                  No analyses registered. Add one to get started.
                </TableCell>
              </TableRow>
            ) : (
              analyses.map((a) => (
                <TableRow key={a.id}>
                  <TableCell>
                    <div>
                      <p className="font-medium">{a.name}</p>
                      {a.description && (
                        <p className="text-sm text-muted-foreground truncate max-w-[200px]">
                          {a.description}
                        </p>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>{a.version}</TableCell>
                  <TableCell>
                    <span className="text-xs font-mono truncate max-w-[150px] block">
                      {a.script_path || '-'}
                    </span>
                  </TableCell>
                  <TableCell>
                    {a.gpu_required ? (
                      <Badge variant="default">GPU</Badge>
                    ) : (
                      <Badge variant="secondary">CPU</Badge>
                    )}
                  </TableCell>
                  <TableCell>{a.estimated_runtime_minutes} min</TableCell>
                  <TableCell>
                    <Badge variant={a.active ? 'default' : 'secondary'}>
                      {a.active ? 'Active' : 'Inactive'}
                    </Badge>
                  </TableCell>
                  <TableCell>{a.job_count ?? 0}</TableCell>
                  <TableCell>
                    <div className="flex gap-1">
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => openEditDialog(a)}
                        title="Edit"
                      >
                        <Pencil className="h-4 w-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => toggleActive(a)}
                        title={a.active ? 'Deactivate' : 'Activate'}
                      >
                        {a.active ? (
                          <PowerOff className="h-4 w-4 text-destructive" />
                        ) : (
                          <Power className="h-4 w-4 text-green-600" />
                        )}
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {/* Add/Edit Dialog */}
      <Dialog
        open={isDialogOpen}
        onOpenChange={(open) => {
          setIsDialogOpen(open)
          if (!open) resetForm()
        }}
      >
        <DialogContent className="max-w-lg max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {editingAnalysis ? 'Edit Analysis' : 'Register New Analysis'}
            </DialogTitle>
            <DialogDescription>
              {editingAnalysis
                ? 'Update the analysis pipeline configuration'
                : 'Add a new analysis pipeline to the registry'}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Name *</label>
                <Input
                  placeholder="e.g. CellViT"
                  value={formName}
                  onChange={(e) => setFormName(e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Version</label>
                <Input
                  placeholder="1.0"
                  value={formVersion}
                  onChange={(e) => setFormVersion(e.target.value)}
                />
              </div>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Description</label>
              <Input
                placeholder="What does this analysis do?"
                value={formDescription}
                onChange={(e) => setFormDescription(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Kind</label>
              <select
                className="w-full rounded-md border bg-background px-3 py-2 text-sm"
                value={formKind}
                onChange={(e) => setFormKind(e.target.value)}
              >
                {kinds.length === 0 && (
                  <option value={formKind}>{formKind}</option>
                )}
                {kinds.map((k) => (
                  <option key={k.id} value={k.id}>{k.name} ({k.id})</option>
                ))}
              </select>
              <p className="text-xs text-muted-foreground">
                {selectedKind?.description || 'Determines which ops are available below and supplies the default ruleset.'}
              </p>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Script Path on Cluster</label>
              <Input
                placeholder="/path/to/run_script.sh"
                value={formScriptPath}
                onChange={(e) => setFormScriptPath(e.target.value)}
                className="font-mono text-sm"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Working Directory</label>
              <Input
                placeholder="/path/to/working/dir"
                value={formWorkingDir}
                onChange={(e) => setFormWorkingDir(e.target.value)}
                className="font-mono text-sm"
              />
              <p className="text-xs text-muted-foreground">Directory to cd into before running the script</p>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Environment Setup</label>
              <textarea
                className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[60px]"
                placeholder="source venv/bin/activate && export TMPDIR=/tmp"
                value={formEnvSetup}
                onChange={(e) => setFormEnvSetup(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">Commands to run before the script (e.g. activate venv, set env vars)</p>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Command Template</label>
              <textarea
                className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[60px]"
                placeholder="./run_script.sh {wsi_dir} {outdir} ./model.pth {gpu} {batch_size}"
                value={formCommandTemplate}
                onChange={(e) => setFormCommandTemplate(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Available placeholders: {'{wsi_path}'}, {'{wsi_dir}'}, {'{outdir}'}, {'{gpu}'}, {'{batch_size}'}, {'{model_path}'}
              </p>
            </div>

            {/* ── Execution model ── */}
            <div className="rounded-md border p-3 space-y-3 bg-muted/20">
              <div className="space-y-2">
                <label className="text-sm font-medium">Execution Mode</label>
                <div className="flex gap-2">
                  <button
                    type="button"
                    className={`text-sm px-3 py-1.5 rounded-md border transition-colors ${formExecutionMode === 'batch' ? 'bg-primary text-primary-foreground border-primary' : 'border-input hover:bg-muted'}`}
                    onClick={() => setFormExecutionMode('batch')}
                  >
                    Batch (1 session / 1 GPU)
                  </button>
                  <button
                    type="button"
                    className={`text-sm px-3 py-1.5 rounded-md border transition-colors ${formExecutionMode === 'sharded' ? 'bg-primary text-primary-foreground border-primary' : 'border-input hover:bg-muted'}`}
                    onClick={() => setFormExecutionMode('sharded')}
                  >
                    Sharded (parallel across GPUs)
                  </button>
                </div>
                <p className="text-xs text-muted-foreground">
                  Sharded splits slides across all GPUs (one warm session each) and reports live per-slide status. Batch is the legacy single-session behavior.
                </p>
              </div>

              {formExecutionMode === 'sharded' && (
                <div className="grid grid-cols-2 gap-3">
                  <div className="space-y-1.5">
                    <label className="text-sm font-medium">Per-slide Done Pattern</label>
                    <input
                      className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono"
                      placeholder="{stem}_cells.pt"
                      value={formDoneGlob}
                      onChange={(e) => setFormDoneGlob(e.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">
                      Output file marking one slide done. {'{stem}'} = slide filename without extension. Empty = any output containing the stem.
                    </p>
                  </div>
                  <div className="space-y-1.5">
                    <label className="text-sm font-medium">Max GPUs</label>
                    <input
                      type="number"
                      min={0}
                      className="w-full rounded-md border bg-background px-3 py-2 text-sm"
                      value={formMaxGpus}
                      onChange={(e) => setFormMaxGpus(Number(e.target.value))}
                    />
                    <p className="text-xs text-muted-foreground">0 = use every GPU the cluster reports.</p>
                  </div>
                </div>
              )}
            </div>

            <TransformsEditor
              value={formTransforms}
              onChange={setFormTransforms}
              kind={formKind}
              defaultRules={selectedKind?.default_rules}
            />

            <div className="space-y-2">
              <label className="text-sm font-medium">Parameters Schema (JSON Schema)</label>
              <textarea
                className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[60px]"
                placeholder='{"type": "object", "properties": {...}}'
                value={formParamsSchema}
                onChange={(e) => setFormParamsSchema(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Default Parameters (JSON)</label>
              <textarea
                className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[60px]"
                placeholder='{"batch_size": 4}'
                value={formDefaultParams}
                onChange={(e) => setFormDefaultParams(e.target.value)}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">GPU Required</label>
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={formGpuRequired}
                    onChange={(e) => setFormGpuRequired(e.target.checked)}
                    className="h-4 w-4"
                  />
                  <span className="text-sm text-muted-foreground">
                    Requires GPU allocation
                  </span>
                </div>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Est. Runtime (minutes)</label>
                <Input
                  type="number"
                  min={1}
                  value={formEstRuntime}
                  onChange={(e) => setFormEstRuntime(parseInt(e.target.value) || 60)}
                />
              </div>
            </div>

            <Button
              className="w-full"
              disabled={isSaving || !formName.trim()}
              onClick={handleSave}
            >
              {isSaving
                ? 'Saving...'
                : editingAnalysis
                  ? 'Update Analysis'
                  : 'Register Analysis'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
