import { useState, useEffect } from 'react'
import { Plus, Users, Trash2, Upload, Filter, Tag } from 'lucide-react'
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
import { CohortBuilder } from '@/components/CohortBuilder'
import type { Cohort } from '@/types/slide'

import { getApiBase } from '@/api'

export function CohortDashboard() {
  const [subView, setSubView] = useState<'list' | 'builder'>('list')
  const [activeCohortId, setActiveCohortId] = useState<number | null>(null)
  const [cohorts, setCohorts] = useState<Cohort[]>([])
  const [loading, setLoading] = useState(true)
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false)
  const [createMode, setCreateMode] = useState<'empty' | 'upload' | 'tag' | null>(null)
  const [newCohortName, setNewCohortName] = useState('')
  const [newCohortDescription, setNewCohortDescription] = useState('')
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [tagName, setTagName] = useState('')
  const [isCreating, setIsCreating] = useState(false)
  const [createResult, setCreateResult] = useState<{ success: boolean; message: string } | null>(null)

  useEffect(() => {
    fetchCohorts()
  }, [])

  const fetchCohorts = async () => {
    setLoading(true)
    try {
      const response = await fetch(`${getApiBase()}/cohorts`)
      if (response.ok) {
        const data = await response.json()
        setCohorts(data)
      }
    } catch (error) {
      console.error('Failed to fetch cohorts:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleCreateEmpty = async () => {
    if (!newCohortName.trim()) return

    setIsCreating(true)
    try {
      const response = await fetch(`${getApiBase()}/cohorts`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: newCohortName,
          description: newCohortDescription || undefined
        })
      })

      if (response.ok) {
        const data = await response.json()
        setIsCreateDialogOpen(false)
        resetCreateDialog()
        // Navigate to builder for new empty cohort
        setActiveCohortId(data.id)
        setSubView('builder')
        return
      } else {
        const error = await response.json()
        setCreateResult({ success: false, message: error.detail || 'Failed to create cohort' })
      }
    } catch (error) {
      setCreateResult({ success: false, message: 'Failed to create cohort' })
    } finally {
      setIsCreating(false)
    }
  }

  const handleCreateFromFile = async () => {
    if (!newCohortName.trim() || !selectedFile) return

    setIsCreating(true)
    try {
      const formData = new FormData()
      formData.append('file', selectedFile)
      formData.append('name', newCohortName)
      if (newCohortDescription) {
        formData.append('description', newCohortDescription)
      }

      const response = await fetch(`${getApiBase()}/cohorts/from-file?name=${encodeURIComponent(newCohortName)}${newCohortDescription ? `&description=${encodeURIComponent(newCohortDescription)}` : ''}`, {
        method: 'POST',
        body: formData
      })

      if (response.ok) {
        const data = await response.json()
        let detail = ''
        if (data.rows_not_matched?.length > 0) {
          detail = `\n\nUnmatched rows (${data.rows_not_matched.length}):\n${data.rows_not_matched.join('\n')}`
        } else if (data.accessions_not_found?.length > 0) {
          detail = ` (${data.accessions_not_found.length} accessions not found: ${data.accessions_not_found.slice(0, 10).join(', ')})`
        }
        setCreateResult({
          success: true,
          message: `Cohort created with ${data.slide_count} slides from ${data.case_count} cases${detail}`
        })
        fetchCohorts()
        resetCreateDialog()
      } else {
        const error = await response.json()
        setCreateResult({ success: false, message: error.detail || 'Failed to create cohort' })
      }
    } catch (error) {
      setCreateResult({ success: false, message: 'Failed to create cohort' })
    } finally {
      setIsCreating(false)
    }
  }

  const handleCreateFromTag = async () => {
    if (!newCohortName.trim() || !tagName.trim()) return

    setIsCreating(true)
    try {
      const response = await fetch(`${getApiBase()}/cohorts/from-tag`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: newCohortName,
          description: newCohortDescription || undefined,
          tag_name: tagName
        })
      })

      if (response.ok) {
        const data = await response.json()
        setCreateResult({
          success: true,
          message: `Cohort created with ${data.slide_count} slides from ${data.case_count} cases`
        })
        fetchCohorts()
        resetCreateDialog()
      } else {
        const error = await response.json()
        setCreateResult({ success: false, message: error.detail || 'Failed to create cohort' })
      }
    } catch (error) {
      setCreateResult({ success: false, message: 'Failed to create cohort' })
    } finally {
      setIsCreating(false)
    }
  }

  const handleDeleteCohort = async (cohortId: number) => {
    if (!confirm('Are you sure you want to delete this cohort?')) return

    try {
      const response = await fetch(`${getApiBase()}/cohorts/${cohortId}`, {
        method: 'DELETE'
      })

      if (response.ok) {
        fetchCohorts()
      }
    } catch (error) {
      console.error('Failed to delete cohort:', error)
    }
  }

  const resetCreateDialog = () => {
    setNewCohortName('')
    setNewCohortDescription('')
    setSelectedFile(null)
    setTagName('')
    setCreateMode(null)
  }

  const getSourceIcon = (sourceType: string) => {
    switch (sourceType) {
      case 'upload':
        return <Upload className="h-4 w-4" />
      case 'filter':
        return <Filter className="h-4 w-4" />
      case 'tag':
        return <Tag className="h-4 w-4" />
      default:
        return <Users className="h-4 w-4" />
    }
  }

  const getSourceLabel = (sourceType: string) => {
    switch (sourceType) {
      case 'upload':
        return 'From File'
      case 'filter':
        return 'From Filter'
      case 'tag':
        return 'From Tag'
      default:
        return 'Manual'
    }
  }

  if (subView === 'builder' && activeCohortId !== null) {
    return (
      <CohortBuilder
        cohortId={activeCohortId}
        onBack={() => {
          setSubView('list')
          setActiveCohortId(null)
          fetchCohorts()
        }}
      />
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold mb-2">Cohorts</h1>
          <p className="text-muted-foreground">
            Manage your slide cohorts for analysis and export
          </p>
        </div>
        <Button onClick={() => setIsCreateDialogOpen(true)}>
          <Plus className="mr-2 h-4 w-4" />
          New Cohort
        </Button>
      </div>

      {createResult && (
        <div className={`p-4 rounded-lg whitespace-pre-wrap ${createResult.success ? 'bg-green-500/10 text-green-700' : 'bg-red-500/10 text-red-700'}`}>
          {createResult.message}
          <button onClick={() => setCreateResult(null)} className="ml-2 underline">Dismiss</button>
        </div>
      )}

      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Slides</TableHead>
              <TableHead>Cases</TableHead>
              <TableHead>Created</TableHead>
              <TableHead className="w-25">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow>
                <TableCell colSpan={6} className="h-24 text-center">
                  Loading cohorts...
                </TableCell>
              </TableRow>
            ) : cohorts.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="h-24 text-center">
                  No cohorts yet. Create one to get started.
                </TableCell>
              </TableRow>
            ) : (
              cohorts.map((cohort) => (
                <TableRow
                  key={cohort.id}
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => {
                    setActiveCohortId(cohort.id)
                    setSubView('builder')
                  }}
                >
                  <TableCell>
                    <div>
                      <p className="font-medium">{cohort.name}</p>
                      {cohort.description && (
                        <p className="text-sm text-muted-foreground truncate max-w-75">
                          {cohort.description}
                        </p>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline" className="gap-1">
                      {getSourceIcon(cohort.source_type)}
                      {getSourceLabel(cohort.source_type)}
                    </Badge>
                  </TableCell>
                  <TableCell>{cohort.slide_count}</TableCell>
                  <TableCell>{cohort.case_count}</TableCell>
                  <TableCell className="text-sm text-muted-foreground">
                    {cohort.created_at ? new Date(cohort.created_at).toLocaleDateString() : '-'}
                  </TableCell>
                  <TableCell>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={(e) => {
                        e.stopPropagation()
                        handleDeleteCohort(cohort.id)
                      }}
                    >
                      <Trash2 className="h-4 w-4 text-destructive" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {/* Create Cohort Dialog */}
      <Dialog open={isCreateDialogOpen} onOpenChange={(open) => {
        setIsCreateDialogOpen(open)
        if (!open) resetCreateDialog()
      }}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Create New Cohort</DialogTitle>
            <DialogDescription>
              Choose how you want to create your cohort
            </DialogDescription>
          </DialogHeader>

          {!createMode ? (
            <div className="grid gap-4 py-4">
              <Button
                variant="outline"
                className="justify-start h-auto py-4"
                onClick={() => setCreateMode('empty')}
              >
                <Users className="mr-3 h-5 w-5" />
                <div className="text-left">
                  <p className="font-medium">Empty Cohort</p>
                  <p className="text-sm text-muted-foreground">Create an empty cohort and add slides manually</p>
                </div>
              </Button>

              <Button
                variant="outline"
                className="justify-start h-auto py-4"
                onClick={() => setCreateMode('upload')}
              >
                <Upload className="mr-3 h-5 w-5" />
                <div className="text-left">
                  <p className="font-medium">From Accession List / Manifest</p>
                  <p className="text-sm text-muted-foreground whitespace-normal">Upload .txt, .csv, or .xlsx — 1 column for accessions, or 3 columns (accession, block, stain) for specific slides</p>
                </div>
              </Button>

              <Button
                variant="outline"
                className="justify-start h-auto py-4"
                onClick={() => setCreateMode('tag')}
              >
                <Tag className="mr-3 h-5 w-5" />
                <div className="text-left">
                  <p className="font-medium">From Tag</p>
                  <p className="text-sm text-muted-foreground">Create from all slides with a specific tag</p>
                </div>
              </Button>
            </div>
          ) : (
            <div className="space-y-4 py-4">
              <Button variant="ghost" size="sm" onClick={() => setCreateMode(null)}>
                ← Back
              </Button>

              <div className="space-y-2">
                <label className="text-sm font-medium">Cohort Name</label>
                <Input
                  placeholder="Enter cohort name..."
                  value={newCohortName}
                  onChange={(e) => setNewCohortName(e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium">Description (optional)</label>
                <Input
                  placeholder="Enter description..."
                  value={newCohortDescription}
                  onChange={(e) => setNewCohortDescription(e.target.value)}
                />
              </div>

              {createMode === 'upload' && (
                <div className="space-y-2">
                  <label className="text-sm font-medium">Accession List / Manifest File</label>
                  <Input
                    type="file"
                    accept=".txt,.csv,.xlsx"
                    onChange={(e) => setSelectedFile(e.target.files?.[0] || null)}
                  />
                  <p className="text-xs text-muted-foreground">
                    Single column: one accession per row (matches all slides). Three columns: accession, block, stain (matches specific slides).
                  </p>
                </div>
              )}

              {createMode === 'tag' && (
                <div className="space-y-2">
                  <label className="text-sm font-medium">Tag Name</label>
                  <Input
                    placeholder="Enter tag name..."
                    value={tagName}
                    onChange={(e) => setTagName(e.target.value)}
                  />
                </div>
              )}

              <Button
                className="w-full"
                disabled={isCreating || !newCohortName.trim() || (createMode === 'upload' && !selectedFile) || (createMode === 'tag' && !tagName.trim())}
                onClick={() => {
                  if (createMode === 'empty') handleCreateEmpty()
                  else if (createMode === 'upload') handleCreateFromFile()
                  else if (createMode === 'tag') handleCreateFromTag()
                }}
              >
                {isCreating ? 'Creating...' : 'Create Cohort'}
              </Button>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
