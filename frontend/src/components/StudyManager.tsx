import { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import {
  ArrowLeft, Plus, Search, X, FolderOpen, Trash2,
  MoreHorizontal, Grid3X3, List, GripVertical,
  ChevronRight, Microscope, Check, Loader2,
  FolderPlus, Users, Edit2, ExternalLink, Filter,
  LayoutGrid, Rows3, Tag,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { Study, StudyDetail, StudySlide, StudyGroup, Slide } from '@/types/slide'
import { getApiBase } from '@/api'

// ─── Color palette for groups ───────────────────────────────
const GROUP_COLORS = [
  '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6',
  '#06b6d4', '#ec4899', '#84cc16', '#f97316', '#6366f1',
]

// ─── Helpers ────────────────────────────────────────────────
function formatBytes(bytes?: number): string {
  if (!bytes) return '—'
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function getInitials(name: string): string {
  return name.split(/[\s_-]+/).map(w => w[0]).filter(Boolean).slice(0, 2).join('').toUpperCase()
}

// ─── Study List View ────────────────────────────────────────

function StudyListView({ onSelect }: { onSelect: (id: number) => void }) {
  const [studies, setStudies] = useState<Study[]>([])
  const [loading, setLoading] = useState(true)
  const [showCreate, setShowCreate] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [newFolder, setNewFolder] = useState('')
  const [creating, setCreating] = useState(false)
  const [error, setError] = useState('')

  const fetchStudies = useCallback(async () => {
    try {
      const res = await fetch(`${getApiBase()}/studies`)
      if (res.ok) setStudies(await res.json())
    } catch {} finally { setLoading(false) }
  }, [])

  useEffect(() => { fetchStudies() }, [fetchStudies])

  const handleCreate = async () => {
    if (!newName.trim()) return
    setCreating(true)
    setError('')
    try {
      const folder = newFolder.trim() || newName.trim().replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '')
      const res = await fetch(`${getApiBase()}/studies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newName, description: newDesc, folder_name: folder }),
      })
      if (!res.ok) {
        const d = await res.json().catch(() => ({}))
        throw new Error(d.detail || 'Failed to create study')
      }
      setShowCreate(false)
      setNewName(''); setNewDesc(''); setNewFolder('')
      fetchStudies()
    } catch (e: any) { setError(e.message) }
    finally { setCreating(false) }
  }

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-lg font-semibold text-foreground">Studies</h2>
          <p className="text-[13px] text-muted-foreground mt-0.5">
            Organize research slides into studies with nested groups
          </p>
        </div>
        <Button size="sm" onClick={() => setShowCreate(true)} className="h-8 text-[12px]">
          <Plus className="h-3.5 w-3.5 mr-1.5" />New Study
        </Button>
      </div>

      {/* Study grid */}
      {loading ? (
        <div className="flex items-center justify-center h-48">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        </div>
      ) : studies.length === 0 ? (
        <div className="flex flex-col items-center justify-center h-48 text-center">
          <FolderOpen className="h-10 w-10 text-muted-foreground/40 mb-3" />
          <p className="text-[13px] text-muted-foreground">No studies yet</p>
          <p className="text-[12px] text-muted-foreground/60 mt-1">
            Create a study to organize research slides
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {studies.map(s => (
            <button
              key={s.id}
              onClick={() => onSelect(s.id)}
              className="group text-left border border-border rounded-lg p-4 hover:border-primary/40 hover:shadow-sm transition-all duration-150 bg-background"
            >
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center gap-2.5">
                  <div className="flex h-9 w-9 items-center justify-center rounded-md bg-primary/10 text-primary text-[12px] font-bold shrink-0">
                    {getInitials(s.name)}
                  </div>
                  <div className="min-w-0">
                    <p className="text-[13px] font-semibold text-foreground truncate">{s.name}</p>
                    <p className="text-[11px] text-muted-foreground font-mono truncate">{s.folder_name}/</p>
                  </div>
                </div>
                <ChevronRight className="h-4 w-4 text-muted-foreground/40 group-hover:text-primary transition-colors shrink-0 mt-0.5" />
              </div>
              {s.description && (
                <p className="text-[12px] text-muted-foreground line-clamp-2 mb-3">{s.description}</p>
              )}
              <div className="flex items-center gap-3 text-[11px] text-muted-foreground">
                <span className="flex items-center gap-1">
                  <Microscope className="h-3 w-3" />{s.slide_count} slides
                </span>
                <span className="flex items-center gap-1">
                  <Users className="h-3 w-3" />{s.group_count} groups
                </span>
              </div>
            </button>
          ))}
        </div>
      )}

      {/* Create dialog */}
      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="text-[15px]">Create Study</DialogTitle>
            <DialogDescription className="text-[12px]">
              A folder will be created on the network drive for this study's slides.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 mt-2">
            <div>
              <label className="text-[12px] font-medium text-foreground mb-1 block">Study Name</label>
              <Input
                value={newName}
                onChange={e => setNewName(e.target.value)}
                placeholder="e.g. Melanoma TMA Project"
                className="h-8 text-[13px]"
              />
            </div>
            <div>
              <label className="text-[12px] font-medium text-foreground mb-1 block">Folder Name</label>
              <Input
                value={newFolder}
                onChange={e => setNewFolder(e.target.value)}
                placeholder={newName.trim().replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '') || 'auto-generated'}
                className="h-8 text-[13px] font-mono"
              />
              <p className="text-[11px] text-muted-foreground mt-1">
                Created at: slides/studies/{newFolder.trim() || newName.trim().replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '') || '...'}/
              </p>
            </div>
            <div>
              <label className="text-[12px] font-medium text-foreground mb-1 block">Description</label>
              <Input
                value={newDesc}
                onChange={e => setNewDesc(e.target.value)}
                placeholder="Optional description"
                className="h-8 text-[13px]"
              />
            </div>
            {error && <p className="text-[12px] text-red-600">{error}</p>}
            <div className="flex justify-end gap-2 pt-1">
              <Button variant="outline" size="sm" onClick={() => setShowCreate(false)} className="h-8 text-[12px]">Cancel</Button>
              <Button size="sm" onClick={handleCreate} disabled={!newName.trim() || creating} className="h-8 text-[12px]">
                {creating ? <Loader2 className="h-3 w-3 animate-spin mr-1.5" /> : <FolderPlus className="h-3 w-3 mr-1.5" />}
                Create
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}


// ─── Slide Chip (compact, draggable) ────────────────────────

function SlideChip({
  slide,
  selected,
  onSelect,
  groupColor,
  compact,
}: {
  slide: StudySlide
  selected: boolean
  onSelect: (hash: string, checked: boolean) => void
  groupColor?: string
  compact?: boolean
}) {
  const stainColor = slide.stain_type?.startsWith('IHC')
    ? 'text-violet-700 bg-violet-50 border-violet-200'
    : slide.stain_type === 'HE'
      ? 'text-blue-700 bg-blue-50 border-blue-200'
      : 'text-gray-600 bg-gray-50 border-gray-200'

  if (compact) {
    return (
      <div
        className={`inline-flex items-center gap-1.5 px-2 py-1 rounded border text-[11px] cursor-pointer transition-all duration-100 select-none ${
          selected ? 'border-primary bg-primary/5 ring-1 ring-primary/20' : 'border-border hover:border-primary/30'
        }`}
        onClick={() => onSelect(slide.slide_hash, !selected)}
        style={groupColor ? { borderLeftColor: groupColor, borderLeftWidth: 3 } : undefined}
      >
        <span className="font-mono text-foreground">
          {slide.accession_number ? `${slide.accession_number}` : slide.slide_hash.slice(0, 8)}
        </span>
        {slide.block_id && <span className="text-muted-foreground">{slide.block_id}</span>}
        <span className={`px-1 rounded-sm text-[10px] border ${stainColor}`}>{slide.stain_type || '?'}</span>
      </div>
    )
  }

  return (
    <div
      className={`flex items-center gap-2 px-3 py-2 rounded-md border cursor-pointer transition-all duration-100 select-none ${
        selected ? 'border-primary bg-primary/5 ring-1 ring-primary/20' : 'border-border hover:border-primary/30 hover:bg-muted/30'
      }`}
      onClick={() => onSelect(slide.slide_hash, !selected)}
      style={groupColor ? { borderLeftColor: groupColor, borderLeftWidth: 3 } : undefined}
    >
      <Checkbox checked={selected} className="h-3.5 w-3.5 pointer-events-none" />
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-1.5">
          <span className="text-[12px] font-mono font-medium text-foreground truncate">
            {slide.accession_number || slide.slide_hash.slice(0, 12)}
          </span>
          {slide.block_id && (
            <span className="text-[11px] text-muted-foreground">{slide.block_id}</span>
          )}
          {slide.slide_number && (
            <span className="text-[11px] text-muted-foreground">#{slide.slide_number}</span>
          )}
        </div>
      </div>
      <span className={`shrink-0 px-1.5 py-0.5 rounded-sm text-[10px] font-medium border ${stainColor}`}>
        {slide.stain_type || '—'}
      </span>
      <span className="text-[10px] text-muted-foreground tabular-nums shrink-0">{formatBytes(slide.file_size_bytes)}</span>
    </div>
  )
}


// ─── Group Card (board column style) ────────────────────────

function GroupCard({
  group,
  slides,
  selectedHashes,
  onSelectSlide,
  onDeleteGroup,
  onEditGroup,
  onRemoveSlides,
  allStudySlides,
  viewMode,
}: {
  group: StudyGroup
  slides: StudySlide[]
  selectedHashes: Set<string>
  onSelectSlide: (hash: string, checked: boolean) => void
  onDeleteGroup: () => void
  onEditGroup: () => void
  onRemoveSlides: (hashes: string[]) => void
  allStudySlides: StudySlide[]
  viewMode: 'chips' | 'rows'
}) {
  const [collapsed, setCollapsed] = useState(false)
  const groupSlides = slides.filter(s => group.slide_hashes.includes(s.slide_hash))
  const selectedInGroup = groupSlides.filter(s => selectedHashes.has(s.slide_hash))
  const allSelected = groupSlides.length > 0 && selectedInGroup.length === groupSlides.length

  const handleSelectAll = () => {
    const newState = !allSelected
    groupSlides.forEach(s => onSelectSlide(s.slide_hash, newState))
  }

  // Stain type summary
  const stainCounts = useMemo(() => {
    const counts: Record<string, number> = {}
    groupSlides.forEach(s => {
      const st = s.stain_type || 'Unknown'
      counts[st] = (counts[st] || 0) + 1
    })
    return counts
  }, [groupSlides])

  return (
    <div className="border border-border rounded-lg bg-background overflow-hidden">
      {/* Group header */}
      <div
        className="flex items-center gap-2 px-3 py-2.5 border-b border-border/60 cursor-pointer select-none hover:bg-muted/20 transition-colors"
        onClick={() => setCollapsed(!collapsed)}
        style={{ borderLeftWidth: 4, borderLeftColor: group.color || '#94a3b8' }}
      >
        <ChevronRight className={`h-3.5 w-3.5 text-muted-foreground shrink-0 transition-transform duration-150 ${collapsed ? '' : 'rotate-90'}`} />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            {group.label && (
              <span
                className="text-[10px] font-bold px-1.5 py-0.5 rounded-sm text-white"
                style={{ backgroundColor: group.color || '#94a3b8' }}
              >
                {group.label}
              </span>
            )}
            <span className="text-[13px] font-medium text-foreground truncate">{group.name}</span>
            <span className="text-[11px] text-muted-foreground tabular-nums">{groupSlides.length}</span>
          </div>
        </div>
        <div className="flex items-center gap-1 shrink-0">
          <button
            className="p-1 text-muted-foreground hover:text-foreground rounded transition-colors"
            onClick={e => { e.stopPropagation(); handleSelectAll() }}
            title={allSelected ? 'Deselect all' : 'Select all'}
          >
            <Check className={`h-3 w-3 ${allSelected ? 'text-primary' : ''}`} />
          </button>
          <button
            className="p-1 text-muted-foreground hover:text-foreground rounded transition-colors"
            onClick={e => { e.stopPropagation(); onEditGroup() }}
          >
            <Edit2 className="h-3 w-3" />
          </button>
          <button
            className="p-1 text-muted-foreground hover:text-red-500 rounded transition-colors"
            onClick={e => { e.stopPropagation(); onDeleteGroup() }}
          >
            <Trash2 className="h-3 w-3" />
          </button>
        </div>
      </div>

      {/* Stain summary bar */}
      {!collapsed && groupSlides.length > 0 && (
        <div className="flex items-center gap-2 px-3 py-1.5 bg-muted/20 border-b border-border/40">
          {Object.entries(stainCounts).map(([stain, count]) => (
            <span key={stain} className="text-[10px] text-muted-foreground">
              <span className="font-medium">{count}</span> {stain}
            </span>
          ))}
        </div>
      )}

      {/* Slides */}
      {!collapsed && (
        <div className={`p-2 ${viewMode === 'chips' ? 'flex flex-wrap gap-1.5' : 'space-y-1'}`}>
          {groupSlides.length === 0 ? (
            <p className="text-[11px] text-muted-foreground/60 italic px-1 py-3 text-center w-full">
              No slides — drag or add from selection
            </p>
          ) : (
            groupSlides.map(slide => (
              <SlideChip
                key={slide.slide_hash}
                slide={slide}
                selected={selectedHashes.has(slide.slide_hash)}
                onSelect={onSelectSlide}
                groupColor={group.color || undefined}
                compact={viewMode === 'chips'}
              />
            ))
          )}
        </div>
      )}

      {/* Group note */}
      {!collapsed && group.note && (
        <div className="px-3 py-1.5 border-t border-border/40 bg-muted/10">
          <p className="text-[11px] text-muted-foreground italic">{group.note}</p>
        </div>
      )}
    </div>
  )
}


// ─── Study Detail View ──────────────────────────────────────

function StudyDetailView({ studyId, onBack }: { studyId: number; onBack: () => void }) {
  const [study, setStudy] = useState<StudyDetail | null>(null)
  const [loading, setLoading] = useState(true)
  const [selectedHashes, setSelectedHashes] = useState<Set<string>>(new Set())
  const [viewMode, setViewMode] = useState<'chips' | 'rows'>('chips')
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState<Slide[]>([])
  const [searching, setSearching] = useState(false)
  const [showAddSlides, setShowAddSlides] = useState(false)
  const [showCreateGroup, setShowCreateGroup] = useState(false)
  const [showEditGroup, setShowEditGroup] = useState<StudyGroup | null>(null)
  const [stainFilter, setStainFilter] = useState<string>('')
  const [groupFilter, setGroupFilter] = useState<string>('all') // 'all' | 'ungrouped' | group_id
  const [addingSlides, setAddingSlides] = useState(false)

  // Group form
  const [gName, setGName] = useState('')
  const [gLabel, setGLabel] = useState('')
  const [gColor, setGColor] = useState(GROUP_COLORS[0])
  const [gNote, setGNote] = useState('')
  const [gParent, setGParent] = useState<number | null>(null)

  const searchRef = useRef<ReturnType<typeof setTimeout>>()

  const fetchStudy = useCallback(async () => {
    try {
      const res = await fetch(`${getApiBase()}/studies/${studyId}`)
      if (res.ok) setStudy(await res.json())
    } catch {} finally { setLoading(false) }
  }, [studyId])

  useEffect(() => { fetchStudy() }, [fetchStudy])

  // Derived data
  const allGroupedHashes = useMemo(() => {
    if (!study) return new Set<string>()
    const set = new Set<string>()
    study.groups.forEach(g => g.slide_hashes.forEach(h => set.add(h)))
    return set
  }, [study])

  const ungroupedSlides = useMemo(() => {
    if (!study) return []
    return study.slides.filter(s => !allGroupedHashes.has(s.slide_hash))
  }, [study, allGroupedHashes])

  const stainTypes = useMemo(() => {
    if (!study) return []
    const set = new Set<string>()
    study.slides.forEach(s => { if (s.stain_type) set.add(s.stain_type) })
    return Array.from(set).sort()
  }, [study])

  // Filtered slides
  const filteredSlides = useMemo(() => {
    if (!study) return []
    let slides = study.slides
    if (stainFilter) slides = slides.filter(s => s.stain_type === stainFilter)
    return slides
  }, [study, stainFilter])

  const handleSelectSlide = useCallback((hash: string, checked: boolean) => {
    setSelectedHashes(prev => {
      const next = new Set(prev)
      if (checked) next.add(hash)
      else next.delete(hash)
      return next
    })
  }, [])

  const handleSelectAll = useCallback(() => {
    if (!study) return
    const visible = filteredSlides
    if (selectedHashes.size === visible.length) {
      setSelectedHashes(new Set())
    } else {
      setSelectedHashes(new Set(visible.map(s => s.slide_hash)))
    }
  }, [study, filteredSlides, selectedHashes])

  // Search for slides to add
  const handleSearchSlides = useCallback(async (query: string) => {
    setSearchQuery(query)
    if (searchRef.current) clearTimeout(searchRef.current)
    if (!query.trim()) { setSearchResults([]); return }
    searchRef.current = setTimeout(async () => {
      setSearching(true)
      try {
        const res = await fetch(`${getApiBase()}/search?q=${encodeURIComponent(query)}&limit=50`)
        if (res.ok) {
          const data = await res.json()
          setSearchResults(data.results || data)
        }
      } catch {} finally { setSearching(false) }
    }, 300)
  }, [])

  // Add searched slides to study
  const handleAddSlides = async (hashes: string[]) => {
    if (!hashes.length) return
    setAddingSlides(true)
    try {
      await fetch(`${getApiBase()}/studies/${studyId}/slides`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: hashes }),
      })
      fetchStudy()
      setShowAddSlides(false)
      setSearchQuery('')
      setSearchResults([])
    } catch {} finally { setAddingSlides(false) }
  }

  // Remove selected slides
  const handleRemoveSlides = async (hashes?: string[]) => {
    const toRemove = hashes || Array.from(selectedHashes)
    if (!toRemove.length) return
    try {
      await fetch(`${getApiBase()}/studies/${studyId}/slides`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: toRemove }),
      })
      setSelectedHashes(new Set())
      fetchStudy()
    } catch {}
  }

  // Create group
  const handleCreateGroup = async () => {
    if (!gName.trim()) return
    try {
      await fetch(`${getApiBase()}/studies/${studyId}/groups`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: gName,
          label: gLabel || undefined,
          color: gColor,
          note: gNote || undefined,
          parent_id: gParent,
        }),
      })
      setShowCreateGroup(false)
      resetGroupForm()
      fetchStudy()
    } catch {}
  }

  // Update group
  const handleUpdateGroup = async () => {
    if (!showEditGroup || !gName.trim()) return
    try {
      await fetch(`${getApiBase()}/studies/${studyId}/groups/${showEditGroup.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: gName,
          label: gLabel || null,
          color: gColor,
          note: gNote || null,
          parent_id: gParent,
        }),
      })
      setShowEditGroup(null)
      resetGroupForm()
      fetchStudy()
    } catch {}
  }

  // Delete group
  const handleDeleteGroup = async (groupId: number) => {
    try {
      await fetch(`${getApiBase()}/studies/${studyId}/groups/${groupId}`, { method: 'DELETE' })
      fetchStudy()
    } catch {}
  }

  // Add selected slides to a group
  const handleAddSlidesToGroup = async (groupId: number) => {
    const hashes = Array.from(selectedHashes)
    if (!hashes.length) return
    try {
      await fetch(`${getApiBase()}/studies/${studyId}/groups/${groupId}/slides`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: hashes }),
      })
      setSelectedHashes(new Set())
      fetchStudy()
    } catch {}
  }

  // Remove slides from a group
  const handleRemoveSlidesFromGroup = async (groupId: number, hashes: string[]) => {
    try {
      await fetch(`${getApiBase()}/studies/${studyId}/groups/${groupId}/slides`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: hashes }),
      })
      fetchStudy()
    } catch {}
  }

  const resetGroupForm = () => {
    setGName(''); setGLabel(''); setGColor(GROUP_COLORS[0]); setGNote(''); setGParent(null)
  }

  const openEditGroup = (g: StudyGroup) => {
    setGName(g.name)
    setGLabel(g.label || '')
    setGColor(g.color || GROUP_COLORS[0])
    setGNote(g.note || '')
    setGParent(g.parent_id || null)
    setShowEditGroup(g)
  }

  const openCreateGroup = () => {
    resetGroupForm()
    // Assign next color
    const usedColors = study?.groups.map(g => g.color) || []
    const nextColor = GROUP_COLORS.find(c => !usedColors.includes(c)) || GROUP_COLORS[0]
    setGColor(nextColor)
    setShowCreateGroup(true)
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (!study) {
    return (
      <div className="text-center py-12">
        <p className="text-[13px] text-muted-foreground">Study not found</p>
        <Button variant="outline" size="sm" onClick={onBack} className="mt-3 h-8 text-[12px]">
          <ArrowLeft className="h-3 w-3 mr-1.5" />Back
        </Button>
      </div>
    )
  }

  // Top-level groups (no parent_id)
  const topLevelGroups = study.groups.filter(g => !g.parent_id)

  return (
    <div className="space-y-0">
      {/* Study header */}
      <div className="flex items-center gap-3 mb-5">
        <Button variant="ghost" size="sm" onClick={onBack} className="h-8 px-2 text-muted-foreground hover:text-foreground">
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex-1 min-w-0">
          <h2 className="text-lg font-semibold text-foreground truncate">{study.name}</h2>
          <p className="text-[11px] text-muted-foreground font-mono">
            {study.folder_path || `slides/studies/${study.folder_name}`}
          </p>
        </div>
        <div className="flex items-center gap-3 text-[12px] text-muted-foreground shrink-0">
          <span className="tabular-nums">{study.slides.length} slides</span>
          <span className="tabular-nums">{study.groups.length} groups</span>
        </div>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-2 mb-4 flex-wrap">
        <Button size="sm" onClick={() => setShowAddSlides(true)} className="h-7 text-[11px] px-2.5">
          <Plus className="h-3 w-3 mr-1" />Add Slides
        </Button>
        <Button size="sm" variant="outline" onClick={openCreateGroup} className="h-7 text-[11px] px-2.5">
          <FolderPlus className="h-3 w-3 mr-1" />New Group
        </Button>

        <div className="h-4 w-px bg-border mx-1" />

        {/* Stain filter */}
        {stainTypes.length > 0 && (
          <div className="flex items-center gap-1">
            <Filter className="h-3 w-3 text-muted-foreground" />
            <select
              value={stainFilter}
              onChange={e => setStainFilter(e.target.value)}
              className="text-[11px] h-7 px-2 rounded border border-border bg-background text-foreground"
            >
              <option value="">All stains</option>
              {stainTypes.map(st => <option key={st} value={st}>{st}</option>)}
            </select>
          </div>
        )}

        {/* View mode */}
        <div className="flex items-center border border-border rounded overflow-hidden ml-auto">
          <button
            className={`p-1.5 ${viewMode === 'chips' ? 'bg-muted text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
            onClick={() => setViewMode('chips')}
            title="Compact chips"
          >
            <LayoutGrid className="h-3.5 w-3.5" />
          </button>
          <button
            className={`p-1.5 ${viewMode === 'rows' ? 'bg-muted text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
            onClick={() => setViewMode('rows')}
            title="List rows"
          >
            <Rows3 className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      {/* Selection actions bar */}
      {selectedHashes.size > 0 && (
        <div className="flex items-center gap-2 mb-4 px-3 py-2 bg-primary/5 border border-primary/20 rounded-md">
          <span className="text-[12px] font-medium text-foreground tabular-nums">{selectedHashes.size} selected</span>
          <div className="h-3 w-px bg-border mx-1" />

          {/* Assign to group dropdown */}
          <div className="flex items-center gap-1">
            <span className="text-[11px] text-muted-foreground">Move to:</span>
            {study.groups.map(g => (
              <button
                key={g.id}
                className="text-[10px] font-bold px-2 py-1 rounded text-white hover:opacity-80 transition-opacity"
                style={{ backgroundColor: g.color || '#94a3b8' }}
                onClick={() => handleAddSlidesToGroup(g.id)}
                title={`Add to ${g.name}`}
              >
                {g.label || g.name.slice(0, 6)}
              </button>
            ))}
          </div>

          <div className="ml-auto flex items-center gap-1">
            <Button variant="ghost" size="sm" onClick={() => handleRemoveSlides()} className="h-6 text-[11px] text-red-600 hover:text-red-700 hover:bg-red-50 px-2">
              <Trash2 className="h-3 w-3 mr-1" />Remove
            </Button>
            <Button variant="ghost" size="sm" onClick={() => setSelectedHashes(new Set())} className="h-6 text-[11px] px-2">
              <X className="h-3 w-3 mr-1" />Clear
            </Button>
          </div>
        </div>
      )}

      {/* Main content: Groups + Ungrouped */}
      <div className="space-y-3">
        {/* Groups */}
        {topLevelGroups.map(group => (
          <GroupCard
            key={group.id}
            group={group}
            slides={filteredSlides}
            selectedHashes={selectedHashes}
            onSelectSlide={handleSelectSlide}
            onDeleteGroup={() => handleDeleteGroup(group.id)}
            onEditGroup={() => openEditGroup(group)}
            onRemoveSlides={(hashes) => handleRemoveSlidesFromGroup(group.id, hashes)}
            allStudySlides={study.slides}
            viewMode={viewMode}
          />
        ))}

        {/* Ungrouped slides */}
        {ungroupedSlides.length > 0 && (
          <div className="border border-dashed border-border rounded-lg bg-muted/10 overflow-hidden">
            <div className="flex items-center gap-2 px-3 py-2.5 border-b border-border/40">
              <span className="text-[13px] font-medium text-muted-foreground">Ungrouped</span>
              <span className="text-[11px] text-muted-foreground tabular-nums">{ungroupedSlides.filter(s => !stainFilter || s.stain_type === stainFilter).length}</span>
              <div className="ml-auto">
                <button
                  className="p-1 text-muted-foreground hover:text-foreground rounded transition-colors"
                  onClick={handleSelectAll}
                  title="Select all"
                >
                  <Check className="h-3 w-3" />
                </button>
              </div>
            </div>
            <div className={`p-2 ${viewMode === 'chips' ? 'flex flex-wrap gap-1.5' : 'space-y-1'}`}>
              {ungroupedSlides
                .filter(s => !stainFilter || s.stain_type === stainFilter)
                .map(slide => (
                  <SlideChip
                    key={slide.slide_hash}
                    slide={slide}
                    selected={selectedHashes.has(slide.slide_hash)}
                    onSelect={handleSelectSlide}
                    compact={viewMode === 'chips'}
                  />
                ))
              }
            </div>
          </div>
        )}

        {/* Empty state */}
        {study.slides.length === 0 && (
          <div className="flex flex-col items-center justify-center py-16 text-center">
            <Microscope className="h-10 w-10 text-muted-foreground/30 mb-3" />
            <p className="text-[13px] text-muted-foreground">No slides in this study yet</p>
            <p className="text-[12px] text-muted-foreground/60 mt-1 mb-4">
              Add clinical slides from the library or import study-specific files
            </p>
            <Button size="sm" onClick={() => setShowAddSlides(true)} className="h-8 text-[12px]">
              <Plus className="h-3 w-3 mr-1.5" />Add Slides
            </Button>
          </div>
        )}
      </div>

      {/* ── Add Slides Dialog ── */}
      <Dialog open={showAddSlides} onOpenChange={setShowAddSlides}>
        <DialogContent className="sm:max-w-lg max-h-[80vh] flex flex-col">
          <DialogHeader>
            <DialogTitle className="text-[15px]">Add Slides to Study</DialogTitle>
            <DialogDescription className="text-[12px]">
              Search for clinical slides by accession number or paste hashes
            </DialogDescription>
          </DialogHeader>

          <div className="mt-2 space-y-3 flex-1 overflow-hidden flex flex-col">
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                value={searchQuery}
                onChange={e => handleSearchSlides(e.target.value)}
                placeholder="Search by accession (e.g. S24-1234)"
                className="h-8 text-[13px] pl-8"
                autoFocus
              />
            </div>

            <div className="flex-1 overflow-y-auto space-y-1 min-h-0">
              {searching && (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                </div>
              )}
              {!searching && searchResults.length === 0 && searchQuery && (
                <p className="text-[12px] text-muted-foreground text-center py-8">No results</p>
              )}
              {!searching && searchResults.map(slide => {
                const alreadyAdded = study?.slides.some(s => s.slide_hash === slide.slide_hash)
                return (
                  <div
                    key={slide.slide_hash}
                    className={`flex items-center gap-2 px-3 py-2 rounded-md border transition-colors ${
                      alreadyAdded ? 'border-border bg-muted/30 opacity-60' : 'border-border hover:border-primary/30 cursor-pointer'
                    }`}
                    onClick={() => !alreadyAdded && handleAddSlides([slide.slide_hash])}
                  >
                    <div className="flex-1 min-w-0">
                      <span className="text-[12px] font-mono font-medium">{slide.accession_number}</span>
                      {slide.block_id && <span className="text-[11px] text-muted-foreground ml-1.5">{slide.block_id}</span>}
                    </div>
                    <Badge variant="outline" className="text-[10px] h-5">{slide.stain_type}</Badge>
                    <span className="text-[10px] text-muted-foreground tabular-nums">{formatBytes(slide.file_size_bytes)}</span>
                    {alreadyAdded ? (
                      <Check className="h-3.5 w-3.5 text-emerald-500 shrink-0" />
                    ) : (
                      <Plus className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                    )}
                  </div>
                )
              })}
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* ── Create / Edit Group Dialog ── */}
      <Dialog
        open={showCreateGroup || !!showEditGroup}
        onOpenChange={open => { if (!open) { setShowCreateGroup(false); setShowEditGroup(null); resetGroupForm() } }}
      >
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle className="text-[15px]">{showEditGroup ? 'Edit Group' : 'Create Group'}</DialogTitle>
            <DialogDescription className="text-[12px]">
              Groups organize slides within a study (patients, cohorts, conditions)
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 mt-2">
            <div>
              <label className="text-[12px] font-medium text-foreground mb-1 block">Name</label>
              <Input value={gName} onChange={e => setGName(e.target.value)} placeholder="e.g. Cohort A, Patient 001" className="h-8 text-[13px]" />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-[12px] font-medium text-foreground mb-1 block">Label</label>
                <Input value={gLabel} onChange={e => setGLabel(e.target.value)} placeholder="P001" className="h-8 text-[13px]" />
              </div>
              <div>
                <label className="text-[12px] font-medium text-foreground mb-1 block">Color</label>
                <div className="flex items-center gap-1 flex-wrap">
                  {GROUP_COLORS.map(c => (
                    <button
                      key={c}
                      className={`h-6 w-6 rounded-sm border-2 transition-all ${gColor === c ? 'border-foreground scale-110' : 'border-transparent hover:border-muted-foreground/30'}`}
                      style={{ backgroundColor: c }}
                      onClick={() => setGColor(c)}
                    />
                  ))}
                </div>
              </div>
            </div>
            {/* Parent group (for nesting) */}
            {study && study.groups.length > 0 && (
              <div>
                <label className="text-[12px] font-medium text-foreground mb-1 block">Parent Group (optional)</label>
                <select
                  value={gParent || ''}
                  onChange={e => setGParent(e.target.value ? Number(e.target.value) : null)}
                  className="text-[12px] h-8 w-full px-2 rounded border border-border bg-background text-foreground"
                >
                  <option value="">None (top-level)</option>
                  {study.groups
                    .filter(g => g.id !== showEditGroup?.id)
                    .map(g => (
                      <option key={g.id} value={g.id}>{g.label ? `${g.label} — ` : ''}{g.name}</option>
                    ))
                  }
                </select>
              </div>
            )}
            <div>
              <label className="text-[12px] font-medium text-foreground mb-1 block">Note</label>
              <Input value={gNote} onChange={e => setGNote(e.target.value)} placeholder="Optional note" className="h-8 text-[13px]" />
            </div>
            <div className="flex justify-end gap-2 pt-1">
              <Button variant="outline" size="sm" onClick={() => { setShowCreateGroup(false); setShowEditGroup(null); resetGroupForm() }} className="h-8 text-[12px]">
                Cancel
              </Button>
              <Button size="sm" onClick={showEditGroup ? handleUpdateGroup : handleCreateGroup} disabled={!gName.trim()} className="h-8 text-[12px]">
                {showEditGroup ? 'Save' : 'Create'}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}


// ─── Main Export ─────────────────────────────────────────────

export function StudyManager() {
  const [selectedStudyId, setSelectedStudyId] = useState<number | null>(null)

  if (selectedStudyId) {
    return <StudyDetailView studyId={selectedStudyId} onBack={() => setSelectedStudyId(null)} />
  }

  return <StudyListView onSelect={setSelectedStudyId} />
}
