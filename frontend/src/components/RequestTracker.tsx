import { useState, useEffect, useCallback, useRef } from 'react'
import {
  Plus,
  Trash2,
  Download,
  Upload,
  ArrowLeft,
  Search,
  ClipboardList,
  Pencil,
  Check,
  X,
  ChevronRight,
  ChevronDown,
  Circle,
  ListPlus,
  Copy,
  ClipboardCheck,
  AlertTriangle,
  Microscope,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
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
  DialogFooter,
} from '@/components/ui/dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { SortableHeader } from '@/components/SortableHeader'
import { useSortable } from '@/hooks/useSortable'
import { getApiBase, normalizeAccession } from '@/api'
import type { RequestSheet, RequestRow, RequestSheetDetail, Cohort } from '@/types/slide'

// ── Status options ──────────────────────────────────────────────
const CASE_STATUSES = [
  'Not Started',
  'Slides Requested',
  'Partial',
  'Slides Received',
  'Missing',
  'No Blocks/Slides',
  'Recut Blocks Requested',
  'Scanned',
] as const

const SCAN_STATUSES = ['', 'Yes', 'No', 'Partial'] as const

const RECUT_STATUSES = [
  '',
  'Not Needed',
  'Blocks Requested',
  'In Progress',
  'Complete',
] as const

// ── Status badge colors ─────────────────────────────────────────
function statusColor(status: string): string {
  switch (status) {
    case 'Scanned':
      return 'bg-violet-100 text-violet-800 border-violet-200'
    case 'Complete':
    case 'Slides Received':
      return 'bg-emerald-100 text-emerald-800 border-emerald-200'
    case 'Partial':
      return 'bg-amber-100 text-amber-800 border-amber-200'
    case 'Slides Requested':
    case 'Recut Blocks Requested':
      return 'bg-blue-100 text-blue-800 border-blue-200'
    case 'Missing':
    case 'No Blocks/Slides':
      return 'bg-red-100 text-red-800 border-red-200'
    case 'Not Started':
      return 'bg-gray-100 text-gray-600 border-gray-200'
    default:
      return 'bg-gray-100 text-gray-600 border-gray-200'
  }
}

function statusDot(status: string): string {
  switch (status) {
    case 'Scanned':
      return 'text-violet-500'
    case 'Complete':
    case 'Slides Received':
      return 'text-emerald-500'
    case 'Partial':
      return 'text-amber-500'
    case 'Slides Requested':
    case 'Recut Blocks Requested':
      return 'text-blue-500'
    case 'Missing':
    case 'No Blocks/Slides':
      return 'text-red-500'
    default:
      return 'text-gray-400'
  }
}

function scanColor(val: string): string {
  if (val === 'Yes') return 'bg-emerald-100 text-emerald-800 border-emerald-200'
  if (val === 'No') return 'bg-red-100 text-red-800 border-red-200'
  if (val === 'Partial') return 'bg-amber-100 text-amber-800 border-amber-200'
  return 'bg-gray-100 text-gray-600 border-gray-200'
}

// ── Section definitions for detail panel ─────────────────────────
interface FieldDef {
  key: keyof RequestRow
  label: string
  type: 'text' | 'number' | 'select' | 'boolean' | 'blocks' | 'status' | 'scan' | 'computed_blocks_diff'
  options?: readonly string[]
  span?: 2  // span full width
}

interface DetailSection {
  id: string
  label: string
  accentColor: string
  fields: FieldDef[]
}

const DETAIL_SECTIONS: DetailSection[] = [
  {
    id: 'requests',
    label: 'Requests',
    accentColor: 'border-blue-400',
    fields: [
      { key: 'all_blocks', label: 'All Blocks', type: 'blocks', span: 2 },
      { key: 'blocks_available', label: 'Blocks Available', type: 'blocks', span: 2 },
      { key: 'order_id', label: 'Order ID', type: 'text' },
      { key: 'is_consult', label: 'Consult?', type: 'boolean' },
      { key: 'blocks_hes_requested', label: 'Blocks H&Es Requested', type: 'blocks', span: 2 },
      { key: 'hes_requested', label: 'H&Es Requested', type: 'number' },
      { key: 'non_hes_requested', label: 'Non-H&Es Requested', type: 'number' },
      { key: 'ihc_stains_requested', label: 'IHC Stains Requested', type: 'text', span: 2 },
    ],
  },
  {
    id: 'receipts',
    label: 'Receipts',
    accentColor: 'border-emerald-400',
    fields: [
      { key: 'block_hes_received', label: 'Block H&Es Received', type: 'blocks', span: 2 },
      { key: 'hes_received', label: 'H&Es Received', type: 'number' },
      { key: 'non_hes_received', label: 'Non-H&Es Received', type: 'number' },
      { key: 'unaccounted_blocks', label: 'Unaccounted Blocks', type: 'computed_blocks_diff', span: 2 },
      { key: 'fs_received', label: 'FS Received', type: 'number' },
      { key: 'uss_received', label: 'USS Received', type: 'number' },
      { key: 'ihc_received', label: 'IHC Received', type: 'number' },
      { key: 'ihc_stains_received', label: 'IHC Stains Received', type: 'text' },
    ],
  },
  {
    id: 'recuts',
    label: 'Recuts',
    accentColor: 'border-amber-400',
    fields: [
      { key: 'recut_blocks', label: 'Recut Blocks', type: 'blocks', span: 2 },
      { key: 'recut_status', label: 'Recut Status', type: 'select', options: RECUT_STATUSES },
    ],
  },
  {
    id: 'scanning',
    label: 'Scanning',
    accentColor: 'border-violet-400',
    fields: [
      { key: 'hes_scanned', label: 'Blocks H&Es Scanned', type: 'blocks', span: 2 },
      { key: 'he_scanning_status', label: 'H&E Scan Status', type: 'select', options: ['Complete', 'Partial', 'Not Scanned'] as const },
      { key: 'non_hes_scanned', label: 'Blocks Non-H&Es Scanned', type: 'blocks', span: 2 },
    ],
  },
  {
    id: 'other',
    label: 'Other',
    accentColor: 'border-gray-400',
    fields: [
      { key: 'notes', label: 'Notes', type: 'text', span: 2 },
    ],
  },
]

// ── Main Component ──────────────────────────────────────────────
export function RequestTracker() {
  const [view, setView] = useState<'list' | 'sheet'>('list')
  const [activeSheetId, setActiveSheetId] = useState<number | null>(null)

  if (view === 'sheet' && activeSheetId !== null) {
    return (
      <SheetView
        sheetId={activeSheetId}
        onBack={() => { setView('list'); setActiveSheetId(null) }}
      />
    )
  }

  return (
    <SheetList onOpenSheet={(id) => { setActiveSheetId(id); setView('sheet') }} />
  )
}

// ── Sheet List ──────────────────────────────────────────────────
function SheetList({ onOpenSheet }: { onOpenSheet: (id: number) => void }) {
  const [sheets, setSheets] = useState<RequestSheet[]>([])
  const [loading, setLoading] = useState(true)
  const [isCreateOpen, setIsCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [creating, setCreating] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<RequestSheet | null>(null)
  const { sorted, sortConfig, handleSort } = useSortable(sheets)

  const fetchSheets = useCallback(async () => {
    try {
      const res = await fetch(`${getApiBase()}/request-sheets`)
      if (res.ok) setSheets(await res.json())
    } catch (e) {
      console.error('Failed to fetch sheets:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchSheets() }, [fetchSheets])

  const handleCreate = async () => {
    if (!newName.trim()) return
    setCreating(true)
    try {
      const res = await fetch(`${getApiBase()}/request-sheets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newName, description: newDesc || undefined }),
      })
      if (res.ok) {
        const sheet = await res.json()
        setIsCreateOpen(false)
        setNewName('')
        setNewDesc('')
        onOpenSheet(sheet.id)
      }
    } catch (e) {
      console.error('Failed to create sheet:', e)
    } finally {
      setCreating(false)
    }
  }

  const handleDelete = async () => {
    if (!deleteTarget) return
    try {
      await fetch(`${getApiBase()}/request-sheets/${deleteTarget.id}`, { method: 'DELETE' })
      setSheets(prev => prev.filter(s => s.id !== deleteTarget.id))
    } catch (e) {
      console.error('Failed to delete sheet:', e)
    }
    setDeleteTarget(null)
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold">Request Tracker</h2>
          <p className="text-[13px] text-muted-foreground">Track slide requests, receipts, and scanning status</p>
        </div>
        <Button size="sm" onClick={() => setIsCreateOpen(true)}>
          <Plus className="h-3.5 w-3.5 mr-1.5" />
          New Sheet
        </Button>
      </div>

      {loading ? (
        <div className="text-center py-12 text-muted-foreground text-sm">Loading...</div>
      ) : sheets.length === 0 ? (
        <div className="text-center py-16 space-y-3">
          <ClipboardList className="h-10 w-10 mx-auto text-muted-foreground/40" />
          <div className="text-sm text-muted-foreground">No request sheets yet</div>
          <Button size="sm" variant="outline" onClick={() => setIsCreateOpen(true)}>
            <Plus className="h-3.5 w-3.5 mr-1.5" />
            Create your first sheet
          </Button>
        </div>
      ) : (
        <div className="border rounded-md">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[40%]">
                  <SortableHeader label="Name" sortKey="name" sortConfig={sortConfig} onSort={handleSort} />
                </TableHead>
                <TableHead>
                  <SortableHeader label="Cases" sortKey="case_count" sortConfig={sortConfig} onSort={handleSort} />
                </TableHead>
                <TableHead>
                  <SortableHeader label="Updated" sortKey="updated_at" sortConfig={sortConfig} onSort={handleSort} />
                </TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {sorted.map((sheet) => (
                <TableRow key={sheet.id} className="cursor-pointer hover:bg-muted/50" onClick={() => onOpenSheet(sheet.id)}>
                  <TableCell>
                    <div>
                      <span className="font-medium text-[13px]">{sheet.name}</span>
                      {sheet.description && <p className="text-[12px] text-muted-foreground mt-0.5 line-clamp-1">{sheet.description}</p>}
                    </div>
                  </TableCell>
                  <TableCell className="text-[13px] tabular-nums">{sheet.case_count}</TableCell>
                  <TableCell className="text-[13px] text-muted-foreground">
                    {sheet.updated_at ? new Date(sheet.updated_at).toLocaleDateString() : '—'}
                  </TableCell>
                  <TableCell>
                    <Button size="sm" variant="ghost" className="h-7 w-7 p-0" onClick={(e) => { e.stopPropagation(); setDeleteTarget(sheet) }}>
                      <Trash2 className="h-3.5 w-3.5 text-muted-foreground" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      <Dialog open={isCreateOpen} onOpenChange={setIsCreateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>New Request Sheet</DialogTitle>
            <DialogDescription>Create a tracking sheet for slide requests</DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <Input placeholder="Sheet name" value={newName} onChange={(e) => setNewName(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && handleCreate()} autoFocus />
            <Input placeholder="Description (optional)" value={newDesc} onChange={(e) => setNewDesc(e.target.value)} />
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setIsCreateOpen(false)}>Cancel</Button>
            <Button size="sm" onClick={handleCreate} disabled={!newName.trim() || creating}>{creating ? 'Creating...' : 'Create'}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={!!deleteTarget} onOpenChange={() => setDeleteTarget(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Sheet</DialogTitle>
            <DialogDescription>Delete "{deleteTarget?.name}" and all its tracked cases? This cannot be undone.</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setDeleteTarget(null)}>Cancel</Button>
            <Button variant="destructive" size="sm" onClick={handleDelete}>Delete</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

// ── Block chip field (click-to-edit) ────────────────────────────
function BlockChipField({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState(value)
  const blocks = value ? value.split(';').map(b => b.trim()).filter(Boolean) : []

  const startEditing = () => {
    setDraft(value)
    setEditing(true)
  }

  const commit = () => {
    setEditing(false)
    if (draft !== value) onChange(draft)
  }

  if (editing) {
    return (
      <div className="space-y-1.5">
        {blocks.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {blocks.map((b, i) => (
              <span key={i} className="inline-flex rounded bg-gray-100 border border-gray-300 px-1.5 py-0.5 text-[12px] font-mono text-gray-700">{b}</span>
            ))}
          </div>
        )}
        <Input
          className="h-7 text-[12px] font-mono"
          placeholder="e.g. A1;A2;B1"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={commit}
          onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); commit() } if (e.key === 'Escape') setEditing(false) }}
          autoFocus
        />
      </div>
    )
  }

  if (blocks.length === 0) {
    return (
      <button
        onClick={startEditing}
        className="text-[12px] text-muted-foreground hover:text-foreground transition-colors px-1 py-1"
      >
        + Add blocks
      </button>
    )
  }

  return (
    <button onClick={startEditing} className="flex flex-wrap gap-1 cursor-text group text-left">
      {blocks.map((b, i) => (
        <span key={i} className="inline-flex rounded bg-gray-100 border border-gray-300 px-1.5 py-0.5 text-[12px] font-mono text-gray-700 group-hover:border-gray-400 transition-colors">{b}</span>
      ))}
      <Pencil className="h-3 w-3 text-muted-foreground/0 group-hover:text-muted-foreground/60 self-center ml-0.5 transition-opacity" />
    </button>
  )
}

// ── Copy as comma-separated list ────────────────────────────────
function CopyCommaList({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  const commaSeparated = text
    .split(/[\n,;]+/)
    .map(l => normalizeAccession(l))
    .filter(l => l.length > 0)
    .join(', ')

  const handleCopy = () => {
    navigator.clipboard.writeText(commaSeparated).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  return (
    <Button variant="ghost" size="sm" className="h-6 text-[11px] text-muted-foreground gap-1 px-2" onClick={handleCopy}>
      {copied ? <ClipboardCheck className="h-3 w-3 text-emerald-600" /> : <Copy className="h-3 w-3" />}
      {copied ? 'Copied!' : 'Copy as comma list'}
    </Button>
  )
}

// ── Progress bar helper ─────────────────────────────────────────
function CaseProgressBar({ row }: { row: RequestRow }) {
  const requested = row.hes_requested + row.non_hes_requested
  const received = row.hes_received + row.non_hes_received
  if (requested === 0 && received === 0) return null
  const pct = requested > 0 ? Math.min(100, Math.round((received / requested) * 100)) : (received > 0 ? 100 : 0)
  return (
    <div className="flex items-center gap-1.5 mt-1">
      <div className="flex-1 h-1 bg-gray-200 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all ${pct >= 100 ? 'bg-emerald-500' : pct > 0 ? 'bg-blue-500' : 'bg-gray-300'}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-[10px] tabular-nums text-muted-foreground shrink-0">{received}/{requested}</span>
    </div>
  )
}

// ── Sheet View (card-based split panel) ──────────────────────────
function SheetView({ sheetId, onBack }: { sheetId: number; onBack: () => void }) {
  const [sheet, setSheet] = useState<RequestSheetDetail | null>(null)
  const [rows, setRows] = useState<RequestRow[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState('')
  const [selectedRowId, setSelectedRowId] = useState<number | null>(null)
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const [consultFilter, setConsultFilter] = useState(false)

  // Expanded detail sections
  const [expandedSections, setExpandedSections] = useState<Set<string>>(
    new Set(['requests', 'receipts', 'recuts', 'scanning', 'other'])
  )

  // Dialogs
  const [isAddOpen, setIsAddOpen] = useState(false)
  const [newAccession, setNewAccession] = useState('')
  const [addError, setAddError] = useState('')
  const [isImportOpen, setIsImportOpen] = useState(false)
  const [cohorts, setCohorts] = useState<Cohort[]>([])
  const [selectedCohortId, setSelectedCohortId] = useState<string>('')
  const [importing, setImporting] = useState(false)
  const [importResult, setImportResult] = useState<{ added: number; skipped: number } | null>(null)

  // CSV import
  const [isCsvImportOpen, setIsCsvImportOpen] = useState(false)
  const [csvImportFile, setCsvImportFile] = useState<File | null>(null)
  const [csvImporting, setCsvImporting] = useState(false)
  const [csvImportMode, setCsvImportMode] = useState<'skip' | 'upsert'>('skip')
  const [csvImportResult, setCsvImportResult] = useState<{ added: number; updated: number; skipped: number; errors: string[] } | null>(null)
  const csvFileRef = useRef<HTMLInputElement>(null)

  // Undo stack
  type UndoEntry =
    | { type: 'field'; rowId: number; field: string; prev: unknown }
    | { type: 'add_rows'; ids: number[] }
    | { type: 'delete_row'; snapshot: RequestRow }
  const [undoStack, setUndoStack] = useState<UndoEntry[]>([])
  const [undoToast, setUndoToast] = useState<string | null>(null)
  const pushUndo = (entry: UndoEntry) => setUndoStack(s => [...s.slice(-49), entry])

  const [isEditingName, setIsEditingName] = useState(false)
  const [editName, setEditName] = useState('')
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null)

  // Batch entry (3-step: paste → staging → done)
  const [isBatchOpen, setIsBatchOpen] = useState(false)
  const [batchStep, setBatchStep] = useState<'paste' | 'staging' | 'done'>('paste')
  const [batchText, setBatchText] = useState('')
  const [batchRowIds, setBatchRowIds] = useState<number[]>([])  // IDs of rows created in step 1
  const [batchSubmitting, setBatchSubmitting] = useState(false)
  const [batchAddResult, setBatchAddResult] = useState<{ added: number; skipped: number } | null>(null)
  // Shared fields (filled in staging step)
  const [batchOrderId, setBatchOrderId] = useState('')
  const [batchStatus, setBatchStatus] = useState<string>('Slides Requested')
  const [batchIsConsult, setBatchIsConsult] = useState(false)
  const [batchApplying, setBatchApplying] = useState(false)

  // Case warnings (already scanned, duplicate across sheets)
  interface CaseWarning {
    type: 'already_scanned' | 'duplicate_request'
    message: string
    slide_count?: number
    stain_breakdown?: Record<string, number>
    sheets?: { sheet_id: number; sheet_name: string; case_status: string }[]
  }
  const [caseWarnings, setCaseWarnings] = useState<CaseWarning[]>([])
  const [loadingWarnings, setLoadingWarnings] = useState(false)

  const fetchSheet = useCallback(async () => {
    try {
      const res = await fetch(`${getApiBase()}/request-sheets/${sheetId}`)
      if (res.ok) {
        const data: RequestSheetDetail = await res.json()
        setSheet(data)

        // Auto-fix: any row with he_scanning_status=Complete should have case_status=Scanned
        const needsSync = data.rows.filter(
          r => r.he_scanning_status === 'Complete' && r.case_status !== 'Scanned'
        )
        if (needsSync.length > 0) {
          await Promise.all(needsSync.map(r =>
            fetch(`${getApiBase()}/request-sheets/${sheetId}/rows/${r.id}`, {
              method: 'PATCH',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ case_status: 'Scanned' }),
            }).catch(() => {})
          ))
          needsSync.forEach(r => { r.case_status = 'Scanned' })
        }

        setRows(data.rows)
        if (data.rows.length > 0 && !selectedRowId) {
          setSelectedRowId(data.rows[0].id)
        }
      }
    } catch (e) {
      console.error('Failed to fetch sheet:', e)
    } finally {
      setLoading(false)
    }
  }, [sheetId])

  useEffect(() => { fetchSheet() }, [fetchSheet])

  // Fetch warnings when selected row changes
  useEffect(() => {
    if (!selectedRowId) { setCaseWarnings([]); return }
    const row = rows.find(r => r.id === selectedRowId)
    if (!row) { setCaseWarnings([]); return }
    let cancelled = false
    setLoadingWarnings(true)
    fetch(`${getApiBase()}/request-sheets/case-warnings?accession=${encodeURIComponent(row.accession_number)}&sheet_id=${sheetId}`)
      .then(res => res.ok ? res.json() : { warnings: [] })
      .then(data => { if (!cancelled) setCaseWarnings(data.warnings || []) })
      .catch(() => { if (!cancelled) setCaseWarnings([]) })
      .finally(() => { if (!cancelled) setLoadingWarnings(false) })
    return () => { cancelled = true }
  }, [selectedRowId, rows, sheetId])

  // ── Section toggle ────────────────────────────────────────────
  const toggleSection = (id: string) => {
    setExpandedSections(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  // ── Field update (direct, no click-to-edit) ────────────────────
  const updateField = async (rowId: number, field: string, value: unknown) => {
    const row = rows.find(r => r.id === rowId)
    if (!row) return
    pushUndo({ type: 'field', rowId, field, prev: row[field as keyof RequestRow] })

    const patch: Record<string, unknown> = { [field]: value }
    if (field === 'he_scanning_status' && value === 'Complete') {
      patch.case_status = 'Scanned'
      pushUndo({ type: 'field', rowId, field: 'case_status', prev: row.case_status })
    }

    setRows(prev => prev.map(r => r.id === rowId ? { ...r, ...patch } : r))
    try {
      const res = await fetch(`${getApiBase()}/request-sheets/${sheetId}/rows/${rowId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch),
      })
      if (!res.ok) {
        setRows(prev => prev.map(r => r.id === rowId ? { ...r, [field]: row[field as keyof RequestRow] } : r))
      }
    } catch {
      setRows(prev => prev.map(r => r.id === rowId ? { ...r, [field]: row[field as keyof RequestRow] } : r))
    }
  }

  // ── Add single row ────────────────────────────────────────────
  const handleAddRow = async () => {
    if (!newAccession.trim()) return
    setAddError('')
    try {
      const res = await fetch(`${getApiBase()}/request-sheets/${sheetId}/rows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ accession_number: normalizeAccession(newAccession) }),
      })
      if (res.ok) {
        const row: RequestRow = await res.json()
        setRows(prev => [...prev, row])
        pushUndo({ type: 'add_rows', ids: [row.id] })
        setSelectedRowId(row.id)
        setNewAccession('')
        setIsAddOpen(false)
      } else {
        const err = await res.json()
        setAddError(err.detail || 'Failed to add row')
      }
    } catch {
      setAddError('Failed to add row')
    }
  }

  // ── Batch step 1: add cases ──────────────────────────────────
  const handleBatchAdd = async () => {
    const lines = batchText
      .split(/[\n,;]+/)
      .map(l => l.trim())
      .filter(l => l.length > 0)
    if (lines.length === 0) return
    setBatchSubmitting(true)
    let added = 0
    let skipped = 0
    const newIds: number[] = []

    for (const raw of lines) {
      const accession = normalizeAccession(raw)
      try {
        const res = await fetch(`${getApiBase()}/request-sheets/${sheetId}/rows`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ accession_number: accession }),
        })
        if (res.ok) {
          const row: RequestRow = await res.json()
          setRows(prev => [...prev, row])
          newIds.push(row.id)
          added++
        } else {
          skipped++
        }
      } catch {
        skipped++
      }
    }

    setBatchSubmitting(false)
    setBatchRowIds(newIds)
    setBatchAddResult({ added, skipped })
    if (newIds.length > 0) {
      pushUndo({ type: 'add_rows', ids: newIds })
      setSelectedRowId(newIds[0])
      setBatchStep('staging')
    }
  }

  // ── Batch step 2: apply shared fields ──────────────────────────
  const handleBatchApply = async () => {
    if (batchRowIds.length === 0) return
    setBatchApplying(true)
    const updates: Record<string, unknown> = {}
    if (batchStatus) updates.case_status = batchStatus
    if (batchOrderId.trim()) updates.order_id = batchOrderId.trim()
    if (batchIsConsult) updates.is_consult = true

    for (const rowId of batchRowIds) {
      // Optimistic update
      setRows(prev => prev.map(r => r.id === rowId ? { ...r, ...updates } as RequestRow : r))
      try {
        const res = await fetch(`${getApiBase()}/request-sheets/${sheetId}/rows/${rowId}`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(updates),
        })
        if (res.ok) {
          const updated = await res.json()
          setRows(prev => prev.map(r => r.id === rowId ? updated : r))
        }
      } catch {}
    }

    setBatchApplying(false)
    setBatchStep('done')
  }

  const openBatchEntry = () => {
    setIsBatchOpen(true)
    setBatchStep('paste')
    setBatchText('')
    setBatchRowIds([])
    setBatchAddResult(null)
    setBatchOrderId('')
    setBatchStatus('Slides Requested')
    setBatchIsConsult(false)
  }

  // ── Delete row ────────────────────────────────────────────────
  const handleDeleteRow = async (rowId: number) => {
    const snapshot = rows.find(r => r.id === rowId)
    if (snapshot) pushUndo({ type: 'delete_row', snapshot })
    const wasSelected = selectedRowId === rowId
    setRows(prev => prev.filter(r => r.id !== rowId))
    setDeleteConfirmId(null)
    if (wasSelected) {
      const remaining = rows.filter(r => r.id !== rowId)
      setSelectedRowId(remaining.length > 0 ? remaining[0].id : null)
    }
    try {
      await fetch(`${getApiBase()}/request-sheets/${sheetId}/rows/${rowId}`, { method: 'DELETE' })
    } catch {
      fetchSheet()
    }
  }

  // ── Undo ──────────────────────────────────────────────────────
  const handleUndo = useCallback(async () => {
    if (undoStack.length === 0) return
    const entry = undoStack[undoStack.length - 1]
    setUndoStack(s => s.slice(0, -1))

    if (entry.type === 'field') {
      setRows(prev => prev.map(r => r.id === entry.rowId ? { ...r, [entry.field]: entry.prev } : r))
      await fetch(`${getApiBase()}/request-sheets/${sheetId}/rows/${entry.rowId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [entry.field]: entry.prev }),
      }).catch(() => fetchSheet())
      setUndoToast('Undid field edit')

    } else if (entry.type === 'add_rows') {
      setRows(prev => prev.filter(r => !entry.ids.includes(r.id)))
      await Promise.all(entry.ids.map(id =>
        fetch(`${getApiBase()}/request-sheets/${sheetId}/rows/${id}`, { method: 'DELETE' }).catch(() => {})
      ))
      setUndoToast(`Undid adding ${entry.ids.length} case${entry.ids.length !== 1 ? 's' : ''}`)

    } else if (entry.type === 'delete_row') {
      const { id: _id, sheet_id: _sid, created_at: _ca, updated_at: _ua, ...fields } = entry.snapshot as any
      const res = await fetch(`${getApiBase()}/request-sheets/${sheetId}/rows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fields),
      }).catch(() => null)
      if (res?.ok) {
        const restored: RequestRow = await res.json()
        setRows(prev => [...prev, restored])
      } else {
        fetchSheet()
      }
      setUndoToast('Undid delete')
    }

    setTimeout(() => setUndoToast(null), 2500)
  }, [undoStack, sheetId, fetchSheet])

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'z' && !e.shiftKey) {
        const tag = (e.target as HTMLElement)?.tagName
        if (tag === 'INPUT' || tag === 'TEXTAREA' || (e.target as HTMLElement)?.isContentEditable) return
        e.preventDefault()
        handleUndo()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [handleUndo])

  // ── Import cohort ─────────────────────────────────────────────
  const openImport = async () => {
    setIsImportOpen(true)
    setImportResult(null)
    setSelectedCohortId('')
    try {
      const res = await fetch(`${getApiBase()}/cohorts`)
      if (res.ok) setCohorts(await res.json())
    } catch (e) {
      console.error('Failed to fetch cohorts:', e)
    }
  }

  const handleImport = async () => {
    if (!selectedCohortId) return
    setImporting(true)
    try {
      const res = await fetch(`${getApiBase()}/request-sheets/${sheetId}/import-cohort`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cohort_id: parseInt(selectedCohortId) }),
      })
      if (res.ok) {
        const result = await res.json()
        setImportResult(result)
        fetchSheet()
      }
    } catch (e) {
      console.error('Failed to import:', e)
    } finally {
      setImporting(false)
    }
  }

  const handleExport = () => {
    window.open(`${getApiBase()}/request-sheets/${sheetId}/export.csv`, '_blank')
  }

  // ── CSV import ────────────────────────────────────────────────
  const openCsvImport = () => {
    setCsvImportFile(null)
    setCsvImportResult(null)
    setCsvImportMode('skip')
    setIsCsvImportOpen(true)
  }

  const handleCsvImport = async () => {
    if (!csvImportFile) return
    setCsvImporting(true)
    try {
      const form = new FormData()
      form.append('file', csvImportFile)
      const res = await fetch(`${getApiBase()}/request-sheets/${sheetId}/import-csv?mode=${csvImportMode}`, {
        method: 'POST',
        body: form,
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Import failed')
      setCsvImportResult(data)
      fetchSheet()
    } catch (e: any) {
      setCsvImportResult({ added: 0, updated: 0, skipped: 0, errors: [e.message || 'Upload failed'] })
    } finally {
      setCsvImporting(false)
    }
  }

  const saveSheetName = async () => {
    if (!editName.trim() || !sheet) return
    try {
      await fetch(`${getApiBase()}/request-sheets/${sheetId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: editName }),
      })
      setSheet({ ...sheet, name: editName })
    } catch {}
    setIsEditingName(false)
  }

  // ── Filter & stats ────────────────────────────────────────────
  const filteredRows = rows.filter(r => {
    const matchesText = !filter || r.accession_number.toLowerCase().includes(filter.toLowerCase()) || (r.notes && r.notes.toLowerCase().includes(filter.toLowerCase()))
    const matchesStatus = statusFilter === 'all' || r.case_status === statusFilter
    const matchesConsult = !consultFilter || r.is_consult
    return matchesText && matchesStatus && matchesConsult
  })

  const selectedRow = rows.find(r => r.id === selectedRowId) || null

  const stats = {
    total: rows.length,
    received: rows.filter(r => r.case_status === 'Slides Received' || r.case_status === 'Complete').length,
    partial: rows.filter(r => r.case_status === 'Partial').length,
    requested: rows.filter(r => r.case_status === 'Slides Requested' || r.case_status === 'Recut Blocks Requested').length,
    missing: rows.filter(r => r.case_status === 'Missing' || r.case_status === 'No Blocks/Slides').length,
    scanned: rows.filter(r => r.case_status === 'Scanned').length,
    notStarted: rows.filter(r => r.case_status === 'Not Started').length,
  }

  if (loading) return <div className="text-center py-12 text-muted-foreground text-sm">Loading...</div>
  if (!sheet) return <div className="text-center py-12 text-muted-foreground text-sm">Sheet not found</div>

  // ── Render detail field ───────────────────────────────────────
  const renderField = (row: RequestRow, field: FieldDef) => {
    const value = row[field.key]

    if (field.type === 'status') {
      return (
        <Select value={String(value || 'Not Started')} onValueChange={(v) => updateField(row.id, field.key, v)}>
          <SelectTrigger className="h-8 text-[13px]">
            <span className={`inline-flex items-center rounded-sm px-1.5 py-0.5 text-[12px] font-medium border ${statusColor(String(value || 'Not Started'))}`}>
              {String(value || 'Not Started')}
            </span>
          </SelectTrigger>
          <SelectContent>
            {CASE_STATUSES.map(s => (
              <SelectItem key={s} value={s} className="text-[13px]">
                <span className={`inline-flex items-center rounded-sm px-1.5 py-0.5 text-[12px] font-medium border ${statusColor(s)}`}>{s}</span>
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      )
    }

    if (field.type === 'scan') {
      return (
        <Select value={String(value || '')} onValueChange={(v) => updateField(row.id, field.key, v === '__empty' ? '' : v)}>
          <SelectTrigger className="h-8 text-[13px]">
            {value ? (
              <span className={`inline-flex items-center rounded-sm px-1.5 py-0.5 text-[12px] font-medium border ${scanColor(String(value))}`}>{String(value)}</span>
            ) : (
              <span className="text-muted-foreground">—</span>
            )}
          </SelectTrigger>
          <SelectContent>
            {SCAN_STATUSES.map(s => (
              <SelectItem key={s || '__empty'} value={s || '__empty'} className="text-[13px]">
                {s ? <span className={`inline-flex items-center rounded-sm px-1.5 py-0.5 text-[12px] font-medium border ${scanColor(s)}`}>{s}</span> : <span className="text-muted-foreground">None</span>}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      )
    }

    if (field.type === 'select' && field.options) {
      return (
        <Select value={String(value || '')} onValueChange={(v) => updateField(row.id, field.key, v === '__empty' ? '' : v)}>
          <SelectTrigger className="h-8 text-[13px]">
            <SelectValue placeholder="—" />
          </SelectTrigger>
          <SelectContent>
            {field.options.map(s => (
              <SelectItem key={s || '__empty'} value={s || '__empty'} className="text-[13px]">{s || '—'}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      )
    }

    if (field.type === 'boolean') {
      return (
        <div className="flex items-center gap-2 h-8">
          <Checkbox checked={!!value} onCheckedChange={() => updateField(row.id, field.key, !value)} />
          <span className="text-[13px] text-muted-foreground">{value ? 'Yes' : 'No'}</span>
        </div>
      )
    }

    if (field.type === 'blocks') {
      return <BlockChipField value={String(value || '')} onChange={(v) => updateField(row.id, field.key, v)} />
    }

    if (field.type === 'computed_blocks_diff') {
      const available = (row.blocks_available || '').split(';').map(b => b.trim().toUpperCase()).filter(Boolean)
      const received = (row.block_hes_received || '').split(';').map(b => b.trim().toUpperCase()).filter(Boolean)
      const receivedSet = new Set(received)
      const unaccounted = available.filter(b => !receivedSet.has(b))
      return (
        <div>
          {unaccounted.length > 0 ? (
            <div className="flex flex-wrap gap-1">
              {unaccounted.map((b, i) => (
                <span key={i} className="inline-flex rounded bg-red-50 border border-red-300 px-1.5 py-0.5 text-[12px] font-mono text-red-700">{b}</span>
              ))}
            </div>
          ) : available.length > 0 && received.length > 0 ? (
            <span className="text-[12px] text-emerald-600 font-medium">All accounted for</span>
          ) : (
            <span className="text-[12px] text-muted-foreground">—</span>
          )}
        </div>
      )
    }

    if (field.type === 'number') {
      return (
        <Input
          type="number"
          className="h-8 text-[13px] tabular-nums"
          value={Number(value) || 0}
          onChange={(e) => updateField(row.id, field.key, parseInt(e.target.value) || 0)}
        />
      )
    }

    // text
    return (
      <Input
        className="h-8 text-[13px]"
        placeholder="—"
        value={String(value || '')}
        onChange={(e) => updateField(row.id, field.key, e.target.value)}
      />
    )
  }

  return (
    <div className="flex flex-col h-full" style={{ minHeight: 0 }}>
      {/* Undo toast */}
      {undoToast && (
        <div className="fixed bottom-4 left-1/2 -translate-x-1/2 z-50 bg-gray-800 text-white text-[13px] px-4 py-2 rounded-lg shadow-lg pointer-events-none">
          {undoToast}
        </div>
      )}
      {/* Header */}
      <div className="flex items-center gap-3 mb-3">
        <Button size="sm" variant="ghost" onClick={onBack} className="h-8 px-2">
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex-1 min-w-0">
          {isEditingName ? (
            <div className="flex items-center gap-2">
              <Input value={editName} onChange={(e) => setEditName(e.target.value)} onKeyDown={(e) => { if (e.key === 'Enter') saveSheetName(); if (e.key === 'Escape') setIsEditingName(false) }} className="h-8 text-lg font-semibold w-64" autoFocus />
              <Button size="sm" variant="ghost" className="h-7 w-7 p-0" onClick={saveSheetName}><Check className="h-3.5 w-3.5" /></Button>
              <Button size="sm" variant="ghost" className="h-7 w-7 p-0" onClick={() => setIsEditingName(false)}><X className="h-3.5 w-3.5" /></Button>
            </div>
          ) : (
            <div className="flex items-center gap-2 group">
              <h2 className="text-lg font-semibold truncate">{sheet.name}</h2>
              <button className="opacity-0 group-hover:opacity-60 hover:!opacity-100 transition-opacity" onClick={() => { setEditName(sheet.name); setIsEditingName(true) }}>
                <Pencil className="h-3.5 w-3.5" />
              </button>
            </div>
          )}
        </div>
        <div className="flex items-center gap-1.5">
          <Button size="sm" className="h-7 text-[12px]" onClick={openBatchEntry}>
            <ListPlus className="h-3 w-3 mr-1" />Batch Entry
          </Button>
          <Button size="sm" variant="outline" className="h-7 text-[12px]" onClick={() => setIsAddOpen(true)}>
            <Plus className="h-3 w-3 mr-1" />Case
          </Button>
          <Button size="sm" variant="outline" className="h-7 text-[12px]" onClick={openImport}>
            <Upload className="h-3 w-3 mr-1" />Import
          </Button>
          <Button size="sm" variant="outline" className="h-7 text-[12px]" onClick={openCsvImport}>
            <Upload className="h-3 w-3 mr-1" />Import CSV
          </Button>
          <Button size="sm" variant="outline" className="h-7 text-[12px]" onClick={handleExport}>
            <Download className="h-3 w-3 mr-1" />CSV
          </Button>
        </div>
      </div>

      {/* Summary stats strip */}
      <div className="flex gap-1.5 mb-3 flex-wrap">
        {[
          { n: stats.total, label: 'Total', cls: 'border-gray-300 bg-gray-50/70', active: statusFilter === 'all', filterVal: 'all' },
          { n: stats.notStarted, label: 'Not Started', cls: 'border-gray-300 bg-gray-50/70 text-gray-600', active: statusFilter === 'Not Started', filterVal: 'Not Started' },
          { n: stats.requested, label: 'Requested', cls: 'border-blue-300 bg-blue-50/70 text-blue-700', active: statusFilter === 'Slides Requested', filterVal: 'Slides Requested' },
          { n: stats.partial, label: 'Partial', cls: 'border-amber-300 bg-amber-50/70 text-amber-700', active: statusFilter === 'Partial', filterVal: 'Partial' },
          { n: stats.received, label: 'Received', cls: 'border-emerald-300 bg-emerald-50/70 text-emerald-700', active: statusFilter === 'Slides Received' || statusFilter === 'Complete', filterVal: 'Slides Received' },
          { n: stats.missing, label: 'Missing', cls: 'border-red-300 bg-red-50/70 text-red-700', active: statusFilter === 'Missing', filterVal: 'Missing' },
          { n: stats.scanned, label: 'Scanned', cls: 'border-violet-300 bg-violet-50/70 text-violet-700', active: statusFilter === 'Scanned', filterVal: 'Scanned' },
        ].map(s => (
          <button
            key={s.label}
            onClick={() => {
              if (s.filterVal && s.filterVal !== '') {
                setStatusFilter(prev => prev === s.filterVal ? 'all' : s.filterVal)
              }
            }}
            className={`rounded-md border px-2.5 py-1 text-center min-w-[60px] transition-all ${s.cls} ${s.active ? 'ring-2 ring-primary/30 ring-offset-1' : 'hover:shadow-sm'}`}
          >
            <div className="text-base font-semibold tabular-nums">{s.n}</div>
            <div className="text-[11px] text-muted-foreground leading-tight">{s.label}</div>
          </button>
        ))}
        <button
          onClick={() => setConsultFilter(p => !p)}
          className={`rounded-md border px-2.5 py-1 text-center min-w-[60px] transition-all border-orange-300 bg-orange-50/70 text-orange-700 ${consultFilter ? 'ring-2 ring-primary/30 ring-offset-1' : 'hover:shadow-sm'}`}
        >
          <div className="text-base font-semibold tabular-nums">{rows.filter(r => r.is_consult).length}</div>
          <div className="text-[11px] text-muted-foreground leading-tight">Consult</div>
        </button>
      </div>

      {/* Main content: split panel */}
      {rows.length === 0 ? (
        <div className="text-center py-12 space-y-3 border border-gray-300 rounded-md flex-1">
          <ClipboardList className="h-10 w-10 mx-auto text-muted-foreground/40" />
          <div className="text-sm text-muted-foreground">No cases tracked yet</div>
          <div className="flex gap-2 justify-center">
            <Button size="sm" onClick={openBatchEntry}>
              <ListPlus className="h-3.5 w-3.5 mr-1.5" />Batch Entry
            </Button>
            <Button size="sm" variant="outline" onClick={openImport}>
              <Upload className="h-3.5 w-3.5 mr-1.5" />Import Cohort
            </Button>
          </div>
        </div>
      ) : (
        <div className="flex gap-3 flex-1 min-h-0">
          {/* LEFT: Case card list */}
          <div className="w-[340px] shrink-0 flex flex-col border border-gray-300 rounded-md overflow-hidden bg-background shadow-sm">
            {/* Search in list */}
            <div className="p-2 border-b border-gray-300 bg-muted/30">
              <div className="relative">
                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  placeholder="Filter accessions..."
                  value={filter}
                  onChange={(e) => setFilter(e.target.value)}
                  className="h-7 pl-7 text-[12px]"
                />
              </div>
              {filteredRows.length !== rows.length && (
                <p className="text-[11px] text-muted-foreground mt-1 px-0.5">
                  {filteredRows.length} of {rows.length} cases
                </p>
              )}
            </div>

            {/* Card list */}
            <div className="flex-1 overflow-y-auto">
              {filteredRows.map(row => {
                const isSelected = row.id === selectedRowId
                return (
                  <button
                    key={row.id}
                    onClick={() => setSelectedRowId(row.id)}
                    className={`w-full text-left px-3 py-2.5 border-b border-gray-200 transition-colors ${
                      isSelected
                        ? 'bg-primary/10 border-l-[3px] border-l-primary'
                        : 'hover:bg-muted/40 border-l-[3px] border-l-transparent'
                    }`}
                  >
                    <div className="flex items-start justify-between gap-2 group/acc">
                      <div className="flex items-center gap-1 min-w-0">
                        <span className="font-mono text-[13px] font-semibold tracking-tight text-foreground">
                          {row.accession_number}
                        </span>
                        <button
                          className="opacity-0 group-hover/acc:opacity-60 hover:!opacity-100 transition-opacity shrink-0"
                          onClick={(e) => { e.stopPropagation(); navigator.clipboard.writeText(row.accession_number) }}
                          title="Copy accession number"
                        >
                          <Copy className="h-3 w-3" />
                        </button>
                      </div>
                      <Circle className={`h-2 w-2 shrink-0 mt-1.5 fill-current ${statusDot(row.case_status)}`} />
                    </div>
                    <div className="flex items-center gap-1.5 mt-1">
                      <span className={`inline-flex items-center rounded-sm px-1.5 py-0.5 text-[10px] font-medium border ${statusColor(row.case_status || 'Not Started')}`}>
                        {row.case_status || 'Not Started'}
                      </span>
                      {row.is_consult && (
                        <span className="inline-flex items-center rounded-sm px-1.5 py-0.5 text-[10px] font-medium border border-orange-300 bg-orange-50 text-orange-700">
                          Consult
                        </span>
                      )}
                      {row.slide_location && (
                        <span className="inline-flex items-center rounded-sm px-1 py-0.5 text-[10px] font-medium text-muted-foreground bg-muted/60 border border-gray-200 truncate max-w-[120px]">
                          {row.slide_location}
                        </span>
                      )}
                    </div>
                    <CaseProgressBar row={row} />
                    {row.notes && (
                      <p className="text-[11px] text-muted-foreground mt-1 truncate">{row.notes}</p>
                    )}
                  </button>
                )
              })}
            </div>
          </div>

          {/* RIGHT: Detail panel */}
          <div className="flex-1 min-w-0 overflow-y-auto border border-gray-300 rounded-md bg-background shadow-sm">
            {selectedRow ? (
              <div className="p-4">
                {/* Detail header */}
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h3 className="font-mono text-lg font-bold tracking-tight">{selectedRow.accession_number}</h3>
                    <p className="text-[12px] text-muted-foreground mt-0.5">
                      Added {selectedRow.created_at ? new Date(selectedRow.created_at).toLocaleDateString() : '—'}
                      {selectedRow.updated_at && ` · Updated ${new Date(selectedRow.updated_at).toLocaleDateString()}`}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-7 text-[12px] text-muted-foreground hover:text-destructive"
                      onClick={() => setDeleteConfirmId(selectedRow.id)}
                    >
                      <Trash2 className="h-3 w-3 mr-1" />Remove
                    </Button>
                  </div>
                </div>

                {/* Case warnings */}
                {caseWarnings.length > 0 && (
                  <div className="mb-4 space-y-2">
                    {caseWarnings.map((w, i) => (
                      <div
                        key={i}
                        className={`flex items-start gap-2.5 px-3 py-2.5 rounded-md border text-[13px] ${
                          w.type === 'already_scanned'
                            ? 'bg-amber-50 border-amber-300 text-amber-900'
                            : 'bg-blue-50 border-blue-300 text-blue-900'
                        }`}
                      >
                        {w.type === 'already_scanned' ? (
                          <Microscope className="h-4 w-4 shrink-0 mt-0.5 text-amber-600" />
                        ) : (
                          <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5 text-blue-600" />
                        )}
                        <div>
                          <p className="font-medium">{w.message}</p>
                          {w.type === 'already_scanned' && w.stain_breakdown && (
                            <p className="text-[12px] mt-0.5 opacity-80">
                              {Object.entries(w.stain_breakdown).map(([stain, count]) => `${count} ${stain}`).join(', ')}
                            </p>
                          )}
                          {w.type === 'duplicate_request' && w.sheets && (
                            <div className="text-[12px] mt-0.5 opacity-80">
                              {w.sheets.map((s, j) => (
                                <span key={j}>
                                  {j > 0 && ', '}
                                  {s.sheet_name} ({s.case_status})
                                </span>
                              ))}
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {/* Status + Location (always visible) */}
                <div className="mb-4 p-3 rounded-md bg-muted/30 border border-gray-300 grid grid-cols-2 gap-4">
                  <div>
                    <label className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Case Status</label>
                    <div className="mt-1.5">
                      {renderField(selectedRow, { key: 'case_status', label: 'Status', type: 'status' })}
                    </div>
                  </div>
                  <div>
                    <label className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Location</label>
                    <div className="mt-1.5">
                      {renderField(selectedRow, { key: 'slide_location', label: 'Location', type: 'text' })}
                    </div>
                  </div>
                </div>

                {/* Collapsible sections */}
                <div className="space-y-2">
                  {DETAIL_SECTIONS.map(section => {
                    const isExpanded = expandedSections.has(section.id)
                    return (
                      <div key={section.id} className="rounded-md border border-gray-300 overflow-hidden">
                        <button
                          onClick={() => toggleSection(section.id)}
                          className={`w-full flex items-center gap-2 px-3 py-2 text-[13px] font-medium hover:bg-muted/30 transition-colors border-l-3 ${section.accentColor}`}
                        >
                          {isExpanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                          {section.label}
                          <span className="text-[11px] text-muted-foreground font-normal">({section.fields.length} fields)</span>
                        </button>
                        {isExpanded && (
                          <div className={`px-3 pb-3 pt-2 border-l-3 ${section.accentColor}`}>
                            <div className="grid grid-cols-2 gap-x-4 gap-y-3">
                              {section.fields.map(field => (
                                <div key={field.key} className={field.span === 2 ? 'col-span-2' : ''}>
                                  <label className="text-[11px] font-medium text-muted-foreground mb-1 block">{field.label}</label>
                                  {renderField(selectedRow, field)}
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-muted-foreground text-sm py-20">
                Select a case from the list to view details
              </div>
            )}
          </div>
        </div>
      )}

      {/* Delete confirmation dialog */}
      <Dialog open={deleteConfirmId !== null} onOpenChange={() => setDeleteConfirmId(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Remove Case</DialogTitle>
            <DialogDescription>Remove this case from the tracker? This cannot be undone.</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setDeleteConfirmId(null)}>Cancel</Button>
            <Button variant="destructive" size="sm" onClick={() => deleteConfirmId && handleDeleteRow(deleteConfirmId)}>Remove</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Add case dialog */}
      <Dialog open={isAddOpen} onOpenChange={setIsAddOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Case</DialogTitle>
            <DialogDescription>Add a surgical accession to track</DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <Input placeholder="Accession number (e.g. BS-08-E31645)" value={newAccession} onChange={(e) => { setNewAccession(e.target.value); setAddError('') }} onKeyDown={(e) => e.key === 'Enter' && handleAddRow()} autoFocus />
            {addError && <p className="text-[13px] text-destructive">{addError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setIsAddOpen(false)}>Cancel</Button>
            <Button size="sm" onClick={handleAddRow} disabled={!newAccession.trim()}>Add</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Import cohort dialog */}
      <Dialog open={isImportOpen} onOpenChange={setIsImportOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Import from Cohort</DialogTitle>
            <DialogDescription>Pre-populate cases from an existing cohort.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            {cohorts.length === 0 ? (
              <p className="text-[13px] text-muted-foreground">No cohorts available</p>
            ) : (
              <Select value={selectedCohortId} onValueChange={setSelectedCohortId}>
                <SelectTrigger className="text-[13px]">
                  <SelectValue placeholder="Select a cohort..." />
                </SelectTrigger>
                <SelectContent>
                  {cohorts.map(c => (
                    <SelectItem key={c.id} value={String(c.id)} className="text-[13px]">
                      {c.name} ({c.case_count} cases, {c.slide_count} slides)
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
            {importResult && (
              <div className="rounded-md bg-emerald-50 border border-emerald-200 px-3 py-2 text-[13px] text-emerald-700">
                Added {importResult.added} cases{importResult.skipped > 0 && `, skipped ${importResult.skipped} duplicates`}
              </div>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setIsImportOpen(false)}>{importResult ? 'Done' : 'Cancel'}</Button>
            {!importResult && (
              <Button size="sm" onClick={handleImport} disabled={!selectedCohortId || importing}>{importing ? 'Importing...' : 'Import'}</Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Import CSV dialog */}
      <Dialog open={isCsvImportOpen} onOpenChange={setIsCsvImportOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Import from CSV / Excel</DialogTitle>
            <DialogDescription>
              Upload a .csv or .xlsx file. Columns are matched by header name — export a sheet first to see the expected format.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <input
              ref={csvFileRef}
              type="file"
              accept=".csv,.xlsx"
              className="hidden"
              onChange={e => setCsvImportFile(e.target.files?.[0] ?? null)}
            />
            <div className="flex items-center gap-2">
              <Button size="sm" variant="outline" onClick={() => csvFileRef.current?.click()}>
                Choose file
              </Button>
              <span className="text-[13px] text-muted-foreground truncate">
                {csvImportFile ? csvImportFile.name : 'No file selected'}
              </span>
            </div>
            {!csvImportResult && (
              <div className="rounded-md border border-gray-200 p-3 space-y-2">
                <p className="text-[12px] font-medium text-gray-700">If accession already exists:</p>
                <div className="flex flex-col gap-1.5">
                  <label className="flex items-start gap-2 cursor-pointer">
                    <input type="radio" className="mt-0.5" checked={csvImportMode === 'skip'} onChange={() => setCsvImportMode('skip')} />
                    <span className="text-[13px]">
                      <span className="font-medium">Skip</span>
                      <span className="text-muted-foreground"> — keep existing data, only add new cases</span>
                    </span>
                  </label>
                  <label className="flex items-start gap-2 cursor-pointer">
                    <input type="radio" className="mt-0.5" checked={csvImportMode === 'upsert'} onChange={() => setCsvImportMode('upsert')} />
                    <span className="text-[13px]">
                      <span className="font-medium">Merge</span>
                      <span className="text-muted-foreground"> — update existing rows with non-empty CSV values</span>
                    </span>
                  </label>
                </div>
              </div>
            )}
            {csvImportResult && (
              <div className={`rounded-md border px-3 py-2 text-[13px] ${(csvImportResult.added + csvImportResult.updated) > 0 ? 'bg-emerald-50 border-emerald-200 text-emerald-700' : 'bg-amber-50 border-amber-200 text-amber-700'}`}>
                {csvImportResult.added > 0 && <span>Added {csvImportResult.added} new case{csvImportResult.added !== 1 ? 's' : ''}</span>}
                {csvImportResult.added > 0 && (csvImportResult.updated > 0 || csvImportResult.skipped > 0) && <span>, </span>}
                {csvImportResult.updated > 0 && <span>updated {csvImportResult.updated} existing</span>}
                {csvImportResult.skipped > 0 && <span>{csvImportResult.updated > 0 ? ', ' : ''}skipped {csvImportResult.skipped} duplicate{csvImportResult.skipped !== 1 ? 's' : ''}</span>}
                {csvImportResult.errors.length > 0 && (
                  <ul className="mt-1 list-disc pl-4 text-[12px] text-red-600">
                    {csvImportResult.errors.slice(0, 5).map((e, i) => <li key={i}>{e}</li>)}
                    {csvImportResult.errors.length > 5 && <li>...and {csvImportResult.errors.length - 5} more</li>}
                  </ul>
                )}
              </div>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setIsCsvImportOpen(false)}>
              {csvImportResult ? 'Done' : 'Cancel'}
            </Button>
            {!csvImportResult && (
              <Button size="sm" onClick={handleCsvImport} disabled={!csvImportFile || csvImporting}>
                {csvImporting ? 'Importing...' : 'Import'}
              </Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Batch entry dialog (3-step) */}
      <Dialog open={isBatchOpen} onOpenChange={setIsBatchOpen}>
        <DialogContent className="sm:max-w-xl">
          {/* Step 1: Paste cases */}
          {batchStep === 'paste' && (
            <>
              <DialogHeader>
                <DialogTitle>Batch Entry — Add Cases</DialogTitle>
                <DialogDescription>Paste accession numbers to add. You'll fill in request details in the next step.</DialogDescription>
              </DialogHeader>
              <div className="space-y-3 py-2">
                <textarea
                  value={batchText}
                  onChange={(e) => setBatchText(e.target.value)}
                  placeholder={"Paste accession numbers, one per line:\nBS08-E12345\nBS26-000029\nBS24-001234"}
                  rows={8}
                  className="w-full rounded-md border border-gray-300 bg-transparent px-3 py-2 text-[13px] font-mono shadow-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring resize-y"
                  autoFocus
                />
                {batchText && (
                  <div className="flex items-center justify-between">
                    <p className="text-[11px] text-muted-foreground">
                      {batchText.split(/[\n,;]+/).filter(l => l.trim()).length} cases detected
                    </p>
                    <CopyCommaList text={batchText} />
                  </div>
                )}
              </div>
              <DialogFooter>
                <Button variant="outline" size="sm" onClick={() => setIsBatchOpen(false)}>Cancel</Button>
                <Button
                  size="sm"
                  onClick={handleBatchAdd}
                  disabled={!batchText.trim() || batchSubmitting}
                >
                  {batchSubmitting ? 'Adding...' : `Add ${batchText.split(/[\n,;]+/).filter(l => l.trim()).length || 0} Cases`}
                </Button>
              </DialogFooter>
            </>
          )}

          {/* Step 2: Staging — fill shared fields */}
          {batchStep === 'staging' && (
            <>
              <DialogHeader>
                <DialogTitle>Batch Entry — Request Details</DialogTitle>
                <DialogDescription>
                  {batchAddResult && (
                    <span>Added {batchAddResult.added} cases{batchAddResult.skipped > 0 && `, skipped ${batchAddResult.skipped} duplicates`}. </span>
                  )}
                  Fill in shared fields below, or skip to add them later.
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-4 py-2">
                {/* Preview of added cases */}
                <div>
                  <label className="text-[12px] font-medium text-muted-foreground mb-1.5 block">Cases added ({batchRowIds.length})</label>
                  <div className="flex flex-wrap gap-1 max-h-[80px] overflow-y-auto p-2 rounded-md border border-gray-300 bg-muted/20">
                    {batchRowIds.map(id => {
                      const row = rows.find(r => r.id === id)
                      return row ? (
                        <span key={id} className="inline-flex rounded bg-gray-100 border border-gray-300 px-1.5 py-0.5 text-[12px] font-mono text-gray-700">
                          {row.accession_number}
                        </span>
                      ) : null
                    })}
                  </div>
                </div>

                {/* Shared fields */}
                <div className="rounded-md border border-gray-300 bg-muted/20 p-3 space-y-3">
                  <p className="text-[12px] font-medium text-foreground">Shared fields <span className="text-muted-foreground font-normal">— applied to all {batchRowIds.length} cases</span></p>

                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="text-[11px] font-medium text-muted-foreground mb-1 block">Status</label>
                      <Select value={batchStatus} onValueChange={setBatchStatus}>
                        <SelectTrigger className="h-8 text-[13px]">
                          <span className={`inline-flex items-center rounded-sm px-1.5 py-0.5 text-[12px] font-medium border ${statusColor(batchStatus)}`}>
                            {batchStatus}
                          </span>
                        </SelectTrigger>
                        <SelectContent>
                          {CASE_STATUSES.map(s => (
                            <SelectItem key={s} value={s} className="text-[13px]">
                              <span className={`inline-flex items-center rounded-sm px-1.5 py-0.5 text-[12px] font-medium border ${statusColor(s)}`}>{s}</span>
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    <div>
                      <label className="text-[11px] font-medium text-muted-foreground mb-1 block">Order ID</label>
                      <Input
                        className="h-8 text-[13px]"
                        placeholder="e.g. ORD-2026-001"
                        value={batchOrderId}
                        onChange={(e) => setBatchOrderId(e.target.value)}
                      />
                    </div>
                  </div>

                  <div className="flex items-center gap-2">
                    <Checkbox checked={batchIsConsult} onCheckedChange={(v) => setBatchIsConsult(!!v)} />
                    <label className="text-[13px] text-muted-foreground">These are consult cases</label>
                  </div>
                </div>
              </div>
              <DialogFooter>
                <Button variant="outline" size="sm" onClick={() => setIsBatchOpen(false)}>
                  Skip for now
                </Button>
                <Button
                  size="sm"
                  onClick={handleBatchApply}
                  disabled={batchApplying}
                >
                  {batchApplying ? 'Applying...' : `Apply to ${batchRowIds.length} Cases`}
                </Button>
              </DialogFooter>
            </>
          )}

          {/* Step 3: Done */}
          {batchStep === 'done' && (
            <>
              <DialogHeader>
                <DialogTitle>Batch Entry — Complete</DialogTitle>
              </DialogHeader>
              <div className="space-y-3 py-2">
                <div className="rounded-md bg-emerald-50 border border-emerald-200 px-3 py-2.5 text-[13px] text-emerald-700">
                  {batchRowIds.length} cases updated successfully
                </div>
                <p className="text-[12px] text-muted-foreground">
                  All set to <span className={`inline-flex items-center rounded-sm px-1 py-0.5 text-[11px] font-medium border ${statusColor(batchStatus)}`}>{batchStatus}</span>
                  {batchOrderId && <> · Order ID: <span className="font-mono font-medium">{batchOrderId}</span></>}
                  {batchIsConsult && <> · Consult</>}
                </p>
              </div>
              <DialogFooter>
                <Button size="sm" onClick={() => setIsBatchOpen(false)}>Done</Button>
              </DialogFooter>
            </>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
