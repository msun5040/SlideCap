import { useState, useEffect, useMemo } from 'react'
import { Loader2, FileUp, ChevronDown, ChevronRight, Folder, FileImage, Check, Search } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription,
} from '@/components/ui/dialog'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { getApiBase } from '@/api'

interface ExternalFile {
  filename: string
  relative_path: string   // path within external/, e.g. "GBM-project/slide1.svs"
  folder: string          // '' for files sitting directly in external/
  stem: string
  slide_hash: string
  file_size_bytes: number
}

const ROOT_LABEL = 'external/ (top level)'

function formatSize(bytes: number) {
  if (!bytes) return ''
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(0)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
}

interface Props {
  open: boolean
  onClose: () => void
  onRegistered?: () => void
}

/**
 * Register non-clinical / outside-hospital slides (no accession). They live in
 * the network drive's `external/` folder; here you attach a name + attributes so
 * they become searchable real slides usable in cohorts and analyses.
 */
export function ExternalSlideDialog({ open, onClose, onRegistered }: Props) {
  const [files, setFiles] = useState<ExternalFile[]>([])
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState<string | null>(null)

  // single-register form — `filename` is the path relative to external/
  const [filename, setFilename] = useState('')
  const [pickerOpen, setPickerOpen] = useState(false)
  const [fileQuery, setFileQuery] = useState('')
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set())
  const [name, setName] = useState('')
  const [block, setBlock] = useState('')
  const [stain, setStain] = useState('')
  const [slideNum, setSlideNum] = useState('')
  const [year, setYear] = useState('')
  const [source, setSource] = useState('')
  const [saving, setSaving] = useState(false)

  // CSV bulk
  const [csv, setCsv] = useState('')
  const [importing, setImporting] = useState(false)

  const loadFiles = async () => {
    setLoading(true)
    try {
      const res = await fetch(`${getApiBase()}/external/unregistered`)
      if (res.ok) setFiles(await res.json())
    } catch (e) { console.error(e) } finally { setLoading(false) }
  }

  useEffect(() => { if (open) { loadFiles(); setMsg(null); setFileQuery(''); setCollapsed(new Set()) } }, [open])

  // Unregistered files grouped by their folder inside external/. Root-level
  // files come first, then folders alphabetically.
  const grouped = useMemo(() => {
    const q = fileQuery.trim().toLowerCase()
    const shown = q ? files.filter(f => f.relative_path.toLowerCase().includes(q)) : files
    const map = new Map<string, ExternalFile[]>()
    for (const f of shown) {
      const key = f.folder || ''
      if (!map.has(key)) map.set(key, [])
      map.get(key)!.push(f)
    }
    return [...map.entries()].sort(([a], [b]) => (a === '' ? -1 : b === '' ? 1 : a.localeCompare(b)))
  }, [files, fileQuery])

  const toggleFolder = (folder: string) => {
    setCollapsed(prev => {
      const next = new Set(prev)
      next.has(folder) ? next.delete(folder) : next.add(folder)
      return next
    })
  }

  const pickFile = (f: ExternalFile) => {
    setFilename(f.relative_path)
    setPickerOpen(false)
    // Pre-fill the name from the filename stem if the user hasn't typed one.
    if (!name.trim()) setName(f.filename.replace(/\.[^.]+$/, ''))
  }

  const registerOne = async () => {
    if (!filename || !name.trim()) { setMsg('Pick a file and enter a name.'); return }
    setSaving(true); setMsg(null)
    try {
      const res = await fetch(`${getApiBase()}/external/slides`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filename, name: name.trim(),
          block_id: block || undefined, stain_type: stain || undefined,
          slide_number: slideNum || undefined, year: year ? Number(year) : undefined,
          source: source || undefined,
        }),
      })
      if (res.ok) {
        setMsg(`Registered "${name}".`)
        setFilename(''); setName(''); setBlock(''); setStain(''); setSlideNum(''); setSource('')
        await loadFiles()
        onRegistered?.()
      } else {
        const e = await res.json().catch(() => ({}))
        setMsg(e.detail || 'Registration failed')
      }
    } catch { setMsg('Network error') } finally { setSaving(false) }
  }

  // Parse CSV: header row with any of filename,name,block,stain,slide_number,year,source
  const importCsv = async () => {
    const lines = csv.trim().split('\n').filter(l => l.trim())
    if (lines.length < 2) { setMsg('Paste a CSV with a header row + at least one row.'); return }
    const header = lines[0].split(',').map(h => h.trim().toLowerCase())
    const idx = (k: string) => header.indexOf(k)
    const rows = lines.slice(1).map(line => {
      const c = line.split(',').map(x => x.trim())
      const get = (k: string) => { const i = idx(k); return i >= 0 ? c[i] : '' }
      return {
        filename: get('filename'),
        name: get('name'),
        block_id: get('block') || get('block_id') || undefined,
        stain_type: get('stain') || get('stain_type') || undefined,
        slide_number: get('slide_number') || get('slide') || undefined,
        year: get('year') ? Number(get('year')) : undefined,
        source: get('source') || undefined,
      }
    }).filter(r => r.filename && r.name)
    if (rows.length === 0) { setMsg('No valid rows (need filename + name).'); return }
    setImporting(true); setMsg(null)
    try {
      const res = await fetch(`${getApiBase()}/external/slides/import`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rows }),
      })
      if (res.ok) {
        const d = await res.json()
        setMsg(`Imported ${d.created} slide(s)${d.errors?.length ? `, ${d.errors.length} error(s)` : ''}.`)
        setCsv('')
        await loadFiles()
        onRegistered?.()
      } else { setMsg('Import failed') }
    } catch { setMsg('Network error') } finally { setImporting(false) }
  }

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-2xl max-h-[85vh] overflow-auto">
        <DialogHeader>
          <DialogTitle>External (non-clinical) slides</DialogTitle>
          <DialogDescription>
            Register outside-hospital scans (no accession). Files placed in the network drive's
            <code className="mx-1 text-[11px] bg-muted px-1 rounded">slides/external/</code> folder appear below,
            including any subfolders you organize them into (one per project, say).
          </DialogDescription>
        </DialogHeader>

        {msg && <div className="text-sm rounded-md bg-muted px-3 py-2">{msg}</div>}

        {/* Register one */}
        <div className="space-y-3 border rounded-lg p-3">
          <div className="text-sm font-medium">Register a file</div>
          <div className="flex items-center gap-2 text-sm">
            <span className="text-muted-foreground w-20">File</span>
            <Popover open={pickerOpen} onOpenChange={setPickerOpen}>
              <PopoverTrigger asChild>
                <Button variant="outline" className="flex-1 justify-between font-normal" disabled={loading}>
                  {filename ? (
                    <span className="truncate text-left">
                      {filename.includes('/') && (
                        <span className="text-muted-foreground">{filename.slice(0, filename.lastIndexOf('/') + 1)}</span>
                      )}
                      {filename.slice(filename.lastIndexOf('/') + 1)}
                    </span>
                  ) : (
                    <span className="text-muted-foreground">
                      {loading ? 'Loading…' : files.length ? 'Choose an unregistered file…' : 'No unregistered files'}
                    </span>
                  )}
                  <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-[480px] p-0" align="start">
                <div className="flex items-center gap-2 border-b px-3 py-2">
                  <Search className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                  <input
                    autoFocus
                    className="flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground"
                    placeholder="Filter by file or folder…"
                    value={fileQuery}
                    onChange={e => setFileQuery(e.target.value)}
                  />
                  <span className="text-[11px] text-muted-foreground tabular-nums shrink-0">
                    {grouped.reduce((n, [, fs]) => n + fs.length, 0)} unregistered
                  </span>
                </div>
                <div className="max-h-72 overflow-auto py-1">
                  {grouped.length === 0 && (
                    <div className="px-3 py-6 text-center text-sm text-muted-foreground">
                      {files.length ? 'No files match that filter.' : 'No unregistered files in external/.'}
                    </div>
                  )}
                  {grouped.map(([folder, folderFiles]) => {
                    const isCollapsed = collapsed.has(folder)
                    return (
                      <div key={folder || '__root__'}>
                        <button
                          type="button"
                          onClick={() => toggleFolder(folder)}
                          className="flex w-full items-center gap-1.5 px-2 py-1 text-left hover:bg-muted/60"
                        >
                          {isCollapsed
                            ? <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                            : <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />}
                          <Folder className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                          <span className="truncate text-xs font-medium" title={folder || ROOT_LABEL}>
                            {folder || ROOT_LABEL}
                          </span>
                          <span className="ml-auto shrink-0 text-[11px] text-muted-foreground tabular-nums">
                            {folderFiles.length}
                          </span>
                        </button>
                        {!isCollapsed && folderFiles.map(f => (
                          <button
                            type="button"
                            key={f.slide_hash}
                            onClick={() => pickFile(f)}
                            className="flex w-full items-center gap-1.5 py-1 pl-8 pr-2 text-left hover:bg-muted"
                          >
                            <FileImage className="h-3.5 w-3.5 shrink-0 text-blue-500" />
                            <span className="truncate font-mono text-xs" title={f.relative_path}>{f.filename}</span>
                            <span className="ml-auto shrink-0 text-[11px] text-muted-foreground tabular-nums">
                              {formatSize(f.file_size_bytes)}
                            </span>
                            {filename === f.relative_path && <Check className="h-3.5 w-3.5 shrink-0 text-primary" />}
                          </button>
                        ))}
                      </div>
                    )
                  })}
                </div>
              </PopoverContent>
            </Popover>
          </div>
          <div className="grid grid-cols-2 gap-2">
            <Input placeholder="Name (required) — e.g. OSH-2024-001" value={name} onChange={e => setName(e.target.value)} />
            <Input placeholder="Source / hospital (optional)" value={source} onChange={e => setSource(e.target.value)} />
            <Input placeholder="Block (e.g. A1)" value={block} onChange={e => setBlock(e.target.value)} />
            <Input placeholder="Stain (e.g. HNE)" value={stain} onChange={e => setStain(e.target.value)} />
            <Input placeholder="Slide # (optional)" value={slideNum} onChange={e => setSlideNum(e.target.value)} />
            <Input placeholder="Year (e.g. 2024)" value={year} onChange={e => setYear(e.target.value)} />
          </div>
          <Button onClick={registerOne} disabled={saving} size="sm">
            {saving ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Registering…</> : 'Register slide'}
          </Button>
        </div>

        {/* Bulk CSV */}
        <div className="space-y-2 border rounded-lg p-3">
          <div className="text-sm font-medium">Bulk import (CSV)</div>
          <p className="text-xs text-muted-foreground">
            Header row then one row per slide. Columns: <code className="text-[11px] bg-muted px-1 rounded">filename,name,block,stain,slide_number,year,source</code> (filename + name required).
            For files in subfolders use the path relative to <code className="text-[11px] bg-muted px-1 rounded">external/</code>, e.g. <code className="text-[11px] bg-muted px-1 rounded">GBM-project/OSH001.svs</code>.
          </p>
          <textarea
            className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[100px]"
            placeholder={'filename,name,block,stain,year,source\nGBM-project/OSH001.svs,OSH-2024-001,A1,HNE,2024,Mercy Hospital'}
            value={csv}
            onChange={e => setCsv(e.target.value)}
          />
          <Button onClick={importCsv} disabled={importing} size="sm" variant="outline">
            {importing ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Importing…</> : <><FileUp className="mr-2 h-4 w-4" />Import CSV</>}
          </Button>
        </div>

        <div className="flex justify-end pt-2">
          <Button variant="outline" onClick={onClose}>Done</Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}
