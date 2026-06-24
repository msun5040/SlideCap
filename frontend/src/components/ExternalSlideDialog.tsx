import { useState, useEffect } from 'react'
import { Loader2, FileUp } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription,
} from '@/components/ui/dialog'
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@/components/ui/select'
import { getApiBase } from '@/api'

interface ExternalFile {
  filename: string
  stem: string
  slide_hash: string
  file_size_bytes: number
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

  // single-register form
  const [filename, setFilename] = useState('')
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

  useEffect(() => { if (open) { loadFiles(); setMsg(null) } }, [open])

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
            <code className="mx-1 text-[11px] bg-muted px-1 rounded">slides/external/</code> folder appear below.
          </DialogDescription>
        </DialogHeader>

        {msg && <div className="text-sm rounded-md bg-muted px-3 py-2">{msg}</div>}

        {/* Register one */}
        <div className="space-y-3 border rounded-lg p-3">
          <div className="text-sm font-medium">Register a file</div>
          <div className="flex items-center gap-2 text-sm">
            <span className="text-muted-foreground w-20">File</span>
            <Select value={filename} onValueChange={setFilename}>
              <SelectTrigger className="flex-1">
                <SelectValue placeholder={loading ? 'Loading…' : files.length ? 'Choose an unregistered file…' : 'No unregistered files'} />
              </SelectTrigger>
              <SelectContent>
                {files.map(f => <SelectItem key={f.slide_hash} value={f.filename}>{f.filename}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
          <div className="grid grid-cols-2 gap-2">
            <Input placeholder="Name (required) — e.g. OSH-2024-001" value={name} onChange={e => setName(e.target.value)} />
            <Input placeholder="Source / hospital (optional)" value={source} onChange={e => setSource(e.target.value)} />
            <Input placeholder="Block (e.g. A1)" value={block} onChange={e => setBlock(e.target.value)} />
            <Input placeholder="Stain (e.g. HE)" value={stain} onChange={e => setStain(e.target.value)} />
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
          </p>
          <textarea
            className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[100px]"
            placeholder={'filename,name,block,stain,year,source\nOSH001.svs,OSH-2024-001,A1,HE,2024,Mercy Hospital'}
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
