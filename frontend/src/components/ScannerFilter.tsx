import { useCallback, useEffect, useState } from 'react'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { getApiBase } from '@/api'

/**
 * Filter a slide search by which scanner produced the slides.
 *
 * Scanner is a batch variable — in the UNI embedding atlas, Aperio and Grundium
 * slides were perfectly separable from the embeddings alone — so being able to
 * drop one make before building a cohort keeps that out of downstream results.
 *
 * Slides whose header has never been read are their own bucket ("Not read yet"),
 * never silently folded into a scanner. An "everything but X" choice KEEPS them:
 * an exclusion should remove what it names, not everything unverified.
 */

export interface ScannerOption {
  scanner: string
  label: string
  count: number
}

export interface ScannerSummary {
  scanners: ScannerOption[]
  unread: number
  no_info: number
}

/** 'all' | 'unknown' | `s:<id>` (only this one) | `x:<id>` (all but this one) */
export type ScannerFilterValue = string

export function applyScannerParams(params: URLSearchParams, value: ScannerFilterValue) {
  if (!value || value === 'all') return
  if (value === 'unknown') {
    params.append('scanner', 'unknown')
    return
  }
  const sep = value.indexOf(':')
  if (sep < 0) return
  const kind = value.slice(0, sep)
  const id = value.slice(sep + 1)
  params.append(kind === 'x' ? 'exclude_scanner' : 'scanner', id)
}

export function useScanners() {
  const [summary, setSummary] = useState<ScannerSummary | null>(null)

  const reload = useCallback(() => {
    fetch(`${getApiBase()}/scanners`)
      .then(r => (r.ok ? r.json() : null))
      .then(d => setSummary(d))
      .catch(() => setSummary(null))
  }, [])

  useEffect(() => { reload() }, [reload])
  return { summary, reload }
}

interface Props {
  value: ScannerFilterValue
  onChange: (v: ScannerFilterValue) => void
  summary: ScannerSummary | null
  className?: string
}

export function ScannerFilter({ value, onChange, summary, className }: Props) {
  const scanners = summary?.scanners ?? []
  // Nothing has ever been read and nothing is indexed — no filter to offer.
  if (!summary || (scanners.length === 0 && summary.unread === 0)) return null

  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger className={className || 'w-48'}>
        <SelectValue placeholder="Scanner" />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="all">All scanners</SelectItem>
        {scanners.map(s => (
          <SelectItem key={`s:${s.scanner}`} value={`s:${s.scanner}`}>
            {s.label} ({s.count})
          </SelectItem>
        ))}
        {summary.unread > 0 && (
          <SelectItem value="unknown">Not read yet ({summary.unread})</SelectItem>
        )}
        {scanners.length > 1 && <SelectSeparator />}
        {scanners.length > 1 && scanners.map(s => (
          <SelectItem key={`x:${s.scanner}`} value={`x:${s.scanner}`}>
            Exclude {s.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}

/**
 * Fill in the scanner for slides nobody has read yet.
 *
 * Indexing never opens slide files, so this is the one place the headers get
 * read. It is header-only (no pixel decode, no label image), which is O(1) in
 * slide size — but it is still one network file open per slide, so it runs in
 * bounded batches and reports what is left rather than blocking on the library.
 *
 * Headless: the caller decides whether this is a button, a menu item, or a row.
 */
export function useScannerDetect(summary: ScannerSummary | null, onDone: () => void) {
  const [running, setRunning] = useState(false)
  const [left, setLeft] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  const run = async () => {
    if (!summary || running) return
    setRunning(true)
    setError(null)
    try {
      let remaining = summary.unread
      // Batches, not one giant request: a stalled share should cost one batch,
      // not the whole run.
      for (let i = 0; i < 40 && remaining > 0; i++) {
        const res = await fetch(`${getApiBase()}/scanners/detect`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ unread: true, limit: 200 }),
        })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        if (data.updated === 0) {
          // Nothing readable left (missing files, share down) — stop rather than spin.
          if (data.errors?.length) setError(`${data.errors.length} slide(s) could not be read`)
          break
        }
        remaining = data.remaining ?? 0
        setLeft(remaining)
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'failed')
    } finally {
      setRunning(false)
      setLeft(null)
      onDone()
    }
  }

  return { running, left, error, run }
}

/** What a scanner filter value reads as in an active-filter chip. */
export function scannerFilterLabel(value: ScannerFilterValue, summary: ScannerSummary | null): string {
  if (!value || value === 'all') return ''
  if (value === 'unknown') return 'Scanner not read'
  const sep = value.indexOf(':')
  if (sep < 0) return ''
  const kind = value.slice(0, sep)
  const id = value.slice(sep + 1)
  const label = summary?.scanners.find(s => s.scanner === id)?.label || id
  return kind === 'x' ? `Not ${label}` : label
}
