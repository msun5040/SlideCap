import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import {
  ArrowDownToLine, Download, Loader2, Lock, Pause, Play, ScrollText, Search, Trash2, X,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { getApiBase } from '@/api'
import { saveBlob } from '@/lib/download'

/**
 * Live view of the backend's console output (Settings → Server log).
 *
 * Gated by ADMIN_LOG_PASSWORD on the server — a stopgap until per-user
 * permissions exist. Entering it returns a short-lived token, kept in
 * sessionStorage so it's forgotten when the tab closes, and sent as
 * X-Server-Log-Token. The normal login still applies on top.
 *
 * Tails by cursor: each poll asks for lines after the last seq seen, so
 * nothing is re-sent. Polls only while this view is open and Live is on.
 */

interface LogLine {
  seq: number
  ts: number
  stream: string
  level: 'error' | 'warning' | 'info' | 'debug'
  text: string
  /** Client-side marker rows (gaps, restarts); not from the server. */
  marker?: boolean
}

interface LogPage {
  lines: LogLine[]
  next_seq: number
  latest_seq: number
  has_more: boolean
  gap: boolean
  boot_id: string | null
  started_at: string | null
  capacity: number
  file_available: boolean
}

const TOKEN_KEY = 'slidecap_server_log_token'
const MAX_LINES = 5000
const POLL_MS = 2000

type LevelFilter = 'all' | 'warn' | 'error'

function readToken(): string | null {
  try { return sessionStorage.getItem(TOKEN_KEY) } catch { return null }
}
function writeToken(token: string | null) {
  try {
    if (token) sessionStorage.setItem(TOKEN_KEY, token)
    else sessionStorage.removeItem(TOKEN_KEY)
  } catch { /* storage unavailable — token just won't persist */ }
}

let markerSeq = -1
function marker(text: string, level: LogLine['level'] = 'warning'): LogLine {
  return { seq: markerSeq--, ts: Date.now() / 1000, stream: 'marker', level, text, marker: true }
}

function formatTime(ts: number) {
  const d = new Date(ts * 1000)
  return d.toLocaleTimeString([], { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0')
}

const LEVEL_CLASS: Record<LogLine['level'], string> = {
  error: 'text-red-400',
  warning: 'text-amber-300',
  info: 'text-neutral-200',
  debug: 'text-neutral-500',
}

export function ServerLogView({ active }: { active: boolean }) {
  const [enabled, setEnabled] = useState<boolean | null>(null)
  const [token, setToken] = useState<string | null>(() => readToken())
  const [password, setPassword] = useState('')
  const [unlocking, setUnlocking] = useState(false)
  const [lockMessage, setLockMessage] = useState('')

  const [lines, setLines] = useState<LogLine[]>([])
  const [live, setLive] = useState(true)
  const [levelFilter, setLevelFilter] = useState<LevelFilter>('all')
  const [query, setQuery] = useState('')
  const [follow, setFollow] = useState(true)
  const [fetchError, setFetchError] = useState('')
  const [meta, setMeta] = useState<{ startedAt: string | null; capacity: number; fileAvailable: boolean }>(
    { startedAt: null, capacity: 0, fileAvailable: false },
  )
  const [downloading, setDownloading] = useState(false)

  const nextSeq = useRef(0)
  const bootId = useRef<string | null>(null)
  const inFlight = useRef(false)
  const scrollRef = useRef<HTMLDivElement | null>(null)

  // ── Status: is the feature switched on for this server? ─────────────
  useEffect(() => {
    if (!active || enabled !== null) return
    fetch(`${getApiBase()}/admin/logs/status`)
      .then(r => (r.ok ? r.json() : null))
      .then(d => setEnabled(d ? !!d.enabled : false))
      .catch(() => setEnabled(false))
  }, [active, enabled])

  const lock = useCallback((message = '') => {
    writeToken(null)
    setToken(null)
    setLines([])
    nextSeq.current = 0
    bootId.current = null
    setLockMessage(message)
  }, [])

  const unlock = async () => {
    if (!password) return
    setUnlocking(true)
    setLockMessage('')
    try {
      const res = await fetch(`${getApiBase()}/admin/logs/unlock`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password }),
      })
      const d = await res.json().catch(() => null)
      if (!res.ok) throw new Error(d?.detail || `Could not unlock (${res.status})`)
      writeToken(d.token)
      setToken(d.token)
      setPassword('')
      setFollow(true)
    } catch (e: any) {
      setLockMessage(e.message || 'Could not unlock')
    } finally {
      setUnlocking(false)
    }
  }

  // ── Tail ─────────────────────────────────────────────────────────────
  const poll = useCallback(async () => {
    if (!token || inFlight.current) return
    inFlight.current = true
    try {
      // Page until caught up (bounded, so a burst can't spin forever).
      for (let page = 0; page < 5; page++) {
        const res = await fetch(
          `${getApiBase()}/admin/logs?after=${nextSeq.current}&limit=2000`,
          { headers: { 'X-Server-Log-Token': token } },
        )
        if (res.status === 401) { lock('Your server-log session expired. Enter the password again.'); return }
        if (res.status === 503) { setEnabled(false); lock(); return }
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data: LogPage = await res.json()
        setFetchError('')

        const extra: LogLine[] = []
        if (bootId.current && data.boot_id && data.boot_id !== bootId.current) {
          // Backend restarted: seq numbering starts over.
          bootId.current = data.boot_id
          nextSeq.current = 0
          extra.push(marker(`— backend restarted at ${data.started_at ?? 'unknown time'} —`))
          setLines(prev => [...prev, ...extra])
          continue
        }
        bootId.current = data.boot_id
        if (data.gap) extra.push(marker('— some lines were skipped (older than the server keeps in memory; see the downloaded log) —'))

        if (data.lines.length > 0 || extra.length > 0) {
          setLines(prev => {
            const merged = prev.concat(extra, data.lines)
            return merged.length > MAX_LINES ? merged.slice(merged.length - MAX_LINES) : merged
          })
        }
        nextSeq.current = data.next_seq
        setMeta({ startedAt: data.started_at, capacity: data.capacity, fileAvailable: data.file_available })
        if (!data.has_more) break
      }
    } catch (e: any) {
      setFetchError(e.message || 'Could not reach the server')
    } finally {
      inFlight.current = false
    }
  }, [token, lock])

  useEffect(() => {
    if (!active || !token || !live) return
    poll()
    const t = setInterval(poll, POLL_MS)
    return () => clearInterval(t)
  }, [active, token, live, poll])

  // ── Filtering ────────────────────────────────────────────────────────
  const visible = useMemo(() => {
    const q = query.trim().toLowerCase()
    return lines.filter(l => {
      if (l.marker) return true
      if (levelFilter === 'error' && l.level !== 'error') return false
      if (levelFilter === 'warn' && l.level !== 'error' && l.level !== 'warning') return false
      return !q || l.text.toLowerCase().includes(q)
    })
  }, [lines, levelFilter, query])

  const counts = useMemo(() => {
    let errors = 0, warnings = 0
    for (const l of lines) {
      if (l.marker) continue
      if (l.level === 'error') errors++
      else if (l.level === 'warning') warnings++
    }
    return { errors, warnings }
  }, [lines])

  // ── Auto-scroll: stick to the bottom unless the user has scrolled up ──
  const onScroll = () => {
    const el = scrollRef.current
    if (!el) return
    setFollow(el.scrollHeight - el.scrollTop - el.clientHeight < 24)
  }
  useLayoutEffect(() => {
    const el = scrollRef.current
    if (el && follow) el.scrollTop = el.scrollHeight
  }, [visible, follow, active])

  const jumpToLatest = () => {
    const el = scrollRef.current
    if (el) el.scrollTop = el.scrollHeight
    setFollow(true)
  }

  const download = async () => {
    if (!token) return
    setDownloading(true)
    try {
      const res = await fetch(`${getApiBase()}/admin/logs/download`, {
        headers: { 'X-Server-Log-Token': token },
      })
      if (res.status === 401) { lock('Your server-log session expired. Enter the password again.'); return }
      if (!res.ok) {
        const d = await res.json().catch(() => null)
        throw new Error(d?.detail || `Download failed (${res.status})`)
      }
      const cd = res.headers.get('Content-Disposition') || ''
      const name = /filename="?([^";]+)"?/.exec(cd)?.[1] || 'slidecap-server.log'
      saveBlob(await res.blob(), name)
    } catch (e: any) {
      setFetchError(e.message || 'Download failed')
    } finally {
      setDownloading(false)
    }
  }

  // ── Locked / disabled ────────────────────────────────────────────────
  if (!token) {
    return (
      <div className="flex h-full min-h-0 items-center justify-center p-6">
        <div className="w-full max-w-md space-y-4 rounded-lg border bg-background p-6">
          <div className="flex items-center gap-2">
            <ScrollText className="h-5 w-5 text-muted-foreground" />
            <h1 className="text-lg font-semibold">Server log</h1>
          </div>
          {enabled === null ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" /> Checking…
            </div>
          ) : !enabled ? (
            <div className="space-y-2 text-sm text-muted-foreground">
              <p>The server log view is turned off on this server.</p>
              <p>
                To turn it on, add a password to the backend's <span className="font-mono">.env</span> file
                and restart the backend:
              </p>
              <pre className="rounded bg-muted px-3 py-2 font-mono text-[12px] text-foreground">ADMIN_LOG_PASSWORD=choose-a-password</pre>
            </div>
          ) : (
            <form
              className="space-y-3"
              onSubmit={e => { e.preventDefault(); unlock() }}
            >
              <p className="text-sm text-muted-foreground">
                Shows the backend's live console output. Enter the server-log password to continue.
              </p>
              <Input
                type="password"
                autoFocus
                autoComplete="current-password"
                value={password}
                onChange={e => setPassword(e.target.value)}
                placeholder="Password"
              />
              {lockMessage && <p className="text-[12px] text-red-600">{lockMessage}</p>}
              <Button type="submit" className="w-full" disabled={unlocking || !password}>
                {unlocking ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Lock className="mr-2 h-4 w-4" />}
                Unlock
              </Button>
            </form>
          )}
        </div>
      </div>
    )
  }

  // ── Viewer ───────────────────────────────────────────────────────────
  return (
    <div className="flex h-full min-h-0 flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2">
        <div className="mr-2 flex items-center gap-2">
          <ScrollText className="h-5 w-5 text-muted-foreground" />
          <h1 className="text-lg font-semibold">Server log</h1>
          <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] ${
            live ? 'bg-emerald-50 text-emerald-700' : 'bg-muted text-muted-foreground'
          }`}>
            <span className={`h-1.5 w-1.5 rounded-full ${live ? 'bg-emerald-500 animate-pulse' : 'bg-muted-foreground'}`} />
            {live ? 'Live' : 'Paused'}
          </span>
        </div>

        <Button variant="outline" size="sm" className="h-8" onClick={() => setLive(v => !v)}>
          {live ? <Pause className="mr-1 h-3.5 w-3.5" /> : <Play className="mr-1 h-3.5 w-3.5" />}
          {live ? 'Pause' : 'Resume'}
        </Button>

        <div className="inline-flex overflow-hidden rounded-md border text-[12px]">
          {([
            ['all', 'All'],
            ['warn', `Warnings + errors${counts.warnings + counts.errors ? ` (${counts.warnings + counts.errors})` : ''}`],
            ['error', `Errors${counts.errors ? ` (${counts.errors})` : ''}`],
          ] as const).map(([v, label]) => (
            <button
              key={v}
              onClick={() => setLevelFilter(v)}
              className={`h-8 border-l px-2.5 first:border-l-0 ${
                levelFilter === v ? 'bg-primary text-primary-foreground' : 'hover:bg-muted'
              }`}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="relative min-w-48 flex-1">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="Filter lines…"
            className="h-8 pl-7 pr-7 text-[13px]"
          />
          {query && (
            <button className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                    onClick={() => setQuery('')}>
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>

        <Button variant="outline" size="sm" className="h-8" onClick={() => setLines([])} title="Clear this view (the server keeps its log)">
          <Trash2 className="mr-1 h-3.5 w-3.5" />Clear view
        </Button>
        <Button variant="outline" size="sm" className="h-8" onClick={download}
                disabled={downloading || !meta.fileAvailable}
                title={meta.fileAvailable ? 'Download the full log file (includes history beyond this view)' : 'This server is not writing a log file'}>
          {downloading ? <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" /> : <Download className="mr-1 h-3.5 w-3.5" />}
          Download
        </Button>
        <Button variant="outline" size="sm" className="h-8" onClick={() => lock()} title="Lock the server log">
          <Lock className="mr-1 h-3.5 w-3.5" />Lock
        </Button>
      </div>

      <div className="relative min-h-0 flex-1 overflow-hidden rounded-lg border border-neutral-800 bg-neutral-950">
        <div
          ref={scrollRef}
          onScroll={onScroll}
          className="h-full overflow-auto px-3 py-2 font-mono text-[12px] leading-[1.45]"
        >
          {visible.length === 0 ? (
            <p className="py-8 text-center text-neutral-500">
              {lines.length === 0 ? 'Waiting for output…' : 'No lines match the current filter.'}
            </p>
          ) : visible.map(l => (
            l.marker ? (
              <div key={l.seq} className="my-1 text-center text-[11px] text-amber-400/80">{l.text}</div>
            ) : (
              <div key={l.seq} className={`flex gap-3 whitespace-pre-wrap break-all ${LEVEL_CLASS[l.level]}`}>
                <span className="shrink-0 select-none text-neutral-600">{formatTime(l.ts)}</span>
                <span className={l.stream === 'access' && l.level === 'info' ? 'text-neutral-400' : ''}>{l.text}</span>
              </div>
            )
          ))}
        </div>

        {!follow && (
          <button
            onClick={jumpToLatest}
            className="absolute bottom-3 right-4 inline-flex items-center gap-1 rounded-full bg-neutral-800 px-3 py-1 text-[12px] text-neutral-100 shadow hover:bg-neutral-700"
          >
            <ArrowDownToLine className="h-3.5 w-3.5" /> Jump to latest
          </button>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
        <span>{visible.length.toLocaleString()} of {lines.length.toLocaleString()} lines shown</span>
        {meta.startedAt && <span>backend started {meta.startedAt.replace('T', ' ')}</span>}
        {meta.capacity > 0 && <span>server keeps the last {meta.capacity.toLocaleString()} lines in memory</span>}
        {fetchError && <span className="text-red-600">{fetchError}</span>}
      </div>
    </div>
  )
}
