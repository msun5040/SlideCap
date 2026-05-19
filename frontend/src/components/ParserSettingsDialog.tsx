import { useCallback, useEffect, useState } from 'react'
import { Code, CheckCircle2, XCircle, Loader2 } from 'lucide-react'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { getApiBase } from '@/api'

type Pattern = { name: string; description: string; regex: string }

type ParserTestResult = {
  filename: string
  matched: boolean
  matched_pattern: string | null
  parsed: null | {
    accession: string
    block_id: string
    slide_number: string
    stain_type: string
    random_id: string
    year: number
    full_stem: string
  }
  attempts: { name: string; matched: boolean; groups: Record<string, string | null> | null }[]
}

interface Props {
  open: boolean
  onOpenChange: (v: boolean) => void
}

export function ParserSettingsDialog({ open, onOpenChange }: Props) {
  const [patterns, setPatterns] = useState<Pattern[]>([])
  const [loadingPatterns, setLoadingPatterns] = useState(false)
  const [testInput, setTestInput] = useState('')
  const [testResult, setTestResult] = useState<ParserTestResult | null>(null)
  const [testing, setTesting] = useState(false)
  const [testError, setTestError] = useState<string | null>(null)

  const loadPatterns = useCallback(async () => {
    setLoadingPatterns(true)
    try {
      const res = await fetch(`${getApiBase()}/parser/patterns`)
      if (res.ok) setPatterns(await res.json())
    } catch (e) { console.error('Failed to load parser patterns:', e) }
    finally { setLoadingPatterns(false) }
  }, [])

  useEffect(() => {
    if (open) {
      loadPatterns()
      setTestResult(null)
      setTestError(null)
    }
  }, [open, loadPatterns])

  const runTest = useCallback(async () => {
    const filename = testInput.trim()
    if (!filename) return
    setTesting(true)
    setTestError(null)
    try {
      const res = await fetch(`${getApiBase()}/parser/test`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }))
        throw new Error(err.detail || `HTTP ${res.status}`)
      }
      setTestResult(await res.json())
    } catch (e: any) {
      setTestError(e.message || 'Test failed')
      setTestResult(null)
    } finally {
      setTesting(false)
    }
  }, [testInput])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Parser configuration</DialogTitle>
          <DialogDescription>
            Read-only view of the filename patterns currently in use. Set <code className="text-[11px] bg-muted px-1 rounded">PARSER_PATTERNS</code> in the backend env to change them. Patterns must include named groups: <code className="text-[11px] bg-muted px-1 rounded">accession</code>, <code className="text-[11px] bg-muted px-1 rounded">year</code>, optional <code className="text-[11px] bg-muted px-1 rounded">block</code> / <code className="text-[11px] bg-muted px-1 rounded">slide</code> / <code className="text-[11px] bg-muted px-1 rounded">stain</code> / <code className="text-[11px] bg-muted px-1 rounded">random</code>.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {/* Configured patterns */}
          <section className="space-y-2">
            <h3 className="text-sm font-medium flex items-center gap-2">
              <Code className="h-4 w-4 text-muted-foreground" />
              Active patterns
              <Badge variant="secondary" className="text-[10px]">{patterns.length}</Badge>
            </h3>
            {loadingPatterns ? (
              <div className="text-xs text-muted-foreground flex items-center gap-2">
                <Loader2 className="h-3 w-3 animate-spin" /> Loading…
              </div>
            ) : patterns.length === 0 ? (
              <p className="text-xs text-muted-foreground">No patterns configured.</p>
            ) : (
              <div className="space-y-2">
                {patterns.map((p, i) => (
                  <div key={i} className="rounded-md border p-3 space-y-1.5">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-semibold">{p.name}</span>
                      <span className="text-[10px] text-muted-foreground">#{i + 1}</span>
                    </div>
                    {p.description && (
                      <p className="text-[11px] text-muted-foreground">{p.description}</p>
                    )}
                    <pre className="text-[10px] font-mono bg-muted/50 rounded px-2 py-1.5 overflow-x-auto whitespace-pre-wrap break-all">
                      {p.regex}
                    </pre>
                  </div>
                ))}
              </div>
            )}
          </section>

          {/* Test box */}
          <section className="space-y-2 border-t pt-4">
            <h3 className="text-sm font-medium">Test a filename</h3>
            <p className="text-xs text-muted-foreground">
              Paste a sample slide filename and see which pattern matches and what gets extracted.
            </p>
            <div className="flex gap-2">
              <Input
                placeholder="e.g. BS25-12345_A1-2_HE_abc123.svs"
                value={testInput}
                onChange={(e) => setTestInput(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter') runTest() }}
                className="font-mono text-sm"
              />
              <Button onClick={runTest} disabled={!testInput.trim() || testing} size="sm">
                {testing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : 'Test'}
              </Button>
            </div>

            {testError && (
              <p className="text-xs text-red-600">{testError}</p>
            )}

            {testResult && (
              <div className="space-y-2.5 pt-1">
                {/* Verdict */}
                <div className={`rounded-md border p-2.5 flex items-start gap-2 ${
                  testResult.matched ? 'border-emerald-300 bg-emerald-50' : 'border-rose-300 bg-rose-50'
                }`}>
                  {testResult.matched ? (
                    <CheckCircle2 className="h-4 w-4 text-emerald-600 shrink-0 mt-0.5" />
                  ) : (
                    <XCircle className="h-4 w-4 text-rose-600 shrink-0 mt-0.5" />
                  )}
                  <div className="text-xs">
                    {testResult.matched ? (
                      <>
                        Matched pattern <span className="font-semibold">{testResult.matched_pattern}</span>
                      </>
                    ) : (
                      <>No configured pattern matched this filename.</>
                    )}
                  </div>
                </div>

                {/* Extracted fields */}
                {testResult.parsed && (
                  <div className="rounded-md border bg-muted/30 p-2.5 grid grid-cols-2 gap-x-4 gap-y-1.5 text-[12px] font-mono">
                    {(['accession', 'year', 'block_id', 'slide_number', 'stain_type', 'random_id', 'full_stem'] as const).map((k) => (
                      <div key={k} className="flex items-baseline gap-2 min-w-0">
                        <span className="text-muted-foreground text-[10px] uppercase tracking-wide shrink-0">{k.replace('_', ' ')}</span>
                        <span className="truncate">{String((testResult.parsed as any)[k] ?? '—') || <span className="text-muted-foreground">—</span>}</span>
                      </div>
                    ))}
                  </div>
                )}

                {/* Per-pattern attempts */}
                {testResult.attempts.length > 1 && (
                  <div className="space-y-1">
                    <p className="text-[11px] text-muted-foreground">Per-pattern results:</p>
                    {testResult.attempts.map((a, i) => (
                      <div key={i} className="flex items-center gap-2 text-[11px]">
                        {a.matched ? (
                          <CheckCircle2 className="h-3 w-3 text-emerald-600" />
                        ) : (
                          <XCircle className="h-3 w-3 text-rose-400" />
                        )}
                        <span className={a.matched ? 'font-medium' : 'text-muted-foreground'}>{a.name}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </section>
        </div>
      </DialogContent>
    </Dialog>
  )
}
