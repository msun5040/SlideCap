import { useEffect, useState } from 'react'
import { Plus, Trash2, Check } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { getApiBase } from '@/api'

interface Rule { match: string; ops: string[] }

interface Props {
  /** JSON-encoded list of rules (Analysis.transforms). Empty string = inherit the kind's defaults. */
  value: string
  onChange: (jsonValue: string) => void
  /** Analysis kind id — scopes the op picker to that plugin's registered ops. */
  kind: string
  /** Optional kind default ruleset; rendered as a placeholder when `value` is empty. */
  defaultRules?: Rule[]
}

export function TransformsEditor({ value, onChange, kind, defaultRules }: Props) {
  const [ops, setOps] = useState<{ name: string; description: string }[]>([])
  const [rules, setRules] = useState<Rule[]>([])
  const [error, setError] = useState<string | null>(null)
  const [opsError, setOpsError] = useState<string | null>(null)

  // Load registry of ops for the selected kind. Refetches when the kind changes
  // so toggling Analysis.kind in the parent form shows the correct op palette.
  useEffect(() => {
    if (!kind) { setOps([]); return }
    let cancelled = false
    setOpsError(null)
    fetch(`${getApiBase()}/transforms/ops?kind=${encodeURIComponent(kind)}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then(data => { if (!cancelled) setOps(data) })
      .catch(e => { if (!cancelled) { setOps([]); setOpsError(e.message || 'Failed to load ops') } })
    return () => { cancelled = true }
  }, [kind])

  // Parse incoming value
  useEffect(() => {
    if (!value) { setRules([]); setError(null); return }
    try {
      const parsed = JSON.parse(value)
      if (Array.isArray(parsed)) {
        setRules(parsed.map((r: any) => ({ match: r?.match || '', ops: Array.isArray(r?.ops) ? r.ops : [] })))
        setError(null)
      } else {
        setRules([])
        setError('Expected a JSON array')
      }
    } catch (e: any) {
      setError(`Invalid JSON: ${e.message}`)
    }
  }, [value])

  const propagate = (next: Rule[]) => {
    setRules(next)
    // Drop empty match rules on serialize
    const cleaned = next.filter(r => r.match.trim())
    onChange(cleaned.length ? JSON.stringify(cleaned) : '')
  }

  const addRule = () => {
    // Seed with the first default match if available, else a sensible blank.
    const seed = defaultRules?.[0]?.match || '*'
    propagate([...rules, { match: seed, ops: [] }])
  }
  const removeRule = (i: number) => propagate(rules.filter((_, j) => j !== i))
  const setRuleMatch = (i: number, match: string) =>
    propagate(rules.map((r, j) => j === i ? { ...r, match } : r))
  const toggleOp = (i: number, op: string) =>
    propagate(rules.map((r, j) => {
      if (j !== i) return r
      const has = r.ops.includes(op)
      return { ...r, ops: has ? r.ops.filter(o => o !== op) : [...r.ops, op] }
    }))

  const usingDefaults = !value && (defaultRules?.length ?? 0) > 0
  const renderRules = usingDefaults ? (defaultRules ?? []) : rules
  const editable = !usingDefaults

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <label className="text-sm font-medium">Read-time Transforms</label>
        <Button size="sm" variant="outline" className="h-7 text-xs" onClick={addRule} type="button">
          <Plus className="h-3 w-3 mr-1" /> {usingDefaults ? 'Override defaults' : 'Add rule'}
        </Button>
      </div>
      <p className="text-xs text-muted-foreground">
        When a result file is fetched with <code className="text-[10px] bg-muted px-1 rounded">apply_transforms=true</code>,
        the first rule whose glob matches drives the bytes through the named ops, in order.
        {usingDefaults && (
          <> Currently inheriting the <span className="font-medium">{kind}</span> kind's default ruleset (shown below, read-only). Click <span className="font-medium">Override defaults</span> to customize.</>
        )}
      </p>

      {error && <p className="text-xs text-red-600">{error}</p>}
      {opsError && <p className="text-xs text-red-600">Failed to load ops for kind "{kind}": {opsError}</p>}

      {renderRules.length === 0 ? (
        <p className="text-xs text-muted-foreground italic px-1 py-2">
          No rules — files served as-is.
        </p>
      ) : (
        <div className="space-y-2">
          {renderRules.map((rule, i) => (
            <div key={i} className={`rounded-md border p-2 space-y-2 ${usingDefaults ? 'bg-muted/40 border-dashed' : 'bg-muted/20'}`}>
              <div className="flex items-center gap-2">
                <span className="text-[10px] uppercase tracking-wide text-muted-foreground shrink-0">Match</span>
                <Input
                  placeholder="*.geojson.snappy"
                  value={rule.match}
                  onChange={(e) => setRuleMatch(i, e.target.value)}
                  className="h-7 text-xs font-mono"
                  disabled={!editable}
                />
                {editable && (
                  <button
                    onClick={() => removeRule(i)}
                    className="p-1 rounded text-muted-foreground hover:text-destructive hover:bg-destructive/10 shrink-0"
                    title="Remove rule"
                    type="button"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </button>
                )}
              </div>

              <div className="space-y-1">
                <span className="text-[10px] uppercase tracking-wide text-muted-foreground">Apply ops in order</span>
                <div className="flex flex-wrap gap-1.5">
                  {ops.length === 0 ? (
                    <span className="text-[11px] text-muted-foreground">No ops registered for this kind.</span>
                  ) : (
                    ops.map((op) => {
                      const selected = rule.ops.includes(op.name)
                      const position = selected ? rule.ops.indexOf(op.name) + 1 : null
                      return (
                        <button
                          key={op.name}
                          type="button"
                          onClick={() => editable && toggleOp(i, op.name)}
                          disabled={!editable}
                          title={op.description}
                          className={`inline-flex items-center gap-1 rounded-sm px-2 py-1 text-[11px] font-mono transition-colors ${
                            selected
                              ? 'border-2 border-black bg-muted'
                              : 'border border-input hover:bg-muted'
                          } ${!editable ? 'cursor-default opacity-80' : ''}`}
                        >
                          {position !== null && (
                            <span className="text-[9px] tabular-nums text-muted-foreground">{position}</span>
                          )}
                          {op.name}
                          {selected && <Check className="h-3 w-3" />}
                        </button>
                      )
                    })
                  )}
                </div>
                {rule.ops.length > 0 && (
                  <p className="text-[10px] text-muted-foreground">
                    Pipeline: {rule.ops.join(' → ')}
                  </p>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
