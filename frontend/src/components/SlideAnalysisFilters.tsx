import { useEffect, useMemo, useState } from 'react'
import { Filter, FlaskConical, XCircle } from 'lucide-react'
import { getApiBase } from '@/api'

/** One completed analysis run against a slide. */
export interface SlideAnalysisEntry {
  job_id: number
  analysis_name: string
  version?: string
  analysis_kind?: string | null
  status?: string
  completed_at?: string | null
}

export type SlideAnalysesMap = Record<string, SlideAnalysisEntry[]>

/** Anything with a hash and a stain is filterable — Slide, tag slide, study slide. */
export interface FilterableSlide {
  slide_hash: string
  stain_type?: string | null
}

/** Filter token meaning "nothing has been run on this slide yet". */
export const UNANALYZED = '__unanalyzed__'

/**
 * Completed analyses for a set of slides, keyed by hash.
 *
 * Only the cohort endpoint returns analysis history inline; slides reached by
 * search, tag or study don't carry it. /slides/pull-analyses answers for any
 * set of hashes, so every selection mode can show the same information.
 */
export function useSlideAnalyses(hashes: string[]): { analyses: SlideAnalysesMap; loading: boolean } {
  const [analyses, setAnalyses] = useState<SlideAnalysesMap>({})
  const [loading, setLoading] = useState(false)

  // Key on the sorted hash list so re-renders with the same slides don't refetch.
  const key = useMemo(() => [...hashes].sort().join(','), [hashes])

  useEffect(() => {
    if (!key) {
      setAnalyses({})
      return
    }
    let cancelled = false
    setLoading(true)
    fetch(`${getApiBase()}/slides/pull-analyses`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ slide_hashes: key.split(',') }),
    })
      .then(res => (res.ok ? res.json() : null))
      .then(data => { if (!cancelled && data) setAnalyses(data.results || {}) })
      .catch(e => console.error('Failed to load slide analyses:', e))
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [key])

  return { analyses, loading }
}

/** Names of completed analyses on a slide. */
export function completedNames(analyses: SlideAnalysesMap, hash: string): string[] {
  return Array.from(new Set((analyses[hash] || []).map(e => e.analysis_name)))
}

/** The badges shown in a slide row's "Analyses" cell. */
export function AnalysisBadges({ names }: { names: string[] }) {
  if (names.length === 0) {
    return <span className="text-[11px] text-muted-foreground">—</span>
  }
  return (
    <div className="flex flex-wrap gap-1">
      {names.map(name => (
        <span
          key={name}
          title={name}
          className="inline-flex items-center gap-1 rounded border border-violet-300 bg-violet-50 px-1.5 py-0.5 text-[10px] font-medium text-violet-700"
        >
          <FlaskConical className="h-2.5 w-2.5" />
          {name.length > 16 ? name.slice(0, 16) + '…' : name}
        </span>
      ))}
    </div>
  )
}

interface FilterBarProps {
  slides: FilterableSlide[]
  analyses: SlideAnalysesMap
  stainFilter: Set<string>
  analysisFilter: Set<string>
  onToggleStain: (stain: string) => void
  onToggleAnalysis: (name: string) => void
  onClear: () => void
  /** Shown on the right, e.g. "12 of 40 shown". */
  summary?: string
}

/**
 * Stain + prior-analysis filters for a slide list.
 *
 * Both option lists are derived from the slides actually in front of you rather
 * than a fixed vocabulary, so there's never a chip that matches nothing (the
 * old hardcoded lists still offered "HE" long after the site moved to "HNE").
 */
export function SlideFilterBar({
  slides, analyses, stainFilter, analysisFilter,
  onToggleStain, onToggleAnalysis, onClear, summary,
}: FilterBarProps) {
  const stainOptions = useMemo(() => {
    const set = new Set<string>()
    for (const s of slides) {
      const v = (s.stain_type || '').trim()
      if (v) set.add(v)
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b))
  }, [slides])

  const analysisOptions = useMemo(() => {
    const set = new Set<string>()
    for (const s of slides) for (const n of completedNames(analyses, s.slide_hash)) set.add(n)
    return Array.from(set).sort()
  }, [slides, analyses])

  const active = stainFilter.size > 0 || analysisFilter.size > 0
  if (stainOptions.length === 0 && analysisOptions.length === 0) return null

  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-2 rounded-md border bg-muted/20 px-3 py-2">
      <div className="flex shrink-0 items-center gap-1.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
        <Filter className="h-3.5 w-3.5" />Filter
      </div>

      {stainOptions.length > 0 && (
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-[11px] text-muted-foreground">Stain</span>
          {stainOptions.map(st => (
            <button
              key={st}
              type="button"
              onClick={() => onToggleStain(st)}
              className={`rounded border px-2 py-0.5 text-[11px] transition-colors ${
                stainFilter.has(st)
                  ? 'border-rose-300 bg-rose-50 text-rose-700'
                  : 'border-input bg-background text-muted-foreground hover:bg-muted/30'
              }`}
            >
              {st}
            </button>
          ))}
        </div>
      )}

      <div className="flex flex-wrap items-center gap-1.5">
        <span className="text-[11px] text-muted-foreground">Analysis</span>
        {analysisOptions.map(name => (
          <button
            key={name}
            type="button"
            onClick={() => onToggleAnalysis(name)}
            className={`inline-flex items-center gap-1 rounded border px-2 py-0.5 text-[11px] transition-colors ${
              analysisFilter.has(name)
                ? 'border-violet-300 bg-violet-50 text-violet-700'
                : 'border-input bg-background text-muted-foreground hover:bg-muted/30'
            }`}
          >
            <FlaskConical className="h-3 w-3" />{name}
          </button>
        ))}
        <button
          type="button"
          onClick={() => onToggleAnalysis(UNANALYZED)}
          title="Slides with no completed analyses yet"
          className={`rounded border px-2 py-0.5 text-[11px] transition-colors ${
            analysisFilter.has(UNANALYZED)
              ? 'border-emerald-300 bg-emerald-50 text-emerald-700'
              : 'border-input bg-background text-muted-foreground hover:bg-muted/30'
          }`}
        >
          Unanalyzed
        </button>
      </div>

      {summary && <span className="ml-auto text-[11px] text-muted-foreground">{summary}</span>}
      {active && (
        <button
          type="button"
          onClick={onClear}
          className={`inline-flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground ${summary ? '' : 'ml-auto'}`}
        >
          <XCircle className="h-3 w-3" />Clear filters
        </button>
      )}
    </div>
  )
}

/** Does this slide pass the stain + analysis filters? Empty filter = everything. */
export function slidePasses(
  slide: FilterableSlide,
  analyses: SlideAnalysesMap,
  stainFilter: Set<string>,
  analysisFilter: Set<string>,
): boolean {
  if (stainFilter.size && !stainFilter.has((slide.stain_type || '').trim())) return false
  if (analysisFilter.size) {
    const done = completedNames(analyses, slide.slide_hash)
    for (const tok of analysisFilter) {
      if (tok === UNANALYZED ? done.length === 0 : done.includes(tok)) return true
    }
    return false
  }
  return true
}

/** Convenience: the toggle-a-token-in-a-Set updater these filters all use. */
export function toggleInSet(set: Set<string>, token: string): Set<string> {
  const next = new Set(set)
  if (next.has(token)) next.delete(token)
  else next.add(token)
  return next
}
