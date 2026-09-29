import { useEffect, useRef, useState, type ReactNode } from 'react'
import { Loader2, MoreHorizontal, Search, SlidersHorizontal, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { SearchableSelect } from '@/components/ui/searchable-select'

/**
 * The search + filter header used above a slide list.
 *
 * Three tiers, because these controls are not equal: you search constantly,
 * narrow occasionally, and manage the library rarely. Search keeps the width,
 * the filters collapse behind one button that reports how many are on, and
 * anything that isn't a filter goes to the overflow menu.
 *
 * Active filters read back as chips under the bar. That is the point of the
 * whole arrangement: a narrowed list becomes a cohort, so the slice has to be
 * readable — and removable — without opening anything.
 */

export interface FilterOption {
  value: string
  label: string
  color?: string
  hint?: string
}

export interface FilterDef {
  key: string
  /** Field name, shown above the control and as the quiet half of its chip. */
  label: string
  value: string
  /** The value that means "not filtering" — no chip, not counted. */
  inactiveValue: string
  onChange: (value: string) => void
  options?: FilterOption[]
  /** Type-ahead select, for lists long enough to need searching. */
  searchable?: boolean
  /** Supply the control yourself (the scanner filter brings its own). */
  control?: (props: { className: string; onChange: (value: string) => void }) => ReactNode
  /** Chip text; defaults to the chosen option's label, else the raw value. */
  chipLabel?: (value: string) => string
}

interface Props {
  searchTerm: string
  onSearchTermChange: (value: string) => void
  onSearch: () => void
  loading?: boolean
  searchPlaceholder?: string
  filters: FilterDef[]
  /** Runs after a filter change, once the new value has been committed. */
  onFilterChange?: () => void
  /** Runs when the filter popover opens — somewhere to lazily load tag lists. */
  onFiltersOpen?: () => void
  /** Menu items for the overflow. Leave undefined and the button disappears. */
  actions?: ReactNode
  /** 'compact' for narrow side panels. */
  size?: 'default' | 'compact'
}

function chipTextFor(f: FilterDef): string {
  if (f.chipLabel) return f.chipLabel(f.value)
  return f.options?.find(o => o.value === f.value)?.label ?? f.value
}

export function SlideFilterBar({
  searchTerm, onSearchTermChange, onSearch, loading, searchPlaceholder = 'Search...',
  filters, onFilterChange, onFiltersOpen, actions, size = 'default',
}: Props) {
  const compact = size === 'compact'
  const h = compact ? 'h-8' : 'h-10'
  const controlH = compact ? 'h-8 text-xs' : 'h-9'

  // A filter change has to reach the parent's own state before the caller can
  // re-run its search, so the callback fires from an effect rather than inline.
  const [nonce, setNonce] = useState(0)
  const cb = useRef(onFilterChange)
  cb.current = onFilterChange
  useEffect(() => {
    if (nonce > 0) cb.current?.()
  }, [nonce])

  const apply = (f: FilterDef, value: string) => {
    f.onChange(value)
    setNonce(n => n + 1)
  }

  const active = filters.filter(f => f.value !== f.inactiveValue)

  const clearAll = () => {
    filters.forEach(f => { if (f.value !== f.inactiveValue) f.onChange(f.inactiveValue) })
    setNonce(n => n + 1)
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-2">
        <div className={`relative ${h} min-w-[200px] flex-1`}>
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder={searchPlaceholder}
            value={searchTerm}
            onChange={e => onSearchTermChange(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter') onSearch() }}
            className={`${h} pl-10`}
          />
        </div>

        <Button onClick={onSearch} disabled={loading} className={h} size={compact ? 'sm' : 'default'}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Search'}
        </Button>

        {filters.length > 0 && (
          <Popover onOpenChange={open => { if (open) onFiltersOpen?.() }}>
            <PopoverTrigger asChild>
              <Button variant="outline" className={h} size={compact ? 'sm' : 'default'}>
                <SlidersHorizontal className={compact ? 'mr-1.5 h-3.5 w-3.5' : 'mr-2 h-4 w-4'} />
                Filters
                {active.length > 0 && (
                  <span className="ml-2 rounded-sm bg-primary px-1.5 py-0.5 text-[11px] font-medium text-primary-foreground">
                    {active.length}
                  </span>
                )}
              </Button>
            </PopoverTrigger>
            <PopoverContent align="end" className="w-[30rem] max-w-[calc(100vw-2rem)] p-4">
              <div className="grid grid-cols-2 gap-3">
                {filters.map(f => (
                  <label key={f.key} className="flex flex-col gap-1.5">
                    <span className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                      {f.label}
                    </span>
                    {f.control
                      ? f.control({ className: `${controlH} w-full`, onChange: v => apply(f, v) })
                      : f.searchable
                        ? (
                          <SearchableSelect
                            className={`${controlH} w-full`}
                            value={f.value}
                            onChange={v => apply(f, v)}
                            searchPlaceholder={`Search ${f.label.toLowerCase()}...`}
                            emptyText={`No ${f.label.toLowerCase()} to choose from`}
                            options={f.options ?? []}
                          />
                        ) : (
                          <Select value={f.value} onValueChange={v => apply(f, v)}>
                            <SelectTrigger className={`${controlH} w-full`}><SelectValue /></SelectTrigger>
                            <SelectContent>
                              {(f.options ?? []).map(o => (
                                <SelectItem key={o.value} value={o.value}>
                                  <div className="flex items-center gap-2">
                                    {o.color && (
                                      <span className="h-2 w-2 rounded-full" style={{ backgroundColor: o.color }} />
                                    )}
                                    {o.label}
                                    {o.hint && <span className="text-xs text-muted-foreground">{o.hint}</span>}
                                  </div>
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        )}
                  </label>
                ))}
              </div>

              {active.length > 0 && (
                <button type="button" onClick={clearAll}
                        className="mt-4 text-xs text-muted-foreground underline-offset-2 hover:underline">
                  Clear all filters
                </button>
              )}
            </PopoverContent>
          </Popover>
        )}

        {actions && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="icon" className={compact ? 'h-8 w-8' : 'h-10 w-10'}
                      title="More actions">
                <MoreHorizontal className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">{actions}</DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>

      {active.length > 0 && (
        <div className="flex flex-wrap items-center gap-1.5">
          {active.map(f => (
            <button
              key={f.key}
              type="button"
              onClick={() => apply(f, f.inactiveValue)}
              title={`Remove ${f.label.toLowerCase()} filter`}
              className="group inline-flex items-center gap-1.5 rounded-full border bg-muted/40 py-1 pl-2.5 pr-1.5
                         text-xs text-foreground transition-colors hover:bg-muted"
            >
              <span className="text-muted-foreground">{f.label}</span>
              <span className="font-medium">{chipTextFor(f)}</span>
              <X className="h-3 w-3 text-muted-foreground group-hover:text-foreground" />
            </button>
          ))}
          <button type="button" onClick={clearAll}
                  className="ml-1 text-xs text-muted-foreground underline-offset-2 hover:underline">
            Clear all
          </button>
        </div>
      )}
    </div>
  )
}
