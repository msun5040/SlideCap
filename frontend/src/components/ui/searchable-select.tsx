import { useEffect, useMemo, useRef, useState } from 'react'
import { Check, ChevronsUpDown, Search } from 'lucide-react'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { cn } from '@/lib/utils'

export interface SearchableOption {
  value: string
  label: string
  /** Optional swatch (tag colours) shown before the label. */
  color?: string
  /** Optional right-aligned muted text, e.g. a slide count. */
  hint?: string
}

interface SearchableSelectProps {
  options: SearchableOption[]
  value: string
  onChange: (value: string) => void
  placeholder?: string
  searchPlaceholder?: string
  emptyText?: string
  disabled?: boolean
  className?: string
  /** Width of the dropdown panel; defaults to matching the trigger. */
  contentClassName?: string
}

/**
 * A select you can type into — for lists long enough that scanning a plain
 * Select is a chore (every tag, every cohort, every stain in the library).
 *
 * Deliberately not a free-text field: the value always comes from the list, so
 * you can't miss by a character and silently target nothing.
 */
export function SearchableSelect({
  options,
  value,
  onChange,
  placeholder = 'Select…',
  searchPlaceholder = 'Search…',
  emptyText = 'No matches',
  disabled,
  className = '',
  contentClassName = '',
}: SearchableSelectProps) {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const [highlight, setHighlight] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLDivElement>(null)

  const selected = options.find(o => o.value === value)

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return options
    return options.filter(o => o.label.toLowerCase().includes(q))
  }, [options, query])

  // Reset the search each time it opens — a stale query hiding every option is
  // a confusing way to reopen a picker.
  useEffect(() => {
    if (open) {
      setQuery('')
      setHighlight(0)
      // Radix moves focus to the content; wait a tick before claiming it.
      const t = setTimeout(() => inputRef.current?.focus(), 0)
      return () => clearTimeout(t)
    }
  }, [open])

  useEffect(() => setHighlight(0), [query])

  // Keep the highlighted row in view while arrowing through a long list.
  useEffect(() => {
    listRef.current?.querySelector('[data-highlighted="true"]')
      ?.scrollIntoView({ block: 'nearest' })
  }, [highlight])

  const commit = (v: string) => {
    onChange(v)
    setOpen(false)
  }

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setHighlight(h => Math.min(h + 1, filtered.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setHighlight(h => Math.max(h - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      const opt = filtered[highlight]
      if (opt) commit(opt.value)
    }
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          disabled={disabled}
          className={cn(
            'flex items-center justify-between gap-2 rounded-md border border-input bg-background',
            'px-3 py-2 text-sm ring-offset-background disabled:cursor-not-allowed disabled:opacity-50',
            'focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2',
            className,
          )}
        >
          <span className={cn('flex items-center gap-2 truncate', !selected && 'text-muted-foreground')}>
            {selected?.color && (
              <span className="h-2 w-2 shrink-0 rounded-[2px]" style={{ backgroundColor: selected.color }} />
            )}
            <span className="truncate">{selected?.label ?? placeholder}</span>
          </span>
          <ChevronsUpDown className="h-3.5 w-3.5 shrink-0 opacity-50" />
        </button>
      </PopoverTrigger>

      <PopoverContent
        className={cn('w-[var(--radix-popover-trigger-width)] p-0', contentClassName)}
        onOpenAutoFocus={e => e.preventDefault()}
      >
        <div className="flex items-center gap-2 border-b px-2.5">
          <Search className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
          <input
            ref={inputRef}
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder={searchPlaceholder}
            className="h-9 w-full bg-transparent text-sm outline-none placeholder:text-muted-foreground"
          />
        </div>

        <div ref={listRef} className="max-h-64 overflow-y-auto p-1">
          {filtered.length === 0 ? (
            <p className="px-2 py-4 text-center text-[13px] text-muted-foreground">{emptyText}</p>
          ) : (
            filtered.map((opt, i) => (
              <button
                key={opt.value}
                type="button"
                data-highlighted={i === highlight}
                onMouseEnter={() => setHighlight(i)}
                onClick={() => commit(opt.value)}
                className={cn(
                  'flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-left text-[13px]',
                  i === highlight && 'bg-accent text-accent-foreground',
                )}
              >
                <Check className={cn('h-3.5 w-3.5 shrink-0', opt.value === value ? 'opacity-100' : 'opacity-0')} />
                {opt.color && (
                  <span className="h-2 w-2 shrink-0 rounded-[2px]" style={{ backgroundColor: opt.color }} />
                )}
                <span className="truncate">{opt.label}</span>
                {opt.hint && <span className="ml-auto shrink-0 text-[11px] text-muted-foreground">{opt.hint}</span>}
              </button>
            ))
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}
