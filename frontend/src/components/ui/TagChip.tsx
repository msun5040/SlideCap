import * as React from 'react'

interface TagChipProps {
  name: string
  color?: string
  /** High-emphasis filled chip (white text on tag color). Default is the quiet hairline form. */
  solid?: boolean
  /** Optional trailing control (e.g. a remove ✕ button). */
  onRemove?: () => void
  className?: string
}

/**
 * SlideCap tag chip (Direction A).
 * Default: a small squared color swatch + ink label inside a 1px hairline-bordered,
 * rounded-sm chip. `solid` renders the filled form for genuine high emphasis only.
 */
export function TagChip({ name, color, solid = false, onRemove, className = '' }: TagChipProps) {
  if (solid) {
    return (
      <span
        className={`inline-flex items-center gap-1.5 rounded-sm px-1.5 py-0.5 text-[11px] font-medium text-white ${className}`}
        style={{ background: color || 'var(--neutral-st)' }}
      >
        {name}
        {onRemove && <RemoveButton onRemove={onRemove} tone="light" />}
      </span>
    )
  }
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-sm border border-border bg-card px-1.5 py-0.5 text-[11px] font-medium text-foreground ${className}`}
    >
      <span className="h-2 w-2 rounded-[2px] shrink-0" style={{ background: color || 'var(--neutral-st)' }} />
      {name}
      {onRemove && <RemoveButton onRemove={onRemove} tone="dark" />}
    </span>
  )
}

function RemoveButton({ onRemove, tone }: { onRemove: () => void; tone: 'light' | 'dark' }) {
  return (
    <button
      onClick={(e) => { e.stopPropagation(); onRemove() }}
      className={`ml-0.5 rounded-[2px] p-0.5 leading-none ${tone === 'light' ? 'hover:bg-white/25' : 'hover:bg-destructive/20'}`}
      aria-label={`Remove tag`}
    >
      <svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round">
        <path d="M18 6 6 18M6 6l12 12" />
      </svg>
    </button>
  )
}
