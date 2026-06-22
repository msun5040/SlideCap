import * as React from 'react'
import { styleFor } from '@/lib/status'

interface StatusBadgeProps {
  status: string
  /** Override the displayed text (defaults to the status string). */
  label?: string
  /** Leading lucide icon (12px) instead of the dot. */
  icon?: React.ReactNode
  /** Emphasized soft-tint chip instead of the quiet dot + label. */
  tint?: boolean
  className?: string
}

/**
 * SlideCap status indicator (Direction A).
 * Default: colored dot + ink label — quiet, scans cleanly down a table column.
 * `tint`: soft background + ink text, for banners / detail headers.
 */
export function StatusBadge({ status, label, icon, tint = false, className = '' }: StatusBadgeProps) {
  const s = styleFor(status)
  const text = label ?? status
  if (tint) {
    return (
      <span
        className={`inline-flex items-center gap-1.5 rounded-sm px-2 py-0.5 text-xs font-semibold ${className}`}
        style={{ background: s.soft, color: s.ink }}
      >
        {icon ?? <span className="inline-block h-[7px] w-[7px] rounded-full" style={{ background: s.dot }} />}
        {text}
      </span>
    )
  }
  return (
    <span className={`inline-flex items-center gap-1.5 text-xs font-medium text-foreground ${className}`}>
      {icon
        ? <span style={{ color: s.dot, display: 'inline-flex' }}>{icon}</span>
        : <span className="inline-block h-[7px] w-[7px] rounded-full shrink-0" style={{ background: s.dot }} />}
      {text}
    </span>
  )
}
