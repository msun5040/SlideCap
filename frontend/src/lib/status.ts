// SlideCap — single source of truth for status presentation (Direction A).
// Replaces the scattered candy-pill class maps in Dashboard.tsx, SlideLibrary.tsx,
// RequestTracker.tsx and AnalysisResults.tsx. Status is a colored DOT + ink label
// by default; pass tint where a soft-filled chip is wanted (banners, headers).

export type StatusTone = 'success' | 'info' | 'warning' | 'danger' | 'neutral'

export interface ToneStyle {
  /** strong color — the dot / icon */
  dot: string
  /** text color when using the soft-tint form */
  ink: string
  /** soft background for the tint form */
  soft: string
}

export const TONE: Record<StatusTone, ToneStyle> = {
  success: { dot: 'var(--success)', ink: 'var(--success-ink)', soft: 'var(--success-soft)' },
  info:    { dot: 'var(--info)',    ink: 'var(--info-ink)',    soft: 'var(--info-soft)' },
  warning: { dot: 'var(--warning)', ink: 'var(--warning-ink)', soft: 'var(--warning-soft)' },
  danger:  { dot: 'var(--danger)',  ink: 'var(--danger-ink)',  soft: 'var(--danger-soft)' },
  neutral: { dot: 'var(--neutral-st)', ink: 'var(--neutral-st-ink)', soft: 'var(--neutral-st-soft)' },
}

// Map every status string the app uses → a semantic tone. Extend as needed.
const STATUS_TONE: Record<string, StatusTone> = {
  // slides / jobs
  available: 'success', completed: 'success', connected: 'success', 'qc pass': 'success',
  'in-analysis': 'warning', running: 'warning', pending: 'warning', partial: 'warning',
  transferring: 'info', staging: 'info', scanned: 'info', 'slides requested': 'info',
  queued: 'info', submitted: 'info',
  failed: 'danger', error: 'danger', missing: 'danger', offline: 'danger',
  archived: 'neutral', 'not started': 'neutral',
  // request tracker
  complete: 'success', 'slides received': 'success',
  'recut blocks requested': 'info',
  'no blocks/slides': 'danger',
}

export function toneFor(status?: string): StatusTone {
  if (!status) return 'neutral'
  return STATUS_TONE[status.toLowerCase()] ?? 'neutral'
}

export function styleFor(status?: string): ToneStyle {
  return TONE[toneFor(status)]
}
