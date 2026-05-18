import { isDemo } from '@/api'

interface SlideLike {
  slide_id?: string | null
  slide_hash?: string | null
  accession_number?: string | null
  block_id?: string | null
  stain_type?: string | null
  filename?: string | null
}

interface CaseLike {
  case_id?: string | null
  case_hash?: string | null
  accession_number?: string | null
}

/**
 * Display identifier for a slide. In demo mode, returns the SlideCap ID
 * (e.g. SL00001) instead of the accession number, falling back to the
 * slide_hash. In prod, returns the accession with block/stain detail.
 *
 * Single source of truth for PHI redaction at the display layer.
 */
export function displaySlide(s: SlideLike): string {
  if (isDemo()) {
    return s.slide_id || (s.slide_hash ? s.slide_hash.slice(0, 12) + '…' : '—')
  }
  if (s.accession_number) {
    const parts = [s.accession_number]
    if (s.block_id) parts.push(s.block_id)
    if (s.stain_type) parts.push(s.stain_type)
    return parts.join(' ')
  }
  if (s.filename) return s.filename
  return s.slide_hash ? s.slide_hash.slice(0, 12) + '…' : '—'
}

/**
 * Short form of the slide identifier (no block/stain). Use in tight columns.
 */
export function displaySlideShort(s: SlideLike): string {
  if (isDemo()) {
    return s.slide_id || (s.slide_hash ? s.slide_hash.slice(0, 10) + '…' : '—')
  }
  return s.accession_number || (s.slide_hash ? s.slide_hash.slice(0, 10) + '…' : '—')
}

/**
 * Display identifier for a case (group of slides).
 */
export function displayCase(c: CaseLike): string {
  if (isDemo()) {
    return c.case_id || (c.case_hash ? c.case_hash.slice(0, 10) + '…' : '—')
  }
  return c.accession_number || (c.case_hash ? c.case_hash.slice(0, 10) + '…' : '—')
}
