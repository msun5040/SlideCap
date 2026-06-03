import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react'
import type { Slide } from '@/types/slide'
import { SlideDetailsDialog } from '@/components/SlideDetailsDialog'

interface SlideDetailsContextValue {
  /**
   * Open the global slide-details dialog. Pass a hash, or pass a partial
   * Slide so the header renders instantly while the rest streams in.
   */
  openSlideDetails: (target: string | (Partial<Slide> & { slide_hash: string })) => void
  closeSlideDetails: () => void
}

const SlideDetailsContext = createContext<SlideDetailsContextValue | null>(null)

/**
 * Mount once at the app root. Exposes `useSlideDetails()` so any component
 * can open the details dialog for a slide_hash without owning state for it.
 */
export function SlideDetailsProvider({ children }: { children: ReactNode }) {
  const [slideHash, setSlideHash] = useState<string | null>(null)
  const [seed, setSeed] = useState<Partial<Slide> | null>(null)

  const openSlideDetails = useCallback((target: string | (Partial<Slide> & { slide_hash: string })) => {
    if (typeof target === 'string') {
      setSlideHash(target)
      setSeed(null)
    } else {
      setSlideHash(target.slide_hash)
      setSeed(target)
    }
  }, [])

  const closeSlideDetails = useCallback(() => {
    setSlideHash(null)
    setSeed(null)
  }, [])

  const value = useMemo(
    () => ({ openSlideDetails, closeSlideDetails }),
    [openSlideDetails, closeSlideDetails],
  )

  return (
    <SlideDetailsContext.Provider value={value}>
      {children}
      <SlideDetailsDialog slideHash={slideHash} seed={seed} onClose={closeSlideDetails} />
    </SlideDetailsContext.Provider>
  )
}

export function useSlideDetails(): SlideDetailsContextValue {
  const ctx = useContext(SlideDetailsContext)
  if (!ctx) {
    throw new Error('useSlideDetails must be used inside <SlideDetailsProvider>')
  }
  return ctx
}
