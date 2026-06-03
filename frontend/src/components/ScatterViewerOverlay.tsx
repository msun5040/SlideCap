import { useEffect, useState } from 'react'
import { getApiBase } from '@/api'
import { SlideViewerOSD } from '@/components/SlideViewerOSD'
import { EmbeddingScatter, type ScatterPayload, type ScatterPoint } from '@/components/EmbeddingScatter'

interface Props {
  jobId: number
  slideHash: string
  slideName: string
  rendererId: string
  rendererName: string
  onClose: () => void
}

/**
 * Full-screen split view: WSI viewer on the left, embedding scatter on the
 * right. Hovering a scatter point highlights the corresponding patch on the
 * slide via SlideViewerOSD's `highlightPatch` prop. Self-fetches the
 * projection from `/results/{job_id}/render/{renderer_id}`.
 *
 * Used from anywhere that exposes a renderer button — the Analysis Results
 * file tree, the Slide Details dialog, future per-cohort dashboards. Mount
 * it conditionally; it occupies the full viewport while open.
 */
export function ScatterViewerOverlay({
  jobId, slideHash, slideName, rendererId, rendererName, onClose,
}: Props) {
  const [data, setData] = useState<ScatterPayload | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [highlightPatch, setHighlightPatch] = useState<{ slide_x: number; slide_y: number; size: number } | null>(null)
  // Bidirectional highlight: when the user clicks on the slide, we find the
  // patch under the click and tell the scatter to light up that point too.
  // Driven by `onImageClick` from SlideViewerOSD.
  const [clickedIdx, setClickedIdx] = useState<number | null>(null)

  useEffect(() => {
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    setData(null)
    fetch(
      `${getApiBase()}/results/${jobId}/render/${rendererId}?slide_hash=${encodeURIComponent(slideHash)}`,
      { signal: ac.signal },
    )
      .then(async r => {
        if (!r.ok) {
          const detail = await r.json().catch(() => ({}))
          throw new Error(detail.detail || `HTTP ${r.status}`)
        }
        return r.json()
      })
      .then((wrapper: { data: ScatterPayload }) => {
        setData(wrapper.data)
        setLoading(false)
      })
      .catch((e: any) => {
        if (e.name === 'AbortError') return
        setError(e.message || 'Failed to load projection')
        setLoading(false)
      })
    return () => ac.abort()
  }, [jobId, slideHash, rendererId])

  // Slide-click handler: find the patch whose box contains the click point.
  // Linear scan — fine up to a few thousand patches; the OSD viewer can't
  // resolve cells finer than that anyway. Sets both the slide highlight box
  // *and* the scatter point so the linkage is visible on both sides.
  const onSlideClick = (image_x: number, image_y: number) => {
    if (!data) return
    const size = data.patch_size
    let bestIdx = -1
    let bestDist = Infinity
    for (let i = 0; i < data.points.length; i++) {
      const p = data.points[i]
      // Patch box is [slide_x, slide_x+size] × [slide_y, slide_y+size].
      // Check containment first (cheap); fall back to nearest-center if no
      // patch covers the click (e.g. clicked between two non-overlapping
      // patches — pick the closest).
      const inside =
        image_x >= p.slide_x && image_x < p.slide_x + size &&
        image_y >= p.slide_y && image_y < p.slide_y + size
      if (inside) { bestIdx = i; break }
      const cx = p.slide_x + size / 2
      const cy = p.slide_y + size / 2
      const dx = cx - image_x
      const dy = cy - image_y
      const d = dx * dx + dy * dy
      if (d < bestDist) { bestDist = d; bestIdx = i }
    }
    if (bestIdx < 0) return
    const p = data.points[bestIdx]
    setClickedIdx(bestIdx)
    setHighlightPatch({ slide_x: p.slide_x, slide_y: p.slide_y, size })
  }

  return (
    <div className="fixed inset-0 z-100 bg-black flex">
      <div className="flex-1 relative">
        {/* embedded=true makes SlideViewerOSD size to this flex region
            (absolute inset-0) instead of taking over the full viewport
            (fixed inset-0). Without this OSD computes fit-to-screen based
            on the full screen width and the slide overflows under the
            scatter panel on the right. */}
        <SlideViewerOSD
          slideHash={slideHash}
          slideName={slideName}
          highlightPatch={highlightPatch}
          onImageClick={onSlideClick}
          embedded
          onClose={onClose}
        />
      </div>
      <div className="w-96 shrink-0 z-101">
        <EmbeddingScatter
          data={data}
          loading={loading}
          error={error}
          title={`${rendererName} · ${slideName}`}
          externalHighlightIdx={clickedIdx}
          onHoverPoint={(p: ScatterPoint | null) => {
            if (!p || !data) {
              setHighlightPatch(null)
              // Don't clear clickedIdx on hover-out — a slide-pinned highlight
              // should persist until the user clicks somewhere else.
              return
            }
            setHighlightPatch({ slide_x: p.slide_x, slide_y: p.slide_y, size: data.patch_size })
            // Hovering a scatter point overrides any slide-pinned click.
            setClickedIdx(null)
          }}
          onSelectPoint={(p: ScatterPoint) => {
            if (!data) return
            setHighlightPatch({ slide_x: p.slide_x, slide_y: p.slide_y, size: data.patch_size })
            setClickedIdx(p.idx)
          }}
          onClose={onClose}
        />
      </div>
    </div>
  )
}
