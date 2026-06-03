import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Loader2, X } from 'lucide-react'
import { Button } from '@/components/ui/button'

export interface ScatterPoint {
  idx: number
  x: number
  y: number
  slide_x: number
  slide_y: number
}

export interface ScatterPayload {
  method: string
  n: number
  patch_size: number
  patch_level: number
  stem: string
  points: ScatterPoint[]
}

interface Props {
  /** Payload from `/results/{job_id}/render/{renderer_id}`. Pass null while loading. */
  data: ScatterPayload | null
  loading?: boolean
  error?: string | null
  title: string
  /** Called when the cursor is over a scatter point. Pass null on mouse-leave. */
  onHoverPoint?: (point: ScatterPoint | null) => void
  /** Called when a point is clicked. */
  onSelectPoint?: (point: ScatterPoint) => void
  /** Index of a point to externally highlight (e.g. from a slide-viewer click).
   *  Drawn with the same red ring as the hover highlight, but persists until
   *  the parent clears it. */
  externalHighlightIdx?: number | null
  onClose: () => void
}

/**
 * Lightweight canvas scatter for projected embeddings (UMAP / PCA).
 *
 * Designed to be paired with the slide viewer: hover a point here →
 * `onHoverPoint` fires with the patch's slide coords; the viewer overlays a
 * highlight box at that location. Canvas (not SVG) so it stays fast on
 * 5k-10k points with no virtualization needed.
 */
export function EmbeddingScatter({
  data, loading, error, title, onHoverPoint, onSelectPoint, externalHighlightIdx, onClose,
}: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [hovered, setHovered] = useState<ScatterPoint | null>(null)

  // Map scatter coords → canvas pixels. Recomputed only when the data
  // bounds change, not on every redraw — keeps hover lookups O(N) on a
  // pre-projected array rather than O(N) plus the projection math.
  const { points, project, unproject } = useMemo(() => {
    if (!data || data.points.length === 0) {
      return { points: [], project: () => ({ px: 0, py: 0 }), unproject: () => null as null | { px: number; py: number } }
    }
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity
    for (const p of data.points) {
      if (p.x < minX) minX = p.x
      if (p.x > maxX) maxX = p.x
      if (p.y < minY) minY = p.y
      if (p.y > maxY) maxY = p.y
    }
    const spanX = maxX - minX || 1
    const spanY = maxY - minY || 1
    return {
      points: data.points,
      project: (canvasW: number, canvasH: number, x: number, y: number) => {
        const pad = 16
        const px = pad + ((x - minX) / spanX) * (canvasW - 2 * pad)
        // Flip y so up = higher value, matching typical scatter conventions
        const py = pad + (1 - (y - minY) / spanY) * (canvasH - 2 * pad)
        return { px, py }
      },
      unproject: () => null,
    }
  }, [data])

  // Redraw on data change or resize
  const draw = useCallback((highlightIdx?: number) => {
    const canvas = canvasRef.current
    if (!canvas || !data) return
    const dpr = window.devicePixelRatio || 1
    const rect = canvas.getBoundingClientRect()
    if (canvas.width !== Math.floor(rect.width * dpr)) {
      canvas.width = Math.floor(rect.width * dpr)
      canvas.height = Math.floor(rect.height * dpr)
    }
    const ctx = canvas.getContext('2d')!
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    ctx.clearRect(0, 0, rect.width, rect.height)

    // Base layer: all points
    ctx.fillStyle = 'rgba(59, 130, 246, 0.55)'  // blue-500/55
    for (const p of points) {
      const { px, py } = (project as any)(rect.width, rect.height, p.x, p.y)
      ctx.beginPath()
      ctx.arc(px, py, 2, 0, Math.PI * 2)
      ctx.fill()
    }

    // Highlight
    if (highlightIdx != null) {
      const p = points[highlightIdx]
      if (p) {
        const { px, py } = (project as any)(rect.width, rect.height, p.x, p.y)
        ctx.fillStyle = '#ef4444'  // red-500
        ctx.beginPath()
        ctx.arc(px, py, 5, 0, Math.PI * 2)
        ctx.fill()
        ctx.strokeStyle = 'white'
        ctx.lineWidth = 1.5
        ctx.stroke()
      }
    }
  }, [data, points, project])

  // Redraw when the external highlight changes too — parent (e.g. slide-click
  // handler) sets externalHighlightIdx and we need to flip the red dot.
  useEffect(() => {
    draw(externalHighlightIdx ?? hovered?.idx)
  }, [draw, externalHighlightIdx, hovered])

  // Resize observer keeps the canvas sharp under panel resize
  useEffect(() => {
    if (!canvasRef.current) return
    const ro = new ResizeObserver(() => draw(externalHighlightIdx ?? hovered?.idx))
    ro.observe(canvasRef.current)
    return () => ro.disconnect()
  }, [draw, hovered, externalHighlightIdx])

  const findNearest = useCallback((clientX: number, clientY: number): ScatterPoint | null => {
    const canvas = canvasRef.current
    if (!canvas || points.length === 0) return null
    const rect = canvas.getBoundingClientRect()
    const mx = clientX - rect.left
    const my = clientY - rect.top
    // Linear scan — fine up to ~50k points. Beyond that we'd want a kd-tree
    // built from the projected coords; not worth it yet.
    let bestIdx = -1
    let bestDist = 16 * 16  // pick within ~16px
    for (let i = 0; i < points.length; i++) {
      const p = points[i]
      const { px, py } = (project as any)(rect.width, rect.height, p.x, p.y)
      const dx = px - mx
      const dy = py - my
      const d = dx * dx + dy * dy
      if (d < bestDist) { bestDist = d; bestIdx = i }
    }
    return bestIdx >= 0 ? points[bestIdx] : null
  }, [points, project])

  const handleMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const p = findNearest(e.clientX, e.clientY)
    if (p?.idx !== hovered?.idx) {
      setHovered(p)
      onHoverPoint?.(p)
      draw(p?.idx)
    }
  }

  const handleLeave = () => {
    if (hovered) {
      setHovered(null)
      onHoverPoint?.(null)
      draw()
    }
  }

  const handleClick = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const p = findNearest(e.clientX, e.clientY)
    if (p) onSelectPoint?.(p)
  }

  return (
    <div className="flex flex-col h-full bg-white border-l">
      <div className="flex items-center justify-between px-3 py-2 border-b">
        <div>
          <div className="text-sm font-medium">{title}</div>
          {data && (
            <div className="text-xs text-muted-foreground">
              {data.method.toUpperCase()} · n={data.n.toLocaleString()} · patch {data.patch_size}px
            </div>
          )}
        </div>
        <Button variant="ghost" size="icon" onClick={onClose} className="h-7 w-7">
          <X className="h-4 w-4" />
        </Button>
      </div>

      <div className="relative flex-1">
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center text-xs text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin mr-2" /> Computing projection…
          </div>
        )}
        {error && !loading && (
          <div className="absolute inset-0 flex items-center justify-center p-4 text-sm text-red-600">
            {error}
          </div>
        )}
        {!loading && !error && (
          <canvas
            ref={canvasRef}
            className="absolute inset-0 w-full h-full cursor-crosshair"
            onMouseMove={handleMove}
            onMouseLeave={handleLeave}
            onClick={handleClick}
          />
        )}
        {hovered && (
          <div className="absolute bottom-2 left-2 bg-black/80 text-white text-[11px] px-2 py-1 rounded pointer-events-none font-mono">
            patch {hovered.idx} · slide ({hovered.slide_x.toLocaleString()}, {hovered.slide_y.toLocaleString()})
          </div>
        )}
      </div>
    </div>
  )
}
