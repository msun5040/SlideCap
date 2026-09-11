import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Maximize2, Minus, Plus } from 'lucide-react'
import type { ProjectionData } from '@/lib/projection'
import { PointGrid } from '@/lib/projection'

/**
 * Cohort-scale scatter plot.
 *
 * Deliberately separate from EmbeddingScatter rather than an extension of it.
 * That one is built around an array of point objects, a linear-scan hit test
 * (documented as good to ~50k) and a fixed extent with no zoom — all fine for
 * one slide's few thousand patches, none of it viable for a million.
 *
 * What's different here:
 *  - Points arrive as typed arrays and are rasterized via ImageData, writing
 *    pixels directly instead of a fillRect/arc per point.
 *  - The full-quality raster is cached offscreen and blitted with a transform
 *    while panning or zooming, then re-rendered once motion settles.
 *  - Hit-testing goes through a uniform grid (PointGrid), not a scan.
 *  - It fixes the idx-vs-array-index conflation in the older component, which
 *    only holds there because the per-slide backend emits idx === i. Here every
 *    index is an array index, full stop.
 */

const PADDING = 24
/** Above this, skip the offscreen re-render mid-gesture and blit instead. */
const REDRAW_SETTLE_MS = 120

export interface ScatterColors {
  /** Per-point palette index; 255 means "unassigned". */
  index: Uint8Array
  /** CSS colors by palette index. */
  palette: string[]
  /** Color for points whose index is 255. */
  unassigned: string
}

interface Props {
  data: ProjectionData
  colors?: ScatterColors | null
  /** Array index of the pinned point, or null. */
  highlightIdx?: number | null
  onHoverPoint?: (idx: number | null) => void
  onSelectPoint?: (idx: number) => void
  className?: string
}

function cssToRgba(css: string): number {
  // Tiny parser: the palette is ours, so only #rgb/#rrggbb need handling.
  let r = 128, g = 128, b = 128
  if (css.startsWith('#')) {
    const h = css.slice(1)
    if (h.length === 3) {
      r = parseInt(h[0] + h[0], 16); g = parseInt(h[1] + h[1], 16); b = parseInt(h[2] + h[2], 16)
    } else if (h.length >= 6) {
      r = parseInt(h.slice(0, 2), 16); g = parseInt(h.slice(2, 4), 16); b = parseInt(h.slice(4, 6), 16)
    }
  }
  // ImageData is little-endian ABGR when viewed as Uint32.
  return (255 << 24) | (b << 16) | (g << 8) | r
}

export function CohortScatter({
  data, colors, highlightIdx, onHoverPoint, onSelectPoint, className = '',
}: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const offscreenRef = useRef<HTMLCanvasElement | null>(null)
  const wrapRef = useRef<HTMLDivElement | null>(null)

  // View transform in data space: centre + scale (pixels per data unit).
  const [view, setView] = useState({ cx: 0, cy: 0, scale: 1 })
  const [size, setSize] = useState({ w: 0, h: 0 })
  const [hovered, setHovered] = useState<number | null>(null)
  const dragRef = useRef<{ x: number; y: number; cx: number; cy: number } | null>(null)
  const settleRef = useRef<number | null>(null)
  const dirtyRef = useRef(true)

  const grid = useMemo(() => new PointGrid(data), [data])

  const fitView = useCallback(() => {
    const { minX, maxX, minY, maxY } = data.bounds
    const w = size.w || 1, h = size.h || 1
    const sx = (w - PADDING * 2) / (maxX - minX || 1)
    const sy = (h - PADDING * 2) / (maxY - minY || 1)
    setView({ cx: (minX + maxX) / 2, cy: (minY + maxY) / 2, scale: Math.min(sx, sy) })
    dirtyRef.current = true
  }, [data, size.w, size.h])

  // Track container size.
  useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => {
      const r = el.getBoundingClientRect()
      setSize({ w: Math.max(1, Math.floor(r.width)), h: Math.max(1, Math.floor(r.height)) })
      dirtyRef.current = true
    })
    ro.observe(el)
    const r = el.getBoundingClientRect()
    setSize({ w: Math.max(1, Math.floor(r.width)), h: Math.max(1, Math.floor(r.height)) })
    return () => ro.disconnect()
  }, [])

  // Fit once the data or the canvas size first becomes known.
  useEffect(() => {
    if (size.w > 0 && size.h > 0) fitView()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, size.w > 0 && size.h > 0])

  const toScreen = useCallback((x: number, y: number) => ({
    // y is flipped so the plot reads the conventional way up.
    sx: (x - view.cx) * view.scale + size.w / 2,
    sy: size.h / 2 - (y - view.cy) * view.scale,
  }), [view, size])

  const toData = useCallback((sx: number, sy: number) => ({
    x: (sx - size.w / 2) / view.scale + view.cx,
    y: view.cy - (sy - size.h / 2) / view.scale,
  }), [view, size])

  /** Rasterize every point into an ImageData buffer on the offscreen canvas. */
  const renderOffscreen = useCallback(() => {
    const { w, h } = size
    if (w <= 0 || h <= 0) return
    let off = offscreenRef.current
    if (!off) { off = document.createElement('canvas'); offscreenRef.current = off }
    if (off.width !== w || off.height !== h) { off.width = w; off.height = h }
    const ctx = off.getContext('2d')
    if (!ctx) return

    const img = ctx.createImageData(w, h)
    const buf = new Uint32Array(img.data.buffer)

    const paletteRgba = colors
      ? colors.palette.map(cssToRgba)
      : []
    const unassignedRgba = cssToRgba(colors?.unassigned ?? '#94a3b8')
    const defaultRgba = cssToRgba('#3b82f6')

    const { x, y } = data
    const n = data.pointCount
    const scale = view.scale
    const halfW = w / 2, halfH = h / 2
    const cx = view.cx, cy = view.cy

    for (let i = 0; i < n; i++) {
      const sx = ((x[i] - cx) * scale + halfW) | 0
      if (sx < 0 || sx >= w) continue
      const sy = (halfH - (y[i] - cy) * scale) | 0
      if (sy < 0 || sy >= h) continue

      let rgba = defaultRgba
      if (colors) {
        const ci = colors.index[i]
        rgba = ci === 255 ? unassignedRgba : (paletteRgba[ci] ?? unassignedRgba)
      }
      // 2x2 block: a single pixel is too faint to read at these densities.
      const o = sy * w + sx
      buf[o] = rgba
      if (sx + 1 < w) buf[o + 1] = rgba
      if (sy + 1 < h) {
        buf[o + w] = rgba
        if (sx + 1 < w) buf[o + w + 1] = rgba
      }
    }
    ctx.putImageData(img, 0, 0)
    dirtyRef.current = false
  }, [data, colors, view, size])

  /** Composite: offscreen raster + interactive overlays (hover/selection). */
  const paint = useCallback(() => {
    const canvas = canvasRef.current
    const { w, h } = size
    if (!canvas || w <= 0 || h <= 0) return
    const dpr = window.devicePixelRatio || 1
    if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
      canvas.width = w * dpr; canvas.height = h * dpr
      canvas.style.width = `${w}px`; canvas.style.height = `${h}px`
    }
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    ctx.clearRect(0, 0, w, h)

    if (dirtyRef.current) renderOffscreen()
    if (offscreenRef.current) ctx.drawImage(offscreenRef.current, 0, 0)

    const ring = (idx: number, color: string, r: number) => {
      const { sx, sy } = toScreen(data.x[idx], data.y[idx])
      ctx.beginPath()
      ctx.arc(sx, sy, r, 0, Math.PI * 2)
      ctx.strokeStyle = color
      ctx.lineWidth = 2
      ctx.stroke()
    }
    if (hovered != null && hovered >= 0) ring(hovered, '#0f172a', 5)
    if (highlightIdx != null && highlightIdx >= 0) {
      ring(highlightIdx, '#ffffff', 7)
      ring(highlightIdx, '#ef4444', 5)
    }
  }, [size, renderOffscreen, toScreen, data, hovered, highlightIdx])

  // The offscreen raster is cached and only redrawn when marked dirty, so a
  // change of colouring (or of the data itself) has to invalidate it explicitly
  // — otherwise the plot keeps showing the previous colours.
  useEffect(() => { dirtyRef.current = true }, [colors, data])

  useEffect(() => { paint() }, [paint])

  // ── Interaction ─────────────────────────────────────────────────────
  const scheduleSettle = useCallback(() => {
    if (settleRef.current) window.clearTimeout(settleRef.current)
    settleRef.current = window.setTimeout(() => {
      dirtyRef.current = true
      paint()
    }, REDRAW_SETTLE_MS)
  }, [paint])

  const onWheel = (e: React.WheelEvent) => {
    e.preventDefault()
    const rect = canvasRef.current!.getBoundingClientRect()
    const mx = e.clientX - rect.left, my = e.clientY - rect.top
    const before = toData(mx, my)
    const factor = Math.exp(-e.deltaY * 0.0015)
    const scale = Math.max(1e-4, Math.min(1e6, view.scale * factor))
    // Keep the point under the cursor fixed while zooming.
    const cx = before.x - (mx - size.w / 2) / scale
    const cy = before.y + (my - size.h / 2) / scale
    setView({ cx, cy, scale })
    dirtyRef.current = true
    scheduleSettle()
  }

  const onMouseDown = (e: React.MouseEvent) => {
    dragRef.current = { x: e.clientX, y: e.clientY, cx: view.cx, cy: view.cy }
  }

  const onMouseMove = (e: React.MouseEvent) => {
    const rect = canvasRef.current!.getBoundingClientRect()
    const mx = e.clientX - rect.left, my = e.clientY - rect.top

    if (dragRef.current) {
      const dx = (e.clientX - dragRef.current.x) / view.scale
      const dy = (e.clientY - dragRef.current.y) / view.scale
      setView(v => ({ ...v, cx: dragRef.current!.cx - dx, cy: dragRef.current!.cy + dy }))
      dirtyRef.current = true
      scheduleSettle()
      return
    }

    const d = toData(mx, my)
    const radius = 6 / view.scale   // 6px in data units
    const hit = grid.nearest(d.x, d.y, radius)
    const next = hit >= 0 ? hit : null
    if (next !== hovered) {
      setHovered(next)
      onHoverPoint?.(next)
    }
  }

  const endDrag = () => { dragRef.current = null }

  const onClick = (e: React.MouseEvent) => {
    // A drag that moved shouldn't register as a click.
    const rect = canvasRef.current!.getBoundingClientRect()
    const d = toData(e.clientX - rect.left, e.clientY - rect.top)
    const hit = grid.nearest(d.x, d.y, 8 / view.scale)
    if (hit >= 0) onSelectPoint?.(hit)
  }

  const zoomBy = (factor: number) => {
    setView(v => ({ ...v, scale: Math.max(1e-4, Math.min(1e6, v.scale * factor)) }))
    dirtyRef.current = true
    scheduleSettle()
  }

  return (
    <div ref={wrapRef} className={`relative h-full w-full overflow-hidden ${className}`}>
      <canvas
        ref={canvasRef}
        className="block cursor-crosshair"
        onWheel={onWheel}
        onMouseDown={onMouseDown}
        onMouseMove={onMouseMove}
        onMouseUp={endDrag}
        onMouseLeave={() => { endDrag(); setHovered(null); onHoverPoint?.(null) }}
        onClick={onClick}
      />
      <div className="absolute bottom-3 right-3 flex flex-col gap-1">
        <button onClick={() => zoomBy(1.4)} title="Zoom in"
          className="rounded border bg-background/90 p-1.5 shadow hover:bg-muted">
          <Plus className="h-3.5 w-3.5" />
        </button>
        <button onClick={() => zoomBy(1 / 1.4)} title="Zoom out"
          className="rounded border bg-background/90 p-1.5 shadow hover:bg-muted">
          <Minus className="h-3.5 w-3.5" />
        </button>
        <button onClick={fitView} title="Fit to view"
          className="rounded border bg-background/90 p-1.5 shadow hover:bg-muted">
          <Maximize2 className="h-3.5 w-3.5" />
        </button>
      </div>
      <div className="pointer-events-none absolute bottom-3 left-3 rounded bg-background/80 px-2 py-1 text-[11px] text-muted-foreground">
        {data.pointCount.toLocaleString()} patches · {data.header.slides.length} slides · {data.header.method.toUpperCase()}
      </div>
    </div>
  )
}
