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

/** A second point set in the same map coordinates (another cohort placed on this projection). */
export interface ScatterOverlay {
  data: ProjectionData
  colors?: ScatterColors | null
  visible?: boolean
}

export type PointSet = 'base' | 'overlay'

interface Props {
  data: ProjectionData
  colors?: ScatterColors | null
  /** Array index of the pinned point, or null. */
  highlightIdx?: number | null
  overlay?: ScatterOverlay | null
  /** Hide the base points (e.g. to look at an overlay alone). */
  baseHidden?: boolean
  /** Array index into the overlay of its pinned point, or null. */
  overlayHighlightIdx?: number | null
  onHoverPoint?: (idx: number | null, set?: PointSet) => void
  onSelectPoint?: (idx: number, set: PointSet) => void
  className?: string
}

/** Pixel value for an empty colour: the point isn't drawn. */
const SKIP = 0

function hslToRgb(h: number, s: number, l: number): [number, number, number] {
  h = ((h % 360) + 360) % 360 / 360
  const q = l < 0.5 ? l * (1 + s) : l + s - l * s
  const p = 2 * l - q
  const f = (t: number) => {
    if (t < 0) t += 1
    if (t > 1) t -= 1
    if (t < 1 / 6) return p + (q - p) * 6 * t
    if (t < 1 / 2) return q
    if (t < 2 / 3) return p + (q - p) * (2 / 3 - t) * 6
    return p
  }
  return [Math.round(f(h + 1 / 3) * 255), Math.round(f(h) * 255), Math.round(f(h - 1 / 3) * 255)]
}

function cssToRgba(css: string): number {
  // Tiny parser for the colours our palettes produce: #rgb, #rrggbb, rgb(), hsl().
  // An empty string means "don't draw".
  if (!css) return SKIP
  let r = 128, g = 128, b = 128
  if (css.startsWith('#')) {
    const h = css.slice(1)
    if (h.length === 3) {
      r = parseInt(h[0] + h[0], 16); g = parseInt(h[1] + h[1], 16); b = parseInt(h[2] + h[2], 16)
    } else if (h.length >= 6) {
      r = parseInt(h.slice(0, 2), 16); g = parseInt(h.slice(2, 4), 16); b = parseInt(h.slice(4, 6), 16)
    }
  } else {
    const nums = css.match(/-?[\d.]+/g)?.map(Number) ?? []
    if (css.startsWith('hsl') && nums.length >= 3) {
      ;[r, g, b] = hslToRgb(nums[0], nums[1] / 100, nums[2] / 100)
    } else if (css.startsWith('rgb') && nums.length >= 3) {
      ;[r, g, b] = nums
    }
  }
  // ImageData is little-endian ABGR when viewed as Uint32.
  return ((255 << 24) | (b << 16) | (g << 8) | r) >>> 0
}

export function CohortScatter({
  data, colors, highlightIdx, overlay, baseHidden = false, overlayHighlightIdx,
  onHoverPoint, onSelectPoint, className = '',
}: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const offscreenRef = useRef<HTMLCanvasElement | null>(null)
  const wrapRef = useRef<HTMLDivElement | null>(null)

  // View transform in data space: centre + scale (pixels per data unit).
  const [view, setView] = useState({ cx: 0, cy: 0, scale: 1 })
  const [size, setSize] = useState({ w: 0, h: 0 })
  const [hovered, setHovered] = useState<{ idx: number; set: PointSet } | null>(null)
  const dragRef = useRef<{ x: number; y: number; cx: number; cy: number } | null>(null)
  const settleRef = useRef<number | null>(null)
  const dirtyRef = useRef(true)

  const grid = useMemo(() => new PointGrid(data), [data])
  const overlayData = overlay?.data ?? null
  const overlayShown = !!overlayData && overlay?.visible !== false
  const overlayGrid = useMemo(() => (overlayData ? new PointGrid(overlayData) : null), [overlayData])

  const fitView = useCallback(() => {
    // Fit whatever is shown: the base, the overlay, or both together.
    const sets = [...(baseHidden && overlayShown ? [] : [data]), ...(overlayShown && overlayData ? [overlayData] : [])]
    const minX = Math.min(...sets.map(d => d.bounds.minX)), maxX = Math.max(...sets.map(d => d.bounds.maxX))
    const minY = Math.min(...sets.map(d => d.bounds.minY)), maxY = Math.max(...sets.map(d => d.bounds.maxY))
    const w = size.w || 1, h = size.h || 1
    const sx = (w - PADDING * 2) / (maxX - minX || 1)
    const sy = (h - PADDING * 2) / (maxY - minY || 1)
    setView({ cx: (minX + maxX) / 2, cy: (minY + maxY) / 2, scale: Math.min(sx, sy) })
    dirtyRef.current = true
  }, [data, overlayData, overlayShown, baseHidden, size.w, size.h])

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
  }, [data, overlayData, size.w > 0 && size.h > 0])

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

    const scale = view.scale
    const halfW = w / 2, halfH = h / 2
    const cx = view.cx, cy = view.cy

    const drawSet = (d: ProjectionData, c: ScatterColors | null | undefined, block: number, defaultCss: string) => {
      const paletteRgba = c ? c.palette.map(cssToRgba) : []
      const unassignedRgba = cssToRgba(c ? c.unassigned : '#94a3b8')
      const defaultRgba = cssToRgba(defaultCss)
      const { x, y } = d
      const n = d.pointCount
      for (let i = 0; i < n; i++) {
        const sx = ((x[i] - cx) * scale + halfW) | 0
        if (sx < 0 || sx >= w) continue
        const sy = (halfH - (y[i] - cy) * scale) | 0
        if (sy < 0 || sy >= h) continue

        let rgba = defaultRgba
        if (c) {
          const ci = c.index[i]
          rgba = ci === 255 ? unassignedRgba : (paletteRgba[ci] ?? unassignedRgba)
        }
        if (rgba === SKIP) continue
        // A block rather than one pixel: a single pixel is too faint at these densities.
        for (let dy = 0; dy < block && sy + dy < h; dy++) {
          const row = (sy + dy) * w
          for (let dx = 0; dx < block && sx + dx < w; dx++) buf[row + sx + dx] = rgba
        }
      }
    }

    if (!(baseHidden && overlayShown)) drawSet(data, colors, 2, '#3b82f6')
    // The overlay draws on top, slightly larger, so it stays legible over a dense reference.
    if (overlayShown && overlayData) drawSet(overlayData, overlay?.colors, 3, '#f97316')

    ctx.putImageData(img, 0, 0)
    dirtyRef.current = false
  }, [data, colors, overlayData, overlay?.colors, overlayShown, baseHidden, view, size])

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

    const ring = (d: ProjectionData, idx: number, color: string, r: number) => {
      if (idx < 0 || idx >= d.pointCount) return
      const { sx, sy } = toScreen(d.x[idx], d.y[idx])
      ctx.beginPath()
      ctx.arc(sx, sy, r, 0, Math.PI * 2)
      ctx.strokeStyle = color
      ctx.lineWidth = 2
      ctx.stroke()
    }
    if (hovered) {
      const d = hovered.set === 'overlay' ? overlayData : data
      if (d) ring(d, hovered.idx, '#0f172a', 5)
    }
    const pin = (d: ProjectionData | null, idx: number | null | undefined) => {
      if (!d || idx == null || idx < 0) return
      ring(d, idx, '#ffffff', 7)
      ring(d, idx, '#ef4444', 5)
    }
    pin(data, highlightIdx)
    pin(overlayShown ? overlayData : null, overlayHighlightIdx)
  }, [size, renderOffscreen, toScreen, data, overlayData, overlayShown, hovered, highlightIdx, overlayHighlightIdx])

  // The offscreen raster is cached and only redrawn when marked dirty, so a
  // change of colouring (or of the data itself) has to invalidate it explicitly
  // — otherwise the plot keeps showing the previous colours.
  useEffect(() => { dirtyRef.current = true }, [colors, data, overlayData, overlay?.colors, overlayShown, baseHidden])

  /** Nearest point under the cursor, overlay first (it's drawn on top). */
  const hitTest = useCallback((dx: number, dy: number, radius: number): { idx: number; set: PointSet } | null => {
    if (overlayShown && overlayGrid) {
      const o = overlayGrid.nearest(dx, dy, radius)
      if (o >= 0) return { idx: o, set: 'overlay' }
    }
    if (!(baseHidden && overlayShown)) {
      const b = grid.nearest(dx, dy, radius)
      if (b >= 0) return { idx: b, set: 'base' }
    }
    return null
  }, [overlayShown, overlayGrid, baseHidden, grid])

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
    const next = hitTest(d.x, d.y, 6 / view.scale)   // 6px in data units
    if (next?.idx !== hovered?.idx || next?.set !== hovered?.set) {
      setHovered(next)
      onHoverPoint?.(next ? next.idx : null, next?.set)
    }
  }

  const endDrag = () => { dragRef.current = null }

  const onClick = (e: React.MouseEvent) => {
    // A drag that moved shouldn't register as a click.
    const rect = canvasRef.current!.getBoundingClientRect()
    const d = toData(e.clientX - rect.left, e.clientY - rect.top)
    const hit = hitTest(d.x, d.y, 8 / view.scale)
    if (hit) onSelectPoint?.(hit.idx, hit.set)
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
        {data.pointCount.toLocaleString()} patches · {data.header.slides.length} slides ·{' '}
        {({ umap: 'UMAP', tsne: 't-SNE', pca: 'PCA' } as Record<string, string>)[data.header.method]
          ?? data.header.method.toUpperCase()}
        {overlayShown && overlayData && (
          <> · overlay {overlayData.pointCount.toLocaleString()} patches · {overlayData.header.slides.length} slides</>
        )}
      </div>
    </div>
  )
}
