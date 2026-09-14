import { useEffect, useRef } from 'react'
import OpenSeadragon from 'openseadragon'

/**
 * Paints every patch of the current slide as a cluster-coloured square — the
 * cluster mask. Patch rows come straight from the projection artifact (level-0
 * top-left + the slide's level-0 patch footprint), so the mask sits exactly
 * where the red selection box and the region.jpeg crop do.
 *
 * Follows GeoJSONOverlay's approach rather than converting each rect: one
 * image->screen affine per frame applied as the canvas transform, rects culled
 * to the visible image area, and one Path2D per colour so a slide with tens of
 * thousands of patches is a handful of fill calls.
 */

export interface PatchMask {
  /** Level-0 top-left of each of this slide's patches. */
  patchX: Int32Array
  patchY: Int32Array
  /** Level-0 patch footprint (patch_size_level0, not the model input size). */
  size: number
  /** Palette index per patch; 255 = unassigned / noise. */
  index: Uint8Array
  /** CSS colour per index; an empty string leaves that index unpainted
   *  (used to show only the cluster focused in the legend). */
  palette: string[]
  /** Colour for index 255, or null to leave those patches unpainted. */
  unassigned: string | null
  opacity: number
}

export function PatchClusterOverlay({ viewer, mask }: { viewer: any; mask: PatchMask | null }) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const maskRef = useRef(mask); maskRef.current = mask
  const scheduleRef = useRef<(() => void) | null>(null)

  useEffect(() => {
    if (!viewer || !canvasRef.current) return
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')!
    let rafId: number | null = null
    let cssW = 0, cssH = 0

    const resize = () => {
      const c = viewer.container as HTMLElement
      const dpr = window.devicePixelRatio || 1
      cssW = c.clientWidth; cssH = c.clientHeight
      canvas.width = Math.floor(cssW * dpr)
      canvas.height = Math.floor(cssH * dpr)
      canvas.style.width = `${cssW}px`
      canvas.style.height = `${cssH}px`
    }

    const redraw = () => {
      rafId = null
      const dpr = window.devicePixelRatio || 1
      ctx.setTransform(1, 0, 0, 1, 0, 0)
      ctx.clearRect(0, 0, canvas.width, canvas.height)
      const m = maskRef.current
      const tiled = viewer.world.getItemAt(0)
      if (!m || !tiled || m.patchX.length === 0) return

      // Image (level-0) -> viewer element affine. OSD here never rotates, so
      // it's a scale + translate.
      const o = tiled.imageToViewerElementCoordinates(new OpenSeadragon.Point(0, 0))
      const px = tiled.imageToViewerElementCoordinates(new OpenSeadragon.Point(1, 0))
      const py = tiled.imageToViewerElementCoordinates(new OpenSeadragon.Point(0, 1))
      const sx = px.x - o.x, sy = py.y - o.y
      if (!(sx > 0) || !(sy > 0)) return

      // Visible image rect, padded by one patch so edge patches don't pop.
      const s = m.size
      const minX = -o.x / sx - s, maxX = (cssW - o.x) / sx
      const minY = -o.y / sy - s, maxY = (cssH - o.y) / sy

      const paths = new Map<number, Path2D>()
      const { patchX, patchY, index } = m
      for (let i = 0; i < patchX.length; i++) {
        const x = patchX[i], y = patchY[i]
        if (x < minX || x > maxX || y < minY || y > maxY) continue
        const ci = index[i]
        if (ci === 255 ? !m.unassigned : !m.palette[ci]) continue
        let p = paths.get(ci)
        if (!p) { p = new Path2D(); paths.set(ci, p) }
        p.rect(x, y, s, s)
      }

      ctx.setTransform(dpr * sx, 0, 0, dpr * sy, dpr * o.x, dpr * o.y)
      ctx.globalAlpha = Math.max(0, Math.min(1, m.opacity))
      paths.forEach((p, ci) => {
        ctx.fillStyle = ci === 255 ? (m.unassigned as string) : (m.palette[ci] ?? '#94a3b8')
        ctx.fill(p)
      })
      ctx.globalAlpha = 1
    }

    const schedule = () => { if (rafId == null) rafId = requestAnimationFrame(redraw) }
    scheduleRef.current = schedule
    const onResize = () => { resize(); schedule() }
    resize()
    viewer.addHandler('update-viewport', schedule)
    viewer.addHandler('resize', onResize)
    viewer.addHandler('open', schedule)
    window.addEventListener('resize', onResize)
    schedule()
    return () => {
      if (rafId) cancelAnimationFrame(rafId)
      scheduleRef.current = null
      viewer.removeHandler('update-viewport', schedule)
      viewer.removeHandler('resize', onResize)
      viewer.removeHandler('open', schedule)
      window.removeEventListener('resize', onResize)
    }
  }, [viewer])

  useEffect(() => { scheduleRef.current?.() }, [mask])

  // z-10: beneath PatchHighlight (z-20), so the selection box stays visible.
  return <canvas ref={canvasRef} className="absolute inset-0 pointer-events-none z-10" />
}
