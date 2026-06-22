import { useEffect, useRef, useState } from 'react'
import OpenSeadragon from 'openseadragon'
import type { OverlayRuntime } from '@/components/SlideOverlay'

interface Props {
  viewer: any
  spec: OverlayRuntime
  onFieldsDetected?: (fields: string[], defaultField: string | null) => void
  /** Called whenever the set of distinct color-by values changes (after load or when colorBy is changed). */
  onClassesDetected?: (classes: { value: string; color: string }[]) => void
}

type GJFeature = { type: 'Feature'; geometry: any; properties: Record<string, any> }
// CellViT outputs a bare JSON array of Features; QuPath / GeoJSON spec uses
// a FeatureCollection wrapper. Accept both.
type GJ = GJFeature[] | { type: string; features: GJFeature[] }

function extractFeatures(doc: GJ): GJFeature[] {
  if (Array.isArray(doc)) return doc
  return doc?.features || []
}

type FeatureCache = {
  bbox: [number, number, number, number]   // image-pixel bounds
  rings: number[][][]                       // per polygon, each ring as [x0,y0,x1,y1,...]
}

const PALETTE = [
  '#ef4444', '#3b82f6', '#10b981', '#f59e0b', '#8b5cf6',
  '#ec4899', '#14b8a6', '#f97316', '#84cc16', '#06b6d4',
  '#a855f7', '#eab308', '#22c55e', '#6366f1', '#f43f5e',
]

function getNested(obj: any, path: string): unknown {
  if (!obj || !path) return undefined
  return path.split('.').reduce((acc, key) => (acc == null ? undefined : acc[key]), obj)
}

function colorForValue(val: unknown): string {
  if (val === undefined || val === null || val === '') return '#888888'
  const s = String(val)
  let h = 0
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) & 0xfffffff
  return PALETTE[h % PALETTE.length]
}

function geometryRings(geom: any): number[][][] {
  if (!geom) return []
  if (geom.type === 'Polygon') {
    return [geom.coordinates.map((ring: number[][]) => ring.flat())]
  }
  if (geom.type === 'MultiPolygon') {
    return geom.coordinates.map((poly: number[][][]) => poly.map((ring: number[][]) => ring.flat()))
  }
  return []
}

function ringsBbox(rings: number[][][]): [number, number, number, number] {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity
  for (const poly of rings) for (const flat of poly) {
    for (let i = 0; i < flat.length; i += 2) {
      const x = flat[i], y = flat[i + 1]
      if (x < minX) minX = x
      if (x > maxX) maxX = x
      if (y < minY) minY = y
      if (y > maxY) maxY = y
    }
  }
  return [minX, minY, maxX, maxY]
}

function collectFieldPaths(props: Record<string, any>, prefix = '', depth = 0): string[] {
  if (!props || typeof props !== 'object' || depth > 2) return []
  const out: string[] = []
  for (const [k, v] of Object.entries(props)) {
    const path = prefix ? `${prefix}.${k}` : k
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      out.push(...collectFieldPaths(v, path, depth + 1))
    } else {
      out.push(path)
    }
  }
  return out
}

function pickDefaultColorBy(fields: string[]): string | null {
  const preferred = [
    'classification.name', 'classification.class', 'classification',
    'class', 'category', 'cell_class', 'label', 'type',
  ]
  for (const p of preferred) if (fields.includes(p)) return p
  return null
}

export function GeoJSONOverlay({ viewer, spec, onFieldsDetected, onClassesDetected }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const featuresRef = useRef<FeatureCache[]>([])
  const propsRef = useRef<Record<string, any>[]>([])
  const [loaded, setLoaded] = useState(false)
  const [errorMsg, setErrorMsg] = useState<string | null>(null)
  const [featureCount, setFeatureCount] = useState(0)

  const opacityRef = useRef(spec.opacity); opacityRef.current = spec.opacity
  const visibleRef = useRef(spec.visible); visibleRef.current = spec.visible
  const colorByRef = useRef<string | undefined>(spec.colorBy); colorByRef.current = spec.colorBy
  const scheduleRedrawRef = useRef<(() => void) | null>(null)
  // Set of disabled class values (stringified). When non-empty, the redraw skips those features.
  const disabledClassesRef = useRef<Set<string>>(new Set())
  disabledClassesRef.current = new Set(
    (spec.classes || []).filter(c => !c.enabled).map(c => c.value)
  )

  // Hold the latest callbacks in refs so the fetch effect can depend solely on
  // `spec.url`. The parent (SlideViewerOSD) recreates these callbacks on every
  // render, which would otherwise re-trigger the fetch repeatedly — races
  // between in-flight fetches were producing spurious "Failed to fetch" errors.
  const onFieldsDetectedRef = useRef(onFieldsDetected); onFieldsDetectedRef.current = onFieldsDetected

  // Single fetch for features + properties
  useEffect(() => {
    const ac = new AbortController()
    let disposed = false
    ;(async () => {
      try {
        const res = await fetch(spec.url, { signal: ac.signal })
        if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`.trim())
        const data: GJ = await res.json()
        if (disposed) return

        const features = extractFeatures(data)
        setFeatureCount(features.length)

        // Detect color-by candidates from a sample
        const sample = features.slice(0, 50)
        const fieldSet = new Set<string>()
        for (const f of sample) {
          for (const p of collectFieldPaths(f.properties || {})) fieldSet.add(p)
        }
        const fields = Array.from(fieldSet).sort()
        onFieldsDetectedRef.current?.(fields, pickDefaultColorBy(fields))

        const cache: FeatureCache[] = []
        const props: Record<string, any>[] = []
        for (const f of features) {
          const rings = geometryRings(f.geometry)
          if (rings.length === 0) continue
          cache.push({ bbox: ringsBbox(rings), rings })
          props.push(f.properties || {})
        }
        featuresRef.current = cache
        propsRef.current = props
        setLoaded(true)
      } catch (e: any) {
        if (disposed || e?.name === 'AbortError') return
        setErrorMsg(e?.message || 'Failed to load overlay')
      }
    })()
    return () => { disposed = true; ac.abort() }
  }, [spec.url])

  // Canvas/viewport sync
  useEffect(() => {
    if (!loaded || !viewer || !canvasRef.current) return

    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')!
    let rafId: number | null = null
    let inMotion = false

    const resize = () => {
      const container = viewer.container as HTMLElement
      const w = container.clientWidth
      const h = container.clientHeight
      const dpr = window.devicePixelRatio || 1
      canvas.width = Math.floor(w * dpr)
      canvas.height = Math.floor(h * dpr)
      canvas.style.width = `${w}px`
      canvas.style.height = `${h}px`
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    }

    const redraw = () => {
      rafId = null
      ctx.clearRect(0, 0, canvas.width, canvas.height)
      if (!visibleRef.current) return

      const features = featuresRef.current
      const props = propsRef.current
      const tiledImage = viewer.world.getItemAt(0)
      if (!tiledImage) return

      const bounds = viewer.viewport.getBounds(true)
      const imgRect = tiledImage.viewportToImageRectangle(bounds)
      const viewMinX = imgRect.x
      const viewMinY = imgRect.y
      const viewMaxX = imgRect.x + imgRect.width
      const viewMaxY = imgRect.y + imgRect.height

      const opacity = opacityRef.current
      const colorBy = colorByRef.current
      const disabled = disabledClassesRef.current

      // ── Derive the affine image-pixel → element-pixel transform ONCE.
      // OSD's coordinate system is a uniform scale + translate when rotation
      // is off (which it is in our viewer config). Computing the transform
      // once per frame and inlining the math collapses ~50ns of function-call
      // overhead per vertex; with 10k+ visible vertices that's the difference
      // between 60fps and visible jank.
      const refOrigin = tiledImage.imageToViewerElementCoordinates(new OpenSeadragon.Point(0, 0))
      const refX = tiledImage.imageToViewerElementCoordinates(new OpenSeadragon.Point(1, 0))
      const scale = refX.x - refOrigin.x          // element px per 1 image px
      const offX = refOrigin.x
      const offY = refOrigin.y

      ctx.globalAlpha = opacity
      ctx.lineWidth = 1

      // Stroke is only useful when cells are big enough to see edges. Below this,
      // skip the stroke pass — saves ~30% per frame at low zoom where the canvas
      // would otherwise spend most of its time stroking sub-pixel polygons.
      const drawStroke = scale > 0.5

      // During pan/zoom, decimate the feature draw for a fast preview. Skip
      // factor scales with feature count so dense slides stay fluid. At rest,
      // every feature is drawn.
      const skip = inMotion ? Math.max(1, Math.floor(features.length / 25000)) : 1

      for (let i = 0; i < features.length; i += skip) {
        const feat = features[i]
        const [minX, minY, maxX, maxY] = feat.bbox
        if (maxX < viewMinX || minX > viewMaxX || maxY < viewMinY || minY > viewMaxY) continue

        const value = colorBy ? getNested(props[i], colorBy) : null
        if (colorBy && disabled.size) {
          const v = value === undefined || value === null ? '' : String(value)
          if (disabled.has(v)) continue
        }
        const fill = colorBy ? colorForValue(value) : '#3b82f6'
        ctx.fillStyle = fill
        ctx.strokeStyle = fill

        for (const poly of feat.rings) {
          for (const flat of poly) {
            ctx.beginPath()
            // Inline transform: ex = imgX * scale + offX, ey = imgY * scale + offY
            ctx.moveTo(flat[0] * scale + offX, flat[1] * scale + offY)
            for (let j = 2; j < flat.length; j += 2) {
              ctx.lineTo(flat[j] * scale + offX, flat[j + 1] * scale + offY)
            }
            ctx.closePath()
            ctx.fill()
            if (drawStroke) {
              ctx.globalAlpha = Math.min(1, opacity + 0.3)
              ctx.stroke()
              ctx.globalAlpha = opacity
            }
          }
        }
      }
      ctx.globalAlpha = 1
    }

    const scheduleRedraw = () => {
      if (rafId != null) return
      rafId = requestAnimationFrame(redraw)
    }
    scheduleRedrawRef.current = scheduleRedraw

    const onResize = () => { resize(); scheduleRedraw() }
    const onAnimStart = () => { inMotion = true }
    const onAnimFinish = () => { inMotion = false; scheduleRedraw() }

    resize()
    viewer.addHandler('update-viewport', scheduleRedraw)
    viewer.addHandler('resize', onResize)
    viewer.addHandler('open', scheduleRedraw)
    viewer.addHandler('animation-start', onAnimStart)
    viewer.addHandler('animation-finish', onAnimFinish)
    window.addEventListener('resize', onResize)

    scheduleRedraw()

    return () => {
      if (rafId) cancelAnimationFrame(rafId)
      scheduleRedrawRef.current = null
      viewer.removeHandler('update-viewport', scheduleRedraw)
      viewer.removeHandler('resize', onResize)
      viewer.removeHandler('open', scheduleRedraw)
      viewer.removeHandler('animation-start', onAnimStart)
      viewer.removeHandler('animation-finish', onAnimFinish)
      window.removeEventListener('resize', onResize)
    }
  }, [loaded, viewer, spec.id])

  // Compute the set of distinct color-by values + their assigned colors whenever
  // the colorBy field (or the file) changes, then report back to the parent.
  useEffect(() => {
    if (!loaded || !onClassesDetected) return
    const colorBy = spec.colorBy
    if (!colorBy) {
      onClassesDetected([])
      return
    }
    const props = propsRef.current
    const seen = new Map<string, string>()  // value → color
    for (let i = 0; i < props.length; i++) {
      const raw = getNested(props[i], colorBy)
      const value = raw === undefined || raw === null ? '' : String(raw)
      if (!seen.has(value)) seen.set(value, colorForValue(value))
    }
    onClassesDetected(
      Array.from(seen.entries())
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([value, color]) => ({ value, color }))
    )
  }, [loaded, spec.colorBy, onClassesDetected])

  // Trigger a redraw when opacity / colorBy / visible / classes change
  useEffect(() => {
    if (!loaded) return
    scheduleRedrawRef.current?.()
  }, [spec.opacity, spec.colorBy, spec.visible, spec.classes, loaded])

  return (
    <>
      <canvas
        ref={canvasRef}
        className="absolute inset-0 pointer-events-none"
      />
      {errorMsg && (
        <div className="absolute top-4 right-4 bg-red-500/20 border border-red-500 text-red-200 px-3 py-1.5 rounded text-xs z-30">
          Overlay error: {errorMsg}
        </div>
      )}
      {loaded && featureCount === 0 && (
        <div className="absolute top-4 right-1/2 translate-x-1/2 bg-yellow-500/20 border border-yellow-500 text-yellow-200 px-3 py-1.5 rounded text-xs z-30">
          {spec.name}: GeoJSON has no features
        </div>
      )}
    </>
  )
}
