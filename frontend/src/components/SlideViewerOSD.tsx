import { useEffect, useRef, useState, useCallback } from 'react'
import OpenSeadragon from 'openseadragon'

/**
 * Tiny canvas overlay that draws a single red box at the given level-0
 * slide-pixel coordinates. Re-syncs with the OSD viewport on pan/zoom.
 * Used by EmbeddingScatter to show "this scatter point is *this* patch."
 */
function PatchHighlight({ viewer, patch }: {
  viewer: any
  patch: { slide_x: number; slide_y: number; size: number } | null
}) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const patchRef = useRef(patch); patchRef.current = patch
  const scheduleRef = useRef<(() => void) | null>(null)

  useEffect(() => {
    if (!viewer || !canvasRef.current) return
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')!
    let rafId: number | null = null

    const resize = () => {
      const c = viewer.container as HTMLElement
      const dpr = window.devicePixelRatio || 1
      canvas.width = Math.floor(c.clientWidth * dpr)
      canvas.height = Math.floor(c.clientHeight * dpr)
      canvas.style.width = `${c.clientWidth}px`
      canvas.style.height = `${c.clientHeight}px`
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    }

    const redraw = () => {
      rafId = null
      ctx.clearRect(0, 0, canvas.width, canvas.height)
      const p = patchRef.current
      if (!p) return
      const tiledImage = viewer.world.getItemAt(0)
      if (!tiledImage) return
      const tl = tiledImage.imageToViewerElementCoordinates(
        new OpenSeadragon.Point(p.slide_x, p.slide_y)
      )
      const br = tiledImage.imageToViewerElementCoordinates(
        new OpenSeadragon.Point(p.slide_x + p.size, p.slide_y + p.size)
      )
      ctx.strokeStyle = '#ef4444'
      ctx.lineWidth = 2
      ctx.shadowColor = 'rgba(239,68,68,0.7)'
      ctx.shadowBlur = 6
      ctx.strokeRect(tl.x, tl.y, br.x - tl.x, br.y - tl.y)
      ctx.shadowBlur = 0
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

  // Redraw immediately when the patch prop changes — without this, the box
  // would only refresh on the next pan/zoom event.
  useEffect(() => { scheduleRef.current?.() }, [patch])

  return <canvas ref={canvasRef} className="absolute inset-0 pointer-events-none z-20" />
}
import { X, ZoomIn, ZoomOut, Home, Loader2, Layers } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { getApiBase, getAuthToken, isDemo } from '@/api'
import { OverlayControls, type OverlaySpec, type OverlayRuntime } from '@/components/SlideOverlay'
import { GeoJSONOverlay } from '@/components/GeoJSONOverlay'

interface SlideViewerOSDProps {
  slideHash: string
  slideName: string
  overlays?: OverlaySpec[]
  /** Draws a highlight rectangle on the slide at the given image-pixel coords.
   *  Used by EmbeddingScatter to point at the patch under the cursor. */
  highlightPatch?: { slide_x: number; slide_y: number; size: number } | null
  /** Click handler called with the click location in level-0 image-pixel
   *  coords. Used by ScatterViewerOverlay so clicking on tissue can highlight
   *  the corresponding scatter point. Not a drag — fires only on canvas-click. */
  onImageClick?: (image_x: number, image_y: number) => void
  /** When true, the viewer fills its parent container instead of taking over
   *  the whole viewport (fixed inset-0). Use this for split-pane layouts
   *  like ScatterViewerOverlay so OSD's fit-to-screen math matches the
   *  actually-visible area. */
  embedded?: boolean
  onClose: () => void
}

export function SlideViewerOSD({ slideHash, slideName, overlays: initialOverlays = [], highlightPatch, onImageClick, embedded = false, onClose }: SlideViewerOSDProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const viewerRef = useRef<any>(null)
  // Held in a ref so the OSD canvas-click handler (registered once at viewer
  // init) sees the latest callback without needing to re-init.
  const onImageClickRef = useRef(onImageClick); onImageClickRef.current = onImageClick
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  // Set while the backend is converting a plain TIFF into a viewable pyramid.
  const [preparing, setPreparing] = useState<string | null>(null)
  const [dimensions, setDimensions] = useState<{ width: number; height: number } | null>(null)
  const [overlays, setOverlays] = useState<OverlayRuntime[]>(
    initialOverlays.map(spec => ({ ...spec, visible: spec.visible ?? true, opacity: spec.opacity ?? 0.55 }))
  )
  const [labelError, setLabelError] = useState(false)

  // Initialize OSD with the DZI source (fetched with auth ourselves)
  useEffect(() => {
    if (!containerRef.current) return
    let disposed = false

    const init = async () => {
      try {
        // Fetch the DZI manifest with auth, parse, hand parsed tile source to OSD
        // A plain TIFF has to be converted to a pyramid before it can be
        // tiled; the backend answers 503 with progress while that runs, so
        // poll rather than failing the open.
        let res = await fetch(`${getApiBase()}/slides/${slideHash}/dzi.json`)
        while (res.status === 503 && !disposed) {
          const d = await res.json().catch(() => null)
          setPreparing(d?.detail?.message || 'Preparing this slide for viewing…')
          await new Promise(r => setTimeout(r, 2500))
          if (disposed) return
          res = await fetch(`${getApiBase()}/slides/${slideHash}/dzi.json`)
        }
        setPreparing(null)
        if (!res.ok) {
          // Surface the backend's reason (e.g. an unreadable TIFF) rather than
          // just the status code — it's the only clue the user gets.
          const detail = await res.json().then(d => d?.detail).catch(() => null)
          throw new Error(typeof detail === 'string' ? detail : `DZI manifest HTTP ${res.status}`)
        }
        const dzi = await res.json()
        const width = parseInt(dzi.Image.Size.Width, 10)
        const height = parseInt(dzi.Image.Size.Height, 10)
        const tileSize = parseInt(dzi.Image.TileSize, 10)
        const overlap = parseInt(dzi.Image.Overlap || '0', 10)
        const format = (dzi.Image.Format || 'jpeg').toLowerCase()

        if (disposed) return
        setDimensions({ width, height })

        const tilesBase = `${getApiBase()}/slides/${slideHash}/tiles`

        const tileSource = {
          height,
          width,
          tileSize,
          tileOverlap: overlap,
          minLevel: 0,
          // DZI levels: 0..maxLevel; max width fits in one tile at level 0
          maxLevel: Math.ceil(Math.log2(Math.max(width, height))),
          getTileUrl(level: number, x: number, y: number) {
            return `${tilesBase}/${level}/${x}_${y}.${format}`
          },
        }

        const token = getAuthToken()
        const viewer = OpenSeadragon({
          element: containerRef.current!,
          tileSources: tileSource,
          loadTilesWithAjax: true,
          ajaxHeaders: token ? { Authorization: `Bearer ${token}` } : {},
          showNavigator: true,
          navigatorPosition: 'TOP_RIGHT',
          navigatorAutoFade: false,
          showRotationControl: false,
          showZoomControl: false,
          showHomeControl: false,
          showFullPageControl: false,
          showNavigationControl: false,
          animationTime: 0.25,
          maxZoomPixelRatio: 4,
          gestureSettingsMouse: { clickToZoom: false, dblClickToZoom: true },
          gestureSettingsTouch: { clickToZoom: false, dblClickToZoom: true },
          // Render lower-resolution tiles immediately while higher levels stream in,
          // so the user sees the whole slide sharpen progressively rather than a
          // single sharp square surrounded by stale blur.
          immediateRender: true,
          blendTime: 0,
          preserveImageSizeOnResize: true,
          // Cap parallel tile downloads to a value comfortable for HTTP/1.1 (browser
          // limit is ~6/host). Default of 0 (unlimited) means the browser queues
          // dozens of tiles and stalls the visible viewport behind off-screen ones.
          imageLoaderLimit: 6,
          // Keep more tiles around — pan/zoom redraws often hit recently-evicted tiles.
          maxImageCacheCount: 1000,
          // Pre-fetch tiles just outside the viewport so pan feels instant.
          preload: true,
          // Disable per-tile edge smoothing — expensive and invisible at our zoom levels.
          smoothTileEdgesMinZoom: Infinity,
          // Solid dark fill behind missing tiles instead of the blurry upscale.
          placeholderFillStyle: '#0a0a0a',
          timeout: 30000,
        } as any)

        viewer.addHandler('open', () => { if (!disposed) setLoading(false) })
        viewer.addHandler('open-failed', (e: any) => {
          if (!disposed) setError(e?.message || 'Failed to load slide tiles')
          if (!disposed) setLoading(false)
        })

        // Canvas click → forward image-pixel coords to the parent. Read the
        // callback off a ref so the parent can swap it without re-mounting
        // the viewer. canvas-click fires only on click (not drag), so pan/
        // zoom interactions aren't misinterpreted as clicks.
        viewer.addHandler('canvas-click', (e: any) => {
          const cb = onImageClickRef.current
          if (!cb || !e?.position) return
          // OSD's pointFromPixel(position) is in viewport coords (0–1). Tiled
          // image's viewportToImageCoordinates maps that to level-0 image px,
          // which is what our scatter point payloads use.
          const tiledImage = viewer.world.getItemAt(0)
          if (!tiledImage) return
          const vp = viewer.viewport.pointFromPixel(e.position)
          const img = tiledImage.viewportToImageCoordinates(vp)
          cb(img.x, img.y)
        })

        viewerRef.current = viewer
      } catch (e: any) {
        if (!disposed) {
          setError(e.message || 'Failed to initialize viewer')
          setLoading(false)
        }
      }
    }

    init()

    return () => {
      disposed = true
      if (viewerRef.current) {
        try { viewerRef.current.destroy() } catch { /* ignore */ }
        viewerRef.current = null
      }
    }
  }, [slideHash])

  // Escape closes
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const zoomIn = useCallback(() => viewerRef.current?.viewport.zoomBy(1.4) || viewerRef.current?.viewport.applyConstraints(), [])
  const zoomOut = useCallback(() => viewerRef.current?.viewport.zoomBy(1 / 1.4) || viewerRef.current?.viewport.applyConstraints(), [])
  const goHome = useCallback(() => viewerRef.current?.viewport.goHome(), [])

  const toggleOverlay = (id: string) =>
    setOverlays(prev => prev.map(o => o.id === id ? { ...o, visible: !o.visible } : o))
  const setOverlayOpacity = (id: string, opacity: number) =>
    setOverlays(prev => prev.map(o => o.id === id ? { ...o, opacity } : o))
  const setOverlayColorBy = (id: string, colorBy: string) =>
    // Changing colorBy invalidates the class list — clear it; classes are reported
    // back as soon as the overlay re-aggregates with the new field.
    setOverlays(prev => prev.map(o => o.id === id ? { ...o, colorBy, classes: undefined } : o))
  const setOverlayClasses = (id: string, classes: { value: string; color: string }[]) =>
    setOverlays(prev => prev.map(o => {
      if (o.id !== id) return o
      // Preserve enabled state across updates (e.g. opacity change shouldn't reset toggles)
      const prevEnabled = new Map((o.classes || []).map(c => [c.value, c.enabled]))
      return { ...o, classes: classes.map(c => ({ ...c, enabled: prevEnabled.get(c.value) ?? true })) }
    }))
  const toggleClass = (id: string, value: string) =>
    setOverlays(prev => prev.map(o => o.id !== id ? o : {
      ...o,
      classes: (o.classes || []).map(c => c.value === value ? { ...c, enabled: !c.enabled } : c),
    }))
  const setAllClasses = (id: string, enabled: boolean) =>
    setOverlays(prev => prev.map(o => o.id !== id ? o : {
      ...o,
      classes: (o.classes || []).map(c => ({ ...c, enabled })),
    }))

  // Fetch the label through the auth-wrapped `fetch` (not as an <img src=...>,
  // which goes through the browser's image loader and bypasses the auth
  // interceptor → 401). Convert to a blob URL we can hand to <img>.
  //
  // Skipped entirely in demo mode: the physical-slide label is the single
  // biggest PHI surface in the viewer (handwritten accession + patient
  // initials), and we don't even want it on the wire — short-circuiting the
  // fetch keeps the request log clean too.
  const [labelBlobUrl, setLabelBlobUrl] = useState<string | null>(null)
  useEffect(() => {
    if (isDemo()) return
    let cancelled = false
    let url: string | null = null
    fetch(`${getApiBase()}/slides/${slideHash}/label.jpeg?max_size=256`)
      .then(r => r.ok ? r.blob() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then(blob => {
        if (cancelled) return
        url = URL.createObjectURL(blob)
        setLabelBlobUrl(url)
      })
      .catch(() => { if (!cancelled) setLabelError(true) })
    return () => {
      cancelled = true
      if (url) URL.revokeObjectURL(url)
    }
  }, [slideHash])

  return (
    <div className={`${embedded ? 'absolute inset-0' : 'fixed inset-0'} z-100 bg-black flex flex-col`}>
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 pt-10 bg-black/80 border-b border-gray-800 relative z-20">
        <div className="text-white min-w-0">
          {/* In demo mode we hide the slide name outright — callers already pass
              a PHI-redacted label via displaySlide(), but suppressing the title
              here guarantees no accession can ever land in the viewer chrome
              regardless of what a future caller forgets to redact. */}
          {!isDemo() && (
            <h2 className="font-semibold truncate">{slideName}</h2>
          )}
          <p className="text-xs text-gray-400">
            {loading ? 'Loading…' : dimensions ? `${dimensions.width.toLocaleString()} × ${dimensions.height.toLocaleString()} px` : ''}
            {overlays.length > 0 && !loading && (
              <span className="ml-2"><Layers className="inline h-3 w-3" /> {overlays.filter(o => o.visible).length}/{overlays.length} overlays</span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="icon" onClick={zoomOut} className="text-white hover:bg-white/20"><ZoomOut className="h-5 w-5" /></Button>
          <Button variant="ghost" size="icon" onClick={zoomIn} className="text-white hover:bg-white/20"><ZoomIn className="h-5 w-5" /></Button>
          <Button variant="ghost" size="icon" onClick={goHome} className="text-white hover:bg-white/20"><Home className="h-5 w-5" /></Button>
          <div className="w-px h-6 bg-gray-600 mx-2" />
          <Button variant="ghost" size="icon" onClick={onClose} className="text-white hover:bg-white/20"><X className="h-5 w-5" /></Button>
        </div>
      </div>

      {/* Viewer */}
      <div className="flex-1 relative">
        <div ref={containerRef} className="absolute inset-0 bg-black" />

        {loading && (
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
            <div className="flex items-center gap-2 text-white"><Loader2 className="h-6 w-6 animate-spin" /> Loading tiles…</div>
          </div>
        )}

        {preparing && !error && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/60 z-20">
            <div className="flex items-center gap-3 rounded-lg bg-black/70 px-5 py-4 text-white max-w-lg">
              <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
              <div>
                <p className="text-sm font-medium">Preparing this slide</p>
                <p className="text-xs text-white/70 break-words">{preparing}</p>
                <p className="mt-1 text-xs text-white/50">One-time conversion — it opens instantly next time.</p>
              </div>
            </div>
          </div>
        )}

        {error && (
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="bg-red-500/20 border border-red-500 text-red-200 px-4 py-3 rounded-lg max-w-lg">
              <p className="font-semibold">Error</p>
              <p className="text-sm break-words">{error}</p>
            </div>
          </div>
        )}

        {/* Slide label corner */}
        {!labelError && labelBlobUrl && (
          <div className="absolute bottom-4 left-4 z-10 pointer-events-none">
            <img
              src={labelBlobUrl}
              alt="Slide label"
              className="max-w-50 max-h-37.5 rounded bg-black/60 p-1 shadow-lg"
              onError={() => setLabelError(true)}
            />
          </div>
        )}

        {/* Patch highlight box — driven by an external scatter / hover source.
            Lives outside the OverlayControls list because it's controlled by
            the parent (e.g. EmbeddingScatter), not by the user via the panel. */}
        {viewerRef.current && !loading && (
          <PatchHighlight viewer={viewerRef.current} patch={highlightPatch ?? null} />
        )}

        {/* GeoJSON overlays — each renders a canvas synced to OSD viewport */}
        {viewerRef.current && !loading && overlays.map(o => (
          o.type === 'geojson' ? (
            <GeoJSONOverlay
              key={o.id}
              viewer={viewerRef.current}
              spec={o}
              onFieldsDetected={(fields, defaultField) => {
                setOverlays(prev => prev.map(x =>
                  x.id === o.id
                    ? { ...x, fields, colorBy: x.colorBy ?? (defaultField || undefined) }
                    : x,
                ))
              }}
              onClassesDetected={(classes) => setOverlayClasses(o.id, classes)}
            />
          ) : null
        ))}

        {/* Overlay controls */}
        {overlays.length > 0 && (
          <OverlayControls
            overlays={overlays}
            onToggle={toggleOverlay}
            onOpacity={setOverlayOpacity}
            onColorBy={setOverlayColorBy}
            onToggleClass={toggleClass}
            onSetAllClasses={setAllClasses}
          />
        )}
      </div>
    </div>
  )
}
