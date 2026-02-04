import { useEffect, useState, useRef, useCallback } from 'react'
import { X, ZoomIn, ZoomOut, RotateCcw, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'

const API_BASE = 'http://localhost:8000'

interface SlideViewerProps {
  slideHash: string
  slideName: string
  onClose: () => void
}

export function SlideViewer({ slideHash, slideName, onClose }: SlideViewerProps) {
  const [imageLoaded, setImageLoaded] = useState(false)
  const [labelLoaded, setLabelLoaded] = useState(false)
  const [labelError, setLabelError] = useState(false)  // Label may not exist
  const [error, setError] = useState<string | null>(null)
  const [zoom, setZoom] = useState(1)
  const [position, setPosition] = useState({ x: 0, y: 0 })
  const [isDragging, setIsDragging] = useState(false)
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 })
  const containerRef = useRef<HTMLDivElement>(null)

  // Thumbnail URL - uses cached embedded thumbnail from SVS file
  const thumbnailUrl = `${API_BASE}/slides/${slideHash}/thumbnail.jpeg?max_size=2048`
  // Slide label (the paper label on the physical slide)
  const labelUrl = `${API_BASE}/slides/${slideHash}/label.jpeg?max_size=256`

  // Allow zoom once image is loaded
  const canZoom = imageLoaded

  const handleZoomIn = () => {
    if (!canZoom) return
    setZoom(z => Math.min(z * 1.5, 10))
  }

  const handleZoomOut = () => {
    if (!canZoom) return
    setZoom(z => Math.max(z / 1.5, 0.5))
  }

  const handleReset = () => {
    setZoom(1)
    setPosition({ x: 0, y: 0 })
  }

  // Mouse wheel zoom
  const handleWheel = useCallback((e: React.WheelEvent) => {
    if (!imageLoaded) return
    e.preventDefault()
    const delta = e.deltaY > 0 ? 0.9 : 1.1
    setZoom(z => Math.min(Math.max(z * delta, 0.5), 10))
  }, [imageLoaded])

  // Pan with mouse drag
  const handleMouseDown = (e: React.MouseEvent) => {
    if (!imageLoaded) return
    if (e.button === 0) {
      setIsDragging(true)
      setDragStart({ x: e.clientX - position.x, y: e.clientY - position.y })
    }
  }

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (isDragging) {
      setPosition({
        x: e.clientX - dragStart.x,
        y: e.clientY - dragStart.y
      })
    }
  }, [isDragging, dragStart])

  const handleMouseUp = () => {
    setIsDragging(false)
  }

  // Escape key to close
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [onClose])

  return (
    <div className="fixed inset-0 z-[100] bg-black flex flex-col">
      {/* Header - always on top so close button works */}
      <div className="flex items-center justify-between px-4 py-3 bg-black/80 border-b border-gray-800 relative z-20">
        <div className="text-white">
          <h2 className="font-semibold">{slideName}</h2>
          <p className="text-sm text-gray-400">
            {imageLoaded
              ? `Scroll to zoom, drag to pan • ${Math.round(zoom * 100)}%`
              : 'Loading...'}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon" onClick={handleZoomOut} disabled={!canZoom} className="text-white hover:bg-white/20 disabled:opacity-40">
            <ZoomOut className="h-5 w-5" />
          </Button>
          <Button variant="ghost" size="icon" onClick={handleZoomIn} disabled={!canZoom} className="text-white hover:bg-white/20 disabled:opacity-40">
            <ZoomIn className="h-5 w-5" />
          </Button>
          <Button variant="ghost" size="icon" onClick={handleReset} disabled={!canZoom} className="text-white hover:bg-white/20 disabled:opacity-40">
            <RotateCcw className="h-5 w-5" />
          </Button>
          <div className="w-px h-6 bg-gray-600 mx-2" />
          <Button variant="ghost" size="icon" onClick={onClose} className="text-white hover:bg-white/20">
            <X className="h-5 w-5" />
          </Button>
        </div>
      </div>

      {/* Image viewer */}
      <div
        ref={containerRef}
        className={`flex-1 overflow-hidden relative ${imageLoaded ? 'cursor-grab active:cursor-grabbing' : 'cursor-default'}`}
        onWheel={handleWheel}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
      >
        {/* Loading spinner */}
        {!imageLoaded && !error && (
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="flex items-center gap-2 text-white">
              <Loader2 className="h-6 w-6 animate-spin" />
              <span>Loading thumbnail...</span>
            </div>
          </div>
        )}

        {error && (
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="bg-red-500/20 border border-red-500 text-red-200 px-4 py-3 rounded-lg">
              <p className="font-semibold">Error loading slide</p>
              <p className="text-sm">{error}</p>
            </div>
          </div>
        )}

        {/* Slide label in bottom-left corner (static, not affected by zoom/pan) */}
        {!labelError && (
          <div className="absolute bottom-4 left-4 z-10">
            <div className="bg-black/60 p-1 rounded-lg shadow-lg">
              <img
                src={labelUrl}
                alt="Slide label"
                className={`max-w-[200px] max-h-[150px] rounded transition-opacity duration-300 ${labelLoaded ? 'opacity-100' : 'opacity-0'}`}
                onLoad={() => setLabelLoaded(true)}
                onError={() => setLabelError(true)}
              />
              {!labelLoaded && !labelError && (
                <div className="w-[100px] h-[75px] flex items-center justify-center text-gray-400 text-xs">
                  Loading label...
                </div>
              )}
            </div>
          </div>
        )}

        <div
          className="w-full h-full flex items-center justify-center relative"
          style={{
            transform: `translate(${position.x}px, ${position.y}px) scale(${zoom})`,
            transformOrigin: 'center center',
            transition: isDragging ? 'none' : 'transform 0.1s ease-out'
          }}
        >
          {/* Thumbnail image */}
          <img
            src={thumbnailUrl}
            alt={slideName}
            className={`max-w-none select-none transition-opacity duration-300 ${imageLoaded ? 'opacity-100' : 'opacity-0'}`}
            style={{ imageRendering: zoom > 2 ? 'pixelated' : 'auto' }}
            draggable={false}
            onLoad={() => setImageLoaded(true)}
            onError={() => setError('Failed to load slide thumbnail')}
          />
        </div>
      </div>
    </div>
  )
}
