import { useEffect, useState } from 'react'
import { Eye, FileText, Layers, Loader2, ScatterChart } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { CopyableText } from '@/components/CopyableText'
import { SlideViewerOSD } from '@/components/SlideViewerOSD'
import { ScatterViewerOverlay } from '@/components/ScatterViewerOverlay'
import type { Slide, AnalysisKind } from '@/types/slide'
import { getApiBase, isDemo } from '@/api'
import { displaySlide } from '@/lib/display'

interface AnnotationFile {
  name: string
  size: number
}

interface AnalysisResultRow {
  job_id: number
  analysis_name: string
  version: string
  status: string
  completed_at?: string
  analysis_kind?: string | null
}

interface Props {
  /** Slide hash to display details for. When null, the dialog is closed. */
  slideHash: string | null
  /** Optionally seed metadata from the caller so the header renders instantly,
   *  while the rest of the fields stream in from /slides/{hash}. */
  seed?: Partial<Slide> | null
  onClose: () => void
}

/**
 * Global "quick look" for a slide. Anywhere a slide is mentioned in the app
 * (cohort builder, analysis results, anywhere else) opens this dialog by
 * passing a hash through the `SlideDetailsProvider` — no caller has to
 * carry the slide object or wire up its own dialog.
 *
 * v1 is read-only: metadata + tags + request sheets + completed analyses +
 * annotation list + "View Slide" button. Editing flows (annotation upload,
 * tag management) stay in Slide Library to avoid duplicating that surface.
 */
export function SlideDetailsDialog({ slideHash, seed, onClose }: Props) {
  const [slide, setSlide] = useState<Slide | null>(seed && seed.slide_hash ? { ...(seed as Slide) } : null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [annotations, setAnnotations] = useState<AnnotationFile[]>([])
  const [annLoading, setAnnLoading] = useState(false)
  const [results, setResults] = useState<AnalysisResultRow[]>([])
  const [viewerOpen, setViewerOpen] = useState(false)
  // Registry of analysis-kind plugins, fetched once. Drives which renderer
  // buttons (UMAP / PCA / …) show up next to a Completed-Analyses row.
  const [kinds, setKinds] = useState<AnalysisKind[]>([])
  // When non-null, mounts the ScatterViewerOverlay above the dialog.
  const [scatterViewer, setScatterViewer] = useState<{
    jobId: number; rendererId: string; rendererName: string
  } | null>(null)

  const open = slideHash != null

  useEffect(() => {
    // Kinds are small + stable; load once and reuse across dialog opens.
    fetch(`${getApiBase()}/analyses/kinds`)
      .then(r => r.ok ? r.json() : [])
      .then((data: AnalysisKind[]) => setKinds(Array.isArray(data) ? data : []))
      .catch(() => setKinds([]))
  }, [])

  useEffect(() => {
    if (!open || !slideHash) {
      setSlide(null)
      setError(null)
      setAnnotations([])
      setResults([])
      setViewerOpen(false)
      return
    }

    // Seed the header if the caller passed partial data so the dialog isn't
    // blank during the fetch — common case from cohort/analysis rows.
    if (seed && seed.slide_hash === slideHash) {
      setSlide({ ...(seed as Slide) })
    } else {
      setSlide(null)
    }

    const ac = new AbortController()
    setLoading(true)
    setError(null)
    fetch(`${getApiBase()}/slides/${slideHash}`, { signal: ac.signal })
      .then(async r => {
        if (!r.ok) {
          const detail = await r.json().catch(() => ({}))
          throw new Error(detail.detail || `HTTP ${r.status}`)
        }
        return r.json()
      })
      .then((data: Slide) => { setSlide(data); setLoading(false) })
      .catch(e => {
        if (e.name === 'AbortError') return
        setError(e.message || 'Failed to load slide')
        setLoading(false)
      })

    setAnnLoading(true)
    fetch(`${getApiBase()}/slides/${slideHash}/annotations`, { signal: ac.signal })
      .then(r => r.ok ? r.json() : [])
      .then((data: AnnotationFile[]) => setAnnotations(Array.isArray(data) ? data : []))
      .catch(() => setAnnotations([]))
      .finally(() => setAnnLoading(false))

    fetch(`${getApiBase()}/slides/${slideHash}/results`, { signal: ac.signal })
      .then(r => r.ok ? r.json() : [])
      .then((data: AnalysisResultRow[]) => setResults(Array.isArray(data) ? data : []))
      .catch(() => setResults([]))

    return () => ac.abort()
  }, [slideHash, open, seed])

  // Render an OSD viewer on top if "View Slide" was clicked.
  const slideName = slide ? `${displaySlide(slide)} - ${slide.block_id}-${slide.slide_number} (${slide.stain_type})` : ''

  return (
    <>
      <Dialog open={open && !viewerOpen && !scatterViewer} onOpenChange={(o) => { if (!o) onClose() }}>
        <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Slide Details</DialogTitle>
            <DialogDescription>
              {slide ? displaySlide(slide) : (loading ? 'Loading…' : '')}
            </DialogDescription>
          </DialogHeader>

          {error && (
            <p className="text-sm text-red-600">{error}</p>
          )}

          {!slide && loading && (
            <div className="flex items-center justify-center py-10 text-muted-foreground">
              <Loader2 className="h-5 w-5 animate-spin mr-2" /> Loading slide…
            </div>
          )}

          {slide && (
            <div className="grid grid-cols-2 gap-4 py-2">
              <div>
                <label className="block text-sm text-muted-foreground">{isDemo() ? 'Slide ID:' : 'Accession #:'}</label>
                <CopyableText
                  className="font-medium text-sm"
                  text={isDemo() ? (slide.slide_id || slide.slide_hash.slice(0, 12)) : slide.accession_number}
                />
              </div>
              <div>
                <label className="block text-sm text-muted-foreground">Slide Hash:</label>
                <CopyableText className="text-sm" text={`${slide.slide_hash.substring(0, 16)}...`} copyValue={slide.slide_hash} />
              </div>
              <div>
                <label className="text-sm text-muted-foreground">Slide #</label>
                <p className="font-medium">{slide.slide_number}</p>
              </div>
              <div>
                <label className="text-sm text-muted-foreground">Block</label>
                <p className="font-medium">{slide.block_id}</p>
              </div>
              <div>
                <label className="text-sm text-muted-foreground">Stain Type</label>
                <p className="font-medium">{slide.stain_type}</p>
              </div>
              {slide.year && (
                <div>
                  <label className="text-sm text-muted-foreground">Year</label>
                  <p className="font-medium">{slide.year}</p>
                </div>
              )}
              {slide.random_id && (
                <div>
                  <label className="text-sm text-muted-foreground">Random ID</label>
                  <p className="font-medium font-mono">{slide.random_id}</p>
                </div>
              )}
              {slide.file_path && (
                <div className="col-span-2">
                  <label className="text-sm text-muted-foreground">File Path</label>
                  <CopyableText className="text-sm break-all" text={slide.file_path} />
                </div>
              )}

              {(slide.slide_id || slide.case_id || slide.patient_id) && (
                <div className="col-span-2 border-t pt-3">
                  <label className="text-sm text-muted-foreground">SlideCap IDs</label>
                  <div className="flex flex-wrap gap-3 mt-1">
                    {slide.slide_id && (
                      <CopyableText className="text-xs font-mono px-2 py-1 bg-muted rounded" text={slide.slide_id} />
                    )}
                    {slide.case_id && (
                      <CopyableText className="text-xs font-mono px-2 py-1 bg-muted rounded" text={slide.case_id} />
                    )}
                    {slide.patient_id && (
                      <CopyableText
                        className="text-xs font-mono px-2 py-1 bg-blue-50 dark:bg-blue-950 rounded text-blue-700 dark:text-blue-300"
                        text={slide.patient_id}
                      />
                    )}
                  </div>
                </div>
              )}

              {slide.request_sheets && slide.request_sheets.length > 0 && (
                <div className="col-span-2 border-t pt-3">
                  <label className="text-sm text-muted-foreground">Request Sheets</label>
                  <div className="flex flex-wrap gap-2 mt-1">
                    {slide.request_sheets.map((rs) => (
                      <span
                        key={rs.sheet_id}
                        className="inline-flex items-center gap-1.5 rounded-sm px-2 py-1 text-xs font-medium border border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-800 dark:bg-violet-950 dark:text-violet-300"
                      >
                        {rs.sheet_name || `Sheet #${rs.sheet_id}`}
                        <span className="text-[10px] opacity-70">({rs.case_status || 'Not Started'})</span>
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {slide.slide_tags && slide.slide_tags.length > 0 && (
                <div className="col-span-2">
                  <label className="text-sm text-muted-foreground">Tags</label>
                  <div className="flex flex-wrap gap-2 mt-1">
                    {slide.slide_tags.map((tag) => (
                      <Badge key={tag} variant="secondary">{tag}</Badge>
                    ))}
                  </div>
                </div>
              )}

              {results.length > 0 && (
                <div className="col-span-2 border-t pt-3">
                  <label className="text-sm text-muted-foreground">Completed Analyses</label>
                  {/* One row per completed job. If the analysis's kind plugin
                      declares renderers (e.g. UNI's UMAP/PCA), expose them
                      inline so users can jump straight from "this slide has
                      UNI features" to "view the UMAP". Slide name passed
                      through to the overlay so the OSD viewer header is
                      already redacted. */}
                  <div className="space-y-2 mt-1">
                    {results.map((r) => {
                      const kind = kinds.find(k => k.id === r.analysis_kind)
                      const renderers = kind?.renderers || []
                      return (
                        <div key={`${r.job_id}-${r.analysis_name}`} className="flex flex-wrap items-center gap-2">
                          <Badge variant="outline" title={`Job #${r.job_id} — ${r.status}`}>
                            {r.analysis_name} v{r.version}
                          </Badge>
                          {renderers.map(rd => (
                            <Button
                              key={rd.id}
                              size="sm"
                              variant="outline"
                              className="h-7 text-xs"
                              onClick={() => slide && setScatterViewer({
                                jobId: r.job_id,
                                rendererId: rd.id,
                                rendererName: rd.name,
                              })}
                              title={rd.description}
                            >
                              <ScatterChart className="h-3 w-3 mr-1" />
                              {rd.name}
                            </Button>
                          ))}
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}

              <div className="col-span-2 border-t pt-3">
                <label className="text-sm text-muted-foreground">Annotations</label>
                {annLoading ? (
                  <p className="text-xs text-muted-foreground mt-1">Loading…</p>
                ) : annotations.length === 0 ? (
                  <p className="text-xs text-muted-foreground mt-1">No annotations uploaded.</p>
                ) : (
                  <ul className="mt-1 space-y-1">
                    {annotations.map((a) => (
                      <li key={a.name} className="flex items-center gap-2 text-sm py-0.5">
                        <FileText className="h-3.5 w-3.5 text-orange-500 shrink-0" />
                        <span className="font-mono text-xs truncate flex-1" title={a.name}>{a.name}</span>
                        <span className="text-xs text-muted-foreground shrink-0">
                          {a.size < 1024
                            ? `${a.size} B`
                            : a.size < 1024 * 1024
                              ? `${(a.size / 1024).toFixed(1)} KB`
                              : `${(a.size / (1024 * 1024)).toFixed(1)} MB`}
                        </span>
                      </li>
                    ))}
                  </ul>
                )}
                <p className="text-[11px] text-muted-foreground mt-1">
                  Upload / delete annotations from Slide Library.
                </p>
              </div>

              <div className="col-span-2 pt-3 border-t flex gap-2">
                <Button
                  onClick={() => setViewerOpen(true)}
                  className="flex-1"
                  disabled={!slide.file_path}
                  title={slide.file_path ? 'Open the WSI in the viewer' : 'Slide file not available locally'}
                >
                  <Eye className="mr-2 h-4 w-4" />
                  View Slide
                </Button>
                {annotations.length > 0 && slide.file_path && (
                  <Button
                    variant="outline"
                    onClick={() => setViewerOpen(true)}
                    title="View slide with overlays — coming soon"
                  >
                    <Layers className="mr-2 h-4 w-4" />
                    With overlays
                  </Button>
                )}
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>

      {viewerOpen && slide && (
        <SlideViewerOSD
          slideHash={slide.slide_hash}
          slideName={slideName}
          onClose={() => setViewerOpen(false)}
        />
      )}

      {/* Renderer view (e.g. UMAP scatter linked to this slide's WSI).
          Pops above the dialog when a renderer button is clicked. */}
      {scatterViewer && slide && (
        <ScatterViewerOverlay
          jobId={scatterViewer.jobId}
          slideHash={slide.slide_hash}
          slideName={slideName}
          rendererId={scatterViewer.rendererId}
          rendererName={scatterViewer.rendererName}
          onClose={() => setScatterViewer(null)}
        />
      )}
    </>
  )
}
