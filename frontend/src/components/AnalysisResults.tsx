import { useState } from 'react'
import { Search, FileDown, Image, FileText, FolderOpen, ChevronRight } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'

const API_BASE = 'http://localhost:8000'

interface SlideResult {
  job_id: number
  job_slide_id: number
  analysis_name: string
  version: string
  status: string
  completed_at?: string
  output_path?: string
}

interface SlideWithResults {
  slide_hash: string
  accession_number: string
  block_id: string
  stain_type: string
  year?: number
  results: SlideResult[]
}

interface ResultFile {
  name: string
  size: number
  is_image: boolean
}

export function AnalysisResults() {
  const [query, setQuery] = useState('')
  const [slides, setSlides] = useState<SlideWithResults[]>([])
  const [searchDone, setSearchDone] = useState(false)
  const [loading, setLoading] = useState(false)

  // File browsing state
  const [selectedSlide, setSelectedSlide] = useState<string | null>(null)
  const [selectedJobId, setSelectedJobId] = useState<number | null>(null)
  const [files, setFiles] = useState<ResultFile[]>([])
  const [loadingFiles, setLoadingFiles] = useState(false)
  const [previewUrl, setPreviewUrl] = useState<string | null>(null)

  const searchResults = async () => {
    if (!query.trim()) return
    setLoading(true)
    setSearchDone(false)
    setSelectedSlide(null)
    setSelectedJobId(null)
    setFiles([])
    setPreviewUrl(null)

    try {
      const res = await fetch(`${API_BASE}/results/search?q=${encodeURIComponent(query.trim())}`)
      if (res.ok) {
        const data = await res.json()
        setSlides(data.results)
      } else {
        setSlides([])
      }
    } catch (e) {
      console.error(e)
      setSlides([])
    } finally {
      setLoading(false)
      setSearchDone(true)
    }
  }

  const loadFiles = async (slideHash: string, jobId: number) => {
    setSelectedSlide(slideHash)
    setSelectedJobId(jobId)
    setLoadingFiles(true)
    setPreviewUrl(null)
    try {
      const res = await fetch(
        `${API_BASE}/results/${jobId}/files?slide_hash=${encodeURIComponent(slideHash)}`
      )
      if (res.ok) {
        setFiles(await res.json())
      } else {
        setFiles([])
      }
    } catch (e) {
      console.error(e)
      setFiles([])
    } finally {
      setLoadingFiles(false)
    }
  }

  const previewFile = (jobId: number, slideHash: string, filename: string) => {
    setPreviewUrl(
      `${API_BASE}/results/${jobId}/file/${encodeURIComponent(filename)}?slide_hash=${encodeURIComponent(slideHash)}`
    )
  }

  const downloadFile = (jobId: number, slideHash: string, filename: string) => {
    const url = `${API_BASE}/results/${jobId}/file/${encodeURIComponent(filename)}?slide_hash=${encodeURIComponent(slideHash)}`
    const a = document.createElement('a')
    a.href = url
    a.download = filename
    a.click()
  }

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  }

  return (
    <div className="space-y-6">
      <div className="flex gap-2">
        <Input
          placeholder="Search by accession number (e.g. S24-12345)..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && searchResults()}
          className="max-w-md"
        />
        <Button onClick={searchResults} disabled={loading}>
          <Search className="mr-2 h-4 w-4" />
          {loading ? 'Searching...' : 'Search'}
        </Button>
      </div>

      {searchDone && slides.length === 0 && (
        <p className="text-sm text-muted-foreground">
          No slides with completed analyses found for this query.
        </p>
      )}

      {slides.length > 0 && (
        <div className="grid gap-3">
          <h3 className="text-sm font-medium">
            {slides.length} slide{slides.length !== 1 ? 's' : ''} with results
          </h3>
          {slides.map((slide) => (
            <div key={slide.slide_hash} className="rounded-lg border">
              <div className="flex items-center gap-3 p-3 bg-muted/30">
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <span className="font-medium font-mono text-sm">
                      {slide.accession_number}
                    </span>
                    <Badge variant="secondary">{slide.block_id}</Badge>
                    <Badge variant="outline">{slide.stain_type}</Badge>
                    {slide.year && (
                      <span className="text-xs text-muted-foreground">{slide.year}</span>
                    )}
                  </div>
                  <p className="text-xs text-muted-foreground mt-0.5 font-mono">
                    {slide.slide_hash.substring(0, 16)}...
                  </p>
                </div>
                <Badge variant="secondary">{slide.results.length} analyses</Badge>
              </div>

              <div className="divide-y">
                {slide.results.map((r) => {
                  const isSelected =
                    selectedSlide === slide.slide_hash && selectedJobId === r.job_id
                  return (
                    <div key={`${r.job_id}-${r.job_slide_id}`}>
                      <div
                        className={`flex items-center justify-between px-3 py-2 cursor-pointer transition-colors ${
                          isSelected ? 'bg-primary/5' : 'hover:bg-muted/50'
                        }`}
                        onClick={() => loadFiles(slide.slide_hash, r.job_id)}
                      >
                        <div className="flex items-center gap-2">
                          <ChevronRight
                            className={`h-4 w-4 text-muted-foreground transition-transform ${
                              isSelected ? 'rotate-90' : ''
                            }`}
                          />
                          <FolderOpen className="h-4 w-4 text-muted-foreground" />
                          <span className="text-sm font-medium">{r.analysis_name}</span>
                          <Badge variant="secondary" className="text-xs">
                            v{r.version}
                          </Badge>
                        </div>
                        <div className="flex items-center gap-2">
                          {r.completed_at && (
                            <span className="text-xs text-muted-foreground">
                              {new Date(r.completed_at).toLocaleDateString()}
                            </span>
                          )}
                          <Badge
                            variant="outline"
                            className="bg-green-500/10 text-green-700"
                          >
                            {r.status}
                          </Badge>
                        </div>
                      </div>

                      {isSelected && (
                        <div className="px-6 py-3 bg-muted/20 border-t">
                          {loadingFiles ? (
                            <p className="text-sm text-muted-foreground">Loading files...</p>
                          ) : files.length === 0 ? (
                            <p className="text-sm text-muted-foreground">
                              No output files found.
                            </p>
                          ) : (
                            <div className="grid gap-1">
                              {files.map((f) => (
                                <div
                                  key={f.name}
                                  className="flex items-center justify-between py-1"
                                >
                                  <div className="flex items-center gap-2">
                                    {f.is_image ? (
                                      <Image className="h-3.5 w-3.5 text-blue-500" />
                                    ) : (
                                      <FileText className="h-3.5 w-3.5 text-muted-foreground" />
                                    )}
                                    <span className="text-sm font-mono">{f.name}</span>
                                    <span className="text-xs text-muted-foreground">
                                      {formatSize(f.size)}
                                    </span>
                                  </div>
                                  <div className="flex gap-1">
                                    {f.is_image && (
                                      <Button
                                        variant="ghost"
                                        size="sm"
                                        className="h-7 text-xs"
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          previewFile(r.job_id, slide.slide_hash, f.name)
                                        }}
                                      >
                                        Preview
                                      </Button>
                                    )}
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      className="h-7"
                                      onClick={(e) => {
                                        e.stopPropagation()
                                        downloadFile(r.job_id, slide.slide_hash, f.name)
                                      }}
                                    >
                                      <FileDown className="h-3.5 w-3.5" />
                                    </Button>
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            </div>
          ))}
        </div>
      )}

      {previewUrl && (
        <div className="rounded-lg border p-4">
          <div className="flex justify-between mb-2">
            <h4 className="text-sm font-medium">Preview</h4>
            <Button variant="ghost" size="sm" onClick={() => setPreviewUrl(null)}>
              Close
            </Button>
          </div>
          <img
            src={previewUrl}
            alt="Result preview"
            className="max-w-full max-h-[500px] rounded"
          />
        </div>
      )}
    </div>
  )
}
