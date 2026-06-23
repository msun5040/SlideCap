import { useState, useEffect, useRef, useCallback } from 'react'
import { Search, Filter, Tag as TagIcon, Eye, Tags, X, Plus, Settings, Trash2, ChevronDown, FileDown, Upload, Pencil, Check, FileText, Download, Loader2, Layers, ScatterChart } from 'lucide-react'
import { ScatterViewerOverlay } from '@/components/ScatterViewerOverlay'
import type { AnalysisKind } from '@/types/slide'
import { Button } from '@/components/ui/button'
import { SlideViewerOSD } from '@/components/SlideViewerOSD'
import { TagInput } from '@/components/TagInput'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { TagChip } from '@/components/ui/TagChip'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { Slide, Tag } from '@/types/slide'
import { DownloadModal } from '@/components/DownloadModal'
import { CopyableText } from '@/components/CopyableText'
import { SortableHeader } from '@/components/SortableHeader'
import { useSortable } from '@/hooks/useSortable'

import { getApiBase, normalizeAccession, isDemo } from '@/api'
import { displaySlide } from '@/lib/display'

// Preset colors for tags
const PRESET_COLORS = [
  '#EF4444', // red
  '#F97316', // orange
  '#EAB308', // yellow
  '#22C55E', // green
  '#14B8A6', // teal
  '#3B82F6', // blue
  '#8B5CF6', // purple
  '#EC4899', // pink
  '#6B7280', // gray
]

export function SlideLibrary() {
  const [slides, setSlides] = useState<Slide[]>([])
  const [loading, setLoading] = useState(false)
  const [searchTerm, setSearchTerm] = useState('')
  const [stainFilter, setStainFilter] = useState<string>('all')
  const [yearFilter, setYearFilter] = useState<string>('all')
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const [tagFilter, setTagFilter] = useState<string>('all')
  const [availableTags, setAvailableTags] = useState<Tag[]>([])
  const [selectedSlide, setSelectedSlide] = useState<Slide | null>(null)
  const [isTagDialogOpen, setIsTagDialogOpen] = useState(false)
  const [isDetailsDialogOpen, setIsDetailsDialogOpen] = useState(false)
  const [isViewerOpen, setIsViewerOpen] = useState(false)
  const [slideTags, setSlideTags] = useState<Tag[]>([])
  const [loadingTags, setLoadingTags] = useState(false)
  const [stats, setStats] = useState<{ total_slides: number; total_cases: number } | null>(null)
  const [resultsTruncated, setResultsTruncated] = useState(false)

  // Bulk selection state
  const [selectedSlides, setSelectedSlides] = useState<Set<string>>(new Set())
  const [isBulkTagDialogOpen, setIsBulkTagDialogOpen] = useState(false)
  const [isBulkRemoveTagDialogOpen, setIsBulkRemoveTagDialogOpen] = useState(false)
  const [bulkTagInput, setBulkTagInput] = useState('')
  const [bulkRemoveTagInput, setBulkRemoveTagInput] = useState('')
  const [isBulkTagging, setIsBulkTagging] = useState(false)
  const [bulkTagColor, setBulkTagColor] = useState(PRESET_COLORS[0])
  const [bulkTagSuggestions, setBulkTagSuggestions] = useState<Tag[]>([])
  const [bulkRemoveTagSuggestions, setBulkRemoveTagSuggestions] = useState<Tag[]>([])
  const [showBulkSuggestions, setShowBulkSuggestions] = useState(false)
  const [showBulkRemoveSuggestions, setShowBulkRemoveSuggestions] = useState(false)
  const bulkInputRef = useRef<HTMLInputElement>(null)
  const bulkSuggestionsRef = useRef<HTMLDivElement>(null)
  const bulkRemoveInputRef = useRef<HTMLInputElement>(null)
  const bulkRemoveSuggestionsRef = useRef<HTMLDivElement>(null)

  // Tag management state
  const [isTagManagementOpen, setIsTagManagementOpen] = useState(false)
  const [newTagName, setNewTagName] = useState('')
  const [newTagColor, setNewTagColor] = useState(PRESET_COLORS[0])
  const [isCreatingTag, setIsCreatingTag] = useState(false)
  const [isDeletingTag, setIsDeletingTag] = useState<number | null>(null)
  const [expandedSlideTags, setExpandedSlideTags] = useState<Set<string>>(new Set())
  const [colorPickerForTag, setColorPickerForTag] = useState<number | null>(null)
  // Slides whose QC verdict is 'fail' (manual or auto) — shown as a small marker.
  const [qcFailHashes, setQcFailHashes] = useState<Set<string>>(new Set())

  // Fetch cached QC for the given slides and record which failed (chunked).
  const loadQcFail = async (hashes: string[]) => {
    if (hashes.length === 0) { setQcFailHashes(new Set()); return }
    const fails = new Set<string>()
    for (let i = 0; i < hashes.length; i += 100) {
      const batch = hashes.slice(i, i + 100)
      try {
        const res = await fetch(`${getApiBase()}/qc?slide_hashes=${batch.map(encodeURIComponent).join(',')}`)
        if (res.ok) {
          const data = await res.json()
          for (const [h, q] of Object.entries(data.results || {})) {
            if ((q as { status?: string }).status === 'fail') fails.add(h)
          }
        }
      } catch (e) { console.error('QC fetch failed', e) }
    }
    setQcFailHashes(fails)
  }

  // Download with analysis state
  const [isJobPickerOpen, setIsJobPickerOpen] = useState(false)
  const [jobsList, setJobsList] = useState<{ id: number; model_name: string; model_version: string | null; status: string; completed_count: number; slide_count: number; completed_at: string | null }[]>([])
  const [loadingJobs, setLoadingJobs] = useState(false)
  const [bundleModal, setBundleModal] = useState<{ open: boolean; jobId: number; slideHashes: string[] }>({
    open: false,
    jobId: 0,
    slideHashes: [],
  })

  // Slide results (cell stats) state for detail dialog
  const [slideResults, setSlideResults] = useState<{ job_id: number; analysis_name: string; version: string; status: string; analysis_kind?: string | null; cell_stats?: Record<string, number> | null }[]>([])
  // Kind registry — loaded once, used to surface renderer buttons (UMAP/PCA/…)
  // next to a completed-analysis row when the analysis's kind plugin declares any.
  const [kinds, setKinds] = useState<AnalysisKind[]>([])
  // Active renderer scatter view (e.g. UNI's UMAP). Mounts ScatterViewerOverlay.
  const [scatterViewer, setScatterViewer] = useState<{
    jobId: number; slideHash: string; slideName: string; rendererId: string; rendererName: string
  } | null>(null)
  const [loadingResults, setLoadingResults] = useState(false)
  // Per-job output files & download state inside the Completed Analyses block
  type OutputGroup = { slide_hash: string; label: string; files: string[]; annotation_count: number; is_local: boolean }
  const [analysisOutputs, setAnalysisOutputs] = useState<Record<number, OutputGroup[] | undefined>>({})
  const [loadingOutputJob, setLoadingOutputJob] = useState<number | null>(null)
  const [downloadingJob, setDownloadingJob] = useState<number | null>(null)

  // Overlay viewer state — opens OSD viewer with a chosen analysis output as overlay
  const [overlayViewer, setOverlayViewer] = useState<{
    slideHash: string
    slideName: string
    overlays: { id: string; name: string; type: 'geojson'; url: string }[]
  } | null>(null)

  // Annotation state for detail dialog
  const [annotations, setAnnotations] = useState<{ name: string; size: number }[]>([])
  const [loadingAnnotations, setLoadingAnnotations] = useState(false)
  const [uploadingAnnotation, setUploadingAnnotation] = useState(false)
  const annotationInputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    fetchStats()
  }, [])

  // Fetch bulk tag autocomplete suggestions
  useEffect(() => {
    if (bulkTagInput.length < 1) {
      setBulkTagSuggestions([])
      return
    }

    const fetchSuggestions = async () => {
      try {
        const response = await fetch(`${getApiBase()}/tags/search?q=${encodeURIComponent(bulkTagInput)}`)
        if (response.ok) {
          const data = await response.json()
          setBulkTagSuggestions(data)
        }
      } catch (error) {
        console.error('Failed to fetch tag suggestions:', error)
      }
    }

    const debounce = setTimeout(fetchSuggestions, 150)
    return () => clearTimeout(debounce)
  }, [bulkTagInput])

  // Close bulk tag suggestions when clicking outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        bulkSuggestionsRef.current &&
        !bulkSuggestionsRef.current.contains(e.target as Node) &&
        !bulkInputRef.current?.contains(e.target as Node)
      ) {
        setShowBulkSuggestions(false)
      }
      if (
        bulkRemoveSuggestionsRef.current &&
        !bulkRemoveSuggestionsRef.current.contains(e.target as Node) &&
        !bulkRemoveInputRef.current?.contains(e.target as Node)
      ) {
        setShowBulkRemoveSuggestions(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Fetch bulk remove tag autocomplete suggestions
  useEffect(() => {
    if (bulkRemoveTagInput.length < 1) {
      setBulkRemoveTagSuggestions([])
      return
    }

    const fetchSuggestions = async () => {
      try {
        const response = await fetch(`${getApiBase()}/tags/search?q=${encodeURIComponent(bulkRemoveTagInput)}`)
        if (response.ok) {
          const data = await response.json()
          setBulkRemoveTagSuggestions(data)
        }
      } catch (error) {
        console.error('Failed to fetch tag suggestions:', error)
      }
    }

    const debounce = setTimeout(fetchSuggestions, 150)
    return () => clearTimeout(debounce)
  }, [bulkRemoveTagInput])

  const fetchStats = async () => {
    try {
      const response = await fetch(`${getApiBase()}/stats`)
      if (response.ok) {
        const data = await response.json()
        setStats(data)
      }
    } catch (error) {
      console.error('Failed to fetch stats:', error)
    }
  }

  const fetchAvailableTags = async () => {
    try {
      const response = await fetch(`${getApiBase()}/tags`)
      if (response.ok) {
        const data = await response.json()
        setAvailableTags(data)
      }
    } catch (error) {
      console.error('Failed to fetch tags:', error)
    }
  }

  // Fetch available tags on mount
  useEffect(() => {
    fetchAvailableTags()
  }, [])

  // Fetch analysis-kind plugins once — used to decide which renderer buttons
  // (UMAP / PCA / …) to surface next to a slide's completed-analysis row.
  useEffect(() => {
    fetch(`${getApiBase()}/analyses/kinds`)
      .then(r => r.ok ? r.json() : [])
      .then((data: AnalysisKind[]) => setKinds(Array.isArray(data) ? data : []))
      .catch(() => setKinds([]))
  }, [])

  const handleSearch = async () => {
    setLoading(true)
    setSelectedSlides(new Set()) // Clear selection on new search
    try {
      const raw = searchTerm.trim()
      const queries = raw.includes(',') ? raw.split(',').map(s => s.trim()).filter(Boolean) : [raw]

      if (queries.length <= 1) {
        // Single query — original behavior
        const params = new URLSearchParams()
        if (raw) params.append('q', normalizeAccession(raw))
        if (yearFilter !== 'all') params.append('year', yearFilter)
        if (stainFilter !== 'all') params.append('stain', stainFilter)
        if (tagFilter !== 'all') params.append('tag', tagFilter)

        const response = await fetch(`${getApiBase()}/search?${params.toString()}`)
        if (response.ok) {
          const data = await response.json()
          setSlides(data.results)
          setResultsTruncated(data.truncated || false)
          loadQcFail((data.results || []).map((s: Slide) => s.slide_hash))
        }
      } else {
        // Multiple comma-separated accessions
        const allResults: typeof slides = []
        const seen = new Set<string>()
        for (const q of queries) {
          const params = new URLSearchParams()
          params.append('q', normalizeAccession(q))
          if (yearFilter !== 'all') params.append('year', yearFilter)
          if (stainFilter !== 'all') params.append('stain', stainFilter)
          if (tagFilter !== 'all') params.append('tag', tagFilter)

          const response = await fetch(`${getApiBase()}/search?${params.toString()}`)
          if (response.ok) {
            const data = await response.json()
            for (const slide of data.results || []) {
              if (!seen.has(slide.slide_hash)) {
                seen.add(slide.slide_hash)
                allResults.push(slide)
              }
            }
          }
        }
        setSlides(allResults)
        setResultsTruncated(false)
        loadQcFail(allResults.map(s => s.slide_hash))
      }
    } catch (error) {
      console.error('Search error:', error)
    } finally {
      setLoading(false)
      // Refresh available tags so badge colors stay current with the latest tag data
      fetchAvailableTags()
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch()
    }
  }

  const matchesStainFilter = (stain: string, filter: string): boolean => {
    if (filter === 'all') return true
    if (filter === 'HE') return stain === 'HE'
    if (filter === 'IHC') return stain.startsWith('IHC')
    if (filter === 'Special') return stain !== 'HE' && !stain.startsWith('IHC')
    return true
  }

  const filteredSlides = slides.filter((slide) => {
    const matchesStain = matchesStainFilter(slide.stain_type, stainFilter)
    const matchesStatus = statusFilter === 'all' || slide.status === statusFilter
    return matchesStain && matchesStatus
  })

  const { sorted: sortedSlides, sortConfig, handleSort } = useSortable(filteredSlides)

  const openTagDialog = async (slide: Slide, e: React.MouseEvent) => {
    e.stopPropagation()
    setSelectedSlide(slide)
    setSlideTags([])  // Clear previous tags
    setLoadingTags(true)
    setIsTagDialogOpen(true)

    // Fetch full tag details for this slide
    try {
      const response = await fetch(`${getApiBase()}/slides/${slide.slide_hash}/tags`)
      if (response.ok) {
        const tags = await response.json()
        setSlideTags(tags)
      } else {
        console.error('Failed to fetch tags:', response.status)
      }
    } catch (error) {
      console.error('Failed to fetch slide tags:', error)
    } finally {
      setLoadingTags(false)
    }
  }

  const handleTagsChange = (newTags: Tag[]) => {
    setSlideTags(newTags)
    // Update the slide in the list to reflect new tags
    if (selectedSlide) {
      setSlides(slides.map(s =>
        s.slide_hash === selectedSlide.slide_hash
          ? { ...s, slide_tags: newTags.map(t => t.name) }
          : s
      ))
    }
    // Refresh available tags list in case a new tag was created
    fetchAvailableTags()
  }

  const openDetailsDialog = (slide: Slide) => {
    setSelectedSlide(slide)
    setIsDetailsDialogOpen(true)
  }

  // Bulk selection handlers
  const toggleSlideSelection = (slideHash: string) => {
    const newSelected = new Set(selectedSlides)
    if (newSelected.has(slideHash)) {
      newSelected.delete(slideHash)
    } else {
      newSelected.add(slideHash)
    }
    setSelectedSlides(newSelected)
  }

  const toggleSelectAll = () => {
    if (selectedSlides.size === sortedSlides.length) {
      setSelectedSlides(new Set())
    } else {
      setSelectedSlides(new Set(sortedSlides.map(s => s.slide_hash)))
    }
  }

  const clearSelection = () => {
    setSelectedSlides(new Set())
  }

  const openDownloadWithAnalysis = async () => {
    setIsJobPickerOpen(true)
    setLoadingJobs(true)
    try {
      const hashParam = Array.from(selectedSlides).join(',')
      const res = await fetch(`${getApiBase()}/jobs?limit=50&slide_hashes=${encodeURIComponent(hashParam)}`)
      if (res.ok) {
        const data = await res.json()
        setJobsList(data.filter((j: any) => j.completed_count > 0))
      }
    } catch (e) {
      console.error(e)
    } finally {
      setLoadingJobs(false)
    }
  }

  const pickJobForBundle = (jobId: number) => {
    setIsJobPickerOpen(false)
    setBundleModal({
      open: true,
      jobId,
      slideHashes: Array.from(selectedSlides),
    })
  }

  // --- Annotation helpers ---

  const fetchAnnotations = useCallback(async (slideHash: string) => {
    setLoadingAnnotations(true)
    try {
      const res = await fetch(`${getApiBase()}/slides/${slideHash}/annotations`)
      if (res.ok) setAnnotations(await res.json())
      else setAnnotations([])
    } catch {
      setAnnotations([])
    } finally {
      setLoadingAnnotations(false)
    }
  }, [])

  const fetchAnalysisOutputs = useCallback(async (jobId: number, slideHash: string) => {
    setLoadingOutputJob(jobId)
    try {
      const res = await fetch(`${getApiBase()}/jobs/${jobId}/output-filenames?slide_hashes=${encodeURIComponent(slideHash)}`)
      if (res.ok) {
        const data: OutputGroup[] = await res.json()
        setAnalysisOutputs((prev) => ({ ...prev, [jobId]: data }))
      }
    } catch (e) { console.error('Failed to load output files:', e) }
    finally { setLoadingOutputJob(null) }
  }, [])

  const isOverlayFile = (name: string) => {
    const lower = name.toLowerCase()
    return lower.endsWith('.geojson')
      || lower.endsWith('.geojson.snappy')
      || lower.endsWith('.json.gz')
      || lower.endsWith('.geojson.gz')
  }

  const openOverlayFile = useCallback((jobId: number, slideHash: string, slideName: string, filePath: string) => {
    const encoded = filePath.split('/').map(encodeURIComponent).join('/')
    const url = `${getApiBase()}/results/${jobId}/file/${encoded}?slide_hash=${encodeURIComponent(slideHash)}&apply_transforms=true`
    const fileName = filePath.split('/').pop() || filePath
    setOverlayViewer({
      slideHash,
      slideName,
      overlays: [{ id: `${jobId}-${slideHash}-${filePath}`, name: fileName, type: 'geojson', url }],
    })
  }, [])

  const downloadAnalysisZip = useCallback(async (jobId: number) => {
    setDownloadingJob(jobId)
    try {
      const res = await fetch(`${getApiBase()}/jobs/${jobId}/download-zip`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `job_${jobId}_results.zip`
      a.click()
      URL.revokeObjectURL(url)
    } catch (e) { console.error('Download failed:', e) }
    finally { setDownloadingJob(null) }
  }, [])

  const uploadAnnotation = async (slideHash: string, file: File) => {
    setUploadingAnnotation(true)
    try {
      const form = new FormData()
      form.append('file', file)
      const res = await fetch(`${getApiBase()}/slides/${slideHash}/annotations`, {
        method: 'POST',
        body: form,
      })
      if (res.ok) {
        await fetchAnnotations(slideHash)
      } else {
        const err = await res.json().catch(() => ({ detail: 'Upload failed' }))
        alert(err.detail || 'Upload failed')
      }
    } catch {
      alert('Upload failed')
    } finally {
      setUploadingAnnotation(false)
    }
  }

  const [deleteAnnotationConfirm, setDeleteAnnotationConfirm] = useState<{
    open: boolean; slideHash: string; filename: string
  }>({ open: false, slideHash: '', filename: '' })

  const confirmDeleteAnnotation = (slideHash: string, filename: string) => {
    setDeleteAnnotationConfirm({ open: true, slideHash, filename })
  }

  const doDeleteAnnotation = async () => {
    const { slideHash, filename } = deleteAnnotationConfirm
    try {
      const res = await fetch(
        `${getApiBase()}/slides/${slideHash}/annotations/${encodeURIComponent(filename)}`,
        { method: 'DELETE' }
      )
      if (res.ok) {
        setAnnotations((prev) => prev.filter((a) => a.name !== filename))
      }
    } catch {
      alert('Delete failed')
    }
    setDeleteAnnotationConfirm({ open: false, slideHash: '', filename: '' })
  }

  // Load annotations and results when detail dialog opens
  useEffect(() => {
    if (isDetailsDialogOpen && selectedSlide) {
      fetchAnnotations(selectedSlide.slide_hash)
      // Fetch cell stats
      setLoadingResults(true)
      fetch(`${getApiBase()}/slides/${selectedSlide.slide_hash}/results`)
        .then((res) => (res.ok ? res.json() : []))
        .then((data) => setSlideResults(data))
        .catch(() => setSlideResults([]))
        .finally(() => setLoadingResults(false))
      setAnalysisOutputs({})
    }
  }, [isDetailsDialogOpen, selectedSlide, fetchAnnotations])

  const handleBulkAddTag = async (tagName?: string, tagColor?: string) => {
    const name = tagName || bulkTagInput.trim()
    const color = tagColor || bulkTagColor
    if (!name || selectedSlides.size === 0) return

    setIsBulkTagging(true)
    setShowBulkSuggestions(false)
    try {
      const response = await fetch(`${getApiBase()}/slides/bulk/tags/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          slide_hashes: Array.from(selectedSlides),
          tags: [name],
          color: color
        })
      })

      if (response.ok) {
        const data = await response.json()

        // Optimistically update local slide state immediately
        setSlides(prevSlides => prevSlides.map(slide => {
          if (selectedSlides.has(slide.slide_hash)) {
            const currentTags = slide.slide_tags || []
            if (!currentTags.includes(name)) {
              return { ...slide, slide_tags: [...currentTags, name] }
            }
          }
          return slide
        }))

        setBulkTagInput('')
        setIsBulkTagDialogOpen(false)

        // Also refresh available tags list
        fetchAvailableTags()

        // Small delay then refresh search to get server state (for network drive sync)
        setTimeout(() => handleSearch(), 500)
      }
    } catch (error) {
      console.error('Failed to bulk add tag:', error)
      alert('Failed to add tag to slides')
    } finally {
      setIsBulkTagging(false)
    }
  }

  const selectBulkSuggestion = (tag: Tag) => {
    handleBulkAddTag(tag.name, tag.color || undefined)
  }

  const handleBulkKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && bulkTagInput.trim()) {
      e.preventDefault()
      // If there's an exact match in suggestions, use that
      const exactMatch = bulkTagSuggestions.find(s => s.name.toLowerCase() === bulkTagInput.toLowerCase())
      if (exactMatch) {
        handleBulkAddTag(exactMatch.name, exactMatch.color || undefined)
      } else {
        handleBulkAddTag(bulkTagInput, bulkTagColor)
      }
    } else if (e.key === 'Escape') {
      setShowBulkSuggestions(false)
    }
  }

  // Check if bulk input matches an existing tag
  const isBulkExistingTag = bulkTagSuggestions.some(s => s.name.toLowerCase() === bulkTagInput.toLowerCase())

  // Bulk remove tag handlers
  const handleBulkRemoveTag = async (tagName?: string) => {
    const name = tagName || bulkRemoveTagInput.trim()
    if (!name || selectedSlides.size === 0) return

    setIsBulkTagging(true)
    setShowBulkRemoveSuggestions(false)
    try {
      const response = await fetch(`${getApiBase()}/slides/bulk/tags/remove`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          slide_hashes: Array.from(selectedSlides),
          tags: [name]
        })
      })

      if (response.ok) {
        // Optimistically update local slide state immediately
        setSlides(prevSlides => prevSlides.map(slide => {
          if (selectedSlides.has(slide.slide_hash)) {
            const currentTags = slide.slide_tags || []
            return { ...slide, slide_tags: currentTags.filter(t => t !== name) }
          }
          return slide
        }))

        setBulkRemoveTagInput('')
        setIsBulkRemoveTagDialogOpen(false)

        // Refresh available tags list
        fetchAvailableTags()

        // Small delay then refresh search
        setTimeout(() => handleSearch(), 500)
      }
    } catch (error) {
      console.error('Failed to bulk remove tag:', error)
      alert('Failed to remove tag from slides')
    } finally {
      setIsBulkTagging(false)
    }
  }

  const selectBulkRemoveSuggestion = (tag: Tag) => {
    handleBulkRemoveTag(tag.name)
  }

  const handleBulkRemoveKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && bulkRemoveTagInput.trim()) {
      e.preventDefault()
      const exactMatch = bulkRemoveTagSuggestions.find(s => s.name.toLowerCase() === bulkRemoveTagInput.toLowerCase())
      if (exactMatch) {
        handleBulkRemoveTag(exactMatch.name)
      } else {
        handleBulkRemoveTag(bulkRemoveTagInput)
      }
    } else if (e.key === 'Escape') {
      setShowBulkRemoveSuggestions(false)
    }
  }

  const isBulkRemoveExistingTag = bulkRemoveTagSuggestions.some(s => s.name.toLowerCase() === bulkRemoveTagInput.toLowerCase())

  // Tag management functions
  const handleCreateTag = async () => {
    if (!newTagName.trim()) return

    setIsCreatingTag(true)
    try {
      const response = await fetch(`${getApiBase()}/tags`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: newTagName.trim(),
          color: newTagColor
        })
      })

      if (response.ok) {
        setNewTagName('')
        setNewTagColor(PRESET_COLORS[0])
        fetchAvailableTags()
      } else {
        const data = await response.json()
        alert(data.detail || 'Failed to create tag')
      }
    } catch (error) {
      console.error('Failed to create tag:', error)
      alert('Failed to create tag')
    } finally {
      setIsCreatingTag(false)
    }
  }

  const handleDeleteTag = async (tagId: number, tagName: string) => {
    if (!confirm(`Delete tag "${tagName}"? This will remove it from all slides.`)) return

    setIsDeletingTag(tagId)
    try {
      const response = await fetch(`${getApiBase()}/tags/${tagId}`, {
        method: 'DELETE'
      })

      if (response.ok || response.status === 404) {
        // 404 means tag was already deleted (e.g., double-click) - treat as success
        fetchAvailableTags()
        // If we're filtering by this tag, clear the filter
        if (tagFilter === tagName) {
          setTagFilter('all')
        }
        // Optimistically remove tag from local slides
        setSlides(prevSlides => prevSlides.map(slide => ({
          ...slide,
          slide_tags: (slide.slide_tags || []).filter(t => t !== tagName)
        })))
        // Refresh search results after a delay
        if (slides.length > 0) {
          setTimeout(() => handleSearch(), 500)
        }
      } else {
        alert('Failed to delete tag')
      }
    } catch (error) {
      console.error('Failed to delete tag:', error)
      alert('Failed to delete tag')
    } finally {
      setIsDeletingTag(null)
    }
  }

  // Tag rename state
  const [editingTagId, setEditingTagId] = useState<number | null>(null)
  const [editingTagName, setEditingTagName] = useState('')
  const [savingTagName, setSavingTagName] = useState(false)

  const startEditTag = (tag: { id: number; name: string }) => {
    setEditingTagId(tag.id)
    setEditingTagName(tag.name)
  }

  const cancelEditTag = () => {
    setEditingTagId(null)
    setEditingTagName('')
  }

  const saveTagName = async (tagId: number, oldName: string) => {
    const newName = editingTagName.trim()
    if (!newName || newName === oldName) {
      cancelEditTag()
      return
    }
    setSavingTagName(true)
    try {
      const res = await fetch(`${getApiBase()}/tags/${tagId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newName }),
      })
      if (res.ok) {
        fetchAvailableTags()
        // Update local slides state
        setSlides((prev) =>
          prev.map((s) => ({
            ...s,
            slide_tags: (s.slide_tags || []).map((t) => (t === oldName ? newName : t)),
          }))
        )
        // Update tag filter if active
        if (tagFilter === oldName) setTagFilter(newName)
        cancelEditTag()
      } else {
        const err = await res.json()
        alert(err.detail || 'Failed to rename tag')
      }
    } catch {
      alert('Failed to rename tag')
    } finally {
      setSavingTagName(false)
    }
  }

  const updateTagColor = async (tagId: number, color: string) => {
    // Optimistic update
    setAvailableTags(prev => prev.map(t => t.id === tagId ? { ...t, color } : t))
    setColorPickerForTag(null)
    try {
      const res = await fetch(`${getApiBase()}/tags/${tagId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ color }),
      })
      if (!res.ok) {
        // Revert on failure
        fetchAvailableTags()
        alert('Failed to update tag color')
      }
    } catch {
      fetchAvailableTags()
      alert('Failed to update tag color')
    }
  }

  const toggleExpandedTags = (slideHash: string, e: React.MouseEvent) => {
    e.stopPropagation()
    const newExpanded = new Set(expandedSlideTags)
    if (newExpanded.has(slideHash)) {
      newExpanded.delete(slideHash)
    } else {
      newExpanded.add(slideHash)
    }
    setExpandedSlideTags(newExpanded)
  }

  const years = ['2024', '2023', '2022', '2021', '2020']
  const stainTypes = ['HE', 'IHC', 'Special']

  return (
    <div className="h-full flex flex-col gap-6 min-h-0">
      <div>
        <h1 className="text-2xl font-semibold mb-2">Slide Library</h1>
        <p className="text-muted-foreground">
          Browse and search your slide collection
          {stats && ` - ${stats.total_slides} slides, ${stats.total_cases} cases`}
        </p>
      </div>

      <div className="flex flex-col gap-4 lg:flex-row">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Search by accession number, slide ID, patient ID..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            onKeyDown={handleKeyDown}
            className="pl-10"
          />
        </div>
        <div className="flex flex-wrap gap-2">
          <Select value={stainFilter} onValueChange={setStainFilter}>
            <SelectTrigger className="w-35">
              <Filter className="mr-2 h-4 w-4" />
              <SelectValue placeholder="Stain" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Stains</SelectItem>
              {stainTypes.map((stain) => (
                <SelectItem key={stain} value={stain}>
                  {stain}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Select value={yearFilter} onValueChange={setYearFilter}>
            <SelectTrigger className="w-30">
              <SelectValue placeholder="Year" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Years</SelectItem>
              {years.map((year) => (
                <SelectItem key={year} value={year}>
                  {year}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Select value={statusFilter} onValueChange={setStatusFilter}>
            <SelectTrigger className="w-32.5">
              <SelectValue placeholder="Status" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Status</SelectItem>
              <SelectItem value="available">Available</SelectItem>
              <SelectItem value="in-analysis">In Analysis</SelectItem>
              <SelectItem value="archived">Archived</SelectItem>
            </SelectContent>
          </Select>

          <Select
            value={tagFilter}
            onValueChange={(value) => {
              if (value === '__manage__') {
                setIsTagManagementOpen(true)
              } else {
                setTagFilter(value)
              }
            }}
            onOpenChange={(open) => { if (open) fetchAvailableTags() }}
          >
            <SelectTrigger className="w-36">
              <TagIcon className="mr-2 h-4 w-4" />
              <SelectValue placeholder="Tag" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Tags</SelectItem>
              {availableTags.map((tag) => (
                <SelectItem key={tag.id} value={tag.name}>
                  <div className="flex items-center gap-2">
                    {tag.color && (
                      <span
                        className="w-2 h-2 rounded-full"
                        style={{ backgroundColor: tag.color }}
                      />
                    )}
                    {tag.name}
                    <span className="text-muted-foreground text-xs">({tag.slide_count ?? 0})</span>
                  </div>
                </SelectItem>
              ))}
              <div className="border-t my-1" />
              <SelectItem value="__manage__">
                <div className="flex items-center gap-2">
                  <Settings className="h-3 w-3" />
                  Manage Tags...
                </div>
              </SelectItem>
            </SelectContent>
          </Select>

          <Button onClick={handleSearch} disabled={loading}>
            {loading ? 'Searching...' : 'Search'}
          </Button>
        </div>
      </div>

      <div className="flex items-center justify-between">
        <div className="text-sm text-muted-foreground">
          Showing {sortedSlides.length} slides
          {resultsTruncated && (
            <span className="ml-1 text-orange-600">(limit reached - refine your search)</span>
          )}
          {selectedSlides.size > 0 && (
            <span className="ml-2 text-foreground font-medium">
              ({selectedSlides.size} selected)
            </span>
          )}
        </div>

        {/* Bulk action bar */}
        {selectedSlides.size > 0 && (
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setIsBulkTagDialogOpen(true)}
            >
              <Tags className="mr-1 h-4 w-4" />
              Add Tag
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setIsBulkRemoveTagDialogOpen(true)}
            >
              <X className="mr-1 h-4 w-4" />
              Remove Tag
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={openDownloadWithAnalysis}
            >
              <FileDown className="mr-1 h-4 w-4" />
              Download with Analysis
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={clearSelection}
            >
              Clear Selection
            </Button>
          </div>
        )}
      </div>

      <div className="rounded-lg border overflow-hidden flex-1 min-h-0">
        <Table containerClassName="h-full">
          <TableHeader className="sticky top-0 z-10 [&_th]:bg-muted/95 [&_th]:backdrop-blur-sm [&_tr]:border-b shadow-[0_1px_0_0_var(--border)]">
            <TableRow>
              <TableHead className="w-12.5">
                <Checkbox
                  checked={sortedSlides.length > 0 && selectedSlides.size === sortedSlides.length}
                  onCheckedChange={toggleSelectAll}
                />
              </TableHead>
              <TableHead><SortableHeader label="Accession #" sortKey="accession_number" sortConfig={sortConfig} onSort={handleSort} /></TableHead>
              <TableHead><SortableHeader label="Block" sortKey="block_id" sortConfig={sortConfig} onSort={handleSort} /></TableHead>
              <TableHead><SortableHeader label="Slide #" sortKey="slide_number" sortConfig={sortConfig} onSort={handleSort} /></TableHead>
              <TableHead><SortableHeader label="Stain" sortKey="stain_type" sortConfig={sortConfig} onSort={handleSort} /></TableHead>
              <TableHead><SortableHeader label="Year" sortKey="year" sortConfig={sortConfig} onSort={handleSort} /></TableHead>
              <TableHead>Tags</TableHead>
              <TableHead>Analyses</TableHead>
              <TableHead className="w-12 text-right" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {sortedSlides.length === 0 ? (
              <TableRow>
                <TableCell colSpan={9} className="h-24 text-center">
                  {slides.length === 0
                    ? 'Search for slides to get started.'
                    : 'No slides found matching your criteria.'}
                </TableCell>
              </TableRow>
            ) : (
              sortedSlides.map((slide) => (
                <TableRow
                  key={slide.slide_hash}
                  className={`hover:bg-muted/50 cursor-pointer ${selectedSlides.has(slide.slide_hash) ? 'bg-muted/30' : ''}`}
                  onClick={() => openDetailsDialog(slide)}
                >
                  <TableCell onClick={(e) => e.stopPropagation()}>
                    <Checkbox
                      checked={selectedSlides.has(slide.slide_hash)}
                      onCheckedChange={() => toggleSlideSelection(slide.slide_hash)}
                    />
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-1.5">
                      <CopyableText
                        className="font-medium text-sm"
                        mono={false}
                        text={isDemo() ? (slide.slide_id || slide.slide_hash.slice(0, 12)) : slide.accession_number}
                      />
                      {slide.request_sheets && slide.request_sheets.length > 0 && (
                        <span className="inline-flex items-center rounded px-1 py-0.5 text-[9px] font-medium bg-violet-100 text-violet-700 dark:bg-violet-900 dark:text-violet-300 whitespace-nowrap" title={slide.request_sheets.map(rs => rs.sheet_name).join(', ')}>
                          REQ
                        </span>
                      )}
                      {qcFailHashes.has(slide.slide_hash) && (
                        <span
                          className="inline-flex items-center gap-0.5 rounded px-1 py-0.5 text-[9px] font-semibold bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300 whitespace-nowrap"
                          title="Failed QC"
                        >
                          QC✗
                        </span>
                      )}
                    </div>
                    {!isDemo() && slide.slide_id && (
                      <span className="block text-[10px] font-mono text-muted-foreground/60 mt-0.5">{slide.slide_id}</span>
                    )}
                  </TableCell>
                  <TableCell className="text-sm">{slide.block_id}</TableCell>
                  <TableCell className="text-sm">{slide.slide_number}</TableCell>
                  <TableCell>
                    <span className="inline-flex items-center h-[21px] px-2 rounded-sm border border-border font-mono text-[11px] font-medium text-foreground">
                      {slide.stain_type}
                    </span>
                  </TableCell>
                  <TableCell className="text-sm">{slide.year || '-'}</TableCell>
                  <TableCell onClick={(e) => e.stopPropagation()}>
                    <div className="flex items-center gap-1 flex-wrap">
                      {slide.slide_tags && slide.slide_tags.length > 0 ? (
                        <>
                          {/* Show first 2 tags, or all if expanded */}
                          {(expandedSlideTags.has(slide.slide_hash)
                            ? slide.slide_tags
                            : slide.slide_tags.slice(0, 2)
                          ).map((tagName: string) => {
                            const tagInfo = availableTags.find(t => t.name === tagName)
                            return (
                              <span key={tagName} onClick={(e) => openTagDialog(slide, e)} className="cursor-pointer hover:opacity-80">
                                <TagChip name={tagName} color={tagInfo?.color} />
                              </span>
                            )
                          })}
                          {/* Show +N more button if there are more than 2 tags */}
                          {slide.slide_tags.length > 2 && !expandedSlideTags.has(slide.slide_hash) && (
                            <Badge
                              variant="outline"
                              className="text-xs cursor-pointer hover:bg-muted"
                              onClick={(e) => toggleExpandedTags(slide.slide_hash, e)}
                            >
                              +{slide.slide_tags.length - 2}
                            </Badge>
                          )}
                          {/* Show collapse button if expanded */}
                          {slide.slide_tags.length > 2 && expandedSlideTags.has(slide.slide_hash) && (
                            <Badge
                              variant="outline"
                              className="text-xs cursor-pointer hover:bg-muted"
                              onClick={(e) => toggleExpandedTags(slide.slide_hash, e)}
                            >
                              <ChevronDown className="h-3 w-3" />
                            </Badge>
                          )}
                        </>
                      ) : (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={(e) => openTagDialog(slide, e)}
                          className="h-6 px-2 text-muted-foreground hover:text-foreground"
                        >
                          <Plus className="h-3 w-3 mr-1" />
                          Add
                        </Button>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-1 flex-wrap">
                      {slide.completed_analyses && slide.completed_analyses.length > 0 ? (
                        slide.completed_analyses.map((name: string) => (
                          <Badge
                            key={name}
                            variant="secondary"
                            className="text-xs"
                            style={{
                              backgroundColor: '#3B82F620',
                              color: '#3B82F6',
                              borderColor: '#3B82F6',
                            }}
                          >
                            {name}
                          </Badge>
                        ))
                      ) : (
                        <span className="text-xs text-muted-foreground">-</span>
                      )}
                    </div>
                  </TableCell>
                  <TableCell className="text-right" onClick={(e) => e.stopPropagation()}>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 text-muted-foreground hover:text-foreground"
                      title="View slide"
                      onClick={() => { setSelectedSlide(slide); setIsViewerOpen(true) }}
                    >
                      <Eye className="h-4 w-4" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {/* Tag Dialog */}
      <Dialog open={isTagDialogOpen} onOpenChange={setIsTagDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Manage Tags</DialogTitle>
            <DialogDescription>
              {selectedSlide && displaySlide(selectedSlide)} - Block {selectedSlide?.block_id}, Slide {selectedSlide?.slide_number}
            </DialogDescription>
          </DialogHeader>
          {loadingTags ? (
            <div className="py-8 text-center text-muted-foreground">Loading tags...</div>
          ) : selectedSlide && (
            <TagInput
              slideHash={selectedSlide.slide_hash}
              currentTags={slideTags}
              onTagsChange={handleTagsChange}
            />
          )}
        </DialogContent>
      </Dialog>

      {/* Slide Details Dialog */}
      <Dialog open={isDetailsDialogOpen} onOpenChange={setIsDetailsDialogOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Slide Details</DialogTitle>
            <DialogDescription>
              {selectedSlide && displaySlide(selectedSlide)}
            </DialogDescription>
          </DialogHeader>
          {selectedSlide && (
            <div className="grid grid-cols-2 gap-4 py-4">
              <div>
                <label className="block text-sm text-muted-foreground">{isDemo() ? 'Slide ID:' : 'Accession #:'}</label>
                <CopyableText
                  className="font-medium text-sm"
                  text={isDemo() ? (selectedSlide.slide_id || selectedSlide.slide_hash.slice(0, 12)) : selectedSlide.accession_number}
                />
              </div>
              <div>
                <label className="block text-sm text-muted-foreground">Slide Hash:</label>
                <CopyableText className="text-sm" text={`${selectedSlide.slide_hash.substring(0, 16)}...`} copyValue={selectedSlide.slide_hash} />
              </div>
              <div>
                <label className="text-sm text-muted-foreground">Slide #</label>
                <p className="font-medium">{selectedSlide.slide_number}</p>
              </div>
              <div>
                <label className="text-sm text-muted-foreground">Block</label>
                <p className="font-medium">{selectedSlide.block_id}</p>
              </div>
              <div>
                <label className="text-sm text-muted-foreground">Stain Type</label>
                <p className="font-medium">{selectedSlide.stain_type}</p>
              </div>
              {selectedSlide.year && (
                <div>
                  <label className="text-sm text-muted-foreground">Year</label>
                  <p className="font-medium">{selectedSlide.year}</p>
                </div>
              )}
              {selectedSlide.random_id && (
                <div>
                  <label className="text-sm text-muted-foreground">Random ID</label>
                  <p className="font-medium font-mono">{selectedSlide.random_id}</p>
                </div>
              )}
              {selectedSlide.file_path && (
                <div className="col-span-2">
                  <label className="text-sm text-muted-foreground">File Path</label>
                  <CopyableText className="text-sm break-all" text={selectedSlide.file_path} />
                </div>
              )}
              {/* SlideCap IDs */}
              {(selectedSlide.slide_id || selectedSlide.case_id || selectedSlide.patient_id) && (
                <div className="col-span-2 border-t pt-3">
                  <label className="text-sm text-muted-foreground">SlideCap IDs</label>
                  <div className="flex flex-wrap gap-3 mt-1">
                    {selectedSlide.slide_id && (
                      <CopyableText className="text-xs font-mono px-2 py-1 bg-muted rounded" text={selectedSlide.slide_id} />
                    )}
                    {selectedSlide.case_id && (
                      <CopyableText className="text-xs font-mono px-2 py-1 bg-muted rounded" text={selectedSlide.case_id} />
                    )}
                    {selectedSlide.patient_id && (
                      <CopyableText className="text-xs font-mono px-2 py-1 bg-blue-50 dark:bg-blue-950 rounded text-blue-700 dark:text-blue-300" text={selectedSlide.patient_id} />
                    )}
                  </div>
                </div>
              )}
              {selectedSlide.request_sheets && selectedSlide.request_sheets.length > 0 && (
                <div className="col-span-2 border-t pt-3">
                  <label className="text-sm text-muted-foreground">Request Sheets</label>
                  <div className="flex flex-wrap gap-2 mt-1">
                    {selectedSlide.request_sheets.map((rs) => (
                      <span key={rs.sheet_id} className="inline-flex items-center gap-1.5 rounded-sm px-2 py-1 text-xs font-medium border border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-800 dark:bg-violet-950 dark:text-violet-300">
                        {rs.sheet_name || `Sheet #${rs.sheet_id}`}
                        <span className="text-[10px] opacity-70">({rs.case_status || 'Not Started'})</span>
                      </span>
                    ))}
                  </div>
                </div>
              )}
              {selectedSlide.slide_tags && selectedSlide.slide_tags.length > 0 && (
                <div className="col-span-2">
                  <label className="text-sm text-muted-foreground">Tags</label>
                  <div className="flex flex-wrap gap-2 mt-1">
                    {selectedSlide.slide_tags.map((tag: string) => {
                      const tagInfo = availableTags.find(t => t.name === tag)
                      return <TagChip key={tag} name={tag} color={tagInfo?.color} />
                    })}
                  </div>
                </div>
              )}
              {selectedSlide.completed_analyses && selectedSlide.completed_analyses.length > 0 && (
                <div className="col-span-2">
                  <label className="text-sm text-muted-foreground">Completed Analyses</label>
                  <div className="space-y-2 mt-1">
                    {loadingResults ? (
                      <p className="text-xs text-muted-foreground">Loading results...</p>
                    ) : slideResults.length > 0 ? (
                      slideResults.map((r) => {
                        const files = analysisOutputs[r.job_id]
                        return (
                          <div key={`${r.job_id}-${r.analysis_name}`} className="rounded border p-2 space-y-2">
                            <div className="flex items-center gap-2">
                              <Badge
                                variant="secondary"
                                style={{
                                  backgroundColor: '#3B82F620',
                                  color: '#3B82F6',
                                  borderColor: '#3B82F6',
                                }}
                              >
                                {r.analysis_name} v{r.version}
                              </Badge>
                              <span className="text-xs text-muted-foreground">Job #{r.job_id}</span>
                              <div className="ml-auto flex items-center gap-1">
                                {/* Renderer buttons (UMAP / PCA / …) for kinds
                                    that declare them. Opens ScatterViewerOverlay
                                    pinned to this slide + job. */}
                                {(() => {
                                  const kind = kinds.find(k => k.id === r.analysis_kind)
                                  return (kind?.renderers || []).map(rd => (
                                    <Button
                                      key={rd.id}
                                      variant="outline"
                                      size="sm"
                                      className="h-7 text-xs"
                                      title={rd.description}
                                      onClick={() => selectedSlide && setScatterViewer({
                                        jobId: r.job_id,
                                        slideHash: selectedSlide.slide_hash,
                                        slideName: `${displaySlide(selectedSlide)} - ${selectedSlide.block_id}-${selectedSlide.slide_number}`,
                                        rendererId: rd.id,
                                        rendererName: rd.name,
                                      })}
                                    >
                                      <ScatterChart className="h-3 w-3 mr-1" />
                                      {rd.name}
                                    </Button>
                                  ))
                                })()}
                                <Button
                                  variant="outline"
                                  size="sm"
                                  className="h-7 text-xs"
                                  disabled={loadingOutputJob === r.job_id}
                                  onClick={() => {
                                    if (files) {
                                      setAnalysisOutputs((prev) => ({ ...prev, [r.job_id]: undefined }))
                                    } else if (selectedSlide) {
                                      fetchAnalysisOutputs(r.job_id, selectedSlide.slide_hash)
                                    }
                                  }}
                                >
                                  {loadingOutputJob === r.job_id ? (
                                    <Loader2 className="h-3 w-3 animate-spin mr-1" />
                                  ) : (
                                    <FileText className="h-3 w-3 mr-1" />
                                  )}
                                  {files ? 'Hide' : 'Files'}
                                </Button>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  className="h-7 text-xs"
                                  disabled={downloadingJob === r.job_id}
                                  onClick={() => downloadAnalysisZip(r.job_id)}
                                >
                                  {downloadingJob === r.job_id ? (
                                    <Loader2 className="h-3 w-3 animate-spin mr-1" />
                                  ) : (
                                    <Download className="h-3 w-3 mr-1" />
                                  )}
                                  Download
                                </Button>
                              </div>
                            </div>
                            {r.cell_stats && (
                              <div className="flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-muted-foreground">
                                {Object.entries(r.cell_stats).map(([name, count]) => (
                                  <span key={name}>
                                    <span className="font-medium text-foreground/70">{name}:</span>{' '}
                                    {count.toLocaleString()}
                                  </span>
                                ))}
                              </div>
                            )}
                            {files && (
                              files.length === 0 ? (
                                <p className="text-xs text-muted-foreground">No output files found.</p>
                              ) : (
                                <ul className="space-y-0.5 pl-1 pt-1 border-t">
                                  {files.flatMap((g) => g.files).map((f) => {
                                    const overlayable = isOverlayFile(f)
                                    return (
                                      <li key={f} className="flex items-center gap-2 text-[11px] font-mono text-muted-foreground">
                                        <FileText className="h-3 w-3 shrink-0" />
                                        <span className="truncate flex-1" title={f}>{f}</span>
                                        {overlayable && selectedSlide && (
                                          <button
                                            className="shrink-0 inline-flex items-center gap-1 text-[10px] text-primary hover:underline"
                                            onClick={() => openOverlayFile(
                                              r.job_id,
                                              selectedSlide.slide_hash,
                                              `${displaySlide(selectedSlide)} - ${selectedSlide.block_id}-${selectedSlide.slide_number}`,
                                              f,
                                            )}
                                            title="Overlay on slide"
                                          >
                                            <Layers className="h-3 w-3" />Overlay
                                          </button>
                                        )}
                                      </li>
                                    )
                                  })}
                                </ul>
                              )
                            )}
                          </div>
                        )
                      })
                    ) : (
                      <div className="flex flex-wrap gap-2">
                        {selectedSlide.completed_analyses.map((name: string) => (
                          <Badge
                            key={name}
                            variant="secondary"
                            style={{
                              backgroundColor: '#3B82F620',
                              color: '#3B82F6',
                              borderColor: '#3B82F6',
                            }}
                          >
                            {name}
                          </Badge>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              )}
              {/* Annotations section */}
              <div className="col-span-2 pt-4 border-t">
                <div className="flex items-center justify-between mb-2">
                  <label className="text-sm font-medium">Imported Annotations</label>
                  <div>
                    <input
                      ref={annotationInputRef}
                      type="file"
                      accept=".geojson,.json"
                      multiple
                      className="hidden"
                      onChange={(e) => {
                        const files = e.target.files
                        if (!files || !selectedSlide) return
                        for (const file of Array.from(files)) {
                          uploadAnnotation(selectedSlide.slide_hash, file)
                        }
                        e.target.value = ''
                      }}
                    />
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-7 text-xs"
                      disabled={uploadingAnnotation}
                      onClick={() => annotationInputRef.current?.click()}
                    >
                      <Upload className="mr-1 h-3.5 w-3.5" />
                      {uploadingAnnotation ? 'Uploading...' : 'Upload'}
                    </Button>
                  </div>
                </div>

                {loadingAnnotations ? (
                  <p className="text-xs text-muted-foreground">Loading...</p>
                ) : annotations.length === 0 ? (
                  <p className="text-xs text-muted-foreground">No annotations uploaded yet.</p>
                ) : (
                  <div className="space-y-1">
                    {annotations.map((ann) => (
                      <div key={ann.name} className="flex items-center justify-between text-sm py-1 px-2 rounded hover:bg-muted/50">
                        <div className="flex items-center gap-2 min-w-0">
                          <FileDown className="h-3.5 w-3.5 text-orange-500 shrink-0" />
                          <span className="font-mono text-xs truncate" title={ann.name}>{ann.name}</span>
                          <span className="text-xs text-muted-foreground shrink-0">
                            {ann.size < 1024 ? `${ann.size} B` : ann.size < 1024 * 1024 ? `${(ann.size / 1024).toFixed(1)} KB` : `${(ann.size / (1024 * 1024)).toFixed(1)} MB`}
                          </span>
                        </div>
                        <button
                          className="p-1 rounded hover:bg-destructive/10 text-muted-foreground hover:text-destructive transition-colors shrink-0"
                          onClick={() => selectedSlide && confirmDeleteAnnotation(selectedSlide.slide_hash, ann.name)}
                        >
                          <Trash2 className="h-3 w-3" />
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="col-span-2 pt-4 border-t">
                <Button
                  onClick={() => setIsViewerOpen(true)}
                  className="w-full"
                >
                  <Eye className="mr-2 h-4 w-4" />
                  View Slide
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>

      {/* Bulk Tag Dialog */}
      <Dialog open={isBulkTagDialogOpen} onOpenChange={(open) => {
        setIsBulkTagDialogOpen(open)
        if (!open) {
          setBulkTagInput('')
          setShowBulkSuggestions(false)
        }
      }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Add Tag to Selected Slides</DialogTitle>
            <DialogDescription>
              Add a tag to {selectedSlides.size} selected slide{selectedSlides.size !== 1 ? 's' : ''}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            {/* Color selector */}
            <div className="space-y-2">
              <div className="flex gap-1 items-center">
                <span className="text-xs text-muted-foreground mr-1">Color:</span>
                {PRESET_COLORS.map((color) => (
                  <button
                    key={color}
                    className={`w-5 h-5 rounded-full transition-all ring-offset-2 ring-offset-background ${
                      bulkTagColor === color ? 'ring-2 ring-foreground' : 'hover:scale-110'
                    }`}
                    style={{ backgroundColor: color }}
                    onClick={() => setBulkTagColor(color)}
                    title={color}
                  />
                ))}
              </div>
            </div>

            {/* Input with autocomplete */}
            <div className="space-y-2">
              <label className="text-sm font-medium">Tag Name</label>
              <div className="relative">
                <Input
                  ref={bulkInputRef}
                  placeholder="Type to search or create a tag..."
                  value={bulkTagInput}
                  onChange={(e) => {
                    setBulkTagInput(e.target.value)
                    setShowBulkSuggestions(true)
                  }}
                  onFocus={() => setShowBulkSuggestions(true)}
                  onKeyDown={handleBulkKeyDown}
                />

                {/* Autocomplete dropdown */}
                {showBulkSuggestions && (bulkTagSuggestions.length > 0 || (bulkTagInput && !isBulkExistingTag)) && (
                  <div
                    ref={bulkSuggestionsRef}
                    className="absolute top-full left-0 right-0 mt-1 bg-popover border rounded-lg shadow-lg z-50 overflow-hidden max-h-48 overflow-y-auto"
                  >
                    {bulkTagSuggestions.map((tag) => (
                      <button
                        key={tag.id}
                        className="w-full px-3 py-2 text-left hover:bg-muted flex items-center gap-2"
                        onClick={() => selectBulkSuggestion(tag)}
                      >
                        {tag.color && (
                          <span
                            className="w-3 h-3 rounded-full"
                            style={{ backgroundColor: tag.color }}
                          />
                        )}
                        <span>{tag.name}</span>
                      </button>
                    ))}

                    {/* Option to create new tag */}
                    {bulkTagInput && !isBulkExistingTag && (
                      <button
                        className="w-full px-3 py-2 text-left hover:bg-muted flex items-center gap-2 border-t"
                        onClick={() => handleBulkAddTag(bulkTagInput, bulkTagColor)}
                      >
                        <Plus className="h-4 w-4" />
                        <span
                          className="w-3 h-3 rounded-full"
                          style={{ backgroundColor: bulkTagColor }}
                        />
                        <span>Create "{bulkTagInput}"</span>
                      </button>
                    )}
                  </div>
                )}
              </div>
            </div>

            <div className="flex justify-end gap-2">
              <Button
                variant="outline"
                onClick={() => setIsBulkTagDialogOpen(false)}
              >
                Cancel
              </Button>
              <Button
                onClick={() => handleBulkAddTag()}
                disabled={isBulkTagging || !bulkTagInput.trim()}
              >
                {isBulkTagging ? 'Adding...' : 'Add Tag'}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* Bulk Remove Tag Dialog */}
      <Dialog open={isBulkRemoveTagDialogOpen} onOpenChange={(open) => {
        setIsBulkRemoveTagDialogOpen(open)
        if (!open) {
          setBulkRemoveTagInput('')
          setShowBulkRemoveSuggestions(false)
        }
      }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Remove Tag from Selected Slides</DialogTitle>
            <DialogDescription>
              Remove a tag from {selectedSlides.size} selected slide{selectedSlides.size !== 1 ? 's' : ''}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            {/* Input with autocomplete */}
            <div className="space-y-2">
              <label className="text-sm font-medium">Tag Name</label>
              <div className="relative">
                <Input
                  ref={bulkRemoveInputRef}
                  placeholder="Type to search for a tag..."
                  value={bulkRemoveTagInput}
                  onChange={(e) => {
                    setBulkRemoveTagInput(e.target.value)
                    setShowBulkRemoveSuggestions(true)
                  }}
                  onFocus={() => setShowBulkRemoveSuggestions(true)}
                  onKeyDown={handleBulkRemoveKeyDown}
                />

                {/* Autocomplete dropdown */}
                {showBulkRemoveSuggestions && bulkRemoveTagSuggestions.length > 0 && (
                  <div
                    ref={bulkRemoveSuggestionsRef}
                    className="absolute top-full left-0 right-0 mt-1 bg-popover border rounded-lg shadow-lg z-50 overflow-hidden max-h-48 overflow-y-auto"
                  >
                    {bulkRemoveTagSuggestions.map((tag) => (
                      <button
                        key={tag.id}
                        className="w-full px-3 py-2 text-left hover:bg-muted flex items-center gap-2"
                        onClick={() => selectBulkRemoveSuggestion(tag)}
                      >
                        {tag.color && (
                          <span
                            className="w-3 h-3 rounded-full"
                            style={{ backgroundColor: tag.color }}
                          />
                        )}
                        <span>{tag.name}</span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </div>

            <div className="flex justify-end gap-2">
              <Button
                variant="outline"
                onClick={() => setIsBulkRemoveTagDialogOpen(false)}
              >
                Cancel
              </Button>
              <Button
                variant="destructive"
                onClick={() => handleBulkRemoveTag()}
                disabled={isBulkTagging || !bulkRemoveTagInput.trim()}
              >
                {isBulkTagging ? 'Removing...' : 'Remove Tag'}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* Tag Management Dialog */}
      <Dialog open={isTagManagementOpen} onOpenChange={setIsTagManagementOpen}>
        <DialogContent className="max-w-lg max-h-[80vh] flex flex-col">
          <DialogHeader>
            <DialogTitle>Manage Tags</DialogTitle>
            <DialogDescription>
              Create, view, and delete tags. Deleting a tag removes it from all slides.
            </DialogDescription>
          </DialogHeader>

          <div className="flex-1 overflow-hidden flex flex-col gap-4">
            {/* Create new tag section */}
            <div className="space-y-2 pb-4 border-b">
              <label className="text-sm font-medium">Create New Tag</label>
              <div className="flex gap-2 items-center">
                <div className="flex gap-1">
                  {PRESET_COLORS.map((color) => (
                    <button
                      key={color}
                      className={`w-5 h-5 rounded-full transition-all ring-offset-2 ring-offset-background ${
                        newTagColor === color ? 'ring-2 ring-foreground' : 'hover:scale-110'
                      }`}
                      style={{ backgroundColor: color }}
                      onClick={() => setNewTagColor(color)}
                    />
                  ))}
                </div>
                <Input
                  placeholder="Tag name..."
                  value={newTagName}
                  onChange={(e) => setNewTagName(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleCreateTag()}
                  className="flex-1"
                />
                <Button
                  onClick={handleCreateTag}
                  disabled={isCreatingTag || !newTagName.trim()}
                  size="sm"
                >
                  {isCreatingTag ? '...' : 'Create'}
                </Button>
              </div>
            </div>

            {/* Tag list */}
            <div className="flex-1 overflow-y-auto">
              <div className="space-y-1">
                {availableTags.length === 0 ? (
                  <p className="text-sm text-muted-foreground text-center py-4">
                    No tags created yet. Create one above.
                  </p>
                ) : (
                  availableTags.map((tag) => (
                    <div
                      key={tag.id}
                      className="flex items-center justify-between p-2 rounded-lg hover:bg-muted/50 group"
                    >
                      <div className="flex items-center gap-2 flex-1 min-w-0">
                        <div className="relative shrink-0">
                          <button
                            className="w-4 h-4 rounded-full ring-1 ring-border hover:ring-2 hover:ring-foreground transition-all"
                            style={{ backgroundColor: tag.color || '#6B7280' }}
                            onClick={() => setColorPickerForTag(colorPickerForTag === tag.id ? null : tag.id)}
                            title="Change color"
                          />
                          {colorPickerForTag === tag.id && (
                            <div className="absolute left-0 top-6 z-50 flex gap-1 p-2 bg-popover border rounded-lg shadow-lg">
                              {PRESET_COLORS.map((color) => (
                                <button
                                  key={color}
                                  className={`w-5 h-5 rounded-full transition-all ring-offset-2 ring-offset-background ${
                                    tag.color === color ? 'ring-2 ring-foreground' : 'hover:scale-110'
                                  }`}
                                  style={{ backgroundColor: color }}
                                  onClick={() => updateTagColor(tag.id, color)}
                                />
                              ))}
                            </div>
                          )}
                        </div>
                        {editingTagId === tag.id ? (
                          <div className="flex items-center gap-1 flex-1">
                            <Input
                              value={editingTagName}
                              onChange={(e) => setEditingTagName(e.target.value)}
                              onKeyDown={(e) => {
                                if (e.key === 'Enter') saveTagName(tag.id, tag.name)
                                if (e.key === 'Escape') cancelEditTag()
                              }}
                              className="h-7 text-sm"
                              autoFocus
                            />
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 px-2 text-green-600 hover:text-green-700"
                              onClick={() => saveTagName(tag.id, tag.name)}
                              disabled={savingTagName}
                            >
                              <Check className="h-3.5 w-3.5" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 px-2"
                              onClick={cancelEditTag}
                            >
                              <X className="h-3.5 w-3.5" />
                            </Button>
                          </div>
                        ) : (
                          <>
                            <span className="font-medium">{tag.name}</span>
                            <span className="text-sm text-muted-foreground">
                              ({tag.slide_count ?? 0} slide{(tag.slide_count ?? 0) !== 1 ? 's' : ''})
                            </span>
                          </>
                        )}
                      </div>
                      {editingTagId !== tag.id && (
                        <div className="flex gap-1 opacity-0 group-hover:opacity-100">
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 px-2 text-muted-foreground hover:text-foreground"
                            onClick={() => startEditTag(tag)}
                          >
                            <Pencil className="h-3.5 w-3.5" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 px-2 text-destructive hover:text-destructive hover:bg-destructive/10"
                            onClick={() => handleDeleteTag(tag.id, tag.name)}
                            disabled={isDeletingTag === tag.id}
                          >
                            {isDeletingTag === tag.id ? '...' : <Trash2 className="h-3.5 w-3.5" />}
                          </Button>
                        </div>
                      )}
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>

          <div className="flex justify-end pt-4 border-t">
            <Button variant="outline" onClick={() => setIsTagManagementOpen(false)}>
              Done
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Slide Viewer Overlay */}
      {isViewerOpen && selectedSlide && (
        <SlideViewerOSD
          slideHash={selectedSlide.slide_hash}
          slideName={`${displaySlide(selectedSlide)} - ${selectedSlide.block_id}-${selectedSlide.slide_number} (${selectedSlide.stain_type})`}
          onClose={() => setIsViewerOpen(false)}
        />
      )}

      {/* Overlay viewer — opened from an analysis output file in the Completed Analyses block */}
      {overlayViewer && (
        <SlideViewerOSD
          slideHash={overlayViewer.slideHash}
          slideName={overlayViewer.slideName}
          overlays={overlayViewer.overlays}
          onClose={() => setOverlayViewer(null)}
        />
      )}

      {/* Renderer scatter view (UMAP / PCA) — opened from a kind-renderer
          button in the Completed Analyses block. Same component the Analysis
          Results view uses; bidirectional slide↔scatter highlighting comes
          for free. */}
      {scatterViewer && (
        <ScatterViewerOverlay
          jobId={scatterViewer.jobId}
          slideHash={scatterViewer.slideHash}
          slideName={scatterViewer.slideName}
          rendererId={scatterViewer.rendererId}
          rendererName={scatterViewer.rendererName}
          onClose={() => setScatterViewer(null)}
        />
      )}

      {/* Job picker for Download with Analysis */}
      <Dialog open={isJobPickerOpen} onOpenChange={setIsJobPickerOpen}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Select Analysis Job</DialogTitle>
            <DialogDescription>
              Choose which analysis job's outputs to include with the {selectedSlides.size} selected slide{selectedSlides.size !== 1 ? 's' : ''}.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-1 max-h-72 overflow-y-auto">
            {loadingJobs ? (
              <p className="text-sm text-muted-foreground py-4 text-center">Loading jobs...</p>
            ) : jobsList.length === 0 ? (
              <p className="text-sm text-muted-foreground py-4 text-center">No completed jobs found.</p>
            ) : (
              jobsList.map((j) => (
                <button
                  key={j.id}
                  className="w-full text-left rounded-md border px-3 py-2 hover:bg-muted/50 transition-colors"
                  onClick={() => pickJobForBundle(j.id)}
                >
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">
                      Job #{j.id} — {j.model_name} {j.model_version || ''}
                    </span>
                    <Badge variant="outline" className="text-xs">
                      {j.completed_count}/{j.slide_count}
                    </Badge>
                  </div>
                  {j.completed_at && (
                    <p className="text-xs text-muted-foreground mt-0.5">
                      {new Date(j.completed_at).toLocaleDateString()}
                    </p>
                  )}
                </button>
              ))
            )}
          </div>
        </DialogContent>
      </Dialog>

      {/* Bundle download modal */}
      <DownloadModal
        open={bundleModal.open}
        onOpenChange={(open) => setBundleModal((prev) => ({ ...prev, open }))}
        slideHashes={bundleModal.slideHashes}
        jobId={bundleModal.jobId}
      />

      {/* Annotation delete confirmation */}
      <Dialog
        open={deleteAnnotationConfirm.open}
        onOpenChange={(open) => setDeleteAnnotationConfirm((prev) => ({ ...prev, open }))}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Delete Annotation</DialogTitle>
            <DialogDescription>
              Are you sure you want to permanently delete{' '}
              <span className="font-medium text-foreground">{deleteAnnotationConfirm.filename}</span>?
              This will remove the file from the network drive.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setDeleteAnnotationConfirm((prev) => ({ ...prev, open: false }))}
            >
              Cancel
            </Button>
            <Button variant="destructive" onClick={doDeleteAnnotation}>
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
