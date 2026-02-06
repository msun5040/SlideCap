import { useState, useEffect, useRef } from 'react'
import { Search, Filter, Tag as TagIcon, Eye, Tags, X, Plus, Settings, Trash2, ChevronDown } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { SlideViewer } from '@/components/SlideViewer'
import { TagInput } from '@/components/TagInput'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
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
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { Slide, Tag } from '@/types/slide'

const API_BASE = 'http://localhost:8000'

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
        const response = await fetch(`${API_BASE}/tags/search?q=${encodeURIComponent(bulkTagInput)}`)
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
        const response = await fetch(`${API_BASE}/tags/search?q=${encodeURIComponent(bulkRemoveTagInput)}`)
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
      const response = await fetch(`${API_BASE}/stats`)
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
      const response = await fetch(`${API_BASE}/tags`)
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

  const handleSearch = async () => {
    setLoading(true)
    setSelectedSlides(new Set()) // Clear selection on new search
    try {
      // Build URL with optional query and filters
      const params = new URLSearchParams()
      if (searchTerm.trim()) params.append('q', searchTerm.trim())
      if (yearFilter !== 'all') params.append('year', yearFilter)
      if (stainFilter !== 'all') params.append('stain', stainFilter)
      if (tagFilter !== 'all') params.append('tag', tagFilter)

      const url = `${API_BASE}/search?${params.toString()}`
      const response = await fetch(url)
      if (response.ok) {
        const data = await response.json()
        setSlides(data.results)
        setResultsTruncated(data.truncated || false)
      }
    } catch (error) {
      console.error('Search error:', error)
    } finally {
      setLoading(false)
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

  const getStatusColor = (status?: string) => {
    switch (status) {
      case 'available':
        return 'bg-green-500/10 text-green-700 hover:bg-green-500/20'
      case 'in-analysis':
        return 'bg-orange-500/10 text-orange-700 hover:bg-orange-500/20'
      case 'archived':
        return 'bg-gray-500/10 text-gray-700 hover:bg-gray-500/20'
      default:
        return 'bg-gray-500/10 text-gray-500'
    }
  }

  const openTagDialog = async (slide: Slide, e: React.MouseEvent) => {
    e.stopPropagation()
    setSelectedSlide(slide)
    setSlideTags([])  // Clear previous tags
    setLoadingTags(true)
    setIsTagDialogOpen(true)

    // Fetch full tag details for this slide
    try {
      const response = await fetch(`${API_BASE}/slides/${slide.slide_hash}/tags`)
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
    if (selectedSlides.size === filteredSlides.length) {
      setSelectedSlides(new Set())
    } else {
      setSelectedSlides(new Set(filteredSlides.map(s => s.slide_hash)))
    }
  }

  const clearSelection = () => {
    setSelectedSlides(new Set())
  }

  const handleBulkAddTag = async (tagName?: string, tagColor?: string) => {
    const name = tagName || bulkTagInput.trim()
    const color = tagColor || bulkTagColor
    if (!name || selectedSlides.size === 0) return

    setIsBulkTagging(true)
    setShowBulkSuggestions(false)
    try {
      const response = await fetch(`${API_BASE}/slides/bulk/tags/add`, {
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
      const response = await fetch(`${API_BASE}/slides/bulk/tags/remove`, {
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
      const response = await fetch(`${API_BASE}/tags`, {
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
      const response = await fetch(`${API_BASE}/tags/${tagId}`, {
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
    <div className="space-y-6">
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

          <Select value={tagFilter} onValueChange={(value) => {
            if (value === '__manage__') {
              setIsTagManagementOpen(true)
            } else {
              setTagFilter(value)
            }
          }}>
            <SelectTrigger className="w-36">
              <TagIcon className="mr-2 h-4 w-4" />
              <SelectValue placeholder="Tag" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Tags</SelectItem>
              {availableTags.filter(t => (t.slide_count ?? 0) > 0).map((tag) => (
                <SelectItem key={tag.id} value={tag.name}>
                  <div className="flex items-center gap-2">
                    {tag.color && (
                      <span
                        className="w-2 h-2 rounded-full"
                        style={{ backgroundColor: tag.color }}
                      />
                    )}
                    {tag.name}
                    <span className="text-muted-foreground text-xs">({tag.slide_count})</span>
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
          Showing {filteredSlides.length} slides
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
              variant="ghost"
              size="sm"
              onClick={clearSelection}
            >
              Clear Selection
            </Button>
          </div>
        )}
      </div>

      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-12.5">
                <Checkbox
                  checked={filteredSlides.length > 0 && selectedSlides.size === filteredSlides.length}
                  onCheckedChange={toggleSelectAll}
                />
              </TableHead>
              <TableHead>Accession #</TableHead>
              <TableHead>Block</TableHead>
              <TableHead>Slide #</TableHead>
              <TableHead>Stain</TableHead>
              <TableHead>Year</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Tags</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredSlides.length === 0 ? (
              <TableRow>
                <TableCell colSpan={8} className="h-24 text-center">
                  {slides.length === 0
                    ? 'Search for slides to get started.'
                    : 'No slides found matching your criteria.'}
                </TableCell>
              </TableRow>
            ) : (
              filteredSlides.map((slide) => (
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
                  <TableCell className="font-medium">{slide.accession_number}</TableCell>
                  <TableCell className="text-sm">{slide.block_id}</TableCell>
                  <TableCell className="text-sm">{slide.slide_number}</TableCell>
                  <TableCell>
                    <Badge variant="outline" className="text-xs">
                      {slide.stain_type}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-sm">{slide.year || '-'}</TableCell>
                  <TableCell>
                    <Badge className={getStatusColor(slide.status)}>
                      {slide.status || 'unknown'}
                    </Badge>
                  </TableCell>
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
                              <Badge
                                key={tagName}
                                variant="secondary"
                                className="text-xs cursor-pointer hover:opacity-80"
                                style={tagInfo?.color ? {
                                  backgroundColor: `${tagInfo.color}20`,
                                  color: tagInfo.color,
                                  borderColor: tagInfo.color
                                } : undefined}
                                onClick={(e) => openTagDialog(slide, e)}
                              >
                                {tagName}
                              </Badge>
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
              {selectedSlide?.accession_number} - Block {selectedSlide?.block_id}, Slide {selectedSlide?.slide_number}
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
              {selectedSlide?.accession_number}
            </DialogDescription>
          </DialogHeader>
          {selectedSlide && (
            <div className="grid grid-cols-2 gap-4 py-4">
              <div>
                <label className="text-sm text-muted-foreground">Accession #</label>
                <p className="font-medium">{selectedSlide.accession_number}</p>
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
                  <p className="font-mono text-sm break-all">{selectedSlide.file_path}</p>
                </div>
              )}
              {selectedSlide.slide_tags && selectedSlide.slide_tags.length > 0 && (
                <div className="col-span-2">
                  <label className="text-sm text-muted-foreground">Tags</label>
                  <div className="flex flex-wrap gap-2 mt-1">
                    {selectedSlide.slide_tags.map((tag: string) => (
                      <Badge key={tag} variant="secondary">{tag}</Badge>
                    ))}
                  </div>
                </div>
              )}
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
                    className={`w-5 h-5 rounded-full border-2 transition-all ${
                      bulkTagColor === color ? 'border-black scale-110 ring-1 ring-black' : 'border-transparent hover:scale-105'
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
                      className={`w-5 h-5 rounded-full border-2 transition-all ${
                        newTagColor === color ? 'border-black scale-110 ring-1 ring-black' : 'border-transparent hover:scale-105'
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
                      <div className="flex items-center gap-2">
                        <span
                          className="w-3 h-3 rounded-full"
                          style={{ backgroundColor: tag.color || '#6B7280' }}
                        />
                        <span className="font-medium">{tag.name}</span>
                        <span className="text-sm text-muted-foreground">
                          ({tag.slide_count ?? 0} slide{(tag.slide_count ?? 0) !== 1 ? 's' : ''})
                        </span>
                      </div>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive hover:bg-destructive/10"
                        onClick={() => handleDeleteTag(tag.id, tag.name)}
                        disabled={isDeletingTag === tag.id}
                      >
                        {isDeletingTag === tag.id ? (
                          '...'
                        ) : (
                          <Trash2 className="h-4 w-4" />
                        )}
                      </Button>
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
        <SlideViewer
          slideHash={selectedSlide.slide_hash}
          slideName={`${selectedSlide.accession_number} - ${selectedSlide.block_id}-${selectedSlide.slide_number} (${selectedSlide.stain_type})`}
          onClose={() => setIsViewerOpen(false)}
        />
      )}
    </div>
  )
}
