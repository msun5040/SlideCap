import { useState, useEffect, useMemo, useCallback } from 'react'
import { ArrowLeft, Search, Filter, X, Plus, ChevronDown, ChevronRight, Check } from 'lucide-react'
import { Button } from '@/components/ui/button'
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
import type { Slide, CohortSlide, CohortDetail, CaseGroup } from '@/types/slide'

const API_BASE = 'http://localhost:8000'

interface CohortBuilderProps {
  cohortId: number
  onBack: () => void
}

export function CohortBuilder({ cohortId, onBack }: CohortBuilderProps) {
  // Cohort state
  const [cohort, setCohort] = useState<CohortDetail | null>(null)
  const [cohortLoading, setCohortLoading] = useState(true)

  // Search state
  const [searchResults, setSearchResults] = useState<Slide[]>([])
  const [searchLoading, setSearchLoading] = useState(false)
  const [searchTerm, setSearchTerm] = useState('')
  const [yearFilter, setYearFilter] = useState<string>('all')
  const [stainFilter, setStainFilter] = useState<string>('all')
  const [tagFilter, setTagFilter] = useState<string>('all')
  const [availableTags, setAvailableTags] = useState<{ id: number; name: string; color?: string; slide_count?: number }[]>([])
  const [resultsTruncated, setResultsTruncated] = useState(false)

  // Selection state
  const [selectedHashes, setSelectedHashes] = useState<Set<string>>(new Set())

  // Right panel collapse state
  const [collapsedCases, setCollapsedCases] = useState<Set<string>>(new Set())

  // Set of slide hashes in the cohort for O(1) lookup
  const cohortHashSet = useMemo(() => {
    if (!cohort) return new Set<string>()
    return new Set(cohort.slides.map(s => s.slide_hash))
  }, [cohort])

  // Group cohort slides by case
  const caseGroups = useMemo((): CaseGroup[] => {
    if (!cohort) return []
    const groupMap = new Map<string, CaseGroup>()

    for (const slide of cohort.slides) {
      const key = slide.case_hash || slide.slide_hash // fallback for slides without case
      if (!groupMap.has(key)) {
        groupMap.set(key, {
          case_hash: slide.case_hash || key,
          accession_number: slide.accession_number,
          year: slide.year,
          slides: [],
        })
      }
      groupMap.get(key)!.slides.push(slide)
    }

    // Sort by accession_number
    const groups = Array.from(groupMap.values())
    groups.sort((a, b) => {
      const aStr = a.accession_number || ''
      const bStr = b.accession_number || ''
      return aStr.localeCompare(bStr)
    })
    return groups
  }, [cohort])

  // Summary stats
  const stats = useMemo(() => {
    if (!cohort) return { slides: 0, cases: 0, stains: {} as Record<string, number> }
    const stains: Record<string, number> = {}
    for (const s of cohort.slides) {
      const category = s.stain_type === 'HE' ? 'HE' : s.stain_type.startsWith('IHC') ? 'IHC' : s.stain_type
      stains[category] = (stains[category] || 0) + 1
    }
    return {
      slides: cohort.slides.length,
      cases: caseGroups.length,
      stains,
    }
  }, [cohort, caseGroups])

  // Fetch cohort details
  const fetchCohort = useCallback(async () => {
    setCohortLoading(true)
    try {
      const response = await fetch(`${API_BASE}/cohorts/${cohortId}`)
      if (response.ok) {
        const data: CohortDetail = await response.json()
        setCohort(data)
      }
    } catch (error) {
      console.error('Failed to fetch cohort:', error)
    } finally {
      setCohortLoading(false)
    }
  }, [cohortId])

  useEffect(() => {
    fetchCohort()
  }, [fetchCohort])

  // Fetch available tags
  useEffect(() => {
    const fetchTags = async () => {
      try {
        const response = await fetch(`${API_BASE}/tags`)
        if (response.ok) {
          setAvailableTags(await response.json())
        }
      } catch (error) {
        console.error('Failed to fetch tags:', error)
      }
    }
    fetchTags()
  }, [])

  // Search slides
  const handleSearch = async () => {
    setSearchLoading(true)
    setSelectedHashes(new Set())
    try {
      const params = new URLSearchParams()
      if (searchTerm.trim()) params.append('q', searchTerm.trim())
      if (yearFilter !== 'all') params.append('year', yearFilter)
      if (stainFilter !== 'all') params.append('stain', stainFilter)
      if (tagFilter !== 'all') params.append('tag', tagFilter)

      const response = await fetch(`${API_BASE}/search?${params.toString()}`)
      if (response.ok) {
        const data = await response.json()
        setSearchResults(data.results)
        setResultsTruncated(data.truncated || false)
      }
    } catch (error) {
      console.error('Search error:', error)
    } finally {
      setSearchLoading(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') handleSearch()
  }

  // Helper: build a CohortSlide from a search result
  const searchSlideToCohortSlide = (s: Slide): CohortSlide => ({
    slide_hash: s.slide_hash,
    accession_number: s.accession_number || null,
    block_id: s.block_id,
    slide_number: s.slide_number || null,
    stain_type: s.stain_type,
    random_id: s.random_id,
    year: s.year || null,
    case_hash: s.case_hash || null,
    tags: s.slide_tags || [],
    file_size_bytes: s.file_size_bytes,
  })

  // Add slides to cohort (optimistic)
  const addSlides = async (hashes: string[]) => {
    if (hashes.length === 0 || !cohort) return

    // Optimistic: immediately add to local state
    const newSlides: CohortSlide[] = []
    for (const hash of hashes) {
      if (cohortHashSet.has(hash)) continue
      const searchSlide = searchResults.find(s => s.slide_hash === hash)
      if (searchSlide) newSlides.push(searchSlideToCohortSlide(searchSlide))
    }
    const prevCohort = cohort
    if (newSlides.length > 0) {
      setCohort({ ...cohort, slides: [...cohort.slides, ...newSlides] })
    }
    setSelectedHashes(new Set())

    // Fire API call in background
    try {
      const response = await fetch(`${API_BASE}/cohorts/${cohortId}/slides`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: hashes }),
      })
      if (response.ok) {
        const data = await response.json()
        // Reconcile with server: use server counts, and remove any not_found slides
        if (data.not_found?.length > 0) {
          const notFoundSet = new Set(data.not_found as string[])
          setCohort(prev => prev ? {
            ...prev,
            slides: prev.slides.filter(s => !notFoundSet.has(s.slide_hash)),
            slide_count: data.total_slides,
            case_count: data.total_cases,
          } : prev)
          console.warn('Some slides could not be added (not in database):', data.not_found)
        } else {
          // Just sync counts from server
          setCohort(prev => prev ? {
            ...prev,
            slide_count: data.total_slides,
            case_count: data.total_cases,
          } : prev)
        }
      } else {
        // Revert on error
        setCohort(prevCohort)
      }
    } catch (error) {
      console.error('Failed to add slides:', error)
      setCohort(prevCohort)
    }
  }

  // Remove slide from cohort (optimistic)
  const removeSlide = async (slideHash: string) => {
    if (!cohort) return

    // Optimistic: immediately remove from local state
    const prevCohort = cohort
    setCohort({
      ...cohort,
      slides: cohort.slides.filter(s => s.slide_hash !== slideHash),
    })

    try {
      const response = await fetch(`${API_BASE}/cohorts/${cohortId}/slides`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slide_hashes: [slideHash] }),
      })
      if (response.ok) {
        const data = await response.json()
        setCohort(prev => prev ? {
          ...prev,
          slide_count: data.total_slides,
          case_count: data.total_cases,
        } : prev)
      } else {
        setCohort(prevCohort)
      }
    } catch (error) {
      console.error('Failed to remove slide:', error)
      setCohort(prevCohort)
    }
  }

  // Selection helpers
  const toggleSelection = (hash: string) => {
    const next = new Set(selectedHashes)
    if (next.has(hash)) next.delete(hash)
    else next.add(hash)
    setSelectedHashes(next)
  }

  const toggleSelectAll = () => {
    if (selectedHashes.size === searchResults.length) {
      setSelectedHashes(new Set())
    } else {
      setSelectedHashes(new Set(searchResults.map(s => s.slide_hash)))
    }
  }

  const toggleCaseCollapse = (caseHash: string) => {
    const next = new Set(collapsedCases)
    if (next.has(caseHash)) next.delete(caseHash)
    else next.add(caseHash)
    setCollapsedCases(next)
  }

  // Count selected slides not already in cohort
  const addableCount = useMemo(() => {
    let count = 0
    for (const hash of selectedHashes) {
      if (!cohortHashSet.has(hash)) count++
    }
    return count
  }, [selectedHashes, cohortHashSet])

  const years = ['2024', '2023', '2022', '2021', '2020']
  const stainTypes = ['HE', 'IHC', 'Special']

  if (cohortLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <p className="text-muted-foreground">Loading cohort...</p>
      </div>
    )
  }

  if (!cohort) {
    return (
      <div className="space-y-4">
        <Button variant="ghost" onClick={onBack}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Cohorts
        </Button>
        <p className="text-muted-foreground">Cohort not found.</p>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-[calc(100vh-10rem)] gap-4">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="sm" onClick={onBack}>
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex-1 min-w-0">
          <h1 className="text-xl font-semibold truncate">{cohort.name}</h1>
          {cohort.description && (
            <p className="text-sm text-muted-foreground truncate">{cohort.description}</p>
          )}
        </div>
      </div>

      {/* Stats bar */}
      <div className="flex items-center gap-3 text-sm text-muted-foreground border-b pb-3">
        <span className="font-medium text-foreground">{stats.slides} slides</span>
        <span>·</span>
        <span>{stats.cases} cases</span>
        {Object.entries(stats.stains).map(([stain, count]) => (
          <span key={stain}>
            · {stain}: {count}
          </span>
        ))}
      </div>

      {/* Split panel */}
      <div className="flex gap-4 flex-1 min-h-0">
        {/* LEFT: Search panel */}
        <div className="flex-1 flex flex-col min-w-0">
          {/* Search controls */}
          <div className="flex flex-col gap-3 mb-3">
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  placeholder="Search accession number..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  onKeyDown={handleKeyDown}
                  className="pl-10"
                />
              </div>
              <Button onClick={handleSearch} disabled={searchLoading} size="default">
                {searchLoading ? 'Searching...' : 'Search'}
              </Button>
            </div>
            <div className="flex flex-wrap gap-2">
              <Select value={yearFilter} onValueChange={setYearFilter}>
                <SelectTrigger className="w-28">
                  <SelectValue placeholder="Year" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Years</SelectItem>
                  {years.map((y) => (
                    <SelectItem key={y} value={y}>{y}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={stainFilter} onValueChange={setStainFilter}>
                <SelectTrigger className="w-32">
                  <Filter className="mr-2 h-4 w-4" />
                  <SelectValue placeholder="Stain" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Stains</SelectItem>
                  {stainTypes.map((s) => (
                    <SelectItem key={s} value={s}>{s}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={tagFilter} onValueChange={setTagFilter}>
                <SelectTrigger className="w-32">
                  <SelectValue placeholder="Tag" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Tags</SelectItem>
                  {availableTags.filter(t => (t.slide_count ?? 0) > 0).map((tag) => (
                    <SelectItem key={tag.id} value={tag.name}>
                      <div className="flex items-center gap-2">
                        {tag.color && (
                          <span className="w-2 h-2 rounded-full" style={{ backgroundColor: tag.color }} />
                        )}
                        {tag.name}
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Bulk add bar */}
          {selectedHashes.size > 0 && (
            <div className="flex items-center gap-2 mb-2 px-2 py-1.5 bg-muted/50 rounded-md">
              <span className="text-sm text-muted-foreground">
                {selectedHashes.size} selected
              </span>
              <Button
                size="sm"
                onClick={() => {
                  const toAdd = Array.from(selectedHashes).filter(h => !cohortHashSet.has(h))
                  addSlides(toAdd)
                }}
                disabled={addableCount === 0}
              >
                <Plus className="mr-1 h-3 w-3" />
                Add {addableCount} to Cohort
              </Button>
              <Button variant="ghost" size="sm" onClick={() => setSelectedHashes(new Set())}>
                Clear
              </Button>
            </div>
          )}

          {/* Search results info */}
          <div className="text-xs text-muted-foreground mb-1">
            {searchResults.length > 0 && (
              <>
                {searchResults.length} results
                {resultsTruncated && <span className="text-orange-600 ml-1">(limit reached)</span>}
              </>
            )}
          </div>

          {/* Results table */}
          <div className="flex-1 overflow-auto rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-10">
                    <Checkbox
                      checked={searchResults.length > 0 && selectedHashes.size === searchResults.length}
                      onCheckedChange={toggleSelectAll}
                    />
                  </TableHead>
                  <TableHead>Accession</TableHead>
                  <TableHead>Block</TableHead>
                  <TableHead>Stain</TableHead>
                  <TableHead>Year</TableHead>
                  <TableHead className="w-20">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {searchResults.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="h-24 text-center text-muted-foreground">
                      {searchLoading ? 'Searching...' : 'Search for slides to add to this cohort.'}
                    </TableCell>
                  </TableRow>
                ) : (
                  searchResults.map((slide) => {
                    const inCohort = cohortHashSet.has(slide.slide_hash)
                    return (
                      <TableRow
                        key={slide.slide_hash}
                        className={`${selectedHashes.has(slide.slide_hash) ? 'bg-muted/30' : ''} ${inCohort ? 'opacity-60' : ''}`}
                      >
                        <TableCell onClick={(e) => e.stopPropagation()}>
                          <Checkbox
                            checked={selectedHashes.has(slide.slide_hash)}
                            onCheckedChange={() => toggleSelection(slide.slide_hash)}
                          />
                        </TableCell>
                        <TableCell className="font-medium text-sm">{slide.accession_number}</TableCell>
                        <TableCell className="text-sm">{slide.block_id}</TableCell>
                        <TableCell>
                          <Badge variant="outline" className="text-xs">{slide.stain_type}</Badge>
                        </TableCell>
                        <TableCell className="text-sm">{slide.year || '-'}</TableCell>
                        <TableCell>
                          {inCohort ? (
                            <Badge className="bg-green-500/10 text-green-700 text-xs gap-1">
                              <Check className="h-3 w-3" />
                              In Cohort
                            </Badge>
                          ) : (
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-6 px-2 text-xs"
                              onClick={(e) => {
                                e.stopPropagation()
                                addSlides([slide.slide_hash])
                              }}
                            >
                              <Plus className="h-3 w-3 mr-1" />
                              Add
                            </Button>
                          )}
                        </TableCell>
                      </TableRow>
                    )
                  })
                )}
              </TableBody>
            </Table>
          </div>
        </div>

        {/* RIGHT: Cohort contents */}
        <div className="w-95 flex flex-col border rounded-lg">
          <div className="px-4 py-3 border-b bg-muted/30">
            <h3 className="text-sm font-semibold">Cohort Contents</h3>
            <p className="text-xs text-muted-foreground">{stats.slides} slides in {stats.cases} cases</p>
          </div>

          <div className="flex-1 overflow-auto">
            {caseGroups.length === 0 ? (
              <div className="flex items-center justify-center h-full text-sm text-muted-foreground p-4 text-center">
                No slides yet. Search and add slides from the left panel.
              </div>
            ) : (
              <div className="divide-y">
                {caseGroups.map((group) => {
                  const isCollapsed = collapsedCases.has(group.case_hash)
                  return (
                    <div key={group.case_hash}>
                      {/* Case header */}
                      <button
                        className="w-full flex items-center gap-2 px-4 py-2 text-left hover:bg-muted/50 transition-colors"
                        onClick={() => toggleCaseCollapse(group.case_hash)}
                      >
                        {isCollapsed ? (
                          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                        ) : (
                          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                        )}
                        <span className="text-sm font-medium truncate">
                          {group.accession_number || group.case_hash.slice(0, 8) + '...'}
                        </span>
                        <span className="text-xs text-muted-foreground ml-auto shrink-0">
                          {group.slides.length} slide{group.slides.length !== 1 ? 's' : ''}
                        </span>
                      </button>

                      {/* Slides under this case */}
                      {!isCollapsed && (
                        <div className="pb-1">
                          {group.slides.map((slide) => (
                            <div
                              key={slide.slide_hash}
                              className="flex items-center gap-2 pl-9 pr-3 py-1.5 text-sm group hover:bg-muted/30"
                            >
                              <span className="text-muted-foreground">{slide.block_id}</span>
                              <Badge variant="outline" className="text-xs h-5">
                                {slide.stain_type}
                              </Badge>
                              {slide.tags.length > 0 && (
                                <div className="flex gap-1">
                                  {slide.tags.slice(0, 2).map(tag => (
                                    <Badge key={tag} variant="secondary" className="text-xs h-5">
                                      {tag}
                                    </Badge>
                                  ))}
                                  {slide.tags.length > 2 && (
                                    <span className="text-xs text-muted-foreground">+{slide.tags.length - 2}</span>
                                  )}
                                </div>
                              )}
                              <button
                                className="ml-auto opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-destructive transition-opacity"
                                onClick={() => removeSlide(slide.slide_hash)}
                                title="Remove from cohort"
                              >
                                <X className="h-3.5 w-3.5" />
                              </button>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
