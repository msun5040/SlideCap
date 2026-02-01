import { useState, useEffect } from 'react'
import { Search, Filter, Tag, Plus, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
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
import type { Slide } from '@/types/slide'

const API_BASE = 'http://localhost:8000'

export function SlideLibrary() {
  const [slides, setSlides] = useState<Slide[]>([])
  const [loading, setLoading] = useState(false)
  const [searchTerm, setSearchTerm] = useState('')
  const [stainFilter, setStainFilter] = useState<string>('all')
  const [yearFilter, setYearFilter] = useState<string>('all')
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const [selectedSlide, setSelectedSlide] = useState<Slide | null>(null)
  const [isTagDialogOpen, setIsTagDialogOpen] = useState(false)
  const [isDetailsDialogOpen, setIsDetailsDialogOpen] = useState(false)
  const [newTag, setNewTag] = useState('')
  const [stats, setStats] = useState<{ total_slides: number; total_cases: number } | null>(null)

  useEffect(() => {
    fetchStats()
  }, [])

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

  const handleSearch = async () => {
    if (!searchTerm.trim()) return

    setLoading(true)
    try {
      let url = `${API_BASE}/search?q=${encodeURIComponent(searchTerm)}`
      if (yearFilter !== 'all') url += `&year=${yearFilter}`
      if (stainFilter !== 'all') url += `&stain=${stainFilter}`

      const response = await fetch(url)
      if (response.ok) {
        const data = await response.json()
        setSlides(data.results)
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

  const openTagDialog = (slide: Slide, e: React.MouseEvent) => {
    e.stopPropagation()
    setSelectedSlide(slide)
    setIsTagDialogOpen(true)
  }

  const openDetailsDialog = (slide: Slide) => {
    setSelectedSlide(slide)
    setIsDetailsDialogOpen(true)
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

          <Button onClick={handleSearch} disabled={loading}>
            {loading ? 'Searching...' : 'Search'}
          </Button>
        </div>
      </div>

      <div className="text-sm text-muted-foreground">
        Showing {filteredSlides.length} slides
      </div>

      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            <TableRow>
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
                <TableCell colSpan={7} className="h-24 text-center">
                  {slides.length === 0
                    ? 'Search for slides to get started.'
                    : 'No slides found matching your criteria.'}
                </TableCell>
              </TableRow>
            ) : (
              filteredSlides.map((slide) => (
                <TableRow
                  key={slide.slide_hash}
                  className="hover:bg-muted/50 cursor-pointer"
                  onClick={() => openDetailsDialog(slide)}
                >
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
                  <TableCell>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={(e) => openTagDialog(slide, e)}
                      className="h-7"
                    >
                      <Tag className="mr-1 h-3 w-3" />
                      {slide.slide_tags?.length || 0}
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
              {selectedSlide?.accession_number} - Block {selectedSlide?.block_id}, Slide {selectedSlide?.slide_number}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Current Tags</label>
              <div className="flex flex-wrap gap-2 min-h-15 rounded-lg border p-3">
                {selectedSlide && selectedSlide.slide_tags && selectedSlide.slide_tags.length > 0 ? (
                  selectedSlide.slide_tags.map((tag: string) => (
                    <Badge key={tag} variant="secondary" className="gap-1">
                      {tag}
                      <button className="ml-1 hover:text-destructive">
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  ))
                ) : (
                  <p className="text-sm text-muted-foreground">No tags yet</p>
                )}
              </div>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Add New Tag</label>
              <div className="flex gap-2">
                <Input
                  placeholder="Enter tag name..."
                  value={newTag}
                  onChange={(e) => setNewTag(e.target.value)}
                />
                <Button disabled={!newTag.trim()}>
                  <Plus className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </div>
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
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
