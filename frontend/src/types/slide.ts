export interface Tag {
  id: number
  name: string
  color?: string  // Hex color like "#FF5733"
  category?: string
  slide_count?: number  // Number of slides with this tag
  case_count?: number   // Number of cases with this tag
}

export interface Slide {
  slide_hash: string
  accession_number: string
  block_id: string           // Block ID like A1, B2
  slide_number: string       // Slide number (1, 2, 3)
  year?: number
  stain_type: string
  random_id?: string
  case_hash?: string
  slide_tags?: string[]      // Tag names from search results
  case_tags?: string[]
  projects?: string[]
  file_size_bytes?: number
  file_path?: string
  status?: 'available' | 'in-analysis' | 'archived'
}

export interface SearchFilters {
  year?: string
  stain?: string
}

export interface Cohort {
  id: number
  name: string
  description?: string
  source_type: 'manual' | 'upload' | 'filter' | 'tag'
  source_details?: string
  slide_count: number
  case_count: number
  created_by?: string
  created_at?: string
  updated_at?: string
}
