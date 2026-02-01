export interface Slide {
  slide_hash: string
  accession_number: string
  block_id: string           // Block ID like A1, B2
  slide_number: string       // Slide number (1, 2, 3)
  year?: number
  stain_type: string
  random_id?: string
  case_hash?: string
  slide_tags?: string[]
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
