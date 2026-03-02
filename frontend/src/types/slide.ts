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
  completed_analyses?: string[]
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

export interface CohortSlide {
  slide_hash: string
  accession_number: string | null
  block_id: string
  slide_number: string | null
  stain_type: string
  random_id?: string
  year: number | null
  case_hash: string | null
  tags: string[]
  file_size_bytes?: number
}

export interface CohortFlag {
  id: number
  name: string
  case_hashes: string[]
}

export interface CohortDetail {
  id: number
  name: string
  description?: string
  source_type: string
  source_details?: string
  slide_count: number
  case_count: number
  created_by?: string
  created_at?: string
  updated_at?: string
  slides: CohortSlide[]
}

export interface CaseGroup {
  case_hash: string
  accession_number: string | null
  year: number | null
  slides: CohortSlide[]
}

export interface PatientSurgery {
  id: number
  surgery_label: string   // "S1", "S2", "S3"
  case_hash: string
  accession_number: string | null
  year: number | null
  slide_count: number
  note?: string
}

export interface CohortPatient {
  id: number
  label: string           // user-defined de-identified label, e.g. "P001"
  note?: string
  surgeries: PatientSurgery[]
}

export interface Analysis {
  id: number
  name: string
  version: string
  description?: string
  script_path?: string
  working_directory?: string
  env_setup?: string
  command_template?: string
  postprocess_template?: string  // Post-processing command template
  parameters_schema?: string   // JSON Schema string
  default_parameters?: string  // JSON string
  gpu_required: boolean
  estimated_runtime_minutes: number
  active: boolean
  created_at?: string
  job_count?: number
}

export interface JobSlide {
  id: number
  slide_hash?: string
  cluster_job_id?: string
  status: 'pending' | 'transferring' | 'running' | 'completed' | 'failed'
  started_at?: string
  completed_at?: string
  error_message?: string
  log_tail?: string
  remote_output_path?: string
}

export interface AnalysisJob {
  id: number
  analysis_id?: number
  model_name: string
  model_version?: string
  parameters?: string
  gpu_index?: number
  status: 'pending' | 'transferring' | 'running' | 'completed' | 'failed'
  submitted_by?: string
  submitted_at?: string
  started_at?: string
  completed_at?: string
  error_message?: string
  // Progress
  slide_count: number
  completed_count: number
  failed_count: number
  // Nested slides (in detail view)
  slides?: JobSlide[]
}

export interface GpuInfo {
  index: number
  name: string
  memory_used_mb: number
  memory_total_mb: number
  utilization_pct: number
}

export interface ClusterStatus {
  connected: boolean
  host?: string
  port?: number
  username?: string
  gpus?: GpuInfo[]
  gpu_error?: string
}
