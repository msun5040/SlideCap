/**
 * Reader for the binary cohort-projection artifact.
 *
 * Layout (see backend/app/services/cohort_projection.py):
 *   magic "SCPROJ01" (8B) | header length (uint32 LE) | JSON header | raw columns
 *
 * Columnar on purpose: at a million patches the per-point JSON objects the
 * single-slide renderer emits would be hundreds of megabytes and would have to
 * be parsed into objects before anything could be drawn. This maps straight
 * onto typed arrays with no per-point allocation.
 */

const MAGIC = 'SCPROJ01'

export interface ProjectionSlide {
  slide_hash: string
  display_name: string
  n_patches: number
  patch_size: number
  patch_level: number
  /** Index of this slide's first point in the column arrays. */
  start: number
}

export interface ProjectionHeader {
  version: number
  method: string
  params: Record<string, unknown>
  point_count: number
  feature_dim: number
  columns: { name: string; dtype: string; offset: number; bytes: number }[]
  slides: ProjectionSlide[]
}

export interface ProjectionData {
  header: ProjectionHeader
  pointCount: number
  x: Float32Array
  y: Float32Array
  /** Index into header.slides — which slide each point came from. */
  slideIdx: Uint16Array | Uint32Array
  /** Patch top-left in level-0 slide pixels. */
  patchX: Int32Array
  patchY: Int32Array
  /** Data-space bounds, precomputed once. */
  bounds: { minX: number; maxX: number; minY: number; maxY: number }
}

const DTYPE_BYTES: Record<string, number> = {
  float32: 4, int32: 4, uint32: 4, uint16: 2, uint8: 1,
}

function typedArray(dtype: string, buf: ArrayBuffer, offset: number, bytes: number) {
  const width = DTYPE_BYTES[dtype]
  if (!width) throw new Error(`Unsupported column dtype: ${dtype}`)

  // A typed-array view needs its byte offset to be a multiple of the element
  // size. The writer pads the header to keep the column block 8-byte aligned,
  // but fall back to copying so an artifact written before that padding (or by
  // any other producer) still loads instead of throwing.
  const src = offset % width === 0 ? buf : buf.slice(offset, offset + bytes)
  const off = offset % width === 0 ? offset : 0

  switch (dtype) {
    case 'float32': return new Float32Array(src, off, bytes / 4)
    case 'int32': return new Int32Array(src, off, bytes / 4)
    case 'uint32': return new Uint32Array(src, off, bytes / 4)
    case 'uint16': return new Uint16Array(src, off, bytes / 2)
    case 'uint8': return new Uint8Array(src, off, bytes)
    default: throw new Error(`Unsupported column dtype: ${dtype}`)
  }
}

export function parseProjection(buf: ArrayBuffer): ProjectionData {
  const magic = new TextDecoder().decode(new Uint8Array(buf, 0, 8))
  if (magic !== MAGIC) {
    throw new Error(`Not a projection artifact (magic "${magic}"). The file may be truncated.`)
  }
  const headerLen = new DataView(buf).getUint32(8, true)
  const header: ProjectionHeader = JSON.parse(
    new TextDecoder().decode(new Uint8Array(buf, 12, headerLen)),
  )
  const base = 12 + headerLen

  const col = (name: string) => {
    const c = header.columns.find(c => c.name === name)
    if (!c) throw new Error(`Projection artifact is missing the "${name}" column.`)
    return typedArray(c.dtype, buf, base + c.offset, c.bytes)
  }

  const x = col('x') as Float32Array
  const y = col('y') as Float32Array
  const slideIdx = col('slide_idx') as Uint16Array | Uint32Array
  const patchX = col('patch_x') as Int32Array
  const patchY = col('patch_y') as Int32Array

  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity
  for (let i = 0; i < x.length; i++) {
    const xi = x[i], yi = y[i]
    if (xi < minX) minX = xi
    if (xi > maxX) maxX = xi
    if (yi < minY) minY = yi
    if (yi > maxY) maxY = yi
  }

  return {
    header,
    pointCount: header.point_count,
    x, y, slideIdx, patchX, patchY,
    bounds: { minX, maxX, minY, maxY },
  }
}

/**
 * Uniform-grid spatial index over the projected points.
 *
 * The per-slide scatter hit-tests by scanning every point (EmbeddingScatter.tsx
 * documents that as good to ~50k). At cohort scale that's a scan of a million
 * points on every mouse move, so bucket once and scan a handful of cells.
 */
export class PointGrid {
  private cells: Int32Array          // flattened bucket contents
  private starts: Int32Array         // per-cell offset into `cells`
  private counts: Int32Array
  readonly cols: number
  readonly rows: number
  private minX: number
  private minY: number
  private cellW: number
  private cellH: number

  constructor(private data: ProjectionData, targetPerCell = 24) {
    const { minX, maxX, minY, maxY } = data.bounds
    const n = data.pointCount
    const target = Math.max(1, Math.ceil(n / targetPerCell))
    const side = Math.max(1, Math.min(1024, Math.round(Math.sqrt(target))))
    this.cols = side
    this.rows = side
    this.minX = minX
    this.minY = minY
    // Guard against a degenerate (zero-extent) axis.
    this.cellW = (maxX - minX || 1) / side
    this.cellH = (maxY - minY || 1) / side

    const nCells = side * side
    this.counts = new Int32Array(nCells)
    const cellOf = new Int32Array(n)
    for (let i = 0; i < n; i++) {
      const c = this.cellIndex(data.x[i], data.y[i])
      cellOf[i] = c
      this.counts[c]++
    }
    this.starts = new Int32Array(nCells + 1)
    for (let c = 0; c < nCells; c++) this.starts[c + 1] = this.starts[c] + this.counts[c]
    const cursor = this.starts.slice(0, nCells)
    this.cells = new Int32Array(n)
    for (let i = 0; i < n; i++) this.cells[cursor[cellOf[i]]++] = i
  }

  private cellIndex(x: number, y: number): number {
    const cx = Math.min(this.cols - 1, Math.max(0, Math.floor((x - this.minX) / this.cellW)))
    const cy = Math.min(this.rows - 1, Math.max(0, Math.floor((y - this.minY) / this.cellH)))
    return cy * this.cols + cx
  }

  /** Nearest point to (x, y) within `radius` data units, or -1. `accept` skips points (e.g. hidden ones). */
  nearest(x: number, y: number, radius: number, accept?: (i: number) => boolean): number {
    const { x: px, y: py } = this.data
    const spanX = Math.ceil(radius / this.cellW)
    const spanY = Math.ceil(radius / this.cellH)
    const cx = Math.min(this.cols - 1, Math.max(0, Math.floor((x - this.minX) / this.cellW)))
    const cy = Math.min(this.rows - 1, Math.max(0, Math.floor((y - this.minY) / this.cellH)))

    let best = -1
    let bestD = radius * radius
    for (let gy = cy - spanY; gy <= cy + spanY; gy++) {
      if (gy < 0 || gy >= this.rows) continue
      for (let gx = cx - spanX; gx <= cx + spanX; gx++) {
        if (gx < 0 || gx >= this.cols) continue
        const c = gy * this.cols + gx
        for (let k = this.starts[c]; k < this.starts[c + 1]; k++) {
          const i = this.cells[k]
          const dx = px[i] - x, dy = py[i] - y
          const d = dx * dx + dy * dy
          if (d < bestD && (!accept || accept(i))) { bestD = d; best = i }
        }
      }
    }
    return best
  }
}

/** Which slide a point index belongs to, and its patch rect in level-0 pixels. */
export function pointPatch(data: ProjectionData, i: number) {
  const slide = data.header.slides[data.slideIdx[i]]
  return {
    slide,
    slide_x: data.patchX[i],
    slide_y: data.patchY[i],
    size: slide?.patch_size ?? 256,
  }
}

/**
 * Find the point covering a level-0 (x, y) on a given slide.
 *
 * Restricted to that slide's contiguous index range — the artifact stores points
 * grouped by slide with a `start` offset precisely so this stays cheap. Prefers
 * a patch that actually contains the click, falling back to the nearest centre.
 */
export function pointAtSlideXY(
  data: ProjectionData, slideIndex: number, x: number, y: number,
): number {
  const slide = data.header.slides[slideIndex]
  if (!slide) return -1
  const start = slide.start
  const end = start + slide.n_patches
  const size = slide.patch_size || 256

  let nearest = -1
  let nearestD = Infinity
  for (let i = start; i < end; i++) {
    const px = data.patchX[i], py = data.patchY[i]
    if (x >= px && x < px + size && y >= py && y < py + size) return i
    const dx = px + size / 2 - x, dy = py + size / 2 - y
    const d = dx * dx + dy * dy
    if (d < nearestD) { nearestD = d; nearest = i }
  }
  return nearest
}
