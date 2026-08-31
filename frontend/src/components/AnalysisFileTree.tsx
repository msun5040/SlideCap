import { useEffect, useState } from 'react'
import {
  Archive,
  ChevronDown,
  ChevronRight,
  FileDown,
  FileText,
  Folder,
  Image as ImageIcon,
  Loader2,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { getApiBase } from '@/api'
import { saveBlob } from '@/lib/download'

export interface FileTreeNode {
  name: string
  type: 'file' | 'dir'
  path: string        // relative path from slide output dir (posix)
  size?: number       // files only
  is_image?: boolean  // files only
  children?: FileTreeNode[]  // dirs only
}

interface Props {
  jobId: number
  slideHash: string
}

function formatSize(bytes: number) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

const encodeFilePath = (path: string) =>
  path.split('/').map(encodeURIComponent).join('/')

/**
 * Read-only, self-contained browser for an analysis's output directory for a
 * single (job, slide). Fetches the same `FileTreeNode[]` tree the Analysis
 * Results tab uses (`/results/{jobId}/files?slide_hash=`) and renders it as an
 * expandable folder/file tree with per-file and per-folder downloads.
 *
 * Intentionally lighter than the Results-tab tree: no cart, no delete, no
 * overlay wiring — just "expand the analysis and see what's in it", suitable
 * for embedding inside the Slide Details dialog.
 */
export function AnalysisFileTree({ jobId, slideHash }: Props) {
  const [tree, setTree] = useState<FileTreeNode[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [expanded, setExpanded] = useState<Set<string>>(new Set())

  useEffect(() => {
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    fetch(
      `${getApiBase()}/results/${jobId}/files?slide_hash=${encodeURIComponent(slideHash)}`,
      { signal: ac.signal },
    )
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data: FileTreeNode[]) => {
        setTree(Array.isArray(data) ? data : [])
        setLoading(false)
      })
      .catch((e) => {
        if (e.name === 'AbortError') return
        setError(e.message || 'Failed to load files')
        setLoading(false)
      })
    return () => ac.abort()
  }, [jobId, slideHash])

  const toggleFolder = (path: string) => {
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(path)) next.delete(path)
      else next.add(path)
      return next
    })
  }

  const downloadViaFetch = async (url: string, filename: string) => {
    try {
      const res = await fetch(url)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      saveBlob(await res.blob(), filename)
    } catch (e) {
      console.error('Download failed:', e)
    }
  }

  const downloadFile = (filePath: string) =>
    downloadViaFetch(
      `${getApiBase()}/results/${jobId}/file/${encodeFilePath(filePath)}?slide_hash=${encodeURIComponent(slideHash)}`,
      filePath.split('/').pop()!,
    )

  const downloadFolder = (folderPath: string) =>
    downloadViaFetch(
      `${getApiBase()}/results/${jobId}/download-folder?slide_hash=${encodeURIComponent(slideHash)}&folder_path=${encodeURIComponent(folderPath)}`,
      `${folderPath.split('/').pop()}.zip`,
    )

  const previewFile = async (filePath: string) => {
    try {
      const res = await fetch(
        `${getApiBase()}/results/${jobId}/file/${encodeFilePath(filePath)}?slide_hash=${encodeURIComponent(slideHash)}`,
      )
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const blob = await res.blob()
      window.open(URL.createObjectURL(blob), '_blank')
    } catch (e) {
      console.error('Preview failed:', e)
    }
  }

  const renderNodes = (nodes: FileTreeNode[], depth: number): React.ReactNode =>
    nodes.map((node) => {
      const indentPx = depth * 16
      if (node.type === 'dir') {
        const isOpen = expanded.has(node.path)
        return (
          <div key={node.path}>
            <div
              className="flex items-center justify-between py-0.5 pr-1 hover:bg-muted/20 rounded-sm"
              style={{ paddingLeft: `${indentPx}px` }}
            >
              <button
                className="flex items-center gap-1.5 text-sm text-left min-w-0"
                onClick={() => toggleFolder(node.path)}
              >
                {isOpen
                  ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                  : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />}
                <Folder className="h-3.5 w-3.5 text-amber-500 shrink-0" />
                <span className="font-mono text-xs truncate">{node.name}/</span>
              </button>
              <button
                className="p-1 rounded hover:bg-muted text-muted-foreground hover:text-foreground transition-colors shrink-0"
                title="Download folder as ZIP"
                onClick={() => downloadFolder(node.path)}
              >
                <Archive className="h-3.5 w-3.5" />
              </button>
            </div>
            {isOpen && node.children && node.children.length > 0 &&
              renderNodes(node.children, depth + 1)}
            {isOpen && node.children && node.children.length === 0 && (
              <p
                className="text-xs text-muted-foreground py-0.5"
                style={{ paddingLeft: `${indentPx + 20}px` }}
              >
                Empty folder
              </p>
            )}
          </div>
        )
      }

      // File node
      return (
        <div
          key={node.path}
          className="flex items-center justify-between py-0.5 pr-1"
          style={{ paddingLeft: `${indentPx + 20}px` }}
        >
          <div className="flex items-center gap-1.5 min-w-0">
            {node.is_image
              ? <ImageIcon className="h-3.5 w-3.5 text-blue-500 shrink-0" />
              : <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />}
            <span className="text-xs font-mono truncate" title={node.name}>{node.name}</span>
            {node.size !== undefined && (
              <span className="text-[11px] text-muted-foreground shrink-0">{formatSize(node.size)}</span>
            )}
          </div>
          <div className="flex gap-0.5 shrink-0">
            {node.is_image && (
              <Button
                variant="ghost"
                size="sm"
                className="h-6 text-xs px-2"
                onClick={() => previewFile(node.path)}
              >
                Preview
              </Button>
            )}
            <Button
              variant="ghost"
              size="sm"
              className="h-6 px-2"
              title="Download file"
              onClick={() => downloadFile(node.path)}
            >
              <FileDown className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>
      )
    })

  if (loading) {
    return (
      <div className="flex items-center gap-2 py-2 pl-2 text-xs text-muted-foreground">
        <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading files…
      </div>
    )
  }

  if (error) {
    return <p className="text-xs text-red-600 py-2 pl-2">Could not load files: {error}</p>
  }

  if (!tree || tree.length === 0) {
    return <p className="text-xs text-muted-foreground py-2 pl-2">No output files found for this analysis.</p>
  }

  return <div className="py-1">{renderNodes(tree, 0)}</div>
}
