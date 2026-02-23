import { useState, useCallback } from 'react'
import { Copy, Check } from 'lucide-react'

interface CopyableTextProps {
  /** The text displayed */
  text: string
  /** The value copied to clipboard (defaults to text) */
  copyValue?: string
  /** Additional CSS classes on the outer wrapper */
  className?: string
  /** Mono font (default true) */
  mono?: boolean
}

export function CopyableText({ text, copyValue, className = '', mono = true }: CopyableTextProps) {
  const [copied, setCopied] = useState(false)

  const handleCopy = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation()
      navigator.clipboard.writeText(copyValue ?? text).then(() => {
        setCopied(true)
        setTimeout(() => setCopied(false), 1500)
      })
    },
    [text, copyValue],
  )

  return (
    <span
      className={`inline-flex items-center gap-1 group ${mono ? 'font-mono' : ''} ${className}`}
      title={copyValue ?? text}
    >
      <span className="truncate">{text}</span>
      <button
        type="button"
        className="shrink-0 p-0.5 rounded hover:bg-muted transition-colors"
        onClick={handleCopy}
        title="Copy to clipboard"
      >
        {copied ? (
          <Check className="h-3 w-3 text-green-500" />
        ) : (
          <Copy className="h-3 w-3 opacity-0 group-hover:opacity-60 transition-opacity" />
        )}
      </button>
    </span>
  )
}
