import { CopyButton } from '@/components/ui/CopyButton'

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
  return (
    <span
      className={`inline-flex items-center gap-1 group ${mono ? 'font-mono' : ''} ${className}`}
      title={copyValue ?? text}
    >
      <span className="truncate">{text}</span>
      <CopyButton
        value={copyValue ?? text}
        iconClassName="h-3 w-3"
        title="Copy to clipboard"
        className="shrink-0 p-0.5 rounded hover:bg-muted [&>svg]:opacity-0 group-hover:[&>svg]:opacity-60 data-[copied]:[&>svg]:opacity-100"
      />
    </span>
  )
}
