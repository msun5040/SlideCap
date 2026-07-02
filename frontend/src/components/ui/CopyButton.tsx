import { useState, useCallback, useRef, useEffect } from 'react'
import { Copy, Check } from 'lucide-react'
import { cn } from '@/lib/utils'
import { copyToClipboard } from '@/lib/clipboard'

interface CopyButtonProps {
  /** Text placed on the clipboard when clicked. */
  value: string
  /** Extra classes on the button element. */
  className?: string
  /** Icon size/color classes (default: h-3.5 w-3.5). */
  iconClassName?: string
  /** Tooltip / aria-label (default: "Copy"). */
  title?: string
  /** Optional label shown next to the icon (idle state). */
  label?: React.ReactNode
  /** Optional label shown while in the copied state (default: "Copied!"). */
  copiedLabel?: React.ReactNode
  /** How long the check mark stays, in ms (default 1500). */
  duration?: number
  disabled?: boolean
  /** Stop click from bubbling to a parent (default true — copy buttons usually sit inside clickable rows). */
  stopPropagation?: boolean
  /** Fired after a successful copy. */
  onCopied?: () => void
}

/**
 * App-wide copy affordance: the copy icon morphs into a green check for a
 * couple of seconds on success. Uses copyToClipboard() so it also works over
 * plain HTTP on the LAN deployment. Renders icon-only, or icon + label.
 */
export function CopyButton({
  value,
  className = '',
  iconClassName = 'h-3.5 w-3.5',
  title = 'Copy',
  label,
  copiedLabel = 'Copied!',
  duration = 1500,
  disabled = false,
  stopPropagation = true,
  onCopied,
}: CopyButtonProps) {
  const [copied, setCopied] = useState(false)
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Clear the pending reset if the button unmounts mid-animation.
  useEffect(() => () => { if (timer.current) clearTimeout(timer.current) }, [])

  const handleClick = useCallback(
    async (e: React.MouseEvent) => {
      if (stopPropagation) e.stopPropagation()
      const ok = await copyToClipboard(value)
      if (!ok) return
      onCopied?.()
      setCopied(true)
      if (timer.current) clearTimeout(timer.current)
      timer.current = setTimeout(() => setCopied(false), duration)
    },
    [value, duration, stopPropagation, onCopied],
  )

  return (
    <button
      type="button"
      onClick={handleClick}
      disabled={disabled}
      title={copied ? 'Copied!' : title}
      aria-label={title}
      data-copied={copied ? '' : undefined}
      className={cn('inline-flex items-center gap-1 transition disabled:opacity-50 disabled:pointer-events-none', className)}
    >
      {copied ? (
        <Check className={cn(iconClassName, 'text-green-600')} />
      ) : (
        <Copy className={iconClassName} />
      )}
      {label != null && <span>{copied ? copiedLabel : label}</span>}
    </button>
  )
}
