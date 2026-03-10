interface LoadingSpinnerProps {
  size?: 'sm' | 'md' | 'lg'
  label?: string
  className?: string
}

export function LoadingSpinner({ size = 'md', label, className = '' }: LoadingSpinnerProps) {
  const sizeMap = {
    sm: { wh: 'h-4 w-4', border: '2px' },
    md: { wh: 'h-8 w-8', border: '3px' },
    lg: { wh: 'h-12 w-12', border: '4px' },
  }

  const s = sizeMap[size]

  return (
    <div className={`flex flex-col items-center gap-3 ${className}`}>
      <div
        className={`${s.wh} rounded-full animate-spin`}
        style={{
          border: `${s.border} solid var(--border)`,
          borderTopColor: 'var(--primary)',
        }}
      />
      {label && (
        <p className="text-sm text-muted-foreground animate-pulse">{label}</p>
      )}
    </div>
  )
}
