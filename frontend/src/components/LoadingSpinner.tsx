interface LoadingSpinnerProps {
  size?: 'sm' | 'md' | 'lg'
  label?: string
  className?: string
}

export function LoadingSpinner({ size = 'md', label, className = '' }: LoadingSpinnerProps) {
  const sizeMap = {
    sm: { wh: 'h-3.5 w-3.5', border: '1.5px' },
    md: { wh: 'h-5 w-5', border: '2px' },
    lg: { wh: 'h-7 w-7', border: '2px' },
  }

  const s = sizeMap[size]

  return (
    <div className={`flex flex-col items-center gap-2.5 ${className}`}>
      <div
        className={`${s.wh} rounded-full animate-spin`}
        style={{
          border: `${s.border} solid var(--border)`,
          borderTopColor: 'var(--primary)',
        }}
      />
      {label && (
        <p className="text-[12px] text-muted-foreground">{label}</p>
      )}
    </div>
  )
}
