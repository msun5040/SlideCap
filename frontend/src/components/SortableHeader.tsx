import { ArrowUp, ArrowDown, ArrowUpDown } from 'lucide-react'
import type { SortConfig } from '@/hooks/useSortable'

interface SortableHeaderProps {
  label: string
  sortKey: string
  sortConfig: SortConfig | null
  onSort: (key: string) => void
  className?: string
}

export function SortableHeader({ label, sortKey, sortConfig, onSort, className = '' }: SortableHeaderProps) {
  const active = sortConfig?.key === sortKey
  const Icon = active
    ? sortConfig.direction === 'asc' ? ArrowUp : ArrowDown
    : ArrowUpDown

  return (
    <button
      className={`inline-flex items-center gap-1 hover:text-foreground transition-colors select-none ${active ? 'text-foreground' : ''} ${className}`}
      onClick={() => onSort(sortKey)}
    >
      {label}
      <Icon className={`h-3 w-3 ${active ? 'opacity-100' : 'opacity-40'}`} />
    </button>
  )
}
