import { useState, useMemo } from 'react'

export type SortDirection = 'asc' | 'desc'

export interface SortConfig {
  key: string
  direction: SortDirection
}

export function useSortable<T>(
  items: T[],
  defaultSort?: SortConfig
) {
  const [sortConfig, setSortConfig] = useState<SortConfig | null>(defaultSort ?? null)

  const handleSort = (key: string) => {
    setSortConfig(prev => {
      if (prev?.key === key) {
        if (prev.direction === 'asc') return { key, direction: 'desc' }
        // clicking a third time clears the sort
        return null
      }
      return { key, direction: 'asc' }
    })
  }

  const sorted = useMemo(() => {
    if (!sortConfig) return items

    const { key, direction } = sortConfig
    return [...items].sort((a, b) => {
      const aVal = (a as Record<string, unknown>)[key]
      const bVal = (b as Record<string, unknown>)[key]

      // nulls/undefined always last
      if (aVal == null && bVal == null) return 0
      if (aVal == null) return 1
      if (bVal == null) return -1

      let cmp: number
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        cmp = aVal - bVal
      } else {
        cmp = String(aVal).localeCompare(String(bVal), undefined, { numeric: true, sensitivity: 'base' })
      }

      return direction === 'asc' ? cmp : -cmp
    })
  }, [items, sortConfig])

  return { sorted, sortConfig, handleSort }
}
