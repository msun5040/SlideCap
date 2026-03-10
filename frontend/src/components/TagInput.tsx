import { useState, useEffect, useRef } from 'react'
import { X, Plus } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import type { Tag } from '@/types/slide'

import { getApiBase } from '@/api'

// Preset colors for tags
const PRESET_COLORS = [
  '#EF4444', // red
  '#F97316', // orange
  '#EAB308', // yellow
  '#22C55E', // green
  '#14B8A6', // teal
  '#3B82F6', // blue
  '#8B5CF6', // purple
  '#EC4899', // pink
  '#6B7280', // gray
]

interface TagInputProps {
  slideHash: string
  currentTags: Tag[]
  onTagsChange: (tags: Tag[]) => void
}

export function TagInput({ slideHash, currentTags, onTagsChange }: TagInputProps) {
  const [inputValue, setInputValue] = useState('')
  const [suggestions, setSuggestions] = useState<Tag[]>([])
  const [showSuggestions, setShowSuggestions] = useState(false)
  const [selectedColor, setSelectedColor] = useState(PRESET_COLORS[0])
  const [isAdding, setIsAdding] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const suggestionsRef = useRef<HTMLDivElement>(null)

  // Fetch autocomplete suggestions
  useEffect(() => {
    if (inputValue.length < 1) {
      setSuggestions([])
      return
    }

    const fetchSuggestions = async () => {
      try {
        const response = await fetch(`${getApiBase()}/tags/search?q=${encodeURIComponent(inputValue)}`)
        if (response.ok) {
          const data = await response.json()
          // Filter out tags already applied (case-insensitive)
          const filtered = data.filter(
            (t: Tag) => !currentTags.some(ct => ct.name.toLowerCase() === t.name.toLowerCase())
          )
          setSuggestions(filtered)
        }
      } catch (error) {
        console.error('Failed to fetch tag suggestions:', error)
      }
    }

    const debounce = setTimeout(fetchSuggestions, 150)
    return () => clearTimeout(debounce)
  }, [inputValue, currentTags])

  // Close suggestions when clicking outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        suggestionsRef.current &&
        !suggestionsRef.current.contains(e.target as Node) &&
        !inputRef.current?.contains(e.target as Node)
      ) {
        setShowSuggestions(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const addTag = async (tagName: string, color?: string) => {
    if (!tagName.trim() || isAdding) return

    // Check for duplicate
    const trimmedName = tagName.trim().toLowerCase()
    if (currentTags.some(t => t.name.toLowerCase() === trimmedName)) {
      setInputValue('')
      setShowSuggestions(false)
      return // Tag already exists on this slide
    }

    const tagColor = color || selectedColor
    const optimisticTag: Tag = {
      id: -1, // Temporary ID
      name: tagName.trim(),
      color: tagColor
    }

    // Optimistic update - add immediately to UI
    const previousTags = currentTags
    onTagsChange([...currentTags, optimisticTag])
    setInputValue('')
    setShowSuggestions(false)

    setIsAdding(true)
    try {
      const response = await fetch(`${getApiBase()}/slides/${slideHash}/tags`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: tagName.trim(), color: tagColor })
      })

      if (response.ok) {
        const data = await response.json()
        // Replace optimistic tag with real tag from server
        onTagsChange([...previousTags, data.tag])
      } else {
        // Revert on failure
        onTagsChange(previousTags)
        const errorData = await response.json().catch(() => ({}))
        console.error('Failed to add tag:', response.status, errorData)
        alert(`Failed to add tag: ${errorData.detail || response.statusText}`)
      }
    } catch (error) {
      // Revert on error
      onTagsChange(previousTags)
      console.error('Failed to add tag:', error)
      alert(`Failed to add tag: ${error}`)
    } finally {
      setIsAdding(false)
    }
  }

  const removeTag = async (tagName: string) => {
    // Optimistic update - remove immediately from UI
    const previousTags = currentTags
    onTagsChange(currentTags.filter(t => t.name !== tagName))

    try {
      const response = await fetch(`${getApiBase()}/slides/${slideHash}/tags/${encodeURIComponent(tagName)}`, {
        method: 'DELETE'
      })

      if (!response.ok) {
        // Revert on failure
        onTagsChange(previousTags)
        console.error('Failed to remove tag')
      }
    } catch (error) {
      // Revert on error
      onTagsChange(previousTags)
      console.error('Failed to remove tag:', error)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && inputValue.trim()) {
      e.preventDefault()
      // If there's an exact match in suggestions, use that
      const exactMatch = suggestions.find(s => s.name.toLowerCase() === inputValue.toLowerCase())
      if (exactMatch) {
        addTag(exactMatch.name, exactMatch.color || undefined)
      } else {
        // Create new tag with selected color
        addTag(inputValue, selectedColor)
      }
    } else if (e.key === 'Escape') {
      setShowSuggestions(false)
    }
  }

  const selectSuggestion = (tag: Tag) => {
    addTag(tag.name, tag.color || undefined)
  }

  // Check if input matches an existing tag
  const isExistingTag = suggestions.some(s => s.name.toLowerCase() === inputValue.toLowerCase())

  return (
    <div className="space-y-3">
      {/* Current tags */}
      <div className="flex flex-wrap gap-2 min-h-[40px] p-2 rounded-lg border bg-muted/30">
        {currentTags.length === 0 ? (
          <span className="text-sm text-muted-foreground py-1">No tags yet</span>
        ) : (
          currentTags.map((tag) => (
            <Badge
              key={tag.name}
              variant="secondary"
              className="gap-1 pr-1"
              style={{
                backgroundColor: tag.color ? `${tag.color}20` : undefined,
                borderColor: tag.color || undefined,
                borderWidth: tag.color ? 1 : undefined,
              }}
            >
              {tag.color && (
                <span
                  className="w-2 h-2 rounded-full mr-1"
                  style={{ backgroundColor: tag.color }}
                />
              )}
              {tag.name}
              <button
                onClick={() => removeTag(tag.name)}
                className="ml-1 hover:bg-destructive/20 rounded p-0.5"
              >
                <X className="h-3 w-3" />
              </button>
            </Badge>
          ))
        )}
      </div>

      {/* Color selector and input */}
      <div className="space-y-2">
        <div className="flex gap-1 items-center">
          <span className="text-xs text-muted-foreground mr-1">Color:</span>
          {PRESET_COLORS.map((color) => (
            <button
              key={color}
              className={`w-5 h-5 rounded-full border-2 transition-all ${
                selectedColor === color ? 'border-black scale-110 ring-1 ring-black' : 'border-transparent hover:scale-105'
              }`}
              style={{ backgroundColor: color }}
              onClick={() => setSelectedColor(color)}
              title={color}
            />
          ))}
        </div>

        {/* Input with autocomplete */}
        <div className="relative">
          <Input
            ref={inputRef}
            placeholder="Type to add a tag..."
            value={inputValue}
            onChange={(e) => {
              setInputValue(e.target.value)
              setShowSuggestions(true)
            }}
            onFocus={() => setShowSuggestions(true)}
            onKeyDown={handleKeyDown}
          />

          {/* Autocomplete dropdown */}
          {showSuggestions && (suggestions.length > 0 || (inputValue && !isExistingTag)) && (
            <div
              ref={suggestionsRef}
              className="absolute top-full left-0 right-0 mt-1 bg-popover border rounded-lg shadow-lg z-50 overflow-hidden"
            >
              {suggestions.map((tag) => (
                <button
                  key={tag.id}
                  className="w-full px-3 py-2 text-left hover:bg-muted flex items-center gap-2"
                  onClick={() => selectSuggestion(tag)}
                >
                  {tag.color && (
                    <span
                      className="w-3 h-3 rounded-full"
                      style={{ backgroundColor: tag.color }}
                    />
                  )}
                  <span>{tag.name}</span>
                  {tag.category && (
                    <span className="text-xs text-muted-foreground ml-auto">
                      {tag.category}
                    </span>
                  )}
                </button>
              ))}

              {/* Option to create new tag */}
              {inputValue && !isExistingTag && (
                <button
                  className="w-full px-3 py-2 text-left hover:bg-muted flex items-center gap-2 border-t"
                  onClick={() => addTag(inputValue, selectedColor)}
                >
                  <Plus className="h-4 w-4" />
                  <span
                    className="w-3 h-3 rounded-full"
                    style={{ backgroundColor: selectedColor }}
                  />
                  <span>Create "{inputValue}"</span>
                </button>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
