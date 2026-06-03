import { Eye, EyeOff, X as Close } from 'lucide-react'
import { useState } from 'react'

/** Spec used to *request* an overlay (passed into the viewer). */
export interface OverlaySpec {
  id: string
  name: string
  type: 'geojson'           // future: 'image', 'points', ...
  url: string               // absolute URL the viewer fetches
  visible?: boolean
  opacity?: number          // 0–1
  colorBy?: string          // dotted path into feature.properties
}

/** A unique color-by value with its assigned color + on/off state. */
export interface OverlayClass {
  value: string                 // raw feature property value, stringified
  color: string                 // hex color used to draw features in this class
  enabled: boolean              // when false, features in this class are skipped
}

/** Runtime overlay state held by the viewer; same fields, no optionals. */
export interface OverlayRuntime extends OverlaySpec {
  visible: boolean
  opacity: number
  /** Available property fields the user can color by (populated as the file loads). */
  fields?: string[]
  /** Distinct color-by values aggregated from features. Drives the legend. */
  classes?: OverlayClass[]
}

interface OverlayControlsProps {
  overlays: OverlayRuntime[]
  onToggle: (id: string) => void
  onOpacity: (id: string, opacity: number) => void
  onColorBy: (id: string, field: string) => void
  onToggleClass: (id: string, value: string) => void
  onSetAllClasses: (id: string, enabled: boolean) => void
}

export function OverlayControls({ overlays, onToggle, onOpacity, onColorBy, onToggleClass, onSetAllClasses }: OverlayControlsProps) {
  const [collapsed, setCollapsed] = useState(false)

  if (collapsed) {
    return (
      <button
        className="absolute top-4 left-4 z-30 bg-black/70 hover:bg-black/85 text-white text-xs px-2.5 py-1.5 rounded shadow-lg backdrop-blur-sm"
        onClick={() => setCollapsed(false)}
      >
        Overlays ({overlays.filter(o => o.visible).length}/{overlays.length})
      </button>
    )
  }

  return (
    <div className="absolute top-4 left-4 z-30 w-72 bg-black/75 text-white rounded-lg shadow-xl backdrop-blur-sm overflow-hidden">
      <div className="flex items-center justify-between px-3 py-2 border-b border-white/10 text-xs uppercase tracking-wider text-white/70">
        <span>Overlays</span>
        <button onClick={() => setCollapsed(true)} className="opacity-70 hover:opacity-100">
          <Close className="h-3.5 w-3.5" />
        </button>
      </div>
      <div className="max-h-[60vh] overflow-y-auto p-2 space-y-2">
        {overlays.map(o => (
          <div key={o.id} className="rounded bg-white/5 p-2 space-y-2">
            <div className="flex items-center gap-2">
              <button
                onClick={() => onToggle(o.id)}
                className="shrink-0 p-1 rounded hover:bg-white/10"
                title={o.visible ? 'Hide' : 'Show'}
              >
                {o.visible
                  ? <Eye className="h-3.5 w-3.5" />
                  : <EyeOff className="h-3.5 w-3.5 opacity-50" />}
              </button>
              <span className="text-[12px] font-medium truncate" title={o.name}>{o.name}</span>
            </div>

            {o.visible && (
              <>
                <div className="space-y-0.5">
                  <label className="text-[10px] uppercase tracking-wide text-white/50">Opacity</label>
                  <input
                    type="range"
                    min={0}
                    max={1}
                    step={0.05}
                    value={o.opacity}
                    onChange={(e) => onOpacity(o.id, parseFloat(e.target.value))}
                    className="w-full accent-emerald-500"
                  />
                </div>

                {o.fields && o.fields.length > 0 && (
                  <div className="space-y-0.5">
                    <label className="text-[10px] uppercase tracking-wide text-white/50">Color by</label>
                    <select
                      value={o.colorBy || ''}
                      onChange={(e) => onColorBy(o.id, e.target.value)}
                      className="w-full bg-black/40 text-[11px] border border-white/15 rounded px-1.5 py-1"
                    >
                      <option value="">(uniform color)</option>
                      {o.fields.map(f => (
                        <option key={f} value={f}>{f}</option>
                      ))}
                    </select>
                  </div>
                )}

                {o.colorBy && o.classes && o.classes.length > 0 && (
                  <div className="space-y-1">
                    <div className="flex items-center justify-between">
                      <label className="text-[10px] uppercase tracking-wide text-white/50">
                        Legend ({o.classes.filter(c => c.enabled).length}/{o.classes.length})
                      </label>
                      <div className="flex gap-1 text-[10px]">
                        <button
                          className="text-white/60 hover:text-white"
                          onClick={() => onSetAllClasses(o.id, true)}
                        >
                          all
                        </button>
                        <span className="text-white/30">·</span>
                        <button
                          className="text-white/60 hover:text-white"
                          onClick={() => onSetAllClasses(o.id, false)}
                        >
                          none
                        </button>
                      </div>
                    </div>
                    <div className="space-y-0.5 max-h-48 overflow-y-auto pr-1">
                      {o.classes.map(cls => (
                        <button
                          key={cls.value}
                          onClick={() => onToggleClass(o.id, cls.value)}
                          className={`flex items-center gap-2 w-full text-left px-1.5 py-1 rounded text-[11px] hover:bg-white/5 transition-colors ${
                            cls.enabled ? '' : 'opacity-40'
                          }`}
                        >
                          <span
                            className="h-3 w-3 rounded-sm shrink-0 border border-white/20"
                            style={{ backgroundColor: cls.color }}
                          />
                          <span className="flex-1 truncate" title={cls.value}>{cls.value || '(empty)'}</span>
                          {cls.enabled
                            ? <Eye className="h-3 w-3 shrink-0 opacity-70" />
                            : <EyeOff className="h-3 w-3 shrink-0 opacity-40" />}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
