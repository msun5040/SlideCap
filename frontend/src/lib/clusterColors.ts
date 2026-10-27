/**
 * The cluster palette, shared.
 *
 * Cluster 3 has to be the same colour in the projection scatter, the WSI mask,
 * the composition bars and the slide dialog — otherwise the one thing these
 * views are for, comparing them, stops working. Lives here rather than in any
 * one component so nothing can quietly drift.
 *
 * Clusters are numbered 1-based everywhere in the UI; indices here are 0-based,
 * matching the label files.
 */
export const CLUSTER_COLORS = [
  '#ef4444', '#3b82f6', '#22c55e', '#eab308', '#8b5cf6',
  '#ec4899', '#14b8a6', '#f97316', '#6366f1', '#84cc16',
]

/** Distinct colours past the first ten: golden-angle hue steps. */
export function clusterColor(i: number): string {
  if (i < CLUSTER_COLORS.length) return CLUSTER_COLORS[i]
  return `hsl(${Math.round((i * 137.508) % 360)} 65% 55%)`
}
