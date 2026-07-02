// Copy text to the clipboard with a fallback for non-secure contexts.
//
// `navigator.clipboard` only exists in secure contexts (HTTPS or localhost).
// SlideCap is deployed on a Windows server and accessed by other machines over
// plain HTTP on the LAN (e.g. http://10.0.0.5:3000), where `navigator.clipboard`
// is undefined — so the Async Clipboard API silently fails there. We fall back to
// the legacy execCommand('copy') path via a hidden textarea, which works over HTTP.
//
// Returns true on success, false if both paths fail.
export async function copyToClipboard(text: string): Promise<boolean> {
  // Preferred path: Async Clipboard API (secure contexts only).
  if (typeof navigator !== 'undefined' && navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text)
      return true
    } catch {
      // fall through to legacy path
    }
  }

  // Legacy fallback: works over HTTP. Uses a temporary off-screen textarea.
  try {
    const textarea = document.createElement('textarea')
    textarea.value = text
    // Keep it out of view and non-disruptive to scroll/focus.
    textarea.setAttribute('readonly', '')
    textarea.style.position = 'fixed'
    textarea.style.top = '-9999px'
    textarea.style.left = '-9999px'
    document.body.appendChild(textarea)
    textarea.select()
    textarea.setSelectionRange(0, text.length)
    const ok = document.execCommand('copy')
    document.body.removeChild(textarea)
    return ok
  } catch {
    return false
  }
}
