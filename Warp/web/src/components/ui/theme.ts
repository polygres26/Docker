// Theme preference: 'system' follows prefers-color-scheme; 'light'/'dark' pin <html data-theme>.
// tokens.css uses light-dark(), which follows the `color-scheme` that data-theme sets.
import { useCallback, useEffect, useState } from 'react'

export type ThemePref = 'system' | 'light' | 'dark'
const KEY = 'warp.theme'

function read(): ThemePref {
  try {
    const v = localStorage.getItem(KEY)
    return v === 'light' || v === 'dark' ? v : 'system'
  } catch { return 'system' }
}

export function applyTheme(pref: ThemePref): void {
  const root = document.documentElement
  if (pref === 'system') root.removeAttribute('data-theme')
  else root.setAttribute('data-theme', pref)
}

/** Call once at startup so the pinned theme is applied before first paint of the app. */
export function initTheme(): void { applyTheme(read()) }

export function useTheme(): { pref: ThemePref; cycle: () => void } {
  const [pref, setPref] = useState<ThemePref>(read)
  useEffect(() => { applyTheme(pref) }, [pref])
  const cycle = useCallback(() => {
    setPref((p) => {
      const next: ThemePref = p === 'system' ? 'light' : p === 'light' ? 'dark' : 'system'
      try { if (next === 'system') localStorage.removeItem(KEY); else localStorage.setItem(KEY, next) } catch { /* private mode */ }
      return next
    })
  }, [])
  return { pref, cycle }
}
