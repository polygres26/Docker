import type { Tone } from '../ui'

/** Migration/sizing tier -> status colour: green = easy/small, amber = medium, red = hard/large. */
export function tierTone(tier: string): Tone {
  const t = tier.toUpperCase()
  if (t.startsWith('EASY') || t === 'SMALL') return 'green'
  if (t.startsWith('MEDIUM')) return 'amber'
  return 'red'
}

export function severityTone(severity: string): Tone {
  if (severity === 'HIGH') return 'red'
  if (severity === 'MEDIUM') return 'amber'
  return 'green'
}
