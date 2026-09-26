import { useMemo, useState } from 'react'
import styles from './charts.module.css'

export interface ChartSeries { label: string; color: string; points: Array<{ t: number; v: number }> }

const W = 560
const H = 150
const PAD = { l: 42, r: 8, t: 8, b: 20 }

/** "Nice" upper bound for a y axis so tick labels are round. */
function niceMax(v: number): number {
  if (v <= 0) return 1
  const p = Math.pow(10, Math.floor(Math.log10(v)))
  const n = v / p
  return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * p
}

function clock(t: number): string {
  return new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

/**
 * Inline-SVG time series (no chart library). Every point is a real sampled value; nothing is smoothed or
 * interpolated beyond the straight segment between two samples. Hover shows the nearest sample per series.
 * Renders an honest empty state when there are fewer than two points.
 */
export function TimeSeries({ series, format = (v: number) => v.toLocaleString(undefined, { maximumFractionDigits: 2 }), label, emptyText, area = false }: {
  series: ChartSeries[]; format?: (v: number) => string; label: string; emptyText?: string; area?: boolean
}) {
  const [hover, setHover] = useState<number | null>(null)
  const live = series.filter((s) => s.points.length >= 1)
  const geo = useMemo(() => {
    const all = live.flatMap((s) => s.points)
    if (all.length === 0) return null
    const t0 = Math.min(...all.map((p) => p.t)); const t1 = Math.max(...all.map((p) => p.t))
    const ymax = niceMax(Math.max(...all.map((p) => p.v)))
    const x = (t: number) => PAD.l + (t1 === t0 ? (W - PAD.l - PAD.r) / 2 : ((t - t0) / (t1 - t0)) * (W - PAD.l - PAD.r))
    const y = (v: number) => PAD.t + (1 - v / ymax) * (H - PAD.t - PAD.b)
    return { t0, t1, ymax, x, y }
  }, [live])

  if (!geo || live.every((s) => s.points.length < 2)) {
    return <div className={styles.empty} role="img" aria-label={label}>{emptyText ?? 'Not enough samples yet. The server samples its counters every 10 seconds; a trend appears after two samples.'}</div>
  }

  const ticks = [0, 0.5, 1]
  const times = Array.from(new Set(live.flatMap((s) => s.points.map((p) => p.t)))).sort((a, b) => a - b)
  const hoverT = hover !== null ? times[hover] : null
  function onMove(e: React.MouseEvent<SVGSVGElement>) {
    const r = e.currentTarget.getBoundingClientRect()
    const px = ((e.clientX - r.left) / r.width) * W
    let best = 0; let bd = Infinity
    times.forEach((t, i) => { const d = Math.abs(geo!.x(t) - px); if (d < bd) { bd = d; best = i } })
    setHover(best)
  }
  return (
    <div className={styles.wrap}>
      <svg className={styles.svg} viewBox={`0 0 ${W} ${H}`} role="img" aria-label={label} onMouseMove={onMove} onMouseLeave={() => setHover(null)}>
        {ticks.map((k) => (
          <g key={k}>
            <line className={styles.grid} x1={PAD.l} x2={W - PAD.r} y1={geo.y(geo.ymax * k)} y2={geo.y(geo.ymax * k)} />
            <text className={styles.axis} x={PAD.l - 6} y={geo.y(geo.ymax * k) + 3} textAnchor="end">{format(geo.ymax * k)}</text>
          </g>
        ))}
        <text className={styles.axis} x={PAD.l} y={H - 4}>{clock(geo.t0)}</text>
        <text className={styles.axis} x={W - PAD.r} y={H - 4} textAnchor="end">{clock(geo.t1)}</text>
        {live.map((s) => {
          const d = s.points.map((p, i) => `${i === 0 ? 'M' : 'L'}${geo.x(p.t).toFixed(1)},${geo.y(p.v).toFixed(1)}`).join(' ')
          const last = s.points[s.points.length - 1]; const first = s.points[0]
          return (
            <g key={s.label}>
              {area && <path className={styles.area} d={`${d} L${geo.x(last.t).toFixed(1)},${geo.y(0)} L${geo.x(first.t).toFixed(1)},${geo.y(0)} Z`} fill={s.color} />}
              <path className={styles.line} d={d} stroke={s.color} />
            </g>
          )
        })}
        {hoverT !== null && <line className={styles.hover} x1={geo.x(hoverT)} x2={geo.x(hoverT)} y1={PAD.t} y2={H - PAD.b} />}
      </svg>
      {hoverT !== null && (
        <div className={styles.tip} style={{ left: `${(geo.x(hoverT) / W) * 100}%`, top: 0 }}>
          <b>{clock(hoverT)}</b>
          {live.map((s) => { const p = s.points.find((q) => q.t === hoverT); return p ? <div key={s.label}>{s.label}: {format(p.v)}</div> : null })}
        </div>
      )}
      {live.length > 1 && <div className={styles.legend}>{live.map((s) => <span key={s.label}><i style={{ background: s.color }} />{s.label}</span>)}</div>}
    </div>
  )
}
