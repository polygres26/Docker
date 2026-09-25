import { useId, useMemo, useState } from 'react'
import type { ButtonHTMLAttributes, ReactNode } from 'react'
import { ArrowDown, ArrowUp, ChevronsUpDown } from 'lucide-react'
import styles from './ui.module.css'

export { default as AppShell } from './AppShell'

function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(' ')
}

/** Page title, one-line description and optional right-aligned actions. */
export function PageHeader({ title, description, actions }: { title: string; description?: ReactNode; actions?: ReactNode }) {
  return (
    <div className={styles.pageHead}>
      <div className={styles.pageHeadText}>
        <h1 className={styles.pageTitle}>{title}</h1>
        {description && <p className={styles.pageDesc}>{description}</p>}
      </div>
      {actions && <div className={styles.pageActions}>{actions}</div>}
    </div>
  )
}

/** Bordered surface with a hairline-divided head. `flush` removes body padding (tables, grids). */
export function Section({ title, meta, children, flush, className }: {
  title?: string; meta?: ReactNode; children: ReactNode; flush?: boolean; className?: string
}) {
  return (
    <section className={cx(styles.section, className)}>
      {(title || meta) && (
        <div className={styles.sectionHead}>
          {title && <h2 className={styles.sectionTitle}>{title}</h2>}
          {meta && <span className={styles.sectionMeta}>{meta}</span>}
        </div>
      )}
      <div className={flush ? undefined : styles.sectionBody}>{children}</div>
    </section>
  )
}

export type Tone = 'ok' | 'warn' | 'bad' | 'muted' | 'accent'

export interface KpiItem { label: string; value?: ReactNode; hint?: ReactNode; tone?: Tone; wide?: boolean }

/** Health-line strip: cells separated by hairlines, first cell may be wide (status text). */
export function KpiStrip({ items, label = 'Key figures' }: { items: KpiItem[]; label?: string }) {
  return (
    <div className={styles.kpiStrip} role="group" aria-label={label}
      style={{ gridTemplateColumns: items.map((i) => (i.wide ? '1.5fr' : '1fr')).join(' ') }}>
      {items.map((it) => (
        <div className={styles.kpiCell} key={it.label}>
          <small className={styles.kpiLabel}>{it.label}</small>
          {it.tone && it.wide
            ? <span className={cx(styles.kpiLive, styles[`tone_${it.tone}`])}><i className={styles.dot} />{it.value}</span>
            : <strong className={styles.kpiValue}>{it.value ?? '—'}</strong>}
          {it.hint && <span className={styles.kpiHint}>{it.hint}</span>}
        </div>
      ))}
    </div>
  )
}

/** Interface summary cards (the mock's "By interface" grid): hairline-divided cells. */
export function SummaryGrid({ items }: { items: Array<{ title: string; sub?: ReactNode; icon?: ReactNode }> }) {
  return (
    <div className={styles.summaryGrid}>
      {items.map((it) => (
        <div className={styles.summaryCell} key={it.title}>
          {it.icon && <div className={styles.summaryIcon}>{it.icon}</div>}
          <strong>{it.title}</strong>
          {it.sub && <span>{it.sub}</span>}
        </div>
      ))}
    </div>
  )
}

/** Horizontally scrolling table container. Pass <thead>/<tbody> as children. */
export function DataTable({ children, minWidth = 560, caption }: { children: ReactNode; minWidth?: number; caption?: string }) {
  return (
    <div className={styles.tableClip} tabIndex={0} role="region" aria-label={caption ?? 'Table'}>
      <table className={styles.table} style={{ minWidth }}>{children}</table>
    </div>
  )
}

export type SortDir = 'asc' | 'desc'

/** Sort state helper: returns sorted rows plus header props for <SortTh>. */
export function useSort<T>(rows: T[], accessors: Record<string, (r: T) => string | number>, initial: { key: string; dir: SortDir }) {
  const [sort, setSort] = useState(initial)
  const sorted = useMemo(() => {
    const get = accessors[sort.key]
    if (!get) return rows
    const out = [...rows].sort((a, b) => {
      const x = get(a); const y = get(b)
      const c = typeof x === 'number' && typeof y === 'number' ? x - y : String(x).localeCompare(String(y))
      return sort.dir === 'asc' ? c : -c
    })
    return out
  }, [rows, sort])
  function toggle(key: string) {
    setSort((s) => (s.key === key ? { key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: 'asc' }))
  }
  return { sorted, sort, toggle }
}

export function SortTh({ label, k, sort, onSort, align }: {
  label: string; k: string; sort: { key: string; dir: SortDir }; onSort: (k: string) => void; align?: 'right'
}) {
  const active = sort.key === k
  const Icon = !active ? ChevronsUpDown : sort.dir === 'asc' ? ArrowUp : ArrowDown
  return (
    <th aria-sort={active ? (sort.dir === 'asc' ? 'ascending' : 'descending') : 'none'} style={align ? { textAlign: align } : undefined}>
      <button type="button" className={styles.sortBtn} onClick={() => onSort(k)}>
        {label}<Icon size={12} aria-hidden="true" />
      </button>
    </th>
  )
}

export function StatusPill({ tone = 'muted', children, dot = true }: { tone?: Tone; children: ReactNode; dot?: boolean }) {
  return <span className={cx(styles.pill, styles[`pill_${tone}`])}>{dot && <i className={styles.dot} aria-hidden="true" />}{children}</span>
}

/** Small uppercase tag (the mock's "mode" chip). */
export function Tag({ children }: { children: ReactNode }) {
  return <span className={styles.tag}>{children}</span>
}

type Variant = 'primary' | 'secondary' | 'ghost' | 'danger'
export function Button({ variant = 'secondary', icon, children, className, type = 'button', ...rest }: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: Variant; icon?: ReactNode }) {
  return (
    <button type={type} className={cx(styles.btn, styles[`btn_${variant}`], className)} {...rest}>
      {icon}{children}
    </button>
  )
}

/** Labelled control. The child receives a generated id via render prop to keep label association. */
export function Field({ label, hint, children, className }: {
  label: ReactNode; hint?: ReactNode; children: (id: string) => ReactNode; className?: string
}) {
  const id = useId()
  return (
    <div className={cx(styles.field, className)}>
      <label htmlFor={id} className={styles.fieldLabel}>{label}</label>
      {hint && <div className={styles.fieldHint} id={`${id}-hint`}>{hint}</div>}
      {children(id)}
    </div>
  )
}

export function EmptyState({ title, children, icon }: { title: string; children?: ReactNode; icon?: ReactNode }) {
  return (
    <div className={styles.empty}>
      {icon && <div className={styles.emptyIcon}>{icon}</div>}
      <strong>{title}</strong>
      {children && <p>{children}</p>}
    </div>
  )
}

/** Inline message: error / success / info. Errors use role=alert. */
export function Notice({ tone = 'muted', children }: { tone?: 'bad' | 'ok' | 'warn' | 'muted'; children: ReactNode }) {
  return <div role={tone === 'bad' ? 'alert' : 'status'} className={cx(styles.notice, styles[`notice_${tone}`])}>{children}</div>
}

export function Loading({ children = 'Loading…' }: { children?: ReactNode }) {
  return <div className={styles.loading} role="status">{children}</div>
}
