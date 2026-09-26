import { useId, useMemo, useRef, useState } from 'react'
import type { ButtonHTMLAttributes, KeyboardEvent, ReactNode } from 'react'
import { ArrowDown, ArrowUp, Check, ChevronsUpDown, Copy } from 'lucide-react'
import { NavLink } from 'react-router-dom'
import styles from './ui.module.css'

export { default as AppShell } from './AppShell'
export { NAV_GROUPS, sectionFor } from './nav'

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


// ---------------------------------------------------------------------------------------------
// Wave-1 additions: tabs, switch, meter, split layout, copy/code helpers.
// ---------------------------------------------------------------------------------------------

export interface TabDef { id: string; label: ReactNode; count?: number }

/** In-page tabs (state stays in the page). Arrow keys / Home / End move between tabs. */
export function Tabs({ tabs, value, onChange, label = 'Sections' }: {
  tabs: TabDef[]; value: string; onChange: (id: string) => void; label?: string
}) {
  const refs = useRef<Array<HTMLButtonElement | null>>([])
  function onKey(e: KeyboardEvent, i: number) {
    let n = -1
    if (e.key === 'ArrowRight') n = (i + 1) % tabs.length
    else if (e.key === 'ArrowLeft') n = (i - 1 + tabs.length) % tabs.length
    else if (e.key === 'Home') n = 0
    else if (e.key === 'End') n = tabs.length - 1
    if (n >= 0) { e.preventDefault(); onChange(tabs[n].id); refs.current[n]?.focus() }
  }
  return (
    <div className={styles.tabs} role="tablist" aria-label={label}>
      {tabs.map((t, i) => (
        <button key={t.id} type="button" role="tab" ref={(el) => { refs.current[i] = el }}
          aria-selected={t.id === value} tabIndex={t.id === value ? 0 : -1}
          className={cx(styles.tab, t.id === value && styles.tabActive)}
          onClick={() => onChange(t.id)} onKeyDown={(e) => onKey(e, i)}>
          {t.label}{t.count !== undefined && <span className={styles.tabCount}>{t.count}</span>}
        </button>
      ))}
    </div>
  )
}

/** Route-linked tab strip (each tab is a page). AppShell renders one for every nav section that has `tabs`. */
export function RouteTabs({ items, label = 'Section pages' }: { items: Array<{ to: string; label: string }>; label?: string }) {
  return (
    <nav className={styles.tabs} aria-label={label}>
      {items.map((t) => (
        <NavLink key={t.to} to={t.to} end className={({ isActive }) => cx(styles.tab, isActive && styles.tabActive)}>{t.label}</NavLink>
      ))}
    </nav>
  )
}

/** Toggle switch (the mock's `.toggle`). Real button with role=switch. */
export function Switch({ checked, onChange, label, disabled, description }: {
  checked: boolean; onChange: (next: boolean) => void; label: string; disabled?: boolean; description?: ReactNode
}) {
  const id = useId()
  return (
    <div className={styles.switchRow}>
      <div className={styles.switchText}>
        <strong id={`${id}-l`}>{label}</strong>
        {description && <span>{description}</span>}
      </div>
      <button type="button" role="switch" aria-checked={checked} aria-labelledby={`${id}-l`} disabled={disabled}
        className={cx(styles.switch, checked && styles.switchOn)} onClick={() => onChange(!checked)}>
        <span className={styles.switchKnob} />
      </button>
    </div>
  )
}

/** Thin usage bar with a numeric caption ("342 / 600"). `value`/`max` are real gauge readings. */
export function Meter({ value, max, tone, label, caption }: { value: number; max: number; tone?: Tone; label: string; caption?: ReactNode }) {
  const pct = max > 0 ? Math.min(100, Math.round((value / max) * 100)) : 0
  const auto: Tone = tone ?? (pct >= 90 ? 'bad' : pct >= 70 ? 'warn' : 'ok')
  return (
    <div className={styles.meter}>
      <div className={styles.track} role="meter" aria-label={label} aria-valuemin={0} aria-valuemax={max} aria-valuenow={value}>
        <span className={styles[`meter_${auto}`]} style={{ width: `${pct}%` }} />
      </div>
      <span className={styles.meterText}>{caption ?? `${value} / ${max}`}</span>
    </div>
  )
}

/** Two-column layout (mock: `.two-col` / `.half`). Collapses to one column below 900px. */
export function Split({ children, ratio = 'even' }: { children: ReactNode; ratio?: 'even' | 'wide-left' }) {
  return <div className={cx(styles.split, ratio === 'wide-left' && styles.splitWide)}>{children}</div>
}

/** Icon-only button; `label` is the accessible name and tooltip. */
export function IconButton({ label, children, danger, ...rest }: ButtonHTMLAttributes<HTMLButtonElement> & { label: string; danger?: boolean }) {
  return (
    <button type="button" className={cx(styles.iconBtn, danger && styles.iconBtnDanger)} title={label} aria-label={label} {...rest}>
      {children}
    </button>
  )
}

/** One-click copy with a short "Copied" confirmation; falls back to a hidden textarea on plain-http admin URLs. */
export function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false)
  async function copy() {
    try {
      await navigator.clipboard.writeText(text)
    } catch {
      const area = document.createElement('textarea')
      area.value = text
      document.body.appendChild(area)
      area.select()
      document.execCommand('copy')
      area.remove()
    }
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1500)
  }
  return (
    <IconButton label={copied ? 'Copied' : label} onClick={copy}>
      {copied ? <Check size={15} strokeWidth={1.8} aria-hidden="true" /> : <Copy size={15} strokeWidth={1.8} aria-hidden="true" />}
    </IconButton>
  )
}

/** Monospace block (connection snippets, JSON). */
export function CodeBlock({ children, label }: { children: string; label?: string }) {
  return (
    <div className={styles.codeWrap}>
      {label && <span className={styles.codeLabel}>{label}</span>}
      <pre className={styles.code}>{children}</pre>
      <div className={styles.codeCopy}><CopyButton text={children} label={label ? `Copy ${label}` : 'Copy'} /></div>
    </div>
  )
}

/** Name over a muted sub-line: the mock's `.name` / `.sub` table cell pair. */
export function NameCell({ name, sub }: { name: ReactNode; sub?: ReactNode }) {
  return (
    <div>
      <div className={styles.cellName}>{name}</div>
      {sub && <div className={styles.cellSub}>{sub}</div>}
    </div>
  )
}

/** Compact number formatting used by stat cards (1.2K, 3.4M). */
export function compact(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return String(n)
}

/** Mode tag for an interface (Relay green, Adapt neutral, Emulate accent), per the mock. */
export function ModeTag({ mode }: { mode: string | null | undefined }) {
  if (!mode) return <span className={styles.cellSub}>n/a</span>
  return <span className={cx(styles.tag, mode === 'Relay' && styles.tagGreen)}>{mode}</span>
}
