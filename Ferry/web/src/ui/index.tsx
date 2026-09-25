import type { ButtonHTMLAttributes, ComponentProps, ComponentType, InputHTMLAttributes, ReactNode } from 'react'
import { Link, type LinkProps } from 'react-router-dom'
import { ArrowLeft } from 'lucide-react'
import s from './ui.module.css'

const cx = (...parts: Array<string | false | null | undefined>) => parts.filter(Boolean).join(' ')

/* ---------- Page header ---------- */

export function PageHeader({ title, subtitle, actions, back }: {
  title: ReactNode
  subtitle?: ReactNode
  actions?: ReactNode
  back?: { to: string; label: string }
}) {
  return (
    <>
      {back && (
        <Link to={back.to} className={s.backLink}><ArrowLeft size={13} strokeWidth={1.8} aria-hidden />{back.label}</Link>
      )}
      <header className={s.pageHead}>
        <div className={s.pageHeadText}>
          <h1 className={s.pageTitle}>{title}</h1>
          {subtitle && <p className={s.pageSub}>{subtitle}</p>}
        </div>
        {actions && <div className={s.pageActions}>{actions}</div>}
      </header>
    </>
  )
}

/* ---------- Section (bordered card) ---------- */

export function Section({ title, meta, actions, flush, warn, children, className, headingLevel: H = 'h2' }: {
  title?: ReactNode
  meta?: ReactNode
  actions?: ReactNode
  /** No body padding -- for tables / lists that go edge to edge. */
  flush?: boolean
  warn?: boolean
  children?: ReactNode
  className?: string
  headingLevel?: 'h2' | 'h3'
}) {
  return (
    <section className={cx(s.section, warn && s.sectionWarn, className)}>
      {(title || actions || meta) && (
        <div className={s.sectionHead}>
          <div>
            {title && <H className={s.sectionTitle}>{title}</H>}
            {meta && <span className={s.sectionMeta}>{meta}</span>}
          </div>
          {actions}
        </div>
      )}
      <div className={flush ? s.sectionBodyFlush : s.sectionBody}>{children}</div>
    </section>
  )
}

/* ---------- KPI strip ---------- */

export interface KpiItem { label: string; value: ReactNode; hint?: ReactNode }

export function KpiStrip({ items, label = 'Summary' }: { items: KpiItem[]; label?: string }) {
  return (
    <dl className={s.kpis} aria-label={label}>
      {items.map((k) => (
        <div key={k.label} className={s.kpi}>
          <dt className={s.kpiLabel}>{k.label}</dt>
          <dd className={s.kpiValue}>{k.value}</dd>
          {k.hint && <dd className={s.kpiHint}>{k.hint}</dd>}
        </div>
      ))}
    </dl>
  )
}

/* ---------- Data table ---------- */

/** Horizontally scrolls inside its own container so the page never scrolls sideways. */
export function DataTable({ children, minWidth = true, caption }: { children: ReactNode; minWidth?: boolean; caption?: string }) {
  return (
    <div className={s.tableClip} tabIndex={0} role="region" aria-label={caption ?? 'Table'}>
      <table className={cx(s.table, minWidth && s.tableMin)}>
        {caption && <caption className="sr-only">{caption}</caption>}
        {children}
      </table>
    </div>
  )
}

/* ---------- Status pill / tag ---------- */

export type Tone = 'green' | 'amber' | 'red' | 'neutral' | 'accent'
const pillTone: Record<Tone, string> = { green: s.pillGreen, amber: s.pillAmber, red: s.pillRed, neutral: s.pillNeutral, accent: s.pillAccent }
const tagTone: Record<Tone, string> = { green: s.tagGreen, amber: s.tagAmber, red: s.tagRed, neutral: s.tagNeutral, accent: '' }

export function StatusPill({ tone = 'neutral', children }: { tone?: Tone; children: ReactNode }) {
  return <span className={cx(s.pill, pillTone[tone])}><span className={s.pillDot} aria-hidden />{children}</span>
}

export function Tag({ tone = 'accent', children }: { tone?: Tone; children: ReactNode }) {
  return <span className={cx(s.tag, tagTone[tone])}>{children}</span>
}

/* ---------- Buttons ---------- */

type Variant = 'primary' | 'secondary' | 'ghost' | 'danger'
const variantClass: Record<Variant, string> = { primary: s.btnPrimary, secondary: s.btnSecondary, ghost: s.btnGhost, danger: s.btnDanger }

function btnClass(variant: Variant, size?: 'sm', full?: boolean, icon?: boolean, extra?: string) {
  return cx(s.btn, variantClass[variant], size === 'sm' && s.btnSm, full && s.btnFull, icon && s.btnIcon, extra)
}

export function Button({ variant = 'secondary', size, full, icon, className, type = 'button', ...rest }: ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: Variant; size?: 'sm'; full?: boolean; icon?: boolean
}) {
  return <button type={type} className={btnClass(variant, size, full, icon, className)} {...rest} />
}

export function LinkButton({ variant = 'secondary', size, full, className, ...rest }: LinkProps & { variant?: Variant; size?: 'sm'; full?: boolean }) {
  return <Link className={btnClass(variant, size, full, false, className)} {...rest} />
}

/* ---------- Form ---------- */

export function Field({ label, htmlFor, hint, children }: { label: ReactNode; htmlFor?: string; hint?: ReactNode; children: ReactNode }) {
  return (
    <div className={s.field}>
      <label className={s.label} htmlFor={htmlFor}>{label}</label>
      {children}
      {hint && <span className={s.hint}>{hint}</span>}
    </div>
  )
}

export function Input({ large, className, ...rest }: ComponentProps<'input'> & { large?: boolean }) {
  return <input className={cx(s.input, large && s.inputLg, className)} {...rest} />
}

export function Select({ className, ...rest }: ComponentProps<'select'>) {
  return <select className={cx(s.input, className)} {...rest} />
}

export function Textarea({ className, ...rest }: ComponentProps<'textarea'>) {
  return <textarea className={cx(s.input, className)} {...rest} />
}

export function Check({ children, ...rest }: InputHTMLAttributes<HTMLInputElement> & { children: ReactNode }) {
  return <label className={s.check}><input type="checkbox" {...rest} />{children}</label>
}

export function FormActions({ children }: { children: ReactNode }) {
  return <div className={s.formActions}>{children}</div>
}

/** Responsive multi-column grid for form fields. */
export function FormGrid({ children }: { children: ReactNode }) {
  return <div className={s.formGrid}>{children}</div>
}

/* ---------- Feedback ---------- */

export function Notice({ tone = 'info', title, children }: { tone?: 'error' | 'warn' | 'ok' | 'info'; title?: ReactNode; children?: ReactNode }) {
  const toneClass = { error: s.noticeError, warn: s.noticeWarn, ok: s.noticeOk, info: s.noticeInfo }[tone]
  return (
    <div className={cx(s.notice, toneClass)} role={tone === 'error' ? 'alert' : 'status'}>
      {title && <strong>{title}</strong>}
      {children && (title ? <p>{children}</p> : <>{children}</>)}
    </div>
  )
}

export function EmptyState({ icon: Icon, title, children, action }: {
  icon?: ComponentType<{ size?: number; strokeWidth?: number }>
  title: ReactNode
  children?: ReactNode
  action?: ReactNode
}) {
  return (
    <div className={s.empty}>
      {Icon && <div className={s.emptyIcon}><Icon size={22} strokeWidth={1.6} /></div>}
      <div className={s.emptyTitle}>{title}</div>
      {children && <p>{children}</p>}
      {action && <div className={s.emptyAction}>{action}</div>}
    </div>
  )
}

export function Loading({ children = 'Loading…' }: { children?: ReactNode }) {
  return <div className={s.loading} role="status"><span className={s.spinner} aria-hidden />{children}</div>
}

/* ---------- Layout helpers ---------- */

export const Narrow = ({ children }: { children: ReactNode }) => <div className={s.narrow}>{children}</div>
export const Stack = ({ children }: { children: ReactNode }) => <div className={s.stack}>{children}</div>
export const Split = ({ children }: { children: ReactNode }) => <div className={s.split}>{children}</div>
export const List = ({ children }: { children: ReactNode }) => <ul className={s.list}>{children}</ul>
export const Bullets = ({ children }: { children: ReactNode }) => <ul className={cx(s.bullets, s.muted)}>{children}</ul>

/** Thin determinate progress bar (green when complete, amber while in progress). */
export function ProgressBar({ pct, label }: { pct: number; label: string }) {
  return (
    <div className={s.bar} role="progressbar" aria-label={label} aria-valuemin={0} aria-valuemax={100} aria-valuenow={pct}>
      <span className={pct >= 100 ? s.barDone : s.barActive} style={{ width: `${Math.min(100, Math.max(0, pct))}%` }} />
    </div>
  )
}

/** Anchor styled as a button, for plain (non-router) links such as file downloads. */
export function AnchorButton({ variant = 'secondary', size, className, ...rest }: ComponentProps<'a'> & { variant?: Variant; size?: 'sm' }) {
  return <a className={btnClass(variant, size, false, false, className)} {...rest} />
}

/** In-page tab strip (state-driven, not routes). */
export function Tabs<T extends string>({ tabs, value, onChange, label }: {
  tabs: Array<{ id: T; label: string }>
  value: T
  onChange: (id: T) => void
  label: string
}) {
  return (
    <div className={s.tabs} role="tablist" aria-label={label}>
      {tabs.map((t) => (
        <button
          key={t.id} type="button" role="tab" aria-selected={value === t.id}
          className={cx(s.tab, value === t.id && s.tabActive)}
          onClick={() => onChange(t.id)}
        >
          {t.label}
        </button>
      ))}
    </div>
  )
}

/** Segmented easy / medium / hard meter with a marker at `pct` (0-100). */
export function Meter({ pct, segments, label }: { pct: number; segments: Array<{ flex: number; tone: 'green' | 'amber' | 'red'; label: string }>; label: string }) {
  const tone = { green: s.meterGreen, amber: s.meterAmber, red: s.meterRed }
  return (
    <div>
      <div className={s.meter} role="img" aria-label={label}>
        {segments.map((seg) => <div key={seg.label} className={tone[seg.tone]} style={{ flex: seg.flex }} />)}
        <div className={s.meterMarker} style={{ left: `calc(${Math.min(100, Math.max(0, pct))}% - 2px)` }} />
      </div>
      <div className={s.meterLabels}>{segments.map((seg) => <span key={seg.label}>{seg.label}</span>)}</div>
    </div>
  )
}

export function CodeBlock({ children, maxHeight }: { children: ReactNode; maxHeight?: number }) {
  return <pre className={s.code} style={maxHeight ? { maxHeight } : undefined} tabIndex={0}>{children}</pre>
}
