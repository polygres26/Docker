import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { ArrowLeftRight, RefreshCw } from 'lucide-react'
import { getAbRouting, getAbStats, type AbCompareEntry, type AbPolicy, type AbState, type AbStats } from '../../api/client'
import { getAbCompareAll, getCapture } from '../../api/wave2b'
import {
  Button, CodeBlock, DataTable, EmptyState, Field, KpiStrip, Loading, NameCell, Notice, PageHeader, Section, StatusPill, Tag, compact,
  type KpiItem, type Tone,
} from '../../components/ui'
import { useLoad } from '../../hooks'
import styles from './wave2b.module.css'

const POLL_MS = 10_000
const COMPARE_LIMIT = 1000

// ---- local (browser-only) settings: readiness thresholds and difference triage ----

interface Thresholds { minCompared: number; maxMismatchPct: number; maxErrorPct: number; maxP95IncreasePct: number }
const DEFAULT_THRESHOLDS: Thresholds = { minCompared: 100, maxMismatchPct: 1, maxErrorPct: 1, maxP95IncreasePct: 25 }
const TH_KEY = 'warp.lab.thresholds'
const DEC_KEY = 'warp.lab.decisions'
type Decision = 'review' | 'accept' | 'fix'

function readLocal<T>(key: string, fallback: T): T {
  try { const v = localStorage.getItem(key); return v ? { ...fallback, ...JSON.parse(v) } : fallback } catch { return fallback }
}
function writeLocal(key: string, value: unknown) {
  try { localStorage.setItem(key, JSON.stringify(value)) } catch { /* private window: keep in memory only */ }
}

function p95(values: number[]): number | null {
  if (values.length < 5) return null
  const s = [...values].sort((a, b) => a - b)
  return s[Math.min(s.length - 1, Math.ceil(0.95 * s.length) - 1)]
}

type Severity = 'High' | 'Medium' | 'Low'
/** Derived from the diff lines the compare buffer records: a status mismatch or a failed side is High, a body difference Medium, header-only Low. */
function severityOf(e: AbCompareEntry): Severity {
  if (e.localStatus !== e.cloudStatus || e.diffs.some((d) => d.startsWith('local side failed') || d.startsWith('cloud side failed'))) return 'High'
  return e.diffs.length > 0 && e.diffs.every((d) => d.startsWith('header')) ? 'Low' : 'Medium'
}
const SEV_TONE: Record<Severity, Tone> = { High: 'bad', Medium: 'warn', Low: 'muted' }
const SEV_RANK: Record<Severity, number> = { High: 0, Medium: 1, Low: 2 }

interface DiffGroup { key: string; store: string; op: string; scenario: string; lines: string[]; localStatus: number; cloudStatus: number; count: number; last: number; severity: Severity }

/** Strips the per-request values (ETags, timestamps, byte counts, list indexes) from a diff line so identical kinds of difference group together. */
function normalise(d: string): string {
  if (d.startsWith('status:')) return d
  return d.replace(/\s*\((local|cloud)=.*\)$/, '').replace(/:\s*(local|cloud)=.*$/, '').replace(/\d+ bytes/g, 'N bytes').replace(/\[\d+\]/g, '[]')
}

const MAX_GROUPS = 40

function groupDiffs(entries: AbCompareEntry[]): DiffGroup[] {
  const map = new Map<string, DiffGroup>()
  for (const e of entries) {
    if (e.equal) continue
    const sig = [...new Set(e.diffs.map(normalise))].join(' | ')
    const key = `${e.store}|${e.op}|${e.localStatus}|${e.cloudStatus}|${sig}`
    const g = map.get(key)
    if (g) { g.count++; g.last = Math.max(g.last, e.ts) }
    else map.set(key, { key, store: e.store, op: e.op, scenario: normalise(e.diffs[0] ?? 'differs'), lines: [...new Set(e.diffs.map(normalise))], localStatus: e.localStatus, cloudStatus: e.cloudStatus, count: 1, last: e.ts, severity: severityOf(e) })
  }
  return [...map.values()].sort((a, b) => SEV_RANK[a.severity] - SEV_RANK[b.severity] || b.count - a.count)
}

/**
 * Compatibility lab. Warp's compare mode serves a request from both the real cloud service ("native") and Warp's emulation and records
 * the normalised difference. Everything below is computed from that stream (`/api/ab-routing`, `/stats`, `/compare`) and from the capture
 * buffer (`/api/capture`). Readiness thresholds and difference decisions are kept in this browser only (localStorage), and are labelled so.
 * Latency P95 is taken over the recorded compare entries of this node, not from a histogram.
 */
export default function CompatLab() {
  const view = useLocation().pathname.replace(/^\/lab\/?/, '') || 'comparison'
  const state = useLoad(getAbRouting, POLL_MS)
  const stats = useLoad(getAbStats, POLL_MS)
  const entries = useLoad(useCallback(() => getAbCompareAll(COMPARE_LIMIT), []), POLL_MS)
  const [store, setStore] = useState('all')
  const [th, setTh] = useState<Thresholds>(() => readLocal(TH_KEY, DEFAULT_THRESHOLDS))
  const [decisions, setDecisions] = useState<Record<string, Decision>>(() => readLocal<Record<string, Decision>>(DEC_KEY, {}))
  useEffect(() => writeLocal(TH_KEY, th), [th])
  useEffect(() => writeLocal(DEC_KEY, decisions), [decisions])

  const policies: AbPolicy[] = Object.values(state.data?.policies ?? {})
  const stores = useMemo(() => [...new Set([...policies.map((p) => p.store), ...Object.keys(stats.data?.compare ?? {}), ...(entries.data ?? []).map((e) => e.store)])].sort(), [policies, stats.data, entries.data])
  const inScope = (s: string) => store === 'all' || s === store
  const scoped: AbCompareEntry[] = useMemo(() => (entries.data ?? []).filter((e) => inScope(e.store)), [entries.data, store])
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const cmp = useMemo(() => sumCompare(stats.data, inScope), [stats.data, store])
  const sideStat = (side: 'local' | 'cloud') => {
    let requests = 0; let errors = 0
    for (const [k, v] of Object.entries(stats.data?.sides ?? {})) {
      const [s, sd] = k.split('|')
      if (sd === side && inScope(s)) { requests += v.requests; errors += v.errors }
    }
    return { requests, errors }
  }
  const local = sideStat('local'); const cloud = sideStat('cloud')
  const p95Local = p95(scoped.map((e) => e.localMs)); const p95Cloud = p95(scoped.map((e) => e.cloudMs))
  const groups = useMemo(() => groupDiffs(scoped), [scoped])
  const decisionOf = (k: string): Decision => decisions[k] ?? 'review'
  const unresolved = groups.filter((g) => decisionOf(g.key) !== 'accept')

  const matchPct = cmp.compared > 0 ? (cmp.equal / cmp.compared) * 100 : null
  const mismatchPct = cmp.compared > 0 ? (cmp.differ / cmp.compared) * 100 : null
  const failPct = cmp.compared > 0 ? (cmp.secondaryFailed / cmp.compared) * 100 : null
  const localErrPct = local.requests > 0 ? (local.errors / local.requests) * 100 : null
  const p95Increase = p95Local !== null && p95Cloud !== null && p95Cloud > 0 ? ((p95Local - p95Cloud) / p95Cloud) * 100 : null
  const times = scoped.map((e) => e.ts)
  const windowMs = times.length > 1 ? Math.max(...times) - Math.min(...times) : 0
  const compareOn = policies.some((p) => p.mode === 'compare')

  type Check = { title: string; sub: string; tone: Tone; text: string }
  const checks: Check[] = [
    { title: 'Compare mode active', sub: compareOn ? `${policies.filter((p) => p.mode === 'compare').map((p) => p.store).join(', ')} in compare mode` : 'no store is in compare mode', tone: compareOn ? 'ok' : 'warn', text: compareOn ? 'Pass' : 'Review' },
    { title: 'Sample size', sub: `${cmp.compared.toLocaleString()} compared, at least ${th.minCompared} required`, tone: cmp.compared >= th.minCompared ? 'ok' : 'warn', text: cmp.compared >= th.minCompared ? 'Pass' : 'Review' },
    { title: 'Response parity', sub: mismatchPct === null ? 'nothing compared yet' : `${mismatchPct.toFixed(2)}% differ, at most ${th.maxMismatchPct}% allowed${unresolved.length === 0 && groups.length > 0 ? ' (all differences accepted)' : ''}`,
      tone: mismatchPct === null ? 'muted' : mismatchPct <= th.maxMismatchPct || (groups.length > 0 && unresolved.length === 0) ? 'ok' : 'bad', text: mismatchPct === null ? 'No data' : mismatchPct <= th.maxMismatchPct || (groups.length > 0 && unresolved.length === 0) ? 'Pass' : 'Fail' },
    { title: 'Native side reachable', sub: failPct === null ? 'nothing compared yet' : `${cmp.secondaryFailed} secondary-side failure(s), ${failPct.toFixed(2)}% (at most ${th.maxErrorPct}%)`, tone: failPct === null ? 'muted' : failPct <= th.maxErrorPct ? 'ok' : 'bad', text: failPct === null ? 'No data' : failPct <= th.maxErrorPct ? 'Pass' : 'Fail' },
    { title: 'Warp error rate', sub: localErrPct === null ? 'no Warp-side requests yet' : `${localErrPct.toFixed(2)}% of ${local.requests.toLocaleString()} (at most ${th.maxErrorPct}%)`, tone: localErrPct === null ? 'muted' : localErrPct <= th.maxErrorPct ? 'ok' : 'bad', text: localErrPct === null ? 'No data' : localErrPct <= th.maxErrorPct ? 'Pass' : 'Fail' },
    { title: 'Latency', sub: p95Increase === null ? 'needs at least 5 compared operations' : `Warp P95 ${p95Local} ms vs native ${p95Cloud} ms (${p95Increase >= 0 ? '+' : ''}${p95Increase.toFixed(0)}%, at most +${th.maxP95IncreasePct}%)`, tone: p95Increase === null ? 'muted' : p95Increase <= th.maxP95IncreasePct ? 'ok' : 'warn', text: p95Increase === null ? 'No data' : p95Increase <= th.maxP95IncreasePct ? 'Pass' : 'Review' },
  ]
  const verdict: Check = checks.some((c) => c.tone === 'bad') ? { title: '', sub: '', tone: 'bad', text: 'Not ready' }
    : checks.every((c) => c.tone === 'ok') ? { title: '', sub: '', tone: 'ok', text: 'Ready' } : { title: '', sub: '', tone: 'warn', text: 'Needs review' }

  const loadErr = state.error ?? stats.error ?? entries.error
  const kpis: KpiItem[] = [
    { label: 'Latest comparison', wide: true, tone: cmp.compared === 0 ? 'muted' : groups.length === 0 ? 'ok' : 'warn', value: cmp.compared === 0 ? 'Nothing compared yet' : groups.length === 0 ? 'No differences' : `${groups.length} distinct difference${groups.length === 1 ? '' : 's'}` },
    { label: 'Operations compared', value: compact(cmp.compared), hint: 'this node, since start' },
    { label: 'Behavioural match', value: matchPct === null ? '—' : `${matchPct.toFixed(2)}%`, hint: cmp.compared > 0 ? `${cmp.differ} differ · ${cmp.secondaryFailed} native failures` : undefined },
    { label: 'Buffer window', value: windowMs > 0 ? fmtDuration(windowMs) : '—', hint: `${scoped.length} entries recorded` },
  ]

  return (
    <div>
      <PageHeader title="Compatibility lab" description="Compare native cloud services with Warp emulation before migrating production traffic. Compare mode runs on live traffic; there are no scheduled test runs."
        actions={<><Link to="/ab-routing"><Button variant="primary" icon={<ArrowLeftRight size={14} aria-hidden="true" />}>Set up compare mode</Button></Link>
          <Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { state.reload(); stats.reload(); entries.reload() }}>Refresh</Button></>} />
      {loadErr && <Notice tone="bad">Could not load: {loadErr}</Notice>}
      {state.data?.killSwitch && <Notice tone="warn">A/B kill switch is active: all traffic goes to the <b>{state.data.killSwitch.side}</b> side, so no new comparisons are recorded.</Notice>}
      {state.loading && stats.loading ? <Loading /> : (view === 'comparison' || view === 'known') && <KpiStrip items={kpis} label="Comparison figures" />}

      {(view === 'comparison' || view === 'known') && stores.length > 1 && (
        <div style={{ maxWidth: 260 }}>
          <Field label="Store">{(id) => (
            <select id={id} value={store} onChange={(e) => setStore(e.target.value)}>
              <option value="all">All stores</option>
              {stores.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
          )}</Field>
        </div>
      )}

      {view === 'comparison' && (
        <>
          <Section title={store === 'all' ? 'Native service vs Warp emulation' : `${store}: native service vs Warp emulation`} meta="Cloud side = the real service, local side = Warp on Postgres">
            <div className={styles.sides} style={{ padding: 0 }}>
              <div className={styles.side}>
                <h3>Native cloud service</h3>
                <dl>
                  <div><dt>Target</dt><dd>{targetsOf(state.data, policies, store) || '—'}</dd></div>
                  <div><dt>Operations compared</dt><dd>{cmp.compared.toLocaleString()}</dd></div>
                  <div><dt>P95 (compare sample)</dt><dd>{p95Cloud !== null ? `${p95Cloud} ms` : '—'}</dd></div>
                  <div><dt>Errors (all requests)</dt><dd>{cloud.errors.toLocaleString()} of {cloud.requests.toLocaleString()}</dd></div>
                </dl>
              </div>
              <div className={styles.arrow} aria-hidden="true"><ArrowLeftRight size={18} /></div>
              <div className={styles.side}>
                <h3>Warp on Postgres</h3>
                <dl>
                  <div><dt>Target</dt><dd>local emulation</dd></div>
                  <div><dt>Operations compared</dt><dd>{cmp.compared.toLocaleString()}</dd></div>
                  <div><dt>P95 (compare sample)</dt><dd>{p95Local !== null ? `${p95Local} ms` : '—'}</dd></div>
                  <div><dt>Errors (all requests)</dt><dd>{local.errors.toLocaleString()} of {local.requests.toLocaleString()}</dd></div>
                  <div><dt>Differences</dt><dd style={{ color: cmp.differ > 0 ? 'var(--sy-amber)' : undefined }}>{cmp.differ.toLocaleString()}</dd></div>
                </dl>
              </div>
            </div>
          </Section>

          <Section title="Promotion readiness" meta={<StatusPill tone={verdict.tone}>{verdict.text}</StatusPill>}>
            <div className={styles.rowList} style={{ margin: '-16px -16px 0' }}>
              {checks.map((c) => (
                <div className={styles.rowItem} key={c.title}>
                  <div><strong>{c.title}</strong><span className={styles.s}>{c.sub}</span></div>
                  <div className={styles.rowEnd}><StatusPill tone={c.tone}>{c.text}</StatusPill></div>
                </div>
              ))}
            </div>
            <h3 style={{ margin: '16px 0 8px', fontSize: 'var(--fs-sm)', fontWeight: 600 }}>Thresholds <span className={styles.sub}>(edit: stored in this browser only)</span></h3>
            <div className={styles.thresholds} style={{ padding: 0 }}>
              <Num label="Minimum operations compared" value={th.minCompared} onChange={(v) => setTh({ ...th, minCompared: v })} />
              <Num label="Max mismatch %" value={th.maxMismatchPct} step={0.1} onChange={(v) => setTh({ ...th, maxMismatchPct: v })} />
              <Num label="Max error %" value={th.maxErrorPct} step={0.1} onChange={(v) => setTh({ ...th, maxErrorPct: v })} />
              <Num label="Max P95 increase %" value={th.maxP95IncreasePct} onChange={(v) => setTh({ ...th, maxP95IncreasePct: v })} />
            </div>
            <Button variant="ghost" onClick={() => setTh(DEFAULT_THRESHOLDS)}>Reset thresholds</Button>
          </Section>

          <DifferencesTable groups={groups} decisionOf={decisionOf} setDecision={(k, d) => setDecisions((m) => ({ ...m, [k]: d }))} loading={entries.loading} compared={cmp.compared} />
        </>
      )}

      {view === 'profiles' && <Profiles policies={policies} state={state.data} loading={state.loading} />}
      {view === 'traffic' && <Traffic />}
      {view === 'known' && (
        <Section flush title="Known differences" meta="Differences you marked Accept (kept in this browser only)">
          {groups.filter((g) => decisionOf(g.key) === 'accept').length === 0
            ? <EmptyState title="No accepted differences">Mark a difference as Accept on the Comparison tab to record that you reviewed it. Documented compatibility notes live in the repository docs (docs/ORACLE_COMPATIBILITY.md, docs/WARP_GUIDE.md); Warp does not serve them.</EmptyState>
            : (
              <DataTable caption="Accepted differences" minWidth={640}>
                <thead><tr><th>Operation</th><th>Scenario</th><th>Severity</th><th style={{ textAlign: 'right' }}>Seen</th></tr></thead>
                <tbody>
                  {groups.filter((g) => decisionOf(g.key) === 'accept').map((g) => (
                    <tr key={g.key}><td><NameCell name={g.op} sub={g.store} /></td><td className={styles.wrapCell}>{g.scenario}</td><td><StatusPill tone={SEV_TONE[g.severity]}>{g.severity}</StatusPill></td><td className={styles.num}>{g.count}</td></tr>
                  ))}
                </tbody>
              </DataTable>
            )}
        </Section>
      )}
    </div>
  )
}

function Num({ label, value, onChange, step = 1 }: { label: string; value: number; onChange: (v: number) => void; step?: number }) {
  return <Field label={label}>{(id) => <input id={id} type="number" min={0} step={step} value={value} onChange={(e) => { const n = Number(e.target.value); if (Number.isFinite(n) && n >= 0) onChange(n) }} />}</Field>
}

function sumCompare(stats: AbStats | null, inScope: (s: string) => boolean) {
  const out = { compared: 0, equal: 0, differ: 0, secondaryFailed: 0 }
  for (const [s, c] of Object.entries(stats?.compare ?? {})) if (inScope(s)) { out.compared += c.compared; out.equal += c.equal; out.differ += c.differ; out.secondaryFailed += c.secondaryFailed }
  return out
}

function targetsOf(state: AbState | null, policies: AbPolicy[], store: string): string {
  const names = new Set(policies.filter((p) => (store === 'all' || p.store === store) && p.target).map((p) => p.target as string))
  return [...names].map((n) => { const t = state?.targets.find((x) => x.name === n); return t ? `${t.name} (${t.region})` : n }).join(', ')
}

function fmtDuration(ms: number): string {
  const s = Math.round(ms / 1000)
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`
}

function DifferencesTable({ groups, decisionOf, setDecision, loading, compared }: {
  groups: DiffGroup[]; decisionOf: (k: string) => Decision; setDecision: (k: string, d: Decision) => void; loading: boolean; compared: number
}) {
  const open = groups.filter((g) => decisionOf(g.key) !== 'accept').length
  return (
    <Section flush title="Behavioural differences" meta={`${open} unresolved of ${groups.length} · severity is derived from the recorded difference, decisions are local to this browser`}>
      {groups.length === 0
        ? loading ? <div className={styles.pad}><Loading /></div>
          : <EmptyState title={compared === 0 ? 'Nothing compared yet' : 'No differences recorded'}>{compared === 0 ? 'Put a store in compare mode on A/B routing: reads go to both sides and the differences are listed here.' : 'Every compared operation matched.'}</EmptyState>
        : (
          <DataTable caption="Behavioural differences" minWidth={960}>
            <thead><tr><th>Operation</th><th>Scenario</th><th>Native (cloud)</th><th>Warp (local)</th><th style={{ textAlign: 'right' }}>Seen</th><th>Severity</th><th>Decision (local)</th></tr></thead>
            <tbody>
              {groups.slice(0, MAX_GROUPS).map((g) => (
                <tr key={g.key}>
                  <td><NameCell name={g.op} sub={g.store} /></td>
                  <td className={styles.wrapCell}>{g.scenario}{g.lines.length > 1 && <ul className={styles.diffList}>{g.lines.slice(1, 4).map((l, i) => <li key={i}>{l}</li>)}</ul>}</td>
                  <td className={styles.mono}>HTTP {g.cloudStatus}</td>
                  <td className={styles.mono}>HTTP {g.localStatus}</td>
                  <td className={styles.num}>{g.count}<div className={styles.sub}>{new Date(g.last).toLocaleTimeString()}</div></td>
                  <td><StatusPill tone={SEV_TONE[g.severity]}>{g.severity}</StatusPill></td>
                  <td>
                    <select aria-label={`Decision for ${g.op}`} value={decisionOf(g.key)} onChange={(e) => setDecision(g.key, e.target.value as Decision)}>
                      <option value="review">Review</option><option value="accept">Accept</option><option value="fix">Fix</option>
                    </select>
                  </td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      {groups.length > MAX_GROUPS && <div className={styles.pad}><span className={styles.sub}>Showing the {MAX_GROUPS} highest-severity groups of {groups.length}.</span></div>}
    </Section>
  )
}

function Profiles({ policies, state, loading }: { policies: AbPolicy[]; state: AbState | null; loading: boolean }) {
  return (
    <Section flush title="Comparison profiles" meta="One A/B policy per store; edit them on A/B routing">
      {loading ? <div className={styles.pad}><Loading /></div> : policies.length === 0
        ? <EmptyState title="No A/B policies">Every store runs locally. Create a policy on <Link to="/ab-routing">A/B routing</Link> and choose compare mode.</EmptyState>
        : (
          <DataTable caption="A/B policies" minWidth={860}>
            <thead><tr><th>Store</th><th>Mode</th><th>Native target</th><th>Compare primary</th><th>Write owner</th><th>Dual-write</th><th style={{ textAlign: 'right' }}>Buffer</th></tr></thead>
            <tbody>
              {policies.map((p) => {
                const t = state?.targets.find((x) => x.name === p.target)
                return (
                  <tr key={p.store}>
                    <td className={styles.mono}>{p.store}</td>
                    <td><Tag>{p.mode}{p.mode === 'split' ? ` ${p.cloudPercent}%` : ''}</Tag></td>
                    <td>{p.target ? (t ? `${t.name} (${t.region})` : p.target) : '—'}</td>
                    <td>{p.compare.primary}</td>
                    <td>{p.writeOwner}</td>
                    <td>{p.dualWrite ? <StatusPill tone="warn">On</StatusPill> : 'Off'}</td>
                    <td className={styles.num}>{p.compare.bufferSize}</td>
                  </tr>
                )
              })}
            </tbody>
          </DataTable>
        )}
    </Section>
  )
}

function Traffic() {
  const cap = useLoad(useCallback(() => getCapture(1000), []), 15_000)
  const off = cap.error && /no such route/i.test(cap.error)
  const rows = cap.data ?? []
  const byBackend = new Map<string, number>()
  for (const r of rows) byBackend.set(r.targetBackend ?? 'default', (byBackend.get(r.targetBackend ?? 'default') ?? 0) + 1)
  return (
    <>
      <Section flush title="Captured traffic" meta={cap.data ? `${rows.length} statements from this node, oldest first (first 1000)` : undefined}>
        {cap.loading ? <div className={styles.pad}><Loading /></div>
          : off ? <EmptyState title="Workload capture is off">Start Warp with WARP_CAPTURE_ENABLED=true to record every SQL statement in an in-memory buffer (size WARP_CAPTURE_BUFFER_SIZE, default 20000).</EmptyState>
          : cap.error ? <div className={styles.pad}><Notice tone="bad">{cap.error}</Notice></div>
          : rows.length === 0 ? <EmptyState title="Nothing captured yet">Capture is on; statements appear as clients run them.</EmptyState>
          : (
            <DataTable caption="Captured statements" minWidth={860}>
              <thead><tr><th>Seq</th><th>Captured</th><th>Protocol</th><th>Tenant</th><th>Backend</th><th>SQL</th></tr></thead>
              <tbody>
                {rows.slice(0, 200).map((r) => (
                  <tr key={`${r.nodeId}-${r.localSeq}`}>
                    <td className={styles.num}>{r.localSeq}</td>
                    <td style={{ whiteSpace: 'nowrap' }}>{new Date(r.wallClock).toLocaleTimeString()}</td>
                    <td>{r.protocol}</td>
                    <td className={styles.mono}>{r.tenantId}</td>
                    <td className={styles.mono}>{r.targetBackend ?? 'default'}</td>
                    <td className={styles.sqlCell}>{r.sqlText}{r.bindParams.length > 0 && <span className={styles.sub}> ({r.bindParams.length} bind value{r.bindParams.length === 1 ? '' : 's'}, not shown)</span>}</td>
                  </tr>
                ))}
              </tbody>
            </DataTable>
          )}
        {rows.length > 200 && <div className={styles.pad}><span className={styles.sub}>Showing the first 200 of {rows.length} fetched.</span></div>}
      </Section>
      <Section title="Replay" meta="Runs outside the console">
        <p className={styles.help}>Replay merges every live node's capture buffer by wall clock and plays it against a Postgres target, using the same WARP_PG_* and WARP_ADMIN_TOKEN settings the server uses. Run it before a buffer wraps.</p>
        <CodeBlock label="command">{'java -cp sayonora-warp.jar com.sayonora.warp.capture.WorkloadReplayer'}</CodeBlock>
      </Section>
    </>
  )
}
