import { useMemo, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { Link } from 'react-router-dom'
import { getWireConfig, getWireMetrics, saveWireConfig } from '../api/client'
import { getCacheStats, getMetricsHistory, invalidateCache, ratePoints, type CacheStatsResponse, type CacheTier } from '../api/insights'
import { TimeSeries } from '../components/charts'
import {
  Button, DataTable, EmptyState, Field, KpiStrip, Loading, Notice, PageHeader, Section, StatusPill, compact, type KpiItem,
} from '../components/ui'
import { errorText, useLoad } from '../hooks'
import styles from './Caching.module.css'

const POLL_MS = 10_000
const pct = (h: number, m: number) => (h + m === 0 ? null : (h / (h + m)) * 100)
const fmtPct = (v: number | null) => (v === null ? '—' : `${v.toFixed(v >= 99.95 || v === 0 ? 0 : 1)}%`)

function ttlText(ms: number | undefined) {
  if (ms === undefined) return '—'
  if (ms >= 60_000 && ms % 60_000 === 0) return `${ms / 60_000} min`
  if (ms >= 1000 && ms % 1000 === 0) return `${ms / 1000} s`
  return `${ms} ms`
}

/**
 * Caching. Counters are the ones Warp keeps in memory on this node (hits, misses, invalidations, entry counts from the
 * shared Ignite caches); they reset when the node restarts. Cache size in bytes, TTL expirations and per-tier eviction
 * counts are not tracked by Warp, so they are not shown; entries leave the query and row caches by TTL or invalidation only.
 */
export default function Caching() {
  const stats = useLoad(getCacheStats, POLL_MS)
  const history = useLoad(getMetricsHistory, POLL_MS)
  const metrics = useLoad(getWireMetrics, POLL_MS)
  const config = useLoad(getWireConfig)

  const s = stats.data
  const tiers = s?.tiers
  const sum = (f: (t: CacheTier) => number) => [tiers?.result, tiers?.pk, tiers?.row].reduce((a, t) => a + (t ? f(t) : 0), 0)
  const hits = sum((t) => t.hits)
  const misses = sum((t) => t.misses)
  const entries = sum((t) => t.entries)
  const rate = pct(hits, misses)
  const samples = useMemo(() => history.data?.samples ?? [], [history.data])
  const hitRate = useMemo(() => ratePoints(samples, 'cacheHits'), [samples])
  const missRate = useMemo(() => ratePoints(samples, 'cacheMisses'), [samples])
  const recentHitsPerSec = hitRate.length ? hitRate.slice(-6).reduce((a, p) => a + p.v, 0) / Math.min(6, hitRate.length) : null

  const kpis: KpiItem[] = s?.enabled ? [
    { label: 'Hit rate', value: fmtPct(rate), hint: hits + misses === 0 ? 'no cacheable lookups yet' : `${hits.toLocaleString()} hits · ${misses.toLocaleString()} misses` },
    { label: 'Hits / sec', value: recentHitsPerSec === null ? '—' : recentHitsPerSec.toFixed(recentHitsPerSec < 10 ? 2 : 1), hint: recentHitsPerSec === null ? 'needs two server samples' : 'mean of the last minute' },
    { label: 'Cached entries', value: compact(entries), hint: `query, key and row caches · TTL ${ttlText(s.ttlMillis)}` },
    { label: 'Entries invalidated', value: compact(s.invalidations?.entriesRemoved ?? 0), hint: `${s.invalidations?.events ?? 0} table events · ${s.invalidations?.fullClears ?? 0} full clears` },
  ] : []

  const perTable = s?.byTable ?? []
  const outcomes = useMemo(() => {
    const by = new Map<string, { hit?: { calls: number; totalMs: number }; read?: { calls: number; totalMs: number } }>()
    for (const r of metrics.data?.rttByOutcome ?? []) {
      if (r.outcome !== 'cache_hit' && r.outcome !== 'pg_read') continue
      const e = by.get(r.protocol) ?? {}
      if (r.outcome === 'cache_hit') e.hit = r; else e.read = r
      by.set(r.protocol, e)
    }
    return [...by.entries()].filter(([, v]) => v.hit).sort((a, b) => (b[1].hit?.calls ?? 0) - (a[1].hit?.calls ?? 0))
  }, [metrics.data])

  return (
    <div>
      <PageHeader title="Caching" description="Query result, primary-key row and translation caches shared across SQL, API and MCP traffic, and the controls that keep them consistent."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { stats.reload(); history.reload(); metrics.reload(); config.reload() }}>Refresh</Button>} />
      {stats.error && <Notice tone="bad">Could not load cache statistics: {stats.error}</Notice>}
      {!s && !stats.error ? <Loading>Loading cache statistics…</Loading> : null}
      {s && !s.enabled && (
        <Notice tone="warn">
          The query result cache is not running on this node: no cache tables were configured when it started. Name tables in the cache policy below
          and restart Warp to start it. Point-lookup row caches and the translation cache are independent of this setting.
        </Notice>
      )}
      {s?.enabled && <KpiStrip items={kpis} label="Cache figures" />}

      {s?.enabled && (
        <div className={styles.half}>
          <Section title="Hits and misses" meta="lookups / sec, all cache tiers">
            {history.error ? <Notice tone="warn">History unavailable: {history.error}</Notice>
              : <TimeSeries label="Cache hits and misses per second" area series={[
                { label: 'Hits', color: 'var(--sy-series-2)', points: hitRate },
                { label: 'Misses', color: 'var(--sy-series-1)', points: missRate },
              ]} format={(v) => v.toFixed(v < 10 ? 2 : 0)} />}
          </Section>
          <Section flush title="By table" meta="query and key caches">
            {perTable.length === 0 ? <EmptyState title="No cacheable lookups yet">Per-table counters appear once a query against a cached table runs.</EmptyState> : (
              <DataTable caption="Cache hits by table" minWidth={420}>
                <thead><tr><th>Table</th><th>Hit rate</th><th>Hits</th><th>Misses</th></tr></thead>
                <tbody>
                  {perTable.slice(0, 12).map((t) => {
                    const r = pct(t.hits, t.misses)
                    return (
                      <tr key={t.table}>
                        <td className={styles.mono}>{t.table}</td>
                        <td><div className={styles.rate}><div className={styles.track}><span style={{ width: `${r ?? 0}%` }} /></div><span className={styles.num}>{fmtPct(r)}</span></div></td>
                        <td className={styles.num}>{t.hits.toLocaleString()}</td><td className={styles.num}>{t.misses.toLocaleString()}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </DataTable>
            )}
          </Section>
        </div>
      )}

      {s && (
        <Section flush title="Cache tiers" meta={`${s.clusterNodes} node${s.clusterNodes === 1 ? '' : 's'} in the cluster`}>
          <DataTable caption="Cache tiers" minWidth={860}>
            <thead><tr><th>Tier</th><th>Holds</th><th>Entries</th><th>Hits</th><th>Misses</th><th>Hit rate</th><th>Bound</th></tr></thead>
            <tbody>
              {tiers?.result && <TierRow name="Query result cache" holds="Whole SELECT results for the tables named in the cache policy" t={tiers.result} bound={`TTL ${ttlText(s.ttlMillis)}, dropped on write`} />}
              {tiers?.pk && <TierRow name="Primary-key row cache" holds="Single-row lookups by primary key on any backend" t={tiers.pk} bound={`TTL ${ttlText(s.ttlMillis)}, dropped on write`} />}
              {tiers?.row && <TierRow name="Shared row cache" holds="DynamoDB GetItem and MongoDB _id lookups" t={tiers.row} bound="TTL, dropped on write" note={tiers.row.attached ? undefined : 'not attached to SQL'} />}
              {tiers?.translation && (
                <tr>
                  <td><strong>Translation cache</strong></td><td>SQL rewritten between dialects</td>
                  <td className={styles.num}>{tiers.translation.entries.toLocaleString()}</td>
                  <td className={styles.num}>{tiers.translation.hits.toLocaleString()}</td><td className={styles.num}>{tiers.translation.misses.toLocaleString()}</td>
                  <td className={styles.num}>{fmtPct(pct(tiers.translation.hits, tiers.translation.misses))}</td>
                  <td>{tiers.translation.maxEntries.toLocaleString()} entries max · {tiers.translation.evictions.toLocaleString()} evicted</td>
                </tr>
              )}
              {!tiers?.result && !tiers?.row && !tiers?.translation && <tr><td colSpan={7}><span className={styles.sub}>No cache tier is active on this node.</span></td></tr>}
            </tbody>
          </DataTable>
        </Section>
      )}

      <div className={styles.half}>
        <Section flush title="Invalidation activity" meta={s?.enabled ? 'most recent first' : undefined}>
          {!s ? <div className={styles.pad}><Loading /></div> : !s.enabled ? <EmptyState title="Result cache not running">Invalidations are recorded while it runs.</EmptyState>
            : (s.invalidations?.recent.length ?? 0) === 0 ? <EmptyState title="No invalidations yet">A write to a cached table, an external change signalled through NOTIFY, or an operator clear is listed here once it removes entries.</EmptyState> : (
              <ul className={styles.feed}>
                {s.invalidations!.recent.slice(0, 12).map((e, i) => (
                  <li key={i}>
                    <i className={`${styles.dot} ${e.kind === 'clear' ? styles.dotWarn : ''}`} aria-hidden="true" />
                    <span><strong>{e.kind === 'clear' ? 'Full clear' : 'Table invalidated'}</strong> <span className={styles.mono}>{e.target}</span><div className={styles.sub}>{e.entries.toLocaleString()} entr{e.entries === 1 ? 'y' : 'ies'} removed · {e.source}</div></span>
                    <time dateTime={e.at}>{new Date(e.at).toLocaleTimeString()}</time>
                  </li>
                ))}
              </ul>
            )}
        </Section>
        <Section flush title="Cache hits by interface" meta="round-trip time">
          {!metrics.data ? <div className={styles.pad}><Loading /></div> : outcomes.length === 0 ? <EmptyState title="No cache hits yet">Interfaces appear once a request is served from cache.</EmptyState> : (
            <DataTable caption="Cache hits by protocol" minWidth={420}>
              <thead><tr><th>Protocol</th><th>Hits</th><th>Avg on hit</th><th>Avg on backend read</th></tr></thead>
              <tbody>{outcomes.map(([p, v]) => (
                <tr key={p}><td className={styles.mono}>{p}</td><td className={styles.num}>{compact(v.hit!.calls)}</td>
                  <td className={styles.num}>{(v.hit!.totalMs / v.hit!.calls).toFixed(2)} ms</td>
                  <td className={styles.num}>{v.read && v.read.calls ? `${(v.read.totalMs / v.read.calls).toFixed(2)} ms` : '—'}</td></tr>
              ))}</tbody>
            </DataTable>
          )}
        </Section>
      </div>

      <PolicyAndControls stats={s ?? null} cfg={config.data ? { tables: config.data.cacheTables ?? '', ttl: config.data.cacheTtlMs ?? '' } : null}
        cfgError={config.error} onChanged={() => { stats.reload(); config.reload() }} />

      <p className={styles.note}>Counters are node-local and reset on restart. Cached data size, TTL expirations and per-tier eviction counts are not measured by Warp.
        Pre-aggregated tables are managed on the <Link to="/rollups">Rollups</Link> tab.</p>
    </div>
  )
}

function TierRow({ name, holds, t, bound, note }: { name: string; holds: string; t: CacheTier; bound: string; note?: string }) {
  const r = pct(t.hits, t.misses)
  return (
    <tr>
      <td><strong>{name}</strong>{note && <div className={styles.sub}>{note}</div>}</td><td>{holds}</td>
      <td className={styles.num}>{t.entries.toLocaleString()}</td><td className={styles.num}>{t.hits.toLocaleString()}</td><td className={styles.num}>{t.misses.toLocaleString()}</td>
      <td className={styles.num}>{fmtPct(r)}</td><td>{bound}</td>
    </tr>
  )
}

function PolicyAndControls({ stats, cfg, cfgError, onChanged }: {
  stats: CacheStatsResponse | null
  cfg: { tables: string; ttl: string } | null; cfgError: string | null; onChanged: () => void
}) {
  const [tables, setTables] = useState<string | null>(null)
  const [ttl, setTtl] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null)
  const [msg, setMsg] = useState<{ tone: 'ok' | 'bad'; text: string } | null>(null)
  const [table, setTable] = useState('')
  const [confirmAll, setConfirmAll] = useState(false)
  const tablesValue = tables ?? cfg?.tables ?? ''
  const ttlValue = ttl ?? cfg?.ttl ?? ''
  const dirty = cfg !== null && (tablesValue !== cfg.tables || ttlValue !== cfg.ttl)
  const ttlNum = ttlValue.trim() === '' ? null : Number(ttlValue)
  const ttlBad = ttlNum !== null && (!Number.isInteger(ttlNum) || ttlNum < 0)
  const names = tablesValue.split(',').map((x) => x.trim()).filter(Boolean)

  async function save(e: React.FormEvent) {
    e.preventDefault()
    if (ttlBad) return
    setBusy('save'); setMsg(null)
    try {
      const saved = await saveWireConfig({ cacheTables: tablesValue.trim() === '' ? null : names.join(','), cacheTtlMs: ttlValue.trim() === '' ? null : ttlValue.trim() })
      setTables(null); setTtl(null)
      setMsg({ tone: 'ok', text: stats?.enabled ? `Saved as config version ${saved.version}. The running cache picks up table and TTL changes; a new TTL starts from an empty cache.` : `Saved as config version ${saved.version}. The cache is not running on this node: restart Warp to start it.` })
      onChanged()
    } catch (err) { setMsg({ tone: 'bad', text: errorText(err) }) } finally { setBusy(null) }
  }
  async function clear(t?: string) {
    setBusy(t ? 'table' : 'all'); setMsg(null)
    try {
      await invalidateCache(t)
      setMsg({ tone: 'ok', text: t ? `Invalidated cached results for ${t}.` : 'Cleared the query result, key and row caches.' })
      setConfirmAll(false); if (t) setTable('')
      onChanged()
    } catch (err) { setMsg({ tone: 'bad', text: errorText(err) }) } finally { setBusy(null) }
  }

  return (
    <Section title="Cache policy and consistency" meta={stats?.enabled ? 'applies to all interfaces on this cluster' : undefined}>
      {cfgError && <Notice tone="bad">Could not load cache configuration: {cfgError}</Notice>}
      {msg && <Notice tone={msg.tone}>{msg.text}</Notice>}
      <form onSubmit={save}>
        <div className={styles.form}>
          <Field label="Cached tables" hint="Comma-separated table names. Reads of these tables are cached; a write to one drops its cached results.">
            {(id) => <input id={id} className={styles.input} value={tablesValue} onChange={(e) => setTables(e.target.value)} placeholder="orders, customers" disabled={!cfg} />}
          </Field>
          <Field label="Time to live (ms)" hint="How long a cached entry lives. Blank uses 30000.">
            {(id) => <input id={id} className={styles.input} inputMode="numeric" value={ttlValue} onChange={(e) => setTtl(e.target.value)} placeholder="30000" aria-invalid={ttlBad} disabled={!cfg} />}
          </Field>
        </div>
        {ttlBad && <Notice tone="bad">Time to live must be a whole number of milliseconds.</Notice>}
        <div className={styles.actions}>
          <Button type="submit" variant="primary" disabled={!dirty || ttlBad || busy !== null}>{busy === 'save' ? 'Saving…' : 'Save policy'}</Button>
          {dirty && <Button onClick={() => { setTables(null); setTtl(null) }}>Discard</Button>}
          {names.length > 0 && <span className={styles.chips}>{names.map((n) => <span key={n} className={styles.chip}>{n}</span>)}</span>}
        </div>
      </form>
      <hr style={{ border: 0, borderTop: '1px solid var(--sy-line)', margin: '16px 0' }} />
      <strong style={{ fontSize: 'var(--fs-sm)' }}>Invalidate</strong>
      <p className={styles.note} style={{ marginTop: 4 }}>
        Writes through Warp drop the affected table&apos;s cached results automatically. Writes made directly to Postgres do so through NOTIFY triggers where they are installed;
        otherwise entries live until the TTL passes. Use these controls after a bulk load or a change made outside Warp.
      </p>
      <div className={styles.inline}>
        <input className={styles.input} value={table} onChange={(e) => setTable(e.target.value)} placeholder="table or schema.table" aria-label="Table to invalidate" disabled={!stats?.enabled} />
        <Button disabled={!stats?.enabled || table.trim() === '' || busy !== null} onClick={() => clear(table.trim())}>Invalidate table</Button>
        {!confirmAll
          ? <Button disabled={!stats?.enabled || busy !== null} onClick={() => setConfirmAll(true)}>Invalidate everything…</Button>
          : <><StatusPill tone="warn">Clears every cached entry cluster-wide</StatusPill><Button variant="danger" disabled={busy !== null} onClick={() => clear()}>{busy === 'all' ? 'Clearing…' : 'Confirm clear'}</Button><Button onClick={() => setConfirmAll(false)}>Cancel</Button></>}
      </div>
    </Section>
  )
}
