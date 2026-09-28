import { useCallback, useMemo, useRef } from 'react'
import { Link } from 'react-router-dom'
import { RefreshCw } from 'lucide-react'
import { getUsage, getWireConfig, listBackendSets, listInterfaces, type SetBackend, type WireConfig } from '../../api/client'
import { getQosCounters, type QosCounter } from '../../api/wave2b'
import {
  Button, DataTable, EmptyState, KpiStrip, Loading, Meter, NameCell, Notice, PageHeader, Section, Split, StatusPill, Tag, compact,
  type KpiItem, type Tone,
} from '../../components/ui'
import { useLoad } from '../../hooks'
import styles from './wave2b.module.css'

const POLL_MS = 10_000

type Health = { tone: Tone; text: string }
interface RouteRow { key: string; origin: string; match: string; strategy: string; destination: string[]; fallback: string; health: Health; note?: string; matchSub?: string }

const filled = (s: string | null | undefined) => !!s && s.trim() !== ''

/** Backend names a value/table shard params field refers to (hash/consistent: flat list; list/range/date: one backend per entry). */
function backendsOfParams(strategy: string, params: string): string[] {
  const names = /^(hash|consistent)$/i.test(strategy)
    ? params.split(',')
    : params.split(';').map((p) => (/^[^=<]+/.exec(p.trim()) ?? [''])[0])
  return [...new Set(names.map((n) => n.trim()).filter(Boolean))]
}

/**
 * Routing and QoS landing page. Every row is read from live config (`/api/config` router* and qos*, `/api/connection-routes`
 * through the backend-set listing, `WARP_*_SET` from `/api/interfaces`) and every counter from Warp's own metrics
 * (`warp_qos_*_total` on /metrics, backend probes and pool gauges from `/api/backend-sets?health=true`). What Warp does not record
 * (queue depth, failover events, per-route QoS class) is not shown; the editors stay on the other tabs.
 */
export default function RoutingQos() {
  const config = useLoad(getWireConfig, 30_000)
  const sets = useLoad(useCallback(() => listBackendSets(true), []), 30_000)
  const ifaces = useLoad(listInterfaces, 30_000)
  const qos = useLoad(getQosCounters, POLL_MS)
  const usage = useLoad(getUsage, POLL_MS)

  // Rejected-per-minute needs two samples of the same monotonic counter; keep the previous one.
  const prev = useRef<{ at: number; rejected: number } | null>(null)
  const rate = useRef<number | null>(null)
  const counters: QosCounter[] = qos.data ?? []
  const admitted = counters.reduce((s, c) => s + c.admitted, 0)
  const rejected = counters.reduce((s, c) => s + c.rejected, 0)
  if (qos.updated) {
    const at = qos.updated.getTime()
    if (prev.current && at > prev.current.at) rate.current = ((rejected - prev.current.rejected) / (at - prev.current.at)) * 60_000
    if (!prev.current || at !== prev.current.at) prev.current = { at, rejected }
  }

  const allSets = sets.data?.sets ?? []
  const backends = useMemo(() => allSets.flatMap((s) => s.backends), [allSets])
  const byName = useMemo(() => new Map(backends.map((b) => [b.name, b])), [backends])
  const setByName = useMemo(() => new Map(allSets.map((s) => [s.name, s])), [allSets])

  function healthOf(names: string[]): Health {
    const targets: SetBackend[] = []
    let unknown = 0
    for (const n of names) {
      const b = byName.get(n)
      const set = setByName.get(n)
      if (b) targets.push(b)
      else if (set) targets.push(...set.backends)
      else unknown++
    }
    const probed = targets.filter((b) => b.health)
    const down = probed.filter((b) => b.health && !b.health.ok)
    if (names.length > 0 && unknown === names.length) return { tone: 'bad', text: 'Unknown destination' }
    if (down.length > 0) return { tone: 'bad', text: down.length === probed.length ? 'Unreachable' : `${down.length} of ${probed.length} down` }
    if (probed.length === 0) return { tone: 'muted', text: 'Not probed' }
    return { tone: unknown > 0 ? 'warn' : 'ok', text: unknown > 0 ? 'Partly unknown' : 'Healthy' }
  }
  const fallbackOf = (names: string[]) => {
    const fb = [...new Set(names.flatMap((n) => { const b = byName.get(n); return b?.fallback ? [b.fallback] : [] }))]
    return fb.length > 0 ? fb.join(', ') : '—'
  }

  const cfg: WireConfig | null = config.data
  const rows: RouteRow[] = useMemo(() => {
    const out: RouteRow[] = []
    const mk = (r: Omit<RouteRow, 'health' | 'fallback'> & { fallback?: string }) =>
      out.push({ ...r, fallback: r.fallback ?? fallbackOf(r.destination), health: healthOf(r.destination) })
    // 1. connect-time routing (before any statement runs)
    for (const r of sets.data?.connectionRouting.routes ?? []) {
      mk({ key: `conn:${r.id}`, origin: 'Connection route', match: `database = ${r.database}${r.protocol ? ` · ${r.protocol}` : ''}${r.user ? ` · user ${r.user}` : ''}`,
        strategy: r.targetKind === 'set' ? 'Route to set' : 'Route to backend', destination: [r.target], fallback: r.defaultBackend ? `default: ${r.defaultBackend}` : undefined })
    }
    if (cfg) {
      // 2. router stage, in the order RouterStage evaluates them
      for (const e of (cfg.routerSchemaRules ?? '').split(',')) {
        const [schema, backend] = e.split(':', 2).map((x) => x?.trim())
        if (schema && backend) mk({ key: `schema:${schema}`, origin: 'Router · schema rule', match: `${schema}.<table> qualifier in SQL`, strategy: 'Route', destination: [backend] })
      }
      for (const e of (cfg.routerPredicateRules ?? '').split(',')) {
        const p = e.split(':', 3).map((x) => x.trim())
        if (p.length === 3) mk({ key: `pred:${e}`, origin: 'Router · predicate rule', match: `bind parameter ${p[0]} = ${p[1]}`, strategy: 'Route', destination: [p[2]] })
      }
      for (const e of (cfg.routerValueShardRules ?? '').split('|')) {
        const p = e.split(':', 3).map((x) => x.trim())
        if (p.length === 3) mk({ key: `vs:${e}`, origin: 'Router · value shard', match: /^\d+$/.test(p[0]) ? `bind parameter ${p[0]}` : `column ${p[0]} (literal or bind)`, strategy: `Shard · ${p[1]}`, destination: backendsOfParams(p[1], p[2]) })
      }
      for (const e of (cfg.routerTableShards ?? '').split('|')) {
        const p = e.split(':', 4).map((x) => x.trim())
        if (p.length === 4 && p[0]) mk({ key: `ts:${e}`, origin: 'Router · table shard', match: `table ${p[0]}, column ${p[2] || '—'}`, strategy: `Shard · ${p[1]}`, destination: backendsOfParams(p[1], p[3]) })
      }
      const shardSchemas = (cfg.routerShardTables ?? '').split(',').map((s) => s.trim()).filter(Boolean)
      const shardBackends = (cfg.shardBackends ?? '').split(',').map((s) => s.trim()).filter(Boolean)
      for (const s of shardSchemas) mk({ key: `sg:${s}`, origin: 'Router · shard schema', match: `${s}.<table> qualifier in SQL`, strategy: 'Scatter-gather', destination: shardBackends })
    }
    // 3. backend-set routing of store frontends (WARP_*_SET). Frontends served from the default set on a single host are one summary row.
    const plain: string[] = []
    for (const i of ifaces.data?.interfaces ?? []) {
      if (!i.store) continue
      const set = setByName.get(i.set ?? '')
      const hosts = i.hosts ?? set?.stores[i.store]?.hosts ?? []
      if (set?.isDefaultSet && hosts.length <= 1) { plain.push(i.label); continue }
      mk({ key: `set:${i.id}`, origin: 'Backend set', match: `${i.label} frontend · port ${i.port}`, strategy: hosts.length > 1 ? 'Shard across hosts' : 'Route', destination: hosts.length > 0 ? hosts : [i.set ?? 'default'],
        note: i.setEnvVar ? `${i.setEnvVar}=${i.set ?? 'default'}` : undefined })
    }
    if (plain.length > 0) {
      const def = allSets.find((s) => s.isDefaultSet)
      mk({ key: 'set:default', origin: 'Backend set', match: `${plain.length} store frontends, WARP_*_SET not set`, matchSub: plain.join(', '), strategy: 'Route', destination: [def?.name ?? 'default'] })
    }
    return out
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cfg, sets.data, ifaces.data, byName, setByName])

  // QoS: configured classes joined with the observed counters
  const classLimits = useMemo(() => {
    const m = new Map<string, { rate: string; burst: string; wait: string }>()
    for (const e of (cfg?.qosClassLimits ?? '').split(',')) {
      const p = e.split(':').map((x) => x.trim())
      if (p.length >= 3 && p[0]) m.set(p[0], { rate: p[1], burst: p[2], wait: p[3] ?? (cfg?.qosMaxWaitMs || '0') })
    }
    return m
  }, [cfg])
  const defaultLimit = { rate: filled(cfg?.qosRatePerSec) ? cfg!.qosRatePerSec! : '200', burst: filled(cfg?.qosBurst) ? cfg!.qosBurst! : String((Number(filled(cfg?.qosRatePerSec) ? cfg!.qosRatePerSec : 200)) * 2), wait: cfg?.qosMaxWaitMs || '0' }
  const qosRows = useMemo(() => {
    const seen = new Set<string>()
    const out: Array<{ key: string; tenant: string; cls: string; admitted: number; rejected: number; limit: { rate: string; burst: string; wait: string }; configured: boolean }> = []
    for (const c of counters) {
      seen.add(c.workloadClass)
      const l = classLimits.get(c.workloadClass)
      out.push({ key: `${c.tenant}:${c.workloadClass}`, tenant: c.tenant, cls: c.workloadClass, admitted: c.admitted, rejected: c.rejected, limit: l ?? defaultLimit, configured: !!l })
    }
    for (const [cls, l] of classLimits) if (!seen.has(cls)) out.push({ key: `-:${cls}`, tenant: '', cls, admitted: 0, rejected: 0, limit: l, configured: true })
    return out.sort((a, b) => b.rejected - a.rejected || b.admitted - a.admitted)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [counters, classLimits, cfg])

  const probed = backends.filter((b) => b.health)
  const healthy = probed.filter((b) => b.health?.ok).length
  const loadErr = config.error ?? sets.error ?? ifaces.error
  const kpis: KpiItem[] = [
    { label: 'Routes', value: config.data && sets.data && ifaces.data ? rows.length : '—', hint: `${(sets.data?.connectionRouting.routes.length ?? 0)} connection · ${rows.filter((r) => r.origin.startsWith('Router')).length} router · ${rows.filter((r) => r.origin === 'Backend set').length} set` },
    { label: 'QoS admitted', value: qos.data ? compact(admitted) : '—', hint: 'statements since this node started' },
    { label: 'QoS rejected', value: qos.data ? compact(rejected) : '—', hint: rate.current !== null ? `${rate.current.toFixed(1)} / min over the last poll` : admitted + rejected > 0 ? `${((rejected / (admitted + rejected)) * 100).toFixed(2)}% of attempts` : 'no statements yet' },
    { label: 'Backends healthy', value: probed.length > 0 ? `${healthy} / ${probed.length}` : '—', hint: probed.length > 0 ? 'from live probes' : 'no probe result yet' },
  ]

  const withPool = backends.filter((b) => b.pool)
  return (
    <div>
      <PageHeader title="Routing & QoS" description="Traffic placement, failover state, admission control and workload isolation, read from the live configuration. Edit rules on the Router rules and QoS tabs."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { config.reload(); sets.reload(); ifaces.reload(); qos.reload(); usage.reload() }}>Refresh</Button>} />
      {loadErr && <Notice tone="bad">Could not load: {loadErr}</Notice>}
      {config.loading && sets.loading ? <Loading /> : <KpiStrip items={kpis} label="Routing figures" />}

      <Section flush title="Active routes" meta="Connection routes first, then router rules in the order the router evaluates them, then backend-set routing">
        {rows.length === 0 && !config.loading && !sets.loading
          ? <EmptyState title="No explicit routes">Statements go to the default backend. Add connection routes on Infrastructure, or router rules on the Router rules tab.</EmptyState>
          : (
            <DataTable caption="Active routes" minWidth={1000}>
              <thead><tr><th>Source</th><th>Match</th><th>Strategy</th><th>Destination</th><th>Fallback</th><th>Health</th></tr></thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.key}>
                    <td style={{ whiteSpace: 'nowrap' }}><NameCell name={r.origin} sub={r.note} /></td>
                    <td><span className={styles.mono}>{r.match}</span>{r.matchSub && <div className={styles.sub}>{r.matchSub}</div>}</td>
                    <td style={{ whiteSpace: 'nowrap' }}><Tag>{r.strategy}</Tag></td>
                    <td className={styles.mono}>{r.destination.length > 0 ? r.destination.join(', ') : '—'}</td>
                    <td className={styles.mono}>{r.fallback}</td>
                    <td><StatusPill tone={r.health.tone}>{r.health.text}</StatusPill></td>
                  </tr>
                ))}
              </tbody>
            </DataTable>
          )}
        {sets.data && <div className={styles.pad}><span className={styles.sub}>Connection routing mode: <b>{sets.data.connectionRouting.mode}</b>. Fallback is the backend's own configured fallback.</span></div>}
      </Section>

      <Split ratio="wide-left">
        <Section flush title="Admission control" meta={`Per tenant and workload class · default ${defaultLimit.rate}/s, burst ${defaultLimit.burst}${cfg && filled(cfg.qosPoolWaitThreshold) ? ` · rejects when ${cfg.qosPoolWaitThreshold}+ threads wait for a pool connection` : ''}`}>
          {qos.error && <div className={styles.pad}><Notice tone="warn">Could not read QoS counters: {qos.error}</Notice></div>}
          {qosRows.length === 0 && !qos.loading
            ? <EmptyState title="No admissions counted yet">Counters appear once a SQL statement has passed the QoS stage. Warp keeps no queue depth: a request either gets a token, waits up to the class's max wait, or is rejected.</EmptyState>
            : (
              <DataTable caption="QoS classes" minWidth={720}>
                <thead><tr><th>Class</th><th>Limit</th><th>Max wait</th><th style={{ textAlign: 'right' }}>Admitted</th><th style={{ textAlign: 'right' }}>Rejected</th><th>State</th></tr></thead>
                <tbody>
                  {qosRows.map((r) => (
                    <tr key={r.key}>
                      <td><NameCell name={r.cls} sub={r.tenant ? `tenant ${r.tenant}` : 'configured, no traffic yet'} /></td>
                      <td className={styles.mono}>{r.limit.rate}/s · burst {r.limit.burst}{!r.configured && <div className={styles.sub}>default limit</div>}</td>
                      <td className={styles.num}>{Number(r.limit.wait) > 0 ? `${r.limit.wait} ms` : 'reject at once'}</td>
                      <td className={styles.num}>{r.admitted.toLocaleString()}</td>
                      <td className={styles.num}>{r.rejected.toLocaleString()}</td>
                      <td>{r.admitted + r.rejected === 0 ? <StatusPill tone="muted">No traffic</StatusPill> : r.rejected > 0 ? <StatusPill tone="warn">Throttled {((r.rejected / (r.admitted + r.rejected)) * 100).toFixed(1)}%</StatusPill> : <StatusPill tone="ok">Open</StatusPill>}</td>
                    </tr>
                  ))}
                </tbody>
              </DataTable>
            )}
          <div className={styles.pad}><span className={styles.sub}>Counters are per node since start. In a cluster each node applies limits divided by the cluster size.</span></div>
        </Section>

        <Section flush title="Pool pressure and fallbacks" meta="Backend health, connection pools, configured fallbacks">
          {backends.length === 0 ? <EmptyState title="No backends">Add a backend on Infrastructure.</EmptyState> : (
            <div className={styles.rowList}>
              {backends.map((b) => (
                <div className={styles.rowItem} key={`${b.set}/${b.name}`}>
                  <div style={{ minWidth: 0 }}>
                    <strong>{b.name}</strong>
                    <span className={styles.s}>{b.set} · {b.type}{b.fallback ? ` · fallback ${b.fallback}` : ' · no fallback'}</span>
                    {b.pool && <div style={{ marginTop: 4, minWidth: 140 }}><Meter value={b.pool.active} max={b.pool.max} label={`${b.name} pool`} caption={`${b.pool.active} / ${b.pool.max} active${b.pool.waiting > 0 ? ` · ${b.pool.waiting} waiting` : ''}`} /></div>}
                  </div>
                  <div className={styles.rowEnd}>
                    {b.health ? <StatusPill tone={b.health.ok ? 'ok' : 'bad'}>{b.health.ok ? `${b.health.tookMs} ms` : 'Unreachable'}</StatusPill> : <StatusPill tone="muted">Not probed</StatusPill>}
                  </div>
                </div>
              ))}
            </div>
          )}
          <div className={styles.pad}><span className={styles.sub}>Warp keeps no failover event log, so failover counts are not shown{withPool.length === 0 ? '; pool gauges appear after the first connection is borrowed' : ''}.</span></div>
        </Section>
      </Split>

      {usage.data && usage.data.byWorkloadClass.length > 0 && (
        <Section flush title="Traffic by workload class" meta="Calls, errors and average latency per class">
          <DataTable caption="Usage by workload class" minWidth={420}>
            <thead><tr><th>Class</th><th style={{ textAlign: 'right' }}>Calls</th><th style={{ textAlign: 'right' }}>Errors</th><th style={{ textAlign: 'right' }}>Avg latency</th></tr></thead>
            <tbody>{usage.data.byWorkloadClass.map((c) => <tr key={c.workloadClass}><td>{c.workloadClass}</td><td className={styles.num}>{c.calls.toLocaleString()}</td><td className={styles.num}>{c.errors}</td><td className={styles.num}>{c.avgMs} ms</td></tr>)}</tbody>
          </DataTable>
        </Section>
      )}
      <p className={styles.help}>AI suggestions for new shard rules and rate limits are drafted on the <Link to="/router">Router rules</Link> and <Link to="/qos">QoS</Link> tabs; a draft is never applied until you save it.</p>
    </div>
  )
}
