import { useCallback, useEffect, useMemo, useState } from 'react'
import { Database, RefreshCw } from 'lucide-react'
import { Link } from 'react-router-dom'
import {
  type BackendInfo, type BackendTestResult, type NodeInfo, type WireMetricsSummary,
  getWireConfig, getWireMetrics, listBackends, listNodes, parseBackendSetNames, testConfiguredBackend,
} from '../api/client'
import {
  Button, DataTable, EmptyState, KpiStrip, Loading, Notice, PageHeader, Section, SortTh, SummaryGrid, StatusPill, Tag, useSort,
  type KpiItem, type Tone,
} from '../components/ui'
import styles from './Dashboard.module.css'

const POLL_MS = 10_000

function fmt(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return String(n)
}

/** Host[:port]/db portion of a JDBC URL, for a compact "target" column. */
function targetOf(jdbcUrl: string): string {
  return jdbcUrl.replace(/^jdbc:[^:]+:(\/\/)?/, '').replace(/\?.*$/, '')
}

type Probe = { state: 'pending' } | { state: 'done'; result: BackendTestResult }

interface Row {
  name: string
  dialect: string
  target: string
  calls: number
  avgMs: number | null
  probe: Probe
}

/**
 * Gateway overview: health strip, backend table and protocol mix, all from the admin API that
 * already exists (/api/metrics/summary, /api/backends, /api/nodes, /api/config). Nothing here is
 * estimated: a figure the API does not report (p95 latency, policy-block counts) is not shown.
 * Backend health is a real probe -- the same POST /api/backends/{name}/test the Backends page uses.
 */
export default function Dashboard() {
  const [metrics, setMetrics] = useState<WireMetricsSummary | null>(null)
  const [metricsError, setMetricsError] = useState<string | null>(null)
  const [backends, setBackends] = useState<BackendInfo[] | null>(null)
  const [backendsError, setBackendsError] = useState<string | null>(null)
  const [nodes, setNodes] = useState<NodeInfo[] | null>(null)
  const [setNames, setSetNames] = useState<string[] | null>(null)
  const [probes, setProbes] = useState<Record<string, Probe>>({})
  const [updated, setUpdated] = useState<Date | null>(null)

  const loadMetrics = useCallback(() => {
    getWireMetrics()
      .then((m) => { setMetrics(m); setMetricsError(null); setUpdated(new Date()) })
      .catch((e) => setMetricsError(e instanceof Error ? e.message : String(e)))
    // Heartbeats are optional (single-node deployments may not run the loop): absent means "no data".
    listNodes().then(setNodes).catch(() => setNodes(null))
  }, [])

  const probeAll = useCallback((list: BackendInfo[]) => {
    setProbes(Object.fromEntries(list.map((b) => [b.name, { state: 'pending' } as Probe])))
    list.forEach((b) => {
      testConfiguredBackend(b.name)
        .catch((e): BackendTestResult => ({ ok: false, message: e instanceof Error ? e.message : String(e), tookMs: 0, serverVersion: null }))
        .then((result) => setProbes((p) => ({ ...p, [b.name]: { state: 'done', result } })))
    })
  }, [])

  const loadBackends = useCallback(() => {
    listBackends()
      .then((list) => { setBackends(list); setBackendsError(null); probeAll(list) })
      .catch((e) => setBackendsError(e instanceof Error ? e.message : String(e)))
  }, [probeAll])

  useEffect(() => {
    loadMetrics()
    loadBackends()
    getWireConfig().then((c) => setSetNames(parseBackendSetNames(c.backendSets))).catch(() => setSetNames(null))
    const id = setInterval(loadMetrics, POLL_MS)
    return () => clearInterval(id)
  }, [loadMetrics, loadBackends])

  const rows: Row[] = useMemo(() => (backends ?? []).map((b) => {
    const stat = metrics?.byBackend.find((x) => x.backend === b.name)
    return {
      name: b.name,
      dialect: b.dialect ?? 'unknown',
      target: targetOf(b.jdbcUrl),
      calls: stat?.calls ?? 0,
      avgMs: stat ? stat.avgMs : null,
      probe: probes[b.name] ?? { state: 'pending' },
    }
  }), [backends, metrics, probes])

  const { sorted, sort, toggle } = useSort(rows, {
    name: (r) => r.name, dialect: (r) => r.dialect, target: (r) => r.target, calls: (r) => r.calls,
    health: (r) => (r.probe.state === 'pending' ? 1 : r.probe.result.ok ? 0 : 2),
  }, { key: 'calls', dir: 'desc' })

  const totalCalls = rows.reduce((s, r) => s + r.calls, 0)
  const done = rows.filter((r) => r.probe.state === 'done')
  const down = done.filter((r) => r.probe.state === 'done' && !r.probe.result.ok).length
  const staleNodes = nodes?.filter((n) => n.status === 'stale').length ?? 0

  let envTone: Tone = 'ok'
  let envText = 'All systems operational'
  if (metricsError) { envTone = 'bad'; envText = 'Admin API error' }
  else if (!metrics || !backends) { envTone = 'muted'; envText = 'Checking…' }
  else if (down > 0) { envTone = 'warn'; envText = `${down} of ${rows.length} backend${rows.length === 1 ? '' : 's'} unreachable` }
  else if (staleNodes > 0) { envTone = 'warn'; envText = `${staleNodes} node${staleNodes === 1 ? '' : 's'} stale` }
  else if (done.length < rows.length) { envTone = 'muted'; envText = 'Probing backends…' }

  const kpis: KpiItem[] = [{ label: 'Environment', value: envText, tone: envTone, wide: true }]
  if (metrics) {
    kpis.push({ label: 'Requests / sec', value: (metrics.readsPerSec + metrics.writesPerSec).toFixed(1), hint: `${metrics.readsPerSec.toFixed(1)} reads · ${metrics.writesPerSec.toFixed(1)} writes` })
    if (metrics.avgRttMs !== null) kpis.push({ label: 'Avg round-trip', value: `${metrics.avgRttMs} ms`, hint: `${fmt(metrics.rttSamples)} sample${metrics.rttSamples === 1 ? '' : 's'}` })
  }
  if (backends) kpis.push({ label: 'Active backends', value: backends.length, hint: setNames && setNames.length > 0 ? `${setNames.length} backend set${setNames.length === 1 ? '' : 's'}` : undefined })
  if (nodes && nodes.length > 0) kpis.push({ label: 'Nodes up', value: `${nodes.length - staleNodes} / ${nodes.length}`, hint: staleNodes > 0 ? `${staleNodes} stale` : 'all reporting' })

  const protocols = metrics ? Object.entries(metrics.protocolCounts).sort((a, b) => b[1] - a[1]) : []
  const topSql = metrics?.topSql.slice(0, 5) ?? []

  return (
    <div>
      <PageHeader
        title="Gateway overview"
        description="Live health and traffic for this Warp process."
        actions={
          <Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { loadMetrics(); loadBackends() }}>Refresh</Button>
        }
      />

      {metricsError && <Notice tone="bad">Could not load metrics: {metricsError}</Notice>}
      {!metrics && !metricsError ? <Loading>Loading overview…</Loading> : <KpiStrip items={kpis} label="Gateway health" />}

      <Section flush title="Backends" meta={updated ? `Updated ${updated.toLocaleTimeString()}` : undefined}>
        {backendsError ? (
          <div className={styles.pad}><Notice tone="bad">Could not list backends: {backendsError}</Notice></div>
        ) : !backends ? (
          <div className={styles.pad}><Loading>Loading backends…</Loading></div>
        ) : backends.length === 0 ? (
          <EmptyState icon={<Database size={18} aria-hidden="true" />} title="No backends configured">
            Add a named Postgres target on the <Link to="/backends">Backends</Link> page and it will show up here with a live health check.
          </EmptyState>
        ) : (
          <DataTable caption="Configured backends" minWidth={720}>
            <thead>
              <tr>
                <SortTh label="Backend" k="name" sort={sort} onSort={toggle} />
                <SortTh label="Dialect" k="dialect" sort={sort} onSort={toggle} />
                <SortTh label="Target" k="target" sort={sort} onSort={toggle} />
                <SortTh label="Health" k="health" sort={sort} onSort={toggle} />
                <SortTh label="Calls" k="calls" sort={sort} onSort={toggle} />
                <th>Share of traffic</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((r) => (
                <tr key={r.name}>
                  <td className={styles.mono}>{r.name}</td>
                  <td><Tag>{r.dialect}</Tag></td>
                  <td className={styles.mono}>{r.target}</td>
                  <td><HealthPill probe={r.probe} /></td>
                  <td className={styles.num}>{r.calls.toLocaleString()}{r.avgMs !== null && <span className={styles.sub}> · {r.avgMs} ms avg</span>}</td>
                  <td>
                    <div className={styles.bar} role="img" aria-label={`${totalCalls ? Math.round((r.calls / totalCalls) * 100) : 0}% of statements`}>
                      <span style={{ width: `${totalCalls ? (r.calls / totalCalls) * 100 : 0}%` }} />
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </Section>

      {metrics && (
        <Section flush title="By protocol" meta="Statements since process start">
          {protocols.length === 0 ? (
            <EmptyState title="No traffic yet">Send a query through any wire protocol to see it here.</EmptyState>
          ) : (
            <SummaryGrid items={protocols.map(([name, count]) => ({ title: name, sub: `${count.toLocaleString()} statement${count === 1 ? '' : 's'}`, icon: <Database size={16} aria-hidden="true" /> }))} />
          )}
        </Section>
      )}

      {metrics && topSql.length > 0 && (
        <Section flush title="Top SQL by cost" meta={<Link to="/metrics">All traffic</Link>}>
          <DataTable caption="Most expensive statements" minWidth={560}>
            <thead><tr><th>SQL</th><th>Calls</th><th>Avg</th><th>Total</th></tr></thead>
            <tbody>
              {topSql.map((s, i) => (
                <tr key={i}>
                  <td className={styles.sql}>{s.sql}</td>
                  <td className={styles.num}>{fmt(s.calls)}</td>
                  <td className={styles.num}>{s.avgMs} ms</td>
                  <td className={styles.num}>{fmt(s.totalMs)} ms</td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        </Section>
      )}
    </div>
  )
}

function HealthPill({ probe }: { probe: Probe }) {
  if (probe.state === 'pending') return <StatusPill tone="muted">Checking</StatusPill>
  if (probe.result.ok) return <StatusPill tone="ok">Healthy · {probe.result.tookMs} ms</StatusPill>
  return <span title={probe.result.message}><StatusPill tone="bad">Unreachable</StatusPill></span>
}
