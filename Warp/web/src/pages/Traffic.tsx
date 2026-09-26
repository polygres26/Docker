import { useMemo } from 'react'
import { RefreshCw } from 'lucide-react'
import { Link } from 'react-router-dom'
import { getAnomalies, getUsage, getWireMetrics, listInterfaces } from '../api/client'
import {
  getMetricsHistory, getPolicyDecisions, getPrometheus, ratePoints, ratioPoints, sumKeys, type PromSample,
} from '../api/insights'
import { TimeSeries } from '../components/charts'
import {
  Button, DataTable, EmptyState, KpiStrip, Loading, ModeTag, NameCell, Notice, PageHeader, Section, StatusPill, compact, type KpiItem,
} from '../components/ui'
import { useLoad } from '../hooks'
import { latencyFor } from './interfaces/InterfaceTable'
import styles from './Traffic.module.css'

const POLL_MS = 10_000
const COLORS = ['var(--sy-series-1)', 'var(--sy-series-2)', 'var(--sy-series-3)', 'var(--sy-series-4)', 'var(--sy-series-6)']

const OUTCOME: Record<string, string> = {
  cache_hit: 'Cache hit', pg_read: 'Backend read', pg_write: 'Backend write', enqueue: 'Enqueue', dequeue: 'Dequeue',
}

/** Requests answered: statements the pipeline executed (per protocol) plus those the cache answered before execution. */
function requestsOf(c: Record<string, number>): number | null {
  return Object.keys(c).some((k) => k.startsWith('proto.')) ? sumKeys(c, 'proto.') + (c.cacheServed ?? 0) : null
}

function qosByClass(prom: PromSample[] | null) {
  if (!prom) return null
  const by = new Map<string, { admitted: number; rejected: number }>()
  for (const s of prom) {
    if (s.name !== 'warp_qos_admitted_total' && s.name !== 'warp_qos_rejected_total') continue
    const k = s.labels.workload_class ?? ''
    const e = by.get(k) ?? { admitted: 0, rejected: 0 }
    if (s.name === 'warp_qos_admitted_total') e.admitted += s.value; else e.rejected += s.value
    by.set(k, e)
  }
  return [...by.entries()].map(([workloadClass, v]) => ({ workloadClass, ...v })).sort((a, b) => b.admitted + b.rejected - (a.admitted + a.rejected))
}

/**
 * Traffic: demand, latency, errors and policy outcomes. Trends come from the server's own in-memory sampler
 * (GET /api/metrics/history, every 10 s, node-local, empty after a restart); tables come from the metrics summary,
 * usage, Prometheus text and the policy decision feed. Percentile latency is not tracked by Warp, so it is not shown.
 */
export default function Traffic() {
  const history = useLoad(getMetricsHistory, POLL_MS)
  const metrics = useLoad(getWireMetrics, POLL_MS)
  const usage = useLoad(getUsage, POLL_MS)
  const ifaces = useLoad(listInterfaces, POLL_MS)
  const anomalies = useLoad(getAnomalies, 30_000)
  const prom = useLoad(getPrometheus, POLL_MS)
  const decisions = useLoad(() => getPolicyDecisions(200), POLL_MS)

  const m = metrics.data
  const samples = useMemo(() => history.data?.samples ?? [], [history.data])

  const charts = useMemo(() => {
    const total = ratePoints(samples, requestsOf)
    const errors = ratePoints(samples, 'errors')
    const qosRej = samples.some((s) => 'qosRejected' in s.c) ? ratePoints(samples, 'qosRejected') : []
    const protos = new Set<string>()
    samples.forEach((s) => Object.keys(s.c).forEach((k) => { if (k.startsWith('rttCalls.')) protos.add(k.slice('rttCalls.'.length)) }))
    const latency = [...protos]
      .map((p) => ({ label: p, points: ratioPoints(samples, `rttMs.${p}`, `rttCalls.${p}`), calls: samples.length ? samples[samples.length - 1].c[`rttCalls.${p}`] ?? 0 : 0 }))
      .sort((a, b) => b.calls - a.calls).slice(0, 5)
      .map((s, i) => ({ label: s.label, color: COLORS[i % COLORS.length], points: s.points }))
    return { total, errors, qosRej, latency }
  }, [samples])

  const recentRate = charts.total.length ? charts.total.slice(-6).reduce((s, p) => s + p.v, 0) / Math.min(6, charts.total.length) : null
  const lastSample = samples.length ? samples[samples.length - 1] : null
  const totalRequests = lastSample && requestsOf(lastSample.c) !== null ? requestsOf(lastSample.c) : m ? Object.values(m.protocolCounts).reduce((s, n) => s + n, 0) : null
  const tenants = usage.data?.byTenant ?? []
  const stmts = tenants.reduce((s, t) => s + t.calls, 0)
  const errs = tenants.reduce((s, t) => s + t.errors, 0)

  const kpis: KpiItem[] = [
    { label: 'Requests / sec', value: recentRate === null ? '—' : recentRate.toFixed(recentRate < 10 ? 2 : 1), hint: recentRate === null ? 'needs two server samples' : 'mean of the last minute, all protocols' },
    { label: 'Requests served', value: totalRequests === null ? '—' : compact(totalRequests), hint: 'executed plus answered from cache, since process start' },
    { label: 'SQL statement errors', value: usage.data ? (stmts === 0 ? '—' : `${((errs / stmts) * 100).toFixed(2)}%`) : '—', hint: usage.data ? `${errs.toLocaleString()} of ${stmts.toLocaleString()} SQL statements` : undefined },
    { label: 'Avg round-trip', value: m?.avgRttMs != null ? (m.avgRttMs < 1 ? '< 1 ms' : `${m.avgRttMs} ms`) : '—', hint: m && m.rttSamples > 0 ? `${compact(m.rttSamples)} samples` : 'no samples yet' },
    { label: 'Active sessions', value: ifaces.data?.activeSessions ?? '—', hint: 'all wire protocols' },
  ]

  const ifaceRows = useMemo(() => [...(ifaces.data?.interfaces ?? [])].filter((i) => (i.requests ?? 0) > 0).sort((a, b) => (b.requests ?? 0) - (a.requests ?? 0)), [ifaces.data])
  const maxReq = ifaceRows[0]?.requests ?? 0
  const idle = (ifaces.data?.interfaces ?? []).filter((i) => (i.requests ?? 0) === 0).length
  const classes = usage.data?.byWorkloadClass ?? []
  const qos = qosByClass(prom.data)
  const blocks = useMemo(() => {
    const by = new Map<string, number>()
    for (const d of decisions.data?.decisions ?? []) if (d.decision === 'Block') by.set(d.policy, (by.get(d.policy) ?? 0) + 1)
    return [...by.entries()].sort((a, b) => b[1] - a[1])
  }, [decisions.data])
  const outcomes = m?.rttByOutcome ?? []
  const notes = anomalies.data?.notes ?? []
  const sampledFor = samples.length >= 2 ? Math.round((samples[samples.length - 1].t - samples[0].t) / 60000) : 0
  const noHistory = history.data && !history.data.enabled

  return (
    <div>
      <PageHeader title="Traffic" description="Demand, latency, errors and policy outcomes across every interface Warp serves."
        actions={<>
          <StatusPill tone={metrics.error ? 'bad' : 'ok'}>{metrics.error ? 'Admin API error' : `Live · ${metrics.updated ? metrics.updated.toLocaleTimeString() : '…'}`}</StatusPill>
          <Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { history.reload(); metrics.reload(); usage.reload(); ifaces.reload(); anomalies.reload(); prom.reload(); decisions.reload() }}>Refresh</Button>
        </>} />
      {metrics.error && <Notice tone="bad">Could not load metrics: {metrics.error}</Notice>}
      {!m && !metrics.error ? <Loading>Loading traffic…</Loading> : <KpiStrip items={kpis} label="Traffic figures" />}

      <div className={styles.half}>
        <Section title="Request volume" meta={sampledFor > 0 ? `requests / sec · last ${sampledFor} min` : 'requests / sec'}>
          {history.error ? <Notice tone="warn">History unavailable: {history.error}</Notice> : noHistory
            ? <EmptyState title="History is not enabled">This Warp build has no metrics sampler.</EmptyState>
            : <TimeSeries area label="Request volume, requests per second" series={[{ label: 'All protocols', color: 'var(--sy-series-1)', points: charts.total }]} format={(v) => v.toFixed(v < 10 ? 2 : 0)} />}
        </Section>
        <Section title="Latency by interface" meta="avg round-trip ms per 10 s window">
          {history.error ? <Notice tone="warn">History unavailable</Notice>
            : <TimeSeries label="Average round-trip latency by protocol" series={charts.latency} format={(v) => `${v.toFixed(v < 10 ? 2 : 1)} ms`}
              emptyText="No latency samples in the window yet. A window with no requests has no average, so idle periods leave gaps." />}
        </Section>
      </div>

      <div className={styles.half}>
        <Section title="Errors" meta="per second">
          {history.error ? <Notice tone="warn">History unavailable</Notice>
            : <TimeSeries label="SQL statement errors per second" series={[
              { label: 'SQL statement errors', color: 'var(--sy-red)', points: charts.errors },
              ...(charts.qosRej.length ? [{ label: 'QoS rejections', color: 'var(--sy-series-3)', points: charts.qosRej }] : []),
            ]} format={(v) => v.toFixed(2)} />}
        </Section>
        <Section title="Policy outcomes" meta="since process start">
          {!prom.data && !prom.error && !decisions.data ? <Loading /> : (
            <div>
              {qos && qos.length > 0 ? (
                <DataTable caption="QoS admissions by workload class" minWidth={320}>
                  <thead><tr><th>QoS class</th><th>Admitted</th><th>Rejected</th></tr></thead>
                  <tbody>{qos.map((q) => <tr key={q.workloadClass || 'default'}><td>{q.workloadClass || 'default'}</td><td className={styles.num}>{q.admitted.toLocaleString()}</td><td className={styles.num}>{q.rejected > 0 ? <StatusPill tone="warn">{q.rejected.toLocaleString()}</StatusPill> : 0}</td></tr>)}</tbody>
                </DataTable>
              ) : <p className={styles.note}>{prom.error ? `Prometheus text unavailable: ${prom.error}` : 'No QoS decisions recorded yet (QoS counts statements as they run).'}</p>}
              <div className={styles.pad}>
                <div className={styles.pillRow}>
                  {blocks.length === 0
                    ? <span className={styles.sub}>No firewall or ACL blocks recorded since this node started.</span>
                    : blocks.map(([policy, n]) => <StatusPill key={policy} tone="bad">{policy}: {n} blocked</StatusPill>)}
                </div>
                <div className={styles.sub} style={{ marginTop: 6 }}>Decisions are listed on <Link to="/policies">Policies</Link>.</div>
              </div>
            </div>
          )}
        </Section>
      </div>

      <Section flush title="Top interfaces" meta={idle > 0 ? `${idle} idle interface${idle === 1 ? '' : 's'} not shown` : 'sorted by requests'}>
        {ifaces.error ? <div className={styles.pad}><Notice tone="bad">{ifaces.error}</Notice></div> : !ifaces.data ? <div className={styles.pad}><Loading /></div>
          : ifaceRows.length === 0 ? <EmptyState title="No traffic yet">No interface has served a request since this node started.</EmptyState> : (
            <DataTable caption="Top interfaces by requests" minWidth={720}>
              <thead><tr><th>Interface</th><th>Mode</th><th>Executed</th><th>From cache</th><th>Share of executed</th><th>Avg round-trip</th></tr></thead>
              <tbody>
                {ifaceRows.map((i) => {
                  const lat = latencyFor(m, i.metricsKey)
                  const total = ifaceRows.reduce((s, r) => s + (r.requests ?? 0), 0)
                  const cached = (m?.rttByOutcome ?? []).filter((r) => r.protocol === i.metricsKey && r.outcome === 'cache_hit').reduce((a, r) => a + r.calls, 0)
                  return (
                    <tr key={i.id}>
                      <td><NameCell name={i.label} sub={`${i.protocol} · port ${i.port}`} /></td>
                      <td><ModeTag mode={i.mode} /></td>
                      <td className={styles.num}>{compact(i.requests ?? 0)}</td>
                      <td className={styles.num}>{cached > 0 ? compact(cached) : <span className={styles.sub}>0</span>}</td>
                      <td><div className={styles.share}><div className={styles.track}><span style={{ width: `${maxReq ? ((i.requests ?? 0) / maxReq) * 100 : 0}%` }} /></div><span className={styles.num}>{total ? Math.round(((i.requests ?? 0) / total) * 100) : 0}%</span></div></td>
                      <td className={styles.num}>{lat ? `${lat.avgMs.toFixed(lat.avgMs < 10 ? 2 : 1)} ms` : <span className={styles.sub}>no samples</span>}</td>
                    </tr>
                  )
                })}
              </tbody>
            </DataTable>
          )}
      </Section>

      <div className={styles.half}>
        <Section flush title="By workload class" meta="App / Analytics / AI or router-assigned">
          {!usage.data ? <div className={styles.pad}>{usage.error ? <Notice tone="warn">{usage.error}</Notice> : <Loading />}</div> : classes.length === 0
            ? <EmptyState title="No classified traffic yet">SQL statements are attributed to a class as they run.</EmptyState> : (
              <DataTable caption="Traffic by workload class" minWidth={420}>
                <thead><tr><th>Class</th><th>Statements</th><th>Errors</th><th>Error rate</th><th>Avg</th></tr></thead>
                <tbody>{classes.map((c) => <tr key={c.workloadClass}><td>{c.workloadClass}</td><td className={styles.num}>{c.calls.toLocaleString()}</td><td className={styles.num}>{c.errors}</td><td className={styles.num}>{c.calls ? `${((c.errors / c.calls) * 100).toFixed(2)}%` : '—'}</td><td className={styles.num}>{c.avgMs} ms</td></tr>)}</tbody>
              </DataTable>
            )}
        </Section>
        <Section flush title="Latency by outcome" meta="cache hit vs backend, per protocol">
          {!m ? <div className={styles.pad}><Loading /></div> : outcomes.length === 0 ? <EmptyState title="No measured traffic yet">Round-trip time is recorded per protocol once requests are served.</EmptyState> : (
            <DataTable caption="Latency by protocol and outcome" minWidth={420}>
              <thead><tr><th>Protocol</th><th>Outcome</th><th>Calls</th><th>Avg</th></tr></thead>
              <tbody>{outcomes.map((r) => <tr key={`${r.protocol}-${r.outcome}`}><td className={styles.mono}>{r.protocol}</td><td>{OUTCOME[r.outcome] ?? r.outcome}</td><td className={styles.num}>{compact(r.calls)}</td><td className={styles.num}>{r.calls ? `${(r.totalMs / r.calls).toFixed(r.totalMs / r.calls < 10 ? 2 : 1)} ms` : '—'}</td></tr>)}</tbody>
            </DataTable>
          )}
        </Section>
      </div>

      <Section title="Anomalies" meta={anomalies.data ? (anomalies.data.enabled ? `${notes.length} recorded` : 'detection off') : undefined}>
        {!anomalies.data ? (anomalies.error ? <Notice tone="warn">{anomalies.error}</Notice> : <Loading />)
          : !anomalies.data.enabled ? <p className={styles.note}>Traffic anomaly detection is not enabled on this deployment.</p>
          : notes.length === 0 ? <p className={styles.note}>No traffic anomaly recorded.</p> : (
            <ul className={styles.anom}>{notes.slice(0, 8).map((n, i) => (
              <li key={i}><span><StatusPill tone="warn">{n.protocol}</StatusPill> {n.ratio}x baseline ({n.currentPerSec}/s vs {n.baselinePerSec}/s)</span><span className={styles.sub}>{new Date(n.timestamp).toLocaleString()}{n.narrative ? ` · ${n.narrative}` : ''}</span></li>
            ))}</ul>
          )}
      </Section>
      <p className={styles.note}>
        Trends are sampled in memory on this node every {history.data?.intervalSeconds ?? 10} seconds and reset when it restarts. Percentile latency is not measured by Warp.
        Per-statement cost, backend and MCP tool tables are on the <Link to="/traffic-detail">SQL and backends</Link> tab.
      </p>
    </div>
  )
}
