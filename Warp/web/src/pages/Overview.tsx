import { useCallback, useMemo } from 'react'
import { Database, RefreshCw } from 'lucide-react'
import { Link } from 'react-router-dom'
import {
  getAnomalies, getUsage, getWireConfig, getWireMetrics, listBackendSets, listInterfaces, listMcpEndpoints, listNodes,
  type InterfaceKind, type InterfaceInfo,
} from '../api/client'
import { readinessOf } from '../api/readiness'
import {
  Button, DataTable, EmptyState, KpiStrip, Loading, Meter, Notice, PageHeader, Section, StatusPill, Tag, compact, formatLicenseLimit,
  type KpiItem, type Tone,
} from '../components/ui'
import { errorText, targetOf, useLoad } from '../hooks'
import { MaturityLegend } from './interfaces/InterfaceTable'
import styles from './Overview.module.css'

const POLL_MS = 10_000

/**
 * Gateway overview: health, demand and risk. Every figure comes from the admin API (metrics summary, interfaces,
 * backend sets with live probes, nodes, MCP endpoints, anomalies, usage). A figure the API does not report
 * (p95 latency, policy blocks, request trend) is not shown. Nothing here is estimated.
 */
export default function Overview() {
  const metrics = useLoad(getWireMetrics, POLL_MS)
  const ifaces = useLoad(listInterfaces, POLL_MS)
  // Health probes connect to every backend, so this is polled more slowly than the metrics.
  const sets = useLoad(useCallback(() => listBackendSets(true), []), 30_000)
  const nodes = useLoad(listNodes, POLL_MS)
  const endpoints = useLoad(listMcpEndpoints, 30_000)
  const anomalies = useLoad(getAnomalies, 30_000)
  const usage = useLoad(getUsage, POLL_MS)
  const config = useLoad(getWireConfig, 30_000)

  const m = metrics.data
  const backends = useMemo(() => (sets.data?.sets ?? []).flatMap((s) => s.backends.map((b) => ({ ...b, set: s.name }))), [sets.data])
  const probed = backends.filter((b) => b.health)
  const down = probed.filter((b) => b.health && !b.health.ok)
  const staleNodes = nodes.data?.filter((n) => n.status === 'stale') ?? []
  const expired = (endpoints.data ?? []).filter((e) => e.status === 'expired')
  const notes = anomalies.data?.notes ?? []

  let envTone: Tone = 'ok'
  let envText = 'All systems operational'
  if (metrics.error) { envTone = 'bad'; envText = 'Admin API error' }
  else if (!m || !sets.data) { envTone = 'muted'; envText = 'Checking…' }
  else if (down.length > 0) { envTone = 'bad'; envText = `${down.length} of ${backends.length} backend${backends.length === 1 ? '' : 's'} unreachable` }
  else if (staleNodes.length > 0) { envTone = 'warn'; envText = `${staleNodes.length} node${staleNodes.length === 1 ? '' : 's'} stale` }
  else if (notes.length > 0) { envTone = 'warn'; envText = 'Traffic anomaly noted' }

  const kpis: KpiItem[] = [{ label: 'Environment', value: envText, tone: envTone, wide: true }]
  if (m) {
    kpis.push({ label: 'Requests / sec', value: (m.readsPerSec + m.writesPerSec).toFixed(1), hint: `${m.readsPerSec.toFixed(1)} reads · ${m.writesPerSec.toFixed(1)} writes` })
    kpis.push({ label: 'Avg round-trip', value: m.avgRttMs !== null ? `${m.avgRttMs} ms` : '—', hint: m.avgRttMs !== null ? `${compact(m.rttSamples)} samples` : 'no samples yet' })
  }
  if (ifaces.data) kpis.push({ label: 'Active sessions', value: ifaces.data.activeSessions, hint: `${ifaces.data.interfaces.length} interfaces listening` })

  const byKind = (k: InterfaceKind) => (ifaces.data?.interfaces ?? []).filter((i) => i.kind === k)
  const sumReq = (k: InterfaceKind) => byKind(k).reduce((s, i) => s + (i.requests ?? 0), 0)
  const protocols = m ? Object.entries(m.protocolCounts).sort((a, b) => b[1] - a[1]) : []
  const maxProto = protocols[0]?.[1] ?? 0
  const topSql = m?.topSql.slice(0, 5) ?? []
  const classes = usage.data?.byWorkloadClass ?? []

  // Risk: things that actually need attention, from real data only.
  const risks: Array<{ tone: Tone; text: React.ReactNode }> = []
  down.forEach((b) => risks.push({ tone: 'bad', text: <><strong>{b.name}</strong> is unreachable: {b.health?.message}</> }))
  backends.filter((b) => b.pool && b.pool.max > 0 && b.pool.active / b.pool.max >= 0.8).forEach((b) => risks.push({ tone: 'warn', text: <><strong>{b.name}</strong> pool is {Math.round(((b.pool?.active ?? 0) / (b.pool?.max ?? 1)) * 100)}% in use</> }))
  backends.filter((b) => b.state !== 'ACTIVE' && b.state !== 'PENDING').forEach((b) => risks.push({ tone: 'warn', text: <><strong>{b.name}</strong> is {b.state.toLowerCase()}</> }))
  staleNodes.forEach((n) => risks.push({ tone: 'warn', text: <>Node <strong>{n.nodeId}</strong> has not heartbeated recently</> }))
  expired.forEach((e) => risks.push({ tone: 'muted', text: <>MCP endpoint <strong>{e.name}</strong> has expired: <Link to="/interfaces/mcp">revoke it</Link></> }))
  notes.slice(0, 3).forEach((n) => risks.push({ tone: 'warn', text: <><strong>{n.protocol}</strong> traffic {n.ratio}x its baseline ({n.currentPerSec}/s vs {n.baselinePerSec}/s){n.narrative ? `: ${n.narrative}` : ''}</> }))
  // These two were already fetched elsewhere on this page (WireConfig for other tabs' editors,
  // MCP endpoints above for the expiry check) but never turned into a risk item themselves --
  // a real, already-loaded gap, not new data collection.
  if (config.data && (config.data.aclRules === null || config.data.aclRules.trim() === '')) {
    risks.push({ tone: 'warn', text: <>No network ACL configured: every client IP can attempt to connect. Add rules on <Link to="/acl">Access</Link>.</> })
  }
  if (endpoints.data && endpoints.data.length === 0) {
    risks.push({ tone: 'muted', text: <>No MCP endpoints are configured: agent/LLM clients have nothing scoped to connect through. Create one on <Link to="/interfaces/mcp">MCP servers</Link>.</> })
  }
  // The shared readiness evaluator (api/readiness.ts), grouped by dimension -- one risk line per
  // real gap category across every interface, not one line per interface (which would swamp this
  // list the moment more than a couple of frontends share the same gap). Uses the same per-
  // dimension facts ReadinessCell already renders per row on Interfaces/Workloads; this is the
  // aggregate view of the identical data, not a new computation.
  if (ifaces.data) {
    const withReadiness = ifaces.data.interfaces.map((i) => ({ i, readiness: readinessOf(i) }))
    const namesOf = (rows: Array<{ i: InterfaceInfo }>) =>
      rows.slice(0, 4).map((r) => r.i.label).join(', ') + (rows.length > 4 ? `, +${rows.length - 4} more` : '')
    const openAuth = withReadiness.filter((r) => r.readiness.dimensions.auth === 'open')
    const unreportedAuth = withReadiness.filter((r) => r.readiness.dimensions.auth === 'not_reported')
    const tlsOff = withReadiness.filter((r) => r.readiness.dimensions.encryption === 'disabled')
    const tlsErrored = withReadiness.filter((r) => r.readiness.dimensions.encryption === 'error')
    const noStore = withReadiness.filter((r) => r.readiness.dimensions.backend === 'not_configured')
    // Least-verified interfaces (Experimental/Preview, see api/maturity.ts's MATURITY_ORDER) --
    // informational (muted), not itself a fault: a low maturity tier just means less evidence
    // exists yet, distinct from the confirmed problems above. Renamed from "Certified" naming per
    // the same UI review this whole readiness effort responds to; see the Maturity scale legend
    // at the bottom of this page for what each tier actually means.
    const leastVerified = withReadiness.filter((r) => r.readiness.dimensions.maturity === 'Experimental' || r.readiness.dimensions.maturity === 'Preview')
    if (openAuth.length > 0) {
      risks.push({ tone: 'bad', text: <>{openAuth.length} interface{openAuth.length === 1 ? '' : 's'} {openAuth.length === 1 ? 'accepts' : 'accept'} connections with no authentication enforced: {namesOf(openAuth)}. <Link to="/acl">Review access</Link>.</> })
    }
    if (tlsErrored.length > 0) {
      risks.push({ tone: 'bad', text: <>{tlsErrored.length} interface{tlsErrored.length === 1 ? '' : 's'} configured TLS but {tlsErrored.length === 1 ? "it's" : "they're"} failing: {namesOf(tlsErrored)}. <Link to="/certificates">Check certificates</Link>.</> })
    }
    if (unreportedAuth.length > 0) {
      risks.push({ tone: 'warn', text: <>{unreportedAuth.length} interface{unreportedAuth.length === 1 ? '' : 's'} {unreportedAuth.length === 1 ? 'reports' : 'report'} no authentication status at all: {namesOf(unreportedAuth)}.</> })
    }
    if (tlsOff.length > 0) {
      risks.push({ tone: 'warn', text: <>{tlsOff.length} interface{tlsOff.length === 1 ? '' : 's'} {tlsOff.length === 1 ? 'has' : 'have'} TLS disabled: {namesOf(tlsOff)}. <Link to="/certificates">Configure TLS</Link>.</> })
    }
    if (noStore.length > 0) {
      risks.push({ tone: 'warn', text: <>{noStore.length} store-backed interface{noStore.length === 1 ? '' : 's'} {noStore.length === 1 ? 'has' : 'have'} no backend configured, served from the default backend instead: {namesOf(noStore)}. <Link to="/infrastructure">Enable a store</Link>.</> })
    }
    if (leastVerified.length > 0) {
      risks.push({ tone: 'muted', text: <>{leastVerified.length} interface{leastVerified.length === 1 ? '' : 's'} {leastVerified.length === 1 ? 'is' : 'are'} still Experimental or Preview maturity (least verified): {namesOf(leastVerified)}. See the maturity scale on <Link to="/interfaces/sql">SQL drivers</Link> or <Link to="/interfaces/api">API endpoints</Link>.</> })
    }
  }
  const riskLoading = !sets.data && !sets.error

  return (
    <div>
      <PageHeader title="Gateway overview" description="Health, demand and risk across every interface Warp fronts."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { metrics.reload(); ifaces.reload(); sets.reload(); nodes.reload(); endpoints.reload(); anomalies.reload(); usage.reload(); config.reload() }}>Refresh</Button>} />
      {metrics.error && <Notice tone="bad">Could not load metrics: {metrics.error}</Notice>}
      {!m && !metrics.error ? <Loading>Loading overview…</Loading> : <KpiStrip items={kpis} label="Gateway health" />}

      <Section flush title="Interfaces" meta={ifaces.data ? `${ifaces.data.interfaces.length} listening` : undefined}>
        {ifaces.error ? <div className={styles.pad}><Notice tone="bad">Could not load interfaces: {errorText(ifaces.error)}</Notice></div> : !ifaces.data ? <div className={styles.pad}><Loading /></div> : (
          <div className={styles.typeGrid}>
            <Link className={styles.typeCard} to="/interfaces/sql"><strong>SQL drivers</strong><span>{byKind('sql').map((i) => i.label).join(', ') || 'none listening'}</span><b>{compact(sumReq('sql'))}</b><small>statements</small></Link>
            <Link className={styles.typeCard} to="/interfaces/api"><strong>API endpoints</strong><span>{byKind('api').length} listening{byKind('api').length ? `: ${byKind('api').slice(0, 4).map((i) => i.label).join(', ')}${byKind('api').length > 4 ? '…' : ''}` : ''}</span><b>{compact(sumReq('api'))}</b><small>operations</small></Link>
            <Link className={styles.typeCard} to="/interfaces/mcp"><strong>MCP servers</strong><span>{endpoints.data ? `${endpoints.data.length} endpoint${endpoints.data.length === 1 ? '' : 's'}` : byKind('mcp').map((i) => i.label).join(', ')}</span><b>{compact((m?.mcpTools ?? []).reduce((s, t) => s + t.calls, 0))}</b><small>tool calls</small></Link>
            <Link className={styles.typeCard} to="/infrastructure"><strong>Backends</strong><span>{sets.data ? `${sets.data.sets.length} set${sets.data.sets.length === 1 ? '' : 's'}` : ''}</span><b>{sets.data ? sets.data.backendCount : '—'}</b><small>of {formatLicenseLimit(sets.data?.maxBackends)} licensed</small></Link>
          </div>
        )}
      </Section>

      <div className={styles.twoCol}>
        <Section flush title="Backends" meta={<Link to="/infrastructure">Infrastructure</Link>}>
          {sets.error ? <div className={styles.pad}><Notice tone="bad">Could not list backends: {sets.error}</Notice></div>
            : !sets.data ? <div className={styles.pad}><Loading>Probing backends…</Loading></div>
            : backends.length === 0 ? <EmptyState icon={<Database size={18} aria-hidden="true" />} title="No backends configured">Add a backend to a set on <Link to="/infrastructure">Infrastructure</Link>.</EmptyState>
            : (
              <DataTable caption="Configured backends" minWidth={640}>
                <thead><tr><th>Backend</th><th>Set</th><th>Engine</th><th>Connections</th><th>Calls</th><th>Health</th></tr></thead>
                <tbody>
                  {backends.map((b) => {
                    const stat = m?.byBackend.find((x) => x.backend === b.name)
                    return (
                      <tr key={b.name}>
                        <td><div className={styles.mono}>{b.name}</div><div className={styles.sub}>{targetOf(b.url)}</div></td>
                        <td>{b.set}</td>
                        <td><Tag>{b.type}</Tag></td>
                        <td>{b.pool ? <Meter value={b.pool.active} max={b.pool.max} label={`${b.name} pool`} /> : <span className={styles.sub}>—</span>}</td>
                        <td className={styles.num}>{stat ? `${stat.calls.toLocaleString()} · ${stat.avgMs} ms` : '0'}</td>
                        <td>{b.health ? <span title={b.health.message}><StatusPill tone={b.health.ok ? 'ok' : 'bad'}>{b.health.ok ? `Healthy · ${b.health.tookMs} ms` : 'Unreachable'}</StatusPill></span> : <StatusPill tone="muted">Checking</StatusPill>}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </DataTable>
            )}
        </Section>
        <Section title="Risk" meta={riskLoading ? 'checking' : risks.length === 0 ? 'clear' : `${risks.length} open`}>
          {riskLoading ? <Loading /> : risks.length === 0
            ? <p className={styles.quiet}>No backend is unreachable, no pool is above 80%, every node is reporting{anomalies.data?.enabled ? ' and no traffic anomaly is recorded' : ''}.{anomalies.data && !anomalies.data.enabled && ' Anomaly detection is not enabled on this deployment.'}</p>
            : <ul className={styles.risks}>{risks.map((r, i) => <li key={i}><StatusPill tone={r.tone}>{''}</StatusPill><span>{r.text}</span></li>)}</ul>}
        </Section>
      </div>

      <div className={styles.twoCol}>
        <Section title="Demand by protocol" meta="statements since process start">
          {!m ? <Loading /> : protocols.length === 0 ? <EmptyState title="No traffic yet">Send a query through any wire protocol to see it here.</EmptyState> : (
            <div className={styles.bars}>
              {protocols.slice(0, 10).map(([name, count]) => (
                <div className={styles.barRow} key={name}>
                  <span className={styles.mono}>{name}</span>
                  <div className={styles.barTrack} role="img" aria-label={`${name}: ${count} statements`}><span style={{ width: `${maxProto ? (count / maxProto) * 100 : 0}%` }} /></div>
                  <span className={styles.num}>{count.toLocaleString()}</span>
                </div>
              ))}
            </div>
          )}
        </Section>
        <Section title="Demand by workload class" meta="from /api/usage">
          {!usage.data ? (usage.error ? <Notice tone="warn">{usage.error}</Notice> : <Loading />) : classes.length === 0
            ? <EmptyState title="No classified traffic yet">Statements are attributed to a workload class (App, Analytics, AI, or one a router rule assigns) as they run.</EmptyState>
            : (
              <DataTable caption="Usage by workload class" minWidth={360}>
                <thead><tr><th>Class</th><th>Calls</th><th>Errors</th><th>Avg</th></tr></thead>
                <tbody>{classes.map((c) => <tr key={c.workloadClass}><td>{c.workloadClass}</td><td className={styles.num}>{c.calls.toLocaleString()}</td><td className={styles.num}>{c.errors}</td><td className={styles.num}>{c.avgMs} ms</td></tr>)}</tbody>
              </DataTable>
            )}
        </Section>
      </div>

      {topSql.length > 0 && (
        <Section flush title="Top SQL by cost" meta={<Link to="/traffic">All traffic</Link>}>
          <DataTable caption="Most expensive statements" minWidth={560}>
            <thead><tr><th>SQL</th><th>Calls</th><th>Avg</th><th>Total</th></tr></thead>
            <tbody>
              {topSql.map((s, i) => (
                <tr key={i}><td className={styles.sql}>{s.sql}</td><td className={styles.num}>{compact(s.calls)}</td><td className={styles.num}>{s.avgMs} ms</td><td className={styles.num}>{compact(s.totalMs)} ms</td></tr>
              ))}
            </tbody>
          </DataTable>
        </Section>
      )}
      <MaturityLegend />
    </div>
  )
}
