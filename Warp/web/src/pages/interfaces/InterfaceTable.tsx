import { Link } from 'react-router-dom'
import type { InterfaceInfo, WireMetricsSummary } from '../../api/client'
import { maturityFor } from '../../api/maturity'
import { DataTable, EmptyState, MaturityTag, ModeTag, NameCell, StatusPill, compact } from '../../components/ui'
import styles from './interfaces.module.css'

/** Average latency of an interface's protocol from the RTT-outcome table (real samples only; null when none). */
export function latencyFor(m: WireMetricsSummary | null, key: string | null): { avgMs: number; samples: number } | null {
  if (!m || !key) return null
  const rows = m.rttByOutcome.filter((r) => r.protocol === key)
  const calls = rows.reduce((s, r) => s + r.calls, 0)
  if (calls === 0) return null
  return { avgMs: rows.reduce((s, r) => s + r.totalMs, 0) / calls, samples: calls }
}

/** The TLS column: port and convention when served; "TLS not enabled" (or "TLS error", with the reason on hover) when the frontend could
 * serve TLS but is not configured or its certificate failed to load; a dash for frontends that report no TLS state at all. */
export function TlsCell({ i }: { i: InterfaceInfo }) {
  if (i.tlsEnabled === undefined) return <span className={styles.sub} title="This protocol reports no TLS state">n/a</span>
  if (!i.tlsEnabled) {
    return <span className={styles.sub} title={i.tlsError ?? 'Set WARP_TLS_CERT and WARP_TLS_KEY (or WARP_TLS_KEYSTORE) to enable TLS'}>{i.tlsError ? 'TLS error' : 'TLS not enabled'}</span>
  }
  const port = i.httpsPort ?? i.tlsPort
  const how = i.tlsMode === 'in-band' ? 'in-band, same port'
    : i.tlsMode === 'sniff-allow' ? 'same port, plaintext also allowed'
    : i.tlsMode === 'sniff-require' ? 'same port, TLS required'
    : i.tlsMode === 'separate-port' ? 'TLS port' : 'HTTPS'
  return (
    <span title={i.selfSigned ? 'Self-signed development certificate: clients will not trust it' : 'TLS'}>
      <span className={styles.mono}>{port}</span>
      <span className={styles.sub}> {how}{i.tlsClientAuth ? ', client cert' : ''}{i.selfSigned ? ', self-signed' : ''}</span>
    </span>
  )
}

/** Auth column: the method and whether it's actually enforced (joined from GET /api/access-summary),
 * or "not reported" for a frontend AccessSummary doesn't cover -- never guessed. An interface with a
 * real method that isn't enforced (open unless configured) is a genuine warning, not a neutral fact. */
export function AuthCell({ i }: { i: InterfaceInfo }) {
  if (i.authMethod === undefined) return <span className={styles.sub} title="This frontend reports no auth status">not reported</span>
  if (!i.authEnforced) {
    return <span title={i.authDetail}><StatusPill tone="warn">Open</StatusPill></span>
  }
  return <span title={i.authDetail}>{i.authMethod}</span>
}

/** "Listening" alone used to be shown for every row regardless of whether the frontend actually has
 * anywhere real to put/read data -- a store-backed frontend whose store isn't enabled on any
 * backend read exactly as healthy as a fully-configured one. Now warn instead when that's the case;
 * every other status still reads as a plain, healthy "Listening". */
export function StatusCell({ i }: { i: InterfaceInfo }) {
  if (i.status === 'listening_no_store') {
    return <span title="Socket is up, but this store isn't enabled on any backend -- see the Serves column"><StatusPill tone="warn">Listening (no store)</StatusPill></span>
  }
  return <StatusPill tone="ok">Listening</StatusPill>
}

/** Table of frontends: name/protocol, port, mode, serving set/backends, traffic, status. */
export function InterfaceTable({ rows, metrics, emptyTitle, emptyText }: {
  rows: InterfaceInfo[]; metrics: WireMetricsSummary | null; emptyTitle: string; emptyText: string
}) {
  if (rows.length === 0) return <EmptyState title={emptyTitle}>{emptyText}</EmptyState>
  return (
    <DataTable caption="Interfaces" minWidth={960}>
      <thead>
        <tr><th>Interface</th><th>Port</th><th>Auth</th><th>TLS</th><th>Mode</th><th>Maturity</th><th>Serves</th><th>Requests</th><th>Avg latency</th><th>Status</th></tr>
      </thead>
      <tbody>
        {rows.map((i) => {
          const lat = latencyFor(metrics, i.metricsKey)
          return (
            <tr key={i.id}>
              <td><NameCell name={i.label} sub={i.protocol} /></td>
              <td className={styles.mono}>{i.port}</td>
              <td><AuthCell i={i} /></td>
              <td><TlsCell i={i} /></td>
              <td><ModeTag mode={i.mode} /></td>
              <td><MaturityTag maturity={maturityFor(i.metricsKey, i.id)} /></td>
              <td>
                {i.store
                  ? <>
                      <NameCell name={i.set ?? 'default'} sub={i.hosts && i.hosts.length > 0 ? i.hosts.join(', ') : 'store not enabled on a backend: served from the default backend'} />
                    </>
                  : <span className={styles.sub}>Chosen per connection (<Link to="/infrastructure">connection routing</Link>)</span>}
              </td>
              <td className={styles.num}>{i.requests === null ? <span className={styles.sub}>not counted</span> : compact(i.requests)}</td>
              <td className={styles.num}>{lat ? `${lat.avgMs.toFixed(1)} ms` : <span className={styles.sub}>no samples</span>}</td>
              <td><StatusCell i={i} /></td>
            </tr>
          )
        })}
      </tbody>
    </DataTable>
  )
}
