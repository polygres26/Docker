import { Link } from 'react-router-dom'
import { getWireMetrics, listBackendSets, listInterfaces } from '../../api/client'
import { KpiStrip, Loading, Notice, PageHeader, Section, Button, DataTable, EmptyState, compact, type KpiItem } from '../../components/ui'
import { useLoad } from '../../hooks'
import { InterfaceTable } from './InterfaceTable'
import { RefreshCw } from 'lucide-react'
import styles from './interfaces.module.css'

const POLL_MS = 10_000

/** SQL drivers: the relational wire frontends this Warp serves (PostgreSQL, MySQL, SQL Server, Oracle, gRPC),
 * from GET /api/interfaces, with the connection-routing table that decides which set/backend a database name reaches. */
export default function SqlDrivers() {
  const ifaces = useLoad(listInterfaces, POLL_MS)
  const metrics = useLoad(getWireMetrics, POLL_MS)
  const sets = useLoad(() => listBackendSets(false))
  const rows = (ifaces.data?.interfaces ?? []).filter((i) => i.kind === 'sql')
  const total = rows.reduce((s, r) => s + (r.requests ?? 0), 0)
  const routing = sets.data?.connectionRouting

  const kpis: KpiItem[] = [
    { label: 'Listening drivers', value: rows.length, hint: rows.length ? rows.map((r) => r.label).join(', ') : 'none' },
    { label: 'Statements served', value: compact(total), hint: 'since process start' },
    { label: 'Active sessions', value: ifaces.data?.activeSessions ?? '—', hint: 'all wire protocols' },
    { label: 'Connect routing', value: routing?.mode ?? '—', hint: routing ? `${routing.routes.length} explicit route${routing.routes.length === 1 ? '' : 's'}` : undefined },
  ]
  return (
    <div>
      <PageHeader title="SQL drivers" description="Relational protocols Warp speaks to clients. Relay forwards the native protocol to a same-engine backend; Adapt translates the client's dialect."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { ifaces.reload(); metrics.reload() }}>Refresh</Button>} />
      {ifaces.error && <Notice tone="bad">Could not load interfaces: {ifaces.error}</Notice>}
      {ifaces.loading ? <Loading>Loading drivers…</Loading> : <KpiStrip items={kpis} label="SQL driver figures" />}
      <Section flush title="Drivers" meta={`${rows.length} listening`}>
        <InterfaceTable rows={rows} metrics={metrics.data} emptyTitle="No SQL driver is listening" emptyText="No relational frontend has bound a port on this Warp instance." />
      </Section>
      <Section flush title="Connection routing" meta={<Link to="/infrastructure">Edit on Infrastructure</Link>}>
        {!routing ? <div className={styles.pad}><Loading /></div>
          : routing.routes.length === 0 ? <EmptyState title="No explicit routes">The database name a client sends selects a backend set or backend directly; no explicit route overrides it.</EmptyState>
          : (
            <DataTable caption="Connection routes" minWidth={560}>
              <thead><tr><th>Database</th><th>Protocol</th><th>User</th><th>Target</th></tr></thead>
              <tbody>
                {routing.routes.map((r) => (
                  <tr key={r.id}><td className={styles.mono}>{r.database}</td><td>{r.protocol ?? 'any'}</td><td className={styles.mono}>{r.user ?? 'any'}</td>
                    <td className={styles.mono}>{r.target} <span className={styles.sub}>{r.targetKind}</span></td></tr>
                ))}
              </tbody>
            </DataTable>
          )}
      </Section>
    </div>
  )
}
