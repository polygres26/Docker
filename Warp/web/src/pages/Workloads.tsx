import { useCallback, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { RefreshCw } from 'lucide-react'
import {
  type InterfaceInfo,
  getAbRouting, getUsage, getWireConfig, getWireMetrics, listBackendSets, listFirewallRules, listInterfaces,
} from '../api/client'
import {
  Button, DataTable, EmptyState, KpiStrip, Loading, ModeTag, NameCell, Notice, PageHeader, Section, StatusPill, compact, type KpiItem,
} from '../components/ui'
import { useLoad } from '../hooks'
import styles from './interfaces/interfaces.module.css'

const PROTOCOL_OF: Record<string, string> = {
  pgwire: 'postgres', mywire: 'mysql', 'mywire-native': 'mysql', mssqlwire: 'sqlserver', 'mssqlwire-native': 'sqlserver',
  orawire: 'oracle', 'orawire-native': 'oracle', 'orawire-tls': 'oracle', mongowire: 'mongodb', boltwire: 'bolt', grpc: 'grpc', 'grpc-tls': 'grpc',
}

const filled = (s: string | null | undefined) => !!s && s.trim() !== ''

/**
 * Workloads. Warp has no stored "workload" object; a workload here is DERIVED: one frontend (interface) joined with the backend set
 * and backends it reaches and the router / QoS / ACL / firewall / A-B policies that apply to it. Every cell is read from live config;
 * nothing is stored or invented, and the derivation is shown so it can be checked.
 */
export default function Workloads() {
  const ifaces = useLoad(listInterfaces, 15_000)
  const sets = useLoad(useCallback(() => listBackendSets(false), []))
  const config = useLoad(getWireConfig)
  const ab = useLoad(getAbRouting)
  const firewall = useLoad(listFirewallRules)
  const metrics = useLoad(getWireMetrics, 15_000)
  const usage = useLoad(getUsage, 15_000)

  const rows = (ifaces.data?.interfaces ?? []).filter((i) => i.kind !== 'mcp')
  const cfg = config.data
  const aclRules = cfg && filled(cfg.aclRules) ? cfg.aclRules!.split(';').filter((r) => r.trim()).length : 0
  const fwRules = (firewall.data ?? []).filter((r) => r.enabled).length
  const routerConfigured = !!cfg && [cfg.routerSchemaRules, cfg.routerPredicateRules, cfg.routerValueShardRules, cfg.routerShardTables, cfg.routerTableShards].some(filled)
  const qosConfigured = !!cfg && (filled(cfg.qosRatePerSec) || filled(cfg.qosClassLimits))

  const backendsOfSet = useMemo(() => Object.fromEntries((sets.data?.sets ?? []).map((s) => [s.name, s.backends.map((b) => b.name)])), [sets.data])
  const defaultSet = sets.data?.sets.find((s) => s.isDefaultSet)?.name

  function reaches(i: InterfaceInfo): { primary: string; sub?: string } {
    if (i.store) {
      const set = i.set ?? defaultSet ?? 'default'
      return { primary: `Set ${set}`, sub: i.hosts && i.hosts.length > 0 ? `hosted on ${i.hosts.join(', ')}` : 'store not enabled on a backend: served from the default backend' }
    }
    const proto = PROTOCOL_OF[i.id]
    const routes = (sets.data?.connectionRouting.routes ?? []).filter((r) => !r.protocol || r.protocol === proto)
    if (routes.length > 0) return { primary: `${routes.length} connection route${routes.length === 1 ? '' : 's'}`, sub: routes.slice(0, 3).map((r) => `${r.database} → ${r.target}`).join(', ') }
    return { primary: `Set ${defaultSet ?? 'default'}`, sub: `by database name · ${(backendsOfSet[defaultSet ?? 'default'] ?? []).length} backend(s)` }
  }

  const err = ifaces.error ?? sets.error ?? config.error
  const kpis: KpiItem[] = [
    { label: 'Derived workloads', value: rows.length, hint: 'one per listening SQL / API frontend' },
    { label: 'Router', value: config ? (routerConfigured ? 'Rules set' : 'None') : '—', hint: 'statement routing rules' },
    { label: 'QoS', value: config ? (qosConfigured ? 'Limits set' : 'Unlimited') : '—', hint: 'rate limits per class' },
    { label: 'ACL / firewall', value: config ? `${aclRules} / ${fwRules}` : '—', hint: 'ACL rules / enabled SQL firewall rules' },
  ]

  return (
    <div>
      <PageHeader title="Workloads"
        description="A workload is derived, not stored: a frontend joined with the backend set it reaches and the policies that apply to it. Policies are edited on their own pages."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { ifaces.reload(); sets.reload(); config.reload(); ab.reload(); firewall.reload(); metrics.reload(); usage.reload() }}>Refresh</Button>} />
      {err && <Notice tone="bad">Could not load: {err}</Notice>}
      {ifaces.loading ? <Loading /> : <KpiStrip items={kpis} label="Workload figures" />}

      <Section flush title="Workloads" meta={`${rows.length} derived`}>
        {rows.length === 0 && !ifaces.loading ? <EmptyState title="No workloads">No SQL or API frontend is listening on this Warp.</EmptyState> : (
          <DataTable caption="Derived workloads" minWidth={980}>
            <thead><tr><th>Workload</th><th>Mode</th><th>Reaches</th><th>Policies that apply</th><th>Requests</th><th>Status</th></tr></thead>
            <tbody>
              {rows.map((i) => {
                const r = reaches(i)
                const abPolicy = i.store ? ab.data?.policies[i.store] : undefined
                return (
                  <tr key={i.id}>
                    <td><NameCell name={i.label} sub={`${i.protocol} · port ${i.port}`} /></td>
                    <td><ModeTag mode={i.mode} /></td>
                    <td><NameCell name={r.primary} sub={r.sub} /></td>
                    <td>
                      <span className={styles.policyList}>
                        {i.kind === 'sql' && <Chip to="/router" on={routerConfigured} label="Router" />}
                        {i.kind === 'sql' && <Chip to="/qos" on={qosConfigured} label="QoS" />}
                        {i.kind === 'sql' && <Chip to="/firewall" on={fwRules > 0} label={fwRules > 0 ? `Firewall ${fwRules}` : 'Firewall'} />}
                        <Chip to="/acl" on={aclRules > 0} label={aclRules > 0 ? `ACL ${aclRules}` : 'ACL'} />
                        {i.store && <Chip to="/ab-routing" on={!!abPolicy && abPolicy.mode !== 'local'} label={abPolicy ? `A/B ${abPolicy.mode}` : 'A/B'} />}
                      </span>
                    </td>
                    <td className={styles.num}>{i.requests === null ? <span className={styles.sub}>not counted</span> : compact(i.requests)}</td>
                    <td><StatusPill tone="ok">Listening</StatusPill></td>
                  </tr>
                )
              })}
            </tbody>
          </DataTable>
        )}
      </Section>

      <Section flush title="Traffic by workload class" meta="App / Analytics / AI, or a class a router rule assigns">
        {!usage.data ? <div className={styles.pad}>{usage.error ? <Notice tone="warn">{usage.error}</Notice> : <Loading />}</div>
          : usage.data.byWorkloadClass.length === 0 ? <EmptyState title="No classified traffic yet">Statements are attributed to a class as they run.</EmptyState>
          : (
            <DataTable caption="Usage by workload class" minWidth={420}>
              <thead><tr><th>Class</th><th>Calls</th><th>Errors</th><th>Avg latency</th></tr></thead>
              <tbody>{usage.data.byWorkloadClass.map((c) => <tr key={c.workloadClass}><td>{c.workloadClass}</td><td className={styles.num}>{c.calls.toLocaleString()}</td><td className={styles.num}>{c.errors}</td><td className={styles.num}>{c.avgMs} ms</td></tr>)}</tbody>
            </DataTable>
          )}
      </Section>
      <p className={styles.help}>
        Filled chip = that policy is configured. Router, QoS and the SQL firewall apply to the SQL pipeline; ACL applies to every TCP frontend; A/B applies per store.
        See <Link to="/router">Routing &amp; QoS</Link>, <Link to="/firewall">Policies</Link> and <Link to="/acl">Access &amp; ACLs</Link>.
      </p>
    </div>
  )
}

function Chip({ to, on, label }: { to: string; on: boolean; label: string }) {
  return <Link to={to} className={`${styles.chip} ${on ? styles.chipOn : styles.chipOff}`}>{on ? '● ' : '○ '}{label}</Link>
}
