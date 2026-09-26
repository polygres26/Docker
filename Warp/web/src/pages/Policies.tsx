import { useMemo, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { Link } from 'react-router-dom'
import {
  getAbRouting, getWireConfig, listFirewallRules, listInterfaces, listMcpEndpoints, type InterfaceInfo,
} from '../api/client'
import { getPolicyDecisions, getPrometheus, type PolicyDecision } from '../api/insights'
import {
  Button, DataTable, EmptyState, KpiStrip, Loading, Notice, PageHeader, Section, StatusPill, Tabs, Tag, type KpiItem, type Tone,
} from '../components/ui'
import { useLoad } from '../hooks'
import styles from './Policies.module.css'

const filled = (s: string | null | undefined) => !!s && s.trim() !== ''

interface Control {
  id: string
  name: string
  type: string
  active: boolean
  /** Text of the status pill. */
  status: string
  tone: Tone
  summary: string
  facts: Array<[string, React.ReactNode]>
  applies: string
  editor: { to: string; label: string }
}

const list = (xs: string[], max = 4) => (xs.length === 0 ? 'none' : xs.slice(0, max).join(', ') + (xs.length > max ? ` and ${xs.length - max} more` : ''))

/**
 * Policies. Warp has no reusable "policy" object, so this page is a governed-controls view over the real policy systems:
 * each card states what the control is, where it applies, its current state read from live configuration, and links to the
 * page that edits it. The decisions table merges the in-memory firewall / ACL denial feed with the audit stream
 * (GET /api/policy-decisions). Firewall and ACL only record blocks; allowed statements are not logged individually.
 */
export default function Policies() {
  const ifaces = useLoad(listInterfaces, 30_000)
  const config = useLoad(getWireConfig, 30_000)
  const firewall = useLoad(listFirewallRules, 30_000)
  const ab = useLoad(getAbRouting, 30_000)
  const endpoints = useLoad(listMcpEndpoints, 30_000)
  const decisions = useLoad(() => getPolicyDecisions(200), 10_000)
  const prom = useLoad(getPrometheus, 30_000)
  const [statusFilter, setStatusFilter] = useState('all')
  const [decisionFilter, setDecisionFilter] = useState('all')

  const cfg = config.data
  const ifs = ifaces.data?.interfaces ?? []
  const sql = ifs.filter((i) => i.kind === 'sql')
  const labels = (xs: InterfaceInfo[]) => list(xs.map((i) => i.label))

  const controls: Control[] = useMemo(() => {
    const out: Control[] = []
    if (firewall.data) {
      const on = firewall.data.filter((r) => r.enabled)
      const deny = on.filter((r) => r.action === 'deny').length
      out.push({
        id: 'firewall', name: 'SQL firewall', type: 'SQL', active: on.length > 0, status: on.length ? `${on.length} rule${on.length === 1 ? '' : 's'} active` : 'Guard only', tone: on.length ? 'ok' : 'muted',
        summary: 'Blocks or allows SQL statements by type, table and text pattern, checked first-match by priority before any statement reaches a backend.',
        facts: [['Rules', `${on.length} enabled (${deny} deny, ${on.length - deny} allow) of ${firewall.data.length}`], ['Always on', 'stacked-query injection guard']],
        applies: sql.length ? `SQL drivers: ${labels(sql)}` : 'SQL drivers', editor: { to: '/firewall', label: 'Edit SQL firewall' },
      })
    }
    if (cfg) {
      const kinds = ([['schema', cfg.routerSchemaRules], ['predicate', cfg.routerPredicateRules], ['value shard', cfg.routerValueShardRules], ['shard tables', cfg.routerShardTables], ['table shards', cfg.routerTableShards]] as const).filter(([, v]) => filled(v)).map(([k]) => k)
      out.push({
        id: 'router', name: 'Router rules', type: 'Routing', active: kinds.length > 0, status: kinds.length ? 'Configured' : 'Not configured', tone: kinds.length ? 'ok' : 'muted',
        summary: 'Sends statements to a backend or shard and classifies them into workload classes by schema, predicate or shard key.',
        facts: [['Rule kinds set', kinds.length ? <span className={styles.tags}>{kinds.map((k) => <Tag key={k}>{k}</Tag>)}</span> : 'none']],
        applies: sql.length ? `SQL drivers: ${labels(sql)}` : 'SQL drivers', editor: { to: '/router', label: 'Edit router rules' },
      })
      const classes = (cfg.qosClassLimits ?? '').split(',').map((x) => x.trim()).filter(Boolean)
      const qosOn = filled(cfg.qosRatePerSec) || classes.length > 0
      const rejected = (prom.data ?? []).filter((s) => s.name === 'warp_qos_rejected_total').reduce((a, s) => a + s.value, 0)
      out.push({
        id: 'qos', name: 'QoS rate limits', type: 'Traffic', active: qosOn, status: qosOn ? 'Limiting' : 'Unlimited', tone: qosOn ? 'ok' : 'muted',
        summary: 'Token-bucket rate limits per workload class, with a pool-saturation guard, so one workload cannot starve another.',
        facts: [
          ['Default', filled(cfg.qosRatePerSec) ? `${cfg.qosRatePerSec}/s, burst ${cfg.qosBurst ?? 'default'}` : 'not limited'],
          ['Class limits', classes.length ? list(classes.map((c) => c.split(':')[0]), 5) : 'none'],
          ...(prom.data ? [['Rejected so far', rejected.toLocaleString()] as [string, string]] : []),
        ],
        applies: sql.length ? `SQL drivers: ${labels(sql)}` : 'SQL drivers', editor: { to: '/qos', label: 'Edit QoS' },
      })
      const acl = (cfg.aclRules ?? '').split(';').map((x) => x.trim()).filter(Boolean)
      out.push({
        id: 'acl', name: 'Network ACL', type: 'Access', active: acl.length > 0, status: acl.length ? `${acl.length} rule${acl.length === 1 ? '' : 's'}` : 'Open to all', tone: acl.length ? 'ok' : 'warn',
        summary: 'IP and CIDR allow / reject rules, first match wins, checked when a connection arrives. No rules means every client is allowed.',
        facts: [['Rules', acl.length ? list(acl, 3) : 'none'], ['PROXY protocol v2', cfg.aclPpv2Enabled === 'true' ? 'on' : 'off'], ['Trusted proxies', filled(cfg.aclTrustedProxies) ? cfg.aclTrustedProxies! : 'none']],
        applies: `Every TCP frontend and the admin API (${ifs.length} listener${ifs.length === 1 ? '' : 's'})`, editor: { to: '/acl', label: 'Edit ACL' },
      })
      const sso = filled(cfg.oauthIssuer)
      out.push({
        id: 'oauth', name: 'OIDC authentication', type: 'Access', active: sso, status: sso ? 'Enabled' : 'Token only', tone: sso ? 'ok' : 'muted',
        summary: 'Verifies JWTs from an OIDC issuer and maps a roles claim to admin or viewer access. The shared admin token keeps working alongside it.',
        facts: [['Issuer', sso ? cfg.oauthIssuer! : 'not configured'], ['Audience', filled(cfg.oauthAudience) ? cfg.oauthAudience! : 'any'], ['Roles claim', filled(cfg.oauthRolesClaim) ? cfg.oauthRolesClaim! : 'default']],
        applies: 'Admin API and HTTP frontends', editor: { to: '/oauth', label: 'Edit OAuth' },
      })
    }
    if (endpoints.data) {
      const active = endpoints.data.filter((e) => e.status === 'active')
      const expired = endpoints.data.length - active.length
      const ro = decisions.data?.mcpReadOnly
      out.push({
        id: 'mcp', name: 'MCP scope', type: 'MCP', active: endpoints.data.length > 0, status: endpoints.data.length ? `${active.length} endpoint${active.length === 1 ? '' : 's'}` : 'No endpoints', tone: endpoints.data.length ? 'ok' : 'muted',
        summary: 'Each MCP endpoint token is scoped to everything, one backend set or one backend, and expires. Scope narrows which tools an agent sees.',
        facts: [
          ['Scopes', endpoints.data.length ? list([...new Set(endpoints.data.map((e) => e.scope))], 4) : 'none'],
          ['Expired', expired ? `${expired} (revoke to clean up)` : 'none'],
          ...(ro === undefined ? [] : [['Read-only mode', ro ? 'on: write tools hidden' : 'off'] as [string, string]]),
        ],
        applies: 'MCP servers', editor: { to: '/interfaces/mcp', label: 'Manage MCP endpoints' },
      })
    }
    if (ab.data) {
      const ps = Object.values(ab.data.policies)
      const active = ps.filter((p) => p.mode !== 'local')
      const killed = ab.data.killSwitch || Object.keys(ab.data.storeKill).length > 0
      out.push({
        id: 'ab', name: 'A/B routing', type: 'Migration', active: active.length > 0, status: killed ? 'Kill switch set' : active.length ? `${active.length} store${active.length === 1 ? '' : 's'} routed` : 'All local', tone: killed ? 'warn' : active.length ? 'ok' : 'muted',
        summary: 'Per-store policy that sends API traffic to the local emulation, a real cloud service, a split, or compares both.',
        facts: [['Policies', active.length ? list(active.map((p) => `${p.store} (${p.mode})`), 4) : 'none beyond local'], ['Kill switch', ab.data.killSwitch ? `${ab.data.killSwitch.side} side` : Object.keys(ab.data.storeKill).length ? `${Object.keys(ab.data.storeKill).length} store` : 'not set']],
        applies: active.length ? `Stores: ${list(active.map((p) => p.store), 5)}` : 'Store-backed API frontends', editor: { to: '/ab-routing', label: 'Edit A/B routing' },
      })
    }
    return out
  }, [firewall.data, cfg, endpoints.data, ab.data, decisions.data, prom.data, sql, ifs.length])

  const shown = controls.filter((c) => statusFilter === 'all' || (statusFilter === 'active' ? c.active : !c.active))
  const errors = [firewall.error, config.error, endpoints.error, ab.error].filter(Boolean)
  const rows: PolicyDecision[] = (decisions.data?.decisions ?? []).filter((d) => decisionFilter === 'all' || (decisionFilter === 'block' ? d.decision === 'Block' : d.decision !== 'Block'))
  const blocked = (decisions.data?.decisions ?? []).filter((d) => d.decision === 'Block').length

  const kpis: KpiItem[] = [
    { label: 'Controls configured', value: controls.length ? `${controls.filter((c) => c.active).length} of ${controls.length}` : '—', hint: 'policy systems with a live setting' },
    { label: 'Firewall rules', value: firewall.data ? firewall.data.filter((r) => r.enabled).length : '—', hint: firewall.data ? `${firewall.data.length} defined` : undefined },
    { label: 'Blocks recorded', value: decisions.data ? decisions.data.policyLogRecorded.toLocaleString() : '—', hint: 'firewall + ACL, since this node started' },
    { label: 'Recent decisions', value: decisions.data ? decisions.data.decisions.length : '—', hint: decisions.data ? `${blocked} blocked in the feed` : undefined },
  ]

  return (
    <div>
      <PageHeader title="Policies" description="The governed controls that apply to your interfaces: what each one is, where it applies, its current state, and where to change it."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { ifaces.reload(); config.reload(); firewall.reload(); ab.reload(); endpoints.reload(); decisions.reload(); prom.reload() }}>Refresh</Button>} />
      {errors.length > 0 && <Notice tone="bad">Some controls could not be loaded: {errors.join('; ')}</Notice>}
      <KpiStrip items={kpis} label="Policy figures" />

      <div className={styles.toolbar}>
        <Tabs label="Control status" value={statusFilter} onChange={setStatusFilter} tabs={[{ id: 'all', label: 'All controls', count: controls.length }, { id: 'active', label: 'Configured', count: controls.filter((c) => c.active).length }, { id: 'off', label: 'Not configured', count: controls.filter((c) => !c.active).length }]} />
      </div>
      {controls.length === 0 && !errors.length ? <Loading>Reading policy configuration…</Loading> : shown.length === 0 ? <EmptyState title="Nothing in this view">No control matches the selected status.</EmptyState> : (
        <div className={styles.grid}>
          {shown.map((c) => (
            <article className={styles.card} key={c.id} aria-label={c.name}>
              <div className={styles.top}><strong>{c.name}</strong><StatusPill tone={c.tone}>{c.status}</StatusPill></div>
              <p>{c.summary}</p>
              <ul className={styles.facts}>{c.facts.map(([k, v]) => <li key={k}><span className={styles.sub}>{k}</span> <b>{v}</b></li>)}</ul>
              <div className={styles.foot}><span>{c.type} · {c.applies}</span><Link to={c.editor.to}>{c.editor.label}</Link></div>
            </article>
          ))}
        </div>
      )}

      <Section flush title="Recent policy decisions" meta={decisions.data ? `${rows.length} shown · newest first` : undefined}>
        <div className={styles.pad} style={{ paddingBottom: 0 }}>
          <Tabs label="Decision filter" value={decisionFilter} onChange={setDecisionFilter} tabs={[{ id: 'all', label: 'All' }, { id: 'block', label: 'Blocked' }, { id: 'other', label: 'Allowed and other' }]} />
        </div>
        {decisions.error ? <div className={styles.pad}><Notice tone="bad">{decisions.error}</Notice></div> : !decisions.data ? <div className={styles.pad}><Loading /></div>
          : rows.length === 0 ? <EmptyState title="No decisions recorded">Blocked statements (SQL firewall), rejected connections (network ACL), access-control decisions, failed logins and MCP tool calls appear here as they happen. {!decisions.data.auditAvailable && 'The audit log is not enabled on this deployment.'}</EmptyState> : (
            <DataTable caption="Recent policy decisions" minWidth={880}>
              <thead><tr><th>Time</th><th>Policy</th><th>Identity</th><th>Target</th><th>Decision</th><th>Reason</th></tr></thead>
              <tbody>
                {rows.slice(0, 100).map((d, i) => (
                  <tr key={`${d.timestamp}-${i}`}>
                    <td className={styles.num}><time dateTime={d.timestamp}>{new Date(d.timestamp).toLocaleTimeString()}</time></td>
                    <td>{d.policy}</td>
                    <td className={styles.mono}>{d.identity ?? '—'}</td>
                    <td className={styles.target}>{d.target ?? '—'}</td>
                    <td><StatusPill tone={d.decision === 'Block' ? 'bad' : d.decision === 'Error' ? 'warn' : 'ok'}>{d.decision}</StatusPill></td>
                    <td className={styles.reason}>{d.reason ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </DataTable>
          )}
      </Section>
      <p className={styles.note}>The SQL firewall and network ACL record blocks only; allowed statements and connections are not logged one by one. The feed is held in memory on this node (200 entries) and, for access control, logins and MCP calls, in the audit log.</p>
    </div>
  )
}
