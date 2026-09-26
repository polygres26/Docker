import { useMemo } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { RefreshCw } from 'lucide-react'
import { getWireConfig, listInterfaces, listMcpEndpoints, type InterfaceInfo, type WireConfig } from '../../api/client'
import { getAccessSummary, getAudit, type AccessFrontend, type AuditEvent } from '../../api/wave2b'
import {
  Button, DataTable, EmptyState, KpiStrip, Loading, NameCell, Notice, PageHeader, Section, StatusPill, Tag, type KpiItem, type Tone,
} from '../../components/ui'
import { useLoad } from '../../hooks'
import styles from './wave2b.module.css'

const POLL_MS = 15_000
const DENIAL_TYPES = ['ACCESS_DENIED', 'DB_LOGIN_FAILED']
const AUDIT_LIMIT = 500
const loadAudit = () => getAudit(AUDIT_LIMIT)
const DAY_MS = 24 * 3600 * 1000

/** Frontends handed the OAuth resolver in Main (bearer JWT accepted when an issuer is configured). */
const OAUTH_IDS = new Set(['dynamowire', 'oswire', 'influxwire', 'mcp', 'a2a'])

const METHOD_LABEL: Record<string, string> = {
  'postgres-roles': 'Postgres roles (SCRAM)', 'sql-credentials': 'Database credentials', 'sasl-plain': 'SASL PLAIN', password: 'Password (AUTH)',
  'bearer-token': 'Static bearer tokens', 'aws-sigv4': 'AWS Signature V4', 'cosmos-master-key': 'Cosmos key / token', 'token-or-basic': 'Token or basic auth',
  'shared-key': 'Azure shared key', oauth: 'OAuth 2.0 / OIDC',
}
const label = (m: string) => METHOD_LABEL[m] ?? m

const filled = (s: string | null | undefined) => !!s && s.trim() !== ''

/** Access-key ids from `id=secret;id=secret`. Only the part before '=' is kept: the secret is dropped at once and never held in state. */
function accessKeyIds(spec: string | null | undefined): string[] {
  return (spec ?? '').split(';').map((e) => e.split('=')[0].trim()).filter(Boolean)
}

interface Coverage { iface: InterfaceInfo; methods: Array<{ method: string; enforced: boolean; detail: string }>; status: 'enforced' | 'open' | 'unreported' }

/**
 * Access & ACLs. Effective access rules are the real IP/CIDR ACL (first match wins, implicit deny once any rule exists); authentication
 * coverage joins /api/interfaces with /api/access-summary (env-derived, secrets never returned) and the live OAuth / SigV4 config;
 * denied activity is the audit stream (ACCESS_DENIED, DB_LOGIN_FAILED). Nothing is invented: what Warp does not record is said so.
 */
export default function AccessAcls() {
  const view = useLocation().pathname.replace(/^\/access\/?/, '') || 'rules'
  const config = useLoad(getWireConfig, POLL_MS)
  const ifaces = useLoad(listInterfaces, POLL_MS)
  const summary = useLoad(getAccessSummary, 60_000)
  const audit = useLoad(loadAudit, POLL_MS)
  const endpoints = useLoad(listMcpEndpoints, 60_000)

  const cfg: WireConfig | null = config.data
  const aclRules = useMemo(() => (cfg?.aclRules ?? '').split(';').map((r) => r.trim()).filter(Boolean).map((r) => {
    const i = r.indexOf(':')
    return { action: r.slice(0, i).toLowerCase(), cidr: r.slice(i + 1).trim() }
  }), [cfg])
  const oauthOn = filled(cfg?.oauthIssuer)
  const iamKeys = useMemo(() => accessKeyIds(cfg?.awsIamCredentials), [cfg])

  const coverage: Coverage[] = useMemo(() => (ifaces.data?.interfaces ?? []).map((iface) => {
    const methods: Coverage['methods'] = []
    const f: AccessFrontend | undefined = summary.data?.frontends.find((x) => x.id === iface.id)
    if (f) methods.push({ method: f.method, enforced: f.enforced, detail: f.detail })
    if (iface.id === 'dynamowire') methods.push({ method: 'aws-sigv4', enforced: iamKeys.length > 0, detail: iamKeys.length > 0 ? `${iamKeys.length} access key(s) in config` : 'no AWS IAM credentials configured: requests are not signature-checked' })
    if (OAUTH_IDS.has(iface.id)) methods.push({ method: 'oauth', enforced: oauthOn, detail: oauthOn ? 'bearer JWT verified against the issuer' : 'no issuer configured' })
    const status: Coverage['status'] = methods.length === 0 ? 'unreported' : methods.some((m) => m.enforced) ? 'enforced' : 'open'
    return { iface, methods, status }
  }), [ifaces.data, summary.data, iamKeys, oauthOn])

  const denials: AuditEvent[] = useMemo(() => (audit.data ?? []).filter((e) => DENIAL_TYPES.includes(e.type)), [audit.data])
  const now = Date.now()
  const denied24 = denials.filter((e) => now - Date.parse(e.timestamp) <= DAY_MS).length
  const auditFull = (audit.data?.length ?? 0) >= AUDIT_LIMIT
  const auditOff = audit.error && audit.error.includes('no such route')

  const enforced = coverage.filter((c) => c.status === 'enforced').length
  const open = coverage.filter((c) => c.status === 'open').length
  const unreported = coverage.filter((c) => c.status === 'unreported').length

  // Identities: named principals Warp can list without exposing a secret.
  const identities = useMemo(() => {
    const out: Array<{ key: string; name: string; kind: string; source: string; usedBy: string }> = []
    const sqlIfaces = coverage.filter((c) => c.methods.some((m) => m.method === 'sql-credentials' || m.method === 'postgres-roles')).map((c) => c.iface.label)
    if (summary.data && summary.data.authMode !== 'postgres_roles') {
      for (const u of summary.data.sqlCredentials.users) out.push({ key: `sql:${u}`, name: u, kind: 'SQL login', source: summary.data.sqlCredentials.mode === 'multi-user' ? 'WARP_AUTH_CREDENTIALS' : 'shared default login (WARP_AUTH_USER)', usedBy: sqlIfaces.join(', ') })
    }
    for (const k of iamKeys) out.push({ key: `iam:${k}`, name: k, kind: 'AWS access key', source: 'AWS IAM credentials (config)', usedBy: 'DynamoDB' })
    for (const f of summary.data?.frontends ?? []) {
      if (f.id === 's3wire') for (const k of f.principals ?? []) out.push({ key: `s3:${k}`, name: k, kind: 'AWS access key', source: 'WARP_S3WIRE_CREDENTIALS', usedBy: 'Amazon S3' })
      if (f.id === 'azblobwire') for (const k of f.principals ?? []) out.push({ key: `az:${k}`, name: k, kind: 'Storage account', source: 'WARP_AZURE_ACCOUNTS', usedBy: 'Azure Storage' })
    }
    for (const e of endpoints.data ?? []) out.push({ key: `mcp:${e.id}`, name: e.name, kind: 'MCP endpoint token', source: `scope ${e.scope}${e.expiresAt ? `, expires ${new Date(e.expiresAt).toLocaleDateString()}` : ', no expiry'}${e.status === 'expired' ? ' (expired)' : ''}`, usedBy: 'MCP servers' })
    const seen = new Map<string, number>()
    for (const e of audit.data ?? []) if (e.userId && e.userId !== 'anonymous' && e.type !== 'DB_LOGIN_FAILED') seen.set(e.userId, (seen.get(e.userId) ?? 0) + 1)
    for (const [u, n] of seen) if (!out.some((o) => o.name === u && o.kind === 'SQL login')) out.push({ key: `audit:${u}`, name: u, kind: 'Seen in audit log', source: `${n} recent audit event(s)`, usedBy: '—' })
    // The same access key can be configured for several frontends: one row, sources and users merged.
    const merged = new Map<string, (typeof out)[number]>()
    for (const o of out) {
      const k = `${o.kind}:${o.name}`
      const m = merged.get(k)
      if (!m) merged.set(k, { ...o })
      else { if (!m.source.includes(o.source)) m.source += `, ${o.source}`; if (!m.usedBy.includes(o.usedBy)) m.usedBy += `, ${o.usedBy}` }
    }
    return [...merged.values()]
  }, [summary.data, coverage, iamKeys, endpoints.data, audit.data])

  const kpis: KpiItem[] = [
    { label: 'Named identities', value: config.data && summary.data ? identities.length : '—', hint: 'SQL logins, access keys, MCP tokens, seen users' },
    { label: 'ACL rules', value: cfg ? aclRules.length : '—', hint: aclRules.length > 0 ? 'first match wins, then deny' : 'none: every client allowed' },
    { label: 'Denied (in audit window)', value: audit.data ? `${auditFull ? `${denied24}+` : denied24}` : auditOff ? 'off' : '—', hint: auditOff ? 'audit log not enabled' : 'login failures and access denials, last 24h' },
    { label: 'Auth enforced', value: ifaces.data && summary.data ? `${enforced} / ${coverage.length}` : '—', hint: ifaces.data ? `${open} open · ${unreported} not reported` : undefined },
  ]
  const loadErr = config.error ?? ifaces.error ?? summary.error

  return (
    <div>
      <PageHeader title="Access & ACLs" description="Network access rules, authentication coverage across every interface, and what was denied. Rules are edited on the ACL and OAuth tabs."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { config.reload(); ifaces.reload(); summary.reload(); audit.reload(); endpoints.reload() }}>Refresh</Button>} />
      {loadErr && <Notice tone="bad">Could not load: {loadErr}</Notice>}
      {config.loading && ifaces.loading ? <Loading /> : <KpiStrip items={kpis} label="Access figures" />}

      {view === 'rules' && (
        <>
          <Section flush title="Effective access rules" meta="IP / CIDR ACL, in order · first match wins · applies to every TCP frontend and HTTP surface">
            {!cfg ? <div className={styles.pad}><Loading /></div> : (
              <DataTable caption="Access rules" minWidth={820}>
                <thead><tr><th>Order</th><th>Rule</th><th>Network</th><th>Scope</th><th>Permission</th><th>Conditions</th><th>Status</th></tr></thead>
                <tbody>
                  {aclRules.map((r, i) => (
                    <tr key={`${i}:${r.cidr}`}>
                      <td className={styles.num}>{i + 1}</td>
                      <td><NameCell name={`${r.action} ${r.cidr}`} sub="ClientAcl rule" /></td>
                      <td className={styles.mono}>{r.cidr}</td>
                      <td>All frontends</td>
                      <td>{r.action === 'allow' ? <StatusPill tone="ok">Allow</StatusPill> : <StatusPill tone="bad">Reject</StatusPill>}</td>
                      <td>{cfg.aclPpv2Enabled === 'true' ? `Client address from PROXY v2${filled(cfg.aclTrustedProxies) ? ` via ${cfg.aclTrustedProxies}` : ''}` : 'Peer address'}</td>
                      <td><StatusPill tone="ok">Active</StatusPill></td>
                    </tr>
                  ))}
                  <tr>
                    <td className={styles.num}>{aclRules.length > 0 ? 'last' : '—'}</td>
                    <td><NameCell name={aclRules.length > 0 ? 'Default deny' : 'No rules'} sub="built in" /></td>
                    <td className={styles.mono}>*</td>
                    <td>All frontends</td>
                    <td>{aclRules.length > 0 ? <StatusPill tone="bad">Reject</StatusPill> : <StatusPill tone="warn">Allow</StatusPill>}</td>
                    <td>{aclRules.length > 0 ? 'No earlier rule matched' : 'No ACL configured'}</td>
                    <td><StatusPill tone="ok">Active</StatusPill></td>
                  </tr>
                </tbody>
              </DataTable>
            )}
            <div className={styles.pad}><span className={styles.sub}>Edit on the <Link to="/acl">ACL</Link> tab. Warp has one network ACL, not per-workload or per-role rules; role and scope limits live in MCP endpoint scopes and OAuth roles.</span></div>
          </Section>
          <Section flush title="Scoped access tokens" meta="MCP endpoints: each token is limited to all backends, one backend set or one backend">
            {(endpoints.data ?? []).length === 0
              ? <EmptyState title="No MCP endpoints">Create scoped endpoints on MCP servers.</EmptyState>
              : (
                <DataTable caption="MCP endpoint scopes" minWidth={640}>
                  <thead><tr><th>Endpoint</th><th>Scope</th><th>Created by</th><th>Expires</th><th>Status</th></tr></thead>
                  <tbody>
                    {(endpoints.data ?? []).map((e) => (
                      <tr key={e.id}>
                        <td><NameCell name={e.name} sub={e.description ?? undefined} /></td>
                        <td className={styles.mono}>{e.scope}</td>
                        <td>{e.createdBy ?? '—'}</td>
                        <td>{e.expiresAt ? new Date(e.expiresAt).toLocaleString() : 'never'}</td>
                        <td><StatusPill tone={e.status === 'active' ? 'ok' : 'muted'}>{e.status === 'active' ? 'Active' : 'Expired'}</StatusPill></td>
                      </tr>
                    ))}
                  </tbody>
                </DataTable>
              )}
          </Section>
        </>
      )}

      {view === 'identities' && (
        <Section flush title="Identities" meta="Read-only: names only, never secrets · Warp has no identity store of its own">
          {identities.length === 0 ? <EmptyState title="No named identities">Set WARP_AUTH_CREDENTIALS, AWS IAM credentials or create MCP endpoints to see them here.</EmptyState> : (
            <DataTable caption="Identities" minWidth={720}>
              <thead><tr><th>Identity</th><th>Kind</th><th>Source</th><th>Used by</th></tr></thead>
              <tbody>
                {identities.map((i) => (
                  <tr key={i.key}><td className={styles.mono}>{i.name}</td><td><Tag>{i.kind}</Tag></td><td>{i.source}</td><td>{i.usedBy}</td></tr>
                ))}
              </tbody>
            </DataTable>
          )}
          <div className={styles.pad}><span className={styles.sub}>{summary.data?.authMode === 'postgres_roles' ? 'SQL logins are Postgres roles (WARP_AUTH_MODE=postgres_roles): they live in the backend Postgres, not in Warp. ' : ''}A Roles tab is omitted: Warp stores no role definitions. Roles come from Postgres (postgres_roles mode) or from the OAuth roles claim{filled(cfg?.oauthRolesClaim) ? ` (${cfg?.oauthRolesClaim})` : ''}.</span></div>
        </Section>
      )}

      {view === 'authentication' && (
        <>
          <Section flush title="Authentication methods" meta="Endpoint coverage from the listening interfaces">
            {ifaces.loading || summary.loading ? <div className={styles.pad}><Loading /></div> : (
              <div className={styles.rowList}>
                {methodGroups(coverage).map((g) => (
                  <div className={styles.rowItem} key={g.method}>
                    <div><strong>{label(g.method)}</strong><span className={styles.s}>{g.names.join(', ')}</span></div>
                    <div className={styles.rowEnd}><StatusPill tone={g.enforcedCount > 0 ? 'ok' : 'warn'}>{g.enforcedCount} of {g.names.length} enforced</StatusPill></div>
                  </div>
                ))}
                <div className={styles.rowItem}>
                  <div><strong>Mutual TLS</strong><span className={styles.s}>Warp does not verify client certificates. Server TLS: {summary.data?.tls.serverKeystoreConfigured ? 'keystore configured (TCPS and gRPC TLS listeners)' : 'no keystore configured'}.</span></div>
                  <div className={styles.rowEnd}><StatusPill tone="muted">Not offered</StatusPill></div>
                </div>
              </div>
            )}
          </Section>
          <Section flush title="Coverage by interface" meta={`${enforced} enforced · ${open} open · ${unreported} not reported`}>
            <DataTable caption="Authentication per interface" minWidth={860}>
              <thead><tr><th>Interface</th><th>Endpoint</th><th>Method</th><th>State</th><th>Detail</th></tr></thead>
              <tbody>
                {coverage.map((c) => (
                  <tr key={c.iface.id}>
                    <td><NameCell name={c.iface.label} sub={c.iface.kind} /></td>
                    <td className={styles.mono}>{c.iface.protocol} :{c.iface.port}</td>
                    <td>{c.methods.length > 0 ? c.methods.map((m) => label(m.method)).join(' + ') : '—'}</td>
                    <td><StatusPill tone={statusTone(c.status)}>{c.status === 'enforced' ? 'Enforced' : c.status === 'open' ? 'Open' : 'Not reported'}</StatusPill></td>
                    <td className={styles.wrapCell}>{c.methods.length > 0 ? c.methods.map((m) => m.detail).join('; ') : 'Warp does not report how this frontend authenticates.'}</td>
                  </tr>
                ))}
              </tbody>
            </DataTable>
            <div className={styles.pad}><span className={styles.sub}>OAuth / OIDC is edited on the <Link to="/oauth">OAuth</Link> tab{oauthOn ? ` (issuer ${cfg?.oauthIssuer})` : ' (currently disabled)'}.</span></div>
          </Section>
        </>
      )}

      {view === 'denied' && (
        <Section flush title="Denied activity" meta={audit.data ? `${denials.length} in the last ${audit.data.length} audit events${auditFull ? ' (window is full, older events are not shown)' : ''}` : undefined}>
          {auditOff ? <EmptyState title="Audit log not available">This Warp does not expose /api/audit.</EmptyState>
            : audit.error ? <div className={styles.pad}><Notice tone="bad">{audit.error}</Notice></div>
            : denials.length === 0 && !audit.loading ? <EmptyState title="No denials in the audit stream">Failed database logins and column / row access denials appear here.</EmptyState>
            : (
              <DataTable caption="Denied activity" minWidth={720}>
                <thead><tr><th>When</th><th>Type</th><th>Who</th><th>What</th></tr></thead>
                <tbody>
                  {denials.map((e, i) => (
                    <tr key={`${e.timestamp}-${i}`}>
                      <td style={{ whiteSpace: 'nowrap' }}>{new Date(e.timestamp).toLocaleString()}</td>
                      <td><StatusPill tone={e.type === 'DB_LOGIN_FAILED' ? 'bad' : 'warn'}>{e.type === 'DB_LOGIN_FAILED' ? 'Login failed' : 'Access denied'}</StatusPill></td>
                      <td className={styles.mono}>{e.userId ?? '—'}</td>
                      <td className={styles.wrapCell}>{e.summary}{Object.keys(e.details).length > 0 && <div className={styles.sub}>{Object.entries(e.details).map(([k, v]) => `${k}=${v}`).join(' · ')}</div>}</td>
                    </tr>
                  ))}
                </tbody>
              </DataTable>
            )}
          <div className={styles.pad}><span className={styles.sub}>Connections rejected by the network ACL are written to the server log only; they are not audited or counted, so they cannot appear here.</span></div>
        </Section>
      )}
    </div>
  )
}

function statusTone(s: Coverage['status']): Tone {
  return s === 'enforced' ? 'ok' : s === 'open' ? 'warn' : 'muted'
}

function methodGroups(coverage: Coverage[]) {
  const map = new Map<string, { method: string; names: string[]; enforcedCount: number }>()
  for (const c of coverage) {
    for (const m of c.methods) {
      const g = map.get(m.method) ?? { method: m.method, names: [], enforcedCount: 0 }
      g.names.push(c.iface.label)
      if (m.enforced) g.enforcedCount++
      map.set(m.method, g)
    }
  }
  return [...map.values()].sort((a, b) => b.enforcedCount - a.enforcedCount)
}
