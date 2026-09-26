import { useCallback, useState } from 'react'
import { Plus, Trash2, Wrench } from 'lucide-react'
import {
  type McpEndpoint, type McpEndpointCreated, type McpTool,
  createMcpEndpoint, getMcpEndpointTools, getWireMetrics, listBackendSets, listInterfaces, listMcpEndpoints, revokeMcpEndpoint,
} from '../../api/client'
import {
  Button, CodeBlock, DataTable, EmptyState, Field, IconButton, KpiStrip, Loading, NameCell, Notice, PageHeader, Section, StatusPill, Tag, Tabs,
  compact, type KpiItem,
} from '../../components/ui'
import { errorText, useLoad } from '../../hooks'
import styles from './interfaces.module.css'

const EXPIRIES: Array<{ label: string; seconds?: number }> = [
  { label: 'Never expires' }, { label: '1 hour', seconds: 3600 }, { label: '24 hours', seconds: 86400 },
  { label: '7 days', seconds: 604800 }, { label: '30 days', seconds: 2592000 },
]

function scopeLabel(scope: string): string {
  if (scope === 'all') return 'All backends'
  if (scope.startsWith('group:')) return `Set ${scope.slice(6)}`
  if (scope.startsWith('db:')) return `Backend ${scope.slice(3)}`
  return scope
}

/** Connection snippet for an endpoint. The token is only known right after creation, so it is a placeholder unless supplied. */
function snippets(host: string, port: number | undefined, path: string, token: string): Array<{ label: string; code: string }> {
  const url = `http://${host}:${port ?? '<mcp-port>'}${path}`
  return [
    { label: 'HTTP (curl)', code: `curl -X POST ${url} \\\n  -H "Authorization: Bearer ${token}" \\\n  -H "Content-Type: application/json" \\\n  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'` },
    { label: 'MCP client config', code: JSON.stringify({ mcpServers: { warp: { url, headers: { Authorization: `Bearer ${token}` } } } }, null, 2) },
  ]
}

/** MCP servers: the MCP listener (from /api/interfaces), user-created endpoints scoped to a backend or set (/api/mcp-endpoints),
 * the tools each endpoint exposes (/api/mcp-endpoints/{id}/tools) and real per-tool call counts from /api/metrics/summary. */
export default function McpServers() {
  const ifaces = useLoad(listInterfaces, 15_000)
  const endpoints = useLoad(listMcpEndpoints, 15_000)
  const metrics = useLoad(getWireMetrics, 15_000)
  const sets = useLoad(useCallback(() => listBackendSets(false), []))
  const [tab, setTab] = useState('endpoints')
  const [creating, setCreating] = useState(false)
  const [name, setName] = useState('')
  const [scope, setScope] = useState('all')
  const [expiry, setExpiry] = useState(0)
  const [description, setDescription] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [created, setCreated] = useState<McpEndpointCreated | null>(null)
  const [pendingRevoke, setPendingRevoke] = useState<McpEndpoint | null>(null)
  const [selected, setSelected] = useState<McpEndpoint | null>(null)
  const [tools, setTools] = useState<McpTool[] | null>(null)
  const [toolsError, setToolsError] = useState<string | null>(null)

  const mcp = ifaces.data?.interfaces.find((i) => i.id === 'mcp')
  const a2a = ifaces.data?.interfaces.find((i) => i.id === 'a2a')
  const host = typeof window === 'undefined' ? 'localhost' : window.location.hostname
  const list = endpoints.data ?? []
  const active = list.filter((e) => e.status === 'active').length
  const toolStats = metrics.data?.mcpTools ?? []
  const toolCalls = toolStats.reduce((s, t) => s + t.calls, 0)

  async function pick(e: McpEndpoint) {
    setSelected(e); setTools(null); setToolsError(null)
    try { setTools((await getMcpEndpointTools(e.id)).tools) } catch (err) { setToolsError(errorText(err)) }
  }

  async function create(ev: React.FormEvent) {
    ev.preventDefault()
    setBusy(true); setError(null)
    try {
      const r = await createMcpEndpoint({ name: name.trim(), scope, description: description.trim() || undefined, ttlSeconds: EXPIRIES[expiry].seconds })
      setCreated(r); setCreating(false); setName(''); setDescription(''); endpoints.reload()
    } catch (err) { setError(errorText(err)) } finally { setBusy(false) }
  }

  async function revoke() {
    if (!pendingRevoke) return
    const e = pendingRevoke
    setBusy(true); setError(null)
    try {
      await revokeMcpEndpoint(e.id)
      setPendingRevoke(null)
      if (selected?.id === e.id) setSelected(null)
      endpoints.reload()
    } catch (err) { setError(errorText(err)); setPendingRevoke(null) } finally { setBusy(false) }
  }

  const kpis: KpiItem[] = [
    { label: 'MCP listener', value: mcp ? 'Listening' : ifaces.loading ? '…' : 'Not running', tone: mcp ? 'ok' : 'muted', wide: true, hint: mcp ? `port ${mcp.port}` : undefined },
    { label: 'Endpoints', value: list.length, hint: `${active} active · ${list.length - active} expired` },
    { label: 'Tool calls', value: compact(toolCalls), hint: `${toolStats.length} tool${toolStats.length === 1 ? '' : 's'} used since start` },
    { label: 'A2A agent', value: a2a ? `:${a2a.port}` : 'Off', hint: a2a ? 'Agent2Agent JSON-RPC' : 'not listening' },
  ]

  return (
    <div>
      <PageHeader title="MCP servers" description="Governed tool endpoints for AI agents. An endpoint is scoped to all backends, one backend set or a single backend, can expire, and is revoked instantly."
        actions={<Button variant="primary" icon={<Plus size={14} aria-hidden="true" />} onClick={() => { setCreating(true); setCreated(null) }}>New endpoint</Button>} />
      {ifaces.error && <Notice tone="bad">Could not load interfaces: {ifaces.error}</Notice>}
      {endpoints.error && <Notice tone="bad">Could not load endpoints: {endpoints.error}</Notice>}
      {error && <Notice tone="bad">{error}</Notice>}
      {ifaces.loading ? <Loading /> : <KpiStrip items={kpis} label="MCP figures" />}

      {created && (
        <Section title={`Endpoint “${created.name}” created`} meta="The token is shown once">
          <Notice tone="warn">Copy the token now: only its hash is stored and it cannot be shown again.</Notice>
          <CodeBlock label="Bearer token">{created.token}</CodeBlock>
          {snippets(host, created.mcpPort, created.path, created.token).map((s) => <CodeBlock key={s.label} label={s.label}>{s.code}</CodeBlock>)}
          <Button variant="ghost" onClick={() => setCreated(null)}>I have copied the token</Button>
        </Section>
      )}

      {creating && (
        <Section title="New MCP endpoint">
          <form onSubmit={create} aria-label="New MCP endpoint">
            <div className={styles.formGrid}>
              <Field label="Name" hint="Shown in the list and in audit events.">
                {(id) => <input id={id} value={name} onChange={(e) => setName(e.target.value)} required autoComplete="off" />}
              </Field>
              <Field label="Scope" hint="What the endpoint's tools can reach.">
                {(id) => (
                  <select id={id} value={scope} onChange={(e) => setScope(e.target.value)}>
                    <option value="all">All backends</option>
                    <optgroup label="One backend set">
                      {(sets.data?.sets ?? []).map((s) => <option key={s.name} value={`group:${s.name}`}>Set {s.name} ({s.backends.length})</option>)}
                    </optgroup>
                    <optgroup label="One backend">
                      {(sets.data?.sets ?? []).flatMap((s) => s.backends.map((b) => <option key={b.name} value={`db:${b.name}`}>{b.name} (in {s.name})</option>))}
                    </optgroup>
                  </select>
                )}
              </Field>
              <Field label="Expiry">
                {(id) => <select id={id} value={expiry} onChange={(e) => setExpiry(Number(e.target.value))}>{EXPIRIES.map((x, i) => <option key={x.label} value={i}>{x.label}</option>)}</select>}
              </Field>
              <Field label="Description" hint="Optional.">
                {(id) => <input id={id} value={description} onChange={(e) => setDescription(e.target.value)} autoComplete="off" />}
              </Field>
            </div>
            <div className={styles.formActions}>
              <Button variant="primary" type="submit" disabled={busy || !name.trim()}>{busy ? 'Creating…' : 'Create endpoint'}</Button>
              <Button variant="ghost" onClick={() => setCreating(false)} disabled={busy}>Cancel</Button>
            </div>
          </form>
        </Section>
      )}

      {pendingRevoke && (
        <Notice tone="warn">
          Revoke endpoint <strong>{pendingRevoke.name}</strong>? Agents using its token lose access immediately.{' '}
          <Button variant="danger" onClick={revoke} disabled={busy}>Revoke</Button>{' '}
          <Button variant="ghost" onClick={() => setPendingRevoke(null)} disabled={busy}>Keep</Button>
        </Notice>
      )}

      <Tabs label="MCP views" value={tab} onChange={setTab} tabs={[{ id: 'endpoints', label: 'Endpoints', count: list.length }, { id: 'usage', label: 'Tool usage', count: toolStats.length }]} />

      {tab === 'endpoints' && (
        <>
          <Section flush title="Endpoints" meta={mcp ? `MCP listener on port ${mcp.port}` : undefined}>
            {endpoints.loading ? <div className={styles.pad}><Loading /></div>
              : list.length === 0 ? <EmptyState title="No MCP endpoints">Create one to give an agent a scoped, expiring token. The built-in listener still serves the deployment-wide scope.</EmptyState>
              : (
                <DataTable caption="MCP endpoints" minWidth={860}>
                  <thead><tr><th>Endpoint</th><th>Scope</th><th>Created</th><th>Expires</th><th>Status</th><th aria-label="Actions"></th></tr></thead>
                  <tbody>
                    {list.map((e) => (
                      <tr key={e.id} className={selected?.id === e.id ? styles.selected : undefined}>
                        <td><NameCell name={<button type="button" className={styles.rowBtn} onClick={() => pick(e)}>{e.name}</button>} sub={e.description ?? e.path} /></td>
                        <td><Tag>{scopeLabel(e.scope)}</Tag></td>
                        <td>{e.createdAt ? new Date(e.createdAt).toLocaleString() : '—'}<div className={styles.sub}>{e.createdBy ?? ''}</div></td>
                        <td>{e.expiresAt ? new Date(e.expiresAt).toLocaleString() : <span className={styles.sub}>never</span>}</td>
                        <td><StatusPill tone={e.status === 'active' ? 'ok' : 'muted'}>{e.status === 'active' ? 'Active' : 'Expired'}</StatusPill></td>
                        <td><div className={styles.actions}>
                          <IconButton label={`Show tools of ${e.name}`} onClick={() => pick(e)}><Wrench size={16} strokeWidth={1.8} /></IconButton>
                          <IconButton danger label={`Revoke ${e.name}`} onClick={() => setPendingRevoke(e)}><Trash2 size={16} strokeWidth={1.8} /></IconButton>
                        </div></td>
                      </tr>
                    ))}
                  </tbody>
                </DataTable>
              )}
          </Section>
          {selected && (
            <Section title={`Tools of “${selected.name}”`} meta={scopeLabel(selected.scope)}>
              {toolsError && <Notice tone="bad">{toolsError}</Notice>}
              {!tools && !toolsError && <Loading>Loading tools…</Loading>}
              {tools && tools.length === 0 && <EmptyState title="No tools in scope">This scope reaches no backend that has tools.</EmptyState>}
              {tools && tools.length > 0 && (
                <div className={styles.toolList}>
                  {tools.map((t) => <div key={t.name} className={styles.tool}><strong>{t.name}</strong><p>{t.description}</p></div>)}
                </div>
              )}
              <h3 style={{ margin: '16px 0 8px' }}>Connect</h3>
              <p className={styles.help}>The token was shown once at creation; put it where the placeholder is.</p>
              {snippets(host, mcp?.port, selected.path, '<token>').map((s) => <CodeBlock key={s.label} label={s.label}>{s.code}</CodeBlock>)}
            </Section>
          )}
        </>
      )}

      {tab === 'usage' && (
        <Section flush title="Tool usage" meta="since process start">
          {toolStats.length === 0 ? <EmptyState title="No tool calls yet">Calls made by agents through any MCP endpoint are counted here.</EmptyState> : (
            <DataTable caption="MCP tool usage" minWidth={520}>
              <thead><tr><th>Tool</th><th>Calls</th><th>Errors</th><th>Avg</th></tr></thead>
              <tbody>
                {toolStats.map((t) => (
                  <tr key={t.tool}><td className={styles.mono}>{t.tool}</td><td className={styles.num}>{t.calls.toLocaleString()}</td>
                    <td className={styles.num}>{t.errors}</td><td className={styles.num}>{t.avgMs} ms</td></tr>
                ))}
              </tbody>
            </DataTable>
          )}
        </Section>
      )}
    </div>
  )
}
