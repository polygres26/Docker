import { useCallback, useEffect, useMemo, useState } from 'react'
import { ArrowRightLeft, Boxes, PlugZap, Pencil, Plus, Trash2 } from 'lucide-react'
import { Link } from 'react-router-dom'
import {
  type BackendSetInfo, type BackendSetsResponse, type BackendTestResult, type BackendWriteResult, type InterfaceInfo,
  type McpEndpoint, type RebalanceNotice, type SetBackend,
  createBackendSet, deleteBackendSet, deleteSetBackend, getConfigVersion, getWireMetrics, listBackendSets, listInterfaces,
  listMcpEndpoints, listNodes, moveSetBackend, testSetBackend, updateBackendSet,
} from '../api/client'
import {
  Button, CopyButton, DataTable, EmptyState, Field, IconButton, KpiStrip, Loading, Meter, NameCell, Notice, PageHeader, Section,
  StatusPill, Tabs, Tag, type KpiItem,
} from '../components/ui'
import { errorText, targetOf, useLoad } from '../hooks'
import { AdvancedRouting, BackendEditor, ConnectPanel, Health, RoutesSection, StoreTags, WriteNotice, storeLabel } from './infra/parts'
import styles from './Infrastructure.module.css'

const POLL_MS = 15_000
const REBALANCE_KEY = 'warp.rebalance'

/** Rebalance notices come from the write that changed a store's hosts; Warp never moves data itself, so
 * we keep them (per set and store) until the operator dismisses them. Stored in sessionStorage only. */
type RebalanceMap = Record<string, RebalanceNotice[]>
function loadRebalance(): RebalanceMap {
  try { return JSON.parse(sessionStorage.getItem(REBALANCE_KEY) ?? '{}') as RebalanceMap } catch { return {} }
}
function saveRebalance(m: RebalanceMap) {
  try { sessionStorage.setItem(REBALANCE_KEY, JSON.stringify(m)) } catch { /* private mode */ }
}

type Editor = { kind: 'add'; set: string } | { kind: 'edit'; set: string; backend: string }
type Pending = { kind: 'set' | 'backend'; set: string; backend?: string }

/** Roles derived from real config: default backend, failover pairs (fallback field) and store hosting. */
function rolesOf(b: SetBackend, all: SetBackend[]): Array<{ label: string; title?: string }> {
  const roles: Array<{ label: string; title?: string }> = []
  if (b.isDefault) roles.push({ label: 'Default', title: 'Warp’s own frontends rely on this backend' })
  if (b.fallback) roles.push({ label: `Fails over to ${b.fallback}` })
  const standbyFor = all.filter((o) => o.fallback === b.name).map((o) => o.name)
  if (standbyFor.length > 0) roles.push({ label: `Standby for ${standbyFor.join(', ')}` })
  if (b.enabledStores.length > 0) roles.push({ label: 'Store host' })
  if (roles.length === 0) roles.push({ label: 'Primary' })
  return roles
}

function engineOf(b: SetBackend): string {
  return b.dialect ? b.dialect.replace(/_/g, ' ').toLowerCase() : b.type
}

export default function Infrastructure() {
  const sets = useLoad(useCallback(() => listBackendSets(false), []), POLL_MS)
  const nodes = useLoad(listNodes, POLL_MS)
  const version = useLoad(getConfigVersion, POLL_MS)
  const metrics = useLoad(getWireMetrics, POLL_MS)
  const interfaces = useLoad(listInterfaces, POLL_MS)
  const endpoints = useLoad(listMcpEndpoints, POLL_MS)

  const [tab, setTab] = useState('sets')
  const [data, setData] = useState<BackendSetsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<React.ReactNode>(null)
  const [editor, setEditor] = useState<Editor | null>(null)
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDescription, setNewDescription] = useState('')
  const [renaming, setRenaming] = useState<{ set: string; name: string; description: string } | null>(null)
  const [moving, setMoving] = useState<{ set: string; backend: string; to: string } | null>(null)
  const [busy, setBusy] = useState(false)
  const [probes, setProbes] = useState<Record<string, BackendTestResult>>({})
  const [testing, setTesting] = useState<Record<string, boolean>>({})
  const [pendingDelete, setPendingDelete] = useState<Pending | null>(null)
  const [rebalance, setRebalance] = useState<RebalanceMap>(loadRebalance)

  // The polled copy (fast, no probes) is the base; it keeps the last probe results by backend name. A health-probed
  // copy (slower: it connects to every backend) replaces it whenever one arrives.
  useEffect(() => {
    if (!sets.data) return
    const next = sets.data
    setData((prev) => (prev ? { ...next, sets: next.sets.map((s) => ({ ...s, backends: s.backends.map((b) => ({ ...b, health: prev.sets.flatMap((x) => x.backends).find((o) => o.name === b.name)?.health })) })) } : next))
  }, [sets.data])
  const probeHealth = useCallback(() => { listBackendSets(true).then(setData).catch(() => undefined) }, [])
  useEffect(() => { probeHealth() }, [probeHealth])
  useEffect(() => { if (sets.error) setError(sets.error) }, [sets.error])

  const reload = useCallback(() => { sets.reload(); nodes.reload(); version.reload(); endpoints.reload(); interfaces.reload(); probeHealth() }, [sets, nodes, version, endpoints, interfaces, probeHealth])

  function done(r: BackendWriteResult, what: string, set: string) {
    setNotice(<WriteNotice result={r} what={what} />)
    if (r.rebalanceRequired.length > 0) {
      const next = { ...rebalance, [set]: [...(rebalance[set] ?? []).filter((x) => !r.rebalanceRequired.some((y) => y.store === x.store)), ...r.rebalanceRequired] }
      setRebalance(next); saveRebalance(next)
    }
    setEditor(null); setRenaming(null); setMoving(null); setProbes({})
    reload()
  }

  function dismissRebalance(set: string, store: string) {
    const next = { ...rebalance, [set]: (rebalance[set] ?? []).filter((x) => x.store !== store) }
    if (next[set].length === 0) delete next[set]
    setRebalance(next); saveRebalance(next)
  }

  async function run(fn: () => Promise<void>) {
    setBusy(true); setError(null)
    try { await fn() } catch (e) { setError(errorText(e)) } finally { setBusy(false) }
  }

  const create = (e: React.FormEvent) => { e.preventDefault(); run(async () => {
    const r = await createBackendSet(newName.trim(), newDescription.trim())
    done(r, `Created backend set ${newName.trim()}`, newName.trim())
    setCreating(false); setNewName(''); setNewDescription('')
  }) }

  const rename = (e: React.FormEvent) => { e.preventDefault(); if (!renaming) return; run(async () => {
    const patch: { name?: string; description?: string | null } = { description: renaming.description.trim() || null }
    if (renaming.name.trim() !== renaming.set) patch.name = renaming.name.trim()
    const r = await updateBackendSet(renaming.set, patch)
    if (patch.name && rebalance[renaming.set]) {
      const next = { ...rebalance, [patch.name]: rebalance[renaming.set] }; delete next[renaming.set]
      setRebalance(next); saveRebalance(next)
    }
    done(r, patch.name ? `Renamed set ${renaming.set} to ${patch.name}` : `Updated set ${renaming.set}`, patch.name ?? renaming.set)
  }) }

  const move = (e: React.FormEvent) => { e.preventDefault(); if (!moving) return; run(async () => {
    done(await moveSetBackend(moving.set, moving.backend, moving.to), `Moved backend ${moving.backend} from ${moving.set} to ${moving.to}`, moving.to)
  }) }

  async function testOne(set: string, name: string) {
    setTesting((t) => ({ ...t, [name]: true }))
    try {
      const r = await testSetBackend(set, name)
      setProbes((p) => ({ ...p, [name]: r }))
    } catch (e) {
      setProbes((p) => ({ ...p, [name]: { ok: false, message: errorText(e), tookMs: 0, serverVersion: null } }))
    } finally { setTesting((t) => ({ ...t, [name]: false })) }
  }

  const confirmDelete = () => { if (!pendingDelete) return; run(async () => {
    const p = pendingDelete
    setPendingDelete(null)
    if (p.kind === 'set') done(await deleteBackendSet(p.set), `Deleted backend set ${p.set}`, p.set)
    else done(await deleteSetBackend(p.set, p.backend!), `Deleted backend ${p.backend}`, p.set)
  }) }

  const allBackends = useMemo(() => (data?.sets ?? []).flatMap((s) => s.backends), [data])
  const upNodes = nodes.data?.filter((n) => n.status === 'up').length ?? 0
  const pools = allBackends.map((b) => b.pool).filter((p): p is NonNullable<SetBackend['pool']> => !!p)
  const poolMax = pools.reduce((s, p) => s + p.max, 0)
  const poolActive = pools.reduce((s, p) => s + p.active, 0)
  const probed = allBackends.filter((b) => b.health)
  const unreachable = probed.filter((b) => b.health && !b.health.ok).length

  const kpis: KpiItem[] = [
    { label: 'Warp nodes', value: nodes.data && nodes.data.length > 0 ? `${upNodes} / ${nodes.data.length}` : '—',
      hint: nodes.data && nodes.data.length > 0 ? (upNodes === nodes.data.length ? 'all reporting' : `${nodes.data.length - upNodes} stale`) : 'no node heartbeats' },
    { label: 'Backends', value: data ? `${data.backendCount} / ${data.maxBackends}` : '—',
      hint: data ? `${data.sets.length} set${data.sets.length === 1 ? '' : 's'}${probed.length > 0 ? ` · ${unreachable === 0 ? 'all reachable' : `${unreachable} unreachable`}` : ''}` : undefined },
    { label: 'Pool utilization', value: poolMax > 0 ? `${Math.round((poolActive / poolMax) * 100)}%` : '—',
      hint: poolMax > 0 ? `${poolActive} of ${poolMax} connections in use` : 'no pool opened yet' },
    { label: 'Config version', value: version.data?.version != null ? `v${version.data.version}` : '—',
      hint: version.data?.createdAt ? `saved ${new Date(version.data.createdAt).toLocaleString()}` : undefined },
  ]

  const editingSet = editor && data ? data.sets.find((s) => s.name === editor.set) : undefined
  const editingBackend = editor?.kind === 'edit' && editingSet ? editingSet.backends.find((b) => b.name === editor.backend) : undefined
  const ports = useMemo(() => Object.fromEntries((interfaces.data?.interfaces ?? []).map((i) => [i.id, i.port])), [interfaces.data])

  return (
    <div>
      <PageHeader
        title="Infrastructure"
        description="Backend sets are the containers Warp routes to; each set holds its backends, and each Postgres backend can host protocol stores. Also: Warp nodes, pools, health and config version."
        actions={<Button variant="primary" icon={<Plus size={14} aria-hidden="true" />} onClick={() => { setCreating(true); setEditor(null); setTab('sets') }}>Create set</Button>}
      />
      {error && <Notice tone="bad">{error}</Notice>}
      {notice}
      <KpiStrip items={kpis} label="Infrastructure figures" />
      <Tabs label="Infrastructure views" value={tab} onChange={setTab} tabs={[
        { id: 'sets', label: 'Backend sets', count: data?.sets.length },
        { id: 'routing', label: 'Connection routing' },
        { id: 'nodes', label: 'Warp nodes', count: nodes.data?.length },
      ]} />

      {tab === 'sets' && (
        <>
          {pendingDelete && (
            <Notice tone="warn">
              <div className={styles.confirm}>
                <span>
                  {pendingDelete.kind === 'set'
                    ? <>Delete the empty backend set <strong>{pendingDelete.set}</strong>?</>
                    : <>Delete backend <strong>{pendingDelete.backend}</strong>? Data it hosts stays in its database but Warp stops serving it.</>}
                </span>
                <span className={styles.confirmActions}>
                  <Button variant="danger" onClick={confirmDelete} disabled={busy}>Delete</Button>
                  <Button variant="ghost" onClick={() => setPendingDelete(null)} disabled={busy}>Keep</Button>
                </span>
              </div>
            </Notice>
          )}
          {creating && (
            <Section title="Create backend set">
              <form onSubmit={create} aria-label="Create backend set">
                <div className={styles.formGrid}>
                  <Field label="Name" hint="Letters, digits, _ - .">
                    {(id) => <input id={id} value={newName} onChange={(e) => setNewName(e.target.value)} required autoComplete="off" />}
                  </Field>
                  <Field label="Description" hint="Optional.">
                    {(id) => <input id={id} value={newDescription} onChange={(e) => setNewDescription(e.target.value)} autoComplete="off" />}
                  </Field>
                </div>
                <div className={styles.formActions}>
                  <Button variant="primary" type="submit" disabled={busy || !newName.trim()}>Create set</Button>
                  <Button variant="ghost" onClick={() => setCreating(false)} disabled={busy}>Cancel</Button>
                </div>
              </form>
            </Section>
          )}
          {!data && !error && <Loading>Loading backend sets…</Loading>}
          {data && (
            <p className={styles.help}>
              {data.backendCount} of {data.maxBackends} backends used on this license. A protocol frontend serves the set that holds
              the <code>default</code> backend unless <code>WARP_&lt;PROTOCOL&gt;_SET</code> names another set.
            </p>
          )}
          {data?.sets.map((set) => (
            <SetPanel key={set.name} set={set} data={data} probes={probes} testing={testing} busy={busy}
              endpoints={endpoints.data ?? []} interfaces={interfaces.data?.interfaces ?? []} metrics={metrics.data?.byBackend ?? []}
              rebalance={rebalance[set.name] ?? []} onDismissRebalance={(store) => dismissRebalance(set.name, store)}
              ports={ports}
              onAdd={() => { setEditor({ kind: 'add', set: set.name }); setCreating(false); setRenaming(null) }}
              onEdit={(b) => { setEditor({ kind: 'edit', set: set.name, backend: b }); setCreating(false); setRenaming(null) }}
              onRename={() => { setRenaming({ set: set.name, name: set.name, description: set.description ?? '' }); setEditor(null); setMoving(null) }}
              onMove={(b) => { setMoving({ set: set.name, backend: b, to: data.sets.find((s) => s.name !== set.name)?.name ?? '' }); setEditor(null); setRenaming(null) }}
              onTest={(b) => testOne(set.name, b)}
              onDelete={(p) => setPendingDelete(p)}>
              {renaming?.set === set.name && (
                <form onSubmit={rename} aria-label={`Rename set ${set.name}`} className={styles.inlineForm}>
                  <div className={styles.formGrid}>
                    <Field label="Set name" hint={set.isDefaultSet ? 'The default set cannot be renamed.' : 'Its backends follow. Connection routes and WARP_*_SET values naming the old name must be changed first.'}>
                      {(id) => <input id={id} value={renaming.name} disabled={set.isDefaultSet} onChange={(e) => setRenaming({ ...renaming, name: e.target.value })} required autoComplete="off" />}
                    </Field>
                    <Field label="Description">
                      {(id) => <input id={id} value={renaming.description} onChange={(e) => setRenaming({ ...renaming, description: e.target.value })} autoComplete="off" />}
                    </Field>
                  </div>
                  <div className={styles.formActions}>
                    <Button variant="primary" type="submit" disabled={busy || !renaming.name.trim()}>Save set</Button>
                    <Button variant="ghost" onClick={() => setRenaming(null)} disabled={busy}>Cancel</Button>
                  </div>
                </form>
              )}
              {moving?.set === set.name && (
                <form onSubmit={move} aria-label={`Move backend ${moving.backend}`} className={styles.inlineForm}>
                  <Field label={`Move ${moving.backend} to set`} hint="Only its membership changes: data already stored stays in its database. Stores it hosts are re-hashed over the new hosts, see the rebalance notice.">
                    {(id) => (
                      <select id={id} value={moving.to} onChange={(e) => setMoving({ ...moving, to: e.target.value })} required>
                        {data.sets.filter((s) => s.name !== set.name).map((s) => <option key={s.name} value={s.name}>{s.name}</option>)}
                      </select>
                    )}
                  </Field>
                  <div className={styles.formActions}>
                    <Button variant="primary" type="submit" disabled={busy || !moving.to}>Move backend</Button>
                    <Button variant="ghost" onClick={() => setMoving(null)} disabled={busy}>Cancel</Button>
                  </div>
                </form>
              )}
              {editor?.set === set.name && (editor.kind === 'add' || editingBackend) && editingSet && (
                <div className={styles.editorWrap}>
                  <BackendEditor key={`${editor.kind}-${editor.set}-${editor.kind === 'edit' ? editor.backend : ''}`}
                    set={editingSet} editing={editingBackend} stores={data.stores}
                    onDone={(r, what) => done(r, what, set.name)} onCancel={() => setEditor(null)} />
                </div>
              )}
            </SetPanel>
          ))}
          {data && data.sets.length === 0 && <EmptyState icon={<Boxes size={18} aria-hidden="true" />} title="No backend sets">Create a set, then add its backends.</EmptyState>}
        </>
      )}

      {tab === 'routing' && data && (
        <>
          <RoutesSection data={data} onChanged={reload} />
          <AdvancedRouting />
        </>
      )}
      {tab === 'routing' && !data && <Loading />}

      {tab === 'nodes' && (
        <Section flush title="Warp nodes" meta={version.data?.version != null ? `warp_config v${version.data.version}` : undefined}>
          {nodes.error ? <div className={styles.pad}><Notice tone="warn">Node heartbeats are not available: {nodes.error}</Notice></div>
            : !nodes.data ? <div className={styles.pad}><Loading /></div>
            : nodes.data.length === 0 ? <EmptyState title="No nodes have reported">Each Warp instance heartbeats its identity to the config database every ~10 seconds; none has yet.</EmptyState>
            : (
              <DataTable caption="Warp nodes" minWidth={640}>
                <thead><tr><th>Node</th><th>Zone</th><th>Version</th><th>Started</th><th>Last heartbeat</th><th>Status</th></tr></thead>
                <tbody>
                  {nodes.data.map((n) => (
                    <tr key={n.nodeId}>
                      <td><NameCell name={n.nodeId} sub={`${n.host}:${n.adminPort}`} /></td>
                      <td>{n.zone ?? <span className={styles.sub}>—</span>}</td>
                      <td className={styles.mono}>{n.version}</td>
                      <td>{new Date(n.startedAt).toLocaleString()}</td>
                      <td>{new Date(n.lastHeartbeat).toLocaleTimeString()}</td>
                      <td><StatusPill tone={n.status === 'up' ? 'ok' : 'warn'}>{n.status === 'up' ? 'Up' : 'Stale'}</StatusPill></td>
                    </tr>
                  ))}
                </tbody>
              </DataTable>
            )}
        </Section>
      )}
    </div>
  )
}

/** One backend set: header, its backends (with pool/health/roles/stores), stores & frontends, MCP endpoints. */
function SetPanel({ set, data, probes, testing, busy, endpoints, interfaces, metrics, rebalance, ports, onDismissRebalance, onAdd, onEdit, onRename, onMove, onTest, onDelete, children }: {
  set: BackendSetInfo; data: BackendSetsResponse; probes: Record<string, BackendTestResult | undefined>; testing: Record<string, boolean>; busy: boolean
  endpoints: McpEndpoint[]; interfaces: InterfaceInfo[]; metrics: Array<{ backend: string; calls: number; avgMs: number }>
  rebalance: RebalanceNotice[]; ports: Record<string, number>; onDismissRebalance: (store: string) => void
  onAdd: () => void; onEdit: (backend: string) => void; onRename: () => void; onMove: (backend: string) => void
  onTest: (backend: string) => void; onDelete: (p: Pending) => void; children?: React.ReactNode
}) {
  const removable = set.backends.length === 0 && !set.isDefaultSet
  const names = new Set(set.backends.map((b) => b.name))
  const setEndpoints = endpoints.filter((e) => e.scope === `group:${set.name}` || (e.scope.startsWith('db:') && names.has(e.scope.slice(3))))
  const storeIds = Object.keys(set.stores)
  const canMove = data.sets.length > 1
  return (
    <Section flush title={set.name} meta={`${set.backends.length} backend${set.backends.length === 1 ? '' : 's'}${set.isDefaultSet ? ' · holds the default backend' : ''}`}>
      <div className={styles.toolbar}>
        <span className={styles.setDesc}>
          <span className={styles.setDescText}>{set.description ?? (set.isDefaultSet ? 'Holds the default backend and everything not placed in another set.' : 'No description.')}</span>
          {set.connectAs ? (
            <span className={styles.connectCell}>
              Connect to the whole set with database <code className={styles.mono}>{set.connectAs}</code>
              <CopyButton text={set.connectAs} label={`Copy set name ${set.connectAs}`} />
            </span>
          ) : <span>Reach the whole set through a route (its name is also a backend).</span>}
        </span>
        <span className={styles.toolbarActions}>
          <Button icon={<Pencil size={14} aria-hidden="true" />} onClick={onRename} aria-label={`Rename or describe set ${set.name}`}>Rename</Button>
          <Button icon={<Plus size={14} aria-hidden="true" />} onClick={onAdd} aria-label={`Add backend to ${set.name}`}>Add backend</Button>
          {removable && <Button variant="danger" icon={<Trash2 size={14} aria-hidden="true" />} onClick={() => onDelete({ kind: 'set', set: set.name })} aria-label={`Delete set ${set.name}`}>Delete set</Button>}
        </span>
      </div>
      {children}
      {set.backends.length === 0 ? (
        <EmptyState icon={<Boxes size={18} aria-hidden="true" />} title="No backends in this set">Add a backend to it{removable ? ', or delete the set' : ''}.</EmptyState>
      ) : (
        <DataTable caption={`Backends in set ${set.name}`} minWidth={1080}>
          <thead>
            <tr><th>Backend</th><th>Engine</th><th>Host</th><th>Role</th><th>Stores</th><th>Connections</th><th>Health</th><th aria-label="Actions"></th></tr>
          </thead>
          <tbody>
            {set.backends.map((b) => {
              const stat = metrics.find((m) => m.backend === b.name)
              return (
                <tr key={b.name}>
                  <td className={styles.nameTd}>
                    <NameCell name={<span className={styles.mono}>{b.name}</span>} sub={b.description ?? undefined} />
                    <span className={styles.connectCell}><code className={styles.mono}>{b.connectAs}</code><CopyButton text={b.connectAs} label={`Copy database name ${b.connectAs}`} /></span>
                  </td>
                  <td><Tag>{b.type}</Tag><div className={styles.sub}>{engineOf(b)}</div></td>
                  <td className={styles.mono}>{targetOf(b.url)}{b.user && <div className={styles.sub}>user {b.user}</div>}</td>
                  <td><span className={styles.tags}>{rolesOf(b, set.backends).map((r) => <span key={r.label} title={r.title}><Tag>{r.label}</Tag></span>)}</span></td>
                  <td><StoreTags backend={b} set={set} stores={data.stores} /></td>
                  <td>
                    {b.pool
                      ? <Meter value={b.pool.active} max={b.pool.max} label={`${b.name} connection pool in use`} caption={`${b.pool.active} / ${b.pool.max}${b.pool.waiting > 0 ? ` · ${b.pool.waiting} waiting` : ''}`} />
                      : <span className={styles.sub}>No pool yet</span>}
                  </td>
                  <td>
                    <Health b={b} probe={probes[b.name]} busy={!!testing[b.name]} />
                    {stat && <div className={styles.sub}>{stat.calls.toLocaleString()} calls · {stat.avgMs} ms avg</div>}
                  </td>
                  <td>
                    <div className={styles.actions}>
                      <IconButton label={`Edit ${b.name}`} onClick={() => onEdit(b.name)}><Pencil size={16} strokeWidth={1.8} /></IconButton>
                      <IconButton label={`Test connection to ${b.name}`} disabled={!!testing[b.name]} onClick={() => onTest(b.name)}><PlugZap size={16} strokeWidth={1.8} /></IconButton>
                      {canMove && !b.isDefault && <IconButton label={`Move ${b.name} to another set`} onClick={() => onMove(b.name)}><ArrowRightLeft size={16} strokeWidth={1.8} /></IconButton>}
                      {!b.isDefault && <IconButton danger label={`Delete ${b.name}`} disabled={busy} onClick={() => onDelete({ kind: 'backend', set: set.name, backend: b.name })}><Trash2 size={16} strokeWidth={1.8} /></IconButton>}
                    </div>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </DataTable>
      )}

      {rebalance.length > 0 && (
        <div className={styles.block}>
          {rebalance.map((r) => (
            <Notice key={r.store} tone="warn">
              <div className={styles.confirm}>
                <span><strong>Rebalance required: {storeLabel(data.stores, r.store)}.</strong> Hosts changed from {r.before.join(', ') || 'none'} to {r.after.join(', ') || 'none'}; existing data is not moved.</span>
                <Button variant="ghost" onClick={() => onDismissRebalance(r.store)}>Dismiss</Button>
              </div>
            </Notice>
          ))}
        </div>
      )}

      {storeIds.length > 0 && (
        <details className={styles.detail}>
          <summary>Stores hosted by this set ({storeIds.length}) and the frontends serving them</summary>
          <div className={styles.detailBody}>
            <DataTable caption={`Stores hosted in set ${set.name}`} minWidth={720}>
              <thead><tr><th>Store</th><th>Hosts</th><th>Layout</th><th>Frontend</th><th>Rebalance</th></tr></thead>
              <tbody>
                {storeIds.map((id) => {
                  const h = set.stores[id]!
                  const fe = interfaces.filter((i) => i.store === id)
                  const rb = rebalance.find((r) => r.store === id)
                  return (
                    <tr key={id}>
                      <td>{storeLabel(data.stores, id)}</td>
                      <td className={styles.mono}>{h.hosts.join(', ')}</td>
                      <td>{h.sharded ? <Tag>{`sharded ×${h.hosts.length}`}</Tag> : <span className={styles.sub}>single host</span>}</td>
                      <td>
                        {fe.length > 0
                          ? <>{fe.map((i) => <span key={i.id}>{i.label} <code>:{i.port}</code> </span>)}
                              {!h.servedFromThisSet && <div className={styles.sub}>Serves another set (<code>{h.frontendSetEnvVar}</code>); set it to <code>{set.name}</code> to serve this one.</div>}</>
                          : <span className={styles.sub}>{h.servedFromThisSet ? 'Frontend not listening' : <>Not served: set <code>{h.frontendSetEnvVar}={set.name}</code></>}</span>}
                      </td>
                      <td>{rb ? <StatusPill tone="warn">Required</StatusPill> : <span className={styles.sub}>None recorded</span>}</td>
                    </tr>
                  )
                })}
              </tbody>
            </DataTable>
          </div>
        </details>
      )}

      <details className={styles.detail}>
        <summary>MCP endpoints scoped to this set ({setEndpoints.length})</summary>
        <div className={styles.detailBody}>
          {setEndpoints.length === 0
            ? <p className={styles.help}>No MCP endpoint is scoped to this set or its backends. Create one on <Link to="/interfaces/mcp">MCP servers</Link> with scope <code>group:{set.name}</code>.</p>
            : (
              <ul className={styles.plain}>
                {setEndpoints.map((e) => (
                  <li key={e.id}><strong>{e.name}</strong> <code>{e.scope}</code> <StatusPill tone={e.status === 'active' ? 'ok' : 'muted'}>{e.status}</StatusPill>{' '}
                    <span className={styles.sub}>{e.expiresAt ? `expires ${new Date(e.expiresAt).toLocaleString()}` : 'never expires'}</span></li>
                ))}
              </ul>
            )}
        </div>
      </details>

      {set.backends.length > 0 && <ConnectPanel setName={set.name} connectAs={set.connectAs} backends={set.backends} ports={ports} />}
    </Section>
  )
}
