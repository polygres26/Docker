import { useCallback, useEffect, useState } from 'react'
import { Boxes, PlugZap, Plus, Pencil, Trash2 } from 'lucide-react'
import { Link } from 'react-router-dom'
import {
  type BackendDraft, type BackendSetInfo, type BackendSetsResponse, type BackendTestResult, type BackendWriteResult,
  type SetBackend, type StoreId, type StoreInfo, type WireConfig,
  addBackendToSet, createBackendSet, deleteBackendSet, deleteSetBackend, getWireConfig, listBackendSets,
  saveWireConfig, testBackendConnection, testSetBackend, updateSetBackend,
} from '../api/client'
import CredentialField from '../components/CredentialField'
import { Button, DataTable, EmptyState, Field, Loading, Notice, PageHeader, Section, StatusPill, Tag } from '../components/ui'
import styles from './BackendSets.module.css'

const isPostgresUrl = (url: string) => url.trim().toLowerCase().startsWith('jdbc:postgresql:')

function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}

/** What the last write reported: version, stores whose shard layout changed, warnings. */
function WriteNotice({ result, what }: { result: BackendWriteResult; what: string }) {
  return (
    <>
      <Notice tone="ok">{what} — warp_config version {result.version}. Every Warp instance picks it up within a moment.</Notice>
      {result.rebalanceRequired.map((r) => (
        <Notice key={r.store} tone="warn"><strong>Rebalance needed for {r.store}.</strong> {r.message}</Notice>
      ))}
      {result.warnings.map((w) => <Notice key={w} tone="warn">{w}</Notice>)}
    </>
  )
}

function storeLabel(stores: StoreInfo[], id: StoreId): string {
  return stores.find((s) => s.id === id)?.label ?? id
}

/** "Enable stores" checkboxes for a Postgres backend, with the sharding / Neo4j-once notes. */
function StoresFieldset({ stores, set, editing, value, onChange, wasEnabled }: {
  stores: StoreInfo[]; set: BackendSetInfo; editing?: SetBackend; value: StoreId[]; onChange: (v: StoreId[]) => void
  wasEnabled: StoreId[]
}) {
  const others = (id: StoreId) => set.backends.filter((b) => b.name !== editing?.name && b.enabledStores.includes(id)).map((b) => b.name)
  return (
    <fieldset className={styles.fieldset}>
      <legend className={styles.legend}>Enable stores</legend>
      <p className={styles.help}>
        Enabling a store makes this Postgres host that protocol's data: Warp creates the tables here and the protocol's
        frontend reads and writes them. Where several backends of this set enable the same store, its data is sharded across them.
      </p>
      <ul className={styles.storeList}>
        {stores.map((s) => {
          const on = others(s.id)
          const checked = value.includes(s.id)
          const blocked = !s.shardable && on.length > 0
          const noteId = `store-note-${s.id}`
          return (
            <li key={s.id} className={styles.storeItem}>
              <label className={styles.storeLabel}>
                <input type="checkbox" checked={checked} disabled={blocked} aria-describedby={noteId}
                  onChange={(e) => onChange(e.target.checked ? [...value, s.id] : value.filter((x) => x !== s.id))} />
                <span>
                  <strong>{s.label}</strong>
                  <span className={styles.storeDesc}>{s.description}</span>
                </span>
              </label>
              <div id={noteId} className={styles.storeNote}>
                {blocked && <>Already enabled on <code>{on.join(', ')}</code>. Neo4j runs on one backend per set: graph traversals cannot be answered correctly when the graph is spread over several databases.</>}
                {!blocked && s.shardable && on.length > 0 && <>Also enabled on <code>{on.join(', ')}</code>: data will be sharded across {on.length + (checked ? 1 : 0)} backend{on.length + (checked ? 1 : 0) === 1 ? '' : 's'}{checked ? '' : ' if you enable it'}. Existing data is not moved.</>}
                {!blocked && !s.shardable && on.length === 0 && <>One backend per set.</>}
                {editing && wasEnabled.includes(s.id) && !checked && <> Disabling keeps the data in this database but Warp stops serving it.</>}
              </div>
            </li>
          )
        })}
      </ul>
    </fieldset>
  )
}

function BackendEditor({ set, editing, stores, onDone, onCancel }: {
  set: BackendSetInfo; editing?: SetBackend; stores: StoreInfo[]
  onDone: (r: BackendWriteResult, what: string) => void; onCancel: () => void
}) {
  const [name, setName] = useState(editing?.name ?? '')
  const [url, setUrl] = useState(editing ? '' : 'jdbc:postgresql://host:5432/postgres')
  const [user, setUser] = useState(editing?.user ?? '')
  const [password, setPassword] = useState('')
  const [description, setDescription] = useState(editing?.description ?? '')
  const [enabled, setEnabled] = useState<StoreId[]>(editing?.enabledStores ?? [])
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [test, setTest] = useState<BackendTestResult | null>(null)

  const urlChanged = url.trim() !== ''
  const effectiveType = urlChanged ? (isPostgresUrl(url) ? 'postgres' : 'other') : (editing?.canHostStores ? 'postgres' : 'other')
  const canHost = effectiveType === 'postgres'

  async function handleTest() {
    setBusy(true); setTest(null); setError(null)
    try {
      setTest(editing && !urlChanged && !password
        ? await testSetBackend(set.name, editing.name)
        : await testBackendConnection({ jdbcUrl: url, user, password }))
    } catch (e) { setError(errorText(e)) } finally { setBusy(false) }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setBusy(true); setError(null)
    try {
      if (editing) {
        const body: Partial<Omit<BackendDraft, 'name'>> = { description, enabledStores: canHost ? enabled : [] }
        if (user !== (editing.user ?? '')) body.user = user
        if (urlChanged) body.url = url
        if (password) body.password = password
        onDone(await updateSetBackend(set.name, editing.name, body), `Saved backend ${editing.name}`)
      } else {
        onDone(await addBackendToSet(set.name, { name, url, user, password, description, enabledStores: canHost ? enabled : [] }),
          `Added backend ${name} to set ${set.name}`)
      }
    } catch (e2) { setError(errorText(e2)) } finally { setBusy(false) }
  }

  const heading = editing ? `Edit backend ${editing.name}` : `Add backend to ${set.name}`
  return (
    <Section title={heading} meta={editing ? `in set ${set.name}` : undefined}>
      <form onSubmit={handleSubmit} aria-label={heading}>
        <div className={styles.formGrid}>
          <Field label="Name" hint={editing ? 'A backend cannot be renamed.' : 'Letters, digits, _ - . (used in routing rules and tool calls).'}>
            {(id) => <input id={id} value={name} onChange={(e) => setName(e.target.value)} disabled={!!editing} required autoComplete="off" />}
          </Field>
          <Field label="Connection URL" hint={editing ? `Leave blank to keep ${editing.url}` : 'JDBC URL, e.g. jdbc:postgresql://host:5432/db. The host must be in WARP_TRUSTED_BACKEND_HOSTS.'}>
            {(id) => <input id={id} value={url} onChange={(e) => setUrl(e.target.value)} required={!editing} autoComplete="off" className={styles.monoInput} />}
          </Field>
          <Field label="User">
            {(id) => <input id={id} value={user} onChange={(e) => setUser(e.target.value)} autoComplete="off" />}
          </Field>
          <Field label="Password" hint={editing ? 'Leave blank to keep the stored credential.' : undefined}>
            {() => <CredentialField value={password} onChange={setPassword} />}
          </Field>
        </div>
        <Field label="Description" hint="Shown in the backend list and to MCP clients.">
          {(id) => <input id={id} value={description} onChange={(e) => setDescription(e.target.value)} autoComplete="off" />}
        </Field>

        {canHost
          ? <StoresFieldset stores={stores} set={set} editing={editing} value={enabled} onChange={setEnabled} wasEnabled={editing?.enabledStores ?? []} />
          : <Notice tone="muted">Only Postgres backends can host protocol stores (InfluxDB, MongoDB, SQS, Neo4j, OpenSearch, DynamoDB).</Notice>}

        {error && <Notice tone="bad">{error}</Notice>}
        {test && (
          <Notice tone={test.ok ? 'ok' : 'bad'}>
            {test.ok ? `Connected in ${test.tookMs} ms${test.serverVersion ? ` — ${test.serverVersion.split(',')[0]}` : ''}` : `Connection failed — ${test.message}`}
          </Notice>
        )}
        <div className={styles.formActions}>
          <Button variant="primary" type="submit" disabled={busy}>{busy ? 'Saving…' : editing ? 'Save changes' : 'Add backend'}</Button>
          <Button onClick={handleTest} disabled={busy || (!editing && !url.trim())} icon={<PlugZap size={14} aria-hidden="true" />}>Test connection</Button>
          <Button variant="ghost" onClick={onCancel} disabled={busy}>Cancel</Button>
        </div>
      </form>
    </Section>
  )
}

function StoreTags({ backend, set, stores }: { backend: SetBackend; set: BackendSetInfo; stores: StoreInfo[] }) {
  if (backend.enabledStores.length === 0) return <span className={styles.sub}>{backend.canHostStores ? 'None' : '—'}</span>
  return (
    <span className={styles.tags}>
      {backend.enabledStores.map((id) => {
        const h = set.stores[id]
        const note = h?.sharded ? ` · sharded ×${h.hosts.length}` : ''
        const unserved = h && !h.servedFromThisSet
        return (
          <span key={id} title={unserved ? `No ${storeLabel(stores, id)} frontend serves this set: set ${h.frontendSetEnvVar}=${set.name}` : undefined}>
            <Tag>{storeLabel(stores, id)}{note}{unserved ? ' · not served' : ''}</Tag>
          </span>
        )
      })}
    </span>
  )
}

function Health({ b, probe, busy }: { b: SetBackend; probe?: BackendTestResult; busy: boolean }) {
  if (busy) return <StatusPill tone="muted">Testing</StatusPill>
  const h = probe ?? b.health
  if (h) return <span title={h.message}><StatusPill tone={h.ok ? 'ok' : 'bad'}>{h.ok ? `Healthy · ${h.tookMs} ms` : 'Unreachable'}</StatusPill></span>
  if (b.state !== 'ACTIVE') return <StatusPill tone="warn">{b.state.toLowerCase()}</StatusPill>
  return <span className={styles.sub}>Checking…</span>
}

type Editor = { kind: 'add'; set: string } | { kind: 'edit'; set: string; backend: string }

export default function BackendSets() {
  const [data, setData] = useState<BackendSetsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<React.ReactNode>(null)
  const [editor, setEditor] = useState<Editor | null>(null)
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDescription, setNewDescription] = useState('')
  const [busy, setBusy] = useState(false)
  const [probes, setProbes] = useState<Record<string, BackendTestResult>>({})
  const [testing, setTesting] = useState<Record<string, boolean>>({})
  const [pendingDelete, setPendingDelete] = useState<{ kind: 'set' | 'backend'; set: string; backend?: string } | null>(null)

  const load = useCallback(async () => {
    try {
      setData(await listBackendSets(false))
      setError(null)
      // health probes can be slow for an unreachable backend: fill them in afterwards
      listBackendSets(true).then(setData).catch(() => undefined)
    } catch (e) { setError(errorText(e)) }
  }, [])
  useEffect(() => { load() }, [load])

  function done(r: BackendWriteResult, what: string) {
    setNotice(<WriteNotice result={r} what={what} />)
    setEditor(null)
    setProbes({})
    load()
  }

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault()
    setBusy(true); setError(null)
    try {
      const r = await createBackendSet(newName.trim(), newDescription.trim())
      done(r, `Created backend set ${newName.trim()}`)
      setCreating(false); setNewName(''); setNewDescription('')
    } catch (e2) { setError(errorText(e2)) } finally { setBusy(false) }
  }

  async function handleTest(set: string, name: string) {
    setTesting((t) => ({ ...t, [name]: true }))
    try {
      const r = await testSetBackend(set, name)
      setProbes((p) => ({ ...p, [name]: r }))
    } catch (e) {
      setProbes((p) => ({ ...p, [name]: { ok: false, message: errorText(e), tookMs: 0, serverVersion: null } }))
    } finally { setTesting((t) => ({ ...t, [name]: false })) }
  }

  async function confirmDelete() {
    if (!pendingDelete) return
    setBusy(true); setError(null)
    try {
      if (pendingDelete.kind === 'set') {
        done(await deleteBackendSet(pendingDelete.set), `Deleted backend set ${pendingDelete.set}`)
      } else {
        done(await deleteSetBackend(pendingDelete.set, pendingDelete.backend!), `Deleted backend ${pendingDelete.backend}`)
      }
      setPendingDelete(null)
    } catch (e) { setError(errorText(e)); setPendingDelete(null) } finally { setBusy(false) }
  }

  const editingSet = editor && data ? data.sets.find((s) => s.name === editor.set) : undefined
  const editingBackend = editor?.kind === 'edit' && editingSet ? editingSet.backends.find((b) => b.name === editor.backend) : undefined

  return (
    <div>
      <PageHeader
        title="Backend sets"
        description="Every backend lives in a backend set. A Postgres backend can also host protocol stores — InfluxDB, MongoDB, SQS, Neo4j, OpenSearch, DynamoDB — sharded across the backends of its set."
        actions={<Button variant="primary" icon={<Plus size={14} aria-hidden="true" />} onClick={() => { setCreating(true); setEditor(null) }}>Create set</Button>}
      />

      {error && <Notice tone="bad">{error}</Notice>}
      {notice}
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
          <form onSubmit={handleCreate} aria-label="Create backend set">
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
          {data.backendCount} of {data.maxBackends} backends used on this license. Protocol frontends serve the set that holds
          the <code>default</code> backend unless <code>WARP_&lt;PROTOCOL&gt;_SET</code> names another set.
        </p>
      )}

      {data?.sets.map((set) => {
        const removable = set.backends.length === 0 && !set.isDefaultSet
        return (
          <Section key={set.name} flush title={set.name} meta={`${set.backends.length} backend${set.backends.length === 1 ? '' : 's'}`}>
            <div className={styles.toolbar}>
              <span className={styles.setDesc}>{set.description ?? (set.isDefaultSet ? 'Holds the default backend and everything not placed in another set.' : 'No description.')}</span>
              <span className={styles.toolbarActions}>
                <Button icon={<Plus size={14} aria-hidden="true" />} onClick={() => { setEditor({ kind: 'add', set: set.name }); setCreating(false) }}
                  aria-label={`Add backend to ${set.name}`}>Add backend</Button>
                {removable && <Button variant="danger" icon={<Trash2 size={14} aria-hidden="true" />}
                  onClick={() => setPendingDelete({ kind: 'set', set: set.name })} aria-label={`Delete set ${set.name}`}>Delete set</Button>}
              </span>
            </div>
            {set.backends.length === 0 ? (
              <EmptyState icon={<Boxes size={18} aria-hidden="true" />} title="No backends in this set">
                Add a backend to it{removable ? ', or delete the set' : ''}.
              </EmptyState>
            ) : (
              <DataTable caption={`Backends in set ${set.name}`} minWidth={860}>
                <thead>
                  <tr>
                    <th>Name</th><th>Type</th><th>Target</th><th>Description</th><th>Stores</th><th>Health</th><th aria-label="Actions"></th>
                  </tr>
                </thead>
                <tbody>
                  {set.backends.map((b) => (
                    <tr key={b.name}>
                      <td className={styles.mono}>{b.name}{b.isDefault && <> <Tag>default</Tag></>}</td>
                      <td><Tag>{b.type}</Tag></td>
                      <td className={styles.mono}>{b.url}</td>
                      <td>{b.description ?? <span className={styles.sub}>—</span>}</td>
                      <td><StoreTags backend={b} set={set} stores={data.stores} /></td>
                      <td><Health b={b} probe={probes[b.name]} busy={!!testing[b.name]} /></td>
                      <td>
                        <div className={styles.actions}>
                          <button type="button" className={styles.iconBtn} title="Edit backend" aria-label={`Edit ${b.name}`}
                            onClick={() => { setEditor({ kind: 'edit', set: set.name, backend: b.name }); setCreating(false) }}><Pencil size={16} strokeWidth={1.8} /></button>
                          <button type="button" className={styles.iconBtn} title="Test connection" aria-label={`Test connection to ${b.name}`}
                            disabled={!!testing[b.name]} onClick={() => handleTest(set.name, b.name)}><PlugZap size={16} strokeWidth={1.8} /></button>
                          {!b.isDefault && <button type="button" className={styles.iconBtn} title="Delete backend" aria-label={`Delete ${b.name}`}
                            onClick={() => setPendingDelete({ kind: 'backend', set: set.name, backend: b.name })}><Trash2 size={16} strokeWidth={1.8} /></button>}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </DataTable>
            )}
          </Section>
        )
      })}

      {data && editor && editingSet && (editor.kind === 'add' || editingBackend) && (
        <BackendEditor key={`${editor.kind}-${editor.set}-${editor.kind === 'edit' ? editor.backend : ''}`}
          set={editingSet} editing={editingBackend} stores={data.stores} onDone={done} onCancel={() => setEditor(null)} />
      )}

      <AdvancedRouting />
    </div>
  )
}

/** Legacy router settings kept for existing configs: router aliases (WARP_BACKEND_SETS -- a name
 * for a list of backends usable in router rules) and the legacy WARP_SHARD_BACKENDS shard group. */
function AdvancedRouting() {
  const [config, setConfig] = useState<WireConfig | null>(null)
  const [aliases, setAliases] = useState('')
  const [shardGroup, setShardGroup] = useState('')
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    getWireConfig().then((c) => {
      setConfig(c)
      setAliases((c.backendSets ?? '').split('|').filter(Boolean).join('\n'))
      setShardGroup((c.shardBackends ?? '').split(',').map((s) => s.trim()).filter(Boolean).join('\n'))
    }).catch((e) => setError(errorText(e)))
  }, [])

  async function save(e: React.FormEvent) {
    e.preventDefault()
    setBusy(true); setError(null); setMessage(null)
    try {
      const r = await saveWireConfig({
        backendSets: aliases.split('\n').map((s) => s.trim()).filter(Boolean).join('|') || null,
        shardBackends: shardGroup.split('\n').map((s) => s.trim()).filter(Boolean).join(',') || null,
      })
      setMessage(`Saved — warp_config version ${r.version}.`)
    } catch (e2) { setError(errorText(e2)) } finally { setBusy(false) }
  }

  return (
    <details className={styles.advanced}>
      <summary>Advanced: router aliases and legacy shard group</summary>
      <div className={styles.advancedBody}>
        <p className={styles.help}>
          Router aliases name a list of backends that <Link to="/router">Router rules</Link> can reference (a backend may be in several).
          They are separate from backend sets, which own a backend and the stores it hosts. The legacy shard group is the
          <code> WARP_SHARD_BACKENDS</code> list DynamoDB, MongoDB, SQS and OpenSearch shard across when none of those stores is enabled on a backend.
        </p>
        {!config && !error && <Loading />}
        {config && (
          <form onSubmit={save} aria-label="Router aliases and shard group">
            <Field label="Router aliases" hint="One per line: alias=backend1,backend2">
              {(id) => <textarea id={id} rows={3} value={aliases} onChange={(e) => setAliases(e.target.value)} className={styles.monoInput} />}
            </Field>
            <Field label="Legacy shard group" hint="One backend name per line, in shard order.">
              {(id) => <textarea id={id} rows={3} value={shardGroup} onChange={(e) => setShardGroup(e.target.value)} className={styles.monoInput} />}
            </Field>
            {error && <Notice tone="bad">{error}</Notice>}
            {message && <Notice tone="ok">{message}</Notice>}
            <Button type="submit" disabled={busy}>{busy ? 'Saving…' : 'Save'}</Button>
          </form>
        )}
      </div>
    </details>
  )
}
