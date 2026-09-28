import { useEffect, useState } from 'react'
import { PlugZap, Route as RouteIcon, Trash2 } from 'lucide-react'
import { Link } from 'react-router-dom'
import {
  type BackendDraft, type BackendSetInfo, type BackendSetsResponse, type BackendTestResult, type BackendWriteResult,
  type ConnectionRoute, type ConnectionRouting, type SetBackend, type StoreId, type StoreInfo, type WireConfig,
  addBackendToSet, addConnectionRoute, deleteConnectionRoute,
  getWireConfig,
  saveWireConfig, testBackendConnection, testSetBackend, updateSetBackend,
} from '../../api/client'
import CredentialField from '../../components/CredentialField'
import { Button, CopyButton, DataTable, EmptyState, Field, IconButton, Loading, Notice, Section, StatusPill, Tag } from '../../components/ui'
import styles from './parts.module.css'

const isPostgresUrl = (url: string) => url.trim().toLowerCase().startsWith('jdbc:postgresql:')

function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}

/** What the last write reported: version, stores whose shard layout changed, warnings. */
export function WriteNotice({ result, what }: { result: BackendWriteResult; what: string }) {
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

export function storeLabel(stores: StoreInfo[], id: StoreId): string {
  return stores.find((s) => s.id === id)?.label ?? id
}

/** "Enable stores" checkboxes for a Postgres backend, with the sharding / Neo4j-once notes. */
export function StoresFieldset({ stores, set, editing, value, onChange, wasEnabled }: {
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
                {s.id === 'redis' && <> Keys are spread over the hosts by Redis Cluster hash slot: multi-key commands and MULTI/EXEC need every key on one host (use a {'{hash tag}'}) or fail with CROSSSLOT. Scripting (Lua) is not available.</>}
                {s.id === 'azblob' && <> Blob limits: 5000 MiB per Put Blob, 4000 MiB per block, 50,000 blocks; the container list lives on the first Azure Blob host. Accounts and keys come from WARP_AZURE_ACCOUNTS.</>}
                {s.id === 'azqueue' && <> A queue lives wholly on one host (hash of account and queue name); messages up to 64 KiB, expiry swept every 10 s.</>}
                {s.id === 'aztable' && <> Entities are placed by hash of table and PartitionKey; an entity group transaction must stay inside one PartitionKey. Queries without a PartitionKey filter fan out to every host.</>}
                {s.id === 'firestore' && <> Documents are placed by hash of database and document path; collection scans, queries and collection-group queries scatter-gather over the hosts with the exact global order. gRPC and REST share WARP_FIRESTOREWIRE_PORT (8080, like the emulator); FIRESTORE_EMULATOR_HOST works; no auth unless WARP_FIRESTOREWIRE_TOKENS is set.</>}
                {s.id === 'datastore' && <> Entities are placed by hash of their root ancestor key, so an entity group and every ancestor query live on one host; kind and kindless queries scatter-gather. gRPC and REST share WARP_DATASTOREWIRE_PORT (8081, like the emulator); DATASTORE_EMULATOR_HOST works; no auth unless WARP_DATASTOREWIRE_TOKENS is set.</>}
                {s.id === 'bigtable' && <> A row (all its cells) lives on one host (hash of table name and row key); the table catalog lives on the first Bigtable host. gRPC data and table admin APIs on WARP_BIGTABLEWIRE_PORT (8088); no auth unless WARP_BIGTABLEWIRE_TOKENS is set. Column family GC rules are applied every WARP_BIGTABLEWIRE_GC_INTERVAL_SECONDS (60).</>}
                {s.id === 'cql' && <> A partition (all its rows) lives on one host (hash of the partition key); the schema catalog lives on the first Cassandra host and queries without a partition key scatter-gather over every host in Cassandra's token order. CQL native protocol v3/v4 on WARP_CQLWIRE_PORT (19042); no auth unless WARP_CQLWIRE_AUTH=true (PasswordAuthenticator, credentials from WARP_AUTH_USER/WARP_AUTH_PASSWORD or WARP_AUTH_CREDENTIALS). TTL expiry is swept every WARP_CQLWIRE_SWEEP_MS (5000).</>}
                {s.id === 'kafka' && <> A partition's log lives on one host (hash of topic and partition); topic metadata, consumer groups and committed offsets live on the first Kafka host. Kafka wire protocol on WARP_KAFKAWIRE_PORT (19092); clients reconnect to WARP_KAFKAWIRE_ADVERTISED_HOST:WARP_KAFKAWIRE_ADVERTISED_PORT (default localhost); no auth unless WARP_KAFKAWIRE_AUTH=true (SASL/PLAIN, credentials from WARP_AUTH_USER/WARP_AUTH_PASSWORD or WARP_AUTH_CREDENTIALS). Retention (retention.ms / retention.bytes) is applied every WARP_KAFKAWIRE_SWEEP_MS (300000, like log.retention.check.interval.ms); transactions are not supported.</>}
                {s.id === 'gremlin' && <> A vertex lives on one host (hash of its id) and an edge with its out-vertex; in-edge steps (in(), inE(), both()) and edge-by-id lookups scatter-gather over every host, so traversals stay correct across hosts (a multi-host write, such as dropping a vertex with in-edges elsewhere, is one transaction per host). WebSocket and HTTP Gremlin Server protocol on WARP_GREMLINWIRE_PORT (8182); no auth unless WARP_GREMLINWIRE_AUTH=true (SASL PLAIN / HTTP Basic, credentials from WARP_AUTH_CREDENTIALS).</>}
                {s.id === 'cosmos' && <> A document lives on one host (hash of database, container and partition key value); the database and container catalog lives on the first Cosmos host, and queries without a partition key scatter-gather over every host (ORDER BY, TOP, OFFSET/LIMIT and aggregates are merged in Warp). Cosmos DB for NoSQL REST API on WARP_COSMOSWIRE_PORT (18081, plain HTTP); master-key auth with the emulator's well-known key unless WARP_COSMOSWIRE_KEYS is set. Stored procedures, triggers and UDFs are stored but never executed (no JavaScript).</>}
                {s.id === 'amqp' && <> A queue and all its messages live wholly on one host (hash of vhost and queue name); exchanges and bindings live on the first AMQP host and a publish is copied into the queue of every matching binding, over several hosts through a durable outbox. AMQP 0-9-1 (RabbitMQ clients) and AMQP 1.0 (Service Bus, ActiveMQ, qpid clients) share WARP_AMQPWIRE_PORT (5672); login is accepted when WARP_AMQPWIRE_AUTH is unset and no WARP_AUTH_CREDENTIALS are configured, otherwise PLAIN against them. Vhosts: WARP_AMQPWIRE_VHOSTS (default /, * for any). Adding a host re-hashes the queues, see rebalanceRequired.</>}
                {s.id === 'gcs' && <> Objects are placed by hash of bucket and object name; the bucket list and HMAC keys live on the first GCS host. Auth: WARP_GCSWIRE_TOKENS (bearer), WARP_GCSWIRE_ALLOW_ANONYMOUS, HMAC keys for the XML API.</>}
                {s.id === 'pubsub' && <> Each subscription queue lives wholly on one host (hash of its name); Publish copies each message into every subscription of the topic. Topics, subscriptions and snapshots are listed from the first host. gRPC on WARP_PUBSUBWIRE_PORT (8085), REST on WARP_PUBSUBWIRE_REST_PORT (8086); no auth unless WARP_PUBSUBWIRE_TOKENS is set.</>}
                {s.id === 'sns' && <> A topic and its subscriptions live wholly on one host (hash of the topic name). Delivery into SQS queues needs sqswire in the same Warp; HTTP(S) endpoints get SNS-style POSTs.</>}
                {s.id === 'kinesis' && <> A stream (shards, records, consumers) lives wholly on one host (hash of the stream name); records past the retention period are swept every minute.</>}
                {s.id === 'awsparams' && <> Secrets and SSM parameters are placed by hash of their name and KMS keys by hash of their id; aliases, STS sessions and IAM roles live on the first host. KMS needs WARP_KMS_MASTER_KEY (or WARP_KMS_INSECURE_DEV_KEY=true for development): secrets and SecureString values are sealed with it.</>}
                {s.id === 's3' && <> Limits: 5 GiB per PUT, keys up to 1024 bytes; the bucket list lives on the first S3 host. For very large objects use s3wire proxy mode instead.</>}
                {editing && wasEnabled.includes(s.id) && !checked && <> Disabling keeps the data in this database but Warp stops serving it.</>}
              </div>
            </li>
          )
        })}
      </ul>
    </fieldset>
  )
}

export function BackendEditor({ set, editing, stores, onDone, onCancel }: {
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
          : <Notice tone="muted">Only Postgres backends can host protocol stores ({stores.map((s) => s.label).join(', ')}).</Notice>}

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

export function StoreTags({ backend, set, stores }: { backend: SetBackend; set: BackendSetInfo; stores: StoreInfo[] }) {
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

export function Health({ b, probe, busy }: { b: SetBackend; probe?: BackendTestResult; busy: boolean }) {
  if (busy) return <StatusPill tone="muted">Testing</StatusPill>
  const h = probe ?? b.health
  if (h) return <span title={h.message}><StatusPill tone={h.ok ? 'ok' : 'bad'}>{h.ok ? `Healthy · ${h.tookMs} ms` : 'Unreachable'}</StatusPill></span>
  if (b.state !== 'ACTIVE') return <StatusPill tone="warn">{b.state.toLowerCase()}</StatusPill>
  return <span className={styles.sub}>Checking…</span>
}

/** Connection examples for the frontends this Warp is really listening on (ports come from GET /api/interfaces). */
const HOST = '<warp-host>'
function connectSnippets(db: string, ports: Record<string, number>): Array<{ label: string; code: string }> {
  const out: Array<{ label: string; code: string }> = []
  const pick = (...ids: string[]) => ids.map((i) => ports[i]).find((p) => p !== undefined)
  const pg = pick('pgwire'); const my = pick('mywire', 'mywire-native'); const ms = pick('mssqlwire', 'mssqlwire-native')
  const ora = pick('orawire', 'orawire-native'); const mongo = pick('mongowire'); const bolt = pick('boltwire'); const grpc = pick('grpc')
  if (pg) out.push({ label: 'psql', code: `psql "host=${HOST} port=${pg} dbname=${db} user=<user>"` }, { label: 'JDBC (PostgreSQL)', code: `jdbc:postgresql://${HOST}:${pg}/${db}` })
  if (my) out.push({ label: 'mysql', code: `mysql -h ${HOST} -P ${my} -D ${db} -u <user> -p` }, { label: 'JDBC (MySQL)', code: `jdbc:mysql://${HOST}:${my}/${db}` })
  if (ms) out.push({ label: 'sqlcmd', code: `sqlcmd -S ${HOST},${ms} -d ${db} -U <user>` }, { label: 'JDBC (SQL Server)', code: `jdbc:sqlserver://${HOST}:${ms};databaseName=${db}` })
  if (ora) out.push({ label: 'sqlplus', code: `sqlplus <user>@//${HOST}:${ora}/${db}` }, { label: 'JDBC (Oracle thin)', code: `jdbc:oracle:thin:@//${HOST}:${ora}/${db}` })
  if (mongo) out.push({ label: 'mongosh', code: `mongosh "mongodb://${HOST}:${mongo}/${db}"` })
  if (bolt) out.push({ label: 'Neo4j driver', code: `driver.session(database="${db}")  // bolt://${HOST}:${bolt}` })
  if (grpc) out.push({ label: 'gRPC', code: `ExecuteRequest(database="${db}", ...)  // ${HOST}:${grpc}` })
  return out
}

/** "Connect with database = <name>" for a backend or a set: the name to copy, and per-driver examples. */
export function ConnectPanel({ setName, connectAs, backends, ports }: { setName: string; connectAs: string | null; backends: SetBackend[]; ports: Record<string, number> }) {
  const targets = [
    ...(connectAs ? [{ value: connectAs, label: `Whole set — ${connectAs}` }] : []),
    ...backends.map((b) => ({ value: b.connectAs, label: `Backend — ${b.connectAs}` })),
  ]
  const [chosen, setChosen] = useState(targets[0]?.value ?? '')
  const active = targets.some((t) => t.value === chosen) ? chosen : targets[0]?.value ?? ''
  const selectId = `connect-target-${setName}`
  return (
    <details className={styles.connect}>
      <summary>How to connect to this set or one of its backends</summary>
      <div className={styles.connectBody}>
        <p className={styles.help}>
          Set the <strong>database</strong> (or Oracle <strong>service name</strong>) your driver already sends to the name below — no client
          code changes. A backend name reaches only that backend; a set name reaches only the backends of that set.
          {!connectAs && <> This set&apos;s name is also a backend name, so a bare <code>{setName}</code> reaches the backend; add a route below to reach the set.</>}
        </p>
        <div className={styles.connectPick}>
          <label htmlFor={selectId} className={styles.sub}>Connect to</label>
          <select id={selectId} value={active} onChange={(e) => setChosen(e.target.value)}>
            {targets.map((t) => <option key={t.value} value={t.value}>{t.label}</option>)}
          </select>
        </div>
        <ul className={styles.snippets}>
          {connectSnippets(active, ports).map((sn) => (
            <li key={sn.label} className={styles.snippet}>
              <span className={styles.snippetLabel}>{sn.label}</span>
              <code className={styles.snippetCode}>{sn.code}</code>
              <CopyButton text={sn.code} label={`Copy ${sn.label} example`} />
            </li>
          ))}
        </ul>
      </div>
    </details>
  )
}

const PROTOCOLS = ['postgres', 'mysql', 'sqlserver', 'oracle', 'mongodb', 'bolt', 'grpc'] as const

const MODE_NOTE: Record<ConnectionRouting['mode'], string> = {
  implicit: 'A database name that is neither a backend, a set, nor a route falls back to Warp’s normal routing (not an error). Set WARP_CONNECT_ROUTING=strict to reject it instead.',
  strict: 'Strict mode: a database name that is not a backend, a set or a route is rejected with the protocol’s own “unknown database” error.',
  off: 'Connect-time routing is switched off (WARP_CONNECT_ROUTING=off): database names and routes are ignored.',
}

/** Explicit connect-time routes: first match wins; they take precedence over backend / set names. */
export function RoutesSection({ data, onChanged }: { data: BackendSetsResponse; onChanged: () => void }) {
  const routing = data.connectionRouting
  const [protocol, setProtocol] = useState('')
  const [database, setDatabase] = useState('')
  const [user, setUser] = useState('')
  const [target, setTarget] = useState('')
  const [defaultBackend, setDefaultBackend] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [message, setMessage] = useState<string | null>(null)

  const allBackends = data.sets.flatMap((s) => s.backends)
  const targetSet = data.sets.find((s) => `set:${s.name}` === target)
  const isSet = !!targetSet

  async function add(e: React.FormEvent) {
    e.preventDefault()
    setBusy(true); setError(null); setMessage(null)
    try {
      await addConnectionRoute({
        database: database.trim(), target,
        ...(protocol ? { protocol } : {}), ...(user.trim() ? { user: user.trim() } : {}),
        ...(isSet && defaultBackend ? { defaultBackend } : {}),
      })
      setMessage(`Added route ${database.trim()} → ${target}. Every Warp instance picks it up within a moment.`)
      setDatabase(''); setUser(''); setDefaultBackend('')
      onChanged()
    } catch (e2) { setError(errorText(e2)) } finally { setBusy(false) }
  }

  async function remove(r: ConnectionRoute) {
    setBusy(true); setError(null); setMessage(null)
    try {
      await deleteConnectionRoute(r.id)
      setMessage(`Removed route ${r.database} → ${r.target}.`)
      onChanged()
    } catch (e2) { setError(errorText(e2)) } finally { setBusy(false) }
  }

  return (
    <Section flush title="Connection routes" meta={`${routing.routes.length} route${routing.routes.length === 1 ? '' : 's'} · mode ${routing.mode}`}>
      <div className={styles.toolbar}>
        <span className={styles.setDesc}>
          Routes map the database name a client sends (exact or <code>*</code>/<code>?</code> pattern, optionally per protocol and login user) to a
          backend or set. The first matching route wins and beats a backend or set of the same name. {MODE_NOTE[routing.mode]}
        </span>
      </div>
      {routing.routes.length === 0 ? (
        <EmptyState icon={<RouteIcon size={18} aria-hidden="true" />} title="No explicit routes">
          Backend and set names already work as database names. Add a route to give a database name of your own, or to choose a backend by login user.
        </EmptyState>
      ) : (
        <DataTable caption="Connection routes, in match order" minWidth={720}>
          <thead>
            <tr><th>Database</th><th>Protocol</th><th>User</th><th>Target</th><th>Set default</th><th aria-label="Actions"></th></tr>
          </thead>
          <tbody>
            {routing.routes.map((r) => (
              <tr key={r.id}>
                <td className={styles.mono}>{r.database}</td>
                <td>{r.protocol ?? <span className={styles.sub}>any</span>}</td>
                <td className={styles.mono}>{r.user ?? <span className={styles.sub}>any</span>}</td>
                <td><span className={styles.mono}>{r.target}</span> <Tag>{r.targetKind}</Tag></td>
                <td className={styles.mono}>{r.defaultBackend ?? <span className={styles.sub}>—</span>}</td>
                <td>
                  <div className={styles.actions}>
                    <IconButton danger label={`Remove route ${r.database}`} disabled={busy} onClick={() => remove(r)}><Trash2 size={16} strokeWidth={1.8} /></IconButton>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </DataTable>
      )}
      <form onSubmit={add} aria-label="Add connection route" className={styles.routeForm}>
        <div className={styles.formGrid}>
          <Field label="Database or service name" hint="Exact, or a pattern such as sales_*">
            {(id) => <input id={id} value={database} onChange={(e) => setDatabase(e.target.value)} required autoComplete="off" className={styles.monoInput} />}
          </Field>
          <Field label="Route to" hint="A backend, or a whole set.">
            {(id) => (
              <select id={id} value={target} onChange={(e) => { setTarget(e.target.value); setDefaultBackend('') }} required>
                <option value="" disabled>Choose a target…</option>
                <optgroup label="Backends">
                  {allBackends.map((b) => <option key={b.name} value={`db:${b.name}`}>{b.name}</option>)}
                </optgroup>
                <optgroup label="Sets">
                  {data.sets.map((s) => <option key={s.name} value={`set:${s.name}`}>{s.name}</option>)}
                </optgroup>
              </select>
            )}
          </Field>
          <Field label="Protocol" hint="Optional; default: every protocol.">
            {(id) => (
              <select id={id} value={protocol} onChange={(e) => setProtocol(e.target.value)}>
                <option value="">Any protocol</option>
                {PROTOCOLS.map((p) => <option key={p} value={p}>{p}</option>)}
              </select>
            )}
          </Field>
          <Field label="Login user" hint="Optional; exact or pattern. Not authentication.">
            {(id) => <input id={id} value={user} onChange={(e) => setUser(e.target.value)} autoComplete="off" className={styles.monoInput} />}
          </Field>
          {isSet && (
            <Field label="Default backend of the set" hint="Where statements no routing rule claims run. Blank: the set’s first backend.">
              {(id) => (
                <select id={id} value={defaultBackend} onChange={(e) => setDefaultBackend(e.target.value)}>
                  <option value="">First backend</option>
                  {targetSet!.backends.map((b) => <option key={b.name} value={b.name}>{b.name}</option>)}
                </select>
              )}
            </Field>
          )}
        </div>
        {error && <Notice tone="bad">{error}</Notice>}
        {message && <Notice tone="ok">{message}</Notice>}
        <div className={styles.formActions}>
          <Button variant="primary" type="submit" disabled={busy || !database.trim() || !target}>{busy ? 'Saving…' : 'Add route'}</Button>
        </div>
      </form>
    </Section>
  )
}

/** Legacy router settings kept for existing configs: router aliases (WARP_BACKEND_SETS -- a name
 * for a list of backends usable in router rules) and the legacy WARP_SHARD_BACKENDS shard group. */
export function AdvancedRouting() {
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
