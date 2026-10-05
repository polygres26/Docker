import { useCallback, useMemo, useState } from 'react'
import { Plus, RefreshCw, Trash2 } from 'lucide-react'
import {
  MAX_REPLICA_LAG_SECONDS, evaluateFailover, getFailover, getReplicas, listBackendSets, switchoverPrimary, updateBackendReplicas,
  type FailoverEvent, type FailoverGroup, type FailoverMode, type FailoverNode, type ObservedRole, type ReplicaConfig,
  type ReplicaGroupStatus, type ReplicaStatus, type SetBackend,
} from '../api/client'
import {
  Button, DataTable, EmptyState, Field, IconButton, KpiStrip, Loading, Meter, Notice, PageHeader, Section, StatusPill,
  type KpiItem, type Tone,
} from '../components/ui'
import { errorText, targetOf, useLoad } from '../hooks'
import styles from './Replicas.module.css'

const POLL_MS = 5_000

/** Engines with replica probes (Postgres, MySQL, Oracle, SQL Server) and the subset Warp can promote itself. */
const REPLICA_DIALECTS = new Set(['POSTGRES', 'MYSQL', 'ORACLE', 'SQL_SERVER'])
const PROMOTE_DIALECTS = new Set(['POSTGRES', 'MYSQL'])

const REASONS: Array<[keyof ReplicaGroupStatus['decisions'], string, Tone]> = [
  ['not_read_safe', 'Not a plain read', 'muted'],
  ['recent_write', 'Recent write', 'muted'],
  ['session_state', 'Session has state', 'muted'],
  ['no_eligible_replica', 'No eligible replica', 'warn'],
  ['replica_retried_on_primary', 'Replica failed, retried', 'bad'],
]

const ATTENTION_EVENTS = new Set(['split-brain-suspected', 'promote-blocked', 'no-writable-node'])

const MODE_COPY: Record<FailoverMode, { title: string; body: string }> = {
  off: { title: 'Off', body: 'Do nothing when the primary fails.' },
  follow: { title: 'Follow', body: 'Use whichever node your HA tooling promotes.' },
  promote: { title: 'Promote', body: 'Warp promotes a replica. Needs a lease and a majority of Warp instances.' },
}

function allSetBackends(sets: Array<{ backends: SetBackend[] }>): Array<{ set: string; backend: SetBackend }> {
  return sets.flatMap((s) => s.backends.map((b) => ({ set: b.set, backend: b })))
}

function replicaState(r: ReplicaStatus): { tone: Tone; label: string; note?: string } {
  if (r.quarantined) return { tone: 'warn', label: 'Quarantined', note: 'Paused after a failed read' }
  if (!r.sample) return { tone: 'muted', label: 'Not measured yet' }
  if (!r.sample.ok) return { tone: 'bad', label: 'Unmeasurable', note: r.sample.message ?? undefined }
  if (!r.sample.isReplica) return { tone: 'warn', label: 'Not a replica', note: 'This node is not replicating' }
  if (r.sample.lagSeconds > r.maxLagSeconds) return { tone: 'warn', label: 'Too far behind' }
  if (r.eligible) return { tone: 'ok', label: 'Eligible' }
  return { tone: 'muted', label: 'Sample is stale', note: `Measured ${Math.round(r.sample.ageSeconds)} s ago` }
}

function roleState(role: ObservedRole | null): { tone: Tone; label: string } {
  switch (role) {
    case 'WRITABLE': return { tone: 'ok', label: 'Writable' }
    case 'READ_ONLY': return { tone: 'warn', label: 'Read-only' }
    case 'UNREACHABLE': return { tone: 'bad', label: 'Unreachable' }
    default: return { tone: 'muted', label: 'Not probed yet' }
  }
}

const seconds = (v: number) => (Number.isInteger(v) ? String(v) : v.toFixed(1))

const EVENT_LABEL: Record<string, { tone: Tone; label: string }> = {
  switched: { tone: 'ok', label: 'Switched' },
  promoted: { tone: 'ok', label: 'Promoted by Warp' },
  'promote-blocked': { tone: 'bad', label: 'Promotion blocked' },
  'split-brain-suspected': { tone: 'warn', label: 'Split brain suspected' },
  'no-writable-node': { tone: 'bad', label: 'No writable node' },
  'switch-suppressed': { tone: 'muted', label: 'Switch held back' },
  unsupported: { tone: 'muted', label: 'Not monitored' },
  switchover: { tone: 'ok', label: 'Planned switchover' },
  'switchover-aborted': { tone: 'warn', label: 'Switchover aborted' },
  'switchover-failed': { tone: 'bad', label: 'Switchover failed' },
  repointed: { tone: 'ok', label: 'Replica repointed' },
  'repoint-failed': { tone: 'bad', label: 'Repoint failed' },
  'repoint-skipped': { tone: 'muted', label: 'Repoint skipped' },
}

/**
 * Replicas and failover. Reads the live router (/api/replicas), the failover monitor (/api/failover) and the
 * backend sets (for the editable configuration); nothing here is derived client-side except display state.
 */
export default function Replicas() {
  const load = useCallback(async () => {
    const [sets, replicas, failover] = await Promise.all([
      listBackendSets(false),
      getReplicas().catch(() => null),
      getFailover().catch(() => null),
    ])
    return { sets, replicas, failover }
  }, [])
  const state = useLoad(load, POLL_MS)
  const [editing, setEditing] = useState<string | null>(null)
  const [notice, setNotice] = useState<{ tone: 'ok' | 'bad' | 'warn'; text: string } | null>(null)

  const data = state.data
  const backends = useMemo(() => allSetBackends(data?.sets.sets ?? []), [data])
  const statusOf = (name: string) => data?.replicas?.primaries.find((p) => p.primary === name) ?? null
  const failoverOf = (name: string): FailoverGroup | null => data?.failover?.groups.find((g) => g.backend === name) ?? null

  const withReplicas = backends.filter((b) => b.backend.replicas.length > 0)
  // Replicas of the implicit WARP_* backend (WARP_REPLICAS) belong to no backend set, so they are not editable here
  const envGroups = (data?.replicas?.primaries ?? []).filter((p) => !backends.some((b) => b.backend.name === p.primary))
  const candidates = backends.filter((b) => b.backend.replicas.length === 0
    && b.backend.dialect !== null && REPLICA_DIALECTS.has(b.backend.dialect))

  const kpis: KpiItem[] = useMemo(() => {
    const groups = data?.replicas?.primaries ?? []
    const all = groups.flatMap((g) => g.replicas)
    const routed = groups.reduce((n, g) => n + (g.decisions.routed_to_replica ?? 0), 0)
    const total = groups.reduce((n, g) => n + Object.values(g.decisions).reduce((a, b) => a + (b ?? 0), 0), 0)
    const latest = new Map<string, FailoverEvent>()
    for (const e of data?.failover?.events ?? []) if (!latest.has(e.backend)) latest.set(e.backend, e)
    const attention = [...latest.values()].filter((e) => ATTENTION_EVENTS.has(e.kind)).length
    return [
      { label: 'Backends with replicas', value: withReplicas.length + envGroups.length },
      { label: 'Replicas eligible for reads', value: `${all.filter((r) => r.eligible).length} / ${all.length}`,
        tone: all.length > 0 && !all.some((r) => r.eligible) ? 'warn' : undefined },
      { label: 'Reads served by replicas', value: total > 0 ? `${Math.round((routed / total) * 100)}%` : '—', hint: 'since this node started' },
      { label: 'Failover', value: attention > 0 ? `${attention} need attention` : 'Healthy', tone: attention > 0 ? 'warn' : 'ok' },
    ]
  }, [data, withReplicas.length, envGroups.length])

  if (state.loading && !data) return <Loading />

  return (
    <div className={styles.stack}>
      <PageHeader title="Replicas and failover"
        description="Send reads to replicas that are caught up, and keep writes flowing when a primary fails."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={state.reload}>Refresh</Button>} />
      {state.error && <Notice tone="bad">Could not load: {state.error}</Notice>}
      {notice && <Notice tone={notice.tone}>{notice.text}</Notice>}
      {data?.replicas && !data.replicas.enabled && (
        <Notice tone="warn">Replica reads are turned off on this node (WARP_REPLICA_READ_ROUTING=false or
          WARP_REPLICA_LAG_CHECK_SECONDS=0). Reads stay on the primary.</Notice>
      )}
      <KpiStrip items={kpis} label="Replica figures" />

      {withReplicas.length === 0 && envGroups.length === 0 && (
        <Section>
          <EmptyState title="No backend has replicas yet">
            Add a replica to a backend below. Reads then go to it while it is within its lag allowance.
          </EmptyState>
        </Section>
      )}

      {withReplicas.map(({ set, backend }) => (
        <GroupCard key={backend.name} set={set} backend={backend} status={statusOf(backend.name)}
          failover={failoverOf(backend.name)} events={(data?.failover?.events ?? []).filter((e) => e.backend === backend.name)}
          failoverConfig={data?.failover ?? null}
          editing={editing === backend.name}
          onEdit={() => { setEditing(editing === backend.name ? null : backend.name); setNotice(null) }}
          onSaved={(text) => { setEditing(null); setNotice({ tone: 'ok', text }); state.reload() }}
          onNotice={(tone, text) => setNotice({ tone, text })} />
      ))}

      {envGroups.map((g) => <EnvGroupCard key={g.primary} status={g} />)}

      {candidates.length > 0 && (
        <Section title="Backends without replicas" meta="Postgres, MySQL, Oracle and SQL Server">
          <div>
            {candidates.map(({ set, backend }) => (
              <div key={backend.name}>
                <div className={styles.pad} style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                  <div style={{ flex: 1 }}>
                    <strong>{backend.name}</strong>
                    <div className={styles.sub}>{backend.type} · set {set} · <span className={styles.mono}>{targetOf(backend.url)}</span></div>
                  </div>
                  <Button icon={<Plus size={14} aria-hidden="true" />}
                    onClick={() => { setEditing(editing === backend.name ? null : backend.name); setNotice(null) }}>
                    Add replicas
                  </Button>
                </div>
                {editing === backend.name && (
                  <ReplicaEditor set={set} backend={backend}
                    onCancel={() => setEditing(null)}
                    onSaved={(text) => { setEditing(null); setNotice({ tone: 'ok', text }); state.reload() }} />
                )}
              </div>
            ))}
          </div>
        </Section>
      )}
    </div>
  )
}

function GroupCard({ set, backend, status, failover, events, failoverConfig, editing, onEdit, onSaved, onNotice }: {
  set: string; backend: SetBackend; status: ReplicaGroupStatus | null; failover: FailoverGroup | null
  events: FailoverEvent[]; failoverConfig: { probeSeconds: number; confirmProbes: number; cooldownSeconds: number } | null
  editing: boolean; onEdit: () => void; onSaved: (text: string) => void; onNotice: (tone: 'ok' | 'bad' | 'warn', text: string) => void
}) {
  const [busy, setBusy] = useState(false)
  const [confirming, setConfirming] = useState<string | null>(null)
  const canSwitch = backend.failoverMode !== 'off' && backend.dialect !== null && PROMOTE_DIALECTS.has(backend.dialect)
  const primaryNode: FailoverNode | undefined = failover?.nodes.find((n) => n.configuredRole === 'primary')
  const primaryState = roleState(primaryNode?.observedRole ?? null)
  const decisions = status?.decisions ?? {}

  const evaluate = async () => {
    setBusy(true)
    try {
      const r = await evaluateFailover(backend.name)
      onNotice(r.action === 'SWITCH' || r.action === 'PROMOTE' ? 'warn' : 'ok', `${backend.name}: ${r.action.toLowerCase().replace('_', ' ')} — ${r.reason}`)
    } catch (e) {
      onNotice('bad', errorText(e))
    } finally {
      setBusy(false)
    }
  }

  const switchover = async (target: string) => {
    setBusy(true)
    try {
      const r = await switchoverPrimary(backend.name, target)
      onNotice('ok', `${backend.name}: ${r.message}`)
    } catch (e) {
      onNotice('bad', `${backend.name}: ${errorText(e)}`)
    } finally {
      setConfirming(null)
      setBusy(false)
    }
  }

  return (
    <Section title={backend.name}
      meta={`${backend.type} · set ${set} · failover ${backend.failoverMode}`}
      flush>
      <div className={styles.pad} style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', flexWrap: 'wrap' }}>
        {backend.failoverMode !== 'off' && (
          <Button onClick={evaluate} disabled={busy}>Evaluate now</Button>
        )}
        <Button onClick={onEdit}>{editing ? 'Close editor' : 'Edit replicas'}</Button>
      </div>
      <DataTable caption={`Nodes of ${backend.name}`} minWidth={640}>
        <thead><tr><th>Node</th><th>Role</th><th>Lag vs allowance</th><th>Reads served</th><th>State</th>{canSwitch && <th>Action</th>}</tr></thead>
        <tbody>
          <tr>
            <td className={styles.mono}>{targetOf(backend.url)}</td>
            <td><StatusPill tone="accent" dot={false}>Primary</StatusPill></td>
            <td className={styles.sub}>n/a</td>
            <td className={styles.num}>—</td>
            <td><StatusPill tone={primaryState.tone}>{primaryState.label}</StatusPill></td>
            {canSwitch && <td />}
          </tr>
          {(status?.replicas ?? backend.replicas.map((r): ReplicaStatus => ({
            id: r.url, url: r.url, maxLagSeconds: r.maxLagSeconds, sample: null, eligible: false, quarantined: false, routedReads: 0,
          }))).map((r) => {
            const st = replicaState(r)
            const lag = r.sample && r.sample.ok && r.sample.isReplica ? r.sample.lagSeconds : null
            return (
              <tr key={r.id}>
                <td className={styles.mono}>{targetOf(r.url)}</td>
                <td><StatusPill tone="muted" dot={false}>Replica</StatusPill></td>
                <td>
                  {lag === null
                    ? <span className={styles.sub}>allowance {seconds(r.maxLagSeconds)} s</span>
                    : <Meter value={lag} max={Math.max(r.maxLagSeconds, 0.001)} label={`Lag of ${targetOf(r.url)}`}
                      tone={lag > r.maxLagSeconds ? 'warn' : 'ok'}
                      caption={`${seconds(Math.round(lag * 10) / 10)} s of ${seconds(r.maxLagSeconds)} s`} />}
                </td>
                <td className={styles.num}>{r.routedReads.toLocaleString()}</td>
                <td>
                  <StatusPill tone={st.tone}>{st.label}</StatusPill>
                  {st.note && <div className={styles.sub}>{st.note}</div>}
                </td>
                {canSwitch && (
                  <td>
                    {confirming === r.url ? (
                      <div role="group" aria-label={`Confirm switchover to ${targetOf(r.url)}`}>
                        <div className={styles.sub}>
                          Writes fail for a few seconds while {targetOf(r.url)} catches up and takes over.
                          {backend.dialect === 'POSTGRES'
                            ? ' The old primary is left read-only and must be rebuilt as a standby.'
                            : ' The old primary becomes a replica of the new one.'}
                        </div>
                        <div style={{ display: 'flex', gap: 8, marginTop: 6 }}>
                          <Button variant="primary" disabled={busy} onClick={() => switchover(r.url)}>
                            {busy ? 'Switching…' : 'Switch now'}
                          </Button>
                          <Button disabled={busy} onClick={() => setConfirming(null)}>Cancel</Button>
                        </div>
                      </div>
                    ) : (
                      <Button disabled={busy || confirming !== null} onClick={() => setConfirming(r.url)}>Make primary</Button>
                    )}
                  </td>
                )}
              </tr>
            )
          })}
        </tbody>
      </DataTable>

      <div className={styles.reasons} aria-label="Why reads stayed on the primary">
        <span className={styles.reasonsLabel}>Reads that stayed on the primary:</span>
        {REASONS.map(([k, label, tone]) => (
          <StatusPill key={k} tone={(decisions[k] ?? 0) > 0 ? tone : 'muted'} dot={false}>{label} · {(decisions[k] ?? 0).toLocaleString()}</StatusPill>
        ))}
      </div>

      {backend.failoverMode !== 'off' && failoverConfig && (
        <div className={styles.facts}>
          <div><small>Confirmation</small><span>{failoverConfig.confirmProbes} probes, every {failoverConfig.probeSeconds} s</span></div>
          <div><small>Cooldown between switches</small><span>{failoverConfig.cooldownSeconds} s</span></div>
          <div><small>Last switch</small><span>{failover?.lastSwitchAt ? new Date(failover.lastSwitchAt).toLocaleString() : 'Never'}</span></div>
        </div>
      )}

      {events.length > 0 && (
        <div className={styles.events}>
          <DataTable caption={`Failover events for ${backend.name}`} minWidth={560}>
            <tbody>
              {events.slice(0, 5).map((e) => {
                const l = EVENT_LABEL[e.kind] ?? { tone: 'muted' as Tone, label: e.kind }
                return (
                  <tr key={e.at + e.kind}>
                    <td className={styles.num}>{new Date(e.at).toLocaleTimeString()}</td>
                    <td><StatusPill tone={l.tone} dot={false}>{l.label}</StatusPill></td>
                    <td>{e.detail}</td>
                  </tr>
                )
              })}
            </tbody>
          </DataTable>
        </div>
      )}

      {editing && <ReplicaEditor set={set} backend={backend} onCancel={onEdit} onSaved={onSaved} />}
    </Section>
  )
}

/** Read-only card for replicas configured through WARP_REPLICAS on the implicit backend. */
function EnvGroupCard({ status }: { status: ReplicaGroupStatus }) {
  const decisions = status.decisions ?? {}
  return (
    <Section title="Default backend" meta="set by WARP_REPLICAS · failover off · read-only here" flush>
      <p className={styles.help} style={{ padding: '0 16px' }}>
        These replicas belong to the single implicit backend, which is not part of a backend set, so they are
        changed with the WARP_REPLICAS environment variable and a restart. Sessions with a per-user identity,
        open transactions or SET state always read from the primary.
      </p>
      <DataTable caption="Replicas of the default backend" minWidth={560}>
        <thead><tr><th>Node</th><th>Lag vs allowance</th><th>Reads served</th><th>State</th></tr></thead>
        <tbody>
          {status.replicas.map((r) => {
            const st = replicaState(r)
            const lag = r.sample && r.sample.ok && r.sample.isReplica ? r.sample.lagSeconds : null
            return (
              <tr key={r.id}>
                <td className={styles.mono}>{targetOf(r.url)}</td>
                <td>
                  {lag === null
                    ? <span className={styles.sub}>allowance {seconds(r.maxLagSeconds)} s</span>
                    : <Meter value={lag} max={Math.max(r.maxLagSeconds, 0.001)} label={`Lag of ${targetOf(r.url)}`}
                      tone={lag > r.maxLagSeconds ? 'warn' : 'ok'}
                      caption={`${seconds(Math.round(lag * 10) / 10)} s of ${seconds(r.maxLagSeconds)} s`} />}
                </td>
                <td className={styles.num}>{r.routedReads.toLocaleString()}</td>
                <td>
                  <StatusPill tone={st.tone}>{st.label}</StatusPill>
                  {st.note && <div className={styles.sub}>{st.note}</div>}
                </td>
              </tr>
            )
          })}
        </tbody>
      </DataTable>
      <div className={styles.reasons} aria-label="Why reads stayed on the primary">
        <span className={styles.reasonsLabel}>Reads that stayed on the primary:</span>
        {REASONS.map(([k, label, tone]) => (
          <StatusPill key={k} tone={(decisions[k] ?? 0) > 0 ? tone : 'muted'} dot={false}>{label} · {(decisions[k] ?? 0).toLocaleString()}</StatusPill>
        ))}
      </div>
    </Section>
  )
}

function ReplicaEditor({ set, backend, onCancel, onSaved }: {
  set: string; backend: SetBackend; onCancel: () => void; onSaved: (text: string) => void
}) {
  const [rows, setRows] = useState<Array<{ url: string; lag: string }>>(
    backend.replicas.length > 0
      ? backend.replicas.map((r) => ({ url: r.url, lag: String(r.maxLagSeconds) }))
      : [{ url: '', lag: '5' }])
  const [mode, setMode] = useState<FailoverMode>(backend.failoverMode === 'off' && backend.replicas.length === 0 ? 'follow' : backend.failoverMode)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const canPromote = backend.dialect !== null && PROMOTE_DIALECTS.has(backend.dialect)

  const setRow = (i: number, patch: Partial<{ url: string; lag: string }>) =>
    setRows((rs) => rs.map((r, j) => (j === i ? { ...r, ...patch } : r)))

  const save = async () => {
    const replicas: ReplicaConfig[] = []
    for (const [i, r] of rows.entries()) {
      if (r.url.trim() === '' && r.lag.trim() === '') continue
      if (!r.url.trim().toLowerCase().startsWith('jdbc:')) { setError(`Replica ${i + 1}: enter a full JDBC URL (it starts with jdbc:).`); return }
      const lag = Number(r.lag)
      if (r.lag.trim() === '' || !Number.isFinite(lag) || lag < 0 || lag > MAX_REPLICA_LAG_SECONDS) {
        setError(`Replica ${i + 1}: max lag must be a number of seconds between 0 and ${MAX_REPLICA_LAG_SECONDS}.`)
        return
      }
      replicas.push({ url: r.url.trim(), maxLagSeconds: lag })
    }
    setSaving(true)
    setError(null)
    try {
      const res = await updateBackendReplicas(set, backend.name, { replicas, failoverMode: replicas.length > 0 ? mode : null })
      onSaved(replicas.length > 0
        ? `Saved ${replicas.length} replica${replicas.length === 1 ? '' : 's'} for ${backend.name} (warp_config version ${res.version}). Every Warp instance applies it within a moment.`
        : `Removed the replicas of ${backend.name} (warp_config version ${res.version}).`)
    } catch (e) {
      setError(errorText(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className={styles.editor}>
      <p className={styles.help}>
        Replicas use this backend's user and password. A replica serves reads only while its replication lag, in seconds,
        is at or below its maximum. Use 0 to require a fully caught-up replica; the limit is {MAX_REPLICA_LAG_SECONDS} seconds.
        Leave the list empty to remove all replicas.
      </p>
      {rows.map((r, i) => (
        <div className={styles.replicaRow} key={i}>
          <Field label={`Replica ${i + 1} URL`}>
            {(id) => <input id={id} value={r.url} placeholder="jdbc:postgresql://replica-host:5432/db"
              onChange={(e) => setRow(i, { url: e.target.value })} />}
          </Field>
          <Field label="Max lag (seconds)">
            {(id) => <input id={id} className={styles.lagInput} type="number" min={0} max={MAX_REPLICA_LAG_SECONDS} step={0.5}
              value={r.lag} onChange={(e) => setRow(i, { lag: e.target.value })} />}
          </Field>
          <IconButton label={`Remove replica ${i + 1}`} danger onClick={() => setRows((rs) => rs.filter((_, j) => j !== i))}>
            <Trash2 size={14} aria-hidden="true" />
          </IconButton>
        </div>
      ))}
      <div>
        <Button icon={<Plus size={14} aria-hidden="true" />} onClick={() => setRows((rs) => [...rs, { url: '', lag: '5' }])}>Add replica</Button>
      </div>

      <fieldset style={{ border: 0, padding: 0, margin: 0 }}>
        <legend className={styles.help}>When the primary fails</legend>
        <div className={styles.modes}>
          {(['off', 'follow', 'promote'] as FailoverMode[]).map((m) => {
            const disabled = m === 'promote' && !canPromote
            return (
              <label key={m} className={`${styles.mode} ${mode === m ? styles.modeOn : ''} ${disabled ? styles.modeOff : ''}`}>
                <span><input type="radio" name="failover-mode" checked={mode === m} disabled={disabled} onChange={() => setMode(m)} />
                  <strong>{MODE_COPY[m].title}</strong></span>
                <span className={styles.sub}>
                  {disabled ? 'Warp can follow this engine but does not promote it; use its own failover tooling.' : MODE_COPY[m].body}
                </span>
              </label>
            )
          })}
        </div>
        {mode === 'promote' && (
          <p className={styles.help}>Promotion refuses to run if the config database is unreachable. Set WARP_FAILOVER_FENCE_COMMAND
            to power off or block the old primary; without it only the majority rule guards against a partitioned primary.</p>
        )}
      </fieldset>

      {error && <Notice tone="bad">{error}</Notice>}
      <div className={styles.actions}>
        <Button onClick={onCancel}>Cancel</Button>
        <Button variant="primary" onClick={save} disabled={saving}>{saving ? 'Saving…' : 'Save replicas'}</Button>
      </div>
    </div>
  )
}
