import { useEffect, useState } from 'react'
import {
  type Connection, type MigrationSourceStatus,
  listConnections, getMigrationStatus,
} from '../api/client'

const POLL_INTERVAL_MS = 5000

function progressPct(status: MigrationSourceStatus): number {
  if (status.partitionsTotal === 0) return status.eventsApplied > 0 ? 100 : 0
  return Math.round((status.partitionsDone / status.partitionsTotal) * 100)
}

function formatLag(seconds: number | null): string {
  if (seconds === null) return '—'
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  return `${Math.round(seconds / 3600)}h`
}

function lagColor(seconds: number | null): string {
  if (seconds === null) return 'var(--muted)'
  if (seconds < 30) return 'var(--accent-strong)'
  if (seconds < 300) return 'var(--medium)'
  return 'var(--hard)'
}

function formatTimestamp(iso: string | null): string {
  if (!iso) return '—'
  try {
    return new Date(iso).toLocaleString()
  } catch {
    return iso
  }
}

/**
 * The "Web Progress Report" for nexagres-migration runs -- reads the same bookkeeping tables
 * (polywire_cdc_checkpoints, migration_partition_leases) the migration workers themselves read
 * and write, via GET /api/migration/status. Polls rather than pushing: a migration run can span
 * hours across separate worker processes Advisor has no direct handle on, so a short poll interval
 * is simpler and correct enough for a status dashboard, not a substitute for real alerting.
 */
export default function DataSync() {
  const [connections, setConnections] = useState<Connection[]>([])
  const [selectedConnection, setSelectedConnection] = useState('')
  const [statuses, setStatuses] = useState<MigrationSourceStatus[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    listConnections().then(setConnections).catch(() => {})
  }, [])

  useEffect(() => {
    if (!selectedConnection) {
      setStatuses([])
      return
    }
    let cancelled = false
    async function refresh() {
      try {
        const result = await getMigrationStatus(selectedConnection)
        if (!cancelled) { setStatuses(result); setError(null) }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    setLoading(true)
    refresh()
    const interval = setInterval(refresh, POLL_INTERVAL_MS)
    return () => { cancelled = true; clearInterval(interval) }
  }, [selectedConnection])

  return (
    <div style={{ maxWidth: 980 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Data Sync</h1>
      <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 0, marginBottom: 20 }}>
        Live progress for massively-parallel migration runs (nexagres-migration) writing into
        Polywire over its own gRPC driver. Select the TARGET Postgres connection a migration is
        pointed at — the same connection Polywire itself uses as its backend — to see each
        source's initial-sync progress and change-feed activity.
      </p>

      <div className="panel" style={{ marginBottom: 20 }}>
        <select
          value={selectedConnection}
          onChange={(e) => setSelectedConnection(e.target.value)}
          style={{ width: '100%', maxWidth: 360, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 12px', color: 'var(--text)', fontSize: 14 }}
        >
          <option value="">Select the target Postgres connection…</option>
          {connections.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
      </div>

      {error && <p style={{ color: 'var(--hard)' }}>{error}</p>}

      {selectedConnection && !error && loading && statuses.length === 0 && (
        <p style={{ color: 'var(--muted)', fontSize: 14 }}>Loading…</p>
      )}

      {selectedConnection && !error && !loading && statuses.length === 0 && (
        <p style={{ color: 'var(--muted)', fontSize: 14 }}>
          No migration has written to this target yet — this is empty until a worker process
          (see the migration module's CLI) has run against it at least once.
        </p>
      )}

      {statuses.length > 0 && (
        <div className="panel" style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13.5 }}>
            <thead>
              <tr style={{ textAlign: 'left', color: 'var(--muted)', fontSize: 12, textTransform: 'uppercase', letterSpacing: 0.4 }}>
                <th style={{ padding: '8px 10px' }}>Source</th>
                <th style={{ padding: '8px 10px' }}>Initial sync</th>
                <th style={{ padding: '8px 10px' }}>Events applied (change feed)</th>
                <th style={{ padding: '8px 10px' }}>Lag</th>
                <th style={{ padding: '8px 10px' }}>Last checkpoint</th>
                <th style={{ padding: '8px 10px' }}>Change-feed leader</th>
              </tr>
            </thead>
            <tbody>
              {statuses.map((s) => (
                <tr key={s.sourceKey} style={{ borderTop: '1px solid var(--border)' }}>
                  <td style={{ padding: '10px', fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace' }}>{s.sourceKey}</td>
                  <td style={{ padding: '10px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <div style={{ flex: 1, minWidth: 80, height: 6, borderRadius: 3, background: 'var(--bg)', border: '1px solid var(--border)', overflow: 'hidden' }}>
                        <div style={{ width: `${progressPct(s)}%`, height: '100%', background: progressPct(s) === 100 ? 'var(--accent-strong)' : 'var(--medium)' }} />
                      </div>
                      <span style={{ color: 'var(--muted)', fontVariantNumeric: 'tabular-nums', whiteSpace: 'nowrap' }}>
                        {s.partitionsTotal > 0 ? `${s.partitionsDone}/${s.partitionsTotal} partitions` : `${progressPct(s)}%`}
                      </span>
                    </div>
                  </td>
                  <td style={{ padding: '10px', fontVariantNumeric: 'tabular-nums' }}>{s.eventsApplied.toLocaleString()}</td>
                  <td style={{ padding: '10px', fontVariantNumeric: 'tabular-nums', color: lagColor(s.lagSeconds), fontWeight: 600 }}>{formatLag(s.lagSeconds)}</td>
                  <td style={{ padding: '10px', color: 'var(--muted)' }}>{formatTimestamp(s.lastCheckpointAt)}</td>
                  <td style={{ padding: '10px', color: 'var(--muted)' }}>{s.leaderWorkerId ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
