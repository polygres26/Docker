import { useEffect, useState } from 'react'
import { type Connection, type MigrationSourceStatus, listConnections, getMigrationStatus } from '../api/client'
import DmsTabs from '../components/DmsTabs'
import { getLastTargetConnectionId, setLastTargetConnectionId } from '../lib/lastTargetConnection'
import {
  MIGRATION_SERVICE_TABS, inputStyle, labelStyle, formatTimestamp, formatLag, lagColor, progressPct,
} from './migrationServiceShared'

const POLL_INTERVAL_MS = 5000

/**
 * Migration Service's "Status" tab -- the "Web Progress Report" for nexagres-migration runs,
 * reading the same bookkeeping tables (polywire_cdc_checkpoints, migration_partition_leases) the
 * migration workers themselves read and write, via GET /api/migration/status. Defaults its
 * connection picker to whichever target the Launch tab was last pointed at (see
 * lastTargetConnection.ts) so starting a job and tracking it feels like one continuous flow
 * across the two tabs, not two disconnected pages that happen to share a sidebar entry.
 */
export default function MigrationServiceStatus() {
  const [connections, setConnections] = useState<Connection[]>([])
  const [selectedConnection, setSelectedConnection] = useState('')
  const [statuses, setStatuses] = useState<MigrationSourceStatus[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    listConnections().then((cs) => {
      setConnections(cs)
      const remembered = getLastTargetConnectionId()
      if (remembered && cs.some((c) => c.id === remembered)) {
        setSelectedConnection(remembered)
      }
    }).catch(() => {})
  }, [])

  function handleSelect(id: string) {
    setSelectedConnection(id)
    if (id) setLastTargetConnectionId(id)
  }

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
    <div style={{ maxWidth: 1080 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Migration Service</h1>
      <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 0, marginBottom: 16 }}>
        Live initial-sync progress and change-feed activity for every source checkpointed against
        a target Postgres connection.
      </p>
      <DmsTabs tabs={MIGRATION_SERVICE_TABS} />

      <div className="panel" style={{ margin: '16px 0 20px' }}>
        <label style={labelStyle}>Target Postgres connection</label>
        <select
          value={selectedConnection}
          onChange={(e) => handleSelect(e.target.value)}
          style={{ ...inputStyle, maxWidth: 360 }}
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
          No migration has written to this target yet — start one on the Launch tab, or run the
          migration module's CLI directly against it.
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
