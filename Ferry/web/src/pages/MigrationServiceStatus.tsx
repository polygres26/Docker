import { useEffect, useState } from 'react'
import { Activity } from 'lucide-react'
import { type Connection, type MigrationSourceStatus, listConnections, getMigrationStatus } from '../api/client'
import DmsTabs from '../components/DmsTabs'
import { getLastTargetConnectionId, setLastTargetConnectionId } from '../lib/lastTargetConnection'
import {
  DataTable, EmptyState, Field, Loading, Notice, PageHeader, ProgressBar, Section, Select, StatusPill,
} from '../ui'
import { table } from '../ui/styles'
import {
  MIGRATION_SERVICE_TABS, formatTimestamp, formatLag, lagTone, progressPct,
} from './migrationServiceShared'
import styles from './MigrationServiceStatus.module.css'

const POLL_INTERVAL_MS = 5000

/**
 * Migration Service's "Status" tab -- the "Web Progress Report" for sayonora-migration runs,
 * reading the same bookkeeping tables (warp_cdc_checkpoints, migration_partition_leases) the
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
    <>
      <DmsTabs tabs={MIGRATION_SERVICE_TABS} />
      <PageHeader
        title="Data sync status"
        subtitle="Live initial-sync progress and change-feed activity for every source checkpointed against a target Postgres connection."
      />

      <Section>
        <div className={styles.picker}>
          <Field label="Target Postgres connection" htmlFor="status-target">
            <Select id="status-target" value={selectedConnection} onChange={(e) => handleSelect(e.target.value)}>
              <option value="">Select the target Postgres connection…</option>
              {connections.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
            </Select>
          </Field>
        </div>
      </Section>

      {error && <Notice tone="error">{error}</Notice>}

      {selectedConnection && !error && loading && statuses.length === 0 && <Section><Loading /></Section>}

      {selectedConnection && !error && !loading && statuses.length === 0 && (
        <Section flush>
          <EmptyState icon={Activity} title="Nothing synced to this target yet">
            No migration has written to this target yet — start one on the Launch tab, or run the
            migration module's CLI directly against it.
          </EmptyState>
        </Section>
      )}

      {statuses.length > 0 && (
        <Section title="Sources" meta={`Refreshes every ${POLL_INTERVAL_MS / 1000}s`} flush>
          <DataTable caption="Sync status by source">
            <thead>
              <tr>
                <th scope="col">Source</th>
                <th scope="col">Initial sync</th>
                <th scope="col" className={table.num}>Events applied (change feed)</th>
                <th scope="col">Lag</th>
                <th scope="col">Last checkpoint</th>
                <th scope="col">Change-feed leader</th>
              </tr>
            </thead>
            <tbody>
              {statuses.map((s) => (
                <tr key={s.sourceKey}>
                  <td className={table.mono}>{s.sourceKey}</td>
                  <td>
                    <div className={styles.progress}>
                      <ProgressBar pct={progressPct(s)} label={`Initial sync for ${s.sourceKey}`} />
                      <span className={styles.progressText}>
                        {s.partitionsTotal > 0 ? `${s.partitionsDone}/${s.partitionsTotal} partitions` : `${progressPct(s)}%`}
                      </span>
                    </div>
                  </td>
                  <td className={table.num}>{s.eventsApplied.toLocaleString()}</td>
                  <td><StatusPill tone={lagTone(s.lagSeconds)}>{formatLag(s.lagSeconds)}</StatusPill></td>
                  <td className={styles.muted}>{formatTimestamp(s.lastCheckpointAt)}</td>
                  <td className={styles.muted}>{s.leaderWorkerId ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        </Section>
      )}
    </>
  )
}
