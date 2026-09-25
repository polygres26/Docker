import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Activity, Plus } from 'lucide-react'
import {
  type Connection, type MigrationJobState, type UploadedReport,
  listConnections, listMigrationJobs, listReports,
} from '../api/client'
import {
  DataTable, EmptyState, KpiStrip, LinkButton, Loading, Notice, PageHeader, Section, StatusPill, Tag, type KpiItem, type Tone,
} from '../ui'
import { table } from '../ui/styles'
import { formatTimestamp } from './migrationServiceShared'
import styles from './Dashboard.module.css'

/**
 * Landing page. Everything here comes from calls the app already makes elsewhere: connections,
 * uploaded reports, and Data sync jobs. A source that fails to load is left out of the KPI strip
 * and listed in a notice instead of being shown as a made-up zero.
 */

interface Loaded<T> { data: T | null; error: string | null }

interface ActivityRow {
  key: string
  kind: 'Connection' | 'Report' | 'Data sync'
  name: string
  detail: string
  at: string
  to: string
  status: { tone: Tone; label: string } | null
}

const jobTone: Record<MigrationJobState['status'], Tone> = { RUNNING: 'amber', COMPLETED: 'green', FAILED: 'red' }

function buildActivity(connections: Connection[], reports: UploadedReport[], jobs: MigrationJobState[]): ActivityRow[] {
  const rows: ActivityRow[] = [
    ...connections.map((c): ActivityRow => ({
      key: 'c' + c.id, kind: 'Connection', name: c.name, detail: c.jdbcUrl, at: c.createdAt,
      to: `/connections/${c.id}`, status: null,
    })),
    ...reports.map((r): ActivityRow => ({
      key: 'r' + r.id, kind: 'Report', name: r.name, detail: `${r.filename} · ${r.dialect.replace('_', ' ').toLowerCase()}`,
      at: r.analyzedAt ?? r.uploadedAt, to: `/reports/${r.id}`,
      status: r.analyzedAt ? { tone: 'green', label: 'Analyzed' } : { tone: 'neutral', label: 'Uploaded' },
    })),
    ...jobs.map((j): ActivityRow => ({
      key: 'j' + j.id, kind: 'Data sync', name: `${j.connectorType} job`, detail: j.sourceKeyHint,
      at: j.startedAt, to: '/data-sync/status', status: { tone: jobTone[j.status], label: j.status.toLowerCase() },
    })),
  ]
  return rows.sort((a, b) => +new Date(b.at) - +new Date(a.at)).slice(0, 8)
}

const errMsg = (e: unknown) => (e instanceof Error ? e.message : String(e))

export default function Dashboard() {
  const [connections, setConnections] = useState<Loaded<Connection[]> | null>(null)
  const [reports, setReports] = useState<Loaded<UploadedReport[]> | null>(null)
  const [jobs, setJobs] = useState<Loaded<MigrationJobState[]> | null>(null)

  useEffect(() => {
    listConnections().then((data) => setConnections({ data, error: null })).catch((e) => setConnections({ data: null, error: errMsg(e) }))
    listReports().then((data) => setReports({ data, error: null })).catch((e) => setReports({ data: null, error: errMsg(e) }))
    listMigrationJobs().then((data) => setJobs({ data, error: null })).catch((e) => setJobs({ data: null, error: errMsg(e) }))
  }, [])

  const loading = !connections || !reports || !jobs
  const failures = [
    connections?.error && `Connections: ${connections.error}`,
    reports?.error && `Reports: ${reports.error}`,
    jobs?.error && `Data sync jobs: ${jobs.error}`,
  ].filter(Boolean) as string[]

  const kpis: KpiItem[] = []
  if (connections?.data) kpis.push({ label: 'Connections', value: connections.data.length, hint: 'source databases' })
  if (reports?.data) {
    const analyzed = reports.data.filter((r) => r.analyzedAt).length
    kpis.push({ label: 'Uploaded reports', value: reports.data.length, hint: `${analyzed} analyzed` })
  }
  if (jobs?.data) {
    const running = jobs.data.filter((j) => j.status === 'RUNNING').length
    const failed = jobs.data.filter((j) => j.status === 'FAILED').length
    kpis.push({ label: 'Data sync jobs', value: jobs.data.length, hint: `${running} running · ${failed} failed` })
  }

  const activity = buildActivity(connections?.data ?? [], reports?.data ?? [], jobs?.data ?? [])

  return (
    <>
      <PageHeader
        title="Overview"
        subtitle="The databases you've connected, the reports you've analyzed, and your data sync jobs."
        actions={<LinkButton to="/connections" variant="primary"><Plus size={15} strokeWidth={2} aria-hidden />Add connection</LinkButton>}
      />

      {failures.length > 0 && <Notice tone="error" title="Some data could not be loaded">{failures.join(' · ')}</Notice>}

      {loading ? (
        <Section><Loading>Loading overview…</Loading></Section>
      ) : (
        <>
          {kpis.length > 0 && <KpiStrip items={kpis} label="Workspace summary" />}

          <Section title="Recent activity" meta="Latest connections, reports and data sync jobs" flush>
            {activity.length === 0 ? (
              <EmptyState
                icon={Activity}
                title={failures.length ? 'Nothing to show' : 'No activity yet'}
                action={!failures.length && <LinkButton to="/connections" variant="primary">Add your first connection</LinkButton>}
              >
                {failures.length ? 'The lists above could not be loaded.' : 'Connections, uploaded reports and data sync jobs will show up here.'}
              </EmptyState>
            ) : (
              <DataTable caption="Recent activity">
                <thead>
                  <tr><th scope="col">Type</th><th scope="col">Name</th><th scope="col">Detail</th><th scope="col">Status</th><th scope="col">When</th></tr>
                </thead>
                <tbody>
                  {activity.map((r) => (
                    <tr key={r.key}>
                      <td><Tag tone={r.kind === 'Data sync' ? 'neutral' : 'accent'}>{r.kind}</Tag></td>
                      <td><Link to={r.to}>{r.name}</Link></td>
                      <td title={r.detail} className={`${table.ellipsis} ${styles.muted}`}>{r.detail}</td>
                      <td>{r.status ? <StatusPill tone={r.status.tone}>{r.status.label}</StatusPill> : <span className={styles.muted}>—</span>}</td>
                      <td className={`${styles.muted} ${styles.nowrap}`}>{formatTimestamp(r.at)}</td>
                    </tr>
                  ))}
                </tbody>
              </DataTable>
            )}
          </Section>
        </>
      )}
    </>
  )
}
