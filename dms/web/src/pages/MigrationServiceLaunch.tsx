import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  type Connection, type MigrationConnectorType, type MigrationJobRequest, type MigrationJobState,
  listConnections, startMigrationJob, listMigrationJobs, stopMigrationJob,
} from '../api/client'
import DmsTabs from '../components/DmsTabs'
import { setLastTargetConnectionId } from '../lib/lastTargetConnection'
import {
  Button, DataTable, Field, FormActions, FormGrid, Input, Notice, PageHeader, Section, Select, StatusPill,
} from '../ui'
import { table } from '../ui/styles'
import {
  MIGRATION_SERVICE_TABS, SOURCE_FIELDS, formatTimestamp, statusTone,
} from './migrationServiceShared'
import styles from './MigrationServiceLaunch.module.css'

const JOBS_POLL_INTERVAL_MS = 3000

function StartMigrationForm({ connections, onStarted }: { connections: Connection[]; onStarted: (targetConnectionId: string) => void }) {
  const [connectorType, setConnectorType] = useState<MigrationConnectorType>('MONGO')
  const [targetConnectionId, setTargetConnectionId] = useState('')
  const [grpcHost, setGrpcHost] = useState('localhost')
  const [grpcPort, setGrpcPort] = useState('7070')
  const [grpcUser, setGrpcUser] = useState('')
  const [grpcPassword, setGrpcPassword] = useState('')
  const [parallelism, setParallelism] = useState('1')
  const [sourceConfig, setSourceConfig] = useState<Record<string, string>>({})
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fields = SOURCE_FIELDS[connectorType]

  function updateField(key: string, value: string) {
    setSourceConfig((prev) => ({ ...prev, [key]: value }))
  }

  async function handleSubmit() {
    setError(null)
    const missing = fields.filter((f) => f.required && !sourceConfig[f.key]?.trim())
    if (!targetConnectionId) {
      setError('Select a target Postgres connection.')
      return
    }
    if (missing.length > 0) {
      setError(`Missing required field(s): ${missing.map((f) => f.label).join(', ')}`)
      return
    }
    const req: MigrationJobRequest = {
      connectorType,
      targetConnectionId,
      warpGrpcHost: grpcHost,
      warpGrpcPort: Number(grpcPort),
      warpGrpcUser: grpcUser,
      warpGrpcPassword: grpcPassword,
      parallelism: Number(parallelism) || 1,
      sourceConfig,
    }
    setSubmitting(true)
    try {
      await startMigrationJob(req)
      onStarted(targetConnectionId)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Section title="Start a migration">
      <FormGrid>
        <Field label="Connector" htmlFor="ms-connector">
          <Select
            id="ms-connector"
            value={connectorType}
            onChange={(e) => { setConnectorType(e.target.value as MigrationConnectorType); setSourceConfig({}) }}
          >
            {Object.keys(SOURCE_FIELDS).map((t) => <option key={t} value={t}>{t}</option>)}
          </Select>
        </Field>
        <Field label="Target Postgres connection (checkpoints/dead-letters, and where Status tracks this job)" htmlFor="ms-target">
          <Select id="ms-target" value={targetConnectionId} onChange={(e) => setTargetConnectionId(e.target.value)}>
            <option value="">Select…</option>
            {connections.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
          </Select>
        </Field>
        <Field label="Warp gRPC host" htmlFor="ms-host">
          <Input id="ms-host" value={grpcHost} onChange={(e) => setGrpcHost(e.target.value)} />
        </Field>
        <Field label="Warp gRPC port" htmlFor="ms-port">
          <Input id="ms-port" value={grpcPort} onChange={(e) => setGrpcPort(e.target.value)} />
        </Field>
        <Field label="Warp user" htmlFor="ms-user">
          <Input id="ms-user" value={grpcUser} onChange={(e) => setGrpcUser(e.target.value)} />
        </Field>
        <Field label="Warp password" htmlFor="ms-password">
          <Input id="ms-password" type="password" value={grpcPassword} onChange={(e) => setGrpcPassword(e.target.value)} />
        </Field>
        <Field label="Parallelism (Enterprise license required for >1)" htmlFor="ms-parallelism">
          <Input id="ms-parallelism" value={parallelism} onChange={(e) => setParallelism(e.target.value)} />
        </Field>
      </FormGrid>

      <div className={styles.sourceBlock}>
        <h3 className={styles.sourceTitle}>{connectorType} source</h3>
        <FormGrid>
          {fields.map((f) => (
            <Field key={f.key} label={`${f.label}${f.required ? ' *' : ''}`} htmlFor={`ms-src-${f.key}`}>
              <Input
                id={`ms-src-${f.key}`}
                value={sourceConfig[f.key] ?? ''}
                onChange={(e) => updateField(f.key, e.target.value)}
                placeholder={f.placeholder}
                type={f.key.toLowerCase().includes('password') || f.key === 'secretKey' ? 'password' : 'text'}
              />
            </Field>
          ))}
        </FormGrid>
      </div>

      {error && <Notice tone="error">{error}</Notice>}

      <FormActions>
        <Button variant="primary" onClick={handleSubmit} disabled={submitting}>
          {submitting ? 'Starting…' : 'Start migration'}
        </Button>
      </FormActions>
    </Section>
  )
}

function JobsPanel({ jobs, onStop }: { jobs: MigrationJobState[]; onStop: (id: string) => void }) {
  if (jobs.length === 0) return null
  return (
    <Section title="Jobs launched from this Ferry process" flush>
      <DataTable caption="Migration jobs">
        <thead>
          <tr>
            <th scope="col">Connector</th>
            <th scope="col">Status</th>
            <th scope="col">Started</th>
            <th scope="col">Finished</th>
            <th scope="col">Error</th>
            <th scope="col"><span className="sr-only">Actions</span></th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((j) => (
            <tr key={j.id}>
              <td className={table.mono}>{j.connectorType}</td>
              <td><StatusPill tone={statusTone(j.status)}>{j.status.toLowerCase()}</StatusPill></td>
              <td className={styles.muted}>{formatTimestamp(j.startedAt)}</td>
              <td className={styles.muted}>{formatTimestamp(j.finishedAt)}</td>
              <td className={styles.error}>{j.errorMessage ?? '—'}</td>
              <td>
                {j.status === 'RUNNING' && <Button size="sm" onClick={() => onStop(j.id)}>Stop</Button>}
              </td>
            </tr>
          ))}
        </tbody>
      </DataTable>
    </Section>
  )
}

/**
 * Migration Service's "Launch" tab -- starts a real sayonora-migration Coordinator run server-side
 * (see MigrationJobRunner's own javadoc) via POST /api/migration/jobs. Parallelism above 1 is an
 * Enterprise feature -- the migration engine itself (not this page) silently runs serial without a
 * license, so nothing here fakes or duplicates that enforcement.
 *
 * On a successful start, remembers the target connection (see lastTargetConnection.ts) and jumps
 * straight to the Status tab -- the whole point of tracking a job is watching it move, and making
 * the user re-pick the same connection they just used on the next tab would be real, avoidable
 * friction for a single-UI tool.
 */
export default function MigrationServiceLaunch() {
  const navigate = useNavigate()
  const [connections, setConnections] = useState<Connection[]>([])
  const [jobs, setJobs] = useState<MigrationJobState[]>([])

  useEffect(() => {
    listConnections().then(setConnections).catch(() => {})
  }, [])

  function refreshJobs() {
    listMigrationJobs().then(setJobs).catch(() => {})
  }

  useEffect(() => {
    refreshJobs()
    const interval = setInterval(refreshJobs, JOBS_POLL_INTERVAL_MS)
    return () => clearInterval(interval)
  }, [])

  async function handleStop(jobId: string) {
    await stopMigrationJob(jobId)
    refreshJobs()
  }

  function handleStarted(targetConnectionId: string) {
    setLastTargetConnectionId(targetConnectionId)
    refreshJobs()
    navigate('/data-sync/status')
  }

  return (
    <>
      <DmsTabs tabs={MIGRATION_SERVICE_TABS} />
      <PageHeader
        title="Data sync"
        subtitle="Launch massively-parallel migration runs (sayonora-migration) writing into Warp over its own gRPC driver. Massively parallel throughput (parallelism > 1, and multi-process distributed coordination) requires an Enterprise Warp license — without one, migrations run correctly but serially, one partition at a time."
      />
      <StartMigrationForm connections={connections} onStarted={handleStarted} />
      <JobsPanel jobs={jobs} onStop={handleStop} />
    </>
  )
}
