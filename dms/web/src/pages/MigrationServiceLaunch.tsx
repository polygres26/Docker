import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  type Connection, type MigrationConnectorType, type MigrationJobRequest, type MigrationJobState,
  listConnections, startMigrationJob, listMigrationJobs, stopMigrationJob,
} from '../api/client'
import DmsTabs from '../components/DmsTabs'
import { setLastTargetConnectionId } from '../lib/lastTargetConnection'
import {
  MIGRATION_SERVICE_TABS, SOURCE_FIELDS, inputStyle, labelStyle, formatTimestamp, statusColor,
} from './migrationServiceShared'

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
    <div className="panel" style={{ marginBottom: 20 }}>
      <h2 style={{ fontSize: 15, marginTop: 0, marginBottom: 12 }}>Start a migration</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12, marginBottom: 12 }}>
        <div>
          <label style={labelStyle}>Connector</label>
          <select
            value={connectorType}
            onChange={(e) => { setConnectorType(e.target.value as MigrationConnectorType); setSourceConfig({}) }}
            style={inputStyle}
          >
            {Object.keys(SOURCE_FIELDS).map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div>
          <label style={labelStyle}>Target Postgres connection (checkpoints/dead-letters, and where Status tracks this job)</label>
          <select value={targetConnectionId} onChange={(e) => setTargetConnectionId(e.target.value)} style={inputStyle}>
            <option value="">Select…</option>
            {connections.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
          </select>
        </div>
        <div>
          <label style={labelStyle}>Warp gRPC host</label>
          <input value={grpcHost} onChange={(e) => setGrpcHost(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>Warp gRPC port</label>
          <input value={grpcPort} onChange={(e) => setGrpcPort(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>Warp user</label>
          <input value={grpcUser} onChange={(e) => setGrpcUser(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>Warp password</label>
          <input type="password" value={grpcPassword} onChange={(e) => setGrpcPassword(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>Parallelism (Enterprise license required for &gt;1)</label>
          <input value={parallelism} onChange={(e) => setParallelism(e.target.value)} style={inputStyle} />
        </div>
      </div>

      <div style={{ borderTop: '1px solid var(--border)', paddingTop: 12, marginBottom: 12 }}>
        <div style={{ fontSize: 12, color: 'var(--muted)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: 0.4 }}>
          {connectorType} source
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12 }}>
          {fields.map((f) => (
            <div key={f.key}>
              <label style={labelStyle}>{f.label}{f.required ? ' *' : ''}</label>
              <input
                value={sourceConfig[f.key] ?? ''}
                onChange={(e) => updateField(f.key, e.target.value)}
                placeholder={f.placeholder}
                type={f.key.toLowerCase().includes('password') || f.key === 'secretKey' ? 'password' : 'text'}
                style={inputStyle}
              />
            </div>
          ))}
        </div>
      </div>

      {error && <p style={{ color: 'var(--hard)', fontSize: 13 }}>{error}</p>}

      <button
        onClick={handleSubmit}
        disabled={submitting}
        style={{
          background: 'var(--accent-strong)', color: 'var(--bg)', border: 'none', borderRadius: 8,
          padding: '10px 18px', fontSize: 14, fontWeight: 600, cursor: submitting ? 'default' : 'pointer',
          opacity: submitting ? 0.6 : 1,
        }}
      >
        {submitting ? 'Starting…' : 'Start migration'}
      </button>
    </div>
  )
}

function JobsPanel({ jobs, onStop }: { jobs: MigrationJobState[]; onStop: (id: string) => void }) {
  if (jobs.length === 0) return null
  return (
    <div className="panel" style={{ overflowX: 'auto' }}>
      <h2 style={{ fontSize: 15, marginTop: 0, marginBottom: 12 }}>Jobs launched from this Ferry process</h2>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13.5 }}>
        <thead>
          <tr style={{ textAlign: 'left', color: 'var(--muted)', fontSize: 12, textTransform: 'uppercase', letterSpacing: 0.4 }}>
            <th style={{ padding: '8px 10px' }}>Connector</th>
            <th style={{ padding: '8px 10px' }}>Status</th>
            <th style={{ padding: '8px 10px' }}>Started</th>
            <th style={{ padding: '8px 10px' }}>Finished</th>
            <th style={{ padding: '8px 10px' }}>Error</th>
            <th style={{ padding: '8px 10px' }} />
          </tr>
        </thead>
        <tbody>
          {jobs.map((j) => (
            <tr key={j.id} style={{ borderTop: '1px solid var(--border)' }}>
              <td style={{ padding: '10px', fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace' }}>{j.connectorType}</td>
              <td style={{ padding: '10px', color: statusColor(j.status), fontWeight: 600 }}>{j.status}</td>
              <td style={{ padding: '10px', color: 'var(--muted)' }}>{formatTimestamp(j.startedAt)}</td>
              <td style={{ padding: '10px', color: 'var(--muted)' }}>{formatTimestamp(j.finishedAt)}</td>
              <td style={{ padding: '10px', color: 'var(--hard)' }}>{j.errorMessage ?? '—'}</td>
              <td style={{ padding: '10px' }}>
                {j.status === 'RUNNING' && (
                  <button
                    onClick={() => onStop(j.id)}
                    style={{ background: 'transparent', border: '1px solid var(--border)', borderRadius: 6, padding: '4px 10px', color: 'var(--text)', fontSize: 12, cursor: 'pointer' }}
                  >
                    Stop
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
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
    <div style={{ maxWidth: 1080 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Migration Service</h1>
      <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 0, marginBottom: 16 }}>
        Launch massively-parallel migration runs (sayonora-migration) writing into Warp over
        its own gRPC driver. Massively parallel throughput (parallelism &gt; 1, and multi-process
        distributed coordination) requires an Enterprise Warp license — without one, migrations
        run correctly but serially, one partition at a time.
      </p>
      <DmsTabs tabs={MIGRATION_SERVICE_TABS} />

      <div style={{ marginTop: 16, display: 'flex', flexDirection: 'column', gap: 20 }}>
        <StartMigrationForm connections={connections} onStarted={handleStarted} />
        <JobsPanel jobs={jobs} onStop={handleStop} />
      </div>
    </div>
  )
}
