import { useEffect, useState } from 'react'
import {
  type Connection, type MigrationSourceStatus, type MigrationConnectorType,
  type MigrationJobRequest, type MigrationJobState,
  listConnections, getMigrationStatus, startMigrationJob, listMigrationJobs, stopMigrationJob,
} from '../api/client'

const POLL_INTERVAL_MS = 5000
const JOBS_POLL_INTERVAL_MS = 3000

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

function statusColor(status: MigrationJobState['status']): string {
  if (status === 'RUNNING') return 'var(--medium)'
  if (status === 'COMPLETED') return 'var(--accent-strong)'
  return 'var(--hard)'
}

interface SourceField {
  key: string
  label: string
  placeholder: string
  required: boolean
}

// Mirrors MigrationSourceFactory's own required/optional sourceConfig keys, connector by
// connector -- kept in this one place rather than one sub-form per connector, since the 7 real
// connectors' config shapes genuinely don't share a single schema (a Mongo URI vs. AWS SDK
// endpoint/region/credentials vs. a Neo4j Bolt URI plus label/relationship lists).
const SOURCE_FIELDS: Record<MigrationConnectorType, SourceField[]> = {
  MONGO: [
    { key: 'uri', label: 'Mongo connection URI', placeholder: 'mongodb://host:27017', required: true },
    { key: 'sourceDb', label: 'Source database', placeholder: 'mydb', required: true },
    { key: 'sourceCollection', label: 'Source collection', placeholder: 'orders', required: true },
    { key: 'targetDb', label: 'Target database (default: same as source)', placeholder: '', required: false },
    { key: 'targetCollection', label: 'Target collection (default: same as source)', placeholder: '', required: false },
    { key: 'partitionCount', label: 'Partition count (default: 1)', placeholder: '4', required: false },
    { key: 'shardKeyField', label: 'Shard key field (default: _id)', placeholder: 'customer_id', required: false },
  ],
  MYSQL: [
    { key: 'host', label: 'Host', placeholder: 'db.internal', required: true },
    { key: 'port', label: 'Port', placeholder: '3306', required: true },
    { key: 'user', label: 'User', placeholder: '', required: true },
    { key: 'password', label: 'Password', placeholder: '', required: true },
    { key: 'sourceDatabase', label: 'Source database', placeholder: 'mydb', required: true },
    { key: 'sourceTable', label: 'Source table', placeholder: 'orders', required: true },
    { key: 'partitionCount', label: 'Partition count (default: 1)', placeholder: '4', required: false },
  ],
  SQLSERVER: [
    { key: 'host', label: 'Host', placeholder: 'db.internal', required: true },
    { key: 'port', label: 'Port', placeholder: '1433', required: true },
    { key: 'user', label: 'User', placeholder: '', required: true },
    { key: 'password', label: 'Password', placeholder: '', required: true },
    { key: 'sourceDatabase', label: 'Source database', placeholder: 'mydb', required: true },
    { key: 'sourceSchema', label: 'Source schema', placeholder: 'dbo', required: true },
    { key: 'sourceTable', label: 'Source table', placeholder: 'orders', required: true },
    { key: 'partitionCount', label: 'Partition count (default: 1)', placeholder: '4', required: false },
  ],
  ORACLE: [
    { key: 'host', label: 'Host', placeholder: 'db.internal', required: true },
    { key: 'port', label: 'Port', placeholder: '1521', required: true },
    { key: 'serviceName', label: 'Service name', placeholder: 'FREEPDB1', required: true },
    { key: 'user', label: 'User', placeholder: '', required: true },
    { key: 'password', label: 'Password', placeholder: '', required: true },
    { key: 'sourceSchema', label: 'Source schema', placeholder: 'MYSCHEMA', required: true },
    { key: 'sourceTable', label: 'Source table', placeholder: 'ORDERS', required: true },
    { key: 'partitionCount', label: 'Partition count (default: 1)', placeholder: '4', required: false },
  ],
  DYNAMODB: [
    { key: 'endpoint', label: 'Endpoint URL', placeholder: 'https://dynamodb.us-east-1.amazonaws.com', required: true },
    { key: 'region', label: 'Region (default: us-east-1)', placeholder: 'us-east-1', required: false },
    { key: 'accessKey', label: 'Access key', placeholder: '', required: true },
    { key: 'secretKey', label: 'Secret key', placeholder: '', required: true },
    { key: 'sourceTable', label: 'Source table', placeholder: 'Orders', required: true },
    { key: 'partitionCount', label: 'Partition count (default: 1)', placeholder: '4', required: false },
  ],
  SQS: [
    { key: 'endpoint', label: 'Endpoint URL', placeholder: 'https://sqs.us-east-1.amazonaws.com', required: true },
    { key: 'region', label: 'Region (default: us-east-1)', placeholder: 'us-east-1', required: false },
    { key: 'accessKey', label: 'Access key', placeholder: '', required: true },
    { key: 'secretKey', label: 'Secret key', placeholder: '', required: true },
    { key: 'queueUrl', label: 'Queue URL', placeholder: '', required: true },
    { key: 'queueName', label: 'Queue name', placeholder: 'orders-queue', required: true },
  ],
  NEO4J: [
    { key: 'boltUri', label: 'Bolt URI', placeholder: 'bolt://host:7687', required: true },
    { key: 'user', label: 'User', placeholder: '', required: true },
    { key: 'password', label: 'Password', placeholder: '', required: true },
    { key: 'nodeLabels', label: 'Node labels (comma-separated)', placeholder: 'Person,Company', required: true },
    { key: 'relationshipSpecs', label: 'Relationship specs (FromLabel:TYPE:ToLabel; ;-separated)', placeholder: 'Person:WORKS_AT:Company', required: false },
  ],
  INFLUXDB: [
    { key: 'host', label: 'Host', placeholder: 'influx.internal', required: true },
    { key: 'port', label: 'Port', placeholder: '8086', required: true },
    { key: 'database', label: 'Database', placeholder: 'mydb', required: true },
    { key: 'measurement', label: 'Measurement', placeholder: 'readings', required: true },
    { key: 'tagKeys', label: 'Tag keys (comma-separated)', placeholder: 'sensor,region', required: true },
  ],
}

const inputStyle: React.CSSProperties = {
  width: '100%', background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8,
  padding: '9px 11px', color: 'var(--text)', fontSize: 13.5,
}
const labelStyle: React.CSSProperties = { fontSize: 12, color: 'var(--muted)', marginBottom: 4, display: 'block' }

function StartMigrationForm({ connections, onStarted }: { connections: Connection[]; onStarted: () => void }) {
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
      polywireGrpcHost: grpcHost,
      polywireGrpcPort: Number(grpcPort),
      polywireGrpcUser: grpcUser,
      polywireGrpcPassword: grpcPassword,
      parallelism: Number(parallelism) || 1,
      sourceConfig,
    }
    setSubmitting(true)
    try {
      await startMigrationJob(req)
      onStarted()
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
          <label style={labelStyle}>Target Postgres connection (checkpoints/dead-letters)</label>
          <select value={targetConnectionId} onChange={(e) => setTargetConnectionId(e.target.value)} style={inputStyle}>
            <option value="">Select…</option>
            {connections.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
          </select>
        </div>
        <div>
          <label style={labelStyle}>Polywire gRPC host</label>
          <input value={grpcHost} onChange={(e) => setGrpcHost(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>Polywire gRPC port</label>
          <input value={grpcPort} onChange={(e) => setGrpcPort(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>Polywire user</label>
          <input value={grpcUser} onChange={(e) => setGrpcUser(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>Polywire password</label>
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
    <div className="panel" style={{ marginBottom: 20, overflowX: 'auto' }}>
      <h2 style={{ fontSize: 15, marginTop: 0, marginBottom: 12 }}>Jobs launched from this DMS process</h2>
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
 * The "Web Progress Report" for nexagres-migration runs, plus (new) the ability to actually
 * LAUNCH one -- reads/writes the same bookkeeping tables (polywire_cdc_checkpoints,
 * migration_partition_leases) the migration workers themselves read and write, via
 * GET /api/migration/status, and now also POSTs to /api/migration/jobs to start a real
 * Coordinator run server-side (see MigrationJobRunner's own javadoc). Parallelism above 1 is an
 * Enterprise feature -- the migration engine itself (not this page) silently runs serial without
 * a license, so nothing here fakes or duplicates that enforcement.
 */
export default function DataSync() {
  const [connections, setConnections] = useState<Connection[]>([])
  const [selectedConnection, setSelectedConnection] = useState('')
  const [statuses, setStatuses] = useState<MigrationSourceStatus[]>([])
  const [jobs, setJobs] = useState<MigrationJobState[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

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

  async function handleStop(jobId: string) {
    await stopMigrationJob(jobId)
    refreshJobs()
  }

  return (
    <div style={{ maxWidth: 1080 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Data Sync</h1>
      <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 0, marginBottom: 20 }}>
        Launch and monitor massively-parallel migration runs (nexagres-migration) writing into
        Polywire over its own gRPC driver. Massively parallel throughput (parallelism &gt; 1, and
        multi-process distributed coordination) requires an Enterprise Polywire license — without
        one, migrations run correctly but serially, one partition at a time.
      </p>

      <StartMigrationForm connections={connections} onStarted={refreshJobs} />
      <JobsPanel jobs={jobs} onStop={handleStop} />

      <div className="panel" style={{ marginBottom: 20 }}>
        <label style={labelStyle}>View progress for target connection</label>
        <select
          value={selectedConnection}
          onChange={(e) => setSelectedConnection(e.target.value)}
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
          No migration has written to this target yet — start one above, or run the migration
          module's CLI directly against it.
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
