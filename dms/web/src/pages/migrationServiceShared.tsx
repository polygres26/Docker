import { type MigrationConnectorType, type MigrationJobState, type MigrationSourceStatus } from '../api/client'
import { type DmsTab } from '../components/DmsTabs'

/** Migration Service's own two tabs (Launch a job, track its Status) -- see DmsTabs' own javadoc
 * for why this is a tab strip within one sidebar entry rather than two separate sidebar items. */
export const MIGRATION_SERVICE_TABS: DmsTab[] = [
  { to: '/data-sync', label: 'Launch' },
  { to: '/data-sync/status', label: 'Status' },
]

export function progressPct(status: MigrationSourceStatus): number {
  if (status.partitionsTotal === 0) return status.eventsApplied > 0 ? 100 : 0
  return Math.round((status.partitionsDone / status.partitionsTotal) * 100)
}

export function formatLag(seconds: number | null): string {
  if (seconds === null) return '—'
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  return `${Math.round(seconds / 3600)}h`
}

export function lagColor(seconds: number | null): string {
  if (seconds === null) return 'var(--muted)'
  if (seconds < 30) return 'var(--accent-strong)'
  if (seconds < 300) return 'var(--medium)'
  return 'var(--hard)'
}

export function formatTimestamp(iso: string | null): string {
  if (!iso) return '—'
  try {
    return new Date(iso).toLocaleString()
  } catch {
    return iso
  }
}

export function statusColor(status: MigrationJobState['status']): string {
  if (status === 'RUNNING') return 'var(--medium)'
  if (status === 'COMPLETED') return 'var(--accent-strong)'
  return 'var(--hard)'
}

export interface SourceField {
  key: string
  label: string
  placeholder: string
  required: boolean
}

// Mirrors MigrationSourceFactory's own required/optional sourceConfig keys, connector by
// connector -- kept in this one place rather than one sub-form per connector, since the 7 real
// connectors' config shapes genuinely don't share a single schema (a Mongo URI vs. AWS SDK
// endpoint/region/credentials vs. a Neo4j Bolt URI plus label/relationship lists).
export const SOURCE_FIELDS: Record<MigrationConnectorType, SourceField[]> = {
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

export const inputStyle: React.CSSProperties = {
  width: '100%', background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8,
  padding: '9px 11px', color: 'var(--text)', fontSize: 13.5,
}
export const labelStyle: React.CSSProperties = { fontSize: 12, color: 'var(--muted)', marginBottom: 4, display: 'block' }
