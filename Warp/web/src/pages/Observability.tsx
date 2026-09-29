import { RefreshCw } from 'lucide-react'
import { getObservability } from '../api/client'
import { Button, CodeBlock, DataTable, KpiStrip, Loading, Notice, PageHeader, Section, StatusPill, Tag, type KpiItem } from '../components/ui'
import { useLoad } from '../hooks'
import styles from './interfaces/interfaces.module.css'

const POLL_MS = 15_000

/** A few real starter queries built from the actual metric names in the catalog below -- plain
 * text, not a generated file or a claim that Warp validated them against a real Prometheus. */
const STARTER_QUERIES: Array<{ label: string; query: string }> = [
  { label: 'Statement rate (5m)', query: 'rate(warp_statements_total[5m])' },
  { label: 'Error ratio (5m)', query: 'rate(warp_statement_errors_total[5m]) / rate(warp_statements_total[5m])' },
  { label: 'QoS rejection rate', query: 'increase(warp_qos_rejected_total[5m]) > 0' },
  { label: 'Pool saturation', query: 'warp_pool_connections{state="active"} / warp_pool_max_size' },
  { label: 'Callers waiting for a connection', query: 'warp_pool_waiting > 0' },
]

/** ISO timestamp -> a short "Xs/m/h ago" string, or null passthrough. */
function ago(iso: string | null): string | null {
  if (!iso) return null
  const ms = Date.now() - new Date(iso).getTime()
  if (ms < 0) return 'just now'
  const s = Math.floor(ms / 1000)
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  return `${Math.floor(m / 60)}h ago`
}

/**
 * Observability: what Warp actually emits about itself, where it goes, and whether delivery is
 * actually confirmed. Every figure comes from GET /api/observability (ObservabilitySummary.java):
 * config fields are read straight from env (never a second OTLP exporter just to answer this
 * page), and exportVerified/the attempt-and-success counters are the REAL, live outcome of every
 * export WarpTelemetry has made (ExportHealthTrackingExporter tracks each export's actual async
 * result) -- not an assumption from the exporter merely being configured.
 */
export default function Observability() {
  const obs = useLoad(getObservability, POLL_MS)
  const data = obs.data
  const otlp = data?.otlp

  const otlpStateLabel = !otlp?.enabled ? 'Disabled'
    : otlp.exportVerified ? 'Verified'
    : otlp.exportAttempts === 0 ? 'Enabled, awaiting first export'
    : 'Unverified'
  const otlpTone = !otlp?.enabled ? 'muted' : otlp.exportVerified ? 'ok' : 'warn'

  const kpis: KpiItem[] = data ? [
    { label: 'OTLP export', value: otlpStateLabel, tone: otlpTone, wide: true,
      hint: otlp?.enabled ? `${otlp.protocol?.toUpperCase()} to ${otlp.endpoint} every ${otlp.exportIntervalMs}ms` : 'set WARP_OTEL_ENDPOINT to enable' },
    { label: 'Signal types', value: data.metricsOnly ? 'Metrics' : '—', hint: 'no traces or logs' },
    { label: 'Prometheus scrape', value: data.prometheus.available ? 'Available' : 'Off', hint: data.prometheus.path },
    { label: 'Cataloged metrics', value: data.catalog.length, hint: 'names Warp actually emits' },
  ] : []

  return (
    <div>
      <PageHeader title="Observability" description="What Warp emits, where it goes, and whether monitoring is complete enough for production."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => obs.reload()}>Refresh</Button>} />
      {obs.error && <Notice tone="bad">Could not load observability status: {obs.error}</Notice>}
      {obs.loading && !data ? <Loading /> : <KpiStrip items={kpis} label="Observability figures" />}

      {otlp?.enabled && !otlp.exportVerified && otlp.exportAttempts > 0 && (
        <Notice tone="warn">
          OTLP export has been attempted {otlp.exportAttempts} time{otlp.exportAttempts === 1 ? '' : 's'}
          {otlp.exportSuccesses > 0 ? `, ${otlp.exportSuccesses} succeeded, but the most recent success was too long ago to trust` : ' with no successful delivery yet'}.
          {otlp.lastError && <> Last error: <code>{otlp.lastError}</code>.</>} Check the collector at <code>{otlp.endpoint}</code>.
        </Notice>
      )}
      {otlp?.enabled && otlp.exportVerified && (
        <Notice tone="ok">OTLP export to <code>{otlp.endpoint}</code> is verified: the most recent export succeeded {ago(otlp.lastSuccessAt)}.</Notice>
      )}

      <Section flush title="Destinations" meta="configured export paths">
        <div className={styles.rowList}>
          <div className={styles.rowItem}>
            <div>
              <strong>OTLP endpoint</strong>
              <span className={styles.sub}>{otlp?.enabled ? `${otlp.protocol?.toUpperCase()} · ${otlp.endpoint}${otlp.headerCount > 0 ? ` · ${otlp.headerCount} header(s)` : ' · no headers configured'}` : 'Not configured -- set WARP_OTEL_ENDPOINT'}</span>
              {otlp?.enabled && <span className={styles.sub}>{otlp.exportAttempts} attempt{otlp.exportAttempts === 1 ? '' : 's'} · {otlp.exportSuccesses} succeeded · {otlp.exportFailures} failed{otlp.lastExportAt ? ` · last attempt ${ago(otlp.lastExportAt)}` : ''}</span>}
            </div>
            <StatusPill tone={otlpTone}>{otlpStateLabel}</StatusPill>
          </div>
          <div className={styles.rowItem}>
            <div><strong>Prometheus endpoint</strong><span className={styles.sub}>{data?.prometheus.path} -- unauthenticated, always on</span></div>
            <StatusPill tone="ok">Passive scrape</StatusPill>
          </div>
        </div>
      </Section>

      <Section flush title="Signal catalog" meta={data ? `${data.catalog.length} metrics` : undefined}>
        {!data ? <div className={styles.pad}><Loading /></div> : (
          <DataTable caption="Metrics Warp exports" minWidth={720}>
            <thead><tr><th>Metric</th><th>Type</th><th>Labels</th><th>Description</th></tr></thead>
            <tbody>
              {data.catalog.map((m) => (
                <tr key={m.name}>
                  <td className={styles.mono}>{m.name}</td>
                  <td><Tag>{m.type}</Tag></td>
                  <td>{m.labels.length > 0 ? <span className={styles.tags}>{m.labels.map((l) => <Tag key={l}>{l}</Tag>)}</span> : <span className={styles.sub}>none</span>}</td>
                  <td className={styles.wrapCell}>{m.description}</td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
        <div className={styles.pad}><span className={styles.sub}>Deliberately excluded: per-statement SQL text (unbounded cardinality) -- stays JSON-only on the Traffic page.</span></div>
      </Section>

      <Section flush title="Starter queries" meta="PromQL, built from the metrics above">
        <div className={styles.pad}>
          <p className={styles.sub}>Example queries against the metrics this Warp actually exports -- not validated against a live Prometheus, just real metric/label names from the catalog above.</p>
          {STARTER_QUERIES.map((q) => <CodeBlock key={q.label} label={q.label}>{q.query}</CodeBlock>)}
        </div>
      </Section>
    </div>
  )
}
