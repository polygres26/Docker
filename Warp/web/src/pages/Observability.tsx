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

/**
 * Observability: what Warp actually emits about itself, where it goes, and what's genuinely
 * unverified. Every figure comes from GET /api/observability (ObservabilitySummary.java), which
 * re-reads the same env vars WarpTelemetry itself parses -- never a second OTLP exporter -- plus
 * the always-on Prometheus scrape endpoint. Nothing here is estimated: a real, disclosed gap
 * (OTLP has no delivery confirmation; only metrics are exported, no traces or logs) is stated as
 * such, not hidden behind a health check that doesn't exist.
 */
export default function Observability() {
  const obs = useLoad(getObservability, POLL_MS)
  const data = obs.data

  const kpis: KpiItem[] = data ? [
    { label: 'OTLP export', value: data.otlp.enabled ? 'Enabled' : 'Disabled', tone: data.otlp.enabled ? 'warn' : 'muted', wide: true,
      hint: data.otlp.enabled ? `${data.otlp.protocol?.toUpperCase()} to ${data.otlp.endpoint} every ${data.otlp.exportIntervalMs}ms -- delivery is not confirmed` : 'set WARP_OTEL_ENDPOINT to enable' },
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

      {data && !data.otlp.exportVerified && data.otlp.enabled && (
        <Notice tone="warn">
          OTLP export is configured, but Warp cannot confirm successful delivery: the exporter runs on a timer with no
          acknowledgement, health check or last-success signal. Treat this destination as unverified until you confirm
          data is arriving on the collector side.
        </Notice>
      )}

      <Section flush title="Destinations" meta="configured export paths">
        <div className={styles.rowList}>
          <div className={styles.rowItem}>
            <div><strong>OTLP endpoint</strong><span className={styles.sub}>{data?.otlp.enabled ? `${data.otlp.protocol?.toUpperCase()} · ${data.otlp.endpoint}${data.otlp.headerCount > 0 ? ` · ${data.otlp.headerCount} header(s)` : ' · no headers configured'}` : 'Not configured -- set WARP_OTEL_ENDPOINT'}</span></div>
            <StatusPill tone={data?.otlp.enabled ? 'warn' : 'muted'}>{data?.otlp.enabled ? 'Unverified' : 'Off'}</StatusPill>
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
