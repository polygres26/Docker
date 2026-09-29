import { RefreshCw } from 'lucide-react'
import { useState } from 'react'
import { getObservability, setObservabilityToggles } from '../api/client'
import { Button, CodeBlock, DataTable, KpiStrip, Loading, Notice, PageHeader, Section, StatusPill, Tabs, Tag, type KpiItem } from '../components/ui'
import { errorText, useLoad } from '../hooks'
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
 *
 * Three sections (Health / Export / Metrics), mirroring the trimmed IA in docs/mockups/
 * warp-observability.html -- unlike that mockup, every control here is real: the Export tab's
 * pause/resume/enable/disable/clear-override buttons all round-trip through the live PATCH /
 * api/observability admin API, not a static illustration.
 */
export default function Observability() {
  const obs = useLoad(getObservability, POLL_MS)
  const data = obs.data
  const otlp = data?.otlp
  const [tab, setTab] = useState('health')
  const [toggling, setToggling] = useState<'otlp' | 'prometheus' | null>(null)
  const [toggleError, setToggleError] = useState<string | null>(null)

  async function toggle(which: 'otlp' | 'prometheus', next: boolean | null) {
    setToggling(which)
    setToggleError(null)
    try {
      await setObservabilityToggles(which === 'otlp' ? { otlpEnabled: next } : { prometheusEnabled: next })
      obs.reload()
    } catch (e) {
      setToggleError(errorText(e))
    } finally {
      setToggling(null)
    }
  }

  const otlpStateLabel = !otlp?.enabled ? 'Disabled'
    : otlp.pausedByAdmin ? 'Paused by admin'
    : otlp.exportVerified ? 'Verified'
    : otlp.exportAttempts === 0 ? 'Enabled, awaiting first export'
    : 'Unverified'
  const otlpTone = !otlp?.enabled ? 'muted' : otlp.pausedByAdmin ? 'muted' : otlp.exportVerified ? 'ok' : 'warn'

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
      {toggleError && <Notice tone="bad">Could not update observability settings: {toggleError}</Notice>}
      {obs.loading && !data ? <Loading /> : <KpiStrip items={kpis} label="Observability figures" />}

      <Tabs label="Observability views" value={tab} onChange={setTab} tabs={[
        { id: 'health', label: 'Health' },
        { id: 'export', label: 'Export' },
        { id: 'metrics', label: 'Metrics', count: data?.catalog.length },
      ]} />

      {tab === 'health' && (
        <>
          {otlp?.enabled && otlp.pausedByAdmin && (
            <Notice tone="muted">OTLP export is paused by an admin -- no exports are being sent to <code>{otlp.endpoint}</code>. Resume it on the Export tab.</Notice>
          )}
          {otlp?.enabled && !otlp.pausedByAdmin && !otlp.exportVerified && otlp.exportAttempts > 0 && (
            <Notice tone="warn">
              OTLP export has been attempted {otlp.exportAttempts} time{otlp.exportAttempts === 1 ? '' : 's'}
              {otlp.exportSuccesses > 0 ? `, ${otlp.exportSuccesses} succeeded, but the most recent success was too long ago to trust` : ' with no successful delivery yet'}.
              {otlp.lastError && <> Last error: <code>{otlp.lastError}</code>.</>} Check the collector at <code>{otlp.endpoint}</code>.
            </Notice>
          )}
          {otlp?.enabled && !otlp.pausedByAdmin && otlp.exportVerified && (
            <Notice tone="ok">OTLP export to <code>{otlp.endpoint}</code> is verified: the most recent export succeeded {ago(otlp.lastSuccessAt)}.</Notice>
          )}
          {!data?.prometheus.available && (
            <Notice tone="muted">The Prometheus scrape endpoint is disabled by an admin -- GET /metrics currently returns 404. Re-enable it on the Export tab.</Notice>
          )}
          <Section flush title="Current signal path" meta="from runtime configuration">
            <div className={styles.pad}>
              <p className={styles.sub}>
                Warp nodes (traffic, latency, pools, QoS, MCP) →{' '}
                {otlp?.enabled ? <>OTLP/{otlp.protocol?.toUpperCase()} every {otlp.exportIntervalMs}ms → {otlp.pausedByAdmin ? 'paused by admin' : otlp.exportVerified ? 'confirmed receiver' : 'receiver unconfirmed'}</> : 'OTLP export disabled'}
                {data?.prometheus.available && <>, and Prometheus scrape at <code>{data.prometheus.path}</code> (passive, pulled by your own collector)</>}.
              </p>
            </div>
          </Section>
        </>
      )}

      {tab === 'export' && (
        <Section flush title="Destinations" meta="configured export paths">
          <div className={styles.rowList}>
            <div className={styles.rowItem}>
              <div>
                <strong>OTLP endpoint</strong>
                <span className={styles.sub}>{otlp?.enabled ? `${otlp.protocol?.toUpperCase()} · ${otlp.endpoint}${otlp.headerCount > 0 ? ` · ${otlp.headerCount} header(s)` : ' · no headers configured'}` : 'Not configured -- set WARP_OTEL_ENDPOINT'}</span>
                {otlp?.enabled && <span className={styles.sub}>{otlp.exportAttempts} attempt{otlp.exportAttempts === 1 ? '' : 's'} · {otlp.exportSuccesses} succeeded · {otlp.exportFailures} failed{otlp.lastExportAt ? ` · last attempt ${ago(otlp.lastExportAt)}` : ''}</span>}
                {otlp?.enabled && !otlp.adminOverrideHasEffect && (
                  <span className={styles.sub}>Admin pause/resume has no effect until this process restarts with an OTLP endpoint configured.</span>
                )}
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <StatusPill tone={otlpTone}>{otlpStateLabel}</StatusPill>
                {otlp?.enabled && (
                  <Button
                    disabled={toggling === 'otlp' || !otlp.adminOverrideHasEffect}
                    onClick={() => toggle('otlp', otlp.pausedByAdmin)}
                  >
                    {otlp.pausedByAdmin ? 'Resume' : 'Pause'}
                  </Button>
                )}
                {otlp?.adminOverride !== null && (
                  <Button
                    variant="ghost"
                    disabled={toggling === 'otlp'}
                    onClick={() => toggle('otlp', null)}
                    title="Forget the admin pause/resume choice and defer to WARP_OTEL_ENDPOINT again"
                  >
                    Clear override
                  </Button>
                )}
              </div>
            </div>
            <div className={styles.rowItem}>
              <div><strong>Prometheus endpoint</strong><span className={styles.sub}>{data?.prometheus.path} -- unauthenticated{data?.prometheus.available ? ', always on' : ''}</span></div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <StatusPill tone={data?.prometheus.available ? 'ok' : 'muted'}>{data?.prometheus.available ? 'Passive scrape' : 'Disabled'}</StatusPill>
                {data && (
                  <Button
                    disabled={toggling === 'prometheus'}
                    onClick={() => toggle('prometheus', !data.prometheus.available)}
                  >
                    {data.prometheus.available ? 'Disable' : 'Enable'}
                  </Button>
                )}
                {data?.prometheus.adminOverride !== null && (
                  <Button
                    variant="ghost"
                    disabled={toggling === 'prometheus'}
                    onClick={() => toggle('prometheus', null)}
                    title="Forget the admin enable/disable choice -- Prometheus scrape defaults back to on"
                  >
                    Clear override
                  </Button>
                )}
              </div>
            </div>
          </div>
        </Section>
      )}

      {tab === 'metrics' && (
        <>
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
        </>
      )}
    </div>
  )
}
