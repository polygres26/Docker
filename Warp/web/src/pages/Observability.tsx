import { RefreshCw } from 'lucide-react'
import { useState } from 'react'
import { getObservability, setObservabilityToggles } from '../api/client'
import { Button, CodeBlock, DataTable, Field, KpiStrip, Loading, Notice, PageHeader, Section, StatusPill, Tabs, Tag, type KpiItem, type Tone } from '../components/ui'
import { OTEL_PRESETS, type OtelPreset } from '../api/otel-presets'
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

          {otlp?.enabled && (
            <Section flush title="Resource identity" meta="attached to every OTLP export from this process">
              {otlp.resource ? (
                <div className={styles.pad}>
                  <p className={styles.sub}>
                    Every exported metric now carries these resource attributes, so a multi-node deployment's data is
                    distinguishable on the receiving end -- previously none were attached at all.
                  </p>
                  <div className={styles.tags}>
                    {Object.entries(otlp.resource).map(([k, v]) => <Tag key={k}>{k}={v}</Tag>)}
                  </div>
                  {!('service.version' in otlp.resource) && (
                    <p className={styles.sub}>service.version is omitted: this build's jar manifest carries no Implementation-Version.</p>
                  )}
                </div>
              ) : (
                <div className={styles.pad}><span className={styles.sub}>No live exporter yet -- resource attributes appear once the first export tick runs.</span></div>
              )}
            </Section>
          )}
        </>
      )}

      {tab === 'export' && (
        <>
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

        <ConfigGenerator prometheusPath={data?.prometheus.path ?? '/metrics'} />
        </>
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

const MATURITY_TONE: Record<OtelPreset['maturity'], Tone> = { available: 'ok', collector: 'warn', planned: 'muted' }
const MATURITY_LABEL: Record<OtelPreset['maturity'], string> = { available: 'Available now', collector: 'Requires Collector', planned: 'Planned' }

/**
 * Generates the real WARP_OTEL_* env vars (or a Prometheus scrape_config) for a chosen
 * destination -- every preset selection changes real content, unlike the docs/mockups/
 * warp-observability.html mock this fixes the "functionally static" flaw of. This never writes
 * or applies anything: WARP_OTEL_PROTOCOL/ENDPOINT/HEADERS have no persisted-config write path
 * (unlike otlpEnabled/prometheusEnabled's PATCH /api/observability), so the only honest action is
 * generating text to set and restart with -- stated explicitly below, not implied away.
 */
function ConfigGenerator({ prometheusPath }: { prometheusPath: string }) {
  const [selectedId, setSelectedId] = useState(OTEL_PRESETS[0].id)
  const selected = OTEL_PRESETS.find((p) => p.id === selectedId) ?? OTEL_PRESETS[0]
  const [protocol, setProtocol] = useState(selected.protocol ?? 'grpc')
  const [endpoint, setEndpoint] = useState(selected.endpointPlaceholder ?? '')
  const [headers, setHeaders] = useState(selected.headerPlaceholder ?? '')
  const [target, setTarget] = useState('')

  function select(p: OtelPreset) {
    setSelectedId(p.id)
    setProtocol(p.protocol ?? 'grpc')
    setEndpoint(p.endpointPlaceholder ?? '')
    setHeaders(p.headerPlaceholder ?? '')
  }

  const otlpConfig = [
    `WARP_OTEL_PROTOCOL=${protocol}`,
    `WARP_OTEL_ENDPOINT=${endpoint || '<endpoint>'}`,
    headers.trim() ? `WARP_OTEL_HEADERS=${headers.trim()}` : null,
    'WARP_OTEL_EXPORT_INTERVAL_MS=5000',
  ].filter(Boolean).join('\n')

  const scrapeConfig = `scrape_configs:\n  - job_name: warp\n    static_configs:\n      - targets: ['${target || '<warp-host:admin-port>'}']\n    metrics_path: ${prometheusPath}`

  return (
    <Section flush title="Generate configuration" meta="pick a destination -- nothing here writes or applies anything">
      <div className={styles.pad}>
        <p className={styles.sub}>
          WARP_OTEL_PROTOCOL/ENDPOINT/HEADERS are environment-variable only today -- there is no live-apply API for
          them (unlike the pause/enable toggles above, which do persist). Generate the text below, set it, and restart.
        </p>
      </div>
      <div className={styles.rowList}>
        {OTEL_PRESETS.map((p) => (
          <div key={p.id} className={styles.rowItem}>
            <div>
              <strong>{p.label}</strong>
              <span className={styles.sub}>{p.sub}</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <StatusPill tone={MATURITY_TONE[p.maturity]}>{MATURITY_LABEL[p.maturity]}</StatusPill>
              <Button variant={p.id === selectedId ? 'primary' : 'secondary'} onClick={() => select(p)}>
                {p.id === selectedId ? 'Selected' : 'Select'}
              </Button>
            </div>
          </div>
        ))}
      </div>

      <div className={styles.pad}>
        <p className={styles.sub}>{selected.notes}</p>

        {selected.kind === 'otlp' && selected.maturity === 'available' && (
          <>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 12 }}>
              <Field label="Transport">{(id) => (
                <select id={id} value={protocol} onChange={(e) => setProtocol(e.target.value as 'grpc' | 'http')}>
                  <option value="grpc">OTLP/gRPC</option>
                  <option value="http">OTLP/HTTP</option>
                </select>
              )}</Field>
              <Field label="Endpoint">{(id) => <input id={id} value={endpoint} onChange={(e) => setEndpoint(e.target.value)} />}</Field>
            </div>
            <Field label="Headers" hint="key=value, comma-separated for more than one -- values are never sent anywhere, this only builds the text below">
              {(id) => <input id={id} value={headers} onChange={(e) => setHeaders(e.target.value)} placeholder="none" />}
            </Field>
            <div style={{ marginTop: 12 }}>
              <CodeBlock label="Generated Warp configuration">{otlpConfig}</CodeBlock>
            </div>
          </>
        )}

        {selected.kind === 'otlp' && selected.maturity !== 'available' && (
          <p className={styles.sub}>
            {selected.maturity === 'collector'
              ? <>Select <strong>OTel Collector</strong> above to generate the config for the Collector Warp would export to -- it then handles the {selected.label}-specific translation.</>
              : 'No configuration to generate -- this destination isn’t supported yet.'}
          </p>
        )}

        {selected.kind === 'prometheus-scrape' && (
          <>
            <Field label="Warp host:admin-port" hint="e.g. warp.internal:19090 -- your own Prometheus server needs to reach this">
              {(id) => <input id={id} value={target} onChange={(e) => setTarget(e.target.value)} placeholder="warp-host:19090" />}
            </Field>
            <div style={{ marginTop: 12 }}>
              <CodeBlock label="Prometheus scrape_config">{scrapeConfig}</CodeBlock>
            </div>
          </>
        )}
      </div>
    </Section>
  )
}
