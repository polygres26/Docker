import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { type WireMetricsSummary, getWireMetrics, listBackendSets } from '../api/client'
import styles from './Metrics.module.css'
import { DataTable, KpiStrip, Loading, Notice, PageHeader, Section, StatusPill, type KpiItem } from '../components/ui'

const PROTOCOL_COLORS: Record<string, string> = {
  pgwire: 'var(--sy-series-2)',
  mywire: 'var(--sy-series-1)',
  mssqlwire: 'var(--sy-series-3)',
  orawire: 'var(--sy-series-4)',
  mongowire: 'var(--sy-series-6)',
  dynamowire: 'var(--sy-series-5)',
}

function colorFor(name: string, index: number): string {
  return PROTOCOL_COLORS[name] ?? ['var(--sy-series-4)', 'var(--sy-series-5)', 'var(--sy-series-6)'][index % 3]
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return String(n)
}

/**
 * Live traffic dashboard for Warp -- which wire protocol customers are actually running
 * (pgwire/mywire/mssqlwire/orawire, from SqlMetricsCollector's per-statement dialect tag), a
 * reads/sec vs writes/sec split, and the top 10 most expensive SQL shapes by cumulative execution
 * time. Polls the admin API every 5s -- cheap, and "live" is the point of this page.
 */
export default function Metrics() {
  const [metrics, setMetrics] = useState<WireMetricsSummary | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [backendSetNames, setBackendSetNames] = useState<string[]>([])

  function load() {
    getWireMetrics()
      .then((m) => { setMetrics(m); setError(null); setLastUpdated(new Date()) })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }

  useEffect(() => {
    load()
    const id = setInterval(load, 5000)
    // Backend sets change rarely (an admin edit, not live traffic) -- fetched once, not on the
    // same 5s poll as the metrics themselves.
    listBackendSets().then((r) => setBackendSetNames(r.sets.map((x) => x.name))).catch(() => {})
    return () => clearInterval(id)
  }, [])

  if (error) {
    return (
      <div>
        <PageHeader title="Traffic" />
        <Notice tone="bad">{error}</Notice>
      </div>
    )
  }

  if (!metrics) {
    return (
      <div>
        <PageHeader title="Traffic" />
        <Loading />
      </div>
    )
  }

  const protocolEntries = Object.entries(metrics.protocolCounts).sort((a, b) => b[1] - a[1])
  const maxProtocolCount = Math.max(1, ...protocolEntries.map(([, c]) => c));
  const totalStatements = metrics.totalReads + metrics.totalWrites + metrics.totalOther;
  const maxSqlCost = Math.max(1, ...metrics.topSql.map((s) => s.totalMs));
  const maxBackendCost = Math.max(1, ...metrics.byBackend.map((x) => x.totalMs))

  const donutSegments = (() => {
    const total = Math.max(1, metrics.totalReads + metrics.totalWrites + metrics.totalOther)
    const parts = [
      { label: 'Reads', value: metrics.totalReads, color: 'var(--sy-series-2)' },
      { label: 'Writes', value: metrics.totalWrites, color: 'var(--sy-series-1)' },
      { label: 'Other', value: metrics.totalOther, color: 'var(--sy-series-5)' },
    ].filter((p) => p.value > 0)
    let offset = 0
    const radius = 42
    const circumference = 2 * Math.PI * radius
    return parts.map((p) => {
      const fraction = p.value / total
      const dash = fraction * circumference
      const seg = { ...p, fraction, dashArray: `${dash} ${circumference - dash}`, dashOffset: -offset }
      offset += dash
      return seg
    })
  })()

  const kpis: KpiItem[] = [
    { label: 'Reads / sec', value: metrics.readsPerSec.toFixed(1), hint: `${formatNumber(metrics.totalReads)} total reads` },
    { label: 'Writes / sec', value: metrics.writesPerSec.toFixed(1), hint: `${formatNumber(metrics.totalWrites)} total writes` },
    { label: 'Statements total', value: formatNumber(totalStatements), hint: 'since this process started' },
    { label: 'Protocols active', value: protocolEntries.length, hint: protocolEntries.map(([n]) => n).join(', ') || 'none yet' },
    { label: 'Avg RTT', value: metrics.avgRttMs === null ? '—' : `${metrics.avgRttMs} ms`, hint: metrics.rttSamples === 0 ? 'no samples yet' : `${formatNumber(metrics.rttSamples)} request(s) measured` },
  ]

  return (
    <div>
      <PageHeader
        title="Traffic"
        description="Live protocol usage, read/write throughput, and the SQL costing you the most, across every backend Warp fronts."
        actions={<StatusPill tone="ok">Live · updated {lastUpdated ? lastUpdated.toLocaleTimeString() : '—'}</StatusPill>}
      />
      <KpiStrip items={kpis} label="Traffic figures" />

      <div className={styles.grid}>
        <Section title="Wire protocol traffic" meta="Statements per protocol since process start">
          {protocolEntries.length === 0 ? (
            <div className={styles.empty}>No traffic yet — send a query through any wire protocol to see it here.</div>
          ) : (
            protocolEntries.map(([name, count], i) => (
              <div className={styles.protoRow} key={name}>
                <span className={styles.protoDot} style={{ background: colorFor(name, i) }} />
                <span className={styles.protoName}>{name}</span>
                <span className={styles.protoBarTrack}>
                  <span className={styles.protoBarFill} style={{
                    width: `${(count / maxProtocolCount) * 100}%`,
                    background: colorFor(name, i),
                  }} />
                </span>
                <span className={styles.protoCount}>{formatNumber(count)}</span>
              </div>
            ))
          )}
        </Section>

        <Section title="Reads vs. writes" meta="Share of all statements executed">
          {totalStatements === 0 ? (
            <div className={styles.empty}>Nothing executed yet.</div>
          ) : (
            <div className={styles.donutWrap}>
              <svg width="112" height="112" viewBox="0 0 100 100" role="img" aria-label="Reads vs writes vs other, as a donut chart">
                <circle cx="50" cy="50" r="42" fill="none" stroke="var(--sy-line)" strokeWidth="12" />
                {donutSegments.map((s) => (
                  <circle key={s.label} cx="50" cy="50" r="42" fill="none" stroke={s.color} strokeWidth="12"
                    strokeDasharray={s.dashArray} strokeDashoffset={s.dashOffset}
                    transform="rotate(-90 50 50)" strokeLinecap="butt" />
                ))}
                <text x="50" y="47" textAnchor="middle" fontSize="13" fontWeight="500" fill="var(--sy-ink)">
                  {formatNumber(totalStatements)}
                </text>
                <text x="50" y="60" textAnchor="middle" fontSize="7" fill="var(--sy-muted)">statements</text>
              </svg>
              <div className={styles.legend}>
                {donutSegments.map((s) => (
                  <div className={styles.legendRow} key={s.label}>
                    <span className={styles.legendDot} style={{ background: s.color }} />
                    {s.label}
                    <span className={styles.legendPct}>{(s.fraction * 100).toFixed(0)}%</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </Section>
      </div>

      <Section flush title="Traffic by backend" meta={
        <>
          <Link to="/backend-sets">Backend sets</Link>
          {backendSetNames.length > 0 && <> ({backendSetNames.join(', ')})</>}
        </>
      }>
        {metrics.byBackend.length === 0 ? (
          <div className={styles.empty}>No traffic yet.</div>
        ) : (
          <DataTable caption="Traffic by backend" minWidth={640}>
            <thead>
              <tr>
                <th>Backend</th>
                <th className={styles.r}>Calls</th>
                <th className={styles.r}>Reads</th>
                <th className={styles.r}>Writes</th>
                <th className={styles.r}>Avg</th>
                <th className={styles.r}>Total cost</th>
              </tr>
            </thead>
            <tbody>
              {metrics.byBackend.map((b) => (
                <tr key={b.backend}>
                  <td className={styles.sqlText}>{b.backend}</td>
                  <td className={styles.numCell}>{formatNumber(b.calls)}</td>
                  <td className={styles.numCell}>{formatNumber(b.reads)}</td>
                  <td className={styles.numCell}>{formatNumber(b.writes)}</td>
                  <td className={styles.numCell}>{b.avgMs} ms</td>
                  <td className={styles.numCell}>
                    <div className={styles.costCell}>
                      <span>{formatNumber(b.totalMs)} ms</span>
                      <span className={styles.costBarTrack}>
                        <span className={styles.costBarFill} style={{ width: `${(b.totalMs / maxBackendCost) * 100}%` }} />
                      </span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </Section>

      <Section flush title="Top 10 SQL by cost" meta="Ranked by total time spent">
        {metrics.topSql.length === 0 ? (
          <div className={styles.empty}>No SQL captured yet.</div>
        ) : (
          <DataTable caption="Top SQL by cost" minWidth={720}>
            <thead>
              <tr>
                <th></th>
                <th>SQL</th>
                <th className={styles.r}>Calls</th>
                <th className={styles.r}>Avg exec</th>
                <th className={styles.r}>Avg RTT</th>
                <th className={styles.r}>Total cost</th>
              </tr>
            </thead>
            <tbody>
              {metrics.topSql.map((s, i) => (
                <tr key={i}>
                  <td className={styles.sqlRank}>{i + 1}</td>
                  <td className={styles.sqlText}>{s.sql}</td>
                  <td className={styles.numCell}>{formatNumber(s.calls)}</td>
                  <td className={styles.numCell}>{s.avgMs} ms</td>
                  <td className={styles.numCell}>{s.avgRttMs === null ? '—' : `${s.avgRttMs} ms`}</td>
                  <td className={styles.numCell}>
                    <div className={styles.costCell}>
                      <span>{formatNumber(s.totalMs)} ms</span>
                      <span className={styles.costBarTrack}>
                        <span className={styles.costBarFill} style={{ width: `${(s.totalMs / maxSqlCost) * 100}%` }} />
                      </span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </Section>

      <Section flush title="MCP tool calls" meta="Server-side time and error counts">
        {metrics.mcpTools.length === 0 ? (
          <div className={styles.empty}>No MCP tool calls yet.</div>
        ) : (
          <DataTable caption="MCP tool calls" minWidth={560}>
            <thead>
              <tr>
                <th>Tool</th>
                <th className={styles.r}>Calls</th>
                <th className={styles.r}>Errors</th>
                <th className={styles.r}>Avg time</th>
                <th className={styles.r}>Total time</th>
              </tr>
            </thead>
            <tbody>
              {metrics.mcpTools.map((t) => (
                <tr key={t.tool}>
                  <td className={styles.sqlText}>{t.tool}</td>
                  <td className={styles.numCell}>{formatNumber(t.calls)}</td>
                  <td className={styles.numCell}>{t.errors > 0 ? t.errors : '—'}</td>
                  <td className={styles.numCell}>{t.avgMs} ms</td>
                  <td className={styles.numCell}>{formatNumber(t.totalMs)} ms</td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </Section>

      <Section flush title="RTT by outcome" meta="Cache hit vs. real Postgres read or write, per protocol">
        {metrics.rttByOutcome.length === 0 ? (
          <div className={styles.empty}>No cacheable or SQL traffic measured yet.</div>
        ) : (
          <DataTable caption="RTT by outcome" minWidth={560}>
            <thead>
              <tr>
                <th>Protocol</th>
                <th>Outcome</th>
                <th className={styles.r}>Calls</th>
                <th className={styles.r}>Avg time</th>
                <th className={styles.r}>Total time</th>
              </tr>
            </thead>
            <tbody>
              {metrics.rttByOutcome.map((r) => (
                <tr key={`${r.protocol}-${r.outcome}`}>
                  <td className={styles.sqlText}>{r.protocol}</td>
                  <td className={styles.sqlText}>{outcomeLabel(r.outcome)}</td>
                  <td className={styles.numCell}>{formatNumber(r.calls)}</td>
                  <td className={styles.numCell}>{r.avgMs} ms</td>
                  <td className={styles.numCell}>{formatNumber(r.totalMs)} ms</td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </Section>
    </div>
  )
}

function outcomeLabel(outcome: string): string {
  switch (outcome) {
    case 'cache_hit':
      return 'Cache hit'
    case 'pg_read':
      return 'Postgres read'
    case 'pg_write':
      return 'Postgres write'
    case 'enqueue':
      return 'Enqueue'
    case 'dequeue':
      return 'Dequeue'
    default:
      return outcome
  }
}
