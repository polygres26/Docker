import { ArrowDownToLine, ArrowUpFromLine, Gauge, Link as LinkIcon, RefreshCw, Timer } from 'lucide-react'
import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { type WireMetricsSummary, getWireMetrics } from '../api/client'
import styles from './Metrics.module.css'

const PROTOCOL_COLORS: Record<string, string> = {
  pgwire: '#1f7a63',
  mywire: '#3d7fd9',
  mssqlwire: '#c2622f',
  orawire: '#8a4fd9',
  mongowire: '#2ea3a0',
  dynamowire: '#d9a12f',
}

function colorFor(name: string, index: number): string {
  return PROTOCOL_COLORS[name] ?? ['#5b6864', '#7a8a84', '#9aa8a2'][index % 3]
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return String(n)
}

/**
 * Live traffic dashboard for Polywire -- which wire protocol customers are actually running
 * (pgwire/mywire/mssqlwire/orawire, from SqlMetricsCollector's per-statement dialect tag), a
 * reads/sec vs writes/sec split, and the top 10 most expensive SQL shapes by cumulative execution
 * time. Polls the admin API every 5s -- cheap, and "live" is the point of this page.
 */
export default function Metrics() {
  const [metrics, setMetrics] = useState<WireMetricsSummary | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  function load() {
    getWireMetrics()
      .then((m) => { setMetrics(m); setError(null); setLastUpdated(new Date()) })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }

  useEffect(() => {
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [])

  if (error) {
    return (
      <div className={styles.page}>
        <h1 style={{ fontSize: 22, marginBottom: 4 }}>Metrics</h1>
        <p style={{ color: 'var(--error, crimson)', fontSize: 13 }}>{error}</p>
      </div>
    )
  }

  if (!metrics) {
    return (
      <div className={styles.page}>
        <h1 style={{ fontSize: 22, marginBottom: 4 }}>Metrics</h1>
        <p style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</p>
      </div>
    )
  }

  const protocolEntries = Object.entries(metrics.protocolCounts).sort((a, b) => b[1] - a[1])
  const maxProtocolCount = Math.max(1, ...protocolEntries.map(([, c]) => c));
  const totalStatements = metrics.totalReads + metrics.totalWrites + metrics.totalOther;
  const maxSqlCost = Math.max(1, ...metrics.topSql.map((s) => s.totalMs));

  const donutSegments = (() => {
    const total = Math.max(1, metrics.totalReads + metrics.totalWrites + metrics.totalOther)
    const parts = [
      { label: 'Reads', value: metrics.totalReads, color: 'var(--accent)' },
      { label: 'Writes', value: metrics.totalWrites, color: '#c2622f' },
      { label: 'Other', value: metrics.totalOther, color: '#8a9790' },
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

  return (
    <div className={styles.page}>
      <div className={styles.hero}>
        <div className={styles.heroTop}>
          <div>
            <h1 className={styles.heroTitle}>Polywire traffic</h1>
            <p className={styles.heroSubtitle}>
              Live protocol usage, read/write throughput, and the SQL costing you the most —
              across every backend Polywire fronts.
            </p>
          </div>
          <div className={styles.heroBadge}>
            <span className={styles.liveDot} />
            Live · updated {lastUpdated ? lastUpdated.toLocaleTimeString() : '—'}
          </div>
        </div>
        <div className={styles.heroStats}>
          <div className={styles.heroStat}>
            <div className={styles.heroStatLabel}><ArrowDownToLine size={13} /> Reads / sec</div>
            <div className={styles.heroStatValue}>{metrics.readsPerSec.toFixed(1)}</div>
            <div className={styles.heroStatSub}>{formatNumber(metrics.totalReads)} total reads</div>
          </div>
          <div className={styles.heroStat}>
            <div className={styles.heroStatLabel}><ArrowUpFromLine size={13} /> Writes / sec</div>
            <div className={styles.heroStatValue}>{metrics.writesPerSec.toFixed(1)}</div>
            <div className={styles.heroStatSub}>{formatNumber(metrics.totalWrites)} total writes</div>
          </div>
          <div className={styles.heroStat}>
            <div className={styles.heroStatLabel}><Gauge size={13} /> Statements total</div>
            <div className={styles.heroStatValue}>{formatNumber(totalStatements)}</div>
            <div className={styles.heroStatSub}>since this process started</div>
          </div>
          <div className={styles.heroStat}>
            <div className={styles.heroStatLabel}><LinkIcon size={13} /> Protocols active</div>
            <div className={styles.heroStatValue}>{protocolEntries.length}</div>
            <div className={styles.heroStatSub}>{protocolEntries.map(([n]) => n).join(', ') || 'none yet'}</div>
          </div>
          <div className={styles.heroStat}>
            <div className={styles.heroStatLabel}><Timer size={13} /> Avg RTT</div>
            <div className={styles.heroStatValue}>{metrics.avgRttMs === null ? '—' : `${metrics.avgRttMs} ms`}</div>
            <div className={styles.heroStatSub}>
              {metrics.rttSamples === 0 ? 'no samples yet' : `${formatNumber(metrics.rttSamples)} request(s) measured`}
            </div>
          </div>
        </div>
      </div>

      <div className={styles.grid}>
        <div className={styles.card}>
          <div className={styles.cardHeadRow}>
            <p className={styles.cardTitle}>Wire protocol traffic</p>
          </div>
          <p className={styles.cardSubtitle}>Statements handled per protocol since process start</p>
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
        </div>

        <div className={styles.card}>
          <div className={styles.cardHeadRow}>
            <p className={styles.cardTitle}>Reads vs. writes</p>
          </div>
          <p className={styles.cardSubtitle}>Share of all statements executed</p>
          {totalStatements === 0 ? (
            <div className={styles.empty}>Nothing executed yet.</div>
          ) : (
            <div className={styles.donutWrap}>
              <svg width="112" height="112" viewBox="0 0 100 100" role="img" aria-label="Reads vs writes vs other, as a donut chart">
                <circle cx="50" cy="50" r="42" fill="none" stroke="var(--border)" strokeWidth="12" />
                {donutSegments.map((s) => (
                  <circle key={s.label} cx="50" cy="50" r="42" fill="none" stroke={s.color} strokeWidth="12"
                    strokeDasharray={s.dashArray} strokeDashoffset={s.dashOffset}
                    transform="rotate(-90 50 50)" strokeLinecap="butt" />
                ))}
                <text x="50" y="47" textAnchor="middle" fontSize="13" fontWeight="700" fill="var(--text)">
                  {formatNumber(totalStatements)}
                </text>
                <text x="50" y="60" textAnchor="middle" fontSize="7" fill="var(--muted)">statements</text>
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
        </div>
      </div>

      <div className={styles.card} style={{ marginBottom: 16 }}>
        <div className={styles.cardHeadRow}>
          <p className={styles.cardTitle}>Traffic by backend</p>
        </div>
        <p className={styles.cardSubtitle}>
          Where statements actually landed, by routing target — see <Link to="/backends">Backends</Link> to
          change what's configured.
        </p>
        {metrics.byBackend.length === 0 ? (
          <div className={styles.empty}>No traffic yet.</div>
        ) : (
          <div className={styles.tableWrap}>
            <table className={styles.sqlTable}>
              <thead>
                <tr>
                  <th>Backend</th>
                  <th style={{ textAlign: 'right' }}>Calls</th>
                  <th style={{ textAlign: 'right' }}>Reads</th>
                  <th style={{ textAlign: 'right' }}>Writes</th>
                  <th style={{ textAlign: 'right' }}>Avg</th>
                  <th style={{ textAlign: 'right' }}>Total cost</th>
                </tr>
              </thead>
              <tbody>
                {metrics.byBackend.map((b) => {
                  const maxBackendCost = Math.max(1, ...metrics.byBackend.map((x) => x.totalMs))
                  return (
                    <tr key={b.backend}>
                      <td className={styles.sqlText}>{b.backend}</td>
                      <td className={styles.numCell}>{formatNumber(b.calls)}</td>
                      <td className={styles.numCell}>{formatNumber(b.reads)}</td>
                      <td className={styles.numCell}>{formatNumber(b.writes)}</td>
                      <td className={styles.numCell}>{b.avgMs} ms</td>
                      <td className={styles.numCell}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, justifyContent: 'flex-end' }}>
                          <span>{formatNumber(b.totalMs)} ms</span>
                          <span className={styles.costBarTrack}>
                            <span className={styles.costBarFill} style={{ width: `${(b.totalMs / maxBackendCost) * 100}%` }} />
                          </span>
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className={styles.card}>
        <div className={styles.cardHeadRow}>
          <p className={styles.cardTitle}>Top 10 SQL by cost</p>
          <RefreshCw size={13} color="var(--muted)" />
        </div>
        <p className={styles.cardSubtitle}>
          Your most expensive statements, ranked by total time spent.
        </p>
        {metrics.topSql.length === 0 ? (
          <div className={styles.empty}>No SQL captured yet.</div>
        ) : (
          <div className={styles.tableWrap}>
            <table className={styles.sqlTable}>
              <thead>
                <tr>
                  <th></th>
                  <th>SQL</th>
                  <th style={{ textAlign: 'right' }}>Calls</th>
                  <th style={{ textAlign: 'right' }}>Avg exec</th>
                  <th style={{ textAlign: 'right' }}>Avg RTT</th>
                  <th style={{ textAlign: 'right' }}>Total cost</th>
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
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8, justifyContent: 'flex-end' }}>
                        <span>{formatNumber(s.totalMs)} ms</span>
                        <span className={styles.costBarTrack}>
                          <span className={styles.costBarFill} style={{ width: `${(s.totalMs / maxSqlCost) * 100}%` }} />
                        </span>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className={styles.card}>
        <div className={styles.cardHeadRow}>
          <p className={styles.cardTitle}>MCP tool calls</p>
          <RefreshCw size={13} color="var(--muted)" />
        </div>
        <p className={styles.cardSubtitle}>
          Every tool call handled by the MCP server, with server-side time and error counts.
        </p>
        {metrics.mcpTools.length === 0 ? (
          <div className={styles.empty}>No MCP tool calls yet.</div>
        ) : (
          <div className={styles.tableWrap}>
            <table className={styles.sqlTable}>
              <thead>
                <tr>
                  <th>Tool</th>
                  <th style={{ textAlign: 'right' }}>Calls</th>
                  <th style={{ textAlign: 'right' }}>Errors</th>
                  <th style={{ textAlign: 'right' }}>Avg time</th>
                  <th style={{ textAlign: 'right' }}>Total time</th>
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
            </table>
          </div>
        )}
      </div>

      <div className={styles.card}>
        <div className={styles.cardHeadRow}>
          <p className={styles.cardTitle}>RTT by outcome</p>
          <RefreshCw size={13} color="var(--muted)" />
        </div>
        <p className={styles.cardSubtitle}>
          How long a cache hit takes vs. a real Postgres read or write, per wire protocol — the
          number that says whether the cache is actually paying for itself.
        </p>
        {metrics.rttByOutcome.length === 0 ? (
          <div className={styles.empty}>No cacheable or SQL traffic measured yet.</div>
        ) : (
          <div className={styles.tableWrap}>
            <table className={styles.sqlTable}>
              <thead>
                <tr>
                  <th>Protocol</th>
                  <th>Outcome</th>
                  <th style={{ textAlign: 'right' }}>Calls</th>
                  <th style={{ textAlign: 'right' }}>Avg time</th>
                  <th style={{ textAlign: 'right' }}>Total time</th>
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
            </table>
          </div>
        )}
      </div>
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
