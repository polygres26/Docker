import { useEffect, useState } from 'react'
import { Activity, ArrowDownToLine, ArrowUpFromLine, Gauge, Server, Waypoints } from 'lucide-react'
import { type BackendInfo, type NodeInfo, type WireMetricsSummary, getWireMetrics, listBackends, listNodes } from '../api/client'

/**
 * Landing page after connecting -- modeled on versitygw's Admin Dashboard (see
 * https://github.com/versity/versitygw/wiki/WebGUI#admin-dashboard): a handful of stat cards
 * giving an at-a-glance read on gateway health before drilling into any one page. Polywire has no
 * single "uptime" figure exposed yet, so this leans on what /api/metrics/summary, /api/backends
 * and /api/nodes already report: throughput, backend count, and node topology health.
 */

interface StatCardProps {
  icon: React.ComponentType<{ size?: number; strokeWidth?: number }>
  label: string
  value: string
  hint?: string
  tone?: 'ok' | 'warn' | 'default'
}

function StatCard({ icon: Icon, label, value, hint, tone = 'default' }: StatCardProps) {
  const toneColor = tone === 'ok' ? 'var(--accent-strong)' : tone === 'warn' ? 'var(--hard, crimson)' : 'var(--text)'
  return (
    <div style={{
      border: '1px solid var(--border)', borderRadius: 10, padding: '16px 18px',
      background: 'var(--panel)', display: 'flex', flexDirection: 'column', gap: 8, minWidth: 0,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: 'var(--muted)', fontSize: 12.5, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
        <Icon size={15} strokeWidth={1.8} />
        {label}
      </div>
      <div style={{ fontSize: 26, fontWeight: 700, color: toneColor, lineHeight: 1.1 }}>{value}</div>
      {hint && <div style={{ fontSize: 12, color: 'var(--muted)' }}>{hint}</div>}
    </div>
  )
}

export default function Dashboard() {
  const [metrics, setMetrics] = useState<WireMetricsSummary | null>(null)
  const [backends, setBackends] = useState<BackendInfo[] | null>(null)
  const [nodes, setNodes] = useState<NodeInfo[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getWireMetrics().then(setMetrics).catch((e) => setError(e instanceof Error ? e.message : String(e)))
    listBackends().then(setBackends).catch(() => setBackends(null))
    // Node heartbeats are a newer, optional endpoint (single-node deployments may not run the
    // heartbeat loop at all) -- absence here just means "no topology data," not an error.
    listNodes().then(setNodes).catch(() => setNodes(null))
  }, [])

  const upNodes = nodes?.filter((n) => n.status === 'up').length ?? null
  const staleNodes = nodes?.filter((n) => n.status === 'stale').length ?? 0
  const totalCalls = metrics
    ? Object.values(metrics.protocolCounts).reduce((sum, c) => sum + c, 0)
    : null

  return (
    <div>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Dashboard</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 24 }}>
        Live snapshot of this Polywire process -- throughput, configured backends, and node topology.
      </p>

      {error && <div style={{ marginBottom: 16, color: 'var(--hard, crimson)', fontSize: 13 }}>{error}</div>}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 14, marginBottom: 28 }}>
        <StatCard
          icon={ArrowDownToLine}
          label="Reads / sec"
          value={metrics ? metrics.readsPerSec.toFixed(1) : '—'}
          hint={metrics ? `${metrics.totalReads.toLocaleString()} total reads` : undefined}
        />
        <StatCard
          icon={ArrowUpFromLine}
          label="Writes / sec"
          value={metrics ? metrics.writesPerSec.toFixed(1) : '—'}
          hint={metrics ? `${metrics.totalWrites.toLocaleString()} total writes` : undefined}
        />
        <StatCard
          icon={Gauge}
          label="Avg round-trip"
          value={metrics?.avgRttMs != null ? `${metrics.avgRttMs.toFixed(1)}ms` : '—'}
          hint={metrics ? `${metrics.rttSamples.toLocaleString()} samples` : undefined}
        />
        <StatCard
          icon={Activity}
          label="Statements handled"
          value={totalCalls != null ? totalCalls.toLocaleString() : '—'}
          hint={metrics ? `${metrics.totalOther.toLocaleString()} other` : undefined}
        />
        <StatCard
          icon={Server}
          label="Backends"
          value={backends ? String(backends.length) : '—'}
          hint={backends && backends.length > 0 ? backends.map((b) => b.name).join(', ') : 'none configured'}
        />
        <StatCard
          icon={Waypoints}
          label="Nodes up"
          value={upNodes != null ? String(upNodes) : '—'}
          tone={staleNodes > 0 ? 'warn' : 'ok'}
          hint={nodes ? (staleNodes > 0 ? `${staleNodes} stale` : 'all healthy') : 'topology not reporting'}
        />
      </div>

      {metrics && metrics.byBackend.length > 0 && (
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Traffic by backend</div>
          <div style={{ border: '1px solid var(--border)', borderRadius: 8, overflow: 'hidden' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ background: 'var(--panel)', textAlign: 'left' }}>
                  <th style={{ padding: '8px 12px', fontWeight: 600, color: 'var(--muted)' }}>Backend</th>
                  <th style={{ padding: '8px 12px', fontWeight: 600, color: 'var(--muted)' }}>Calls</th>
                  <th style={{ padding: '8px 12px', fontWeight: 600, color: 'var(--muted)' }}>Reads</th>
                  <th style={{ padding: '8px 12px', fontWeight: 600, color: 'var(--muted)' }}>Writes</th>
                  <th style={{ padding: '8px 12px', fontWeight: 600, color: 'var(--muted)' }}>Avg ms</th>
                </tr>
              </thead>
              <tbody>
                {metrics.byBackend.map((b) => (
                  <tr key={b.backend} style={{ borderTop: '1px solid var(--border)' }}>
                    <td style={{ padding: '8px 12px', fontFamily: 'monospace' }}>{b.backend}</td>
                    <td style={{ padding: '8px 12px' }}>{b.calls.toLocaleString()}</td>
                    <td style={{ padding: '8px 12px' }}>{b.reads.toLocaleString()}</td>
                    <td style={{ padding: '8px 12px' }}>{b.writes.toLocaleString()}</td>
                    <td style={{ padding: '8px 12px' }}>{b.avgMs.toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
