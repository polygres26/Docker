import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Database, FileText, Gauge } from 'lucide-react'
import { type Connection, type UploadedReport, listConnections, listReports } from '../api/client'

/**
 * Landing page after login -- modeled on versitygw's Admin Dashboard (see
 * https://github.com/versity/versitygw/wiki/WebGUI#admin-dashboard): a handful of stat cards
 * before drilling into Connections/Reports. Advisor has no gateway-health/uptime concept of its
 * own (it's a one-shot assessment tool, not a running proxy), so the cards here are the two
 * counts an admin actually opens this page to check, plus a shortcut into whichever one is empty.
 */

function StatCard({ icon: Icon, label, value, hint, to }: {
  icon: React.ComponentType<{ size?: number; strokeWidth?: number }>
  label: string
  value: string
  hint: string
  to: string
}) {
  return (
    <Link
      to={to}
      style={{
        border: '1px solid var(--border)', borderRadius: 10, padding: '16px 18px',
        background: 'var(--panel)', display: 'flex', flexDirection: 'column', gap: 8,
        textDecoration: 'none', color: 'inherit',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: 'var(--muted)', fontSize: 12.5, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
        <Icon size={15} strokeWidth={1.8} />
        {label}
      </div>
      <div style={{ fontSize: 26, fontWeight: 700, lineHeight: 1.1 }}>{value}</div>
      <div style={{ fontSize: 12, color: 'var(--muted)' }}>{hint}</div>
    </Link>
  )
}

export default function Dashboard() {
  const [connections, setConnections] = useState<Connection[] | null>(null)
  const [reports, setReports] = useState<UploadedReport[] | null>(null)

  useEffect(() => {
    listConnections().then(setConnections).catch(() => setConnections(null))
    listReports().then(setReports).catch(() => setReports(null))
  }, [])

  const analyzed = reports?.filter((r) => r.analyzedAt).length ?? 0

  return (
    <div>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Dashboard</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 24 }}>
        Overview of the databases you've connected and the reports you've analyzed.
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 14 }}>
        <StatCard
          icon={Database}
          label="Connections"
          value={connections ? String(connections.length) : '—'}
          hint={connections && connections.length > 0 ? connections.map((c) => c.name).join(', ') : 'none yet'}
          to="/connections"
        />
        <StatCard
          icon={FileText}
          label="Reports"
          value={reports ? String(reports.length) : '—'}
          hint={reports ? `${analyzed} analyzed` : 'none yet'}
          to="/reports"
        />
        <StatCard
          icon={Gauge}
          label="Sizing runs"
          value={connections && connections.length > 0 ? String(connections.length) : '0'}
          hint="run from a connection"
          to="/sizing"
        />
      </div>
    </div>
  )
}
