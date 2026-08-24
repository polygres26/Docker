import { useEffect, useState } from 'react'
import { type QueueInfo, deleteQueue, listQueues } from '../api/client'

const REFRESH_MS = 5000

function Badge({ children, tone = 'muted' }: { children: React.ReactNode; tone?: 'muted' | 'accent' | 'warn' }) {
  const colors = {
    muted: { background: 'var(--surface-2, #f2f2f2)', color: 'var(--muted)' },
    accent: { background: 'var(--accent-soft)', color: 'var(--accent-strong)' },
    warn: { background: 'var(--hard-soft, #fbeae8)', color: 'var(--hard, crimson)' },
  }[tone]
  return (
    <span style={{
      display: 'inline-block', padding: '2px 7px', borderRadius: 5, fontSize: 11.5, fontWeight: 600,
      ...colors,
    }}>
      {children}
    </span>
  )
}

function QueueRow({ queue, onDeleted }: { queue: QueueInfo; onDeleted: (name: string) => void }) {
  const [deleting, setDeleting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleDelete() {
    if (!confirm(`Delete queue "${queue.name}"? This drops its physical table and cannot be undone.`)) return
    setDeleting(true)
    setError(null)
    try {
      await deleteQueue(queue.name)
      onDeleted(queue.name)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setDeleting(false)
    }
  }

  return (
    <tr style={{ borderTop: '1px solid var(--border)' }}>
      <td style={{ padding: '9px 10px', fontFamily: 'monospace', fontSize: 12.5 }}>{queue.name}</td>
      <td style={{ padding: '9px 10px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{queue.visible}</td>
      <td style={{ padding: '9px 10px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{queue.inFlight}</td>
      <td style={{ padding: '9px 10px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{queue.visibilityTimeout}s</td>
      <td style={{ padding: '9px 10px' }}>{queue.fifo ? <Badge tone="accent">FIFO</Badge> : <span style={{ color: 'var(--muted)' }}>—</span>}</td>
      <td style={{ padding: '9px 10px', fontSize: 12.5 }}>
        {queue.dlqQueueName
          ? <span>{queue.dlqQueueName} <span style={{ color: 'var(--muted)' }}>(after {queue.maxReceiveCount})</span></span>
          : <span style={{ color: 'var(--muted)' }}>—</span>}
      </td>
      <td style={{ padding: '9px 10px', fontSize: 12.5, fontFamily: 'monospace', color: 'var(--muted)' }}>{queue.backend}</td>
      <td style={{ padding: '9px 10px', textAlign: 'right' }}>
        <button type="button" onClick={handleDelete} disabled={deleting}
          style={{ fontSize: 12, padding: '3px 9px', color: 'var(--hard, crimson)' }}>
          {deleting ? 'Deleting…' : 'Delete'}
        </button>
        {error && <div style={{ color: 'var(--error, crimson)', fontSize: 11.5, marginTop: 4 }}>{error}</div>}
      </td>
    </tr>
  )
}

/**
 * Read-only-ish view onto sqswire's queues (see com.polygres.wire.sqswire) -- live depth,
 * FIFO/DLQ attributes, and which shard backend each queue currently resolves to, so sharding
 * being real (not just configured) is visible at a glance the same way the Backends page shows
 * SQL's shard group. Polls every 5s rather than pushing -- queue depth is the kind of number
 * that's stale the instant it's read anyway, so a short poll is simpler than plumbing
 * server-sent events for it.
 */
export default function Queues() {
  const [queues, setQueues] = useState<QueueInfo[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function refresh() {
    try {
      const data = await listQueues()
      data.sort((a, b) => a.name.localeCompare(b.name))
      setQueues(data)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, REFRESH_MS)
    return () => clearInterval(id)
  }, [])

  function handleDeleted(name: string) {
    setQueues((prev) => (prev ? prev.filter((q) => q.name !== name) : prev))
  }

  const totalVisible = queues?.reduce((sum, q) => sum + q.visible, 0) ?? 0
  const totalInFlight = queues?.reduce((sum, q) => sum + q.inFlight, 0) ?? 0

  return (
    <div style={{ maxWidth: 960 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Queues</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        sqswire's Amazon SQS-compatible queues, backed by Postgres (pgmq-style storage, no
        extension required). Depth refreshes every {REFRESH_MS / 1000}s.
      </p>

      {error && <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>}

      {!queues && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : queues && queues.length === 0 ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>
          No queues yet. Create one with <code>CreateQueue</code> against sqswire's endpoint (default port 9324).
        </div>
      ) : queues ? (
        <>
          <div style={{ display: 'flex', gap: 24, marginBottom: 16, fontSize: 13 }}>
            <div><strong style={{ fontVariantNumeric: 'tabular-nums' }}>{queues.length}</strong> queue{queues.length === 1 ? '' : 's'}</div>
            <div><strong style={{ fontVariantNumeric: 'tabular-nums' }}>{totalVisible}</strong> visible</div>
            <div><strong style={{ fontVariantNumeric: 'tabular-nums' }}>{totalInFlight}</strong> in flight</div>
          </div>
          <div style={{ overflowX: 'auto', border: '1px solid var(--border)', borderRadius: 8 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ textAlign: 'left', color: 'var(--muted)', fontSize: 11.5, textTransform: 'uppercase', letterSpacing: 0.3 }}>
                  <th style={{ padding: '9px 10px' }}>Queue</th>
                  <th style={{ padding: '9px 10px', textAlign: 'right' }}>Visible</th>
                  <th style={{ padding: '9px 10px', textAlign: 'right' }}>In flight</th>
                  <th style={{ padding: '9px 10px', textAlign: 'right' }}>VT</th>
                  <th style={{ padding: '9px 10px' }}>Type</th>
                  <th style={{ padding: '9px 10px' }}>DLQ</th>
                  <th style={{ padding: '9px 10px' }}>Backend</th>
                  <th style={{ padding: '9px 10px' }}></th>
                </tr>
              </thead>
              <tbody>
                {queues.map((q) => <QueueRow key={q.name} queue={q} onDeleted={handleDeleted} />)}
              </tbody>
            </table>
          </div>
        </>
      ) : null}
    </div>
  )
}
