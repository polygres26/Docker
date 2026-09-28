import { useEffect, useState } from 'react'
import { type QueueInfo, deleteQueue, listQueues } from '../api/client'
import { ListOrdered } from 'lucide-react'
import { DataTable, EmptyState, KpiStrip, Loading, Notice, PageHeader, Section, Tag } from '../components/ui'

const REFRESH_MS = 5000

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
    <tr>
      <td className="mono">{queue.name}</td>
      <td className="num">{queue.visible}</td>
      <td className="num">{queue.inFlight}</td>
      <td className="num">{queue.visibilityTimeout}s</td>
      <td>{queue.fifo ? <Tag>FIFO</Tag> : <span style={{ color: 'var(--muted)' }}>—</span>}</td>
      <td>
        {queue.dlqQueueName
          ? <span>{queue.dlqQueueName} <span style={{ color: 'var(--muted)' }}>(after {queue.maxReceiveCount})</span></span>
          : <span style={{ color: 'var(--muted)' }}>—</span>}
      </td>
      <td className="mono" style={{ color: 'var(--muted)' }}>{queue.backend}</td>
      <td style={{ textAlign: 'right' }}>
        <button type="button" onClick={handleDelete} disabled={deleting}
          style={{ color: 'var(--sy-red)' }}>
          {deleting ? 'Deleting…' : 'Delete'}
        </button>
        {error && <Notice tone="bad">{error}</Notice>}
      </td>
    </tr>
  )
}

/**
 * Read-only-ish view onto sqswire's queues (see com.sayonora.warp.sqswire) -- live depth,
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
    <div>
      <PageHeader title="Queues" description={<>sqswire's Amazon SQS-compatible queues, backed by Postgres (pgmq-style storage, no extension required). Depth refreshes every {REFRESH_MS / 1000}s.</>} />

      {error && <Notice tone="bad">{error}</Notice>}

      {!queues && !error ? (
        <Loading />
      ) : queues && queues.length === 0 ? (
        <Section>
          <EmptyState icon={<ListOrdered size={18} aria-hidden="true" />} title="No queues yet">
            Create one with <code>CreateQueue</code> against sqswire's endpoint (default port 9324).
          </EmptyState>
        </Section>
      ) : queues ? (
        <>
          <KpiStrip label="Queue totals" items={[
            { label: 'Queues', value: queues.length },
            { label: 'Visible', value: totalVisible.toLocaleString() },
            { label: 'In flight', value: totalInFlight.toLocaleString() },
          ]} />
          <Section flush title="All queues" meta={`Refreshes every ${REFRESH_MS / 1000}s`}>
            <DataTable caption="Queues" minWidth={760}>
              <thead>
                <tr>
                  <th>Queue</th>
                  <th style={{ textAlign: 'right' }}>Visible</th>
                  <th style={{ textAlign: 'right' }}>In flight</th>
                  <th style={{ textAlign: 'right' }}>VT</th>
                  <th>Type</th>
                  <th>DLQ</th>
                  <th>Backend</th>
                  <th aria-label="Actions"></th>
                </tr>
              </thead>
              <tbody>
                {queues.map((q) => <QueueRow key={q.name} queue={q} onDeleted={handleDeleted} />)}
              </tbody>
            </DataTable>
          </Section>
        </>
      ) : null}
    </div>
  )
}
