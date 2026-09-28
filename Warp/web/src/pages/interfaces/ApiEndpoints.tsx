import { RefreshCw } from 'lucide-react'
import { getWireMetrics, listInterfaces } from '../../api/client'
import { Button, KpiStrip, Loading, Notice, PageHeader, Section, compact, type KpiItem } from '../../components/ui'
import { useLoad } from '../../hooks'
import { InterfaceTable } from './InterfaceTable'

const POLL_MS = 10_000

/** API endpoints: the storage, messaging, search and database-API frontends Warp emulates on Postgres (or relays), from GET /api/interfaces. */
export default function ApiEndpoints() {
  const ifaces = useLoad(listInterfaces, POLL_MS)
  const metrics = useLoad(getWireMetrics, POLL_MS)
  const rows = (ifaces.data?.interfaces ?? []).filter((i) => i.kind === 'api')
  const counted = rows.filter((r) => r.requests !== null)
  const total = counted.reduce((s, r) => s + (r.requests ?? 0), 0)
  const unhosted = rows.filter((r) => r.store && (r.hosts?.length ?? 0) === 0)
  const kpis: KpiItem[] = [
    { label: 'Listening APIs', value: rows.length, hint: rows.length ? `${rows.filter((r) => r.mode === 'Emulate').length} emulated on Postgres` : 'none' },
    { label: 'Operations served', value: compact(total), hint: `${counted.length} of ${rows.length} counted, since start` },
    { label: 'Not enabled on a backend', value: unhosted.length, hint: unhosted.length ? 'served from the default backend' : 'every store is enabled on a backend' },
  ]
  return (
    <div>
      <PageHeader title="API endpoints" description="Cloud and database APIs Warp implements on top of your Postgres backends. Each is served from one backend set; enable its store on a backend under Infrastructure."
        actions={<Button icon={<RefreshCw size={14} aria-hidden="true" />} onClick={() => { ifaces.reload(); metrics.reload() }}>Refresh</Button>} />
      {ifaces.error && <Notice tone="bad">Could not load interfaces: {ifaces.error}</Notice>}
      {ifaces.loading ? <Loading>Loading endpoints…</Loading> : <KpiStrip items={kpis} label="API endpoint figures" />}
      <Section flush title="Endpoints" meta={`${rows.length} listening`}>
        <InterfaceTable rows={rows} metrics={metrics.data} emptyTitle="No API endpoint is listening" emptyText="Enable a protocol store on a Postgres backend (Infrastructure) and its frontend starts at the next restart, or set WARP_<PROTOCOL>_PORT." />
      </Section>
    </div>
  )
}
