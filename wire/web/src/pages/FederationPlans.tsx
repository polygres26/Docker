import { useEffect, useState } from 'react'
import { FederationPlansNotEnabledError, type FederationLeafScan, type FederationPlanEntry, listFederationPlans } from '../api/client'
import { GitMerge } from 'lucide-react'
import { DataTable, EmptyState, KpiStrip, Loading, Notice, PageHeader, Section, StatusPill, Tag } from '../components/ui'

const REFRESH_MS = 5000

/** One backend/shard name as its own pill, so a query that fanned out across N shards or two
 * different vertically-sharded backends reads at a glance as "this many real sources," not one
 * opaque comma-joined string. */
function BackendPills({ backends }: { backends: string }) {
  const names = backends.split(',').map((s) => s.trim()).filter(Boolean)
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
      {names.map((name) => <Tag key={name}>{name}</Tag>)}
    </div>
  )
}

/** Real, MEASURED per-leaf-scan rows -- see LeafScanProfiler's own javadoc for exactly how these
 * numbers are obtained (a genuine, separate re-execution of just that one leaf's own pushed-down
 * SQL against its own real backend). A bar under each row's elapsed time, scaled against the
 * slowest leaf in this same plan, makes real skew across shards/backends visible at a glance --
 * exactly the kind of thing a static EXPLAIN PLAN FOR estimate can never show. */
function LeafScansTable({ leafScans }: { leafScans: FederationLeafScan[] }) {
  const maxMs = Math.max(1, ...leafScans.map((l) => l.elapsedMillis))
  return (
    <table style={{ fontSize: 12 }}>
      <thead>
        <tr>
          <th style={{ padding: '4px 8px' }}>Backend</th>
          <th style={{ padding: '4px 8px' }}>Real leaf SQL sent</th>
          <th style={{ textAlign: 'right' }}>Measured time</th>
          <th style={{ textAlign: 'right' }}>Actual rows</th>
        </tr>
      </thead>
      <tbody>
        {leafScans.map((leaf, i) => (
          <tr key={i}>
            <td style={{ padding: '4px 8px' }}><Tag>{leaf.backend}</Tag></td>
            <td style={{
              padding: '4px 8px', fontFamily: 'monospace', maxWidth: 320,
              overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
            }} title={leaf.sqlText}>
              {leaf.errorMessage ? <span style={{ color: 'var(--hard)' }}>{leaf.errorMessage}</span> : leaf.sqlText}
            </td>
            <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', whiteSpace: 'nowrap' }}>
              <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                <div style={{ width: 60, height: 6, background: 'var(--sy-line)', borderRadius: 3, overflow: 'hidden' }}>
                  <div style={{
                    width: `${Math.max(4, Math.round((leaf.elapsedMillis / maxMs) * 100))}%`, height: '100%',
                    background: 'var(--sy-accent)',
                  }} />
                </div>
                {leaf.elapsedMillis} ms
              </div>
            </td>
            <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{leaf.rowCount}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function PlanRow({ entry }: { entry: FederationPlanEntry }) {
  const [expanded, setExpanded] = useState(false)
  const ageSeconds = Math.max(0, Math.round((Date.now() - new Date(entry.capturedAt).getTime()) / 1000))
  const age = ageSeconds < 60 ? `${ageSeconds}s ago` : ageSeconds < 3600 ? `${Math.round(ageSeconds / 60)}m ago` : `${Math.round(ageSeconds / 3600)}h ago`
  const canExpand = Boolean(entry.planText) || entry.leafScans.length > 0

  return (
    <>
      <tr style={{ cursor: canExpand ? 'pointer' : 'default' }}
        onClick={() => canExpand && setExpanded((v) => !v)}>
        <td >#{entry.planId}</td>
        <td style={{ fontSize: 12, color: 'var(--muted)', whiteSpace: 'nowrap' }}>{age}</td>
        <td style={{ minWidth: 140 }}><BackendPills backends={entry.backends} /></td>
        <td style={{
          padding: '9px 10px', fontFamily: 'monospace', fontSize: 12,
          maxWidth: 420, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }} title={entry.sqlText}>
          {entry.sqlText}
        </td>
        <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{entry.elapsedMillis} ms</td>
        <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{entry.rowCount}</td>
        <td style={{ padding: '9px 10px' }}>
          {entry.success ? <StatusPill tone="ok">OK</StatusPill> : <StatusPill tone="bad">Failed</StatusPill>}
        </td>
        <td style={{ fontSize: 12, color: 'var(--muted)' }}>
          {canExpand ? (
            <button type="button" aria-expanded={expanded} onClick={(e) => { e.stopPropagation(); setExpanded((v) => !v) }}
              style={{ border: 0, background: 'none', padding: 0, minHeight: 0, color: 'var(--sy-accent-strong)', fontSize: 12 }}>
              {expanded ? '▾ hide plan' : '▸ show plan'}
            </button>
          ) : '—'}
        </td>
      </tr>
      {expanded && (
        <tr>
          <td colSpan={8} style={{ padding: '10px 10px 14px', background: 'var(--sy-surface-2)' }}>
            {!entry.success && entry.errorMessage && (
              <div style={{ color: 'var(--hard)', fontSize: 12.5, marginBottom: 8 }}>{entry.errorMessage}</div>
            )}
            {entry.leafScans.length > 0 && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.3 }}>
                  Real measured per-shard/backend scan (actual rows &amp; time, not estimated)
                </div>
                <div style={{ border: '1px solid var(--border)', borderRadius: 6, overflow: 'hidden', background: 'var(--surface)' }}>
                  <LeafScansTable leafScans={entry.leafScans} />
                </div>
              </div>
            )}
            {entry.planText && (
              <>
                <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.3 }}>
                  Real Calcite plan (EXPLAIN PLAN FOR — planner's own estimate, not measured)
                </div>
                <pre style={{
                  margin: 0, fontSize: 12, fontFamily: 'monospace', whiteSpace: 'pre', overflowX: 'auto',
                  background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 6, padding: 10,
                }}>
                  {entry.planText}
                </pre>
              </>
            )}
          </td>
        </tr>
      )}
    </>
  )
}

/**
 * Real, captured history of every federated query -- one from {@code ShardJoinExecutor} (a JOIN
 * across the SAME table horizontally partitioned by row, e.g. shard.orders on shard1/shard2) or
 * {@code SchemaFederationStage} (a JOIN across two DIFFERENT tables vertically sharded onto
 * separate backends, e.g. orders_db.orders + customers_db.customers) both land here, since both
 * write into the same shared SqlPlanStore -- see MetricsServer's own /api/federation/plans
 * javadoc. Each row is a genuine Calcite EXPLAIN PLAN FOR plan tree, not a synthetic summary --
 * click a row to see exactly how the planner chose to join/union/scan, informed by real
 * pg_class.reltuples row-count statistics (StatisticsAwareSchema/StatisticsStore) instead of
 * Calcite's own default Statistics.UNKNOWN.
 */
export default function FederationPlans() {
  const [plans, setPlans] = useState<FederationPlanEntry[] | null>(null)
  const [notEnabled, setNotEnabled] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function refresh() {
    try {
      const data = await listFederationPlans()
      setPlans(data)
      setNotEnabled(false)
      setError(null)
    } catch (e) {
      if (e instanceof FederationPlansNotEnabledError) {
        setNotEnabled(true)
        setError(null)
        return
      }
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, REFRESH_MS)
    return () => clearInterval(id)
  }, [])

  const failedCount = plans?.filter((p) => !p.success).length ?? 0
  const avgMs = plans && plans.length > 0 ? Math.round(plans.reduce((sum, p) => sum + p.elapsedMillis, 0) / plans.length) : 0

  return (
    <div>
      <PageHeader title="Federation Plans" description={<>Real captured history of every cross-shard and cross-backend {'JOIN'} Warp has federated via Calcite — a genuine <code>EXPLAIN PLAN FOR</code> plan, timing, and row count per query, the same {'V$SQL_PLAN'}-style visibility a real database gives you for a query that spans several of your own backends. Expand a row for the planner's own estimated plan tree AND real, MEASURED actual rows/time per shard or backend (Calcite's own <code>EXPLAIN PLAN FOR</code>{' '} only ever estimates — this re-runs each leaf's own pushed-down scan separately to get real numbers). Refreshes every {REFRESH_MS / 1000}s.</>} />

      {notEnabled && (
        <Section>
          <EmptyState icon={<GitMerge size={18} aria-hidden="true" />} title="Federation plan history isn't enabled">
            Set <code>WARP_FEDERATION_PLAN_HISTORY=&lt;capacity&gt;</code> (e.g. <code>200</code>) to start capturing every federated query's real plan here.
          </EmptyState>
        </Section>
      )}

      {error && <Notice tone="bad">{error}</Notice>}

      {!plans && !error && !notEnabled ? (
        <Loading />
      ) : plans && plans.length === 0 ? (
        <Section>
          <EmptyState icon={<GitMerge size={18} aria-hidden="true" />} title="No federated queries captured yet">
            This fills in the moment a query joins across shards (<code>ShardJoinExecutor</code>) or across
            vertically-sharded backends (<code>SchemaFederationStage</code>).
          </EmptyState>
        </Section>
      ) : plans ? (
        <>
          <KpiStrip label="Plan history" items={[
            { label: 'Captured', value: plans.length },
            { label: 'Avg elapsed', value: `${avgMs} ms` },
            { label: 'Failed', value: failedCount, hint: failedCount > 0 ? 'see status column' : 'none' },
          ]} />
          <Section flush title="Captured plans" meta={`Refreshes every ${REFRESH_MS / 1000}s`}>
            <DataTable caption="Federation plans" minWidth={880}>
              <thead>
                <tr>
                  <th>#</th>
                  <th>When</th>
                  <th>Backends</th>
                  <th>SQL</th>
                  <th style={{ textAlign: 'right' }}>Elapsed</th>
                  <th style={{ textAlign: 'right' }}>Rows</th>
                  <th>Status</th>
                  <th>Plan</th>
                </tr>
              </thead>
              <tbody>
                {plans.map((p) => <PlanRow key={p.planId} entry={p} />)}
              </tbody>
            </DataTable>
          </Section>
        </>
      ) : null}
    </div>
  )
}
