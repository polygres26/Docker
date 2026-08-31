import { useEffect, useState } from 'react'
import { FederationPlansNotEnabledError, type FederationLeafScan, type FederationPlanEntry, listFederationPlans } from '../api/client'

const REFRESH_MS = 5000

function Badge({ children, tone = 'muted' }: { children: React.ReactNode; tone?: 'muted' | 'accent' | 'warn' | 'good' }) {
  const colors = {
    muted: { background: 'var(--surface-2, #f2f2f2)', color: 'var(--muted)' },
    accent: { background: 'var(--accent-soft)', color: 'var(--accent-strong)' },
    warn: { background: 'var(--hard-soft, #fbeae8)', color: 'var(--hard, crimson)' },
    good: { background: '#e6f6ec', color: '#1a7f4b' },
  }[tone]
  return (
    <span style={{
      display: 'inline-block', padding: '2px 7px', borderRadius: 5, fontSize: 11.5, fontWeight: 600,
      whiteSpace: 'nowrap', ...colors,
    }}>
      {children}
    </span>
  )
}

/** One backend/shard name as its own pill, so a query that fanned out across N shards or two
 * different vertically-sharded backends reads at a glance as "this many real sources," not one
 * opaque comma-joined string. */
function BackendPills({ backends }: { backends: string }) {
  const names = backends.split(',').map((s) => s.trim()).filter(Boolean)
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
      {names.map((name) => <Badge key={name} tone="accent">{name}</Badge>)}
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
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
      <thead>
        <tr style={{ textAlign: 'left', color: 'var(--muted)', fontSize: 10.5, textTransform: 'uppercase', letterSpacing: 0.3 }}>
          <th style={{ padding: '4px 8px' }}>Backend</th>
          <th style={{ padding: '4px 8px' }}>Real leaf SQL sent</th>
          <th style={{ padding: '4px 8px', textAlign: 'right' }}>Measured time</th>
          <th style={{ padding: '4px 8px', textAlign: 'right' }}>Actual rows</th>
        </tr>
      </thead>
      <tbody>
        {leafScans.map((leaf, i) => (
          <tr key={i} style={{ borderTop: '1px solid var(--border)' }}>
            <td style={{ padding: '4px 8px' }}><Badge tone="accent">{leaf.backend}</Badge></td>
            <td style={{
              padding: '4px 8px', fontFamily: 'monospace', maxWidth: 320,
              overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
            }} title={leaf.sqlText}>
              {leaf.errorMessage ? <span style={{ color: 'var(--hard, crimson)' }}>{leaf.errorMessage}</span> : leaf.sqlText}
            </td>
            <td style={{ padding: '4px 8px', textAlign: 'right', fontVariantNumeric: 'tabular-nums', whiteSpace: 'nowrap' }}>
              <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                <div style={{ width: 60, height: 6, background: 'var(--surface-2, #eee)', borderRadius: 3, overflow: 'hidden' }}>
                  <div style={{
                    width: `${Math.max(4, Math.round((leaf.elapsedMillis / maxMs) * 100))}%`, height: '100%',
                    background: 'var(--accent-strong, #5b6cff)',
                  }} />
                </div>
                {leaf.elapsedMillis} ms
              </div>
            </td>
            <td style={{ padding: '4px 8px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{leaf.rowCount}</td>
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
      <tr style={{ borderTop: '1px solid var(--border)', cursor: canExpand ? 'pointer' : 'default' }}
        onClick={() => canExpand && setExpanded((v) => !v)}>
        <td style={{ padding: '9px 10px', fontVariantNumeric: 'tabular-nums', color: 'var(--muted)', fontSize: 12 }}>#{entry.planId}</td>
        <td style={{ padding: '9px 10px', fontSize: 12, color: 'var(--muted)', whiteSpace: 'nowrap' }}>{age}</td>
        <td style={{ padding: '9px 10px', minWidth: 140 }}><BackendPills backends={entry.backends} /></td>
        <td style={{
          padding: '9px 10px', fontFamily: 'monospace', fontSize: 12,
          maxWidth: 420, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }} title={entry.sqlText}>
          {entry.sqlText}
        </td>
        <td style={{ padding: '9px 10px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{entry.elapsedMillis} ms</td>
        <td style={{ padding: '9px 10px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{entry.rowCount}</td>
        <td style={{ padding: '9px 10px' }}>
          {entry.success ? <Badge tone="good">OK</Badge> : <Badge tone="warn">Failed</Badge>}
        </td>
        <td style={{ padding: '9px 10px', fontSize: 12, color: 'var(--muted)' }}>
          {canExpand ? (expanded ? '▾ hide plan' : '▸ show plan') : '—'}
        </td>
      </tr>
      {expanded && (
        <tr style={{ borderTop: '1px solid var(--border)' }}>
          <td colSpan={8} style={{ padding: '10px 10px 14px', background: 'var(--surface-2, #f8f8f8)' }}>
            {!entry.success && entry.errorMessage && (
              <div style={{ color: 'var(--hard, crimson)', fontSize: 12.5, marginBottom: 8 }}>{entry.errorMessage}</div>
            )}
            {entry.leafScans.length > 0 && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.3 }}>
                  Real measured per-shard/backend scan (actual rows &amp; time, not estimated)
                </div>
                <div style={{ border: '1px solid var(--border)', borderRadius: 6, overflow: 'hidden', background: 'var(--surface, #fff)' }}>
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
                  background: 'var(--surface, #fff)', border: '1px solid var(--border)', borderRadius: 6, padding: 10,
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
    <div style={{ maxWidth: 1100 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Federation Plans</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Real captured history of every cross-shard and cross-backend {'JOIN'} NexaGate has federated
        via Calcite — a genuine <code>EXPLAIN PLAN FOR</code> plan, timing, and row count per query,
        the same {'V$SQL_PLAN'}-style visibility a real database gives you for a query that spans
        several of your own backends. Expand a row for the planner's own estimated plan tree AND
        real, MEASURED actual rows/time per shard or backend (Calcite's own <code>EXPLAIN PLAN FOR</code>{' '}
        only ever estimates — this re-runs each leaf's own pushed-down scan separately to get real
        numbers). Refreshes every {REFRESH_MS / 1000}s.
      </p>

      {notEnabled && (
        <div style={{ color: 'var(--muted)', fontSize: 13, border: '1px dashed var(--border)', borderRadius: 8, padding: 16 }}>
          Federation plan history isn't enabled. Set <code>POLYWIRE_FEDERATION_PLAN_HISTORY=&lt;capacity&gt;</code> (e.g.{' '}
          <code>200</code>) to start capturing every federated query's real plan here.
        </div>
      )}

      {error && <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>}

      {!plans && !error && !notEnabled ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : plans && plans.length === 0 ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>
          No federated queries captured yet. This fills in the moment a query joins across shards
          (<code>ShardJoinExecutor</code>) or across vertically-sharded backends
          (<code>SchemaFederationStage</code>).
        </div>
      ) : plans ? (
        <>
          <div style={{ display: 'flex', gap: 24, marginBottom: 16, fontSize: 13 }}>
            <div><strong style={{ fontVariantNumeric: 'tabular-nums' }}>{plans.length}</strong> captured</div>
            <div><strong style={{ fontVariantNumeric: 'tabular-nums' }}>{avgMs}</strong> ms avg</div>
            {failedCount > 0 && (
              <div style={{ color: 'var(--hard, crimson)' }}>
                <strong style={{ fontVariantNumeric: 'tabular-nums' }}>{failedCount}</strong> failed
              </div>
            )}
          </div>
          <div style={{ overflowX: 'auto', border: '1px solid var(--border)', borderRadius: 8 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ textAlign: 'left', color: 'var(--muted)', fontSize: 11.5, textTransform: 'uppercase', letterSpacing: 0.3 }}>
                  <th style={{ padding: '9px 10px' }}>#</th>
                  <th style={{ padding: '9px 10px' }}>When</th>
                  <th style={{ padding: '9px 10px' }}>Backends</th>
                  <th style={{ padding: '9px 10px' }}>SQL</th>
                  <th style={{ padding: '9px 10px', textAlign: 'right' }}>Elapsed</th>
                  <th style={{ padding: '9px 10px', textAlign: 'right' }}>Rows</th>
                  <th style={{ padding: '9px 10px' }}>Status</th>
                  <th style={{ padding: '9px 10px' }}>Plan</th>
                </tr>
              </thead>
              <tbody>
                {plans.map((p) => <PlanRow key={p.planId} entry={p} />)}
              </tbody>
            </table>
          </div>
        </>
      ) : null}
    </div>
  )
}
