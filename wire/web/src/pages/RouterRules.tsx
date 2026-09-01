import { useEffect, useState } from 'react'
import { type WireConfig, getWireConfig, saveWireConfig } from '../api/client'

type ShardStrategy = 'hash' | 'consistent' | 'list' | 'range' | 'date'

interface TableShardRow {
  table: string
  strategy: ShardStrategy
  column: string
  params: string
}

const STRATEGY_PLACEHOLDER: Record<ShardStrategy, string> = {
  hash: 'shard1,shard2,shard3',
  consistent: 'shard1,shard2,shard3',
  list: 'shard1=US,CA;shard2=UK,DE',
  range: 'shard1<100;shard2<1000;shard3',
  date: 'shard1<2024-07-01;shard2<2025-01-01;shard3',
}

const STRATEGY_HELP: Record<ShardStrategy, string> = {
  hash: 'Backends, comma-separated — the column’s value is hashed to pick one.',
  consistent: 'Backends, comma-separated — hashed on a consistent-hash ring (stable under backend list changes).',
  list: 'backend=value,value;backend=value — routes by an exact-match value list per backend.',
  range: 'backend<upperBound;backend<upperBound;lastBackend — numeric ranges, ascending, last one open-ended.',
  date: 'backend<yyyy-mm-dd;backend<yyyy-mm-dd;lastBackend — date ranges, ascending, last one open-ended.',
}

function emptyRow(): TableShardRow {
  return { table: '', strategy: 'hash', column: '', params: '' }
}

// table:strategy:column:params, entries |-delimited -- see RouterStage.fromConfig's own javadoc
// for exactly this grammar (params may itself contain ';', hence '|' between whole entries).
function parseTableShards(spec: string): TableShardRow[] {
  if (!spec.trim()) return []
  return spec.split('|').map((entry) => {
    const parts = entry.split(':')
    const [table = '', strategy = 'hash', column = '', ...rest] = parts
    return { table, strategy: (strategy as ShardStrategy) || 'hash', column, params: rest.join(':') }
  })
}

function serializeTableShards(rows: TableShardRow[]): string {
  return rows
    .filter((r) => r.table.trim())
    .map((r) => `${r.table.trim()}:${r.strategy}:${r.column.trim()}:${r.params.trim()}`)
    .join('|')
}

/**
 * Statement routing rules -- edits the `polywire_config.router*` fields RouterStage parses (see
 * RouterStage#reconfigure for the exact grammar). Raw spec editors for the older rule kinds, same
 * escape-hatch treatment as the SQL Firewall page's regex field: those formats are compact and
 * specific enough that reproducing Warp's own grammar directly is more honest than a leaky
 * friendly widget. Declarative table sharding gets a real structured editor instead (below) --
 * it's the mechanism meant to replace hand-written per-query schema-qualifier conventions with
 * one simple declaration per table, so the UI for it should be equally declarative, not another
 * raw string field.
 */
export default function RouterRules() {
  const [schemaRules, setSchemaRules] = useState('')
  const [predicateRules, setPredicateRules] = useState('')
  const [valueShardRules, setValueShardRules] = useState('')
  const [shardTables, setShardTables] = useState('')
  const [tableShardRows, setTableShardRows] = useState<TableShardRow[]>([])
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getWireConfig()
      .then((s: WireConfig) => {
        setSchemaRules(s.routerSchemaRules ?? '')
        setPredicateRules(s.routerPredicateRules ?? '')
        setValueShardRules(s.routerValueShardRules ?? '')
        setShardTables(s.routerShardTables ?? '')
        setTableShardRows(parseTableShards(s.routerTableShards ?? ''))
        setLoaded(true)
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  function updateRow(index: number, patch: Partial<TableShardRow>) {
    setTableShardRows((rows) => rows.map((r, i) => (i === index ? { ...r, ...patch } : r)))
  }

  function removeRow(index: number) {
    setTableShardRows((rows) => rows.filter((_, i) => i !== index))
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    setMessage(null)
    try {
      const saved = await saveWireConfig({
        routerSchemaRules: schemaRules,
        routerPredicateRules: predicateRules,
        routerValueShardRules: valueShardRules,
        routerShardTables: shardTables,
        routerTableShards: serializeTableShards(tableShardRows),
      })
      setMessage(`Saved — polywire_config version ${saved.version}.`)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ maxWidth: 720 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Router rules</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Decides which backend a statement goes to.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
        <form onSubmit={handleSave}>
          <h2 style={{ fontSize: 15, marginBottom: 4 }}>Declarative table sharding</h2>
          <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 12 }}>
            One row per horizontally-partitioned table. No schema-qualifier prefix needed in queries —
            the table's own bare name is matched directly. A query supplying the partition column's
            value routes straight to the one shard that owns it; a full-table aggregate or a{' '}
            <code>JOIN</code> of two declared tables falls back to scatter-gather / a real federated
            join across exactly that table's own shards.
          </p>
          {tableShardRows.length === 0 && (
            <div style={{ fontSize: 13, color: 'var(--muted)', marginBottom: 12 }}>No tables declared yet.</div>
          )}
          {tableShardRows.map((row, i) => (
            <div key={i} style={{
              display: 'grid', gridTemplateColumns: '1fr 130px 1fr 2fr auto', gap: 8,
              alignItems: 'start', marginBottom: 8, padding: 10,
              border: '1px solid var(--border, #333)', borderRadius: 6,
            }}>
              <label>
                <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 3 }}>Table</div>
                <input type="text" value={row.table} onChange={(e) => updateRow(i, { table: e.target.value })}
                  placeholder="orders" style={{ width: '100%', padding: '6px 8px', fontSize: 13, fontFamily: 'monospace' }} />
              </label>
              <label>
                <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 3 }}>Strategy</div>
                <select value={row.strategy} onChange={(e) => updateRow(i, { strategy: e.target.value as ShardStrategy })}
                  style={{ width: '100%', padding: '6px 8px', fontSize: 13 }}>
                  <option value="hash">hash</option>
                  <option value="consistent">consistent hash</option>
                  <option value="list">list</option>
                  <option value="range">range (numeric)</option>
                  <option value="date">date</option>
                </select>
              </label>
              <label>
                <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 3 }}>Partition column</div>
                <input type="text" value={row.column} onChange={(e) => updateRow(i, { column: e.target.value })}
                  placeholder="customer_id" style={{ width: '100%', padding: '6px 8px', fontSize: 13, fontFamily: 'monospace' }} />
              </label>
              <label>
                <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 3 }}>{STRATEGY_HELP[row.strategy]}</div>
                <input type="text" value={row.params} onChange={(e) => updateRow(i, { params: e.target.value })}
                  placeholder={STRATEGY_PLACEHOLDER[row.strategy]}
                  style={{ width: '100%', padding: '6px 8px', fontSize: 13, fontFamily: 'monospace' }} />
              </label>
              <button type="button" onClick={() => removeRow(i)} title="Remove"
                style={{ alignSelf: 'end', padding: '6px 10px', fontSize: 13 }}>✕</button>
            </div>
          ))}
          <button type="button" onClick={() => setTableShardRows((rows) => [...rows, emptyRow()])}
            style={{ marginBottom: 24, padding: '6px 12px', fontSize: 13 }}>
            + Add table
          </button>

          <h2 style={{ fontSize: 15, marginBottom: 4 }}>Advanced / legacy rules</h2>
          <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 16 }}>
            Raw spec editors — schema-qualifier-based sharding, predicate routing, and single-shard
            value routing. Still fully supported; declarative table sharding above covers the common
            case with less to type.
          </p>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Schema rules — comma-separated <code>schema:backend</code></div>
            <input type="text" value={schemaRules} onChange={(e) => setSchemaRules(e.target.value)}
              placeholder="reporting:reporting,analytics:analytics"
              style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }} />
          </label>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Predicate rules — comma-separated <code>priority:regex:backend</code></div>
            <input type="text" value={predicateRules} onChange={(e) => setPredicateRules(e.target.value)}
              placeholder="10:(?i)FROM\s+orders:reporting"
              style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }} />
          </label>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Value-shard rules — <code>|</code>-separated <code>bindIndex:modulus:table</code></div>
            <input type="text" value={valueShardRules} onChange={(e) => setValueShardRules(e.target.value)}
              placeholder="0:4:orders"
              style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }} />
          </label>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Shard tables — comma-separated schema list</div>
            <input type="text" value={shardTables} onChange={(e) => setShardTables(e.target.value)}
              placeholder="orders,order_items"
              style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }} />
          </label>
          <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
          {message && <span style={{ marginLeft: 12, color: 'var(--success, green)', fontSize: 13 }}>{message}</span>}
        </form>
      )}
    </div>
  )
}
