import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { type WireConfig, getWireConfig, saveWireConfig } from '../api/client'

/**
 * Statement routing rules -- edits the four `polywire_config.router*` fields RouterStage parses
 * (see RouterStage#reconfigure for the exact grammar). Raw spec editors, same escape-hatch
 * treatment as the SQL Firewall page's regex field: these formats are compact and specific enough
 * that reproducing PolyWire's own grammar directly is more honest than a leaky friendly widget.
 */
export default function WireRouterRules() {
  const [schemaRules, setSchemaRules] = useState('')
  const [predicateRules, setPredicateRules] = useState('')
  const [valueShardRules, setValueShardRules] = useState('')
  const [shardTables, setShardTables] = useState('')
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
        setLoaded(true)
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [])

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
        Decides which backend a statement goes to. Not configured yet? Set the connection on the{' '}
        <Link to="/wire-settings">Wire connection</Link> page first.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
        <form onSubmit={handleSave}>
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
