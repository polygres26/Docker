import { Trash2 } from 'lucide-react'
import { useEffect, useState } from 'react'
import { type BackendInfo, type WireConfig, getWireConfig, listBackends, saveWireConfig } from '../api/client'

interface SetRow {
  name: string
  members: string
}

function emptyRow(): SetRow {
  return { name: '', members: '' }
}

// name=backend1,backend2,... entries, |-delimited -- see BackendRegistry#parseBackendSets's own
// javadoc for exactly this grammar (same delimiter convention WARP_TABLE_SHARDS uses).
function parseSets(spec: string): SetRow[] {
  if (!spec.trim()) return []
  return spec.split('|').map((entry) => {
    const eq = entry.indexOf('=')
    if (eq < 0) return { name: entry.trim(), members: '' }
    return { name: entry.slice(0, eq).trim(), members: entry.slice(eq + 1) }
  })
}

function serializeSets(rows: SetRow[]): string {
  return rows
    .filter((r) => r.name.trim() && r.members.trim())
    .map((r) => `${r.name.trim()}=${r.members.split(',').map((m) => m.trim()).filter(Boolean).join(',')}`)
    .join('|')
}

/**
 * Backend sets -- a named, reusable set of backends (mixing engines freely: a Postgres, an
 * Oracle, a MySQL, a SQL Server, and a MongoDB backend all in one set is exactly what this is
 * for) that can be referenced BY NAME in a Router rule's "backends" field instead of retyping
 * every backend each time -- see RouterStage#expandBackendSets and this page's own link out to
 * Router rules below. Edits `warp_config.backendSets`; a save appends a new warp_config
 * version, every running Warp process picks it up within milliseconds over LISTEN/NOTIFY.
 */
export default function BackendSets() {
  const [rows, setRows] = useState<SetRow[]>([])
  const [backends, setBackends] = useState<BackendInfo[] | null>(null)
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    Promise.all([getWireConfig(), listBackends()])
      .then(([config, backendList]: [WireConfig, BackendInfo[]]) => {
        setRows(parseSets(config.backendSets ?? ''))
        setBackends(backendList)
        setLoaded(true)
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  function updateRow(index: number, patch: Partial<SetRow>) {
    setRows((current) => current.map((r, i) => (i === index ? { ...r, ...patch } : r)))
  }

  function removeRow(index: number) {
    setRows((current) => current.filter((_, i) => i !== index))
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    setMessage(null)
    try {
      const saved = await saveWireConfig({ backendSets: serializeSets(rows) || null })
      setMessage(`Saved — warp_config version ${saved.version}.`)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ maxWidth: 760 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Backend sets</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Name a reusable set of backends — any mix of engines — then reference the set's name
        instead of every backend individually in a <a href="/router">Router rule</a>'s hash/
        consistent-hash sharding.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
        <form onSubmit={handleSave}>
          {backends && backends.length > 0 && (
            <div style={{ fontSize: 12.5, color: 'var(--muted)', marginBottom: 16 }}>
              Configured backends: {backends.map((b) => `${b.name}${b.dialect ? ` (${b.dialect})` : ''}`).join(', ')}
            </div>
          )}
          {rows.map((row, i) => (
            <div key={i} style={{
              display: 'grid', gridTemplateColumns: '160px 1fr 32px', gap: 8, marginBottom: 8, alignItems: 'start',
            }}>
              <input type="text" value={row.name} onChange={(e) => updateRow(i, { name: e.target.value })}
                placeholder="set name" style={{ padding: '6px 8px' }} />
              <input type="text" value={row.members} onChange={(e) => updateRow(i, { members: e.target.value })}
                placeholder="backend1,backend2,backend3" style={{ padding: '6px 8px', fontFamily: 'monospace' }} />
              <button type="button" onClick={() => removeRow(i)} title="Remove" style={{ padding: 4 }}>
                <Trash2 size={14} />
              </button>
            </div>
          ))}
          <button type="button" onClick={() => setRows((r) => [...r, emptyRow()])} style={{ marginBottom: 16 }}>
            + Add set
          </button>
          <div>
            <button type="submit" disabled={saving} style={{ marginRight: 8 }}>{saving ? 'Saving…' : 'Save'}</button>
            {message && <span style={{ color: 'var(--success, green)', fontSize: 13 }}>{message}</span>}
          </div>
        </form>
      )}
    </div>
  )
}
