import { Trash2 } from 'lucide-react'
import { useEffect, useState } from 'react'
import {
  type FirewallRule,
  createFirewallRule,
  deleteFirewallRule,
  listFirewallRules,
  updateFirewallRule,
} from '../api/client'

/**
 * SQL Firewall rule management -- create/edit/delete against `polywire_firewall_rules`, straight
 * against NexaGate's own admin API. Changes take effect on every running NexaGate process within
 * milliseconds (LISTEN/NOTIFY), no restart.
 */
export default function FirewallRules() {
  const [rules, setRules] = useState<FirewallRule[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [editing, setEditing] = useState<FirewallRule | 'new' | null>(null)

  function reload() {
    listFirewallRules().then(setRules).catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }

  useEffect(reload, [])

  async function handleDelete(id: number) {
    if (!confirm('Delete this rule?')) return
    try {
      await deleteFirewallRule(id)
      reload()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  return (
    <div style={{ maxWidth: 900 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>SQL Firewall</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Rules are checked in priority order (lowest first); the first match wins.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      <button onClick={() => setEditing('new')} style={{ marginBottom: 16 }}>+ Add rule</button>

      {editing && (
        <RuleForm
          initial={editing === 'new' ? null : editing}
          onCancel={() => setEditing(null)}
          onSaved={() => { setEditing(null); reload() }}
        />
      )}

      {rules === null ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : rules.length === 0 ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>No rules yet — everything is allowed by default.</div>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: 'left', borderBottom: '1px solid var(--border, #ddd)' }}>
              <th style={{ padding: '6px 8px' }}>Priority</th>
              <th style={{ padding: '6px 8px' }}>Action</th>
              <th style={{ padding: '6px 8px' }}>Statement</th>
              <th style={{ padding: '6px 8px' }}>Table pattern</th>
              <th style={{ padding: '6px 8px' }}>Description</th>
              <th style={{ padding: '6px 8px' }}></th>
            </tr>
          </thead>
          <tbody>
            {rules.map((r) => (
              <tr key={r.id} style={{ borderBottom: '1px solid var(--border, #eee)', opacity: r.enabled ? 1 : 0.5 }}>
                <td style={{ padding: '6px 8px' }}>{r.priority}</td>
                <td style={{ padding: '6px 8px', textTransform: 'uppercase', color: r.action === 'deny' ? 'var(--error, crimson)' : 'inherit' }}>
                  {r.action}
                </td>
                <td style={{ padding: '6px 8px' }}>{r.statementType ?? '*'}</td>
                <td style={{ padding: '6px 8px', fontFamily: 'monospace' }}>{r.tablePattern ?? '*'}</td>
                <td style={{ padding: '6px 8px' }}>{r.description ?? ''}</td>
                <td style={{ padding: '6px 8px', whiteSpace: 'nowrap' }}>
                  <button onClick={() => setEditing(r)} style={{ marginRight: 8 }}>Edit</button>
                  <button onClick={() => handleDelete(r.id)} title="Delete" style={{ padding: 4 }}>
                    <Trash2 size={14} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function RuleForm({ initial, onCancel, onSaved }: {
  initial: FirewallRule | null
  onCancel: () => void
  onSaved: () => void
}) {
  const [priority, setPriority] = useState(initial?.priority ?? 100)
  const [action, setAction] = useState<'allow' | 'deny'>(initial?.action ?? 'deny')
  const [statementType, setStatementType] = useState(initial?.statementType ?? '')
  const [tablePattern, setTablePattern] = useState(initial?.tablePattern ?? '')
  const [sqlPattern, setSqlPattern] = useState(initial?.sqlPattern ?? '')
  const [enabled, setEnabled] = useState(initial?.enabled ?? true)
  const [description, setDescription] = useState(initial?.description ?? '')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    const payload = { priority, action, statementType, tablePattern, sqlPattern, enabled, description }
    try {
      if (initial) {
        await updateFirewallRule(initial.id, payload)
      } else {
        await createFirewallRule(payload)
      }
      onSaved()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} style={{
      border: '1px solid var(--border, #ddd)', borderRadius: 8, padding: 16, marginBottom: 20,
    }}>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 12 }}>
        <label>
          <div style={{ fontSize: 13, marginBottom: 4 }}>Priority (lower runs first)</div>
          <input type="number" value={priority} onChange={(e) => setPriority(Number(e.target.value))}
            style={{ width: '100%', padding: '6px 8px' }} />
        </label>
        <label>
          <div style={{ fontSize: 13, marginBottom: 4 }}>Action</div>
          <select value={action} onChange={(e) => setAction(e.target.value as 'allow' | 'deny')}
            style={{ width: '100%', padding: '6px 8px' }}>
            <option value="deny">Deny</option>
            <option value="allow">Allow</option>
          </select>
        </label>
        <label>
          <div style={{ fontSize: 13, marginBottom: 4 }}>Statement type (blank = any)</div>
          <input type="text" value={statementType} onChange={(e) => setStatementType(e.target.value)}
            placeholder="SELECT / INSERT / UPDATE / DELETE / DDL" style={{ width: '100%', padding: '6px 8px' }} />
        </label>
        <label>
          <div style={{ fontSize: 13, marginBottom: 4 }}>Table pattern (glob, blank = any)</div>
          <input type="text" value={tablePattern} onChange={(e) => setTablePattern(e.target.value)}
            placeholder="*orders*" style={{ width: '100%', padding: '6px 8px' }} />
        </label>
        <label style={{ gridColumn: '1 / -1' }}>
          <div style={{ fontSize: 13, marginBottom: 4 }}>Raw SQL regex (optional escape hatch)</div>
          <input type="text" value={sqlPattern} onChange={(e) => setSqlPattern(e.target.value)}
            placeholder="(?i)DROP\s+TABLE" style={{ width: '100%', padding: '6px 8px', fontFamily: 'monospace' }} />
        </label>
        <label style={{ gridColumn: '1 / -1' }}>
          <div style={{ fontSize: 13, marginBottom: 4 }}>Description</div>
          <input type="text" value={description} onChange={(e) => setDescription(e.target.value)}
            style={{ width: '100%', padding: '6px 8px' }} />
        </label>
      </div>
      <label style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 12, fontSize: 13 }}>
        <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
        Enabled
      </label>
      <button type="submit" disabled={saving} style={{ marginRight: 8 }}>{saving ? 'Saving…' : 'Save'}</button>
      <button type="button" onClick={onCancel}>Cancel</button>
      {error && <div style={{ marginTop: 12, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>}
    </form>
  )
}
