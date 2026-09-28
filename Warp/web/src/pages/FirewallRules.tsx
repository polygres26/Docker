import { Shield, Trash2 } from 'lucide-react'
import { useEffect, useState } from 'react'
import {
  type FirewallRule,
  createFirewallRule,
  deleteFirewallRule,
  draftFirewallRule,
  listFirewallRules,
  updateFirewallRule,
} from '../api/client'
import { DataTable, EmptyState, Loading, Notice, PageHeader, Section, StatusPill } from '../components/ui'

/** A drafted-but-never-saved rule: same fields as a real one, minus the `id`/`createdAt` a row
 * only gets once it's actually inserted. Distinguishing on `'id' in x` (rather than a separate
 * flag) is what lets `RuleForm` reuse its existing create/update branch for both "+ Add rule" and
 * a prefilled AI suggestion. */
type DraftRule = Omit<FirewallRule, 'id' | 'createdAt'>

/**
 * SQL Firewall rule management -- create/edit/delete against `warp_firewall_rules`, straight
 * against Warp's own admin API. Changes take effect on every running Warp process within
 * milliseconds (LISTEN/NOTIFY), no restart.
 */
export default function FirewallRules() {
  const [rules, setRules] = useState<FirewallRule[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [editing, setEditing] = useState<FirewallRule | 'new' | DraftRule | null>(null)
  const [suggesting, setSuggesting] = useState(false)

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
    <div style={{ maxWidth: 1100 }}>
      <PageHeader title="SQL Firewall" description="Rules are checked in priority order (lowest first); the first match wins." />

      {error && (
        <Notice tone="bad">{error}</Notice>
      )}

      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        <button className="primary" onClick={() => setEditing('new')}>+ Add rule</button>
        <button onClick={() => setSuggesting(true)}>✦ Suggest with AI</button>
      </div>

      {suggesting && (
        <AiSuggestPanel
          onCancel={() => setSuggesting(false)}
          onDrafted={(draft) => { setSuggesting(false); setEditing(draft) }}
        />
      )}

      {editing && (
        <RuleForm
          initial={editing === 'new' ? null : editing}
          onCancel={() => setEditing(null)}
          onSaved={() => { setEditing(null); reload() }}
        />
      )}

      {rules === null ? (
        <Loading />
      ) : rules.length === 0 ? (
        <Section>
          <EmptyState icon={<Shield size={18} aria-hidden="true" />} title="No rules yet">
            Everything is allowed by default.
          </EmptyState>
        </Section>
      ) : (
        <Section flush title="Rules" meta={`${rules.length} rule${rules.length === 1 ? '' : 's'} · lowest priority first`}>
          <DataTable caption="SQL firewall rules" minWidth={720}>
            <thead>
              <tr>
                <th>Priority</th>
                <th>Action</th>
                <th>Statement</th>
                <th>Table pattern</th>
                <th>Description</th>
                <th aria-label="Actions"></th>
              </tr>
            </thead>
            <tbody>
              {rules.map((r) => (
                <tr key={r.id} style={{ opacity: r.enabled ? 1 : 0.55 }}>
                  <td style={{ fontVariantNumeric: 'tabular-nums' }}>{r.priority}</td>
                  <td><StatusPill tone={r.action === 'deny' ? 'bad' : 'ok'}>{r.action === 'deny' ? 'Deny' : 'Allow'}</StatusPill>{!r.enabled && <span style={{ color: 'var(--sy-muted)' }}> · off</span>}</td>
                  <td>{r.statementType ?? '*'}</td>
                  <td className="mono">{r.tablePattern ?? '*'}</td>
                  <td>{r.description ?? ''}</td>
                  <td style={{ whiteSpace: 'nowrap', textAlign: 'right' }}>
                    <button onClick={() => setEditing(r)} style={{ marginRight: 8 }}>Edit</button>
                    <button onClick={() => handleDelete(r.id)} title="Delete rule" aria-label={`Delete rule ${r.id}`} style={{ padding: 6 }}>
                      <Trash2 size={14} />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        </Section>
      )}
    </div>
  )
}

function RuleForm({ initial, onCancel, onSaved }: {
  // A real rule (has `id`) is edited in place; `null` ("+ Add rule") and a `DraftRule` (a
  // prefilled AI suggestion, no `id` yet) both submit through createFirewallRule below.
  initial: FirewallRule | DraftRule | null
  onCancel: () => void
  onSaved: () => void
}) {
  const isExisting = initial !== null && 'id' in initial
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
      if (isExisting) {
        await updateFirewallRule((initial as FirewallRule).id, payload)
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
    <form onSubmit={handleSubmit} className="card">
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
      {error && <Notice tone="bad">{error}</Notice>}
    </form>
  )
}

/**
 * "Suggest with AI": takes a plain-English prompt, calls the draft-only
 * `POST /api/firewall-rules/draft` endpoint, and hands the (never-saved) result to `RuleForm` for
 * review/editing -- nothing is written here. Requires an LLM provider already configured via the
 * LLM Config page; a 503 from the backend surfaces that directly.
 */
function AiSuggestPanel({ onCancel, onDrafted }: {
  onCancel: () => void
  onDrafted: (draft: DraftRule) => void
}) {
  const [prompt, setPrompt] = useState('')
  const [drafting, setDrafting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleDraft(e: React.FormEvent) {
    e.preventDefault()
    if (!prompt.trim()) return
    setDrafting(true)
    setError(null)
    try {
      const { draft } = await draftFirewallRule(prompt)
      onDrafted({
        priority: draft.priority,
        action: draft.action,
        statementType: draft.statementType,
        tablePattern: draft.tablePattern,
        sqlPattern: draft.sqlPattern,
        enabled: draft.enabled,
        description: draft.description,
      })
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setDrafting(false)
    }
  }

  return (
    <form onSubmit={handleDraft} className="card">
      <div style={{ fontSize: 13, marginBottom: 8 }}>
        Describe the rule in plain English — e.g. "block any DELETE against orders without a
        WHERE clause". Nothing is created yet; you'll review and edit the proposed rule below
        before saving it.
      </div>
      <textarea value={prompt} onChange={(e) => setPrompt(e.target.value)} rows={2} autoFocus
        placeholder="Describe the rule you want…"
        style={{ width: '100%', padding: '6px 8px', marginBottom: 12, fontFamily: 'inherit' }} />
      <button type="submit" disabled={drafting || !prompt.trim()} style={{ marginRight: 8 }}>
        {drafting ? 'Drafting…' : 'Draft rule'}
      </button>
      <button type="button" onClick={onCancel}>Cancel</button>
      {error && <Notice tone="bad">{error}</Notice>}
    </form>
  )
}
