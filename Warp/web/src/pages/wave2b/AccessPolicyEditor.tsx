import { useEffect, useState } from 'react'
import { type WireConfig, getWireConfig, saveWireConfig } from '../../api/client'
import { Notice, Loading } from '../../components/ui'

const PLACEHOLDER = `row_filters:
  - table_pattern: '^orders$'
    filter_column: tenant_id
    required_attribute: tenant
    bypass_roles: [admin]
column_grants:
  - table_pattern: '^employees$'
    columns: [ssn, salary]
    required_attribute: clearance
    allowed_values: [high]
    on_violation: mask`

/**
 * Row-filter/column-masking policy authoring -- edits `warp_config.accessPolicy`, the same YAML
 * `AccessPolicyYamlConfig.parse`/`AccessControlStage` read at runtime (mirrors Rollups.tsx's own
 * "edit a YAML field on WarpConfig" pattern). A row filter injects a `WHERE filter_column = ?`
 * clause into any matching query unless the caller's AccessContext attributes/roles bypass it
 * (fail-closed: a matching table with no bypass and no attribute value is rejected, not left
 * unfiltered); a column grant either masks a column to NULL or rejects the statement outright,
 * depending on `on_violation`. A save appends a new warp_config version; every running Warp
 * process applies it live within milliseconds over LISTEN/NOTIFY, no restart -- and, since this
 * stage runs before the query cache in the pipeline, a policy change is reflected in what gets
 * cached from the next statement on, not just what gets returned.
 */
export default function AccessPolicyEditor() {
  const [yaml, setYaml] = useState('')
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getWireConfig()
      .then((s: WireConfig) => {
        setYaml(s.accessPolicy ?? '')
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
      const saved = await saveWireConfig({ accessPolicy: yaml || null })
      setMessage(`Saved — warp_config version ${saved.version}.`)
    } catch (e) {
      // Malformed YAML, a missing required field, or an invalid table-pattern regex all come back
      // as a 400 from AccessPolicyYamlConfig.parse via the generic /api/config validation path --
      // surfaced here verbatim rather than silently discarded.
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ maxWidth: 1100 }}>
      {error && <Notice tone="bad">{error}</Notice>}

      {!loaded && !error ? (
        <Loading />
      ) : (
        <form onSubmit={handleSave} className="card">
          <div style={{ fontSize: 13, marginBottom: 12, color: 'var(--muted)' }}>
            Row filters inject a WHERE clause and column grants mask or deny a column, both based
            on the caller's AccessContext roles/attributes — enforced by AccessControlStage on
            every statement, before the query cache.
          </div>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Access policy (YAML)</div>
            <textarea value={yaml} onChange={(e) => setYaml(e.target.value)} rows={20}
              placeholder={PLACEHOLDER}
              style={{ width: '100%', padding: '8px 10px', fontFamily: 'monospace', fontSize: 13 }} />
          </label>
          <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
          {message && <span style={{ marginLeft: 12, color: 'var(--success)', fontSize: 13 }}>{message}</span>}
        </form>
      )}
    </div>
  )
}
