import { useEffect, useState } from 'react'
import { type WireConfig, draftRollupSuggestion, getWireConfig, saveWireConfig } from '../api/client'

/**
 * Rollup (pre-aggregation) definitions -- edits `warp_config.rollupDefinitionsYaml`, the same
 * YAML `RollupConfig.parse`/`RollupStage` read at runtime. Warp keeps each defined rollup's
 * materialized table fresh on its own schedule and rewrites matching client queries to read from
 * it automatically. A save appends a new warp_config version; every running Warp process picks it
 * up within milliseconds over LISTEN/NOTIFY, no restart.
 */
export default function Rollups() {
  const [yaml, setYaml] = useState('')
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [suggestion, setSuggestion] = useState<Awaited<ReturnType<typeof draftRollupSuggestion>> | null>(null)
  const [suggesting, setSuggesting] = useState(false)
  const [suggestError, setSuggestError] = useState<string | null>(null)

  useEffect(() => {
    getWireConfig()
      .then((s: WireConfig) => {
        setYaml(s.rollupDefinitionsYaml ?? '')
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
      const saved = await saveWireConfig({ rollupDefinitionsYaml: yaml || null })
      setMessage(`Saved — warp_config version ${saved.version}.`)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  async function handleSuggest() {
    setSuggesting(true)
    setSuggestError(null)
    try {
      setSuggestion(await draftRollupSuggestion())
    } catch (e) {
      setSuggestError(e instanceof Error ? e.message : String(e))
    } finally {
      setSuggesting(false)
    }
  }

  function applySuggestionToForm() {
    if (suggestion?.rollupDefinitionsYamlIfApplied !== undefined) {
      setYaml(suggestion.rollupDefinitionsYamlIfApplied)
    }
    setSuggestion(null)
  }

  return (
    <div style={{ maxWidth: 720 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Rollups</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Pre-aggregated summary tables, kept fresh on a schedule — matching client queries are
        rewritten to read from them automatically.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      <button type="button" onClick={handleSuggest} disabled={suggesting} style={{ marginBottom: 16 }}>
        {suggesting ? 'Drafting…' : '✦ Suggest a rollup with AI'}
      </button>
      {suggestError && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{suggestError}</div>
      )}
      {suggestion && (
        <div style={{ border: '1px solid var(--border, #ddd)', borderRadius: 8, padding: 16, marginBottom: 20 }}>
          {suggestion.draft ? (
            <>
              <div style={{ fontSize: 13, marginBottom: 12 }}>
                Proposed: <code>{suggestion.draft.name}</code> on <code>{suggestion.draft.sourceTable}</code>,
                grouped by {suggestion.draft.groupBy.join(', ')} — {suggestion.draft.aggregations.join(', ')},
                refreshed every {suggestion.draft.refreshIntervalMinutes}m.
              </div>
              <button type="button" onClick={applySuggestionToForm} style={{ marginRight: 8 }}>Fill into form</button>
              <button type="button" onClick={() => setSuggestion(null)}>Dismiss</button>
            </>
          ) : (
            <>
              <div style={{ fontSize: 13, marginBottom: 12 }}>{suggestion.note}</div>
              <button type="button" onClick={() => setSuggestion(null)}>Dismiss</button>
            </>
          )}
        </div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
        <form onSubmit={handleSave}>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Rollup definitions (YAML)</div>
            <textarea value={yaml} onChange={(e) => setYaml(e.target.value)} rows={16}
              placeholder={'- name: daily_order_totals\n  sourceTable: orders\n  groupBy: [customer_id]\n  aggregations: [\'SUM(amount) AS total\']\n  refreshIntervalMinutes: 15\n  maxStalenessMinutes: 30'}
              style={{ width: '100%', padding: '8px 10px', fontFamily: 'monospace', fontSize: 13 }} />
          </label>
          <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
          {message && <span style={{ marginLeft: 12, color: 'var(--success, green)', fontSize: 13 }}>{message}</span>}
        </form>
      )}
    </div>
  )
}
