import { useEffect, useState } from 'react'
import { type WireConfig, draftQosSuggestion, getWireConfig, saveWireConfig } from '../api/client'

/**
 * QoS / rate limiting -- edits the five `warp_config.qos*` fields QosControlStage.fromConfig
 * parses. Applies per session-class token buckets in front of every backend pool.
 */
export default function Qos() {
  const [ratePerSec, setRatePerSec] = useState('')
  const [burst, setBurst] = useState('')
  const [maxWaitMs, setMaxWaitMs] = useState('')
  const [classLimits, setClassLimits] = useState('')
  const [poolWaitThreshold, setPoolWaitThreshold] = useState('')
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [suggestion, setSuggestion] = useState<Awaited<ReturnType<typeof draftQosSuggestion>> | null>(null)
  const [suggesting, setSuggesting] = useState(false)
  const [suggestError, setSuggestError] = useState<string | null>(null)

  useEffect(() => {
    getWireConfig()
      .then((s: WireConfig) => {
        setRatePerSec(s.qosRatePerSec ?? '')
        setBurst(s.qosBurst ?? '')
        setMaxWaitMs(s.qosMaxWaitMs ?? '')
        setClassLimits(s.qosClassLimits ?? '')
        setPoolWaitThreshold(s.qosPoolWaitThreshold ?? '')
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
        qosRatePerSec: ratePerSec,
        qosBurst: burst,
        qosMaxWaitMs: maxWaitMs || null,
        qosClassLimits: classLimits || null,
        qosPoolWaitThreshold: poolWaitThreshold || null,
      })
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
      setSuggestion(await draftQosSuggestion())
    } catch (e) {
      setSuggestError(e instanceof Error ? e.message : String(e))
    } finally {
      setSuggesting(false)
    }
  }

  function applySuggestionToForm() {
    if (!suggestion) return
    if (suggestion.qosClassLimitsIfApplied !== undefined) {
      setClassLimits(suggestion.qosClassLimitsIfApplied)
    } else {
      if (suggestion.qosRatePerSecIfApplied !== undefined) setRatePerSec(suggestion.qosRatePerSecIfApplied)
      if (suggestion.qosBurstIfApplied !== undefined) setBurst(suggestion.qosBurstIfApplied)
      if (suggestion.qosMaxWaitMsIfApplied !== undefined) setMaxWaitMs(suggestion.qosMaxWaitMsIfApplied)
    }
    setSuggestion(null)
  }

  return (
    <div style={{ maxWidth: 560 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>QoS / rate limiting</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Token-bucket rate limiting applied before a statement reaches a backend pool.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      <button type="button" onClick={handleSuggest} disabled={suggesting} style={{ marginBottom: 16 }}>
        {suggesting ? 'Drafting…' : '✦ Suggest with AI'}
      </button>
      {suggestError && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{suggestError}</div>
      )}
      {suggestion && (
        <div style={{ border: '1px solid var(--border, #ddd)', borderRadius: 8, padding: 16, marginBottom: 20 }}>
          <div style={{ fontSize: 13, marginBottom: 8 }}>
            Proposed change to <b>{suggestion.draft.target}</b>: rate {suggestion.draft.ratePerSecond}/s,
            burst {suggestion.draft.burstCapacity}, max wait {suggestion.draft.maxWaitMillis}ms.
          </div>
          {suggestion.draft.rationale && (
            <div style={{ fontSize: 13, color: 'var(--muted)', marginBottom: 12 }}>{suggestion.draft.rationale}</div>
          )}
          <button type="button" onClick={applySuggestionToForm} style={{ marginRight: 8 }}>Fill into form</button>
          <button type="button" onClick={() => setSuggestion(null)}>Dismiss</button>
          <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 8 }}>{suggestion.note}</div>
        </div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
        <form onSubmit={handleSave}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
            <label>
              <div style={{ fontSize: 13, marginBottom: 4 }}>Rate per second</div>
              <input type="text" value={ratePerSec} onChange={(e) => setRatePerSec(e.target.value)}
                placeholder="5" style={{ width: '100%', padding: '6px 8px' }} />
            </label>
            <label>
              <div style={{ fontSize: 13, marginBottom: 4 }}>Burst</div>
              <input type="text" value={burst} onChange={(e) => setBurst(e.target.value)}
                placeholder="5" style={{ width: '100%', padding: '6px 8px' }} />
            </label>
            <label>
              <div style={{ fontSize: 13, marginBottom: 4 }}>Max wait (ms, blank = default)</div>
              <input type="text" value={maxWaitMs} onChange={(e) => setMaxWaitMs(e.target.value)}
                placeholder="2000" style={{ width: '100%', padding: '6px 8px' }} />
            </label>
            <label>
              <div style={{ fontSize: 13, marginBottom: 4 }}>Pool-wait threshold (blank = default)</div>
              <input type="text" value={poolWaitThreshold} onChange={(e) => setPoolWaitThreshold(e.target.value)}
                placeholder="0.8" style={{ width: '100%', padding: '6px 8px' }} />
            </label>
          </div>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Per-class limits (comma-separated <code>class:ratePerSec</code>)</div>
            <input type="text" value={classLimits} onChange={(e) => setClassLimits(e.target.value)}
              placeholder="batch:1,interactive:10"
              style={{ width: '100%', padding: '8px 10px', fontFamily: 'monospace' }} />
          </label>
          <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
          {message && <span style={{ marginLeft: 12, color: 'var(--success, green)', fontSize: 13 }}>{message}</span>}
        </form>
      )}
    </div>
  )
}
