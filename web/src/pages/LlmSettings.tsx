import { useEffect, useState } from 'react'
import {
  type LlmProviderType,
  type LlmRole,
  getLlmSettings,
  saveLlmSettings,
} from '../api/client'

export default function LlmSettings() {
  return (
    <div style={{ maxWidth: 640 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>LLM configuration</h1>
      <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 0, marginBottom: 24 }}>
        Polygres Advisor's LLM-backed features (PL/SQL summarization, workload classification) use
        the <strong>Primary</strong> model below -- <strong>Local</strong> by default, so it works
        out of the box with no API key and no data leaving this machine. <strong>Judge</strong> is
        optional: a second, independently-configured model that reviews Primary's PL/SQL summaries
        for accuracy before you see them -- useful because a genuinely different model catches more
        real mistakes than the same model checking its own work. Judge only reviews summarization,
        not the high-volume workload classification, to keep cost proportional to what's actually
        at stake.
      </p>

      <RoleCard
        role="primary"
        title="Primary"
        description="Does the actual work: PL/SQL summarization and workload classification."
        showEnabledToggle={false}
      />
      <div style={{ height: 20 }} />
      <RoleCard
        role="judge"
        title="Judge (optional)"
        description="Reviews Primary's PL/SQL summaries for completeness and accuracy. Point it at a different model or provider than Primary for the best results."
        showEnabledToggle={true}
      />
    </div>
  )
}

function RoleCard({
  role, title, description, showEnabledToggle,
}: { role: LlmRole; title: string; description: string; showEnabledToggle: boolean }) {
  const [providerType, setProviderType] = useState<LlmProviderType>('local')
  const [apiKey, setApiKey] = useState('')
  const [baseUrl, setBaseUrl] = useState('')
  const [modelPath, setModelPath] = useState('')
  const [model, setModel] = useState('')
  const [enabled, setEnabled] = useState(role === 'primary')
  const [hasStoredKey, setHasStoredKey] = useState(false)
  const [updatedAt, setUpdatedAt] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [status, setStatus] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getLlmSettings(role).then((s) => {
      setProviderType(s.providerType.toLowerCase() as LlmProviderType)
      setBaseUrl(s.baseUrl ?? '')
      setModelPath(s.modelPath ?? '')
      setModel(s.model ?? '')
      setEnabled(s.enabled)
      setUpdatedAt(s.updatedAt)
      setHasStoredKey(s.providerType === 'EXTERNAL' && !!s.updatedAt)
    }).catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [role])

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true); setError(null); setStatus(null)
    try {
      await saveLlmSettings(role, { providerType, apiKey, baseUrl, modelPath, model, enabled })
      setStatus('Saved.')
      setApiKey('')
      if (providerType === 'external' && apiKey) setHasStoredKey(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  return (
    <form className="panel" onSubmit={handleSave}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
        <h3 style={{ margin: 0 }}>{title}</h3>
        {showEnabledToggle && (
          <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, color: 'var(--muted)' }}>
            <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
            Enabled
          </label>
        )}
      </div>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 4 }}>{description}</p>

      <div style={{ display: 'flex', gap: 16, marginBottom: 14, flexWrap: 'wrap' }}>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 14 }}>
          <input type="radio" checked={providerType === 'local'} onChange={() => setProviderType('local')} />
          Local (llama.cpp)
        </label>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 14 }}>
          <input type="radio" checked={providerType === 'builtin'} onChange={() => setProviderType('builtin')} />
          Built-in (Claude)
        </label>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 14 }}>
          <input type="radio" checked={providerType === 'external'} onChange={() => setProviderType('external')} />
          External (OpenAI-compatible)
        </label>
      </div>

      {providerType === 'local' && (
        <div className="field">
          <label htmlFor={`${role}-modelPath`}>Model file path (.gguf)</label>
          <input
            id={`${role}-modelPath`}
            value={modelPath}
            onChange={(e) => setModelPath(e.target.value)}
            placeholder="/path/to/model.gguf"
          />
          <p style={{ color: 'var(--muted)', fontSize: 12, marginTop: 4, marginBottom: 0 }}>
            No API key, no network call leaves this machine. Runs via a locally-managed llama-server
            sidecar -- needs llama-server installed (on PATH, or set POLYGRES_LLM_LOCAL_SERVER_PATH)
            and a .gguf model file at the path above.
          </p>
        </div>
      )}

      {providerType === 'external' && (
        <>
          <div className="field">
            <label htmlFor={`${role}-apiKey`}>API key {hasStoredKey && <span style={{ color: 'var(--muted)' }}>(configured -- leave blank to keep it)</span>}</label>
            <input
              id={`${role}-apiKey`}
              type="password"
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              placeholder={hasStoredKey ? '••••••••••••' : 'sk-...'}
            />
          </div>
          <div className="field">
            <label htmlFor={`${role}-baseUrl`}>Base URL</label>
            <input
              id={`${role}-baseUrl`}
              value={baseUrl}
              onChange={(e) => setBaseUrl(e.target.value)}
              placeholder="https://api.openai.com/v1"
            />
          </div>
        </>
      )}

      {providerType !== 'local' && (
        <div className="field">
          <label htmlFor={`${role}-model`}>Model</label>
          <input
            id={`${role}-model`}
            value={model}
            onChange={(e) => setModel(e.target.value)}
            placeholder={providerType === 'builtin' ? 'e.g. claude-sonnet-5' : 'e.g. gpt-4.1'}
          />
        </div>
      )}

      {error && <p style={{ color: 'var(--hard)', fontSize: 13 }}>{error}</p>}
      {status && <p style={{ color: 'var(--accent)', fontSize: 13 }}>{status}</p>}
      {updatedAt && <p style={{ color: 'var(--muted)', fontSize: 12 }}>Last updated {new Date(updatedAt).toLocaleString()}</p>}

      <button className="primary" type="submit" disabled={saving}>
        {saving ? 'Saving…' : 'Save'}
      </button>
    </form>
  )
}
