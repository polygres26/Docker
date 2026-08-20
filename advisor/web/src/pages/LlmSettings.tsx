import { useEffect, useState } from 'react'
import {
  type LlmProviderType,
  type LlmRole,
  type LocalModelPreset,
  getLlmSettings,
  getLocalModelPresets,
  saveLlmSettings,
} from '../api/client'

export default function LlmSettings() {
  const [presets, setPresets] = useState<{ qwen: LocalModelPreset; gemma: LocalModelPreset } | null>(null)

  useEffect(() => { getLocalModelPresets().then(setPresets).catch(() => {}) }, [])

  return (
    <div style={{ maxWidth: 640 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>LLM configuration</h1>
      {/* Not migration-only -- this same Primary/Judge model config also drives PolyWire's SQL
          translation and any other feature that calls the LLM, which is why it's a standalone
          "Shared" sidebar entry rather than a tab under Migration. */}
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Primary and Judge can each independently use the built-in local model (Qwen or Gemma) or an OpenAI API key.
      </p>

      <RoleCard
        role="primary"
        title="Primary"
        description="Does the work: PL/SQL summarization and workload classification."
        showEnabledToggle={false}
        presets={presets}
      />
      <div style={{ height: 20 }} />
      <RoleCard
        role="judge"
        title="Judge (optional)"
        description="Second opinion on Primary's summaries -- use a different model than Primary for the best results."
        showEnabledToggle={true}
        presets={presets}
      />
    </div>
  )
}

function RoleCard({
  role, title, description, showEnabledToggle, presets,
}: {
  role: LlmRole; title: string; description: string; showEnabledToggle: boolean
  presets: { qwen: LocalModelPreset; gemma: LocalModelPreset } | null
}) {
  const [providerType, setProviderType] = useState<LlmProviderType>('local')
  const [apiKey, setApiKey] = useState('')
  const [baseUrl, setBaseUrl] = useState('https://api.openai.com/v1')
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
      // BUILTIN (server-side Claude) is a legacy provider type -- no longer offered on this page,
      // but a role saved with it before this change still needs somewhere to land; local is the
      // safer default now that Claude isn't a selectable choice here.
      const type = s.providerType.toLowerCase() as LlmProviderType
      setProviderType(type === 'builtin' ? 'local' : type)
      setBaseUrl(s.baseUrl || 'https://api.openai.com/v1')
      setModelPath(s.modelPath ?? '')
      setModel(s.model ?? '')
      setEnabled(s.enabled)
      setUpdatedAt(s.updatedAt)
      setHasStoredKey(s.providerType === 'EXTERNAL' && !!s.updatedAt)
    }).catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [role])

  function chooseLocalModel(preset: LocalModelPreset) {
    setModelPath(preset.modelPath)
    setModel(preset.label)
  }

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
          Local
        </label>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 14 }}>
          <input type="radio" checked={providerType === 'external'} onChange={() => setProviderType('external')} />
          OpenAI API
        </label>
      </div>

      {providerType === 'local' && presets && (
        <div className="field">
          <label>Model</label>
          <div style={{ display: 'flex', gap: 10 }}>
            {[presets.qwen, presets.gemma].map((preset) => (
              <button
                key={preset.label}
                type="button"
                onClick={() => chooseLocalModel(preset)}
                style={{
                  flex: 1, padding: '10px 12px', borderRadius: 8, cursor: 'pointer',
                  border: modelPath === preset.modelPath ? '2px solid var(--accent)' : '1px solid var(--border)',
                  background: modelPath === preset.modelPath ? 'var(--accent-soft)' : 'var(--bg)',
                  color: 'var(--text)', fontSize: 14, fontWeight: modelPath === preset.modelPath ? 600 : 400,
                  textAlign: 'left',
                }}
              >
                {preset.label}
              </button>
            ))}
          </div>
          {/* One line on what "local" means -- not the full sidecar-process/PATH explanation, that's implementation detail, not decision-relevant. */}
          <p style={{ color: 'var(--muted)', fontSize: 12, marginTop: 6, marginBottom: 0 }}>
            Runs on this machine -- no API key, nothing sent over the network.
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
            <label htmlFor={`${role}-baseUrl`}>Base URL <span style={{ color: 'var(--muted)', fontWeight: 400 }}>(change only for Azure OpenAI or a compatible endpoint)</span></label>
            <input
              id={`${role}-baseUrl`}
              value={baseUrl}
              onChange={(e) => setBaseUrl(e.target.value)}
              placeholder="https://api.openai.com/v1"
            />
          </div>
          <div className="field">
            <label htmlFor={`${role}-model`}>Model</label>
            <input
              id={`${role}-model`}
              value={model}
              onChange={(e) => setModel(e.target.value)}
              placeholder="e.g. gpt-4.1"
            />
          </div>
        </>
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
