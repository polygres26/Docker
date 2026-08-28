import { useEffect, useState } from 'react'
import { type LlmProvider, getLlmConfig, saveLlmConfig } from '../api/client'

const OPENAI_DEFAULT_BASE_URL = 'https://api.openai.com/v1'

/**
 * Configures Polywire's SQL-dialect-translation LLM fallback. Polywire always tries a fast
 * deterministic AST-based rewrite first for each statement -- this only matters for the (usually
 * small) share of statements the rewriter can't handle on its own, where it falls back to an LLM
 * call. Setting the provider to "none" disables the fallback entirely: those statements just fail
 * the rewrite instead of going to a model.
 */
export default function LlmConfig() {
  const [provider, setProvider] = useState<LlmProvider>('none')
  const [apiKey, setApiKey] = useState('')
  const [baseUrl, setBaseUrl] = useState('')
  const [model, setModel] = useState('')
  const [apiKeySet, setApiKeySet] = useState(false)
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getLlmConfig()
      .then((c) => {
        setProvider(c.provider)
        setBaseUrl(c.baseUrl ?? (c.provider === 'openai' ? OPENAI_DEFAULT_BASE_URL : ''))
        setModel(c.model ?? '')
        setApiKeySet(c.apiKeySet)
        setLoaded(true)
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  function handleProviderChange(next: LlmProvider) {
    setProvider(next)
    if (next === 'openai') setBaseUrl(OPENAI_DEFAULT_BASE_URL)
    else if (next === 'none') setBaseUrl('')
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    setMessage(null)
    try {
      const saved = await saveLlmConfig({
        provider,
        ...(apiKey ? { apiKey } : {}),
        baseUrl: provider === 'none' ? null : baseUrl || null,
        model: provider === 'none' ? null : model || null,
      })
      setProvider(saved.provider)
      setBaseUrl(saved.baseUrl ?? '')
      setModel(saved.model ?? '')
      setApiKeySet(saved.apiKeySet)
      setApiKey('')
      setMessage('Saved.')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ maxWidth: 560 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>LLM configuration</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Polywire translates SQL between dialects with a fast, deterministic AST-based rewriter
        first. Only statements that rewriter can't handle fall back to an LLM call -- so this is a
        <strong> coverage setting for the long tail</strong>, not Polywire's primary translation
        path. Choosing "None" disables the fallback: unhandled statements fail the rewrite instead
        of being sent to a model.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
        <form onSubmit={handleSave}>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Provider</div>
            <select value={provider} onChange={(e) => handleProviderChange(e.target.value as LlmProvider)}
              style={{ width: '100%', padding: '8px 10px', fontSize: 14 }}>
              <option value="none">None (AST-rewrite only, no LLM fallback)</option>
              <option value="openai">OpenAI</option>
              <option value="custom">Custom (OpenAI-compatible)</option>
            </select>
          </label>

          {provider !== 'none' && (
            <>
              <label style={{ display: 'block', marginBottom: 16 }}>
                <div style={{ fontSize: 13, marginBottom: 4 }}>
                  API key {apiKeySet && <span style={{ color: 'var(--muted)' }}>(leave blank to keep the stored one)</span>}
                </div>
                <input
                  type="password"
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                  placeholder={apiKeySet ? '••••••••' : 'sk-…'}
                  style={{ width: '100%', padding: '8px 10px', fontSize: 14 }}
                  autoComplete="off"
                />
                <div style={{ fontSize: 11.5, color: 'var(--muted)', marginTop: 4 }}>
                  Write-only -- once saved, the key is never sent back to this page.
                </div>
              </label>

              <label style={{ display: 'block', marginBottom: 16 }}>
                <div style={{ fontSize: 13, marginBottom: 4 }}>Base URL</div>
                <input
                  type="text"
                  value={baseUrl}
                  onChange={(e) => setBaseUrl(e.target.value)}
                  placeholder={OPENAI_DEFAULT_BASE_URL}
                  disabled={provider === 'openai'}
                  style={{ width: '100%', padding: '8px 10px', fontSize: 14, fontFamily: 'monospace' }}
                />
                {provider === 'openai' && (
                  <div style={{ fontSize: 11.5, color: 'var(--muted)', marginTop: 4 }}>
                    Fixed for the OpenAI provider. Switch to "Custom" to point at another
                    OpenAI-compatible endpoint.
                  </div>
                )}
              </label>

              <label style={{ display: 'block', marginBottom: 16 }}>
                <div style={{ fontSize: 13, marginBottom: 4 }}>Model</div>
                <input
                  type="text"
                  value={model}
                  onChange={(e) => setModel(e.target.value)}
                  placeholder="gpt-4o-mini"
                  style={{ width: '100%', padding: '8px 10px', fontSize: 14, fontFamily: 'monospace' }}
                />
              </label>
            </>
          )}

          <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
          {message && <span style={{ marginLeft: 12, color: 'var(--success, green)', fontSize: 13 }}>{message}</span>}
        </form>
      )}
    </div>
  )
}
