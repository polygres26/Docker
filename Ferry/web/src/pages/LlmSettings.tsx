import { useEffect, useState } from 'react'
import {
  type LlmProviderType,
  type LlmRole,
  type LocalModelPreset,
  getLlmSettings,
  getLocalModelPresets,
  saveLlmSettings,
} from '../api/client'
import { Button, Check, Field, Input, Notice, PageHeader, Section, Stack } from '../ui'
import styles from './LlmSettings.module.css'

export default function LlmSettings() {
  const [presets, setPresets] = useState<{ qwen: LocalModelPreset; gemma: LocalModelPreset } | null>(null)

  useEffect(() => { getLocalModelPresets().then(setPresets).catch(() => {}) }, [])

  return (
    <>
      {/* Not migration-only -- this same Primary/Judge model config also drives Warp's SQL
          translation and any other feature that calls the LLM, which is why it's its own
          "Configure" sidebar entry rather than a tab under Assessments. */}
      <PageHeader
        title="LLM settings"
        subtitle="Primary and Judge can each independently use the built-in local model (Qwen or Gemma) or an OpenAI API key."
      />
      <div className={styles.narrow}>
        <Stack>
          <RoleCard
            role="primary"
            title="Primary"
            description="Does the work: PL/SQL summarization and workload classification."
            showEnabledToggle={false}
            presets={presets}
          />
          <RoleCard
            role="judge"
            title="Judge (optional)"
            description="Second opinion on Primary's summaries -- use a different model than Primary for the best results."
            showEnabledToggle={true}
            presets={presets}
          />
        </Stack>
      </div>
    </>
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

  const idp = `llm-${role}`
  return (
    <Section
      title={title}
      meta={description}
      actions={showEnabledToggle && <Check checked={enabled} onChange={(e) => setEnabled(e.target.checked)}>Enabled</Check>}
    >
      <form onSubmit={handleSave}>
        <fieldset className={styles.provider}>
          <legend className="sr-only">Provider for {title}</legend>
          <label className={styles.radio}>
            <input type="radio" name={`${idp}-provider`} checked={providerType === 'local'} onChange={() => setProviderType('local')} />
            Local
          </label>
          <label className={styles.radio}>
            <input type="radio" name={`${idp}-provider`} checked={providerType === 'external'} onChange={() => setProviderType('external')} />
            OpenAI API
          </label>
        </fieldset>

        {providerType === 'local' && presets && (
          <Field
            label="Model"
            /* One line on what "local" means -- not the full sidecar-process/PATH explanation, that's implementation detail, not decision-relevant. */
            hint="Runs on this machine -- no API key, nothing sent over the network."
          >
            <div className={styles.presets} role="group" aria-label="Local model">
              {[presets.qwen, presets.gemma].map((preset) => (
                <button
                  key={preset.label}
                  type="button"
                  className={`${styles.preset} ${modelPath === preset.modelPath ? styles.presetOn : ''}`}
                  aria-pressed={modelPath === preset.modelPath}
                  onClick={() => chooseLocalModel(preset)}
                >
                  {preset.label}
                </button>
              ))}
            </div>
          </Field>
        )}

        {providerType === 'external' && (
          <>
            <Field label={<>API key {hasStoredKey && <span className={styles.subtle}>(configured -- leave blank to keep it)</span>}</>} htmlFor={`${role}-apiKey`}>
              <Input
                id={`${role}-apiKey`}
                type="password"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder={hasStoredKey ? '••••••••••••' : 'sk-...'}
              />
            </Field>
            <Field label={<>Base URL <span className={styles.subtle}>(change only for Azure OpenAI or a compatible endpoint)</span></>} htmlFor={`${role}-baseUrl`}>
              <Input
                id={`${role}-baseUrl`}
                value={baseUrl}
                onChange={(e) => setBaseUrl(e.target.value)}
                placeholder="https://api.openai.com/v1"
              />
            </Field>
            <Field label="Model" htmlFor={`${role}-model`}>
              <Input
                id={`${role}-model`}
                value={model}
                onChange={(e) => setModel(e.target.value)}
                placeholder="e.g. gpt-4.1"
              />
            </Field>
          </>
        )}

        {error && <Notice tone="error">{error}</Notice>}
        {status && <Notice tone="ok">{status}</Notice>}

        <div className={styles.foot}>
          <Button variant="primary" type="submit" disabled={saving}>
            {saving ? 'Saving…' : 'Save'}
          </Button>
          {updatedAt && <span className={styles.subtle}>Last updated {new Date(updatedAt).toLocaleString()}</span>}
        </div>
      </form>
    </Section>
  )
}
