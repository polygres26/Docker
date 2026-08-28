import { useEffect, useState } from 'react'
import {
  type BackendInfo, type BackendTestResult, type WireConfig,
  getWireConfig, listBackends, saveWireConfig, testBackendConnection, testConfiguredBackend,
} from '../api/client'
import CredentialField from '../components/CredentialField'

function TestResultBadge({ result }: { result: BackendTestResult }) {
  return (
    <div style={{
      marginTop: 8, padding: '8px 10px', borderRadius: 6, fontSize: 12.5,
      background: result.ok ? 'var(--accent-soft)' : 'var(--hard-soft, #fbeae8)',
      color: result.ok ? 'var(--accent-strong)' : 'var(--hard, crimson)',
    }}>
      {result.ok ? '✓ Connected' : '✗ Failed'} in {result.tookMs}ms
      {result.ok && result.serverVersion && <> — {result.serverVersion.split(',')[0]}</>}
      {!result.ok && <> — {result.message}</>}
    </div>
  )
}

/**
 * A quick "does this actually connect" probe before a backend spec line gets pasted into config
 * (or committed at all) -- built after onboarding a real Supabase project surfaced exactly the
 * kind of mistake this exists to catch fast: a plausible-looking host that turned out to be
 * unreachable, found out only after saving and trying to browse it. See
 * com.nexagres.wire.core.BackendConnectivityTest for the server side.
 */
function ConnectionTester() {
  const [jdbcUrl, setJdbcUrl] = useState('jdbc:postgresql://host:5432/postgres?sslmode=require')
  const [user, setUser] = useState('')
  const [password, setPassword] = useState('')
  const [testing, setTesting] = useState(false)
  const [result, setResult] = useState<BackendTestResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function handleTest() {
    setTesting(true)
    setError(null)
    setResult(null)
    try {
      setResult(await testBackendConnection({ jdbcUrl, user, password }))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setTesting(false)
    }
  }

  return (
    <div style={{ border: '1px solid var(--border)', borderRadius: 8, padding: 16, marginBottom: 24 }}>
      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 10 }}>Test a connection before adding it</div>
      <label style={{ display: 'block', marginBottom: 10 }}>
        <div style={{ fontSize: 12.5, marginBottom: 4 }}>JDBC URL</div>
        <input value={jdbcUrl} onChange={(e) => setJdbcUrl(e.target.value)}
          style={{ width: '100%', padding: '7px 9px', fontSize: 13, fontFamily: 'monospace' }} />
      </label>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 10 }}>
        <label>
          <div style={{ fontSize: 12.5, marginBottom: 4 }}>User</div>
          <input value={user} onChange={(e) => setUser(e.target.value)}
            style={{ width: '100%', padding: '7px 9px', fontSize: 13 }} />
        </label>
        <div>
          <div style={{ fontSize: 12.5, marginBottom: 4 }}>Credential</div>
          <CredentialField value={password} onChange={setPassword} />
        </div>
      </div>
      <button type="button" onClick={handleTest} disabled={testing || !jdbcUrl.trim()}>
        {testing ? 'Testing…' : 'Test connection'}
      </button>
      {error && <div style={{ marginTop: 8, color: 'var(--error, crimson)', fontSize: 12.5 }}>{error}</div>}
      {result && <TestResultBadge result={result} />}
    </div>
  )
}

function ConfiguredBackendRow({ backend }: { backend: BackendInfo }) {
  const [testing, setTesting] = useState(false)
  const [result, setResult] = useState<BackendTestResult | null>(null)

  async function handleTest() {
    setTesting(true)
    setResult(null)
    try {
      setResult(await testConfiguredBackend(backend.name))
    } catch (e) {
      setResult({ ok: false, message: e instanceof Error ? e.message : String(e), tookMs: 0, serverVersion: null })
    } finally {
      setTesting(false)
    }
  }

  return (
    <div style={{ padding: '10px 0', borderBottom: '1px solid var(--border)' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        <span style={{ fontFamily: 'monospace', fontSize: 13, fontWeight: 600 }}>{backend.name}</span>
        <span style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--muted)', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {backend.jdbcUrl}
        </span>
        <button type="button" onClick={handleTest} disabled={testing} style={{ flexShrink: 0 }}>
          {testing ? 'Testing…' : 'Test'}
        </button>
      </div>
      {result && <TestResultBadge result={result} />}
    </div>
  )
}

/**
 * Multi-backend routing targets -- edits `polywire_config.backends` (name=jdbcUrl|user|password,
 * one per line here, `;`-joined on the wire) and `.shardBackends` (the ordered shard group, one
 * name per line here, `,`-joined on the wire). See BackendRegistry.fromConfig for the exact
 * grammar and the POLYWIRE_TRUSTED_BACKEND_HOSTS allowlist check every entry has to clear.
 */
export default function Backends() {
  const [backends, setBackends] = useState('')
  const [shardBackends, setShardBackends] = useState('')
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [configured, setConfigured] = useState<BackendInfo[] | null>(null)

  function apply(s: WireConfig) {
    setBackends((s.backends ?? '').split(';').map((r) => r.trim()).filter(Boolean).join('\n'))
    setShardBackends((s.shardBackends ?? '').split(',').map((r) => r.trim()).filter(Boolean).join('\n'))
  }

  function reloadConfigured() {
    listBackends().then(setConfigured).catch(() => setConfigured(null))
  }

  useEffect(() => {
    getWireConfig()
      .then((s) => { apply(s); setLoaded(true) })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
    reloadConfigured()
  }, [])

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    setMessage(null)
    try {
      const saved = await saveWireConfig({
        backends: backends.split('\n').map((r) => r.trim()).filter(Boolean).join(';'),
        shardBackends: shardBackends.split('\n').map((r) => r.trim()).filter(Boolean).join(','),
      })
      setMessage(`Saved — polywire_config version ${saved.version}.`)
      reloadConfigured()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ maxWidth: 720 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Backends</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Named Postgres targets the router can send statements to. Each entry's host must already
        be in <code>POLYWIRE_TRUSTED_BACKEND_HOSTS</code> on the PolyWire process, or it's
        silently skipped.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
        <>
          <ConnectionTester />

          {configured && configured.length > 0 && (
            <div style={{ marginBottom: 24 }}>
              <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Configured backends</div>
              {configured.map((b) => <ConfiguredBackendRow key={b.name} backend={b} />)}
            </div>
          )}

          <form onSubmit={handleSave}>
            <label style={{ display: 'block', marginBottom: 16 }}>
              <div style={{ fontSize: 13, marginBottom: 4 }}>Backends (one per line: name=jdbcUrl|user|password)</div>
              <div style={{ fontSize: 12, color: 'var(--muted)', marginBottom: 6 }}>
                The password segment doesn't have to be a literal password -- it's resolved at connect time, so a
                secret reference works too: <code>vault:secret/data/prod/postgres#password</code> (needs{' '}
                <code>VAULT_ADDR</code>/<code>VAULT_TOKEN</code> on PolyWire) or{' '}
                <code>cyberark:AppID=PolyWire&amp;Safe=DB-Secrets&amp;Object=prod-postgres</code> (needs{' '}
                <code>CYBERARK_CCP_URL</code>). A plain password still works exactly as before.
              </div>
              <textarea
                value={backends}
                onChange={(e) => setBackends(e.target.value)}
                placeholder={'reporting=jdbc:postgresql://reporting-host:5432/app|app_user|app_pass\nanalytics=jdbc:postgresql://analytics-host:5432/app|app_user|vault:secret/data/prod/postgres#password'}
                rows={6}
                style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
              />
            </label>
            <label style={{ display: 'block', marginBottom: 16 }}>
              <div style={{ fontSize: 13, marginBottom: 4 }}>Shard group (one backend name per line, in shard order)</div>
              <textarea
                value={shardBackends}
                onChange={(e) => setShardBackends(e.target.value)}
                placeholder={'shard0\nshard1\nshard2'}
                rows={4}
                style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
              />
            </label>
            <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
            {message && <span style={{ marginLeft: 12, color: 'var(--success, green)', fontSize: 13 }}>{message}</span>}
          </form>
        </>
      )}
    </div>
  )
}
