import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { type WireConfig, getWireConfig, saveWireConfig } from '../api/client'

/**
 * Multi-backend routing targets -- edits `polywire_config.backends` (name=jdbcUrl|user|password,
 * one per line here, `;`-joined on the wire) and `.shardBackends` (the ordered shard group, one
 * name per line here, `,`-joined on the wire). See BackendRegistry.fromConfig for the exact
 * grammar and the POLYWIRE_TRUSTED_BACKEND_HOSTS allowlist check every entry has to clear.
 */
export default function WireBackends() {
  const [backends, setBackends] = useState('')
  const [shardBackends, setShardBackends] = useState('')
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  function apply(s: WireConfig) {
    setBackends((s.backends ?? '').split(';').map((r) => r.trim()).filter(Boolean).join('\n'))
    setShardBackends((s.shardBackends ?? '').split(',').map((r) => r.trim()).filter(Boolean).join('\n'))
  }

  useEffect(() => {
    getWireConfig()
      .then((s) => { apply(s); setLoaded(true) })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
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
        silently skipped. Not configured yet? Set the connection on the{' '}
        <Link to="/wire-settings">Wire connection</Link> page first.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
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
      )}
    </div>
  )
}
