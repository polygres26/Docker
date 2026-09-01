import { useEffect, useState } from 'react'
import { type WireConfig, getWireConfig, saveWireConfig } from '../api/client'

/**
 * IP / CIDR access control -- edits `warp_config.aclRules` (one `allow:<cidr>` or
 * `reject:<cidr>` entry per line, first match wins, checked before any query reaches
 * pgwire/mysqlwire/mssqlwire/mongowire). A save appends a new warp_config version; every
 * running Warp process picks it up within milliseconds over LISTEN/NOTIFY, no restart.
 */
export default function AclRules() {
  const [rules, setRules] = useState('')
  const [trustedProxies, setTrustedProxies] = useState('')
  const [ppv2Enabled, setPpv2Enabled] = useState(false)
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  function apply(s: WireConfig) {
    setRules((s.aclRules ?? '').split(';').map((r) => r.trim()).filter(Boolean).join('\n'))
    setTrustedProxies(s.aclTrustedProxies ?? '')
    setPpv2Enabled(s.aclPpv2Enabled === 'true')
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
      const spec = rules.split('\n').map((r) => r.trim()).filter(Boolean).join(';')
      const saved = await saveWireConfig({
        aclRules: spec,
        aclPpv2Enabled: String(ppv2Enabled),
        aclTrustedProxies: trustedProxies || null,
      })
      setMessage(`Saved — warp_config version ${saved.version}.`)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ maxWidth: 720 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>ACL: IP / CIDR access</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        One rule per line, checked top to bottom — the first matching CIDR wins. No rules means
        every client is allowed.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
        <form onSubmit={handleSave}>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Rules</div>
            <textarea
              value={rules}
              onChange={(e) => setRules(e.target.value)}
              placeholder={'allow:10.0.0.0/8\nallow:192.168.1.0/24\nreject:0.0.0.0/0'}
              rows={8}
              style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
            />
          </label>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>
              Trusted proxies (CIDR list, comma-separated — required to honor PROXY protocol v2 source IPs)
            </div>
            <input
              type="text"
              value={trustedProxies}
              onChange={(e) => setTrustedProxies(e.target.value)}
              placeholder="10.0.0.0/8"
              style={{ width: '100%', padding: '8px 10px', fontSize: 14 }}
            />
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 16, fontSize: 13 }}>
            <input type="checkbox" checked={ppv2Enabled} onChange={(e) => setPpv2Enabled(e.target.checked)} />
            Trust PROXY protocol v2 (behind a load balancer that sends it)
          </label>
          <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
          {message && <span style={{ marginLeft: 12, color: 'var(--success, green)', fontSize: 13 }}>{message}</span>}
        </form>
      )}
    </div>
  )
}
