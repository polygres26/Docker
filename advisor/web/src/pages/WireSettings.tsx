import { useEffect, useState } from 'react'
import { type WireConnectionStatus, getWireSettings, saveWireSettings } from '../api/client'

/**
 * Where PolyWire's admin API lives and what bearer token to reach it with -- the one setting
 * every other Wire page (Firewall Rules, and future ones) needs before it can do anything.
 */
export default function WireSettings() {
  const [status, setStatus] = useState<WireConnectionStatus | null>(null)
  const [adminUrl, setAdminUrl] = useState('http://localhost:19090')
  const [adminToken, setAdminToken] = useState('')
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getWireSettings().then((s) => {
      setStatus(s)
      if (s.adminUrl) setAdminUrl(s.adminUrl)
    }).catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    setMessage(null)
    try {
      const saved = await saveWireSettings(adminUrl, adminToken)
      setStatus(saved)
      setAdminToken('')
      setMessage('Saved.')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ maxWidth: 560 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Wire connection</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Where PolyWire's admin API lives. Advisor's backend calls it server-to-server -- the
        browser never sees the admin token.
      </p>

      {status && (
        <div style={{
          padding: '8px 12px', borderRadius: 6, marginBottom: 16, fontSize: 13,
          background: status.configured ? 'var(--success-bg, #e6f4ea)' : 'var(--warn-bg, #fff4e5)',
        }}>
          {status.configured
            ? `Configured — ${status.adminUrl}`
            : 'Not configured yet — set the admin URL and token below.'}
        </div>
      )}

      <form onSubmit={handleSave}>
        <label style={{ display: 'block', marginBottom: 12 }}>
          <div style={{ fontSize: 13, marginBottom: 4 }}>Admin URL</div>
          <input
            type="text"
            value={adminUrl}
            onChange={(e) => setAdminUrl(e.target.value)}
            placeholder="http://localhost:19090"
            style={{ width: '100%', padding: '8px 10px', fontSize: 14 }}
          />
        </label>
        <label style={{ display: 'block', marginBottom: 16 }}>
          <div style={{ fontSize: 13, marginBottom: 4 }}>
            Admin token {status?.hasToken && <span style={{ color: 'var(--muted)' }}>(leave blank to keep the stored one)</span>}
          </div>
          <input
            type="password"
            value={adminToken}
            onChange={(e) => setAdminToken(e.target.value)}
            placeholder={status?.hasToken ? '••••••••' : 'POLYWIRE_ADMIN_TOKEN value'}
            style={{ width: '100%', padding: '8px 10px', fontSize: 14 }}
          />
        </label>
        <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
        {message && <span style={{ marginLeft: 12, color: 'var(--success, green)', fontSize: 13 }}>{message}</span>}
        {error && <div style={{ marginTop: 12, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>}
      </form>
    </div>
  )
}
