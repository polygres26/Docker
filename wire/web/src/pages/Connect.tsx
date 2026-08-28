import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { getStoredConnection, storeConnection, testConnection } from '../api/client'

/**
 * Entry screen for this SPA. Polywire's admin API is bearer-token protected and deliberately has
 * no session/cookie machinery (see MetricsServer's javadoc) -- so instead of a login form backed
 * by a server session, this just collects the admin URL + token once and keeps them in
 * sessionStorage (cleared when the tab closes, unlike localStorage). Every subsequent request
 * attaches `Authorization: Bearer <token>` and is prefixed with the stored base URL; a 401 anywhere
 * clears the stored token and bounces back here (see src/api/client.ts).
 */
export default function Connect() {
  const navigate = useNavigate()
  const existing = getStoredConnection()
  const [adminUrl, setAdminUrl] = useState(existing?.baseUrl ?? 'http://localhost:19090')
  const [adminToken, setAdminToken] = useState('')
  const [testing, setTesting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setTesting(true)
    setError(null)
    try {
      const baseUrl = adminUrl.trim().replace(/\/+$/, '')
      await testConnection(baseUrl, adminToken)
      storeConnection(baseUrl, adminToken)
      navigate('/metrics')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setTesting(false)
    }
  }

  return (
    <div style={{ maxWidth: 480, margin: '80px auto', padding: '0 20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 24 }}>
        <div style={{
          width: 36, height: 36, borderRadius: 10, background: 'var(--accent)', color: '#fff',
          display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 800,
          fontFamily: 'ui-monospace, monospace',
        }}>
          PW
        </div>
        <div>
          <div style={{ fontWeight: 700, fontSize: 16 }}>Polywire</div>
          <div style={{ fontSize: 12, color: 'var(--muted)' }}>Admin console</div>
        </div>
      </div>

      <div className="panel">
        <h1 style={{ fontSize: 18, marginTop: 0, marginBottom: 6 }}>Connect</h1>
        <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
          Enter the admin URL and bearer token for a running Polywire process
          (<code>POLYWIRE_ADMIN_TOKEN</code>). This browser talks to Polywire's admin API directly
          -- nothing is sent anywhere else. The token is kept only in this tab's session storage
          and is cleared when the tab closes.
        </p>

        <form onSubmit={handleSubmit}>
          <div className="field">
            <label htmlFor="adminUrl">Admin URL</label>
            <input
              id="adminUrl"
              type="text"
              value={adminUrl}
              onChange={(e) => setAdminUrl(e.target.value)}
              placeholder="http://localhost:19090"
            />
          </div>
          <div className="field">
            <label htmlFor="adminToken">Admin token</label>
            <input
              id="adminToken"
              type="password"
              value={adminToken}
              onChange={(e) => setAdminToken(e.target.value)}
              placeholder="POLYWIRE_ADMIN_TOKEN value"
            />
          </div>
          <button type="submit" className="primary" disabled={testing || !adminUrl.trim() || !adminToken.trim()}>
            {testing ? 'Connecting…' : 'Connect'}
          </button>
          {error && <div style={{ marginTop: 12, color: 'var(--hard, crimson)', fontSize: 13 }}>{error}</div>}
        </form>
      </div>
    </div>
  )
}
