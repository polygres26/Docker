import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import {
  getRememberPreference, getRequestTimeoutMs, getStoredConnection, setRequestTimeoutMs,
  storeConnection, testConnection,
} from '../api/client'

/**
 * Entry screen for this SPA. Warp's admin API is bearer-token protected and deliberately has
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
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [remember, setRemember] = useState(getRememberPreference)
  const [timeoutSec, setTimeoutSec] = useState(getRequestTimeoutMs() / 1000)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setTesting(true)
    setError(null)
    try {
      setRequestTimeoutMs(Math.max(1, timeoutSec) * 1000)
      const baseUrl = adminUrl.trim().replace(/\/+$/, '')
      await testConnection(baseUrl, adminToken)
      storeConnection(baseUrl, adminToken, remember)
      navigate('/dashboard')
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
          W
        </div>
        <div>
          <div style={{ fontWeight: 700, fontSize: 16 }}>Warp</div>
          <div style={{ fontSize: 12, color: 'var(--muted)' }}>Admin console</div>
        </div>
      </div>

      <div className="panel">
        <h1 style={{ fontSize: 18, marginTop: 0, marginBottom: 6 }}>Connect</h1>
        <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
          Enter the admin URL and bearer token for a running Warp process
          (<code>POLYWIRE_ADMIN_TOKEN</code>). This browser talks to Warp's admin API directly
          -- nothing is sent anywhere else. By default the token is kept only in this tab's session
          storage and is cleared when the tab closes (see Advanced options to change that).
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
          <button
            type="button"
            onClick={() => setShowAdvanced((v) => !v)}
            style={{
              display: 'flex', alignItems: 'center', gap: 5, background: 'none', border: 'none',
              padding: '4px 0', margin: '2px 0 14px', color: 'var(--muted)', fontSize: 12.5,
              cursor: 'pointer',
            }}
          >
            {showAdvanced ? <ChevronDown size={14} strokeWidth={2} /> : <ChevronRight size={14} strokeWidth={2} />}
            Advanced options
          </button>

          {showAdvanced && (
            <div style={{ marginBottom: 16, padding: '12px 14px', border: '1px solid var(--border)', borderRadius: 8 }}>
              <label style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 14, cursor: 'pointer' }}>
                <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
                <div>
                  <div style={{ fontSize: 13 }}>Remember on this device</div>
                  <div style={{ fontSize: 11.5, color: 'var(--muted)' }}>
                    Keeps the admin URL and token in this browser (localStorage) past closing the tab, instead of
                    clearing them when it closes. Only turn this on for a machine you trust.
                  </div>
                </div>
              </label>
              <label style={{ display: 'block' }}>
                <div style={{ fontSize: 13, marginBottom: 4 }}>Request timeout (seconds)</div>
                <input
                  type="number"
                  min={1}
                  value={timeoutSec}
                  onChange={(e) => setTimeoutSec(Number(e.target.value))}
                  style={{ width: 100 }}
                />
                <div style={{ fontSize: 11.5, color: 'var(--muted)', marginTop: 4 }}>
                  How long to wait for the admin API before giving up, on this connect attempt and every request
                  after it. Raise this if you're on a slow link to a remote Warp process.
                </div>
              </label>
            </div>
          )}

          <button type="submit" className="primary" disabled={testing || !adminUrl.trim() || !adminToken.trim()}>
            {testing ? 'Connecting…' : 'Connect'}
          </button>
          {error && <div style={{ marginTop: 12, color: 'var(--hard, crimson)', fontSize: 13 }}>{error}</div>}
        </form>
      </div>
    </div>
  )
}
