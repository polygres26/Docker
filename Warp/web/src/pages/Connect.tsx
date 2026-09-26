import { useState } from 'react'
import { ChevronDown, ChevronRight, Eye, EyeOff, KeyRound, ShieldCheck } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import {
  getRememberPreference, getRequestTimeoutMs, getStoredConnection, setRequestTimeoutMs,
  storeConnection, testConnection,
} from '../api/client'
import logo from '../assets/logo.png'
import styles from './Connect.module.css'

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
  const [showToken, setShowToken] = useState(false)
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
      navigate('/overview')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setTesting(false)
    }
  }

  return (
    <div className={styles.screen}>
      <section className={styles.context} aria-label="About Warp admin">
        <div className={styles.brand}>
          <span className={styles.logoTile}><img src={logo} alt="" className={styles.logo} /></span>
          <span>Sayonora</span>
        </div>
        <div className={styles.message}>
          <h1>One gateway.<br />Every backend.</h1>
          <p>Route, govern and observe the SQL traffic Warp fronts, from one controlled admin console.</p>
        </div>
        <ul className={styles.signals}>
          <li><ShieldCheck size={14} aria-hidden="true" />Bearer-token protected admin API</li>
          <li><KeyRound size={14} aria-hidden="true" />Token kept in this tab unless you opt in</li>
        </ul>
      </section>

      <main className={styles.formWrap}>
        <form className={styles.form} onSubmit={handleSubmit}>
          <p className={styles.eyebrow}>Warp admin console</p>
          <h2>Connect to Warp</h2>
          <p className={styles.formCopy}>Enter the admin URL and token of the Warp process you want to manage.</p>

          <label className={styles.label} htmlFor="adminUrl">Admin URL</label>
          <input id="adminUrl" className={styles.input} type="text" value={adminUrl} autoComplete="url"
            onChange={(e) => setAdminUrl(e.target.value)} placeholder="http://localhost:19090" />

          <label className={styles.label} htmlFor="adminToken">Admin token</label>
          <div className={styles.inputWrap}>
            <input id="adminToken" className={styles.input} type={showToken ? 'text' : 'password'} value={adminToken}
              autoComplete="current-password" onChange={(e) => setAdminToken(e.target.value)} placeholder="WARP_ADMIN_TOKEN value" />
            <button type="button" className={styles.eye} onClick={() => setShowToken((v) => !v)}
              aria-label={showToken ? 'Hide token' : 'Show token'} aria-pressed={showToken}>
              {showToken ? <EyeOff size={16} /> : <Eye size={16} />}
            </button>
          </div>

          <button type="button" className={styles.advToggle} onClick={() => setShowAdvanced((v) => !v)}
            aria-expanded={showAdvanced} aria-controls="advanced-options">
            {showAdvanced ? <ChevronDown size={14} strokeWidth={2} /> : <ChevronRight size={14} strokeWidth={2} />}
            Advanced options
          </button>

          {showAdvanced && (
            <div id="advanced-options" className={styles.advanced}>
              <label className={styles.check}>
                <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
                <span>
                  <span className={styles.checkTitle}>Remember on this device</span>
                  <span className={styles.help}>
                    Keeps the admin URL and token in this browser (localStorage) past closing the tab, instead of
                    clearing them when it closes. Only turn this on for a machine you trust.
                  </span>
                </span>
              </label>
              <label className={styles.label} htmlFor="timeoutSec">Request timeout (seconds)</label>
              <input id="timeoutSec" className={styles.input} type="number" min={1} value={timeoutSec}
                onChange={(e) => setTimeoutSec(Number(e.target.value))} style={{ width: 110 }} />
              <span className={styles.help}>
                How long to wait for the admin API before giving up, on this connect attempt and every request
                after it. Raise this if you're on a slow link to a remote Warp process.
              </span>
            </div>
          )}

          <button type="submit" className={styles.submit} disabled={testing || !adminUrl.trim() || !adminToken.trim()}>
            {testing ? 'Connecting…' : 'Connect'}
          </button>
          {error && <div role="alert" className={styles.error}>{error}</div>}
          <p className={styles.helpline}>Having trouble? Check that WARP_ADMIN_TOKEN is set on the Warp process.</p>
        </form>
      </main>
    </div>
  )
}
