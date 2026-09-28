import { Eye, EyeOff } from 'lucide-react'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { login } from '../api/client'
import logo from '../assets/logo.png'
import { Button, Field, Input, Notice } from '../ui'
import styles from './Login.module.css'

export default function Login() {
  const navigate = useNavigate()
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('')
  const [showPassword, setShowPassword] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)
    try {
      await login(username, password)
      navigate('/dashboard')
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className={styles.screen}>
      <section className={styles.context} aria-label="About Sayonora Ferry">
        <div className={styles.brand}>
          <span className={styles.markTile}><img src={logo} alt="" /></span>
          Sayonora Ferry
        </div>
        <div className={styles.message}>
          <h2>Assess, size and move your databases to Postgres.</h2>
          <p>Score migration difficulty from a live connection or an uploaded report, size the target, and run data sync jobs from one console.</p>
        </div>
      </section>

      <div className={styles.formWrap}>
        <form className={styles.form} onSubmit={handleSubmit}>
          <p className={styles.eyebrow}>Database Migration Service</p>
          <h1>Sign in</h1>
          <p className={styles.copy}>Use your admin account to continue.</p>

          {error && <Notice tone="error">{error}</Notice>}

          <Field label="Username" htmlFor="login-username">
            <Input id="login-username" large value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" />
          </Field>
          <Field label="Password" htmlFor="login-password">
            <div className={styles.fieldWrap}>
              <Input
                id="login-password" large
                type={showPassword ? 'text' : 'password'}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete="current-password"
                style={{ paddingRight: 44 }}
              />
              <button
                type="button" className={styles.reveal}
                onClick={() => setShowPassword((v) => !v)}
                aria-label={showPassword ? 'Hide password' : 'Show password'}
                title={showPassword ? 'Hide password' : 'Show password'}
              >
                {showPassword ? <EyeOff size={17} strokeWidth={1.8} /> : <Eye size={17} strokeWidth={1.8} />}
              </button>
            </div>
          </Field>
          <Button type="submit" variant="primary" full disabled={loading} style={{ minHeight: 44 }}>
            {loading ? 'Signing in…' : 'Sign in'}
          </Button>
          <p className={styles.help}>
            Default admin credentials are printed to the server log on first startup if
            SAYONORA_ADMIN_PASSWORD isn't set.
          </p>
        </form>
      </div>
    </div>
  )
}
