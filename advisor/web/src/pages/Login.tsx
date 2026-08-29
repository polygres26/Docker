import { Eye, EyeOff } from 'lucide-react'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { login } from '../api/client'
import styles from '../Login.module.css'

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
    <div className={styles.page}>
      <div>
        <form className={styles.box} onSubmit={handleSubmit}>
          <div className={styles.brand}>
            <div className={styles.brandMark} />
            <div>
              <p className={styles.title}>Nexagres Advisor</p>
              <p className={styles.subtitle}>Postgres migration advisor</p>
            </div>
          </div>

          {error && <div className={styles.error}>{error}</div>}

          <input
            className={styles.field}
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="Username"
          />
          <div style={{ position: 'relative' }}>
            <input
              className={styles.field}
              type={showPassword ? 'text' : 'password'}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Password"
              style={{ paddingRight: 40, width: '100%', boxSizing: 'border-box' }}
            />
            <button
              type="button"
              onClick={() => setShowPassword((v) => !v)}
              aria-label={showPassword ? 'Hide password' : 'Show password'}
              title={showPassword ? 'Hide password' : 'Show password'}
              style={{
                position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
                background: 'none', border: 'none', padding: 4, cursor: 'pointer',
                color: 'var(--muted)', display: 'flex', alignItems: 'center',
              }}
            >
              {showPassword ? <EyeOff size={17} strokeWidth={1.8} /> : <Eye size={17} strokeWidth={1.8} />}
            </button>
          </div>
          <button className={styles.submit} type="submit" disabled={loading}>
            {loading ? 'Signing in…' : 'Sign in'}
          </button>
        </form>
        <p className={styles.note}>
          Default admin credentials are printed to the server log on first startup if
          NEXAGRES_ADMIN_PASSWORD isn't set.
        </p>
      </div>
    </div>
  )
}
