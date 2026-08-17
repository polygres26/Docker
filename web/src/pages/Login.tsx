import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { login } from '../api/client'
import styles from '../Login.module.css'

export default function Login() {
  const navigate = useNavigate()
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)
    try {
      await login(username, password)
      navigate('/connections')
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
              <p className={styles.title}>Polygres Advisor</p>
              <p className={styles.subtitle}>Oracle → Postgres migration assessment</p>
            </div>
          </div>

          {error && <div className={styles.error}>{error}</div>}

          <input
            className={styles.field}
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="Username"
          />
          <input
            className={styles.field}
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="Password"
          />
          <button className={styles.submit} type="submit" disabled={loading}>
            {loading ? 'Signing in…' : 'Sign in'}
          </button>
        </form>
        <p className={styles.note}>
          Default admin credentials are printed to the server log on first startup if
          POLYGRES_ADMIN_PASSWORD isn't set.
        </p>
      </div>
    </div>
  )
}
