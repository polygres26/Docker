import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { login } from '../api/client'

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
    <div className="app-shell" style={{ maxWidth: 420 }}>
      <h1>Polygres Advisor</h1>
      <form className="panel" onSubmit={handleSubmit}>
        <div className="field">
          <label htmlFor="username">Username</label>
          <input id="username" value={username} onChange={(e) => setUsername(e.target.value)} />
        </div>
        <div className="field">
          <label htmlFor="password">Password</label>
          <input id="password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
        </div>
        {error && <p style={{ color: 'var(--hard)' }}>{error}</p>}
        <button className="primary" type="submit" disabled={loading}>
          {loading ? 'Signing in…' : 'Sign in'}
        </button>
      </form>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 12 }}>
        Default admin credentials are printed to the server log on first startup if
        POLYGRES_ADMIN_PASSWORD isn't set.
      </p>
    </div>
  )
}
