import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { runScan } from '../api/client'

/**
 * Oracle-only today, per the project's stated sequencing (Oracle first, then MariaDB/MySQL) --
 * the jdbcUrl placeholder and copy reflect that; ScanRoute on the backend enforces it too (a
 * non-Oracle URL gets a clear 501, not a silent failure).
 */
export default function Connect() {
  const navigate = useNavigate()
  const [jdbcUrl, setJdbcUrl] = useState('jdbc:oracle:thin:@localhost:1521/ORCLPDB1')
  const [user, setUser] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleScan(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)
    try {
      const result = await runScan({ jdbcUrl, user, password })
      navigate('/report', { state: result })
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="app-shell">
      <h1>Polygres Advisor</h1>
      <p style={{ color: 'var(--muted)' }}>
        Connect to a source database to assess Postgres-migration difficulty. Oracle 19c is
        supported today; MariaDB/MySQL is next.
      </p>

      <form className="panel" onSubmit={handleScan}>
        <div className="field">
          <label htmlFor="jdbcUrl">JDBC URL</label>
          <input
            id="jdbcUrl"
            value={jdbcUrl}
            onChange={(e) => setJdbcUrl(e.target.value)}
            placeholder="jdbc:oracle:thin:@host:1521/service"
          />
        </div>
        <div className="field">
          <label htmlFor="user">Schema / user</label>
          <input id="user" value={user} onChange={(e) => setUser(e.target.value)} />
        </div>
        <div className="field">
          <label htmlFor="password">Password</label>
          <input
            id="password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </div>

        {error && <p style={{ color: 'var(--hard)' }}>{error}</p>}

        <button className="primary" type="submit" disabled={loading}>
          {loading ? 'Scanning…' : 'Scan database'}
        </button>
      </form>
    </div>
  )
}
