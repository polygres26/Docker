import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { type Connection, createConnection, deleteConnection, listConnections } from '../api/client'

export default function Connections() {
  const [connections, setConnections] = useState<Connection[]>([])
  const [showForm, setShowForm] = useState(false)
  const [name, setName] = useState('')
  const [jdbcUrl, setJdbcUrl] = useState('jdbc:oracle:thin:@localhost:1521/ORCLPDB1')
  const [user, setUser] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)

  async function refresh() {
    setConnections(await listConnections())
  }

  useEffect(() => { refresh() }, [])

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault()
    setError(null)
    try {
      await createConnection({ name, jdbcUrl, user, password })
      setShowForm(false)
      setName(''); setJdbcUrl('jdbc:oracle:thin:@localhost:1521/ORCLPDB1'); setUser(''); setPassword('')
      await refresh()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  async function handleDelete(id: string) {
    await deleteConnection(id)
    await refresh()
  }

  return (
    <div>
      <h1 style={{ fontSize: 22, marginBottom: 20 }}>Connections</h1>

      <div className="panel" style={{ marginBottom: 20, maxWidth: 640 }}>
        {connections.length === 0 && <p style={{ color: 'var(--muted)' }}>No connections yet.</p>}
        {connections.map((c) => (
          <div key={c.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 0', borderBottom: '1px solid var(--border)' }}>
            <div>
              <Link to={`/connections/${c.id}`} style={{ color: 'var(--accent)', fontWeight: 600, textDecoration: 'none' }}>{c.name}</Link>
              <div style={{ color: 'var(--muted)', fontSize: 13 }}>{c.jdbcUrl}</div>
            </div>
            <button onClick={() => handleDelete(c.id)} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 6, padding: '4px 10px', color: 'var(--hard)', cursor: 'pointer' }}>
              Delete
            </button>
          </div>
        ))}
      </div>

      {!showForm && <button className="primary" onClick={() => setShowForm(true)}>Add connection</button>}

      {showForm && (
        <form className="panel" onSubmit={handleCreate}>
          <div className="field">
            <label htmlFor="name">Name</label>
            <input id="name" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Prod Oracle 19c" />
          </div>
          <div className="field">
            <label htmlFor="jdbcUrl">JDBC URL</label>
            <input id="jdbcUrl" value={jdbcUrl} onChange={(e) => setJdbcUrl(e.target.value)} />
          </div>
          <div className="field">
            <label htmlFor="user">Schema / user</label>
            <input id="user" value={user} onChange={(e) => setUser(e.target.value)} />
          </div>
          <div className="field">
            <label htmlFor="password">Password</label>
            <input id="password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
          </div>
          {error && <p style={{ color: 'var(--hard)' }}>{error}</p>}
          <div style={{ display: 'flex', gap: 10 }}>
            <button className="primary" type="submit">Save</button>
            <button type="button" onClick={() => setShowForm(false)} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 18px', color: 'var(--text)', cursor: 'pointer' }}>
              Cancel
            </button>
          </div>
        </form>
      )}
    </div>
  )
}
