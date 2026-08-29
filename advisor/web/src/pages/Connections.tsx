import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Search, Star, Trash2 } from 'lucide-react'
import { type Connection, createConnection, deleteConnection, listConnections } from '../api/client'
import AdvisorTabs from '../components/AdvisorTabs'
import CredentialField from '../components/CredentialField'

// Favorite connections pinned to the top of the list, same pattern as versitygw's bucket
// favorites (star icon, persists across sessions -- see
// https://github.com/versity/versitygw/wiki/WebGUI#buckets).
const FAVORITES_KEY = 'advisor.favoriteConnections'

function loadFavorites(): Set<string> {
  try {
    return new Set(JSON.parse(localStorage.getItem(FAVORITES_KEY) ?? '[]'))
  } catch {
    return new Set()
  }
}

function saveFavorites(favs: Set<string>) {
  localStorage.setItem(FAVORITES_KEY, JSON.stringify([...favs]))
}

export default function Connections() {
  const [connections, setConnections] = useState<Connection[]>([])
  const [showForm, setShowForm] = useState(false)
  const [name, setName] = useState('')
  const [jdbcUrl, setJdbcUrl] = useState('jdbc:oracle:thin:@localhost:1521/ORCLPDB1')
  const [user, setUser] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [favorites, setFavorites] = useState<Set<string>>(loadFavorites)

  function toggleFavorite(id: string) {
    setFavorites((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      saveFavorites(next)
      return next
    })
  }

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
      <AdvisorTabs />
      <h1 style={{ fontSize: 22, marginBottom: 20 }}>Connections</h1>

      <div className="panel" style={{ marginBottom: 20, maxWidth: 640 }}>
        {connections.length === 0 && <p style={{ color: 'var(--muted)' }}>No connections yet.</p>}
        {[...connections]
          .sort((a, b) => Number(favorites.has(b.id)) - Number(favorites.has(a.id)))
          .map((c) => (
          <div key={c.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 0', borderBottom: '1px solid var(--border)', gap: 10 }}>
            <button
              type="button"
              onClick={() => toggleFavorite(c.id)}
              title={favorites.has(c.id) ? 'Remove from favorites' : 'Add to favorites'}
              aria-label={favorites.has(c.id) ? 'Remove from favorites' : 'Add to favorites'}
              style={{ background: 'none', border: 'none', padding: 2, cursor: 'pointer', color: favorites.has(c.id) ? 'var(--accent)' : 'var(--muted)', display: 'flex', flexShrink: 0 }}
            >
              <Star size={15} strokeWidth={1.8} fill={favorites.has(c.id) ? 'currentColor' : 'none'} />
            </button>
            <div style={{ minWidth: 0, flex: 1 }}>
              <Link to={`/connections/${c.id}`} style={{ color: 'var(--accent)', fontWeight: 600, textDecoration: 'none' }}>{c.name}</Link>
              {/* Full JDBC URL can run well past the panel width (SQL Server's "database=...;encrypt=...;" style
                  strings especially) -- truncate with an ellipsis here and rely on the connection detail page
                  (linked via the name above, and via clicking this text too) to show it in full, wrapped. */}
              <Link
                to={`/connections/${c.id}`}
                title={c.jdbcUrl}
                style={{
                  display: 'block', color: 'var(--muted)', fontSize: 13, textDecoration: 'none',
                  overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                }}
              >
                {c.jdbcUrl}
              </Link>
            </div>
            <Link
              to={`/connections/${c.id}`}
              title="Browse objects"
              aria-label="Browse objects"
              style={{ display: 'flex', color: 'var(--muted)', flexShrink: 0, padding: 4 }}
            >
              <Search size={16} strokeWidth={1.8} />
            </Link>
            <button
              onClick={() => handleDelete(c.id)}
              title="Delete connection"
              aria-label="Delete connection"
              style={{ background: 'none', border: 'none', padding: 4, color: 'var(--hard)', cursor: 'pointer', flexShrink: 0, display: 'flex' }}
            >
              <Trash2 size={16} strokeWidth={1.8} />
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
            <label htmlFor="password">Credential</label>
            <CredentialField value={password} onChange={setPassword} />
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
