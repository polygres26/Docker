import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Database, Plus, Search, Star, Trash2 } from 'lucide-react'
import { type Connection, createConnection, deleteConnection, listConnections } from '../api/client'
import DmsTabs from '../components/DmsTabs'
import CredentialField from '../components/CredentialField'
import {
  Button, DataTable, EmptyState, Field, FormActions, Input, LinkButton, Notice, PageHeader, Section,
} from '../ui'
import { table } from '../ui/styles'
import { formatTimestamp } from './migrationServiceShared'
import styles from './Connections.module.css'

// Favorite connections pinned to the top of the list, same pattern as versitygw's bucket
// favorites (star icon, persists across sessions -- see
// https://github.com/versity/versitygw/wiki/WebGUI#buckets).
// Deliberately kept as the pre-rename "advisor.*" key (not renamed to "dms.*" along with
// everything else in this rebrand): it's a per-browser localStorage key, not a code identifier --
// renaming it would silently reset every existing user's favorited connections on their next
// visit for no functional benefit.
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

  const sorted = [...connections].sort((a, b) => Number(favorites.has(b.id)) - Number(favorites.has(a.id)))

  return (
    <>
      <DmsTabs />
      <PageHeader
        title="Connections"
        subtitle="Source databases to assess. Open one to browse its objects, score migration difficulty, capture workload and size the target."
        actions={!showForm && <Button variant="primary" onClick={() => setShowForm(true)}><Plus size={15} strokeWidth={2} aria-hidden />Add connection</Button>}
      />

      {showForm && (
        <Section title="New connection">
          <form onSubmit={handleCreate}>
            <Field label="Name" htmlFor="name">
              <Input id="name" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Prod Oracle 19c" />
            </Field>
            <Field label="JDBC URL" htmlFor="jdbcUrl">
              <Input id="jdbcUrl" value={jdbcUrl} onChange={(e) => setJdbcUrl(e.target.value)} />
            </Field>
            <Field label="Schema / user" htmlFor="user">
              <Input id="user" value={user} onChange={(e) => setUser(e.target.value)} />
            </Field>
            <Field label="Credential" htmlFor="password">
              <CredentialField id="password" value={password} onChange={setPassword} />
            </Field>
            {error && <Notice tone="error">{error}</Notice>}
            <FormActions>
              <Button variant="primary" type="submit">Save</Button>
              <Button onClick={() => setShowForm(false)}>Cancel</Button>
            </FormActions>
          </form>
        </Section>
      )}

      <Section title="Saved connections" meta={`${connections.length} total`} flush>
        {connections.length === 0 ? (
          <EmptyState icon={Database} title="No connections yet" action={!showForm && <Button variant="primary" onClick={() => setShowForm(true)}>Add connection</Button>}>
            Add a source database to start an assessment.
          </EmptyState>
        ) : (
          <DataTable caption="Saved connections">
            <thead>
              <tr>
                <th scope="col" className={table.narrow}><span className="sr-only">Favorite</span></th>
                <th scope="col">Name</th>
                <th scope="col">JDBC URL</th>
                <th scope="col">Added</th>
                <th scope="col"><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((c) => (
                <tr key={c.id}>
                  <td>
                    <Button
                      variant="ghost" size="sm" icon
                      className={favorites.has(c.id) ? styles.starOn : undefined}
                      onClick={() => toggleFavorite(c.id)}
                      title={favorites.has(c.id) ? 'Remove from favorites' : 'Add to favorites'}
                      aria-label={favorites.has(c.id) ? 'Remove from favorites' : 'Add to favorites'}
                      aria-pressed={favorites.has(c.id)}
                    >
                      <Star size={15} strokeWidth={1.8} fill={favorites.has(c.id) ? 'currentColor' : 'none'} />
                    </Button>
                  </td>
                  <td><Link to={`/connections/${c.id}`} className={table.main}>{c.name}</Link></td>
                  {/* Full JDBC URLs can run long (SQL Server's "database=...;encrypt=...;" strings) -- truncated
                      here, shown in full on the connection detail page. */}
                  <td className={`${table.ellipsis} ${table.mono}`} title={c.jdbcUrl}>{c.jdbcUrl}</td>
                  <td className={styles.when}>{formatTimestamp(c.createdAt)}</td>
                  <td>
                    <div className={styles.actions}>
                      <LinkButton to={`/connections/${c.id}`} variant="ghost" size="sm" className={styles.iconLink} title="Browse objects" aria-label="Browse objects">
                        <Search size={15} strokeWidth={1.8} />
                      </LinkButton>
                      <Button variant="ghost" size="sm" icon onClick={() => handleDelete(c.id)} title="Delete connection" aria-label={`Delete connection ${c.name}`} className={styles.danger}>
                        <Trash2 size={15} strokeWidth={1.8} />
                      </Button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </Section>
    </>
  )
}
