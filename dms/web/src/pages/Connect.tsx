import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { runScan } from '../api/client'
import DmsTabs from '../components/DmsTabs'
import { Button, Field, Input, Notice, Narrow, PageHeader, Section } from '../ui'

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
    <>
      <DmsTabs />
      <PageHeader
        title="Quick scan"
        subtitle="Connect to a source database once, without saving it, to assess Postgres-migration difficulty. Oracle, MySQL/MariaDB, and SQL Server are all supported."
      />

      <Narrow>
      <Section title="Source database">
        <form onSubmit={handleScan}>
          <Field label="JDBC URL" htmlFor="jdbcUrl">
            <Input
              id="jdbcUrl"
              value={jdbcUrl}
              onChange={(e) => setJdbcUrl(e.target.value)}
              placeholder="jdbc:oracle:thin:@host:1521/service"
            />
          </Field>
          <Field label="Schema / user" htmlFor="user">
            <Input id="user" value={user} onChange={(e) => setUser(e.target.value)} />
          </Field>
          <Field label="Password" htmlFor="password">
            <Input id="password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
          </Field>

          {error && <Notice tone="error">{error}</Notice>}

          <Button variant="primary" type="submit" disabled={loading}>
            {loading ? 'Scanning…' : 'Scan database'}
          </Button>
        </form>
      </Section>
      </Narrow>
    </>
  )
}
