import { useEffect, useState } from 'react'
import { Database, PlugZap, Star, TableProperties } from 'lucide-react'
import { Link } from 'react-router-dom'
import {
  type BackendInfo, type BackendTestResult, type WireConfig,
  getWireConfig, listBackends, saveWireConfig, testBackendConnection, testConfiguredBackend,
} from '../api/client'
import CredentialField from '../components/CredentialField'
import styles from './Backends.module.css'
import { DataTable, Loading, Notice, PageHeader, Section, SortTh, StatusPill, SummaryGrid, Tag, useSort } from '../components/ui'

// Favorite backends pinned to the top of the configured-backends list, same pattern as
// versitygw's bucket favorites (star icon, persists across sessions -- see
// https://github.com/versity/versitygw/wiki/WebGUI#buckets). Scoped to localStorage (not
// sessionStorage, unlike the connection token) since "which backends I care about" is a per-user
// preference worth keeping across tabs and restarts, not sensitive like the admin token.
const FAVORITES_KEY = 'warp.favoriteBackends'

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

function TestResultBadge({ result }: { result: BackendTestResult }) {
  return (
    <Notice tone={result.ok ? 'ok' : 'bad'}>
      {result.ok ? '✓ Connected' : '✗ Failed'} in {result.tookMs}ms
      {result.ok && result.serverVersion && <> — {result.serverVersion.split(',')[0]}</>}
      {!result.ok && <> — {result.message}</>}
    </Notice>
  )
}

/**
 * A quick "does this actually connect" probe before a backend spec line gets pasted into config
 * (or committed at all) -- built after onboarding a real Supabase project surfaced exactly the
 * kind of mistake this exists to catch fast: a plausible-looking host that turned out to be
 * unreachable, found out only after saving and trying to browse it. See
 * com.sayonora.wire.core.BackendConnectivityTest for the server side.
 */
function ConnectionTester() {
  const [jdbcUrl, setJdbcUrl] = useState('jdbc:postgresql://host:5432/postgres?sslmode=require')
  const [user, setUser] = useState('')
  const [password, setPassword] = useState('')
  const [testing, setTesting] = useState(false)
  const [result, setResult] = useState<BackendTestResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function handleTest() {
    setTesting(true)
    setError(null)
    setResult(null)
    try {
      setResult(await testBackendConnection({ jdbcUrl, user, password }))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setTesting(false)
    }
  }

  return (
    <Section title="Test a connection before adding it">
      <label style={{ display: 'block', marginBottom: 10 }}>
        <div style={{ fontSize: 12.5, marginBottom: 4 }}>JDBC URL</div>
        <input value={jdbcUrl} onChange={(e) => setJdbcUrl(e.target.value)}
          style={{ width: '100%', padding: '7px 9px', fontSize: 13, fontFamily: 'monospace' }} />
      </label>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 10 }}>
        <label>
          <div style={{ fontSize: 12.5, marginBottom: 4 }}>User</div>
          <input value={user} onChange={(e) => setUser(e.target.value)}
            style={{ width: '100%', padding: '7px 9px', fontSize: 13 }} />
        </label>
        <div>
          <div style={{ fontSize: 12.5, marginBottom: 4 }}>Credential</div>
          <CredentialField value={password} onChange={setPassword} />
        </div>
      </div>
      <button type="button" onClick={handleTest} disabled={testing || !jdbcUrl.trim()}>
        {testing ? 'Testing…' : 'Test connection'}
      </button>
      {error && <div style={{ marginTop: 10 }}><Notice tone="bad">{error}</Notice></div>}
      {result && <div style={{ marginTop: 10 }}><TestResultBadge result={result} /></div>}
    </Section>
  )
}

function ConfiguredBackendRow({ backend, favorite, onToggleFavorite }: {
  backend: BackendInfo
  favorite: boolean
  onToggleFavorite: () => void
}) {
  const [testing, setTesting] = useState(false)
  const [result, setResult] = useState<BackendTestResult | null>(null)

  async function handleTest() {
    setTesting(true)
    setResult(null)
    try {
      setResult(await testConfiguredBackend(backend.name))
    } catch (e) {
      setResult({ ok: false, message: e instanceof Error ? e.message : String(e), tookMs: 0, serverVersion: null })
    } finally {
      setTesting(false)
    }
  }

  const dialect = backend.dialect ?? 'unknown'
  return (
    <tr>
      <td style={{ width: 40 }}>
        <button
          type="button"
          onClick={onToggleFavorite}
          title={favorite ? 'Remove from favorites' : 'Add to favorites'}
          aria-label={favorite ? `Remove ${backend.name} from favorites` : `Add ${backend.name} to favorites`}
          aria-pressed={favorite}
          className={styles.iconBtn}
          style={{ color: favorite ? 'var(--sy-accent-strong)' : 'var(--sy-muted)' }}
        >
          <Star size={15} strokeWidth={1.8} fill={favorite ? 'currentColor' : 'none'} />
        </button>
      </td>
      <td className={styles.mono}>{backend.name}</td>
      <td><Tag>{dialect}</Tag></td>
      <td className={styles.mono}>{backend.jdbcUrl}</td>
      <td>
        {testing ? <StatusPill tone="muted">Testing</StatusPill>
          : result ? (
            <span title={result.message}>
              <StatusPill tone={result.ok ? 'ok' : 'bad'}>{result.ok ? `Connected · ${result.tookMs} ms` : 'Failed'}</StatusPill>
              {result.ok && result.serverVersion && <div className={styles.sub}>{result.serverVersion.split(',')[0]}</div>}
              {!result.ok && <div className={styles.sub}>{result.message}</div>}
            </span>
          ) : <span className={styles.sub}>Not tested</span>}
      </td>
      <td>
        <div className={styles.actions}>
          <Link to="/data" title="Open in data explorer" aria-label={`Open ${backend.name} in data explorer`} className={styles.iconBtn}>
            <TableProperties size={16} strokeWidth={1.8} />
          </Link>
          <button type="button" onClick={handleTest} disabled={testing} title="Test connection"
            aria-label={`Test connection to ${backend.name}`} className={styles.iconBtn}>
            <PlugZap size={16} strokeWidth={1.8} />
          </button>
        </div>
      </td>
    </tr>
  )
}

/**
 * Multi-backend routing targets -- edits `warp_config.backends` (name=jdbcUrl|user|password,
 * one per line here, `;`-joined on the wire) and `.shardBackends` (the ordered shard group, one
 * name per line here, `,`-joined on the wire). See BackendRegistry.fromConfig for the exact
 * grammar and the WARP_TRUSTED_BACKEND_HOSTS allowlist check every entry has to clear.
 */
export default function Backends() {
  const [backends, setBackends] = useState('')
  const [shardBackends, setShardBackends] = useState('')
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [configured, setConfigured] = useState<BackendInfo[] | null>(null)
  const [favorites, setFavorites] = useState<Set<string>>(loadFavorites)

  const { sorted, sort, toggle } = useSort(configured ?? [], {
    name: (b) => b.name, dialect: (b) => b.dialect ?? 'unknown', url: (b) => b.jdbcUrl,
  }, { key: 'name', dir: 'asc' })
  // Favorites stay pinned above whatever sort is chosen (same pinning as before).
  const sortedBackends = [...sorted].sort((a, b) => Number(favorites.has(b.name)) - Number(favorites.has(a.name)))
  const dialectCounts = Object.entries((configured ?? []).reduce<Record<string, number>>((acc, b) => {
    const d = b.dialect ?? 'unknown'
    acc[d] = (acc[d] ?? 0) + 1
    return acc
  }, {}))

  function toggleFavorite(name: string) {
    setFavorites((prev) => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name); else next.add(name)
      saveFavorites(next)
      return next
    })
  }

  function apply(s: WireConfig) {
    setBackends((s.backends ?? '').split(';').map((r) => r.trim()).filter(Boolean).join('\n'))
    setShardBackends((s.shardBackends ?? '').split(',').map((r) => r.trim()).filter(Boolean).join('\n'))
  }

  function reloadConfigured() {
    listBackends().then(setConfigured).catch(() => setConfigured(null))
  }

  useEffect(() => {
    getWireConfig()
      .then((s) => { apply(s); setLoaded(true) })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
    reloadConfigured()
  }, [])

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    setMessage(null)
    try {
      const saved = await saveWireConfig({
        backends: backends.split('\n').map((r) => r.trim()).filter(Boolean).join(';'),
        shardBackends: shardBackends.split('\n').map((r) => r.trim()).filter(Boolean).join(','),
      })
      setMessage(`Saved — warp_config version ${saved.version}.`)
      reloadConfigured()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div>
      <PageHeader title="Backends" description={<>Named Postgres targets the router can send statements to. Each entry's host must already be in <code>WARP_TRUSTED_BACKEND_HOSTS</code> on the Warp process, or it's silently skipped.</>} />

      {error && (
        <Notice tone="bad">{error}</Notice>
      )}

      {!loaded && !error ? (
        <Loading />
      ) : (
        <>
          <ConnectionTester />

          {configured && configured.length > 0 && (
            <Section flush title="Configured backends" meta={`${configured.length} backend${configured.length === 1 ? '' : 's'}`}>
              <SummaryGrid items={dialectCounts.map(([d, n]) => ({ title: d, sub: `${n} backend${n === 1 ? '' : 's'}`, icon: <Database size={16} aria-hidden="true" /> }))} />
              <DataTable caption="Configured backends" minWidth={760}>
                <thead>
                  <tr>
                    <th aria-label="Favorite"></th>
                    <SortTh label="Name" k="name" sort={sort} onSort={toggle} />
                    <SortTh label="Dialect" k="dialect" sort={sort} onSort={toggle} />
                    <SortTh label="Target" k="url" sort={sort} onSort={toggle} />
                    <th>Status</th>
                    <th aria-label="Actions"></th>
                  </tr>
                </thead>
                <tbody>
                  {sortedBackends.map((b) => (
                    <ConfiguredBackendRow
                      key={b.name}
                      backend={b}
                      favorite={favorites.has(b.name)}
                      onToggleFavorite={() => toggleFavorite(b.name)}
                    />
                  ))}
                </tbody>
              </DataTable>
            </Section>
          )}

          <Section title="Edit backend definitions">
          <form onSubmit={handleSave}>
            <label style={{ display: 'block', marginBottom: 16 }}>
              <div style={{ fontSize: 13, marginBottom: 4 }}>Backends (one per line: name=jdbcUrl|user|password)</div>
              <div style={{ fontSize: 12, color: 'var(--muted)', marginBottom: 6 }}>
                The password segment doesn't have to be a literal password -- it's resolved at connect time, so a
                secret reference works too: <code>vault:secret/data/prod/postgres#password</code> (needs{' '}
                <code>VAULT_ADDR</code>/<code>VAULT_TOKEN</code> on Warp) or{' '}
                <code>cyberark:AppID=Warp&amp;Safe=DB-Secrets&amp;Object=prod-postgres</code> (needs{' '}
                <code>CYBERARK_CCP_URL</code>). A plain password still works exactly as before.
              </div>
              <textarea
                value={backends}
                onChange={(e) => setBackends(e.target.value)}
                placeholder={'reporting=jdbc:postgresql://reporting-host:5432/app|app_user|app_pass\nanalytics=jdbc:postgresql://analytics-host:5432/app|app_user|vault:secret/data/prod/postgres#password'}
                rows={6}
                style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
              />
            </label>
            <label style={{ display: 'block', marginBottom: 16 }}>
              <div style={{ fontSize: 13, marginBottom: 4 }}>Shard group (one backend name per line, in shard order)</div>
              <textarea
                value={shardBackends}
                onChange={(e) => setShardBackends(e.target.value)}
                placeholder={'shard0\nshard1\nshard2'}
                rows={4}
                style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
              />
            </label>
            <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
            {message && <span style={{ marginLeft: 12, color: 'var(--success)', fontSize: 13 }}>{message}</span>}
          </form>
          </Section>
        </>
      )}
    </div>
  )
}
