import { Play } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  type BackendInfo, type QueryResult, type TableInfo,
  listBackendTables, listBackends, runBackendQuery,
} from '../api/client'
import styles from './DataExplorer.module.css'

/**
 * Object browser + ad-hoc SQL console for Polywire's configured backends -- pick a backend,
 * browse its schemas/tables in the left pane, click one to preview it, or write any SQL in the
 * console and run it. Talks straight to com.nexagres.wire.core.DataExplorer via the admin API;
 * see that class's javadoc for why this bypasses SQL Firewall/ACL by design (it's an admin tool,
 * not a client-facing wire protocol) and is gated by the same admin token as the rest of
 * Polywire's config surface.
 */
export default function DataExplorer() {
  const [backends, setBackends] = useState<BackendInfo[] | null>(null);
  const [backend, setBackend] = useState<string>('')
  const [tables, setTables] = useState<TableInfo[] | null>(null)
  const [tableFilter, setTableFilter] = useState('')
  const [sql, setSql] = useState('')
  const [result, setResult] = useState<QueryResult | null>(null)
  const [running, setRunning] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedTable, setSelectedTable] = useState<string | null>(null)

  useEffect(() => {
    listBackends()
      .then((list) => {
        setBackends(list)
        if (list.length > 0) setBackend(list[0].name)
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  useEffect(() => {
    if (!backend) return
    setTables(null)
    setSelectedTable(null)
    listBackendTables(backend)
      .then(setTables)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [backend])

  const grouped = useMemo(() => {
    const byFilter = (tables ?? []).filter((t) =>
      tableFilter.trim() === '' || `${t.schema}.${t.name}`.toLowerCase().includes(tableFilter.toLowerCase()))
    const bySchema = new Map<string, TableInfo[]>()
    for (const t of byFilter) {
      if (!bySchema.has(t.schema)) bySchema.set(t.schema, [])
      bySchema.get(t.schema)!.push(t)
    }
    return bySchema
  }, [tables, tableFilter])

  async function runSql(sqlToRun: string) {
    setRunning(true)
    setError(null)
    try {
      const res = await runBackendQuery(backend, sqlToRun)
      setResult(res)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setResult(null)
    } finally {
      setRunning(false)
    }
  }

  function previewTable(t: TableInfo) {
    setSelectedTable(`${t.schema}.${t.name}`)
    const query = `SELECT * FROM ${t.schema}.${t.name} LIMIT 100`
    setSql(query)
    runSql(query)
  }

  if (backends !== null && backends.length === 0) {
    return (
      <div className={styles.page}>
        <h1 style={{ fontSize: 22, marginBottom: 4 }}>Data explorer</h1>
        <p style={{ color: 'var(--muted)', fontSize: 13 }}>
          No backends configured yet. Add one on the <Link to="/backends">Backends</Link> page first.
        </p>
      </div>
    )
  }

  return (
    <div className={styles.page}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Data explorer</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 16 }}>
        Browse objects and run ad-hoc SQL directly against a backend. Bypasses SQL Firewall/ACL by
        design -- this is an admin tool, not client traffic.
      </p>

      <div className={styles.toolbar}>
        <select className={styles.select} value={backend} onChange={(e) => setBackend(e.target.value)}>
          {(backends ?? []).map((b) => (
            <option key={b.name} value={b.name}>{b.name} ({b.dialect ?? 'unknown'})</option>
          ))}
        </select>
        {error && <span style={{ color: 'var(--hard, crimson)', fontSize: 13 }}>{error}</span>}
      </div>

      <div className={styles.body}>
        <div className={styles.tablePane}>
          <input
            className={styles.tableSearch}
            placeholder="Filter tables…"
            value={tableFilter}
            onChange={(e) => setTableFilter(e.target.value)}
          />
          {tables === null ? (
            <div style={{ color: 'var(--muted)', fontSize: 12.5, padding: 8 }}>Loading…</div>
          ) : tables.length === 0 ? (
            <div style={{ color: 'var(--muted)', fontSize: 12.5, padding: 8 }}>No tables found.</div>
          ) : (
            Array.from(grouped.entries()).map(([schema, ts]) => (
              <div className={styles.tableGroup} key={schema}>
                <div className={styles.schemaLabel}>{schema}</div>
                {ts.map((t) => (
                  <button
                    key={`${t.schema}.${t.name}`}
                    className={`${styles.tableRow} ${selectedTable === `${t.schema}.${t.name}` ? styles.tableRowActive : ''}`}
                    onClick={() => previewTable(t)}
                    title={t.type}
                  >
                    {t.name}
                  </button>
                ))}
              </div>
            ))
          )}
        </div>

        <div className={styles.mainPane}>
          <div className={styles.editorCard}>
            <textarea
              className={styles.sqlBox}
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              placeholder="SELECT * FROM your_table LIMIT 100"
              onKeyDown={(e) => {
                if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') runSql(sql)
              }}
            />
            <div className={styles.editorFooter}>
              <button
                className="primary"
                disabled={running || !sql.trim() || !backend}
                onClick={() => runSql(sql)}
                style={{ display: 'flex', alignItems: 'center', gap: 6 }}
              >
                <Play size={14} /> {running ? 'Running…' : 'Run'}
              </button>
              <span style={{ fontSize: 11.5, color: 'var(--muted)' }}>⌘/Ctrl+Enter to run · capped at 500 rows, 15s timeout</span>
            </div>
          </div>

          <div className={styles.resultCard}>
            {result === null ? (
              <div className={styles.empty}>Run a query or click a table to preview it.</div>
            ) : (
              <>
                <div className={styles.resultMeta}>
                  {result.rowCount} row{result.rowCount === 1 ? '' : 's'}
                  {result.truncated ? ' (truncated at 500)' : ''} · {result.tookMs}ms
                </div>
                <div className={styles.resultScroll}>
                  <table className={styles.resultTable}>
                    <thead>
                      <tr>{result.columns.map((c) => <th key={c}>{c}</th>)}</tr>
                    </thead>
                    <tbody>
                      {result.rows.map((row, i) => (
                        <tr key={i}>
                          {row.map((cell, j) => (
                            <td key={j} className={cell === null ? styles.nullCell : undefined}>
                              {cell === null ? 'null' : String(cell)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
