import { useEffect, useState } from 'react'
import { Split } from 'lucide-react'
import {
  type AbCompareEntry, type AbPolicy, type AbState, type AbStats,
  clearAbKillSwitch, deleteAbPolicy, getAbCompare, getAbRouting, getAbStats, putAbPolicy, setAbKillSwitch,
} from '../api/client'
import { Button, DataTable, EmptyState, KpiStrip, Loading, Notice, PageHeader, Section, StatusPill, Tag } from '../components/ui'

const STORES = ['s3', 'dynamodb', 'sqs'] as const
const REFRESH_MS = 5000

function PolicyRow({ store, policy, targets, onChange }: {
  store: string; policy: AbPolicy | undefined; targets: string[]; onChange: (s: AbState) => void
}) {
  const [mode, setMode] = useState<AbPolicy['mode']>(policy?.mode ?? 'local')
  const [pct, setPct] = useState(policy?.cloudPercent ?? 0)
  const [owner, setOwner] = useState<'local' | 'cloud'>(policy?.writeOwner ?? 'local')
  const [dual, setDual] = useState(policy?.dualWrite ?? false)
  const [target, setTarget] = useState(policy?.target ?? targets[0] ?? '')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function save() {
    setBusy(true)
    setError(null)
    try {
      // rules, sticky key, compare settings and role overrides are kept as they are
      onChange(await putAbPolicy(store, { ...(policy ?? {}), mode, cloudPercent: pct, writeOwner: owner, dualWrite: dual, target: target || null }))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  async function remove() {
    setBusy(true)
    try {
      onChange(await deleteAbPolicy(store))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <tr>
      <td className="mono">{store}</td>
      <td>
        <select value={mode} onChange={(e) => setMode(e.target.value as AbPolicy['mode'])} aria-label={`${store} mode`}>
          <option value="local">local</option>
          <option value="cloud">cloud</option>
          <option value="split">split</option>
          <option value="compare">compare</option>
        </select>
      </td>
      <td>
        <input type="number" min={0} max={100} step={1} value={pct} disabled={mode !== 'split'} style={{ width: 70 }}
          onChange={(e) => setPct(Number(e.target.value))} aria-label={`${store} cloud percent`} /> %
      </td>
      <td>
        <select value={owner} onChange={(e) => setOwner(e.target.value as 'local' | 'cloud')} aria-label={`${store} write owner`}>
          <option value="local">local</option>
          <option value="cloud">cloud</option>
        </select>
      </td>
      <td title="Off by default. Dual-write sends every write to both sides; if one fails the two DRIFT.">
        <label><input type="checkbox" checked={dual} onChange={(e) => setDual(e.target.checked)} /> dual-write</label>
      </td>
      <td>
        <select value={target} onChange={(e) => setTarget(e.target.value)} aria-label={`${store} cloud target`}>
          {targets.map((t) => <option key={t} value={t}>{t}</option>)}
          {targets.length === 0 && <option value="">(no target configured)</option>}
        </select>
      </td>
      <td style={{ textAlign: 'right' }}>
        <Button onClick={save} disabled={busy}>Apply</Button>{' '}
        {policy && <Button onClick={remove} disabled={busy}>Reset</Button>}
        {error && <Notice tone="bad">{error}</Notice>}
      </td>
    </tr>
  )
}

/**
 * A/B routing between the real cloud service and Warp's local emulation for the AWS-family frontends
 * (s3wire, dynamowire, sqswire, awswire). Policies apply on every node immediately; the kill switch sets every store
 * to one side. Cloud credentials are configured through the API and never shown here. The compare list is the
 * ring buffer of THIS node.
 */
export default function AbRouting() {
  const [state, setState] = useState<AbState | null>(null)
  const [stats, setStats] = useState<AbStats | null>(null)
  const [entries, setEntries] = useState<AbCompareEntry[]>([])
  const [onlyDiff, setOnlyDiff] = useState(true)
  const [error, setError] = useState<string | null>(null)

  async function refresh() {
    try {
      const [s, st, c] = await Promise.all([getAbRouting(), getAbStats(), getAbCompare(onlyDiff)])
      setState(s)
      setStats(st)
      setEntries(c.entries)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, REFRESH_MS)
    return () => clearInterval(id)
  }, [onlyDiff])

  async function kill(side: 'local' | 'cloud') {
    if (!confirm(`Send ALL AWS-family traffic to the ${side.toUpperCase()} side now? Every policy and rule is overridden until you lift it.`)) return
    try {
      setState(await setAbKillSwitch(side, 'admin UI'))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  async function lift() {
    try {
      setState(await clearAbKillSwitch())
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  const targets = state?.targets.map((t) => t.name) ?? []
  const compared = stats ? Object.values(stats.compare).reduce((n, c) => n + c.compared, 0) : 0
  const differ = stats ? Object.values(stats.compare).reduce((n, c) => n + c.differ, 0) : 0

  return (
    <div>
      <PageHeader title="A/B routing"
        description={<>Serve each request from Warp's local emulation, the real cloud service, or both (compare). Clients keep pointing at Warp. Writes in a split go to ONE side, the write owner; dual-write is opt-in and drifts if either side fails.</>} />
      {error && <Notice tone="bad">{error}</Notice>}
      {!state && !error && <Loading />}
      {state && (
        <>
          {state.killSwitch
            ? <Notice tone="warn">Kill switch ACTIVE: everything is going to the <b>{state.killSwitch.side}</b> side{state.killSwitch.reason ? ` (${state.killSwitch.reason})` : ''}. <Button onClick={lift}>Lift</Button></Notice>
            : <Section title="Kill switch" meta="Effective immediately on every node, no restart">
                <Button variant="danger" onClick={() => kill('local')}>Everything to LOCAL</Button>{' '}
                <Button variant="danger" onClick={() => kill('cloud')}>Everything to CLOUD</Button>
              </Section>}
          {!state.secretsEncryptedAtRest && <Notice tone="warn">SAYONORA_ENCRYPTION_KEY is not set: cloud secrets cannot be stored (use default-chain or web-identity targets).</Notice>}
          <KpiStrip label="A/B routing" items={[
            { label: 'Config version', value: state.version },
            { label: 'Cloud targets', value: targets.length },
            { label: 'Compared (this node)', value: compared.toLocaleString() },
            { label: 'Differences', value: differ.toLocaleString() },
          ]} />
          <Section flush title="Policies" meta="Per store; rules by access key, IP or header are edited through the API">
            <DataTable caption="A/B routing policies" minWidth={880}>
              <thead>
                <tr><th>Store</th><th>Mode</th><th>Cloud share</th><th>Write owner</th><th>Dual-write</th><th>Target</th><th aria-label="Actions"></th></tr>
              </thead>
              <tbody>
                {STORES.map((s) => (
                  <PolicyRow key={`${s}:${state.version}`} store={s} policy={state.policies[s]} targets={targets} onChange={setState} />
                ))}
              </tbody>
            </DataTable>
          </Section>
          {stats && Object.keys(stats.sides).length > 0 && (
            <Section flush title="Per side" meta="This node">
              <DataTable caption="Requests per store and side" minWidth={560}>
                <thead><tr><th>Store / side</th><th style={{ textAlign: 'right' }}>Requests</th><th style={{ textAlign: 'right' }}>Errors</th><th style={{ textAlign: 'right' }}>Avg ms</th></tr></thead>
                <tbody>
                  {Object.entries(stats.sides).sort().map(([k, v]) => (
                    <tr key={k}>
                      <td className="mono">{k.replace('|', ' / ')}</td>
                      <td className="num">{v.requests}</td>
                      <td className="num">{v.errors} ({(v.errorRate * 100).toFixed(1)}%)</td>
                      <td className="num">{v.avgMs.toFixed(1)}</td>
                    </tr>
                  ))}
                </tbody>
              </DataTable>
            </Section>
          )}
          <Section flush title="Compare results" meta={<label><input type="checkbox" checked={onlyDiff} onChange={(e) => setOnlyDiff(e.target.checked)} /> only differences (ring buffer of this node)</label>}>
            {entries.length === 0
              ? <EmptyState icon={<Split size={18} aria-hidden="true" />} title="Nothing compared yet">
                  Set a store to <code>compare</code>: reads go to both sides, the primary's answer is returned and differences are listed here.
                </EmptyState>
              : <DataTable caption="Compare results" minWidth={900}>
                  <thead><tr><th>When</th><th>Store</th><th>Operation</th><th>Client</th><th>Status L/C</th><th style={{ textAlign: 'right' }}>ms L/C</th><th>Result</th></tr></thead>
                  <tbody>
                    {entries.map((e, i) => (
                      <tr key={`${e.ts}-${i}`}>
                        <td className="mono">{new Date(e.ts).toLocaleTimeString()}</td>
                        <td className="mono">{e.store}</td>
                        <td>{e.op} <Tag>{e.primary} primary</Tag></td>
                        <td className="mono">{e.client}</td>
                        <td className="num">{e.localStatus} / {e.cloudStatus}</td>
                        <td className="num">{e.localMs} / {e.cloudMs}</td>
                        <td>{e.equal ? <StatusPill tone="ok">same</StatusPill> : <div><StatusPill tone="bad">differs</StatusPill><ul>{e.diffs.map((d, j) => <li key={j} className="mono">{d}</li>)}</ul></div>}</td>
                      </tr>
                    ))}
                  </tbody>
                </DataTable>}
          </Section>
        </>
      )}
    </div>
  )
}
