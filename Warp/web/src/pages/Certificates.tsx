import { useCallback, useState } from 'react'
import { ShieldCheck } from 'lucide-react'
import { type TlsCertificate, getTlsCertificates, renewTlsCertificate } from '../api/client'
import { maturityFor } from '../api/maturity'
import { Button, DataTable, EmptyState, Loading, MaturityTag, Notice, PageHeader, Section, StatusPill, Tag } from '../components/ui'
import { errorText, useLoad } from '../hooks'
import { MaturityLegend } from './interfaces/InterfaceTable'

/** A cert's `listeners` field carries the same NAMEs InterfaceRegistry's TLS_NAMES map uses
 * (e.g. "DYNAMOWIRE", "PUBSUBWIRE_REST", plus non-protocol ones like "ADMIN" that have no
 * maturity concept at all and correctly render nothing) -- not the lowercase wire ids
 * maturity-data.json is keyed by. Recovers the id well enough for maturityFor's own suffix-
 * stripping/normalization to take it from there. */
function maturityForListener(listener: string) {
  return maturityFor(null, listener.toLowerCase().replace(/_/g, '-'))
}

const POLL_MS = 15_000

function daysTone(days: number, placeholder: boolean): 'ok' | 'warn' | 'bad' | 'muted' {
  if (placeholder) return 'bad'
  if (days <= 7) return 'bad'
  if (days <= 30) return 'warn'
  return 'ok'
}

function sourceLabel(c: TlsCertificate): string {
  if (c.placeholder) return 'Temporary (self-signed)'
  if (c.source === 'acme') return 'Automatic (ACME)'
  if (c.source === 'self-signed') return 'Self-signed (dev)'
  if (c.source === 'file') return 'File (WARP_TLS_CERT/KEY)'
  return 'Unavailable'
}

function fmt(iso?: string | null): string {
  if (!iso) return '—'
  try { return new Date(iso).toLocaleString() } catch { return iso }
}

export default function Certificates() {
  const load = useLoad(getTlsCertificates, POLL_MS)
  const [renewing, setRenewing] = useState(false)
  const [renewMsg, setRenewMsg] = useState<string | null>(null)
  const [renewErr, setRenewErr] = useState<string | null>(null)
  const [confirming, setConfirming] = useState(false)

  const doRenew = useCallback(() => {
    setConfirming(false)
    setRenewing(true)
    setRenewMsg(null)
    setRenewErr(null)
    renewTlsCertificate()
      .then((r) => {
        if (r.error) setRenewErr(r.error + (r.retryAt ? ` (try again after ${fmt(r.retryAt)})` : ''))
        else setRenewMsg(r.message ?? 'Renewal started.')
        load.reload()
      })
      .catch((e) => setRenewErr(errorText(e)))
      .finally(() => setRenewing(false))
  }, [load])

  const acme = load.data?.acme
  const certs = load.data?.certificates ?? []

  return (
    <div>
      <PageHeader
        title="Certificates"
        description="TLS certificates serving the admin console, MCP and every HTTPS frontend -- issued automatically by built-in ACME (Let's Encrypt), from files you provided, or a development self-signed certificate."
        actions={acme?.enabled ? (
          confirming ? (
            <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
              <span>Force a renewal now? This counts against the CA's rate limits.</span>
              <Button variant="primary" onClick={doRenew} disabled={renewing}>Yes, renew now</Button>
              <Button variant="secondary" onClick={() => setConfirming(false)}>Cancel</Button>
            </div>
          ) : (
            <Button variant="primary" icon={<ShieldCheck size={16} />} onClick={() => setConfirming(true)} disabled={renewing}>
              Renew now
            </Button>
          )
        ) : undefined}
      />

      {load.loading && !load.data && <Loading />}
      {load.error && <Notice tone="bad">{load.error}</Notice>}
      {renewMsg && <Notice tone="ok">{renewMsg}</Notice>}
      {renewErr && <Notice tone="warn">{renewErr}</Notice>}

      {acme && (
        <Section title="Built-in ACME (Let's Encrypt)" meta={acme.enabled ? <StatusPill tone="ok">Enabled</StatusPill> : <StatusPill tone="muted">Disabled</StatusPill>}>
          {!acme.enabled && (
            <Notice tone="muted">
              ACME is not enabled{acme.reason ? <>: {acme.reason}</> : '.'} Set <code>WARP_ACME_DOMAINS</code>,{' '}
              <code>WARP_ACME_EMAIL</code> and <code>WARP_ACME_TERMS_ACCEPTED=true</code> to turn it on -- see the "Automatic
              certificates (ACME / Let's Encrypt)" section of the docs.
            </Notice>
          )}
          {acme.enabled && (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 16 }}>
              <div><Tag>Domains</Tag><div>{(acme.domains ?? []).join(', ')}</div></div>
              <div><Tag>Directory</Tag><div>{acme.directory}{acme.staging ? ' (staging)' : ''}</div></div>
              <div><Tag>Challenge</Tag><div>{acme.challenge}{acme.dnsProvider ? ` (${acme.dnsProvider})` : ''}</div></div>
              <div><Tag>Renew at</Tag><div>{acme.renewDays} days left</div></div>
              <div><Tag>Key type</Tag><div>{acme.keyType}</div></div>
              <div><Tag>Shared certificate</Tag><div>{acme.shared ? 'Yes (control-plane Postgres)' : 'No (this node only)'}</div></div>
              <div><Tag>Last renewal</Tag><div>{fmt(acme.lastRenewal)}</div></div>
              <div><Tag>Next check</Tag><div>{fmt(acme.nextCheck)}</div></div>
              <div><Tag>Status</Tag><div>{acme.issuing ? 'Issuing now…' : (acme.lastOutcome ?? '—')}</div></div>
            </div>
          )}
          {acme.enabled && acme.lastError && (
            <Notice tone="bad">
              Last attempt failed: {acme.lastError}. The current certificate keeps serving until a renewal succeeds.
              {acme.backoffUntil && <> Next automatic attempt after {fmt(acme.backoffUntil)}.</>}
            </Notice>
          )}
          {acme.enabled && acme.httpError && (
            <Notice tone="bad">The http-01 challenge listener could not start: {acme.httpError}. Use WARP_ACME_CHALLENGE=dns-01 instead, or fix the port.</Notice>
          )}
        </Section>
      )}

      <Section title="Certificates in use">
        {certs.length === 0 ? (
          <EmptyState title="No HTTPS listeners configured">Set WARP_ACME_DOMAINS, WARP_TLS_CERT/KEY, WARP_TLS_KEYSTORE, or WARP_TLS_SELF_SIGNED=true for development.</EmptyState>
        ) : (
          <DataTable caption="Certificates serving Warp's HTTPS listeners">
            <thead>
              <tr>
                <th>Listeners</th><th>Maturity</th><th>Source</th><th>Domain names</th><th>Issuer</th><th>Expires</th><th>Days left</th><th>Error</th>
              </tr>
            </thead>
            <tbody>
              {certs.map((c) => (
                <tr key={c.listeners + c.sha256}>
                  <td>{c.listeners.split(',').map((l) => <Tag key={l}>{l}</Tag>)}</td>
                  <td>{c.listeners.split(',').map((l) => <MaturityTag key={l} maturity={maturityForListener(l)} />)}</td>
                  <td>{sourceLabel(c)}</td>
                  <td>{c.domainNames.join(', ') || '—'}</td>
                  <td>{c.issuer}</td>
                  <td>{fmt(c.notAfter)}</td>
                  <td><StatusPill tone={daysTone(c.daysLeft, c.placeholder)}>{c.placeholder ? 'placeholder' : `${c.daysLeft}d`}</StatusPill></td>
                  <td>{c.lastError ? <Notice tone="warn">{c.lastError}</Notice> : '—'}</td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </Section>
      {certs.length > 0 && <MaturityLegend />}
    </div>
  )
}
