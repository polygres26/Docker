import { useState } from 'react'

export type CredentialSource = 'plaintext' | 'vault' | 'cyberark' | 'awssm' | 'azurekv' | 'gcpsm'

/**
 * Password input that can also point at an external secret instead of holding a literal value --
 * the value this component produces (`onChange`) is always the one flat string the backend
 * already expects in a password field: a plain password, or a `vault:...`/`cyberark:...`/
 * `awssm:...`/`azurekv:...`/`gcpsm:...` reference (see `com.sayonora.dms.secrets.SecretRef` for
 * the exact grammar and `SecretResolver` for how each is resolved at connect time). No new API
 * shape needed -- this is purely a friendlier way to build the string that already round-trips
 * through every password field.
 */
export default function CredentialField({
  value, onChange, placeholder, existingLabel,
}: {
  value: string
  onChange: (value: string) => void
  placeholder?: string
  existingLabel?: string
}) {
  const initialSource: CredentialSource =
    value.startsWith('vault:') ? 'vault' :
    value.startsWith('cyberark:') ? 'cyberark' :
    value.startsWith('awssm:') ? 'awssm' :
    value.startsWith('azurekv:') ? 'azurekv' :
    value.startsWith('gcpsm:') ? 'gcpsm' :
    'plaintext'
  const [source, setSource] = useState<CredentialSource>(initialSource)
  const [plaintext, setPlaintext] = useState(initialSource === 'plaintext' ? value : '')
  const [vaultPath, setVaultPath] = useState(initialSource === 'vault' ? value.replace(/^vault:/, '').split('#')[0] : 'secret/data/prod/postgres')
  const [vaultField, setVaultField] = useState(initialSource === 'vault' && value.includes('#') ? value.split('#')[1] : 'password')
  const [cyberarkQuery, setCyberarkQuery] = useState(initialSource === 'cyberark' ? value.replace(/^cyberark:/, '') : 'AppID=Warp&Safe=DB-Secrets&Object=prod-postgres')
  const [awsSecretId, setAwsSecretId] = useState(initialSource === 'awssm' ? value.replace(/^awssm:/, '').split('?')[0] : 'prod/postgres-password')
  const [awsRegion, setAwsRegion] = useState(initialSource === 'awssm' ? paramFrom(value, 'region') : '')
  const [azurePath, setAzurePath] = useState(initialSource === 'azurekv' ? value.replace(/^azurekv:/, '').split('?')[0] : 'my-vault/postgres-password')
  const [gcpPath, setGcpPath] = useState(initialSource === 'gcpsm' ? value.replace(/^gcpsm:/, '').split('?')[0] : 'my-project/postgres-password')

  function paramFrom(v: string, key: string): string {
    const q = v.indexOf('?')
    if (q < 0) return ''
    for (const pair of v.slice(q + 1).split('&')) {
      const [k, val] = pair.split('=')
      if (k === key) return val ?? ''
    }
    return ''
  }

  function emit(next: {
    source?: CredentialSource, plaintext?: string, vaultPath?: string, vaultField?: string,
    cyberarkQuery?: string, awsSecretId?: string, awsRegion?: string, azurePath?: string, gcpPath?: string,
  }) {
    const s = next.source ?? source
    const p = next.plaintext ?? plaintext
    const vp = next.vaultPath ?? vaultPath
    const vf = next.vaultField ?? vaultField
    const cq = next.cyberarkQuery ?? cyberarkQuery
    const asi = next.awsSecretId ?? awsSecretId
    const ar = next.awsRegion ?? awsRegion
    const azp = next.azurePath ?? azurePath
    const gp = next.gcpPath ?? gcpPath
    if (s === 'plaintext') onChange(p)
    else if (s === 'vault') onChange(`vault:${vp}#${vf}`)
    else if (s === 'cyberark') onChange(`cyberark:${cq}`)
    else if (s === 'awssm') onChange(`awssm:${asi}${ar ? `?region=${ar}` : ''}`)
    else if (s === 'azurekv') onChange(`azurekv:${azp}`)
    else onChange(`gcpsm:${gp}`)
  }

  function handleSourceChange(next: CredentialSource) {
    setSource(next)
    emit({ source: next })
  }

  return (
    <div>
      <div style={{ display: 'flex', gap: 6, marginBottom: 8, flexWrap: 'wrap' }}>
        {(['plaintext', 'vault', 'cyberark', 'awssm', 'azurekv', 'gcpsm'] as const).map((opt) => (
          <button
            key={opt}
            type="button"
            onClick={() => handleSourceChange(opt)}
            style={{
              padding: '4px 10px', fontSize: 12, borderRadius: 999, cursor: 'pointer',
              border: source === opt ? '1px solid var(--accent)' : '1px solid var(--border)',
              background: source === opt ? 'var(--accent-soft)' : 'none',
              color: source === opt ? 'var(--accent-strong)' : 'var(--muted)',
            }}
          >
            {sourceLabel(opt)}
          </button>
        ))}
      </div>

      {source === 'plaintext' && (
        <input
          type="password"
          value={plaintext}
          placeholder={placeholder ?? (existingLabel ? `leave blank to keep ${existingLabel}` : undefined)}
          onChange={(e) => { setPlaintext(e.target.value); emit({ plaintext: e.target.value }) }}
          style={{ width: '100%', padding: '8px 10px', fontSize: 14 }}
        />
      )}

      {source === 'vault' && (
        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 8 }}>
          <input
            value={vaultPath}
            onChange={(e) => { setVaultPath(e.target.value); emit({ vaultPath: e.target.value }) }}
            placeholder="secret/data/prod/postgres"
            style={{ padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
          />
          <input
            value={vaultField}
            onChange={(e) => { setVaultField(e.target.value); emit({ vaultField: e.target.value }) }}
            placeholder="password"
            style={{ padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
          />
        </div>
      )}

      {source === 'cyberark' && (
        <input
          value={cyberarkQuery}
          onChange={(e) => { setCyberarkQuery(e.target.value); emit({ cyberarkQuery: e.target.value }) }}
          placeholder="AppID=Warp&Safe=DB-Secrets&Object=prod-postgres"
          style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
        />
      )}

      {source === 'awssm' && (
        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 8 }}>
          <input
            value={awsSecretId}
            onChange={(e) => { setAwsSecretId(e.target.value); emit({ awsSecretId: e.target.value }) }}
            placeholder="prod/postgres-password"
            style={{ padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
          />
          <input
            value={awsRegion}
            onChange={(e) => { setAwsRegion(e.target.value); emit({ awsRegion: e.target.value }) }}
            placeholder="region (optional)"
            style={{ padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
          />
        </div>
      )}

      {source === 'azurekv' && (
        <input
          value={azurePath}
          onChange={(e) => { setAzurePath(e.target.value); emit({ azurePath: e.target.value }) }}
          placeholder="my-vault/postgres-password"
          style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
        />
      )}

      {source === 'gcpsm' && (
        <input
          value={gcpPath}
          onChange={(e) => { setGcpPath(e.target.value); emit({ gcpPath: e.target.value }) }}
          placeholder="my-project/postgres-password"
          style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
        />
      )}

      <div style={{ fontSize: 11.5, color: 'var(--muted)', marginTop: 4 }}>
        {source === 'plaintext' && 'Stored as-is. Resolved at connect time either way.'}
        {source === 'vault' && 'Resolved from HashiCorp Vault (KV v2) at connect time -- needs VAULT_ADDR/VAULT_TOKEN set on the server.'}
        {source === 'cyberark' && 'Resolved from CyberArk’s Central Credential Provider at connect time -- needs CYBERARK_CCP_URL set on the server.'}
        {source === 'awssm' && 'Resolved from AWS Secrets Manager at connect time -- uses the AWS SDK’s own default credential chain on the server (env vars, instance role, ...).'}
        {source === 'azurekv' && 'Resolved from Azure Key Vault at connect time -- needs AZURE_TENANT_ID/AZURE_CLIENT_ID/AZURE_CLIENT_SECRET set on the server.'}
        {source === 'gcpsm' && 'Resolved from GCP Secret Manager at connect time -- needs GOOGLE_APPLICATION_CREDENTIALS (a service-account key file) set on the server.'}
      </div>
    </div>
  )
}

function sourceLabel(opt: CredentialSource): string {
  switch (opt) {
    case 'plaintext': return 'Password'
    case 'vault': return 'Vault'
    case 'cyberark': return 'CyberArk'
    case 'awssm': return 'AWS Secrets Manager'
    case 'azurekv': return 'Azure Key Vault'
    case 'gcpsm': return 'GCP Secret Manager'
  }
}
