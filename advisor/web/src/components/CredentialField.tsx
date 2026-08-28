import { useState } from 'react'

export type CredentialSource = 'plaintext' | 'vault' | 'cyberark'

/**
 * Password input that can also point at an external secret instead of holding a literal value --
 * the value this component produces (`onChange`) is always the one flat string the backend
 * already expects in a password field: a plain password, or a `vault:...`/`cyberark:...`
 * reference (see `com.nexagres.advisor.secrets.SecretRef` / PolyWire's twin package for the exact
 * grammar and how it's resolved at connect time). No new API shape needed -- this is purely a
 * friendlier way to build the string that already round-trips through every password field.
 */
export default function CredentialField({
  value, onChange, placeholder, existingLabel,
}: {
  value: string
  onChange: (value: string) => void
  placeholder?: string
  existingLabel?: string
}) {
  const initialSource: CredentialSource = value.startsWith('vault:') ? 'vault' : value.startsWith('cyberark:') ? 'cyberark' : 'plaintext'
  const [source, setSource] = useState<CredentialSource>(initialSource)
  const [plaintext, setPlaintext] = useState(initialSource === 'plaintext' ? value : '')
  const [vaultPath, setVaultPath] = useState(initialSource === 'vault' ? value.replace(/^vault:/, '').split('#')[0] : 'secret/data/prod/postgres')
  const [vaultField, setVaultField] = useState(initialSource === 'vault' && value.includes('#') ? value.split('#')[1] : 'password')
  const [cyberarkQuery, setCyberarkQuery] = useState(initialSource === 'cyberark' ? value.replace(/^cyberark:/, '') : 'AppID=PolyWire&Safe=DB-Secrets&Object=prod-postgres')

  function emit(nextSource: CredentialSource, nextPlaintext: string, nextVaultPath: string, nextVaultField: string, nextCyberarkQuery: string) {
    if (nextSource === 'plaintext') onChange(nextPlaintext)
    else if (nextSource === 'vault') onChange(`vault:${nextVaultPath}#${nextVaultField}`)
    else onChange(`cyberark:${nextCyberarkQuery}`)
  }

  function handleSourceChange(next: CredentialSource) {
    setSource(next)
    emit(next, plaintext, vaultPath, vaultField, cyberarkQuery)
  }

  return (
    <div>
      <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
        {(['plaintext', 'vault', 'cyberark'] as const).map((opt) => (
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
            {opt === 'plaintext' ? 'Password' : opt === 'vault' ? 'Vault' : 'CyberArk'}
          </button>
        ))}
      </div>

      {source === 'plaintext' && (
        <input
          type="password"
          value={plaintext}
          placeholder={placeholder ?? (existingLabel ? `leave blank to keep ${existingLabel}` : undefined)}
          onChange={(e) => { setPlaintext(e.target.value); emit('plaintext', e.target.value, vaultPath, vaultField, cyberarkQuery) }}
          style={{ width: '100%', padding: '8px 10px', fontSize: 14 }}
        />
      )}

      {source === 'vault' && (
        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 8 }}>
          <input
            value={vaultPath}
            onChange={(e) => { setVaultPath(e.target.value); emit('vault', plaintext, e.target.value, vaultField, cyberarkQuery) }}
            placeholder="secret/data/prod/postgres"
            style={{ padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
          />
          <input
            value={vaultField}
            onChange={(e) => { setVaultField(e.target.value); emit('vault', plaintext, vaultPath, e.target.value, cyberarkQuery) }}
            placeholder="password"
            style={{ padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
          />
        </div>
      )}

      {source === 'cyberark' && (
        <input
          value={cyberarkQuery}
          onChange={(e) => { setCyberarkQuery(e.target.value); emit('cyberark', plaintext, vaultPath, vaultField, e.target.value) }}
          placeholder="AppID=PolyWire&Safe=DB-Secrets&Object=prod-postgres"
          style={{ width: '100%', padding: '8px 10px', fontSize: 13, fontFamily: 'monospace' }}
        />
      )}

      <div style={{ fontSize: 11.5, color: 'var(--muted)', marginTop: 4 }}>
        {source === 'plaintext' && 'Stored as-is. Resolved at connect time either way.'}
        {source === 'vault' && 'Resolved from HashiCorp Vault (KV v2) at connect time -- needs VAULT_ADDR/VAULT_TOKEN set on the server.'}
        {source === 'cyberark' && 'Resolved from CyberArk’s Central Credential Provider at connect time -- needs CYBERARK_CCP_URL set on the server.'}
      </div>
    </div>
  )
}
