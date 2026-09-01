import { useEffect, useState } from 'react'
import { type WireConfig, getWireConfig, saveWireConfig } from '../api/client'

/**
 * OAuth / OIDC token verification -- edits the four `warp_config.oauth*` fields
 * AccessContextResolver reloads. Setting an issuer turns on bearer-token auth for the HTTP-facing
 * surfaces (admin API, MCP, DynamoDB wire); leaving it blank keeps OAuth disabled.
 */
export default function OAuth() {
  const [issuer, setIssuer] = useState('')
  const [audience, setAudience] = useState('')
  const [userIdClaim, setUserIdClaim] = useState('')
  const [rolesClaim, setRolesClaim] = useState('')
  const [loaded, setLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getWireConfig()
      .then((s: WireConfig) => {
        setIssuer(s.oauthIssuer ?? '')
        setAudience(s.oauthAudience ?? '')
        setUserIdClaim(s.oauthUserIdClaim ?? '')
        setRolesClaim(s.oauthRolesClaim ?? '')
        setLoaded(true)
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    setMessage(null)
    try {
      const saved = await saveWireConfig({
        oauthIssuer: issuer || null,
        oauthAudience: audience || null,
        oauthUserIdClaim: userIdClaim || null,
        oauthRolesClaim: rolesClaim || null,
      })
      setMessage(`Saved — warp_config version ${saved.version}.`)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ maxWidth: 560 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>OAuth / OIDC</h1>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0, marginBottom: 20 }}>
        Bearer-token verification for Warp's HTTP-facing surfaces. Leave the issuer blank to
        keep OAuth disabled.
      </p>

      {error && (
        <div style={{ marginBottom: 16, color: 'var(--error, crimson)', fontSize: 13 }}>{error}</div>
      )}

      {!loaded && !error ? (
        <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>
      ) : (
        <form onSubmit={handleSave}>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Issuer</div>
            <input type="text" value={issuer} onChange={(e) => setIssuer(e.target.value)}
              placeholder="https://your-tenant.auth0.com/" style={{ width: '100%', padding: '8px 10px', fontSize: 14 }} />
          </label>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Audience</div>
            <input type="text" value={audience} onChange={(e) => setAudience(e.target.value)}
              placeholder="warp" style={{ width: '100%', padding: '8px 10px', fontSize: 14 }} />
          </label>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>User-ID claim</div>
            <input type="text" value={userIdClaim} onChange={(e) => setUserIdClaim(e.target.value)}
              placeholder="sub" style={{ width: '100%', padding: '8px 10px', fontSize: 14 }} />
          </label>
          <label style={{ display: 'block', marginBottom: 16 }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Roles claim</div>
            <input type="text" value={rolesClaim} onChange={(e) => setRolesClaim(e.target.value)}
              placeholder="https://warp/roles" style={{ width: '100%', padding: '8px 10px', fontSize: 14 }} />
          </label>
          <button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
          {message && <span style={{ marginLeft: 12, color: 'var(--success, green)', fontSize: 13 }}>{message}</span>}
        </form>
      )}
    </div>
  )
}
