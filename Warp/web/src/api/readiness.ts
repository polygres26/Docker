import type { InterfaceInfo } from './client'
import { policyCount, policyOn } from './client'
import { maturityFor, type Maturity } from './maturity'
import type { Tone } from '../components/ui'

/**
 * The shared readiness evaluator: one place that turns an interface's already-scattered real
 * fields (auth*, tls*, status, policies -- all server data, see InterfaceRegistry.toJson /
 * AccessSummary / PolicySummary) plus its editorial maturity tier (maturity.ts) into a single,
 * consistent verdict. Before this, three independent computations existed for pieces of this
 * (AuthCell, StatusCell, and Workloads.tsx's own policy booleans) with no single place answering
 * the actual question an operator asks: "is this interface actually ready?" See the shared-
 * readiness-evaluator scoping notes for the fuller design and why maturity stays client-side.
 *
 * Every consumer (Interfaces tables, Workloads, a future Overview/Security aggregate) should call
 * `readinessOf` instead of inspecting `authEnforced`/`tlsEnabled`/`status` directly -- exactly the
 * same "one real join, not N reimplementations" reasoning that produced AccessSummary and
 * PolicySummary server-side.
 */

export type AuthStatus = 'enforced' | 'open' | 'not_reported'
export type EncryptionStatus = 'enabled' | 'disabled' | 'error' | 'not_reported'
export type BackendStatus = 'configured' | 'not_configured' | 'n/a'
export type Overall = 'production_ready' | 'needs_attention' | 'not_ready'

export interface ReadinessDimensions {
  auth: AuthStatus
  encryption: EncryptionStatus
  backend: BackendStatus
  /** Raw policy tags from PolicySummary (e.g. ["router","qos","acl:0"]), shown as-is -- policy
   * presence/absence is informational on this page, not judged good/bad by `overall` below (an
   * interface with no ACL rule isn't necessarily insecure; that's a deployment choice, unlike an
   * auth method that reports itself unenforced). */
  policies: string[]
  maturity: Maturity | null
}

export interface Readiness {
  overall: Overall
  dimensions: ReadinessDimensions
}

export function authStatusOf(i: InterfaceInfo): AuthStatus {
  if (i.authMethod === undefined) return 'not_reported'
  return i.authEnforced ? 'enforced' : 'open'
}

export function encryptionStatusOf(i: InterfaceInfo): EncryptionStatus {
  if (i.tlsEnabled === undefined) return 'not_reported'
  if (i.tlsError) return 'error'
  return i.tlsEnabled ? 'enabled' : 'disabled'
}

/** `'n/a'` for a frontend with no store concept at all (every SQL driver) -- readiness never
 * penalizes an interface for a dimension that doesn't apply to it. */
export function backendStatusOf(i: InterfaceInfo): BackendStatus {
  if (!i.store) return 'n/a'
  return i.status === 'listening_no_store' ? 'not_configured' : 'configured'
}

const BAD = new Set<string>(['open', 'disabled', 'error', 'not_configured'])
const SOFT = new Set<string>(['not_reported'])

/**
 * The rollup: any dimension actually confirmed BAD (open auth, disabled/errored TLS, a store not
 * configured) is `not_ready` for a Preview/Experimental/unclassified protocol -- one still settling
 * or with disclosed gaps shouldn't also get the benefit of the doubt on top of that. The SAME bad
 * dimension on a Production/Certified protocol (real, verified, at worst with disclosed gaps) is
 * `needs_attention` instead -- a real problem, but the underlying protocol itself is trustworthy
 * enough that "go fix this one thing" is the honest framing, not "this is broadly unproven."
 * A dimension that's merely unknown (`not_reported`) never alone produces `not_ready` -- absence of
 * information isn't confirmation of a problem -- but does prevent a clean `production_ready`.
 */
export function readinessOf(i: InterfaceInfo): Readiness {
  const auth = authStatusOf(i)
  const encryption = encryptionStatusOf(i)
  const backend = backendStatusOf(i)
  const maturity = maturityFor(i.metricsKey, i.id)
  const values = [auth, encryption, backend]

  const anyBad = values.some((v) => BAD.has(v))
  const anySoft = values.some((v) => SOFT.has(v))
  const matureProtocol = maturity === 'Certified' || maturity === 'Production'

  let overall: Overall
  if (anyBad) {
    overall = matureProtocol ? 'needs_attention' : 'not_ready'
  } else if (anySoft) {
    overall = 'needs_attention'
  } else {
    overall = 'production_ready'
  }

  return {
    overall,
    dimensions: { auth, encryption, backend, maturity, policies: i.policies ?? [] },
  }
}

export const OVERALL_LABEL: Record<Overall, string> = {
  production_ready: 'Production ready',
  needs_attention: 'Needs attention',
  not_ready: 'Not ready',
}

export const OVERALL_TONE: Record<Overall, Tone> = {
  production_ready: 'ok',
  needs_attention: 'warn',
  not_ready: 'bad',
}

/** Re-exported so a consumer building a custom readiness summary (e.g. a per-set or per-page
 * aggregate) doesn't need to import both this module and client.ts separately for policy tags. */
export { policyCount, policyOn }
