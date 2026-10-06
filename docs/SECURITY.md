# Warp — Security: Threat Model and Vulnerability Disclosure Policy

> **This is a technical/internal reference** for operators, contributors, and anyone evaluating
> Warp's security posture before deploying it. For deployment guidance, see
> [`WARP_GUIDE.md`](WARP_GUIDE.md); for the compatibility/positioning context this document is
> part of, see [`COMPETITIVE_POSITIONING_ROADMAP.md`](COMPETITIVE_POSITIONING_ROADMAP.md) (Phase H).

Every claim below is derived from reading the current implementation, cited by file and line, not
from a generic template. Where a real gap exists, it is stated as a gap — this document's job is
to let an operator make an informed decision, not to make Warp look more hardened than it is.

---

## 1. Trust boundaries

Warp sits between untrusted-to-semi-trusted clients (application traffic on its wire protocols)
and trusted backends (the real Postgres/Oracle/MySQL/SQL Server databases it proxies to). The
threat model below is organized around the boundaries a real attacker would actually hit, in the
order they'd hit them: network admission → authentication → authorization/data-scoping →
backend credential handling → cluster/peer trust → the admin surface.

### 1.1 Network admission — `ClientAcl` (IP/CIDR)

[`acl/ClientAcl.java`](../Warp/src/main/java/com/sayonora/warp/acl/ClientAcl.java) — CIDR
allow/deny rules from `WARP_ACL_RULES`. **Default posture: open.** With no rules configured,
every source IP is accepted (`isAllowed()` returns `true` on an empty rule list). Once any rule
exists, the default flips to deny for anything no rule matches. This is IP-based network
admission, not identity — it answers "should this network connection even reach the protocol
handler," before any credential is checked.

### 1.2 SQL wire protocols — per-protocol identity

| Protocol | Mechanism | Verified where |
|---|---|---|
| pgwire | SCRAM-SHA-256 or MD5 | [`auth/PostgresPasswordVerifier.java:17-70`](../Warp/src/main/java/com/sayonora/warp/auth/PostgresPasswordVerifier.java) — SCRAM path uses PBKDF2-HMAC-SHA256 with a constant-time compare |
| mywire, mssqlwire, orawire (translated/emulated mode) | Shared credential list, single or multi-user | [`auth/CredentialStore.java:12-122`](../Warp/src/main/java/com/sayonora/warp/auth/CredentialStore.java) — `username=password;...` spec, or a real migrated password sourced from an external vault |
| mongowire | SASL/SCRAM-SHA-256 | [`mongowire/auth/MongoScramConversation.java`](../Warp/src/main/java/com/sayonora/warp/mongowire/auth/MongoScramConversation.java) — **disclosed gap in the class's own javadoc (lines 24-30): there is no persisted per-user SCRAM verifier at rest**; Warp only holds `CredentialStore`'s plaintext password and generates a fresh salt per login rather than looking one up. Functionally correct SCRAM, but not the same at-rest guarantee a real MongoDB deployment gives. |

### 1.3 AWS-family emulator protocols — SigV4

[`awswire/AwsSigV4.java:20-28`](../Warp/src/main/java/com/sayonora/warp/awswire/AwsSigV4.java),
verified per-request in [`awswire/AwsHttp.java:56`](../Warp/src/main/java/com/sayonora/warp/awswire/AwsHttp.java).
Credentials come from `WARP_AWS_IAM_CREDENTIALS`
([`awswire/AwsConfig.java:9`](../Warp/src/main/java/com/sayonora/warp/awswire/AwsConfig.java)).
**Disclosed gap: if `WARP_AWS_IAM_CREDENTIALS` is unset, requests are not signature-checked at
all** (this is surfaced today as a security-findings item in the admin UI's Access & ACLs page,
not hidden).

### 1.4 Azure and Cosmos emulator protocols — HMAC / master key

Both are real, enforced, cryptographic checks, living in a sibling class to the wire-protocol
dispatcher rather than inline in it:

- **Azure Blob/Queue/Table**: [`azurewire/AzureAuth.java`](../Warp/src/main/java/com/sayonora/warp/azurewire/AzureAuth.java)
  verifies `SharedKey`/`SharedKeyLite` (HMAC-SHA256 over the canonicalized request) or a Bearer
  token, called from each service's own request path (`BlobService`/`QueueService`/`TableService`).
  `AzureWireServer.serve()` itself only gates on `ConnectionGate` (network-level) and account
  existence before dispatch — the actual signature check happens one layer down, in the service
  classes.
- **Cosmos**: [`cosmoswire/CosmosAuth.java`](../Warp/src/main/java/com/sayonora/warp/cosmoswire/CosmosAuth.java),
  invoked from [`cosmoswire/CosmosHttp.java:148`](../Warp/src/main/java/com/sayonora/warp/cosmoswire/CosmosHttp.java)
  (`auth.check(...)`) — the emulator's well-known master key (or a real configured one) is
  verified per request, not merely accepted.

### 1.5 HTTP/admin surface — OAuth/OIDC + shared admin token

[`http/auth/AccessContextResolver.java`](../Warp/src/main/java/com/sayonora/warp/http/auth/AccessContextResolver.java) —
real JWKS discovery/refresh (`WARP_OAUTH_JWKS_REFRESH_SECONDS`, default 300s), issuer/audience/exp
validation. **Design choice, not a bug: when `WARP_OAUTH_ISSUER` is unset, `enforce()` returns
`AccessContext.ANONYMOUS`** (lines 149-151) rather than rejecting — the feature is off, not
silently broken. This matters because several AWS-family/MCP/A2A frontends accept this anonymous
context when no issuer is configured (a genuine "no identity enforced" state an operator must
opt into avoiding by setting `WARP_OAUTH_ISSUER`).

The admin API (`MetricsServer`) is the one surface that stays **fail-closed** even with OAuth
unset: [`MetricsServer.java:1053-1063`](../Warp/src/main/java/com/sayonora/warp/http/admin/MetricsServer.java)
(`bearerTokenValid()`) returns `false` unconditionally when `WARP_ADMIN_TOKEN` is unset — "unset
means this whole API surface is disabled by the caller never being able to authenticate, not
silently open" (the code's own comment). Combined with `resolveAdminRole()`'s explicit
default-deny (lines 967-981) for an anonymous/role-less caller, the admin API is locked out by
default, not open by default — the one documented exception is `GET /metrics` (Prometheus scrape),
which has no `authorized()` gate at all by design (scrapers don't carry admin roles), but can be
disabled process-wide via `PATCH /api/observability`.

### 1.6 Row/column data-scoping — `AccessControlStage`

[`core/AccessControlStage.java:51-104`](../Warp/src/main/java/com/sayonora/warp/core/AccessControlStage.java) —
**explicitly fail-closed**: a row-filter-restricted table referenced by a caller whose
`AccessContext` lacks the required attribute is rejected (`ERR_ACCESS_MISSING_ATTRIBUTE`, SQLSTATE
`42501`), not silently left unfiltered (line 62-65's own comment: "fail-closed, not unfiltered").
Column grants either mask to `NULL` or reject, per policy. An empty policy is a pure no-op
pass-through — see [`ACCESSPOLICY authoring`](../Warp/src/main/java/com/sayonora/warp/core/access/AccessPolicy.java)
for how a policy is authored.

### 1.7 SQL firewall — `FirewallStage`

[`core/FirewallStage.java:94-118`](../Warp/src/main/java/com/sayonora/warp/core/FirewallStage.java) —
**default-allow**: evaluates rules by priority, and if no configured rule matches a statement, the
statement proceeds (there is no implicit deny-by-default the way `ClientAcl` has once rules
exist). Firewall enforcement is opt-in per rule, not a default-deny gate. This is a real, distinct
posture from `AccessControlStage`'s fail-closed row/column enforcement above — an operator relying
on the firewall for a default-deny SQL policy needs to author an explicit catch-all deny rule
themselves; the stage does not do this for them.

### 1.8 Backend credential storage — `FieldCipher`

[`secrets/FieldCipher.java`](../Warp/src/main/java/com/sayonora/warp/secrets/FieldCipher.java) —
AES-256-GCM per value, never as a whole-blob wrapper (so `warp_config.payload` stays a real jsonb document).

**What is encrypted, where:**

| Stored in | Secret |
|---|---|
| `warp_config` (every version) | `backends` (the whole spec: each backend's and replica's password), `awsIamCredentials`, `llmApiKey` ([`config/ConfigStore.java`](../Warp/src/main/java/com/sayonora/warp/config/ConfigStore.java)) |
| `warp_config.mcpUpstreams` (JSON inside the config) | each upstream's `bearerToken`, `clientSecret`, `accessToken`, `refreshToken` |
| `warp_xa_log.backend_password` | the password of an in-doubt XA branch's backend |
| `warp_acme_state.value` | the ACME account and certificate state |

(Earlier revisions of this document said "exactly 3 fields"; the MCP upstream, XA log and ACME values are encrypted with the same cipher.)
Backend passwords can also be `vault:` / `cyberark:` references, in which case the config holds the reference and not the secret.

**Keys and format.** `SAYONORA_ENCRYPTION_KEY` is the active key (base64, 32 raw bytes: `openssl rand -base64 32`) and the only one ever used to
encrypt. Each key has an id (the first 4 bytes of its SHA-256, hex; not secret). A stored value is `encv2:<keyId>:<base64(iv||ciphertext)>`, so it names
the key it needs. The older `encv1:<base64>` (no key id) is still read, by trying the active key and then the previous ones; plaintext passes through
`decrypt()` unchanged.

**Fail closed (opt-in).** By default an unset key stores the value in **plaintext** with one logged warning, which keeps existing deployments working.
`WARP_REQUIRE_ENCRYPTION_KEY=true` turns that into an error: Warp refuses to start without a valid key and never writes a secret in the clear. A malformed
key (not base64, not 32 bytes) is refused at startup either way.

**Rotation.** `SAYONORA_ENCRYPTION_KEY_PREVIOUS` is a comma-separated list of older keys, used to decrypt only.
1. Start Warp with the new key as `SAYONORA_ENCRYPTION_KEY` and the old one in `SAYONORA_ENCRYPTION_KEY_PREVIOUS`.
2. `GET /api/security/encryption` (any admin role) shows the active key id, the previous key ids and, per stored secret, which key protects it
   (`needsRotation` is true while anything is under another key, in `encv1`, or in plaintext).
3. `POST /api/security/encryption/rotate` (admin role) re-encrypts everything under the active key: a new `warp_config` version for the three config
   fields and the MCP upstream tokens, the XA log passwords and the ACME state. It is idempotent, and a second call changes nothing. It also encrypts values
   that were stored in plaintext before a key existed.
4. When the status shows nothing under the old key, remove it from `SAYONORA_ENCRYPTION_KEY_PREVIOUS`.

Starting with a key that does not open a stored value fails at startup and names the missing key id ("encrypted with key d370c30d, which is not configured:
set it as SAYONORA_ENCRYPTION_KEY or list it in SAYONORA_ENCRYPTION_KEY_PREVIOUS"). **Older `warp_config` versions are immutable history** and keep the key
they were written with, so the old key is still needed to read them (for example to roll back to one); keep it in `..._PREVIOUS` for as long as you keep those
versions. Verified live (`SecretsRotationLiveTest`): key A, then B active with A previous, rotate, then B alone starts; A alone is refused with the message
above; `WARP_REQUIRE_ENCRYPTION_KEY=true` without a key is refused.

**Still true:** the key lives in the process environment, so anyone who can read the environment of a running Warp or the secrets store that feeds it has
it; there is no HSM/KMS integration; the failover rejoin command (`WARP_FAILOVER_REJOIN_COMMAND`) is given the current primary's password in its environment
(`PGPASSWORD`), because the rebuild tools need it; and rotation does not rewrite old config versions.

### 1.9 Cluster and peer trust — three independent TLS domains

Deliberately kept separate (each independently configurable/revocable), per the code's own
comments:

1. **Client-facing SQL/gRPC TLS** — [`server/TlsSupport.java:18-32`](../Warp/src/main/java/com/sayonora/warp/server/TlsSupport.java),
   covers gRPC/orawire/pgwire/mywire/mssqlwire.
2. **Ignite/cache-cluster peer TLS** — [`cluster/WarpCluster.java:192-197`](../Warp/src/main/java/com/sayonora/warp/cluster/WarpCluster.java).
3. **Query-execution peer gRPC mTLS** (the parallel-join remote-partition RPC) —
   [`grpc/WarpPeerGrpcServer.java:16-60`](../Warp/src/main/java/com/sayonora/warp/grpc/WarpPeerGrpcServer.java):
   TLS-only, no plaintext fallback, `ClientAuth.REQUIRE`. A separate keystore/env-var pair from
   both of the above by explicit design ("never conflated," per the class javadoc).

### 1.10 Dependency/supply-chain surface

`pom.xml` pins every dependency to an exact version (no open ranges found across ~50+
declarations). **No automated dependency-vulnerability scanning is configured in the build** — no
OWASP dependency-check, Snyk, or SBOM (CycloneDX) plugin exists in `pom.xml` today. This is a real
gap for a project claiming enterprise readiness and is the first concrete item in §3's backlog.

---

## 2. Summary: fail-closed vs. fail-open, by surface

| Surface | Default when unconfigured | Fail behavior |
|---|---|---|
| `ClientAcl` (network) | Open (allow all) | Deny-by-default only once a rule exists |
| pgwire/mongowire/AWS-family credential check | N/A — credential always required when configured | Rejects on mismatch |
| AWS SigV4 | **Unsigned requests accepted** if `WARP_AWS_IAM_CREDENTIALS` unset | — |
| OAuth/OIDC (`AccessContextResolver`) | Anonymous accepted if `WARP_OAUTH_ISSUER` unset | — |
| Admin API (`MetricsServer`) | **Locked out** if `WARP_ADMIN_TOKEN`/OAuth unset | Fail-closed |
| `AccessControlStage` (row/column) | No-op if no policy configured; **fail-closed** once a policy exists and an attribute is missing | Fail-closed |
| `FirewallStage` | **Allow** if no rule matches | Fail-open by design; opt-in deny rules only |
| `FieldCipher` (secrets at rest) | **Plaintext** if `SAYONORA_ENCRYPTION_KEY` unset | Fail-open by default, with a logged warning; `WARP_REQUIRE_ENCRYPTION_KEY=true` makes it fail-closed |

The operational takeaway: Warp's identity/authorization layers default toward being explicitly
turned on by an operator, and the admin API and row/column policy engine fail closed once
configured — but the SQL firewall and secrets-at-rest encryption both fail open when not
configured, and AWS SigV4 fails open when credentials aren't set. None of this is hidden inside
the code, but it has never been collected into one place before this document.

---

## 3. Known gaps backlog (prioritized)

1. **No dependency-vulnerability scanning in CI/build** (§1.10) — add an OWASP dependency-check or
   equivalent Maven plugin; smallest-effort, highest-leverage item here.
2. **No `SAYONORA_ENCRYPTION_KEY` rotation mechanism** (§1.8) — real risk for any deployment that
   needs to rotate a compromised key; needs a re-encrypt-in-place migration path.
3. **MongoDB SCRAM has no persisted per-user verifier at rest** (§1.2) — a structural limitation of
   `CredentialStore`'s single-shared-password-list model, not fixable without a real per-user
   credential store.
4. **`FirewallStage` is fail-open by default** (§1.7) — worth an explicit "deny-by-default" mode
   as a configurable posture for operators who want it, rather than requiring a hand-authored
   catch-all rule.
5. **No documented rolling-upgrade version-skew policy for the peer gRPC protocol** — see
   [`ARCHITECTURE_LIMITS.md`](ARCHITECTURE_LIMITS.md) §3.

None of these are exploitable-today findings from a code audit — they are honest gaps found while
grounding this document in the real implementation, listed so they can be tracked and closed
deliberately rather than rediscovered by a customer or an incident.

---

## 4. Vulnerability disclosure and CVE response policy

This is a process document — Warp does not yet have a public bug-bounty program or a dedicated
security contact address; the policy below is the minimum real, honest response commitment this
project can currently make.

### 4.1 Reporting

- Report a suspected vulnerability privately — do not open a public GitHub issue.
- If this project later designates a security contact address or a GitHub Security Advisory
  intake, that channel supersedes this note; until then, report through the same private channel
  used for any other confidential communication with the maintainers (e.g. a direct message to a
  maintainer, not a public issue or PR).
- Include: affected version/commit, a minimal reproduction, and the realistic impact (what an
  attacker could actually do, not just "this looks wrong").

### 4.2 Response commitment

- **Acknowledgment**: within 5 business days of a private report.
- **Triage**: the maintainers confirm whether it's a real, in-scope vulnerability and its severity
  (using CVSS-like reasoning: is it remotely exploitable, does it need authentication, what's the
  impact) within 10 business days of acknowledgment.
- **Fix timeline**: no fixed SLA is promised today (this project does not yet have a dedicated
  security response team) — but a confirmed critical/high-severity vulnerability (remote,
  unauthenticated, meaningful impact) is treated as the top priority over all other in-flight work
  until a fix or mitigation ships.
- **Disclosure**: coordinated disclosure is preferred — the reporter and maintainers agree on a
  disclosure date once a fix is available (or, for a gap that cannot be fixed quickly, once a
  documented mitigation/workaround exists). Absent an agreement, 90 days from the initial report
  is the default disclosure window, consistent with common industry practice (e.g. Google
  Project Zero's default).
- **Credit**: reporters are credited in the fix's release notes/changelog unless they ask to
  remain anonymous.

### 4.3 Scope

In scope: the Warp codebase itself (this repository), its default configuration, and its
documented deployment guidance. Out of scope: vulnerabilities in a third-party dependency that are
already tracked upstream (report those to the upstream project; Warp will still take a dependency
bump once a fix is available), and any finding that requires an operator to have already
misconfigured Warp against this document's own guidance (e.g. deliberately running the admin API
with `WARP_ADMIN_TOKEN` set to a known/weak value).

### 4.4 What this policy does not yet cover

Honestly stated, not glossed over: there is no CVE-numbering authority (CNA) status for this
project today, no automated security-advisory publishing pipeline, and no dedicated security
contact distinct from the general maintainer contact. A real CVE would currently be requested
through GitHub's own Security Advisory → CVE request flow once a fix is ready, not through a
project-run process. Closing this gap (a real `SECURITY.md` at the repository root, wired to
GitHub's Security Advisory feature) is tracked as a near-term follow-up, not assumed done by this
document's existence.

---

## Critical files (for whoever extends this document)

- [`Warp/src/main/java/com/sayonora/warp/acl/ClientAcl.java`](../Warp/src/main/java/com/sayonora/warp/acl/ClientAcl.java) / [`ConnectionGate.java`](../Warp/src/main/java/com/sayonora/warp/acl/ConnectionGate.java) — network admission
- [`Warp/src/main/java/com/sayonora/warp/http/auth/AccessContextResolver.java`](../Warp/src/main/java/com/sayonora/warp/http/auth/AccessContextResolver.java) — OAuth/OIDC
- [`Warp/src/main/java/com/sayonora/warp/http/admin/MetricsServer.java`](../Warp/src/main/java/com/sayonora/warp/http/admin/MetricsServer.java) — admin API role resolution
- [`Warp/src/main/java/com/sayonora/warp/core/AccessControlStage.java`](../Warp/src/main/java/com/sayonora/warp/core/AccessControlStage.java) / [`core/FirewallStage.java`](../Warp/src/main/java/com/sayonora/warp/core/FirewallStage.java) — data-scoping and SQL firewall
- [`Warp/src/main/java/com/sayonora/warp/secrets/FieldCipher.java`](../Warp/src/main/java/com/sayonora/warp/secrets/FieldCipher.java) — secrets at rest
- [`Warp/src/main/java/com/sayonora/warp/grpc/WarpPeerGrpcServer.java`](../Warp/src/main/java/com/sayonora/warp/grpc/WarpPeerGrpcServer.java) / [`server/TlsSupport.java`](../Warp/src/main/java/com/sayonora/warp/server/TlsSupport.java) — TLS/mTLS trust domains
- [`Warp/pom.xml`](../Warp/pom.xml) — dependency pinning; where a scanning plugin would be added
