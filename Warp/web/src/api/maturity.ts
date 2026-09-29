/**
 * Per-protocol maturity classification, shown as a badge in the interfaces list.
 *
 * This is Warp's OWN editorial classification, not a live-measured metric -- unlike everything
 * else this console shows (see Overview.tsx's own header comment: "A figure the API does not
 * report... is not shown. Nothing here is estimated."), maturity is a product/documentation
 * judgment call about verification depth, not something a running process can report about
 * itself. It is grounded in real evidence (test coverage, real-client/official-emulator
 * verification, disclosed gaps in docs/WARP_GUIDE.md), reviewed 2026-09-29, but it is a snapshot
 * assessment that needs revisiting as protocols mature or new gaps are found -- not a live signal.
 *
 * Tiers, in descending order of verification confidence:
 * - Certified: real official client/server, a large/quantified conformance suite, no major
 *   open gaps found (e.g. mongowire: 2,683 recorded steps vs real mongod; boltwire: all 3,810
 *   openCypher TCK scenario instances that pass on real Neo4j also pass here).
 * - Production: real backend/emulator verification exists, but with real, DISCLOSED gaps (e.g.
 *   Bridge mode's session-state-reset limitations for SQL frontends, MCP's native-mode tool
 *   narrowing).
 * - Preview: verification exists but relies on an imperfect oracle the docs themselves flag
 *   ("MinIO doesn't implement every S3 op", "fake-gcs-server... many gaps", "the official Java
 *   driver/console were not run"), or a real, disclosed feature gap blocks production use
 *   end-to-end (e.g. influxwire's cross-backend sharding not yet implemented).
 * - Experimental: newest, still-settling, or no verification-oracle language found in the docs
 *   at all for this specific protocol.
 */
export type Maturity = 'Certified' | 'Production' | 'Preview' | 'Experimental'

const TIERS: Record<Maturity, string[]> = {
  Certified: ['pgwire', 'mongowire', 'dynamowire', 'boltwire', 'cqlwire', 'kafkawire', 'amqpwire'],
  Production: ['mywire', 'orawire', 'mssqlwire', 'sqswire', 'oswire', 'firestorewire', 'datastorewire', 'rediswire', 'mcp'],
  Preview: ['s3wire', 'gcswire', 'pubsubwire', 'azurewire', 'gremlinwire', 'awswire', 'influxwire'],
  Experimental: ['a2a', 'cosmoswire', 'bigtablewire', 'grpc'],
}

const KEY_TO_TIER: Record<string, Maturity> = Object.fromEntries(
  Object.entries(TIERS).flatMap(([tier, keys]) => keys.map((k) => [k, tier as Maturity])),
)

/** Normalizes an interface's `metricsKey` (falling back to its `id`) to the family key used
 * above -- strips mode/transport suffixes (`-tls`, `-native`, `-rest`) that don't change a
 * protocol's own maturity, and folds the three per-service Azure Storage frontends (blob/queue/
 * table, ids like `azblobwire`) into the one `azurewire` family the docs and tests treat as a
 * single protocol. */
function normalize(idOrKey: string): string {
  const stripped = idOrKey.replace(/-(tls|native|rest)$/, '')
  if (/^az\w+wire$/.test(stripped) && stripped !== 'azurewire') return 'azurewire'
  return stripped
}

/** Returns this interface's maturity tier, or `null` if it isn't classified yet (a new protocol
 * added after this file was last reviewed) -- callers should render nothing rather than guess. */
export function maturityFor(metricsKey: string | null | undefined, id: string): Maturity | null {
  const key = normalize(metricsKey || id)
  return KEY_TO_TIER[key] ?? null
}
