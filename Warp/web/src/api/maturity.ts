import maturityData from './maturity-data.json'

/**
 * Per-protocol maturity classification, shown as a badge in the interfaces list.
 *
 * This is Warp's OWN editorial classification, not a live-measured metric -- unlike everything
 * else this console shows (see Overview.tsx's own header comment: "A figure the API does not
 * report... is not shown. Nothing here is estimated."), maturity is a product/documentation
 * judgment call about verification depth, not something a running process can report about
 * itself.
 *
 * The actual classification data -- the tier order, each tier's description, and which protocol
 * ids belong to which tier -- lives in ./maturity-data.json, NOT in this file: it's editorial
 * metadata that changes as protocols mature or new gaps are found, and editing it should never
 * require touching application logic (this file just loads and indexes it). This module is the
 * one place that reads that file; every consumer (MaturityTag/MaturityLegend in components/ui,
 * readiness.ts, every page listing interfaces) goes through the exports below, never the JSON
 * directly, so there's exactly one parsing/indexing step to keep correct.
 *
 * See docs/WARP_GUIDE.md for the real evidence behind each protocol's placement (e.g. mongowire:
 * 2,683 recorded steps vs real mongod; boltwire: all 3,810 openCypher TCK scenario instances that
 * pass on real Neo4j also pass here) -- that level of per-protocol detail lives in the docs, not
 * duplicated here or in the JSON.
 *
 * A UI review flagged the previous 4-tier naming ("Certified" as the top tier) as genuinely
 * ambiguous -- nothing told a reader whether Certified outranked Production, and "Certified"
 * implies an external certification program Warp doesn't actually have. The top tier is named
 * "Verified" in the data file for that reason, and every tier carries an explicit numeric
 * MATURITY_RANK (derived from the data file's own order, never hand-maintained separately) so
 * ordering is never left to the reader -- or a second hardcoded copy of the scale -- to infer.
 */
export type Maturity = string

interface MaturityData {
  order: string[]
  descriptions: Record<string, string>
  tiers: Record<string, string[]>
}

const data = maturityData as MaturityData

/** Ascending order, least to most verified -- exactly the JSON file's own `order` array. */
export const MATURITY_ORDER: Maturity[] = data.order

export const MATURITY_RANK: Record<Maturity, number> = Object.fromEntries(
  MATURITY_ORDER.map((m, i) => [m, i + 1]),
)

export const MATURITY_DESCRIPTION: Record<Maturity, string> = data.descriptions

const KEY_TO_TIER: Record<string, Maturity> = Object.fromEntries(
  Object.entries(data.tiers).flatMap(([tier, keys]) => keys.map((k) => [k, tier])),
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
 * added after maturity-data.json was last reviewed) -- callers should render nothing rather than
 * guess. */
export function maturityFor(metricsKey: string | null | undefined, id: string): Maturity | null {
  const key = normalize(metricsKey || id)
  return KEY_TO_TIER[key] ?? null
}
