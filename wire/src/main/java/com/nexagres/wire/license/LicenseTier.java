package com.nexagres.wire.license;

/**
 * The two commercial tiers PolyWire ships under -- see {@link License}'s class doc for the full
 * enforcement design. Both tiers get every protocol and every feature; they differ only on scale
 * limits and commercial rights, never on capability, so an evaluation under {@link #DEVELOPER}
 * proves the exact architecture a real deployment under {@link #ENTERPRISE} would run.
 */
public enum LicenseTier {

    /** Free forever, no key required. Hard, locally-enforced caps: {@link
     * License#DEVELOPER_MAX_CONNECTIONS} concurrent client connections per instance, {@link
     * License#DEVELOPER_MAX_INSTANCES} live instances (cluster-wide, checked via {@code
     * polywire_nodes} -- see {@code NodeRegistry#countLive}), {@link
     * License#DEVELOPER_MAX_BACKENDS} registered Postgres backends. Not licensed for commercial
     * production use regardless of whether a deployment happens to stay under these caps --  the
     * caps are the technical enforcement mechanism, "not for production" is a separate license
     * term they exist to make practically true, not the whole of it. */
    DEVELOPER,

    /** Unlocked by a valid, unexpired {@code POLYWIRE_LICENSE_KEY} -- see {@link
     * License#fromEnv()}. No caps of any kind; every {@code DEVELOPER} limit becomes
     * {@link Integer#MAX_VALUE} in enforcement code rather than a separate code path, so
     * Enterprise never silently behaves differently beyond "the ceiling isn't there." */
    ENTERPRISE
}
