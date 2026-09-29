package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the admin-persisted, UI/API-settable per-store serving-set assignment
 * ({@code warp_config.storeFrontendSets}, applied via {@link BackendRegistry#applyStoreFrontendSets})
 * -- the alternative to hand-setting a protocol's {@code WARP_<PROTO>WIRE_SET} env var. Doesn't
 * cover the env-var branch of {@link BackendRegistry#frontendSet} itself (no existing test in this
 * suite sets process env vars), only that the persisted override takes priority and degrades
 * safely, matching {@code BackendRegistryTest}'s style for the sibling WARP_BACKEND_SETS/GROUPS
 * config.
 */
class StoreFrontendSetOverrideTest {

    private static final String BACKENDS = "pg1=jdbc:postgresql://h:5432/db1|u|p"
            + ";pg2=jdbc:postgresql://h:5432/db2|u|p";
    private static final String GROUPS = "set-a=pg1|set-b=pg2";

    private static BackendRegistry registry() {
        return BackendRegistry.fromConfig(BACKENDS, null, null, GROUPS, null, Map.of());
    }

    @Test
    void noOverrideFallsBackToTheDefaultSet() {
        BackendRegistry r = registry();
        // Neither pg1 nor pg2 is named "default", so with no override and no env var, frontendSet
        // falls all the way to "the first set" -- set-a, in declaration order.
        assertEquals("set-a", r.frontendSet(StoreType.S3));
    }

    @Test
    void persistedOverrideSelectsTheNamedSet() {
        BackendRegistry r = registry();
        r.applyStoreFrontendSets("s3=set-b");
        assertEquals("set-b", r.frontendSet(StoreType.S3));
        assertEquals("set-b", r.storeFrontendSetOverride(StoreType.S3));
    }

    @Test
    void overrideIsPerStoreNotGlobal() {
        BackendRegistry r = registry();
        r.applyStoreFrontendSets("s3=set-b|cosmos=set-a");
        assertEquals("set-b", r.frontendSet(StoreType.S3));
        assertEquals("set-a", r.frontendSet(StoreType.COSMOS));
        assertNull(r.storeFrontendSetOverride(StoreType.DYNAMODB));
    }

    @Test
    void anOverrideNamingAnUnknownSetIsIgnoredNotThrown() {
        BackendRegistry r = registry();
        r.applyStoreFrontendSets("s3=no-such-set");
        // Falls back exactly as if nothing were configured -- never throws, matching
        // applyStoreConfig's "malformed entry is logged and skipped" contract.
        assertEquals("set-a", r.frontendSet(StoreType.S3));
    }

    @Test
    void clearingTheOverrideRestoresTheFallback() {
        BackendRegistry r = registry();
        r.applyStoreFrontendSets("s3=set-b");
        assertEquals("set-b", r.frontendSet(StoreType.S3));
        r.applyStoreFrontendSets(null);
        assertEquals("set-a", r.frontendSet(StoreType.S3));
        assertNull(r.storeFrontendSetOverride(StoreType.S3));
    }

    @Test
    void renderAndParseRoundTrip() {
        Map<StoreType, String> assignments = Map.of(StoreType.S3, "set-b", StoreType.COSMOS, "set-a");
        String spec = BackendRegistry.renderStoreFrontendSets(assignments);
        assertEquals(assignments, BackendRegistry.parseStoreFrontendSets(spec));
    }

    @Test
    void emptySpecRendersAsNull() {
        assertNull(BackendRegistry.renderStoreFrontendSets(Map.of()));
    }
}
