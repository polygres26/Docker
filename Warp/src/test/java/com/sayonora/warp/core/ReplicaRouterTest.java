package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.ReplicaRouter.LagSample;
import com.sayonora.warp.core.ReplicaRouter.Reason;
import com.sayonora.warp.core.ReplicaRouter.Replica;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Lag-gated replica eligibility, with a fake lag probe and a fake clock (no database). */
class ReplicaRouterTest {

    private static final String SPEC = "pg=jdbc:postgresql://p/db|u|pw||jdbc:postgresql://r1/db~5^jdbc:postgresql://r2/db~10";

    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final Map<String, LagSample> answers = new HashMap<>();

    private ReplicaRouter router() {
        BackendRegistry registry = BackendRegistry.fromConfig(SPEC, null);
        // The registry builds its own router; build a test one over the same registry with the fake probe.
        // Like a real probe, stamp the sample with the time it was taken (not when the answer was staged).
        return new ReplicaRouter(registry, (target, now) -> {
            LagSample a = answers.getOrDefault(target.jdbcUrl(), new LagSample(false, false, 0, "no answer", now));
            return new LagSample(a.ok(), a.isReplica(), a.lagSeconds(), a.message(), now);
        }, clock::get, 5, 30);
    }

    private LagSample ok(double lag) {
        return new LagSample(true, true, lag, null, clock.get());
    }

    @Test
    void replicasAreParsedOntoThePrimaryWithItsCredentialsAndNotRegisteredAsBackends() {
        BackendRegistry registry = BackendRegistry.fromConfig(SPEC, null);
        assertEquals(2, registry.replicaSpecsOf("pg").size());
        assertEquals(1, registry.all().size(), "replicas must not become routable/license-counted backends");
        var replicas = registry.replicaRouter().replicasOf("pg");
        assertEquals("pg#1", replicas.get(0).id());
        assertEquals("u", replicas.get(0).target().user());
        assertEquals(5.0, replicas.get(0).maxLagSeconds());
        assertEquals(10.0, replicas.get(1).maxLagSeconds());
    }

    @Test
    void noSampleYetMeansNotEligible() {
        ReplicaRouter r = router();
        assertNull(r.pick("pg"));
        assertEquals(1L, r.reasonCounts("pg").get(Reason.NO_ELIGIBLE_REPLICA));
    }

    @Test
    void onlyReplicasWithinTheirOwnLagThresholdAreUsed() {
        answers.put("jdbc:postgresql://r1/db", ok(4.0));   // within 5
        answers.put("jdbc:postgresql://r2/db", ok(12.0));  // over 10
        ReplicaRouter r = router();
        r.probeAll();
        for (int i = 0; i < 6; i++) {
            Replica picked = r.pick("pg");
            assertNotNull(picked);
            assertEquals("pg#1", picked.id());
        }
        assertEquals(6, r.routedCount("jdbc:postgresql://r1/db"));
        assertEquals(0, r.routedCount("jdbc:postgresql://r2/db"));
    }

    @Test
    void roundRobinsAcrossEligibleReplicas() {
        answers.put("jdbc:postgresql://r1/db", ok(1.0));
        answers.put("jdbc:postgresql://r2/db", ok(1.0));
        ReplicaRouter r = router();
        r.probeAll();
        for (int i = 0; i < 10; i++) {
            assertNotNull(r.pick("pg"));
        }
        assertEquals(5, r.routedCount("jdbc:postgresql://r1/db"));
        assertEquals(5, r.routedCount("jdbc:postgresql://r2/db"));
    }

    @Test
    void aStaleSampleIsTreatedAsUnknownNotAsFine() {
        answers.put("jdbc:postgresql://r1/db", ok(0.0));
        ReplicaRouter r = router();
        r.probeAll();
        assertNotNull(r.pick("pg"));
        clock.addAndGet(60_000); // far past 3 intervals + 10s with no new sample
        assertNull(r.pick("pg"));
    }

    @Test
    void aProbeThatCannotTellOrIsNotAReplicaIsNeverEligible() {
        answers.put("jdbc:postgresql://r1/db", new LagSample(false, false, 0, "unreachable", clock.get()));
        answers.put("jdbc:postgresql://r2/db", new LagSample(true, false, 0, "not a replica", clock.get()));
        ReplicaRouter r = router();
        r.probeAll();
        assertNull(r.pick("pg"));
    }

    @Test
    void aFailingReplicaIsQuarantinedThenComesBack() {
        answers.put("jdbc:postgresql://r1/db", ok(1.0));
        ReplicaRouter r = router();
        r.probeAll();
        Replica first = r.pick("pg");
        assertNotNull(first);
        r.quarantine(first, "test");
        assertTrue(r.isQuarantined(first));
        assertNull(r.pick("pg"));
        clock.addAndGet(31_000);
        r.probeAll(); // fresh sample so it is not stale
        assertFalse(r.isQuarantined(first));
        assertNotNull(r.pick("pg"));
    }

    @Test
    void aBackendWithoutReplicasIsNotTouched() {
        ReplicaRouter r = router();
        assertFalse(r.hasReplicas("other"));
        assertNull(r.pick("other"));
        assertTrue(r.reasonCounts("other").isEmpty());
    }

    @Test
    void unsupportedEngineReplicasAreNeverEligible() {
        BackendTarget mssql = new BackendTarget("s#1", "jdbc:sqlserver://r:1433", "u", "p");
        LagSample s = ReplicaRouter.probeByEngine(mssql, 1);
        assertFalse(s.ok());
    }

    @Test
    void statusJsonShowsLagEligibilityAndDecisions() {
        answers.put("jdbc:postgresql://r1/db", ok(2.0));
        ReplicaRouter r = router();
        r.probeAll();
        r.pick("pg");
        r.record("pg", Reason.NOT_READ_SAFE);
        var json = r.toJson();
        var p = json.getAsJsonArray("primaries").get(0).getAsJsonObject();
        assertEquals("pg", p.get("primary").getAsString());
        var r1 = p.getAsJsonArray("replicas").get(0).getAsJsonObject();
        assertEquals(2.0, r1.getAsJsonObject("sample").get("lagSeconds").getAsDouble());
        assertTrue(r1.get("eligible").getAsBoolean());
        assertEquals(1, p.getAsJsonObject("decisions").get("routed_to_replica").getAsInt());
        assertEquals(1, p.getAsJsonObject("decisions").get("not_read_safe").getAsInt());
    }
}
