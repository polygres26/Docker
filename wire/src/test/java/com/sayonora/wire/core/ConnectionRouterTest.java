package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Resolution order, globbing, strict mode, scope semantics and scale of {@link ConnectionRouter}. */
class ConnectionRouterTest {

    private static final String PG = "jdbc:postgresql://unreachable.invalid:1/";

    /** {@code sets} maps set name -> backend names; the first set also gets "default". */
    private static BackendRegistry registry(Map<String, List<String>> sets) {
        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        StringBuilder groups = new StringBuilder();
        for (Map.Entry<String, List<String>> e : sets.entrySet()) {
            if (groups.length() > 0) {
                groups.append('|');
            }
            groups.append(e.getKey()).append(":plain=").append(String.join(",", e.getValue()));
            for (String b : e.getValue()) {
                targets.put(b, new BackendTarget(b, PG + b, "u", "p"));
            }
        }
        return BackendRegistry.fromConfig(null, null, null, groups.toString(), null, targets);
    }

    private static BackendRegistry small() {
        Map<String, List<String>> sets = new LinkedHashMap<>();
        sets.put("east", List.of("pg_e1", "pg_e2"));
        sets.put("west", List.of("pg_w1", "pg_w2", "pg_w3"));
        return registry(sets);
    }

    private static ConnectionRouter router(BackendRegistry r, ConnectionRouter.Mode mode) {
        ConnectionRouter router = new ConnectionRouter(r, mode);
        return router;
    }

    @Test
    void implicitBackendNamePinsToThatBackendAndSetNameScopesToTheSet() {
        ConnectionRouter r = router(small(), ConnectionRouter.Mode.IMPLICIT);

        ConnectionRoute backend = r.resolve("postgres", "pg_e2", "app");
        assertEquals(ConnectionRoute.Kind.BACKEND, backend.kind());
        assertEquals("pg_e2", backend.backend());
        assertTrue(backend.scope().permits("pg_e2"));
        assertFalse(backend.scope().permits("pg_e1"));
        assertFalse(backend.scope().permits("default"));

        ConnectionRoute set = r.resolve("mysql", "west", "app");
        assertEquals(ConnectionRoute.Kind.SET, set.kind());
        assertEquals(java.util.Set.of("pg_w1", "pg_w2", "pg_w3"), set.scope().allowedBackends());
        assertEquals("pg_w1", set.scope().defaultBackend(), "the first member is the set's default backend");
        assertFalse(set.scope().permits("pg_e1"));
    }

    @Test
    void namesAreCaseInsensitiveButAnExactCaseMatchIsPreferred() {
        ConnectionRouter r = router(small(), ConnectionRouter.Mode.IMPLICIT);
        assertEquals("pg_e1", r.resolve("postgres", "PG_E1", null).backend());
        assertEquals("west", r.resolve("postgres", "West", null).setName());
    }

    @Test
    void unknownNameIsUnroutedInImplicitModeAndRejectedInStrictMode() {
        BackendRegistry reg = small();
        assertSame(ConnectionRoute.UNROUTED, router(reg, ConnectionRouter.Mode.IMPLICIT).resolve("postgres", "nope", "u"));
        assertSame(ConnectionRoute.UNROUTED, router(reg, ConnectionRouter.Mode.IMPLICIT).resolve("postgres", "", "u"));
        ConnectionRouter strict = router(reg, ConnectionRouter.Mode.STRICT);
        assertTrue(strict.resolve("postgres", "nope", "u").isRejected());
        assertEquals("nope", strict.resolve("postgres", "nope", "u").requestedName());
        assertTrue(strict.resolve("postgres", null, "u").isRejected(), "a blank name is unknown in strict mode");
        assertFalse(strict.resolve("postgres", "pg_e1", "u").isRejected());
    }

    @Test
    void offModeDisablesEverythingIncludingRoutes() {
        ConnectionRouter r = router(small(), ConnectionRouter.Mode.OFF);
        r.load("[{\"database\":\"*\",\"target\":\"east\"}]");
        assertSame(ConnectionRoute.UNROUTED, r.resolve("postgres", "pg_e1", "u"));
        assertSame(ConnectionRoute.UNROUTED, r.resolve("postgres", "zzz", "u"));
    }

    @Test
    void explicitRoutesBeatImplicitNamesAndTheFirstMatchWins() {
        ConnectionRouter r = router(small(), ConnectionRouter.Mode.IMPLICIT);
        r.load("[{\"database\":\"pg_e1\",\"target\":\"pg_w3\"},"
                + "{\"database\":\"sales_*\",\"target\":\"west\",\"defaultBackend\":\"pg_w2\"},"
                + "{\"database\":\"sales_eu\",\"target\":\"east\"}]");

        assertEquals("pg_w3", r.resolve("postgres", "pg_e1", "u").backend(), "an explicit route shadows the implicit name");
        ConnectionRoute globbed = r.resolve("postgres", "SALES_eu", "u");
        assertEquals("west", globbed.setName(), "first matching route wins over the later, more specific one");
        assertEquals("pg_w2", globbed.scope().defaultBackend());
        assertEquals("pg_e2", r.resolve("postgres", "pg_e2", "u").backend());
    }

    @Test
    void routesMatchByProtocolAndLoginUser() {
        ConnectionRouter r = router(small(), ConnectionRouter.Mode.IMPLICIT);
        r.load("[{\"protocol\":\"oracle\",\"database\":\"FREEPDB1\",\"user\":\"hr_*\",\"target\":\"pg_w1\"},"
                + "{\"protocol\":\"oracle\",\"database\":\"FREEPDB1\",\"target\":\"east\"}]");

        assertEquals("pg_w1", r.resolve("oracle", "freepdb1", "HR_APP").backend());
        assertEquals("east", r.resolve("oracle", "FREEPDB1", "someone").setName());
        assertSame(ConnectionRoute.UNROUTED, r.resolve("postgres", "FREEPDB1", "HR_APP"), "protocol must match");
    }

    @Test
    void aMatchedRouteWhoseTargetVanishedRejectsInsteadOfFallingThrough() {
        ConnectionRouter r = router(small(), ConnectionRouter.Mode.IMPLICIT);
        r.load("[{\"database\":\"ghost\",\"target\":\"no_such_backend\"}]");
        assertTrue(r.resolve("postgres", "ghost", "u").isRejected(), "fail closed, never unscoped");
    }

    @Test
    void targetPrefixesDisambiguateABackendAndASetOfTheSameName() {
        // a set called "pg_e1" alongside the backend "pg_e1"
        Map<String, List<String>> sets = new LinkedHashMap<>();
        sets.put("pg_e1", List.of("pg_e1", "pg_x"));
        BackendRegistry reg = registry(sets);
        ConnectionRouter r = router(reg, ConnectionRouter.Mode.IMPLICIT);

        assertEquals("pg_e1", r.resolve("postgres", "pg_e1", "u").backend(), "a bare name is the backend");
        assertNull(r.connectAsSet("pg_e1"), "the set is shadowed: it is reached through a route");
        r.load("[{\"database\":\"whole\",\"target\":\"set:pg_e1\"}]");
        assertEquals(java.util.Set.of("pg_e1", "pg_x"), r.resolve("postgres", "whole", "u").scope().allowedBackends());
    }

    @Test
    void globMatchingIsCaseInsensitiveWithStarAndQuestionMark() {
        assertTrue(ConnectionRouter.glob("sales_*", "SALES_"));
        assertTrue(ConnectionRouter.glob("a?c", "AbC"));
        assertTrue(ConnectionRouter.glob("*", ""));
        assertTrue(ConnectionRouter.glob("*_x_*", "a_x_b"));
        assertFalse(ConnectionRouter.glob("a?c", "ac"));
        assertFalse(ConnectionRouter.glob("sales_*", "sale"));
        assertTrue(ConnectionRouter.glob("*a*b*c*", "xxaxxbxxcxx"));
        assertFalse(ConnectionRouter.glob("*a*b*c*", "xxaxxcxxbxx"));
    }

    @Test
    void routeValidationRejectsDuplicatesUnknownTargetsAndBadDefaults() {
        ConnectionRouter r = router(small(), ConnectionRouter.Mode.IMPLICIT);
        assertThrows(IllegalArgumentException.class, () -> r.validateAgainstRegistry(ConnectionRouter.parse(
                "[{\"database\":\"a\",\"target\":\"east\"},{\"database\":\"A\",\"target\":\"west\"}]")));
        assertThrows(IllegalArgumentException.class, () -> r.validateAgainstRegistry(ConnectionRouter.parse(
                "[{\"database\":\"a\",\"target\":\"nowhere\"}]")));
        assertThrows(IllegalArgumentException.class, () -> r.validateAgainstRegistry(ConnectionRouter.parse(
                "[{\"database\":\"a\",\"target\":\"east\",\"defaultBackend\":\"pg_w1\"}]")));
        assertThrows(IllegalArgumentException.class, () -> r.validateAgainstRegistry(ConnectionRouter.parse(
                "[{\"database\":\"a\",\"target\":\"pg_e1\",\"defaultBackend\":\"pg_e1\"}]")));
        assertThrows(IllegalArgumentException.class, () -> ConnectionRouter.parse("[{\"target\":\"east\"}]"));
        assertThrows(IllegalArgumentException.class, () -> ConnectionRouter.parse(
                "[{\"protocol\":\"ftp\",\"database\":\"a\",\"target\":\"east\"}]"));
        r.validateAgainstRegistry(ConnectionRouter.parse(
                "[{\"database\":\"a\",\"user\":\"u1\",\"target\":\"east\"},{\"database\":\"a\",\"user\":\"u2\",\"target\":\"west\"}]"));
    }

    @Test
    void aMalformedReloadKeepsThePreviousRoutes() {
        ConnectionRouter r = router(small(), ConnectionRouter.Mode.IMPLICIT);
        r.load("[{\"database\":\"a\",\"target\":\"east\"}]");
        r.load("not json at all");
        assertEquals(1, r.routes().size());
        assertEquals("east", r.resolve("postgres", "a", "u").setName());
        r.load(null);
        assertEquals(0, r.routes().size());
    }

    @Test
    void routesRoundTripThroughTheirJsonForm() {
        String json = "[{\"protocol\":\"mysql\",\"database\":\"d*\",\"user\":\"u\",\"target\":\"west\",\"defaultBackend\":\"pg_w2\"}]";
        assertEquals(json, ConnectionRouter.render(ConnectionRouter.parse(json)));
    }

    @Test
    void theSnapshotFollowsBackendRegistryReloads() {
        BackendRegistry reg = small();
        ConnectionRouter r = reg.connectionRouter();
        assertSame(ConnectionRoute.UNROUTED, r.resolve("postgres", "pg_new", "u"));
        // a fresh WARP_BACKENDS reload adds pg_new (staticExtraTargets are re-merged; use the spec path)
        reg.reload("pg_new=" + PG + "pg_new|u|p", null, null, null);
        assertEquals("pg_new", r.resolve("postgres", "pg_new", "u").backend());
    }

    @Test
    void aHundredBackendsInTenSetsResolveTenThousandNamesQuickly() {
        Map<String, List<String>> sets = new LinkedHashMap<>();
        for (int s = 0; s < 10; s++) {
            java.util.ArrayList<String> members = new java.util.ArrayList<>();
            for (int b = 0; b < 10; b++) {
                members.add("s" + s + "_b" + b);
            }
            sets.put("set" + s, members);
        }
        BackendRegistry reg = registry(sets);
        ConnectionRouter r = router(reg, ConnectionRouter.Mode.STRICT);
        assertEquals(100, reg.orderedNames().size());
        r.load("[{\"database\":\"legacy_*\",\"target\":\"set3\"}]");

        // warm the snapshot, then time 10k resolutions across backends, sets, routes and unknown names
        r.resolve("postgres", "s0_b0", "u");
        long start = System.nanoTime();
        int routed = 0;
        int rejected = 0;
        for (int i = 0; i < 10_000; i++) {
            String name = switch (i % 4) {
                case 0 -> "s" + (i % 10) + "_b" + ((i / 10) % 10);
                case 1 -> "set" + (i % 10);
                case 2 -> "legacy_" + i;
                default -> "unknown_" + i;
            };
            ConnectionRoute route = r.resolve("postgres", name, "user" + (i % 7));
            if (route.isRejected()) {
                rejected++;
            } else {
                routed++;
                assertFalse(route.scope().allowedBackends().isEmpty());
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertEquals(7_500, routed);
        assertEquals(2_500, rejected);
        assertTrue(elapsedMs < 1_000, "10k resolutions took " + elapsedMs + "ms");
        assertEquals(10, r.resolve("postgres", "set7", "u").scope().allowedBackends().size());
        assertEquals(1, r.resolve("postgres", "s7_b3", "u").scope().allowedBackends().size());
        // connectAs: a backend is reached by its own name, a set by its own name
        assertEquals("s7_b3", r.connectAsBackend("s7_b3"));
        assertEquals("set7", r.connectAsSet("set7"));
    }

    @Test
    void applyPinsABackendRouteAndOnlyScopesASetRoute() {
        ConnectionRouter r = router(small(), ConnectionRouter.Mode.IMPLICIT);
        Statement base = Statement.of(SourceDialect.POSTGRES, "select 1", List.of());

        Statement pinned = r.resolve("postgres", "pg_e1", "u").apply(base);
        assertEquals("pg_e1", pinned.targetBackend());
        assertEquals(java.util.Set.of("pg_e1"), pinned.backendScope().allowedBackends());

        Statement scoped = r.resolve("postgres", "east", "u").apply(base);
        assertNull(scoped.targetBackend(), "a set route leaves target resolution to the router rules");
        assertEquals(java.util.Set.of("pg_e1", "pg_e2"), scoped.backendScope().allowedBackends());

        assertSame(base, ConnectionRoute.UNROUTED.apply(base), "unrouted adds nothing per statement");
    }

    @Test
    void storeBackendsFiltersToPostgresAndReportsUnroutedAsNull() {
        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        targets.put("pg1", new BackendTarget("pg1", PG + "pg1", "u", "p"));
        targets.put("my1", new BackendTarget("my1", "jdbc:mysql://unreachable.invalid:1/my1", "u", "p"));
        BackendRegistry reg = BackendRegistry.fromConfig(null, null, null, "mixed:plain=pg1,my1", null, targets);
        ConnectionRouter r = router(reg, ConnectionRouter.Mode.IMPLICIT);

        assertNull(r.storeBackends(ConnectionRoute.UNROUTED));
        assertEquals(List.of("pg1"), r.storeBackends(r.resolve("mongodb", "mixed", null)));
        assertEquals(List.of("pg1"), r.storeBackends(r.resolve("mongodb", "pg1", null)));
        assertEquals(List.of(), r.storeBackends(r.resolve("mongodb", "my1", null)), "a MySQL backend cannot host documents");
    }

    @Test
    void aHundredBackendConfigFlowsThroughModelRegistryAndResolver() {
        // The Developer licence caps WARP_BACKENDS at 3, so the 100-backend config is read by the model
        // (which does not cap when reading) and its backends are registered as static targets: the
        // model / registry / resolver logic is what is being exercised, not the licence.
        StringBuilder spec = new StringBuilder();
        StringBuilder groups = new StringBuilder();
        for (int s = 0; s < 10; s++) {
            if (s > 0) {
                groups.append('|');
            }
            groups.append("set").append(s).append(":plain=");
            for (int b = 0; b < 10; b++) {
                String name = "s" + s + "_b" + b;
                spec.append(spec.length() > 0 ? ";" : "").append(name).append('=').append(PG).append(name).append("|u|p");
                groups.append(b > 0 ? "," : "").append(name);
            }
        }
        com.sayonora.wire.config.WarpConfig cfg = new com.sayonora.wire.config.WarpConfig(null, null, null, null,
                null, null, null, spec.toString(), null, null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, groups.toString(), null, null, null, null,
                null, null);
        BackendSetModel model = BackendSetModel.from(cfg, null);
        assertEquals(100, model.allBackends().size());
        assertEquals(10, model.sets().size());
        assertEquals(10, model.backendsOf("set4").size());

        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        for (BackendSetModel.Backend b : model.allBackends()) {
            targets.put(b.name(), new BackendTarget(b.name(), b.url(), b.user(), b.password()));
        }
        BackendRegistry reg = BackendRegistry.fromConfig(null, null, null, cfg.backendGroups(), null, targets);
        ConnectionRouter router = new ConnectionRouter(reg, ConnectionRouter.Mode.IMPLICIT);

        for (BackendSetModel.BackendSet set : model.sets()) {
            ConnectionRoute route = router.resolve("postgres", set.name(), "u");
            assertEquals(10, route.scope().allowedBackends().size(), set.name());
            for (BackendSetModel.Backend b : model.backendsOf(set.name())) {
                assertTrue(route.scope().permits(b.name()));
                ConnectionRoute one = router.resolve("postgres", b.name(), "u");
                assertEquals(java.util.Set.of(b.name()), one.scope().allowedBackends());
                assertFalse(one.scope().permits("s0_b0") && !b.name().equals("s0_b0"));
            }
        }
    }
}
