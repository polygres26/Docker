package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.core.BackendSetModel.ModelException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Validation, migration and rendering rules of the backend-set model and the registry's store hosting. */
class BackendSetModelTest {

    private static final String PG = "jdbc:postgresql://h:5432/db";
    private static final BackendTarget IMPLICIT = new BackendTarget("default", PG, "u", "p");

    private static WarpConfig config(String backends, String groups) {
        return new WarpConfig(null, null, null, null, null, null, null, backends, null, null,
                null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, groups, null, null, null, null, null, null);
    }

    @Test
    void existingUngroupedBackendsMigrateIntoTheDefaultSetWithoutRewriting() {
        WarpConfig c = config("default=" + PG + "|u|p;pg2=" + PG + "2|u|p", null);
        BackendSetModel m = BackendSetModel.from(c, IMPLICIT);
        assertEquals(List.of("default"), m.sets().stream().map(BackendSetModel.BackendSet::name).toList());
        assertEquals("default", m.backend("pg2").set());
        WarpConfig rendered = m.applyTo(c);
        assertEquals(c.backends(), rendered.backends());
        assertNull(rendered.backendGroups(), "the implicit default set must not be written out as a group");
    }

    @Test
    void implicitDefaultStaysImplicitUntilAnotherBackendIsAdded() {
        WarpConfig c = config(null, null);
        BackendSetModel m = BackendSetModel.from(c, IMPLICIT);
        assertEquals(1, m.allBackends().size());
        assertNull(m.applyTo(c).backends());
        m.addBackend("default", "pg2", PG + "2", "u", "p", null, "second", List.of(StoreType.MONGODB));
        WarpConfig out = m.applyTo(c);
        assertEquals("default=" + PG + "|u|p;pg2=" + PG + "2|u|p", out.backends());
        assertEquals("pg2=mongodb", out.backendStores());
    }

    @Test
    void existingGroupsBecomeSets() {
        WarpConfig c = config("default=" + PG + "|u|p;a=" + PG + "a|u|p", "team:sharded=a");
        BackendSetModel m = BackendSetModel.from(c, IMPLICIT);
        assertEquals("team", m.backend("a").set());
        assertTrue(m.set("team").sharded());
        assertEquals("team:sharded=a", m.applyTo(c).backendGroups());
    }

    @Test
    void backendWithoutASetIsRejectedWith400() {
        BackendSetModel m = BackendSetModel.from(config(null, null), IMPLICIT);
        ModelException e = assertThrows(ModelException.class,
                () -> m.addBackend(null, "x", PG, "u", "p", null, null, List.of()));
        assertEquals(400, e.status());
        assertTrue(e.getMessage().contains("set"));
    }

    @Test
    void storesOnlyOnPostgres() {
        BackendSetModel m = BackendSetModel.from(config(null, null), IMPLICIT);
        ModelException e = assertThrows(ModelException.class, () -> m.addBackend("default", "my",
                "jdbc:mysql://h:3306/db", "u", "p", null, null, List.of(StoreType.SQS)));
        assertEquals(400, e.status());
        assertTrue(e.getMessage().contains("Postgres"));
    }

    @Test
    void neo4jOnlyOnOneBackendPerSet() {
        BackendSetModel m = BackendSetModel.from(config(null, null), IMPLICIT);
        m.patchBackend("default", null, null, null, false, null, List.of(StoreType.NEO4J));
        ModelException e = assertThrows(ModelException.class, () -> m.addBackend("default", "pg2", PG + "2", "u", "p",
                null, null, List.of(StoreType.NEO4J)));
        assertEquals(400, e.status());
        assertTrue(e.getMessage().contains("ONE backend per backend set"));
        m.addSet("other", null);
        m.addBackend("other", "pg3", PG + "3", "u", "p", null, null, List.of(StoreType.NEO4J));
    }

    @Test
    void setDeleteRules() {
        BackendSetModel m = BackendSetModel.from(config(null, null), IMPLICIT);
        assertEquals(409, assertThrows(ModelException.class, () -> m.deleteSet("default")).status());
        m.addSet("s", "d");
        m.addBackend("s", "pg2", PG + "2", "u", "p", null, null, List.of());
        assertEquals(409, assertThrows(ModelException.class, () -> m.deleteSet("s")).status());
        m.deleteBackend("pg2");
        m.deleteSet("s");
        assertNull(m.set("s"));
        assertEquals(409, assertThrows(ModelException.class, () -> m.deleteBackend("default")).status());
    }

    @Test
    void theSetHoldingDefaultCannotBeDeleted() {
        WarpConfig c = config("default=" + PG + "|u|p", "home=default");
        BackendSetModel m = BackendSetModel.from(c, IMPLICIT);
        assertEquals(409, assertThrows(ModelException.class, () -> m.deleteSet("home")).status());
    }

    @Test
    void developerLicenseCapIsRespected() {
        BackendSetModel m = BackendSetModel.from(config(null, null), IMPLICIT);
        m.addBackend("default", "b2", PG + "2", "u", "p", null, null, List.of());
        m.addBackend("default", "b3", PG + "3", "u", "p", null, null, List.of());
        ModelException e = assertThrows(ModelException.class,
                () -> m.addBackend("default", "b4", PG + "4", "u", "p", null, null, List.of()));
        assertEquals(400, e.status());
        assertTrue(e.getMessage().contains("capped"));
    }

    @Test
    void duplicatesAndBadNamesAreRejected() {
        BackendSetModel m = BackendSetModel.from(config(null, null), IMPLICIT);
        assertEquals(409, assertThrows(ModelException.class,
                () -> m.addBackend("default", "default", PG, "u", "p", null, null, List.of())).status());
        assertEquals(400, assertThrows(ModelException.class,
                () -> m.addBackend("default", "bad name", PG, "u", "p", null, null, List.of())).status());
        assertEquals(404, assertThrows(ModelException.class,
                () -> m.addBackend("nope", "x", PG, "u", "p", null, null, List.of())).status());
        assertEquals(400, assertThrows(ModelException.class,
                () -> m.addBackend("default", "y", PG, "u", "pa|ss", null, null, List.of())).status());
    }

    @Test
    void roundTripPreservesEverything() {
        WarpConfig c = config(null, null);
        BackendSetModel m = BackendSetModel.from(c, IMPLICIT);
        m.addSet("analytics", "the analytics fleet");
        m.addBackend("analytics", "a1", PG + "a1", "u", "p", null, "first", List.of(StoreType.DYNAMODB, StoreType.SQS));
        m.addBackend("analytics", "a2", PG + "a2", "u", "p", null, null, List.of(StoreType.DYNAMODB));
        WarpConfig out = m.applyTo(c);
        BackendSetModel again = BackendSetModel.from(out, IMPLICIT);
        assertEquals("analytics", again.backend("a1").set());
        assertEquals("first", again.backend("a1").description());
        assertEquals("the analytics fleet", again.set("analytics").description());
        assertEquals(List.of(StoreType.DYNAMODB, StoreType.SQS), again.backend("a1").stores());
        assertEquals("analytics=a1,a2", out.backendGroups());
    }

    @Test
    void registryHostsAStoreOnEveryEnablingBackendOfTheFrontendSet() {
        String spec = "default=" + PG + "|u|p;pg2=" + PG + "2|u|p;my=jdbc:mysql://h:3306/db|u|p";
        BackendRegistry r = BackendRegistry.fromConfig(spec, null, null, null, null, Map.of());
        assertEquals(List.of(), r.storeHosts(StoreType.DYNAMODB));
        r.applyStoreConfig("default=dynamodb,sqs|pg2=dynamodb|my=dynamodb", null);
        assertEquals(List.of("default", "pg2"), r.storeHosts(StoreType.DYNAMODB), "MySQL never hosts a store");
        assertEquals(List.of("default"), r.storeHosts(StoreType.SQS));
        assertEquals("default", r.storeHome(StoreType.DYNAMODB));
        assertFalse(r.storeShardGroup(StoreType.MONGODB).contains("pg2"));
    }

    @Test
    void storesOfAnotherSetAreNotServedByTheDefaultSetFrontend() {
        String spec = "default=" + PG + "|u|p;pg2=" + PG + "2|u|p";
        BackendRegistry r = BackendRegistry.fromConfig(spec, null, null, "other=pg2", null, Map.of());
        r.applyStoreConfig("pg2=mongodb", "other");
        assertEquals("default", r.setOf("default"));
        assertEquals("other", r.setOf("pg2"));
        assertEquals(List.of(), r.storeHosts(StoreType.MONGODB));
    }
}
