package com.polygres.wire.dynamowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.testsupport.RealPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * As {@code com.polygres.wire.mongowire.PostgresDocumentStoreDrainRoutingTest} /
 * {@code com.polygres.wire.oswire.PostgresSearchStoreDrainRoutingTest} /
 * {@code com.polygres.wire.sqswire.PgQueueStoreDrainRoutingTest}, for dynamowire: {@link
 * PgItemStore}'s catalog connection previously resolved via the exact, unredirected {@code
 * BackendRegistry.get}, AND {@code ensureCatalog} only ever ran once at construction against
 * whichever backend that resolved to then -- so even after fixing the redirect, the fallback's
 * {@code _dynamo_tables} table would never have existed. Proves both are fixed: the catalog
 * connection redirects while draining, AND the catalog table gets created on the fallback the
 * first time routing actually lands there.
 *
 * <p>Deliberately tests {@link PgItemStore#listTables()} rather than {@code PutItem} on an
 * existing table -- dynamowire's item tables are created once, explicitly, at {@code CreateTable}
 * time (real DynamoDB has no "add a replica later" concept for a table's physical storage either),
 * a pre-existing, already-documented limitation independent of this feature: a table created
 * before a drain was never given a copy on the fallback, same as it never gets one when a shard is
 * added to an existing group. {@code listTables()} only touches the catalog, which this feature
 * does make follow a drain correctly.
 */
class PgItemStoreDrainRoutingTest {

    private RealPostgres primary;
    private RealPostgres fallback;
    private BackendRegistry registry;

    @BeforeEach
    void startInfra() throws Exception {
        primary = RealPostgres.start();
        fallback = RealPostgres.start();
        registry = BackendRegistry.fromConfig(
                "default=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password() + "|fallback"
                        + ";fallback=" + fallback.jdbcUrl() + "|" + fallback.username() + "|" + fallback.password(),
                null);
    }

    @AfterEach
    void stopInfra() {
        if (primary != null) primary.close();
        if (fallback != null) fallback.close();
    }

    @Test
    void catalogReadsRedirectToTheFallbackAndCreateItsOwnCatalogTableWhileDraining() throws Exception {
        PgItemStore store = new PgItemStore(registry);
        store.createTable("Orders", "pk", "S", null, null);
        assertEquals(1, store.listTables().size(), "the catalog must list the table just created on primary");

        registry.setState(BackendRegistry.DEFAULT_BACKEND_NAME, BackendRegistry.BackendState.DRAINING);

        // Before this feature: this threw "relation _dynamo_tables does not exist" -- the
        // redirected connection landed on a real Postgres that had never seen this catalog table.
        // Now: ensureCatalog creates it fresh on the fallback, and the query succeeds -- returning
        // an empty list is the CORRECT answer, since "Orders" (its catalog row) genuinely only
        // exists on primary, a real, independent Postgres the fallback never shared data with.
        assertTrue(store.listTables().isEmpty(),
                "the fallback is a genuinely separate Postgres -- it must never have heard of "
                        + "\"Orders\", but the query itself must succeed, not throw");
    }
}
