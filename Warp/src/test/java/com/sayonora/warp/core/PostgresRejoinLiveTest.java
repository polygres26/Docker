package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.LocalPostgres;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Live check of {@code scripts/pg-rebuild-standby.sh} driven through {@link PostgresHa#runRejoinCommand}: an old primary that kept
 * writing after the standby was promoted (so it has diverged) is rewound and comes back as a streaming standby of the new
 * primary, without the diverged write. Opt-in: set WARP_TEST_BROWNOUT_PG_BIN to a Postgres bin directory.
 */
class PostgresRejoinLiveTest {

    private static void exec(LocalPostgres pg, String sql) throws Exception {
        try (Connection c = pg.conn(); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static long count(LocalPostgres pg, String where) throws Exception {
        try (Connection c = pg.conn(); Statement st = c.createStatement();
                var rs = st.executeQuery("select count(*) from t where " + where)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private void rebuildsADivergedOldPrimaryAsAStandby(String mode) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path base = Files.createTempDirectory("pgrejoin");
        LocalPostgres oldPrimary = LocalPostgres.primary(bin, base, "p", LocalPostgres.freePort());
        LocalPostgres newPrimary = null;
        try {
            exec(oldPrimary, "create table t(id int primary key, note text)");
            exec(oldPrimary, "insert into t values (1, 'replicated')");
            newPrimary = LocalPostgres.replicaOf(bin, base, "r", oldPrimary, LocalPostgres.freePort());
            long deadline = System.currentTimeMillis() + 20_000;
            while (count(newPrimary, "id = 1") < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            // promote the standby while the old primary is still up, then let the old one keep writing: it diverges
            exec(newPrimary, "select pg_promote()");
            deadline = System.currentTimeMillis() + 20_000;
            while (!newPrimary.writable() && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            exec(oldPrimary, "insert into t values (2, 'only on the old primary')");
            exec(newPrimary, "insert into t values (3, 'only on the new primary')");

            String script = Path.of("scripts/pg-rebuild-standby.sh").toAbsolutePath().toString();
            BackendTarget node = new BackendTarget("old", oldPrimary.url(), "warp", "secret");
            BackendTarget primary = new BackendTarget("new", newPrimary.url(), "warp", "secret");
            EngineHa.RejoinResult res = PostgresHa.runRejoinCommand(
                    "LC_ALL=en_US.UTF-8 '" + script + "' '" + bin + "' '" + oldPrimary.dir() + "' " + mode, node, primary, 120);
            assertEquals(EngineHa.RejoinOutcome.REJOINED, res.outcome(), res.detail());

            assertFalse(oldPrimary.writable(), "the rebuilt node is a standby");
            deadline = System.currentTimeMillis() + 20_000;
            while (count(oldPrimary, "id = 3") < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            assertEquals(1, count(oldPrimary, "id = 3"), "streams the new primary's writes");
            assertEquals(0, count(oldPrimary, "id = 2"), "the diverged write is discarded");
            assertEquals(1, count(oldPrimary, "id = 1"));
            exec(newPrimary, "insert into t values (4, 'after rejoin')");
            deadline = System.currentTimeMillis() + 20_000;
            while (count(oldPrimary, "id = 4") < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            assertTrue(count(oldPrimary, "id = 4") == 1, "keeps replicating");
        } finally {
            oldPrimary.stop("immediate");
            if (newPrimary != null) {
                newPrimary.stop("immediate");
            }
        }
    }

    @Test
    void pgRewindRebuildsADivergedOldPrimary() throws Exception {
        rebuildsADivergedOldPrimaryAsAStandby("");
    }

    @Test
    void basebackupRebuildsADivergedOldPrimary() throws Exception {
        rebuildsADivergedOldPrimaryAsAStandby("--basebackup");
    }
}
