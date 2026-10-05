package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.LocalMySql;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalDouble;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * The two split-brain guards against real MySQL servers: a replica reports when it last heard from its source (and nothing once the
 * source is gone), and a stale writable node is frozen. Opt-in: set WARP_TEST_MYSQL_BIN to a MySQL bin directory.
 */
class MySqlSplitBrainGuardLiveTest {

    @Test
    void aReplicaReportsHowRecentlyItHeardFromItsSourceAndASourceThatIsGoneIsNoEvidence() throws Exception {
        String bin = System.getenv("WARP_TEST_MYSQL_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_MYSQL_BIN");
        Path base = Files.createTempDirectory("mysplit");
        LocalMySql primary = LocalMySql.create(bin, base, "p", FreePort.next(), 1);
        LocalMySql replica = LocalMySql.create(bin, base, "r", FreePort.next(), 2);
        try {
            replica.replicateFrom(primary, 2);
            primary.exec("create database if not exists test", "create table if not exists test.t(id int primary key)", "insert into test.t values (1)");
            long deadline = System.currentTimeMillis() + 30_000;
            while (replica.scalar("select count(*) from information_schema.tables where table_schema='test' and table_name='t'") < 1
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            EngineHa my = EngineHa.forDialect(SourceDialect.MYSQL);
            BackendTarget p = new BackendTarget("p", primary.url(), "root", "");
            BackendTarget r = new BackendTarget("r", replica.url(), "root", "");

            OptionalDouble heard = my.heardFromPrimarySecondsAgo(r, p);
            assertTrue(heard.isPresent(), "a connected replica hears from its source");
            assertTrue(heard.getAsDouble() < 10, "recently: " + heard.getAsDouble());

            BackendTarget unrelated = new BackendTarget("x", "jdbc:mysql://127.0.0.1:" + (primary.port() + 1000) + "/mysql", "root", "");
            assertTrue(my.heardFromPrimarySecondsAgo(r, unrelated).isEmpty(), "heard from a different server is no evidence about this one");

            primary.crash();
            deadline = System.currentTimeMillis() + 30_000;
            OptionalDouble after = my.heardFromPrimarySecondsAgo(r, p);
            while (after.isPresent() && System.currentTimeMillis() < deadline) {
                Thread.sleep(500);
                after = my.heardFromPrimarySecondsAgo(r, p);
            }
            assertTrue(after.isEmpty(), "a crashed source leaves the replica reconnecting, not connected");
        } finally {
            replica.stop();
            primary.stop();
        }
    }

    @Test
    void aStaleWritableNodeIsFrozenAndItsSessionsEnded() throws Exception {
        String bin = System.getenv("WARP_TEST_MYSQL_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_MYSQL_BIN");
        Path base = Files.createTempDirectory("mysplit");
        LocalMySql stale = LocalMySql.create(bin, base, "s", FreePort.next(), 3);
        try {
            stale.exec("create database test", "create table test.t(id int primary key)", "insert into test.t values (1)");
            java.sql.Connection client = stale.conn();
            client.createStatement().execute("select 1");

            EngineHa.forDialect(SourceDialect.MYSQL).fenceStaleWriter(new BackendTarget("s", stale.url(), "root", ""));

            assertEquals(1, stale.scalar("select @@global.super_read_only"));
            var e = org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class,
                    () -> stale.exec("insert into test.t values (2)"), "a new write is refused");
            assertTrue(e.getMessage().toLowerCase().contains("read-only") || e.getMessage().toLowerCase().contains("read only"), e.getMessage());
            assertEquals(1, stale.scalar("select count(*) from test.t"));
            assertFalse(client.isValid(2), "the sessions that were open were ended");
        } finally {
            stale.stop();
        }
    }

    private static final class FreePort {
        static int next() throws java.io.IOException {
            try (var s = new java.net.ServerSocket(0)) {
                return s.getLocalPort();
            }
        }
    }
}
