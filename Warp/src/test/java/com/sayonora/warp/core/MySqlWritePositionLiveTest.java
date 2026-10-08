package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.LocalMySql;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** The MySQL log positions read-your-writes compares: the replica's applied position reaches a write's position only once it applied it. */
class MySqlWritePositionLiveTest {

    @Test
    void aReplicasAppliedPositionReachesAWritesPositionOnlyAfterTheWriteIsApplied() throws Exception {
        String bin = System.getenv("WARP_TEST_MYSQL_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_MYSQL_BIN");
        Path base = Files.createTempDirectory("mywp");
        LocalMySql primary = LocalMySql.create(bin, base, "p", free(), 1);
        LocalMySql replica = LocalMySql.create(bin, base, "r", free(), 2);
        try {
            replica.replicateFrom(primary, 2);
            primary.exec("create database test", "create table test.t(id int primary key)");
            EngineHa my = EngineHa.forDialect(SourceDialect.MYSQL);
            BackendTarget p = new BackendTarget("p", primary.url(), "root", "");
            BackendTarget r = new BackendTarget("r", replica.url(), "root", "");
            long deadline = System.currentTimeMillis() + 30_000;
            while (replica.scalar("select count(*) from information_schema.tables where table_schema='test'") < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            replica.exec("stop replica sql_thread");
            primary.exec("insert into test.t values (1)");
            var written = my.writePosition(p).orElseThrow();
            Thread.sleep(1500);
            assertTrue(my.appliedPosition(r).orElseThrow().compareTo(written) < 0, "the replica has not applied the write while its SQL thread is stopped");
            replica.exec("start replica sql_thread");
            deadline = System.currentTimeMillis() + 30_000;
            while (my.appliedPosition(r).orElseThrow().compareTo(written) < 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            assertFalse(my.appliedPosition(r).orElseThrow().compareTo(written) < 0, "applied position reaches the write once applied");
            assertTrue(replica.scalar("select count(*) from test.t") == 1);
        } finally {
            replica.stop();
            primary.stop();
        }
    }

    private static int free() throws java.io.IOException {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
