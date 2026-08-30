package com.nexagres.migration.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.migration.checkpoint.DeadLetterStore;
import com.nexagres.migration.core.ChangeEvent;
import com.nexagres.migration.core.Sink;
import com.nexagres.migration.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Real, not mocked, dead-letter/retry behavior against a real Postgres -- {@link DeadLetterStore}
 * genuinely writes rows a real JDBC connection can read back. Uses a small in-memory fake {@link
 * Sink} to control exactly how many times a write fails before succeeding, which a real gRPC
 * target can't deterministically simulate.
 */
class ResilientSinkTest {

    private static long deadLetterCount(RealPostgres postgres) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM migration_dead_letters")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void succeedsWithinRetryBudgetWithoutDeadLettering() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            DeadLetterStore deadLetters = new DeadLetterStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            deadLetters.ensureSchema();

            AtomicInteger attempts = new AtomicInteger(0);
            Sink flakyThenOk = new Sink() {
                @Override
                public void apply(ChangeEvent event) throws Exception {
                    if (attempts.incrementAndGet() < 3) {
                        throw new java.sql.SQLException("transient failure #" + attempts.get());
                    }
                }
            };

            ResilientSink resilient = new ResilientSink(flakyThenOk, deadLetters, 5, 10);
            resilient.apply(new ChangeEvent("INSERT INTO t (id) VALUES (?)", List.of("1")));

            assertEquals(3, attempts.get(), "should have retried exactly until the 3rd attempt succeeded");
            assertEquals(0, deadLetterCount(postgres), "a write that eventually succeeded should never be dead-lettered");
        }
    }

    @Test
    void deadLettersAfterExhaustingAllRetries() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            DeadLetterStore deadLetters = new DeadLetterStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            deadLetters.ensureSchema();

            Sink alwaysFails = new Sink() {
                @Override
                public void apply(ChangeEvent event) throws Exception {
                    throw new java.sql.SQLException("permanently broken");
                }
            };

            ResilientSink resilient = new ResilientSink(alwaysFails, deadLetters, 3, 10);
            // Must NOT throw -- a permanently-failing event is handled (dead-lettered), not
            // propagated to kill the caller's partition-read or change-feed loop.
            resilient.apply(new ChangeEvent("INSERT INTO t (id) VALUES (?)", List.of("bad-row")));

            assertEquals(1, deadLetterCount(postgres));
            try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT sql, params, error_message, attempts FROM migration_dead_letters")) {
                assertTrue(rs.next());
                assertEquals("INSERT INTO t (id) VALUES (?)", rs.getString("sql"));
                assertTrue(rs.getString("params").contains("bad-row"));
                assertTrue(rs.getString("error_message").contains("permanently broken"));
                assertEquals(3, rs.getInt("attempts"));
            }
        }
    }

    @Test
    void batchFallsBackToPerEventReplayAndIsolatesTheBadOne() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            DeadLetterStore deadLetters = new DeadLetterStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            deadLetters.ensureSchema();

            List<String> applied = new ArrayList<>();
            Sink rejectsWholeBatchOnceThenOnlyRejectsRowTwo = new Sink() {
                private boolean firstBatchCall = true;

                @Override
                public void apply(ChangeEvent event) throws Exception {
                    if (event.params().get(0).equals("bad")) {
                        throw new java.sql.SQLException("row 'bad' is permanently invalid");
                    }
                    applied.add(event.params().get(0));
                }

                @Override
                public void applyBatch(List<ChangeEvent> events) throws Exception {
                    if (firstBatchCall) {
                        firstBatchCall = false;
                        throw new java.sql.SQLException("whole-batch transient failure");
                    }
                    // Second whole-batch attempt: still fails because one row is permanently bad --
                    // ResilientSink should then fall back to one-at-a-time.
                    throw new java.sql.SQLException("whole-batch failure -- one row is bad");
                }
            };

            ResilientSink resilient = new ResilientSink(rejectsWholeBatchOnceThenOnlyRejectsRowTwo, deadLetters, 2, 10);
            resilient.applyBatch(List.of(
                    new ChangeEvent("INSERT", List.of("good-1")),
                    new ChangeEvent("INSERT", List.of("bad")),
                    new ChangeEvent("INSERT", List.of("good-2"))));

            assertEquals(List.of("good-1", "good-2"), applied, "the two good rows should have landed via the per-event fallback");
            assertEquals(1, deadLetterCount(postgres), "only the genuinely bad row should be dead-lettered");
        }
    }
}
