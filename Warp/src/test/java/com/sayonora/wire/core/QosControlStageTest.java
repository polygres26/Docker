package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Standalone coverage confirming {@link QosControlStage} is exactly as dialect-agnostic as its
 * {@code handle()} implementation looks -- rate limiting is keyed purely on {@code
 * Statement.tenantId()}/{@code workloadClass()}, never on {@code sourceDialect()}, so the same
 * admission control applies whether a statement is headed for the default dialect-translated
 * Postgres backend or a same-dialect native backend (see {@code MssqlWireSessionHandler}/{@code
 * MySqlWireSessionHandler}'s native-mode branches, which now route statements through this same
 * stage instead of bypassing it). No standalone unit test existed for this stage's {@code
 * handle()} logic before this; only an HTTP-admin integration test for the unrelated QoS-tuning-
 * suggestion endpoint did.
 */
class QosControlStageTest {

    private static Statement statement(SourceDialect dialect, String tenant, String workloadClass,
            String targetBackend) {
        return new Statement(tenant, dialect, "SELECT 1", List.of(), workloadClass, targetBackend,
                AccessContext.ANONYMOUS);
    }

    private static ExecutionResult proceed(QosControlStage qos, Statement statement) throws SQLException {
        return qos.handle(statement, s -> ExecutionResult.ofQuery(List.of(), List.of()));
    }

    @Test
    void firstStatementInABurstOfOneIsAdmittedRegardlessOfSourceDialectOrTargetBackend() {
        QosControlStage.ClassLimit oneAtATime = new QosControlStage.ClassLimit(0.0001, 1, 0);
        for (SourceDialect dialect : List.of(SourceDialect.POSTGRES, SourceDialect.MYSQL, SourceDialect.SQL_SERVER)) {
            for (String targetBackend : new String[] { null, "default", "mysql-native", "mssql-native" }) {
                QosControlStage qos = new QosControlStage(oneAtATime, Map.of(), -1, null);
                Statement stmt = statement(dialect, "tenant-" + dialect, "default", targetBackend);
                assertDoesNotThrow(() -> proceed(qos, stmt),
                        "first statement in an empty bucket should always be admitted ("
                                + dialect + ", target=" + targetBackend + ")");
            }
        }
    }

    @Test
    void secondStatementBeyondBurstCapacityIsRejectedRegardlessOfSourceDialect() throws SQLException {
        // burstCapacity=1, near-zero refill rate, maxWait=0 -- the second call in immediate
        // succession has no time to refill a token, so rejection here is deterministic, not timing-
        // dependent.
        QosControlStage.ClassLimit oneAtATime = new QosControlStage.ClassLimit(0.0001, 1, 0);
        for (SourceDialect dialect : List.of(SourceDialect.POSTGRES, SourceDialect.MYSQL, SourceDialect.SQL_SERVER)) {
            QosControlStage qos = new QosControlStage(oneAtATime, Map.of(), -1, null);
            String tenant = "tenant-" + dialect;
            proceed(qos, statement(dialect, tenant, "default", "mssql-native")); // consumes the one token
            Statement second = statement(dialect, tenant, "default", "mssql-native");
            assertThrows(SQLException.class, () -> proceed(qos, second),
                    "a statement beyond burst capacity should be rejected regardless of source dialect (" + dialect + ")");
        }
    }

    @Test
    void rateLimitingIsKeyedPerTenantNotPerTargetBackend() throws SQLException {
        // Same tenant, two different target backends (default Postgres vs. a native backend) --
        // the token bucket is keyed on tenantId+workloadClass only, so exhausting it against one
        // target backend also exhausts it for a statement pinned to a different one. This is the
        // behavior that makes QosControlStage safe to reuse unmodified for native-backend-mode
        // statements: there's no separate, unenforced bucket per backend name to slip through.
        QosControlStage.ClassLimit oneAtATime = new QosControlStage.ClassLimit(0.0001, 1, 0);
        QosControlStage qos = new QosControlStage(oneAtATime, Map.of(), -1, null);
        proceed(qos, statement(SourceDialect.POSTGRES, "shared-tenant", "default", "default"));
        Statement onNativeBackend = statement(SourceDialect.MYSQL, "shared-tenant", "default", "mysql-native");
        assertThrows(SQLException.class, () -> proceed(qos, onNativeBackend));
    }
}
