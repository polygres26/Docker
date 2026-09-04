package com.sayonora.wire.pgwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Real proof that pgwire reports the REAL Postgres OID for UUID/JSONB/timestamptz/smallint result
 * columns instead of collapsing them all to TEXT -- a genuine gap found auditing this frontend for
 * GA transparency: pgjdbc's own typed accessors (getObject() dispatching on the declared OID,
 * java.util.UUID, PGobject) depend on RowDescription naming the real type, and pgjdbc reports BOTH
 * uuid and jsonb columns as the same generic java.sql.Types.OTHER, so jdbcType alone was never
 * enough to tell them apart.
 */
class PgRealColumnTypeIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void uuidJsonbAndSmallintColumnsComeBackAsTheirRealTypesNotText() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE pg_real_type_it (id uuid PRIMARY KEY, data jsonb, small smallint, "
                        + "created_at timestamptz)");
                UUID id = UUID.randomUUID();
                setup.execute("INSERT INTO pg_real_type_it (id, data, small, created_at) VALUES "
                        + "('" + id + "', '{\"a\":1}'::jsonb, 42, now())");

                try (ResultSet rs = setup.executeQuery("SELECT id, data, small, created_at FROM pg_real_type_it")) {
                    assertTrue(rs.next());

                    // Real typed accessors -- these throw/misbehave if the declared OID lied and
                    // said "text" instead of the real type.
                    Object idValue = rs.getObject(1);
                    assertTrue(idValue instanceof UUID, "expected a real java.util.UUID, got: " + idValue.getClass());
                    assertEquals(id, idValue);

                    Object dataValue = rs.getObject(2);
                    assertTrue(dataValue instanceof org.postgresql.util.PGobject,
                            "expected a real PGobject for jsonb, got: " + dataValue.getClass());
                    assertEquals("jsonb", ((org.postgresql.util.PGobject) dataValue).getType());

                    assertEquals(java.sql.Types.SMALLINT, rs.getMetaData().getColumnType(3),
                            "expected the real SMALLINT/int2 type reported, not a widened int4");

                    assertTrue(rs.getObject(4) instanceof java.sql.Timestamp
                                    || rs.getObject(4) instanceof java.time.OffsetDateTime,
                            "expected a real timestamptz-typed value, not a plain string");
                }

                setup.execute("DROP TABLE pg_real_type_it");
            }
        }
    }
}
