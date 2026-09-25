package com.sayonora.wire.pgwire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import org.junit.jupiter.api.Test;

/**
 * Real proof that {@code Describe(Statement)} reports a parameterized statement's real parameter
 * count -- a genuine gap found auditing this frontend for GA transparency: it always claimed 0
 * parameters regardless of the real statement, so a driver/tool using it for real introspection
 * (pgjdbc's own {@code PreparedStatement.getParameterMetaData()}, confirmed live here) got told
 * every parameterized statement has no parameters at all.
 */
class PgParameterDescriptionIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void parameterMetaDataReportsTheRealParameterCount() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres)) {
                try (java.sql.Statement setup = conn.createStatement()) {
                    setup.execute("CREATE TABLE pg_param_desc_it (id INTEGER, val VARCHAR(20))");
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO pg_param_desc_it (id, val) VALUES (?, ?)")) {
                    ParameterMetaData meta = ps.getParameterMetaData();
                    assertEquals(2, meta.getParameterCount(),
                            "a 2-placeholder statement must report 2 parameters, not 0");
                }
                try (java.sql.Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE pg_param_desc_it");
                }
            }
        }
    }
}
