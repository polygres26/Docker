package com.sayonora.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a MySQL client's AUTO_INCREMENT primary keys actually come back through
 * mywire -- a genuine gap found auditing this frontend for GA transparency: {@code
 * JdbcBackendExecutor} never asked the backend for generated keys, so mywire's OK packet always
 * hardcoded its last-insert-id field to 0, meaning {@code Statement.getGeneratedKeys()} and
 * {@code LAST_INSERT_ID()} always returned 0/empty regardless of the real assigned id --
 * silently breaking essentially any ORM's default "let the database assign the id" strategy.
 */
class MySqlGeneratedKeysIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void generatedKeyIsReturnedForAutoIncrementInsert() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE mysql_genkey_it (id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val VARCHAR(20))");

                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO mysql_genkey_it (val) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
                    ps.setString(1, "first");
                    ps.executeUpdate();
                    try (ResultSet keys = ps.getGeneratedKeys()) {
                        assertTrue(keys.next(), "expected a generated key for the auto-assigned id");
                        assertEquals(1, keys.getLong(1));
                    }

                    ps.setString(1, "second");
                    ps.executeUpdate();
                    try (ResultSet keys = ps.getGeneratedKeys()) {
                        assertTrue(keys.next());
                        assertEquals(2, keys.getLong(1), "the second insert must get the next assigned id, not the first's again");
                    }
                }

                // LAST_INSERT_ID() itself is pg_mysql's own SQL-level emulation (a separate
                // Postgres extension, not always installed on a plain test Postgres) -- this test
                // targets mywire's own OK-packet generated-key field, which is what
                // Statement.getGeneratedKeys() above actually reads on the wire, independent of
                // whether that extension is present.
                stmt.execute("DROP TABLE mysql_genkey_it");
            }
        }
    }
}
