package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real Windows/SSPI client (mssql-jdbc's {@code authenticationScheme=NTLM},
 * which -- unlike Kerberos/native SSPI -- runs a full NTLM implementation in pure Java, so this is
 * exercisable from any host, not just Windows) can log in via a genuine NTLMv2 handshake -- from
 * the "scope the remaining large items" list: LOGIN7's {@code fIntSecurity} bit was already
 * detected, but every Windows-auth login was refused outright with a fixed 18456 error.
 *
 * <p>The real wire exchange (confirmed live, not assumed from the spec alone): LOGIN7 carries the
 * client's NTLM Type-1 (Negotiate) message inline in its {@code ibSSPI}/{@code cbSSPI} field; the
 * server replies with a real Type-2 (Challenge) message wrapped in a TOKEN_SSPI (0xED) token
 * inside a normal TABULAR_RESULT packet (a bare, unwrapped blob is rejected client-side as an
 * unrecognized token); the client answers with its own standalone {@code TdsPacketType.SSPI}
 * (0x11) packet carrying a raw Type-3 (Authenticate) message; the server verifies the NTLMv2
 * response server-side against {@link com.sayonora.wire.auth.CredentialStore}'s plaintext
 * credential and only then sends LOGINACK or a real login-failure error.
 */
class MssqlNtlmAuthIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    private String ntlmUrl(String password) {
        return "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "authenticationScheme=NTLM;integratedSecurity=true;domain=;"
                + "user=" + postgres.username() + ";password=" + password + ";databaseName=" + postgres.database();
    }

    @Test
    void correctCredentialsCompleteARealNtlmv2HandshakeAndCanQuery() throws SQLException {
        try (Connection conn = DriverManager.getConnection(ntlmUrl(postgres.password()));
                Statement stmt = conn.createStatement();
                var rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next(), "a real query must succeed after a real NTLMv2 login");
            org.junit.jupiter.api.Assertions.assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void wrongPasswordFailsTheNtlmv2ResponseVerification() {
        assertThrows(SQLException.class, () -> {
            try (Connection ignored = DriverManager.getConnection(ntlmUrl("definitely-the-wrong-password"))) {
                // unreachable if verification is real
            }
        });
    }
}
