package com.nexagres.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.RealPostgres;
import com.nexagres.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * <b>Known, real, unfixed bug -- this test documents and fails fast on it, it does not verify
 * correct behavior.</b> Found live while writing {@code OracleCacheInvalidationIntegrationTest}:
 * calling {@code executeQuery()} TWICE on the same {@link PreparedStatement} against orawire
 * deadlocks the whole session permanently -- no exception, no timeout on either side. Reproduces
 * identically with {@code WARP_CACHE_TABLES} completely unset, so this is a general orawire
 * cursor-reexecution bug, not specific to any cache tier.
 *
 * <p>What was confirmed live (thread dumps of both the real ojdbc11 client JVM and the warp
 * subprocess, captured mid-hang):
 * <ul>
 *   <li>The client is blocked inside {@code T4CTTIfun.receive} / {@code T4C8Oall.doOALL},
 *       waiting to read a function-response header byte that never arrives -- it believes it
 *       already sent a complete request and is purely waiting on the reply.
 *   <li>The server ({@link RequestLoop#run}) is blocked inside {@link
 *       com.nexagres.wire.orawire.wireformat.TnsPacketReader#readPacket}, waiting to read a brand
 *       new TNS packet header -- it believes the previous request/response cycle is already
 *       complete and is waiting for the next one.
 * </ul>
 * Both sides are correct about their own state and both are waiting on the other -- a genuine
 * protocol-framing mismatch on the second execute of an already-open cursor, not a slow backend or
 * a thread deadlock inside this codebase. {@link RequestLoop#handleReexecute} exists and looks
 * complete by inspection (reads {@code cursor_id}/{@code iters}/options, looks up the recorded
 * {@code StatementSignature}, replays the bind values, delegates to {@code handleExecute}) --
 * whether the client actually sends {@code FUNC_REEXECUTE} for a re-run {@code SELECT} at all, or
 * some other function code / field layout this codebase doesn't yet parse, is unconfirmed. Per
 * this project's own established methodology for exactly this class of TTC framing bug (see
 * {@code RequestLoop}'s own extensive "confirmed live against a real Oracle 23c instance" /
 * "byte-diffing... against a real Oracle-to-Oracle self-loop capture" comments throughout), a real
 * fix needs a real Oracle server to capture and diff the actual second-execute wire bytes against
 * -- not available in this test environment (only orawire-emulating-Postgres), so this is
 * deliberately left as a documented, fast-failing regression rather than a guessed fix.
 *
 * <p><b>Practical impact</b>: any real client code that prepares a SELECT once and re-executes it
 * (a very common pattern -- exactly what proving a cache actually got hit requires) will hang
 * forever against orawire today. {@code OracleCacheInvalidationIntegrationTest} works around this
 * by using a fresh {@code PreparedStatement} per read instead of reusing one, which sidesteps the
 * bug (each fresh statement's first execute is unaffected) without hiding it.
 */
class OracleRepeatedQueryIsolationTest {

    private RealPostgres postgres;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    /** Disabled so this known, tracked bug doesn't perpetually fail the suite -- re-enable once
     * {@link RequestLoop#handleReexecute} (or whatever the real fix turns out to be) is fixed and
     * confirmed against a real Oracle capture; at that point this test should pass outright, no
     * changes needed beyond removing {@code @Disabled}. {@code @Timeout} stays as a safety net so
     * a future run against a still-broken build fails fast (~30s) rather than hanging CI. */
    @Test
    @Disabled("Known bug: re-executing the same PreparedStatement's SELECT against orawire "
            + "deadlocks the session -- see this class's javadoc for the confirmed root-cause "
            + "investigation. Tracked, not silently worked around.")
    @Timeout(30)
    void reExecutingTheSamePreparedStatementTwiceHangsTheSession() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password())) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE isolation_test (id INTEGER PRIMARY KEY, status VARCHAR(20))");
                stmt.executeUpdate("INSERT INTO isolation_test (id, status) VALUES (1, 'pending')");
                conn.commit();
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT status FROM isolation_test WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                }
                // This second call on the SAME PreparedStatement is the one that hangs -- @Timeout
                // above interrupts it after 30s (confirmed: the interrupt surfaces to the client
                // as a real ORA-18730 "Interrupted IO error", not a clean return -- that's this
                // bug's signature, not a different one). If this line ever completes normally
                // instead, the bug has been fixed: remove @Disabled and this comment, and drop the
                // fresh-PreparedStatement workaround from OracleCacheInvalidationIntegrationTest
                // (and its MySQL/SQL Server siblings, which carry the same defensive workaround),
                // since it would no longer be necessary.
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                }
            }
        }
    }
}
