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
 * some other function code / field layout this codebase doesn't yet parse, is unconfirmed.
 *
 * <p><b>Further narrowed via temporary function-code logging in {@link RequestLoop#handleData}
 * </b> (added and removed for this investigation, not left in): the full sequence the server
 * actually receives before hanging is {@code FUNC_EXECUTE} (CREATE TABLE), {@code FUNC_EXECUTE}
 * (INSERT), {@code FUNC_COMMIT}, {@code FUNC_EXECUTE} (the first SELECT) -- then nothing. The hang
 * is inside {@link com.nexagres.wire.orawire.wireformat.TnsPacketReader#readPacket}'s very FIRST
 * {@code readFully} call, reading the TNS packet HEADER itself, before any function code is even
 * parsed -- ruling out a length-field miscalculation on a received-but-misread packet (the kind of
 * bug {@code BackendDriverRegistry}'s own {@code realCatalogSchemaName} javadoc documents fixing
 * elsewhere in this project) in favor of a stronger, stranger conclusion: the server never receives ANY
 * bytes of a new packet at all for the second request. Since the client's own {@code
 * T4C8Oall.doOALL} stack trace shows it genuinely attempting a fresh OALL8 (execute) call, not a
 * cursor-close or cancel, this points toward client-side ojdbc11 behavior gating the actual socket
 * write behind some session/statement-cache precondition this minimal server implementation never
 * satisfies (real Oracle servers do more post-query bookkeeping -- FAN/ONS notifications, server-
 * side result caching negotiation, session state validation -- that a thin Phase-1 orawire never
 * emits) -- not confirmed, would need the same real-Oracle-capture methodology this project
 * already relies on for TTC framing bugs, just pointed at ojdbc11's OWN client-side decision logic
 * rather than at the wire bytes themselves. Per this project's own established
 * methodology for exactly this class of bug (see {@code RequestLoop}'s own extensive "confirmed
 * live against a real Oracle 23c instance" / "byte-diffing... against a real Oracle-to-Oracle
 * self-loop capture" comments throughout), a definitive fix needs a real packet capture (tcpdump/
 * Wireshark) of a real ojdbc11 client re-executing the same PreparedStatement against a REAL
 * Oracle server (this project already has a real, disposable one available via {@code RealOracle}
 * for exactly this kind of investigation) to see what, if anything, differs about the SECOND
 * request/response cycle that a real server does but this one doesn't -- not attempted here since
 * it's a genuinely open-ended investigation, not a bounded fix. Left as a documented, fast-failing
 * regression rather than a guessed fix that could be wrong in a way that's hard to detect later.
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

    /** Fixed: root cause was NOT in {@link RequestLoop#handleReexecute} -- a real Oracle packet
     * capture (see ResponseWriter#writeSuccessEndWithWarning's javadoc) proved the client's
     * second-request bytes were always correct and already handled; the actual bug was that
     * Warp's response to the FIRST select never told the client its cursor was exhausted (real
     * Oracle embeds an inline "ORA-01403: no data found" warning alongside the real row data on
     * that exact response shape), leaving the client's internal cursor state confused enough that
     * it hung before ever writing its second request. {@code @Timeout} stays as a safety net so a
     * future regression fails fast (~30s) rather than hanging CI. */
    @Test
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
