package com.polygres.wire.mssqlwire.session;

import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.core.RoutingBackendExecutor;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
import com.polygres.wire.mssqlwire.frontend.Login7Handler;
import com.polygres.wire.mssqlwire.frontend.PreLoginHandshake;
import com.polygres.wire.mssqlwire.frontend.SqlBatchReader;
import com.polygres.wire.mssqlwire.frontend.TdsTlsChannel;
import com.polygres.wire.mssqlwire.frontend.TdsTokens;
import com.polygres.wire.mssqlwire.wireformat.TdsPacket;
import com.polygres.wire.mssqlwire.wireformat.TdsPacketType;
import com.polygres.wire.pgwire.PgConnections;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.server.TlsSupport;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Speaks the SQL Server TDS wire protocol directly: PRELOGIN handshake, LOGIN7 auth (SQL auth
 * only), then SQL_BATCH round-tripping onto the shared Postgres backend through the canonical
 * pipeline — same "basic connectivity" scope and same "proxy through {@link StatementPipeline}
 * with a fresh borrowed backend connection per statement" design as {@link
 * com.polygres.wire.mywire.MySqlWireSessionHandler} and {@code PgWireSessionHandler}; see those
 * classes' javadoc for the general rationale.
 *
 * <p><b>TLS</b>: PRELOGIN's ENCRYPTION option (MS-TDS §2.2.6.4) is now negotiated in-band, the
 * same shape as pgwire's {@code SSLRequest}/mywire's {@code CLIENT_SSL} (see {@link
 * com.polygres.wire.pgwire.PgWireSessionHandler#readStartupMessage}'s javadoc for the general
 * pattern) — this session's socket is upgraded to an {@link SSLSocket} in place immediately after
 * the PRELOGIN response is flushed, and every byte from LOGIN7 onward (including the whole
 * SQL_BATCH loop) runs over that TLS-wrapped socket. See {@link #performHandshake} for the actual
 * negotiation logic and {@link PreLoginHandshake}'s javadoc for the real MS-TDS ENCRYPTION byte
 * values (cross-checked against {@code mssql-jdbc}'s own {@code TDS.class} constants).
 *
 * <p>Still out of scope: no RPC/prepared-statement support (SQL_BATCH only), no dialect
 * translation depth beyond {@link com.polygres.wire.core.DialectTranslations#translate} — see that
 * class's SQL_SERVER normalizer for what T-SQL surface area is actually covered today.
 *
 * <p>Clean-room implementation: no Babelfish/Microsoft source was copied. Byte-level protocol
 * shape (packet header, PRELOGIN/LOGIN7 field layout, response token formats) was cross-checked
 * against the public MS-TDS specification; the project owner's suggested primary reference —
 * Babelfish for PostgreSQL's C implementation of the same protocol, in the {@code
 * babelfish-for-postgresql/postgresql_modified_for_babelfish} repo — was consulted for the org/
 * repo identification but its exact TDS-handling source directory could not be located within
 * this pass's time budget (its `src/backend` tree is large and the TDS front-end code, if
 * present in that repo at all rather than in a separate protocol-listener process, wasn't found
 * by path search); the public MS-TDS spec was the actual byte-level source of truth used here as
 * a result. Documented plainly rather than overclaiming the Babelfish cross-check.
 */
public final class MssqlWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MssqlWireSessionHandler.class);

    private final Socket clientSocket;
    // Unlike pgwire/mywire, this never gets reassigned to a different Socket instance on TLS
    // upgrade -- TdsTlsChannel (see performHandshake) layers directly on this same raw socket's
    // streams via an SSLEngine, so plain-socket close() semantics are all that's ever needed here.
    private volatile Socket activeSocket;
    private final ServerOptions options;
    private final CredentialStore credentials = new CredentialStore();
    private final JdbcBackendExecutor terminalExecutor = new JdbcBackendExecutor(null);
    private final StatementPipeline pipeline;

    public MssqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<PipelineStage> sharedStages, BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.options = options;
        this.pipeline = new StatementPipeline(sharedStages,
                new RoutingBackendExecutor(backendRegistry, terminalExecutor));
    }

    @Override
    public void run() {
        activeSocket = clientSocket;
        try {
            DataInputStream in = new DataInputStream(activeSocket.getInputStream());
            OutputStream out = activeSocket.getOutputStream();
            TdsPacket packets = new TdsPacket();

            HandshakeStreams streams = performHandshake(in, out, packets);
            if (streams == null) {
                return; // PRELOGIN/LOGIN7 failed (or Windows auth requested -- unsupported)
            }
            queryLoop(streams.in(), streams.out(), packets);
        } catch (java.io.EOFException e) {
            // client disconnected mid-message; not worth logging as a warning
        } catch (Exception e) {
            log.warn("mssqlwire session terminated: {}", e.getMessage(), e);
        } finally {
            try {
                activeSocket.close();
            } catch (IOException ignoredOnSessionTeardown) {
                // closing on the way out -- nothing left to report this to
            }
        }
    }

    // ---- PRELOGIN + LOGIN7 --------------------------------------------------

    private record HandshakeStreams(DataInputStream in, OutputStream out) {
    }

    private HandshakeStreams performHandshake(DataInputStream in, OutputStream out, TdsPacket packets) throws IOException {
        TdsPacket.Message preloginReq = packets.readMessage(in);
        if (preloginReq.type() != TdsPacketType.PRE_LOGIN) {
            log.warn("mssqlwire: expected PRELOGIN (0x12), got 0x{}", Integer.toHexString(preloginReq.type()));
            return null;
        }
        Map<Integer, byte[]> clientOptions = PreLoginHandshake.parse(preloginReq.payload());
        byte requestedEncryption = PreLoginHandshake.requestedEncryption(clientOptions);
        boolean willUpgrade = options.tlsEnabled()
                && (requestedEncryption == PreLoginHandshake.ENCRYPT_ON
                        || requestedEncryption == PreLoginHandshake.ENCRYPT_REQUIRED);
        if (!options.tlsEnabled() && requestedEncryption == PreLoginHandshake.ENCRYPT_REQUIRED) {
            // TLS isn't configured (POLYWIRE_TLS_KEYSTORE unset) and the client hard-requires
            // encryption -- can't be served; fail the handshake cleanly rather than pretend.
            log.warn("mssqlwire: client requires encryption but TLS isn't configured (set POLYWIRE_TLS_KEYSTORE)");
            return null;
        }
        byte negotiatedEncryption = willUpgrade
                ? PreLoginHandshake.ENCRYPT_ON
                : PreLoginHandshake.ENCRYPT_NOT_SUPPORTED;
        // A real server's PRELOGIN *response* packet is framed as TABULAR_RESULT (0x04), not
        // PRE_LOGIN (0x12) -- found live: mssql-jdbc's SQLServerConnection.prelogin() rejects
        // anything else with "Unexpected response type". Only the *request* packet (client->
        // server) uses type 0x12; MS-TDS's own wording is easy to misread as symmetric here.
        packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, PreLoginHandshake.buildResponse(negotiatedEncryption));

        if (willUpgrade) {
            // In-band TLS upgrade: both sides begin a TLS handshake on this same socket right
            // after the PRELOGIN response is flushed, before LOGIN7 -- same shape as pgwire's
            // SSLRequest upgrade (see PgWireSessionHandler#readStartupMessage's javadoc).
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(TlsSupport.buildKeyManagerFactory(options).getKeyManagers(), null, null);
                // See TdsTlsChannel's javadoc for why this needs an SSLEngine-driven channel
                // rather than a raw-socket SSLSocket wrap (the shape pgwire/mywire both use) --
                // real mssql-jdbc's TDS-wrapped-handshake framing turns out asymmetric between
                // its own read and write sides, found only by live-testing against the simpler
                // approach first.
                TdsTlsChannel tls = new TdsTlsChannel(sslContext, activeSocket);
                tls.handshake();
                in = new DataInputStream(tls.inputStream());
                out = tls.outputStream();
            } catch (GeneralSecurityException e) {
                throw new IOException("mssqlwire TLS upgrade failed", e);
            }
        }

        TdsPacket.Message loginReq = packets.readMessage(in);
        if (loginReq.type() != TdsPacketType.LOGIN7) {
            log.warn("mssqlwire: expected LOGIN7 (0x10), got 0x{}", Integer.toHexString(loginReq.type()));
            return null;
        }
        Login7Handler.Credentials creds = Login7Handler.parse(loginReq.payload());
        if (creds.integratedSecurity()) {
            log.warn("mssqlwire: client requested Windows/SSPI auth, only SQL auth is supported");
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(18456, "Windows authentication is not supported"));
            return null;
        }

        byte[] expected = credentials.lookupPassword(creds.userName());
        String expectedPassword = expected == null ? null : new String(expected, java.nio.charset.StandardCharsets.UTF_8);
        if (expected == null || !expectedPassword.equals(creds.password())) {
            log.warn("mssqlwire: login failed for user '{}'", creds.userName());
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(18456, "Login failed for user '" + creds.userName() + "'"));
            return null;
        }

        packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, TdsTokens.loginAck(creds.database()));
        return new HandshakeStreams(in, out);
    }

    // ---- SQL_BATCH loop ------------------------------------------------

    // Driver-issued session-setup batches (mssql-jdbc sends these right after login, e.g.
    // "SET ANSI_NULL_DFLT_ON ON" / "SET IMPLICIT_TRANSACTIONS OFF" / "SET QUOTED_IDENTIFIER ON")
    // have no Postgres equivalent and would otherwise fail every connection before the client's
    // real query ever runs -- no-op'd here rather than forwarded, matching mywire's identical
    // shim for MySQL client libraries' own "SET NAMES ..." session setup (see
    // MySqlWireSessionHandler.SET_STATEMENT's javadoc for the general rationale).
    private static final Pattern SET_STATEMENT = Pattern.compile("^\\s*set\\s+", Pattern.CASE_INSENSITIVE);

    private void queryLoop(DataInputStream in, OutputStream out, TdsPacket packets) throws IOException {
        while (true) {
            TdsPacket.Message msg = packets.readMessage(in);
            switch (msg.type()) {
                case TdsPacketType.SQL_BATCH -> {
                    String sql = SqlBatchReader.readSqlText(msg.payload());
                    executeQuery(out, packets, sql);
                }
                case TdsPacketType.ATTENTION -> {
                    // cancel request; this pass executes statements synchronously and has nothing
                    // in flight to cancel by the time it would see this -- ack with an empty DONE.
                    ByteArrayOutputStream body = new ByteArrayOutputStream();
                    TdsTokens.writeDone(body, TdsTokens.doneFinalStatus(), 0, 0);
                    packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
                }
                case TdsPacketType.RPC -> {
                    log.warn("mssqlwire: RPC batches not supported in this pass");
                    packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                            TdsTokens.errorMessage(50000, "RPC/prepared-statement calls are not supported yet"));
                }
                default -> {
                    if (msg.payload().length == 0 && msg.type() == 0) {
                        return; // client closed the connection (LOGOUT has no dedicated packet type in this pass)
                    }
                    log.warn("mssqlwire: unsupported message type 0x{}", Integer.toHexString(msg.type()));
                    packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                            TdsTokens.errorMessage(50000, "unsupported TDS message type 0x" + Integer.toHexString(msg.type())));
                }
            }
        }
    }

    private void executeQuery(OutputStream out, TdsPacket packets, String sql) throws IOException {
        if (SET_STATEMENT.matcher(sql).find()) {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            TdsTokens.writeDone(body, TdsTokens.doneFinalStatus(), 0, 0);
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
            return;
        }
        try (Connection backend = PgConnections.open(options)) {
            backend.setAutoCommit(true);
            terminalExecutor.rebind(backend);
            Statement statement = Statement.of(SourceDialect.SQL_SERVER, sql, List.of());
            ExecutionResult result = pipeline.execute(statement);

            ByteArrayOutputStream body = new ByteArrayOutputStream();
            if (result.isQuery()) {
                List<String> columnNames = result.columnNames();
                TdsTokens.writeColMetaData(body, columnNames);
                for (List<Object> row : result.rows()) {
                    TdsTokens.writeRow(body, row);
                }
                TdsTokens.writeDone(body, TdsTokens.doneCountStatus(), TdsTokens.curCmdSelect(), result.rows().size());
            } else {
                // mssql-jdbc's StreamDone.getUpdateCount() (found live via bytecode inspection --
                // see TdsTokens.curCmdFor's javadoc) asserts DONE's CurCmd is one of a specific
                // whitelist (INSERT/UPDATE/DELETE/... ) before it will even read DoneRowCount --
                // an all-zero/generic CurCmd silently produces -1 rather than the real row count,
                // even though DONE_COUNT was set correctly.
                TdsTokens.writeDone(body, TdsTokens.doneCountStatus(), TdsTokens.curCmdFor(sql), result.updateCount());
            }
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
        } catch (SQLException e) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, e.getMessage() == null ? "backend error" : e.getMessage()));
        }
    }
}
