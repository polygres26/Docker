package com.polygres.wire.mywire;

import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.config.FailedStatementLog;
import com.polygres.wire.core.DialectTranslationStage;
import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.SqlStateErrorMapper;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
import com.polygres.wire.core.TranslationCache;
import com.polygres.wire.core.TranslationLlmClient;
import com.polygres.wire.core.UntranslatableQueryException;
import com.polygres.wire.pgwire.PgConnections;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.server.TlsSupport;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Speaks the MySQL client/server protocol directly: handshake v10,
 * mysql_native_password auth, then COM_QUERY (text protocol only — no
 * prepared statements/COM_STMT_*, no SSL). Same narrow-slice scope and same
 * "proxy onto the shared Postgres backend through the canonical pipeline"
 * design as {@code com.polygres.wire.pgwire.PgWireSessionHandler}; see that
 * class's javadoc for the rationale — including borrowing a pooled backend
 * connection fresh per statement rather than holding one for the session.
 */
public final class MySqlWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MySqlWireSessionHandler.class);
    private static final AtomicLong NEXT_CONNECTION_ID = new AtomicLong(1);

    private final Socket clientSocket;
    // Reassigned to the SSLSocket layer if the client negotiates CLIENT_SSL (see performHandshake) --
    // tracked here purely so run()'s cleanup closes whichever socket is actually live.
    private volatile Socket activeSocket;
    private final ServerOptions options;
    private final CredentialStore credentials = new CredentialStore();
    // RTT optimization (ARCHITECTURE.md §11) — same reasoning as PgWireSessionHandler's identical
    // fields: built once per session, rebound per statement, not rebuilt per statement.
    private final JdbcBackendExecutor terminalExecutor = new JdbcBackendExecutor(null);
    private final StatementPipeline pipeline;

    // Shared across every session on this node (mirrors DialectTranslationStage's own per-registry
    // singleton cache/llmClient) -- this is what gives mywire's default homogeneous
    // mywire->Postgres proxy path (no POLYWIRE_BACKENDS/routing configured, so
    // DialectTranslationStage never fires -- see executeQuery's comment below) the exact same
    // cache-check -> deterministic-rule -> LLM-fallback -> clean-reject guarantee the routed
    // @link path already has, via DialectTranslationStage.translateWithFallback, rather than a
    // second, uncached, no-fallback implementation calling DialectTranslations.translate directly.
    private static final TranslationCache DEFAULT_PATH_CACHE = new TranslationCache();
    private static final TranslationLlmClient DEFAULT_PATH_LLM_CLIENT = new TranslationLlmClient();

    private final FailedStatementLog failedStatementLog;

    public MySqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.polygres.wire.core.PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.options = options;
        this.pipeline = new StatementPipeline(sharedStages,
                new com.polygres.wire.core.RoutingBackendExecutor(backendRegistry, terminalExecutor));
        this.failedStatementLog = new FailedStatementLog(options.pgHost(), options.pgPort(),
                options.pgDatabase(), options.pgUser(), options.pgPassword());
        this.failedStatementLog.ensureSchema();
    }

    @Override
    public void run() {
        activeSocket = clientSocket;
        try {
            DataInputStream in = new DataInputStream(activeSocket.getInputStream());
            OutputStream out = activeSocket.getOutputStream();
            MySqlPacket packets = new MySqlPacket();

            HandshakeStreams handshake = performHandshake(in, out, packets);
            if (handshake == null) {
                return; // auth failed
            }
            queryLoop(handshake.in(), handshake.out(), packets);
        } catch (java.io.EOFException e) {
            // client disconnected mid-message; not worth logging as a warning
        } catch (Exception e) {
            log.warn("mywire session terminated: {}", e.getMessage(), e);
        } finally {
            try {
                activeSocket.close(); // closes the TLS layer (if any) and the underlying socket
            } catch (IOException ignoredOnSessionTeardown) {
                // closing on the way out -- nothing left to report this to
            }
        }
    }

    // ---- handshake + auth --------------------------------------------------

    /** Bundles the (possibly TLS-upgraded) streams a caller must switch to after {@link #performHandshake}. */
    private record HandshakeStreams(DataInputStream in, OutputStream out) {
    }

    /**
     * MySQL protocol's own in-band TLS negotiation (mirrors {@code PgWireSessionHandler}'s
     * {@code SSLRequest} handling — see that class's javadoc for the general rationale). The
     * server's initial Handshake packet advertises {@code CLIENT_SSL} only when
     * {@link ServerOptions#tlsEnabled()}; a real client that wants TLS (e.g. {@code pymysql}
     * with an {@code ssl=} context) responds with a partial "SSLRequest" packet — capability
     * flags (with {@code CLIENT_SSL} set) + max packet size + charset + 23 reserved bytes, no
     * username/auth-response yet — then immediately starts a TLS handshake on the same socket.
     * Detected here by checking the {@code CLIENT_SSL} bit on the very first response packet;
     * when set, the socket is upgraded in place via {@code SSLSocketFactory} and the *real*
     * handshake-response packet (username, auth bytes, etc.) is read fresh over the new TLS
     * streams, exactly as the protocol spec describes. A client that never sets the bit (or a
     * deployment with no keystore configured, so the flag was never advertised) proceeds exactly
     * as before this change — the plain port is unaffected.
     */
    private HandshakeStreams performHandshake(DataInputStream in, OutputStream out, MySqlPacket packets) throws IOException {
        byte[] scramble = new byte[20];
        new SecureRandom().nextBytes(scramble);
        long connectionId = NEXT_CONNECTION_ID.getAndIncrement();
        packets.writePayload(out, MySqlMessages.handshakeV10(connectionId, scramble, options.tlsEnabled()));

        byte[] response = packets.readPayload(in);
        int clientCapabilities = (response[0] & 0xFF) | ((response[1] & 0xFF) << 8)
                | ((response[2] & 0xFF) << 16) | ((response[3] & 0xFF) << 24);
        if (options.tlsEnabled() && (clientCapabilities & MySqlMessages.CLIENT_SSL) != 0) {
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(TlsSupport.buildKeyManagerFactory(options).getKeyManagers(), null, null);
                SSLSocketFactory factory = sslContext.getSocketFactory();
                SSLSocket sslSocket = (SSLSocket) factory.createSocket(activeSocket, null, activeSocket.getPort(), true);
                sslSocket.setUseClientMode(false);
                sslSocket.startHandshake();
                activeSocket = sslSocket;
                in = new DataInputStream(sslSocket.getInputStream());
                out = sslSocket.getOutputStream();
            } catch (GeneralSecurityException e) {
                throw new IOException("mywire TLS upgrade failed", e);
            }
            response = packets.readPayload(in); // real HandshakeResponse41, now over TLS
        }

        int[] pos = {0};
        pos[0] += 4; // client capability flags
        pos[0] += 4; // max packet size
        pos[0] += 1; // character set
        pos[0] += 23; // reserved
        String username = MySqlPacket.readNulString(response, pos);
        int authLen = response[pos[0]++] & 0xFF;
        byte[] authResponse = Arrays.copyOfRange(response, pos[0], pos[0] + authLen);

        byte[] expected = credentials.lookupPassword(username);
        String expectedPassword = expected == null ? "" : new String(expected, StandardCharsets.UTF_8);
        byte[] expectedScramble = MySqlMessages.nativePasswordScramble(expectedPassword, scramble);
        if (!Arrays.equals(expectedScramble, authResponse)) {
            packets.writePayload(out, MySqlMessages.errPacket(1045, "28000",
                    "Access denied for user '" + username + "'"));
            return null;
        }

        packets.writePayload(out, MySqlMessages.okPacket(0));
        return new HandshakeStreams(in, out);
    }

    // ---- COM_QUERY loop ------------------------------------------------

    private static final int COM_QUIT = 0x01;
    private static final int COM_INIT_DB = 0x02;
    private static final int COM_QUERY = 0x03;
    private static final int COM_PING = 0x0e;

    private void queryLoop(DataInputStream in, OutputStream out, MySqlPacket packets) throws IOException {
        while (true) {
            byte[] payload = packets.readPayload(in);
            if (payload.length == 0) {
                continue;
            }
            int command = payload[0] & 0xFF;
            switch (command) {
                case COM_QUIT -> {
                    return;
                }
                case COM_PING, COM_INIT_DB -> packets.writePayload(out, MySqlMessages.okPacket(0));
                case COM_QUERY -> {
                    String sql = new String(payload, 1, payload.length - 1, StandardCharsets.UTF_8);
                    executeQuery(out, packets, sql);
                }
                default -> packets.writePayload(out, MySqlMessages.errPacket(1047, "08S01",
                        "unsupported command: 0x" + Integer.toHexString(command)));
            }
        }
    }

    // MySQL client libraries (mysql CLI, mysqlclient, pymysql, ...) send session setup
    // statements like "SET NAMES utf8mb4" / "SET autocommit=1" right after connecting.
    // These have no Postgres equivalent and would otherwise fail every connection before
    // the client's real query ever runs, so they're no-op'd here rather than forwarded —
    // the same shim any MySQL-wire-protocol proxy in front of a non-MySQL backend needs.
    // NOT applicable to ORAPG_MYWIRE_BACKEND=mysql (see options.mywireNativeBackend()): a real
    // MySQL/MariaDB backend understands these statements natively, so forwarding them is both
    // correct and necessary there (a client's charset/session settings should actually apply).
    private static final java.util.regex.Pattern SET_STATEMENT =
            java.util.regex.Pattern.compile("^\\s*set\\s+", java.util.regex.Pattern.CASE_INSENSITIVE);

    private void executeQuery(OutputStream out, MySqlPacket packets, String sql) throws IOException {
        if (!options.mywireNativeBackend() && SET_STATEMENT.matcher(sql).find()) {
            packets.writePayload(out, MySqlMessages.okPacket(0));
            return;
        }
        // MYSQL->POSTGRES dialect translation (backtick identifiers, SHOW TABLES/DATABASES,
        // two-arg LIMIT, NVL/NOW(), sequence syntax -- see DialectTranslations' javadoc for the
        // full rule vocabulary): needed here, not just for DialectTranslationStage's own @link
        // cross-dialect path, because THIS is mywire's actual common case -- a real MySQL client's
        // dialect proxied straight onto a real Postgres backend with no @link involved at all.
        // Found live to matter: a real pymysql client's `SELECT \`id\` FROM \`orders\`` got a
        // Postgres syntax error even though DialectTranslations already had a backtick rule --
        // DialectTranslationStage only ever fires for a RouterStage/DbLinkStage-resolved
        // targetBackend, which the default homogeneous mywire->Postgres proxy path never has, so
        // the rule existed but this path never reached it. Not applicable when
        // options.mywireNativeBackend() -- the backend really is MySQL/MariaDB there, so the
        // client's own dialect is already correct as-is.
        //
        // Goes through DialectTranslationStage.translateWithFallback -- the exact same
        // cache-check -> deterministic-rule -> LLM-fallback -> clean-reject logic the routed
        // @link path uses -- rather than calling DialectTranslations.translate() directly, which
        // used to (a) re-run the regex normalize/render logic on every single query forever (no
        // cache) and (b) silently pass the original untranslated SQL straight through to Postgres
        // whenever no deterministic rule matched, directly contradicting
        // UntranslatableQueryException's "never silently pass through" principle.
        String translatedSql = sql;
        if (!options.mywireNativeBackend()) {
            try {
                translatedSql = DialectTranslationStage.translateWithFallback(
                        sql, SourceDialect.MYSQL, SourceDialect.POSTGRES, DEFAULT_PATH_CACHE, DEFAULT_PATH_LLM_CLIENT);
            } catch (UntranslatableQueryException e) {
                failedStatementLog.record(SourceDialect.MYSQL, sql,
                        FailedStatementLog.FailureType.UNTRANSLATABLE, null, null, e.getMessage());
                packets.writePayload(out, MySqlMessages.errPacket(SqlStateErrorMapper.MYSQL_DEFAULT, e.getSQLState(),
                        e.getMessage() == null ? "statement could not be translated" : e.getMessage()));
                return;
            }
        }
        try (Connection backend = options.mywireNativeBackend()
                ? MySqlBackendConnections.open(options) : PgConnections.open(options)) {
            backend.setAutoCommit(true);
            terminalExecutor.rebind(backend);
            Statement statement = Statement.of(SourceDialect.MYSQL, translatedSql, List.of());
            ExecutionResult result = pipeline.execute(statement);
            if (result.isQuery()) {
                List<String> columnNames = result.columnNames();
                List<Integer> columnJdbcTypes = result.columnJdbcTypes();
                packets.writePayload(out, columnCountPayload(columnNames.size()));
                for (int i = 0; i < columnNames.size(); i++) {
                    packets.writePayload(out, MySqlMessages.columnDefinition(columnNames.get(i), columnJdbcTypes.get(i)));
                }
                packets.writePayload(out, MySqlMessages.eofPacket());
                for (List<Object> row : result.rows()) {
                    packets.writePayload(out, MySqlMessages.textRow(row));
                }
                packets.writePayload(out, MySqlMessages.eofPacket());
            } else {
                packets.writePayload(out, MySqlMessages.okPacket(result.updateCount()));
            }
        } catch (SQLException e) {
            String state = sqlState(e);
            int nativeError = SqlStateErrorMapper.toMySqlError(state);
            failedStatementLog.record(SourceDialect.MYSQL, sql,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), nativeError, e.getMessage());
            packets.writePayload(out, MySqlMessages.errPacket(nativeError, state,
                    e.getMessage() == null ? "backend error" : e.getMessage()));
        }
    }

    private static byte[] columnCountPayload(int count) {
        java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
        MySqlPacket.writeLenEncInt(b, count);
        return b.toByteArray();
    }

    private static String sqlState(SQLException e) {
        String state = e.getSQLState();
        return state == null || state.isBlank() ? "HY000" : state;
    }
}
