package com.polygres.wire.pgwire;

import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.server.TlsSupport;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Speaks the PostgreSQL frontend/backend wire protocol (v3.0) directly:
 * startup handshake, cleartext-password auth, then the Simple Query
 * subprotocol ('Q' -&gt; RowDescription/DataRow(s)/CommandComplete). This is a
 * narrow slice — Simple Query only, no Parse/Bind/Execute (extended query),
 * no SSL, no COPY — analogous in scope to the Oracle frontend's initial cut
 * (see {@code com.polygres.wire.orawire.session.RequestLoop}), but far simpler to
 * implement since Postgres's wire format is openly documented (no
 * clean-room reverse-engineering needed, unlike Oracle's TTC).
 *
 * <p>Reuses the same backend Postgres instance the Oracle frontend proxies
 * to ({@link ServerOptions#pgHost()} etc.). Query execution runs through
 * the shared {@link StatementPipeline} (ARCHITECTURE.md §3/§4) instead of
 * talking to JDBC directly — this is the reference integration proving the
 * canonical {@link Statement} abstraction works end-to-end against a real
 * client (psql); the Oracle frontend still executes directly and is the
 * next candidate to migrate onto the same pipeline.
 *
 * <p>Each Simple Query statement is its own auto-committed unit of work, so
 * {@link #executeSimpleQuery} borrows a connection from {@link
 * com.polygres.wire.core.BackendConnectionPools} (via {@link PgConnections#open})
 * fresh per statement and returns it immediately after — a session sitting
 * idle between queries (or one that never queries at all) holds zero
 * backend connections, and many concurrent pgwire sessions share the same
 * small backend pool instead of each pinning one connection for their
 * entire lifetime.
 *
 * <p><b>Extended Query Protocol (Parse/Bind/Describe/Execute/Sync/Close) is also supported</b> —
 * added after discovering live that real clients relying on it (found via {@code postgres_fdw},
 * which never uses Simple Query at all) couldn't connect. Unlike Simple Query, this needs a
 * <em>session-scoped</em> connection ({@link #extendedConnection}, lazily borrowed and held until
 * the session ends), not a fresh one per statement — {@code postgres_fdw} genuinely issues
 * {@code DECLARE CURSOR}/{@code FETCH}/{@code CLOSE}/{@code COMMIT} as real SQL text, and a cursor
 * is backend-session state that a per-statement borrow/release model would destroy between the
 * {@code DECLARE} and the first {@code FETCH}. {@link JdbcBackendExecutor#execute} already detects
 * query-vs-non-query generically via {@code PreparedStatement.execute()}'s own boolean return, not
 * SQL-verb sniffing — so {@code DECLARE}/{@code FETCH}/{@code CLOSE} are handled correctly with no
 * changes needed there; a real backend connection genuinely creates a real server-side cursor.
 *
 * <p><b>Deliberately narrow, matching this project's usual scope discipline</b>:
 * <ul>
 *   <li><b>No real server-side incremental cursors of our own</b> — {@code Bind} eagerly runs the
 *   statement in full and caches the {@link ExecutionResult} on the {@link Portal}; repeated
 *   {@code Execute} calls against the same portal slice that already-computed row list by
 *   {@code maxRows}, they don't re-fetch from the backend incrementally. Fine for the row counts
 *   this project's scope targets; a genuinely large result set would materialize fully in memory at
 *   {@code Bind} time rather than streaming.</li>
 *   <li><b>Text-format bind parameters only</b> — a binary-format parameter is rejected with a
 *   real {@code ErrorResponse}, not silently misread.</li>
 *   <li><b>{@code $1}/{@code $2} positional parameters are rewritten to JDBC {@code ?} placeholders</b>
 *   ({@link #DOLLAR_PARAM}), reordering the bind list to match each {@code ?}'s textual occurrence
 *   order (handles repeats/reordering, e.g. {@code $2} appearing before {@code $1}) — Postgres's own
 *   native parameter syntax isn't valid JDBC {@code PreparedStatement} placeholder syntax, and
 *   {@link JdbcBackendExecutor} binds positionally by {@code ?} order.</li>
 *   <li><b>{@code Describe Statement}</b> (before {@code Bind}) always reports zero parameters and
 *   no row shape — this project doesn't pre-analyze a statement's real parameter types or result
 *   shape without running it, and {@code postgres_fdw} in practice describes the <em>portal</em>
 *   (after {@code Bind}, when the real shape is already known), not the bare statement.</li>
 * </ul>
 */
public final class PgWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PgWireSessionHandler.class);

    private final Socket clientSocket;
    // Reassigned to the SSLSocket layer if the client negotiates SSLRequest (see readStartupMessage) --
    // tracked here (rather than threaded through every method signature) purely so run()'s cleanup
    // closes whichever socket is actually live, TLS-upgraded or not.
    private volatile Socket activeSocket;
    private final ServerOptions options;
    private final CredentialStore credentials = new CredentialStore();
    // RTT optimization (ARCHITECTURE.md §11): built once per session (this class is already
    // instantiated fresh per accepted TCP connection), not per statement — see
    // JdbcBackendExecutor#rebind's javadoc for why. terminalExecutor starts unbound; every call
    // site rebinds it to that statement's freshly-borrowed connection before running pipeline.execute.
    private final JdbcBackendExecutor terminalExecutor = new JdbcBackendExecutor(null);
    // Kept as a field (not just handed to StatementPipeline and forgotten) so handleTransactionControl
    // can drive its beginTransaction()/endTransaction() — see that executor's javadoc for why a real
    // explicit transaction needs this, not just the pipeline's normal per-statement execute() path.
    private final com.polygres.wire.core.RoutingBackendExecutor routingExecutor;
    private final StatementPipeline pipeline;

    // ---- Extended Query Protocol session state (see class javadoc) ----
    private Connection sessionConnection; // lazily borrowed, shared by Simple and Extended paths -- see sessionConnection()
    private final Map<String, String> preparedStatements = new LinkedHashMap<>();
    private final Map<String, Portal> portals = new LinkedHashMap<>();
    private boolean skipUntilSync; // set on any Extended Query error; real protocol semantics: ignore messages until the next Sync

    private static final Pattern DOLLAR_PARAM = Pattern.compile("\\$(\\d+)");

    private record Portal(String sqlText, ExecutionResult result, int[] nextRow) {
        Portal(String sqlText, ExecutionResult result) {
            this(sqlText, result, new int[1]);
        }
    }

    public PgWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.polygres.wire.core.PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.options = options;
        this.routingExecutor = new com.polygres.wire.core.RoutingBackendExecutor(backendRegistry, terminalExecutor);
        this.pipeline = new StatementPipeline(sharedStages, routingExecutor);
    }

    @Override
    public void run() {
        activeSocket = clientSocket;
        try {
            DataInputStream in = new DataInputStream(activeSocket.getInputStream());
            DataOutputStream out = new DataOutputStream(activeSocket.getOutputStream());

            StartupStreams startup = performStartup(in, out);
            if (startup == null) {
                return; // auth failed or client bailed after SSL negotiation
            }
            queryLoop(startup.in(), startup.out());
        } catch (java.io.EOFException e) {
            // client disconnected mid-message; not worth logging as a warning
        } catch (Exception e) {
            log.warn("pgwire session terminated: {}", e.getMessage(), e);
        } finally {
            try {
                activeSocket.close(); // closes the TLS layer (if any) and the underlying socket
            } catch (IOException ignoredOnSessionTeardown) {
                // closing on the way out -- nothing left to report this to
            }
            try {
                routingExecutor.endTransaction(false); // a client that disconnects mid-transaction rolls back -- real Postgres does the same on connection loss, and this is what actually releases any routed connections beginTransaction() opened
            } catch (SQLException ignoredOnSessionTeardown) {
                // closing on the way out -- nothing left to report this to
            }
            if (sessionConnection != null) {
                try {
                    sessionConnection.close();
                } catch (SQLException ignoredOnSessionTeardown) {
                    // closing on the way out -- nothing left to report this to
                }
            }
        }
    }

    /**
     * Borrowed once, lazily, on the first statement that actually needs to run something — shared
     * by <em>both</em> Simple Query and Extended Query Protocol paths, not one connection per
     * protocol. Found live to matter: {@code postgres_fdw} sends {@code BEGIN} via Simple Query but
     * {@code DECLARE CURSOR}/{@code FETCH}/{@code CLOSE} via Extended Query — a real Postgres
     * session is inherently one continuous connection/transaction context regardless of which
     * sub-protocol carries any given statement, so giving each path its own separate physical
     * connection (this class's original design, before that was discovered) meant a real
     * {@code BEGIN} on one connection never reached the session the cursor was later declared on,
     * and the target backend correctly rejected it ("DECLARE CURSOR can only be used in transaction
     * blocks").
     */
    private Connection sessionConnection() throws SQLException {
        if (sessionConnection == null) {
            sessionConnection = PgConnections.open(options);
            sessionConnection.setAutoCommit(true); // matches real Postgres: autocommit until an explicit BEGIN
        }
        return sessionConnection;
    }

    /**
     * {@code true} if {@code sql} was itself a transaction-control statement and has already been
     * fully handled — real {@code BEGIN}/{@code COMMIT}/{@code ROLLBACK} SQL text is intercepted
     * and translated into real {@link Connection#setAutoCommit}/{@link Connection#commit}/
     * {@link Connection#rollback} calls against the shared session connection, rather than forwarded
     * as literal SQL text — this project's {@link JdbcBackendExecutor} always runs through
     * {@code PreparedStatement}, and keeping this connection's own {@code autoCommit} flag
     * genuinely in sync with what the client asked for is what makes the rest of the session's
     * statements (including a later {@code DECLARE CURSOR}) actually run inside the transaction the
     * client thinks it started.
     *
     * <p>Also drives {@link #routingExecutor}'s {@code beginTransaction()}/{@code endTransaction()}
     * in lockstep with the session connection's own transaction boundary — see that executor's
     * javadoc for why a {@code table@link}-routed statement needs to know an explicit transaction is
     * open at all, not just this connection.
     */
    private boolean handleTransactionControl(Connection connection, String sql) throws SQLException {
        String verb = sql.strip().split("\\s+", 2)[0].toUpperCase(java.util.Locale.ROOT);
        switch (verb) {
            case "BEGIN", "START" -> {
                connection.setAutoCommit(false);
                routingExecutor.beginTransaction();
            }
            case "COMMIT", "END" -> {
                connection.commit();
                connection.setAutoCommit(true);
                routingExecutor.endTransaction(true);
            }
            case "ROLLBACK" -> {
                connection.rollback();
                connection.setAutoCommit(true);
                routingExecutor.endTransaction(false);
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    private static char readyForQueryStatus(Connection connection) {
        try {
            return connection != null && !connection.getAutoCommit() ? 'T' : 'I';
        } catch (SQLException ignoredStatusIsBestEffort) {
            return 'I';
        }
    }

    // ---- startup + auth --------------------------------------------------

    /** Bundles the (possibly TLS-upgraded) streams a caller must switch to after {@link #performStartup}. */
    private record StartupStreams(DataInputStream in, DataOutputStream out) {
    }

    private StartupStreams performStartup(DataInputStream in, DataOutputStream out) throws IOException {
        StartupMessage startup = readStartupMessage(in, out);
        if (startup == null) {
            return null;
        }
        in = startup.in();
        out = startup.out();
        Map<String, String> params = startup.params();
        String username = params.getOrDefault("user", "");

        PgMessages.writeAuthCleartextPassword(out);
        int type = in.readUnsignedByte();
        if (type != 'p') {
            throw new IOException("expected PasswordMessage, got type " + type);
        }
        int len = in.readInt();
        byte[] body = new byte[len - 4];
        in.readFully(body);
        String password = new String(body, 0, body.length - 1, StandardCharsets.UTF_8); // strip NUL

        byte[] expected = credentials.lookupPassword(username);
        if (expected == null || !password.equals(new String(expected, StandardCharsets.UTF_8))) {
            PgMessages.writeErrorAndReady(out, "28P01", "password authentication failed for user \"" + username + "\"");
            return null;
        }

        PgMessages.writeAuthOk(out);
        PgMessages.writeParameterStatus(out, "server_version", "14.0 (polywire)");
        PgMessages.writeParameterStatus(out, "client_encoding", "UTF8");
        PgMessages.writeBackendKeyData(out);
        PgMessages.writeReadyForQuery(out, 'I');
        out.flush();
        return new StartupStreams(in, out);
    }

    private record StartupMessage(DataInputStream in, DataOutputStream out, Map<String, String> params) {
    }

    /**
     * Returns the startup params (and the streams to keep using -- TLS-upgraded if the client sent
     * {@code SSLRequest} and {@link ServerOptions#tlsEnabled()}), or {@code null} if the connection
     * ended during SSL/GSS negotiation or startup.
     *
     * <p>{@code SSLRequest} handling: real Postgres wire protocol v3 negotiates TLS in-band on the
     * same port a plaintext client also uses -- the client sends an {@code SSLRequest} startup
     * packet before anything else, the server replies a single {@code 'S'} (yes) or {@code 'N'} (no)
     * byte, and on {@code 'S'} both sides immediately begin a TLS handshake using the very socket
     * the plaintext bytes were just exchanged over. This is what a real {@code psql
     * sslmode=require} client actually speaks, so it's implemented here directly (upgrading this
     * session's socket in place via {@code SSLSocketFactory.createSocket(Socket, ...)} in server
     * mode) rather than only standing up a separate always-TLS listener port the way orawire/mywire
     * do -- unlike them, pgwire has its own real, well-documented in-protocol negotiation that real
     * clients expect on the standard port, and skipping it would mean {@code sslmode=require}
     * against the plain pgwire port simply fails for every real Postgres client/driver out there.
     * When TLS isn't configured ({@code POLYWIRE_TLS_KEYSTORE} unset), the reply is {@code 'N'},
     * exactly as before this change -- a plaintext-only client is completely unaffected.
     */
    private StartupMessage readStartupMessage(DataInputStream in, DataOutputStream out) throws IOException {
        while (true) {
            int len = in.readInt();
            int code = in.readInt();
            if (code == PgMessages.SSL_REQUEST_CODE) {
                if (options.tlsEnabled()) {
                    out.writeByte('S');
                    out.flush();
                    try {
                        SSLContext sslContext = SSLContext.getInstance("TLS");
                        sslContext.init(TlsSupport.buildKeyManagerFactory(options).getKeyManagers(), null, null);
                        SSLSocketFactory factory = sslContext.getSocketFactory();
                        SSLSocket sslSocket = (SSLSocket) factory.createSocket(activeSocket, null, activeSocket.getPort(), true);
                        sslSocket.setUseClientMode(false);
                        sslSocket.startHandshake();
                        activeSocket = sslSocket;
                        in = new DataInputStream(sslSocket.getInputStream());
                        out = new DataOutputStream(sslSocket.getOutputStream());
                    } catch (GeneralSecurityException e) {
                        throw new IOException("pgwire TLS upgrade failed", e);
                    }
                } else {
                    out.writeByte('N'); // "no" to SSL -- plaintext only, matches pre-TLS behavior
                    out.flush();
                }
                continue;
            }
            if (code == PgMessages.GSSENC_REQUEST_CODE) {
                out.writeByte('N'); // GSS encryption still unsupported
                out.flush();
                continue;
            }
            if (code != PgMessages.PROTOCOL_VERSION_3_0) {
                throw new IOException("unsupported startup protocol version: " + code);
            }
            byte[] rest = new byte[len - 8];
            in.readFully(rest);
            return new StartupMessage(in, out, parseStartupParams(rest));
        }
    }

    private Map<String, String> parseStartupParams(byte[] rest) {
        Map<String, String> params = new LinkedHashMap<>();
        int i = 0;
        while (i < rest.length && rest[i] != 0) {
            int keyEnd = indexOfNul(rest, i);
            String key = new String(rest, i, keyEnd - i, StandardCharsets.UTF_8);
            int valStart = keyEnd + 1;
            int valEnd = indexOfNul(rest, valStart);
            String value = new String(rest, valStart, valEnd - valStart, StandardCharsets.UTF_8);
            params.put(key, value);
            i = valEnd + 1;
        }
        return params;
    }

    private static int indexOfNul(byte[] data, int from) {
        for (int i = from; i < data.length; i++) {
            if (data[i] == 0) {
                return i;
            }
        }
        return data.length;
    }

    // ---- query loop: Simple Query + Extended Query Protocol ---------------

    private void queryLoop(DataInputStream in, DataOutputStream out) throws IOException {
        while (true) {
            int type = in.readUnsignedByte();
            int len = in.readInt();
            byte[] body = new byte[len - 4];
            in.readFully(body);

            switch (type) {
                case 'X' -> { return; } // Terminate
                case 'Q' -> executeSimpleQuery(out, new String(body, 0, body.length - 1, StandardCharsets.UTF_8));
                case 'P' -> dispatchExtended(out, () -> handleParse(out, body));
                case 'B' -> dispatchExtended(out, () -> handleBind(out, body));
                case 'D' -> dispatchExtended(out, () -> handleDescribe(out, body));
                case 'E' -> dispatchExtended(out, () -> handleExecute(out, body));
                case 'C' -> dispatchExtended(out, () -> handleClose(out, body));
                case 'H' -> out.flush(); // Flush -- no response message of its own
                case 'S' -> handleSync(out); // always runs, even mid-error -- this is what clears the error state
                default -> throw new IOException("unsupported pgwire message type: " + (char) type);
            }
        }
    }

    @FunctionalInterface
    private interface ExtendedStep {
        void run() throws IOException, SQLException;
    }

    /** Real Extended Query Protocol semantics: once one message in a pipelined batch errors, every subsequent Bind/Describe/Execute/Close is silently skipped until the client's next Sync — not just the one that failed. */
    private void dispatchExtended(DataOutputStream out, ExtendedStep step) throws IOException {
        if (skipUntilSync) {
            return;
        }
        try {
            step.run();
        } catch (SQLException e) {
            PgMessages.writeErrorResponse(out, sqlState(e), e.getMessage() == null ? "backend error" : e.getMessage());
            out.flush();
            skipUntilSync = true;
        }
    }

    private void handleSync(DataOutputStream out) throws IOException {
        skipUntilSync = false;
        PgMessages.writeReadyForQuery(out, readyForQueryStatus(sessionConnection));
        out.flush();
    }

    private void handleParse(DataOutputStream out, byte[] body) throws IOException {
        PgBodyReader r = new PgBodyReader(body);
        String stmtName = r.readCString();
        String sql = r.readCString();
        int numParamTypes = r.readInt16();
        r.skip(numParamTypes * 4); // param type OIDs -- not tracked; JDBC infers types from the bound Java values
        preparedStatements.put(stmtName, sql);
        PgMessages.writeParseComplete(out);
    }

    private void handleBind(DataOutputStream out, byte[] body) throws IOException, SQLException {
        PgBodyReader r = new PgBodyReader(body);
        String portalName = r.readCString();
        String stmtName = r.readCString();
        int numParamFormats = r.readInt16();
        int[] paramFormats = new int[numParamFormats];
        for (int i = 0; i < numParamFormats; i++) {
            paramFormats[i] = r.readInt16();
        }
        int numParams = r.readInt16();
        List<Object> rawParams = new ArrayList<>(numParams);
        for (int i = 0; i < numParams; i++) {
            int valueLen = r.readInt32();
            if (valueLen == -1) {
                rawParams.add(null);
                continue;
            }
            int format = numParamFormats == 0 ? 0 : paramFormats[Math.min(i, numParamFormats - 1)];
            if (format != 0) {
                throw new IOException("binary-format bind parameters are not supported (param " + (i + 1) + ")");
            }
            rawParams.add(new String(r.readBytes(valueLen), StandardCharsets.UTF_8));
        }
        int numResultFormats = r.readInt16();
        r.skip(numResultFormats * 2); // result format codes -- always text here, so nothing to honor

        String sql = preparedStatements.get(stmtName);
        if (sql == null) {
            throw new IOException("no such prepared statement: " + stmtName);
        }

        Connection backend = sessionConnection(); // shared with Simple Query -- see that method's javadoc
        if (handleTransactionControl(backend, sql)) {
            portals.put(portalName, new Portal(sql, ExecutionResult.ofUpdate(0)));
            PgMessages.writeBindComplete(out);
            return;
        }

        List<Object> orderedBinds = new ArrayList<>();
        String jdbcSql = rewriteDollarParams(sql, rawParams, orderedBinds);

        terminalExecutor.rebind(backend);
        Statement statement = Statement.of(SourceDialect.POSTGRES, jdbcSql, orderedBinds);
        ExecutionResult result = pipeline.execute(statement);
        portals.put(portalName, new Portal(sql, result));
        PgMessages.writeBindComplete(out);
    }

    /**
     * Postgres's own {@code $1}/{@code $2} positional parameter syntax isn't valid JDBC
     * {@code PreparedStatement} placeholder syntax (which only understands {@code ?}, bound
     * strictly by textual occurrence order) — {@link JdbcBackendExecutor} binds positionally, so
     * this rewrites each {@code $N} to {@code ?} and builds {@code orderedBinds} to match, handling
     * repeats and out-of-order references (e.g. {@code $2} appearing in the text before {@code $1})
     * correctly by looking up {@code rawParams.get(N - 1)} at each occurrence rather than assuming
     * {@code $N} appears in order.
     */
    private static String rewriteDollarParams(String sql, List<Object> rawParams, List<Object> orderedBinds) {
        Matcher matcher = DOLLAR_PARAM.matcher(sql);
        StringBuilder rewritten = new StringBuilder();
        int last = 0;
        while (matcher.find()) {
            rewritten.append(sql, last, matcher.start());
            rewritten.append('?');
            int index = Integer.parseInt(matcher.group(1)) - 1;
            orderedBinds.add(index >= 0 && index < rawParams.size() ? rawParams.get(index) : null);
            last = matcher.end();
        }
        rewritten.append(sql, last, sql.length());
        return rewritten.toString();
    }

    private void handleDescribe(DataOutputStream out, byte[] body) throws IOException {
        PgBodyReader r = new PgBodyReader(body);
        char kind = (char) r.readByte();
        String name = r.readCString();
        if (kind == 'S') {
            // Describe Statement, before Bind -- see class javadoc: not pre-analyzed, honestly
            // reported as zero parameters and no known row shape rather than guessed.
            PgMessages.writeParameterDescription(out, 0);
            PgMessages.writeNoData(out);
            return;
        }
        Portal portal = portals.get(name);
        if (portal == null) {
            throw new IOException("no such portal: " + name);
        }
        if (portal.result().isQuery()) {
            PgMessages.writeRowDescription(out, portal.result().columnNames(), portal.result().columnJdbcTypes());
        } else {
            PgMessages.writeNoData(out);
        }
    }

    private void handleExecute(DataOutputStream out, byte[] body) throws IOException {
        PgBodyReader r = new PgBodyReader(body);
        String portalName = r.readCString();
        int maxRows = r.readInt32();
        Portal portal = portals.get(portalName);
        if (portal == null) {
            throw new IOException("no such portal: " + portalName);
        }
        if (!portal.result().isQuery()) {
            PgMessages.writeCommandComplete(out, commandTag(portal.sqlText(), (int) portal.result().updateCount()));
            return;
        }
        List<List<Object>> rows = portal.result().rows();
        int start = portal.nextRow()[0];
        int end = maxRows <= 0 ? rows.size() : Math.min(rows.size(), start + maxRows);
        for (int i = start; i < end; i++) {
            PgMessages.writeDataRow(out, rows.get(i));
        }
        portal.nextRow()[0] = end;
        if (end < rows.size()) {
            PgMessages.writePortalSuspended(out); // maxRows cut it short -- client is expected to Execute again for the rest
        } else {
            PgMessages.writeCommandComplete(out, "SELECT " + rows.size());
        }
    }

    private void handleClose(DataOutputStream out, byte[] body) throws IOException {
        PgBodyReader r = new PgBodyReader(body);
        char kind = (char) r.readByte();
        String name = r.readCString();
        if (kind == 'S') {
            preparedStatements.remove(name);
        } else {
            portals.remove(name);
        }
        PgMessages.writeCloseComplete(out);
    }

    private void executeSimpleQuery(DataOutputStream out, String sql) throws IOException {
        try {
            Connection backend = sessionConnection(); // shared with Extended Query Protocol -- see that method's javadoc
            if (handleTransactionControl(backend, sql)) {
                PgMessages.writeCommandComplete(out, sql.strip().split("\\s+", 2)[0].toUpperCase(java.util.Locale.ROOT));
                PgMessages.writeReadyForQuery(out, readyForQueryStatus(backend));
                out.flush();
                return;
            }
            terminalExecutor.rebind(backend); // see field javadoc — pipeline itself is session-scoped, not rebuilt here
            Statement statement = Statement.of(SourceDialect.POSTGRES, sql, List.of());
            ExecutionResult result = pipeline.execute(statement);
            if (result.isQuery()) {
                PgMessages.writeRowDescription(out, result.columnNames(), result.columnJdbcTypes());
                for (List<Object> row : result.rows()) {
                    PgMessages.writeDataRow(out, row);
                }
                PgMessages.writeCommandComplete(out, "SELECT " + result.rows().size());
            } else {
                PgMessages.writeCommandComplete(out, commandTag(sql, (int) result.updateCount()));
            }
            PgMessages.writeReadyForQuery(out, readyForQueryStatus(backend));
            out.flush();
        } catch (SQLException e) {
            PgMessages.writeErrorAndReady(out, sqlState(e), e.getMessage() == null ? "backend error" : e.getMessage());
            out.flush();
        }
    }

    private static String sqlState(SQLException e) {
        String state = e.getSQLState();
        return state == null || state.isBlank() ? "58000" : state;
    }

    private static String commandTag(String sql, int updateCount) {
        String verb = sql.strip().split("\\s+", 2)[0].toUpperCase(java.util.Locale.ROOT);
        return switch (verb) {
            case "INSERT" -> "INSERT 0 " + updateCount;
            case "UPDATE" -> "UPDATE " + updateCount;
            case "DELETE" -> "DELETE " + updateCount;
            default -> verb; // CREATE/DROP/ALTER/BEGIN/COMMIT etc. take no row count
        };
    }
}
