package com.sayonora.wire.pgwire;

import com.sayonora.wire.config.FailedStatementLog;
import com.sayonora.wire.core.ExecutionResult;
import com.sayonora.wire.core.JdbcBackendExecutor;
import com.sayonora.wire.core.SourceDialect;
import com.sayonora.wire.core.Statement;
import com.sayonora.wire.core.StatementPipeline;
import com.sayonora.wire.core.UntranslatableQueryException;
import com.sayonora.wire.auth.CredentialStore;
import com.sayonora.wire.server.ServerOptions;
import com.sayonora.wire.server.TlsSupport;
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

public final class PgWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PgWireSessionHandler.class);

    private final Socket clientSocket;
    
    private volatile Socket activeSocket;
    private final ServerOptions options;
    private final CredentialStore credentials = new CredentialStore();
    
    private final com.sayonora.wire.auth.PgRoleAuthCache roleAuthCache;

    private boolean authenticate(String username, String presentedPassword) {
        if (roleAuthCache != null) {
            return roleAuthCache.verify(username, presentedPassword);
        }
        byte[] expected = credentials.lookupPassword(username);
        return expected != null && presentedPassword.equals(new String(expected, StandardCharsets.UTF_8));
    }
    
    private final JdbcBackendExecutor terminalExecutor;
    private final com.sayonora.wire.audit.AuditLog auditLog;
    private volatile com.sayonora.wire.core.AccessContext accessContext = com.sayonora.wire.core.AccessContext.ANONYMOUS;
    
    private final com.sayonora.wire.core.RoutingBackendExecutor routingExecutor;
    private final StatementPipeline pipeline;
    private final com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;

    private final FailedStatementLog failedStatementLog;

    private Connection sessionConnection;
    /** @param sql the prepared statement's original {@code $n}-placeholder SQL text
     *  @param paramTypeOids the real Postgres type OIDs the client declared in its own Parse
     *      message, one per {@code $n} placeholder, in order -- 0 for a slot the client left
     *      unspecified (meaning "infer it," which a real client only does for text-format binds;
     *      a binary-format bind always names a real OID). Needed to decode a binary-format Bind
     *      value correctly -- see {@link PgBinaryParamDecoder} -- which {@code handleParse} used
     *      to discard entirely (a real gap: every binary-format parameter was refused outright,
     *      since there was no way left to know what shape it was in). */
    private record PreparedStatementInfo(String sql, int[] paramTypeOids) {
    }

    private final Map<String, PreparedStatementInfo> preparedStatements = new LinkedHashMap<>();
    private final Map<String, Portal> portals = new LinkedHashMap<>();
    private boolean skipUntilSync;

    private volatile String lastExtendedSql;

    // Tracks transaction state locally instead of asking the JDBC driver (connection.getAutoCommit())
    // on every single statement -- readyForQueryStatus() used to do that unconditionally, which is a
    // real driver/socket call, not a free field read. handleTransactionControl is the only place this
    // ever changes, so mirroring its transitions here is exact, not a heuristic.
    private boolean inTransaction;

    private static final Pattern DOLLAR_PARAM = Pattern.compile("\\$(\\d+)");

    // Cheap prefilter for handleTransactionControl -- anchored, case-insensitive, matches only the
    // five verbs that can possibly be transaction control. Lets every ordinary SELECT/INSERT/UPDATE/
    // DELETE statement (the overwhelming majority of traffic) skip the strip/substring/split/
    // toUpperCase chain entirely and fall straight through on this one regex match, the same
    // fast-reject shape mywire/mssqlwire already use for their own per-statement SET-statement check.
    private static final Pattern TXN_CONTROL_PREFIX = Pattern.compile(
            "^\\s*(begin|start|commit|end|rollback)\\b", Pattern.CASE_INSENSITIVE);

    /** @param resultFormatCodes the client's own requested result format per column, from its
     *      Bind message -- empty means "text for every column" (the common case), one entry
     *      means "this format for every column," N entries means one per column, all per the
     *      real pgwire wire format's own broadcast rule. Resolved against
     *      {@link PgBinaryResultEncoder#supports} once in {@link #binaryColumnsFor} rather than
     *      per row, since every row in one portal's result set shares the same column types. */
    private record Portal(String sqlText, ExecutionResult result, int[] nextRow, int[] resultFormatCodes) {
        Portal(String sqlText, ExecutionResult result, int[] resultFormatCodes) {
            this(sqlText, result, new int[1], resultFormatCodes);
        }

        Portal(String sqlText, ExecutionResult result) {
            this(sqlText, result, new int[1], new int[0]);
        }
    }

    /** Resolves this portal's requested-per-column format codes against what
     * {@link PgBinaryResultEncoder} can actually encode -- a column whose OID has no binary
     * encoder stays text even if the client asked for binary (encoding it as raw UTF-8 text bytes
     * while RowDescription claims binary would be a real protocol violation, not a graceful
     * fallback). {@code null} when the portal isn't a query at all (no columns to resolve). */
    private static boolean[] binaryColumnsFor(Portal portal) {
        if (!portal.result().isQuery()) {
            return null;
        }
        List<Integer> jdbcTypes = portal.result().columnJdbcTypes();
        int[] codes = portal.resultFormatCodes();
        boolean[] binary = new boolean[jdbcTypes.size()];
        for (int i = 0; i < binary.length; i++) {
            int requested = codes.length == 0 ? 0 : codes[codes.length == 1 ? 0 : Math.min(i, codes.length - 1)];
            binary[i] = requested != 0 && PgBinaryResultEncoder.supports(PgMessages.oidForJdbcType(jdbcTypes.get(i)));
        }
        return binary;
    }

    public PgWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.sayonora.wire.core.PipelineStage> sharedStages, com.sayonora.wire.core.BackendRegistry backendRegistry) {
        this(clientSocket, options, sharedStages, backendRegistry, null, null);
    }

    public PgWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.sayonora.wire.core.PipelineStage> sharedStages, com.sayonora.wire.core.BackendRegistry backendRegistry,
            com.sayonora.wire.auth.PgRoleAuthCache roleAuthCache) {
        this(clientSocket, options, sharedStages, backendRegistry, roleAuthCache, null);
    }

    public PgWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.sayonora.wire.core.PipelineStage> sharedStages, com.sayonora.wire.core.BackendRegistry backendRegistry,
            com.sayonora.wire.auth.PgRoleAuthCache roleAuthCache, com.sayonora.wire.audit.AuditLog auditLog) {
        this.clientSocket = clientSocket;
        this.options = options;
        // Real, per-connection identity for whichever login actually verified a distinct Postgres
        // role (roleAuthCache != null) -- propagated into every Statement this session executes,
        // and from there into JdbcBackendExecutor's native-RLS session-context call (see
        // terminalExecutor below), so a real Postgres RLS policy on the backend can key off
        // current_setting('warp.user_id'). Stays AccessContext.ANONYMOUS for the
        // shared-credential fallback -- that path has no real distinguishable identity to assert.
        this.terminalExecutor = new JdbcBackendExecutor(null, new com.sayonora.wire.core.access.PostgresRlsSessionInitializer());
        this.routingExecutor = new com.sayonora.wire.core.RoutingBackendExecutor(backendRegistry, terminalExecutor,
                new com.sayonora.wire.xa.XaRecoveryLog(options),
                com.sayonora.wire.core.RouterStage.shardRulesIn(sharedStages), com.sayonora.wire.core.RouterStage.tableShardRulesIn(sharedStages))
                .withFederationSupport(com.sayonora.wire.core.RouterStage.statisticsStoreIn(sharedStages),
                        com.sayonora.wire.core.RouterStage.planStoreIn(sharedStages));
        this.pipeline = new StatementPipeline(sharedStages, routingExecutor);
        this.sqlMetrics = com.sayonora.wire.core.StatsCollectorStage.findIn(sharedStages);
        this.failedStatementLog = new FailedStatementLog(options);
        this.failedStatementLog.ensureSchema();
        this.roleAuthCache = roleAuthCache;
        this.auditLog = auditLog;
    }

    @Override
    public void run() {
        activeSocket = clientSocket;
        try {
            DataInputStream in = new DataInputStream(activeSocket.getInputStream());
            DataOutputStream out = new DataOutputStream(activeSocket.getOutputStream());

            StartupStreams startup = performStartup(in, out);
            if (startup == null) {
                return;
            }
            queryLoop(startup.in(), startup.out());
        } catch (java.io.EOFException e) {
            
        } catch (Exception e) {
            log.warn("pgwire session terminated: {}", e.getMessage(), e);
        } finally {
            try {
                activeSocket.close();
            } catch (IOException ignoredOnSessionTeardown) {
                
            }
            try {
                routingExecutor.endTransaction(false);
            } catch (SQLException ignoredOnSessionTeardown) {
                
            }
            if (sessionConnection != null) {
                try {
                    sessionConnection.close();
                } catch (SQLException ignoredOnSessionTeardown) {
                    
                }
            }
        }
    }

    private Connection sessionConnection() throws SQLException {
        if (sessionConnection == null) {
            sessionConnection = PgConnections.open(options);
            sessionConnection.setAutoCommit(true);
        }
        return sessionConnection;
    }

    private boolean handleTransactionControl(Connection connection, String sql) throws SQLException {
        Matcher prefix = TXN_CONTROL_PREFIX.matcher(sql);
        if (!prefix.find()) {
            return false;
        }
        String verb = prefix.group(1).toUpperCase(java.util.Locale.ROOT);
        switch (verb) {
            case "BEGIN", "START" -> {
                connection.setAutoCommit(false);
                inTransaction = true;
                routingExecutor.beginTransaction();
            }
            case "COMMIT", "END" -> {
                connection.commit();
                connection.setAutoCommit(true);
                inTransaction = false;
                routingExecutor.endTransaction(true);
            }
            case "ROLLBACK" -> {
                connection.rollback();
                connection.setAutoCommit(true);
                inTransaction = false;
                routingExecutor.endTransaction(false);
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads locally-tracked state instead of {@code connection.getAutoCommit()} -- that used to be
     * called on every statement's reply, a real JDBC/driver call (and potentially a socket round
     * trip) for something {@link #handleTransactionControl} already knows exactly, and is the only
     * place that ever changes it.
     */
    private char readyForQueryStatus() {
        return inTransaction ? 'T' : 'I';
    }

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
        String password = new String(body, 0, body.length - 1, StandardCharsets.UTF_8);

        if (!authenticate(username, password)) {
            if (auditLog != null) {
                auditLog.record(com.sayonora.wire.audit.AuditEvent.of(
                        com.sayonora.wire.audit.AuditEvent.Type.DB_LOGIN_FAILED, username,
                        "pgwire login failed for user \"" + username + "\""));
            }
            PgMessages.writeErrorAndReady(out, "28P01", "password authentication failed for user \"" + username + "\"");
            return null;
        }
        if (roleAuthCache != null) {
            // Only when a real, distinguishable Postgres role was actually verified -- the
            // shared-credential fallback has no real per-connection identity to assert, and
            // asserting one anyway would let a single shared secret masquerade as a real user for
            // native-RLS/audit purposes.
            accessContext = new com.sayonora.wire.core.AccessContext(username, java.util.Set.of(), java.util.Map.of());
        }
        if (auditLog != null) {
            auditLog.record(com.sayonora.wire.audit.AuditEvent.of(
                    com.sayonora.wire.audit.AuditEvent.Type.DB_LOGIN_SUCCEEDED, username,
                    "pgwire login succeeded for user \"" + username + "\""
                            + (roleAuthCache != null ? " (real Postgres role)" : " (shared credential)")));
        }

        PgMessages.writeAuthOk(out);
        PgMessages.writeParameterStatus(out, "server_version", "14.0 (warp)");
        PgMessages.writeParameterStatus(out, "client_encoding", "UTF8");
        // Real gap found live fixing binary-format timestamp results: pgjdbc checks this exact
        // startup parameter to decide whether a binary date/timestamp value is the modern
        // int64-microseconds-since-2000-01-01 encoding (PgBinaryResultEncoder/PgBinaryParamDecoder
        // both assume this -- correct for every real Postgres server since 8.4) or the pre-8.4
        // float8-seconds encoding. Every real Postgres server still sends this parameter for
        // exactly that driver-compatibility reason, even though the underlying GUC has been
        // hardcoded "on" (unconfigurable) since 8.4 -- omitting it left pgjdbc to guess, and it
        // guessed wrong, silently decoding a correct binary timestamp as 2000-01-01 (its epoch).
        PgMessages.writeParameterStatus(out, "integer_datetimes", "on");
        PgMessages.writeBackendKeyData(out);
        PgMessages.writeReadyForQuery(out, 'I');
        out.flush();
        return new StartupStreams(in, out);
    }

    private record StartupMessage(DataInputStream in, DataOutputStream out, Map<String, String> params) {
    }

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
                    out.writeByte('N');
                    out.flush();
                }
                continue;
            }
            if (code == PgMessages.GSSENC_REQUEST_CODE) {
                out.writeByte('N');
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

    private void queryLoop(DataInputStream in, DataOutputStream out) throws IOException {
        while (true) {
            int type = in.readUnsignedByte();
            int len = in.readInt();
            byte[] body = new byte[len - 4];
            in.readFully(body);

            switch (type) {
                case 'X' -> { return; }
                case 'Q' -> {
                    String simpleQuerySql = new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
                    // RTT: from here (request fully parsed) through executeSimpleQuery's own
                    // out.flush() -- the one client-message-in, response-out boundary the simple
                    // query protocol actually has. Extended protocol (P/B/D/E/C below) can't offer
                    // the same honest span -- see SqlMetricsCollector's RTT javadoc.
                    long rttStart = System.nanoTime();
                    executeSimpleQuery(out, simpleQuerySql);
                    if (sqlMetrics != null) {
                        sqlMetrics.recordRtt(SourceDialect.POSTGRES, simpleQuerySql, System.nanoTime() - rttStart);
                    }
                }
                case 'P' -> dispatchExtended(out, () -> handleParse(out, body));
                case 'B' -> dispatchExtended(out, () -> handleBind(out, body));
                case 'D' -> dispatchExtended(out, () -> handleDescribe(out, body));
                case 'E' -> dispatchExtended(out, () -> handleExecute(out, body));
                case 'C' -> dispatchExtended(out, () -> handleClose(out, body));
                case 'H' -> out.flush();
                case 'S' -> handleSync(out);
                default -> throw new IOException("unsupported pgwire message type: " + (char) type);
            }
        }
    }

    @FunctionalInterface
    private interface ExtendedStep {
        void run() throws IOException, SQLException;
    }

    private void dispatchExtended(DataOutputStream out, ExtendedStep step) throws IOException {
        if (skipUntilSync) {
            return;
        }
        try {
            step.run();
        } catch (SQLException e) {

            routingExecutor.markTransactionFailed();
            if (lastExtendedSql != null) {
                recordFailure(lastExtendedSql, e);
            }
            PgMessages.writeErrorResponse(out, sqlState(e), e.getMessage() == null ? "backend error" : e.getMessage());
            out.flush();
            skipUntilSync = true;
        } catch (IOException e) {
            // Real bug, found live auditing this frontend: PgBinaryParamDecoder throws a plain
            // IOException (not SQLException) for a bind value whose binary-format type it doesn't
            // decode (UUID/JSON/JSONB/NUMERIC/arrays -- see that class's own javadoc). Before this
            // fix, that IOException propagated straight out of dispatchExtended and up through
            // queryLoop uncaught, killing the ENTIRE connection over one unsupported parameter
            // type -- turning "this one statement can't bind a UUID" into "this client's session
            // is now dead," including every other unrelated statement already pending on it.
            // Every real Postgres server error is recoverable at the statement level; this must
            // be too. IOExceptions from genuine socket I/O failure (a truly dead connection)
            // still ultimately terminate the session the same way they always did -- they just
            // do it via the normal write-side IOException the next real I/O call raises, not via
            // this decode-time exception being conflated with one.
            // Not recordFailure: this is a protocol-level bind-decode failure before the
            // statement ever reached the backend, not a real backend SQL failure -- there's
            // nothing meaningful to attribute to the backend/failed-statement log here.
            routingExecutor.markTransactionFailed();
            PgMessages.writeErrorResponse(out, "08P01", e.getMessage() == null ? "protocol error" : e.getMessage());
            out.flush();
            skipUntilSync = true;
        }
    }

    private void handleSync(DataOutputStream out) throws IOException {
        skipUntilSync = false;
        PgMessages.writeReadyForQuery(out, readyForQueryStatus());
        out.flush();
    }

    private void handleParse(DataOutputStream out, byte[] body) throws IOException {
        PgBodyReader r = new PgBodyReader(body);
        String stmtName = r.readCString();
        String sql = r.readCString();
        int numParamTypes = r.readInt16();
        int[] paramTypeOids = new int[numParamTypes];
        for (int i = 0; i < numParamTypes; i++) {
            paramTypeOids[i] = r.readInt32();
        }
        preparedStatements.put(stmtName, new PreparedStatementInfo(sql, paramTypeOids));
        PgMessages.writeParseComplete(out);
    }

    private void handleBind(DataOutputStream out, byte[] body) throws IOException, SQLException {
        PgBodyReader r = new PgBodyReader(body);
        String portalName = r.readCString();
        String stmtName = r.readCString();
        PreparedStatementInfo stmtInfo = preparedStatements.get(stmtName);
        if (stmtInfo == null) {
            throw new IOException("no such prepared statement: " + stmtName);
        }
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
            byte[] valueBytes = r.readBytes(valueLen);
            int oid = i < stmtInfo.paramTypeOids().length ? stmtInfo.paramTypeOids()[i] : 0;
            if (format == 0) {
                rawParams.add(PgBinaryParamDecoder.decodeText(oid, new String(valueBytes, StandardCharsets.UTF_8)));
            } else {
                // oid above is the real Postgres type OID the client declared for this
                // placeholder in its own Parse message -- see PreparedStatementInfo's javadoc
                // for why this has to be threaded through from there instead of guessed here.
                rawParams.add(PgBinaryParamDecoder.decode(oid, valueBytes, i));
            }
        }
        int numResultFormats = r.readInt16();
        int[] resultFormatCodes = new int[numResultFormats];
        for (int i = 0; i < numResultFormats; i++) {
            resultFormatCodes[i] = r.readInt16();
        }

        String sql = stmtInfo.sql();
        lastExtendedSql = sql;

        Connection backend = sessionConnection();
        if (handleTransactionControl(backend, sql)) {
            portals.put(portalName, new Portal(sql, ExecutionResult.ofUpdate(0), resultFormatCodes));
            PgMessages.writeBindComplete(out);
            return;
        }

        List<Object> orderedBinds = new ArrayList<>();
        String jdbcSql = rewriteDollarParams(sql, rawParams, orderedBinds);

        terminalExecutor.rebind(backend);
        Statement statement = Statement.of(SourceDialect.POSTGRES, jdbcSql, orderedBinds, accessContext);
        ExecutionResult result = pipeline.execute(statement);
        portals.put(portalName, new Portal(sql, result, resultFormatCodes));
        PgMessages.writeBindComplete(out);
    }

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
            // Real bug, found live auditing this frontend for GA transparency: this always
            // claimed 0 parameters regardless of the real statement -- but the client's own real
            // declared OIDs from its Parse message are sitting right here in preparedStatements
            // the whole time (see PreparedStatementInfo's own javadoc). Some drivers/tools use
            // Describe(Statement) for real parameter-count/type introspection (e.g. building
            // ParameterMetaData) and would misbehave being told a parameterized statement has no
            // parameters at all.
            PreparedStatementInfo stmtInfo = preparedStatements.get(name);
            PgMessages.writeParameterDescription(out, stmtInfo == null ? new int[0] : stmtInfo.paramTypeOids());
            PgMessages.writeNoData(out);
            return;
        }
        Portal portal = portals.get(name);
        if (portal == null) {
            throw new IOException("no such portal: " + name);
        }
        if (portal.result().isQuery()) {
            PgMessages.writeRowDescription(out, portal.result().columnNames(), portal.result().columnJdbcTypes(),
                    binaryColumnsFor(portal));
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
        // RTT: Execute's own span is honest on its own -- unlike Bind (which ran the query but
        // sends nothing) it does no backend re-execution and has no client-paced gap inside it,
        // just "read this request, stream rows, write the response" -- see SqlMetricsCollector's
        // RTT javadoc for why Bind itself never gets a sample.
        long rttStart = System.nanoTime();
        try {
            if (!portal.result().isQuery()) {
                PgMessages.writeCommandComplete(out, commandTag(portal.sqlText(), (int) portal.result().updateCount()));
                return;
            }
            List<List<Object>> rows = portal.result().rows();
            boolean[] binaryColumns = binaryColumnsFor(portal);
            int start = portal.nextRow()[0];
            int end = maxRows <= 0 ? rows.size() : Math.min(rows.size(), start + maxRows);
            for (int i = start; i < end; i++) {
                PgMessages.writeDataRow(out, rows.get(i), portal.result().columnJdbcTypes(), binaryColumns);
            }
            portal.nextRow()[0] = end;
            if (end < rows.size()) {
                PgMessages.writePortalSuspended(out);
            } else {
                PgMessages.writeCommandComplete(out, "SELECT " + rows.size());
            }
        } finally {
            if (sqlMetrics != null) {
                sqlMetrics.recordRtt(SourceDialect.POSTGRES, portal.sqlText(), System.nanoTime() - rttStart);
            }
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
            Connection backend = sessionConnection();
            if (handleTransactionControl(backend, sql)) {
                PgMessages.writeCommandComplete(out, sql.strip().split("\\s+", 2)[0].toUpperCase(java.util.Locale.ROOT));
                PgMessages.writeReadyForQuery(out, readyForQueryStatus());
                out.flush();
                return;
            }
            terminalExecutor.rebind(backend);
            Statement statement = Statement.of(SourceDialect.POSTGRES, sql, List.of(), accessContext);
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
            PgMessages.writeReadyForQuery(out, readyForQueryStatus());
            out.flush();
        } catch (SQLException e) {
            
            routingExecutor.markTransactionFailed();
            recordFailure(sql, e);
            PgMessages.writeErrorAndReady(out, sqlState(e), e.getMessage() == null ? "backend error" : e.getMessage());
            out.flush();
        }
    }

    private void recordFailure(String sql, SQLException e) {
        if (e instanceof UntranslatableQueryException) {
            failedStatementLog.record(SourceDialect.POSTGRES, sql,
                    FailedStatementLog.FailureType.UNTRANSLATABLE, null, null, e.getMessage());
        } else {
            failedStatementLog.record(SourceDialect.POSTGRES, sql,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), null, e.getMessage());
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
            default -> verb;
        };
    }
}
