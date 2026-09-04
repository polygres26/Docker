package com.sayonora.wire.mywire;

import com.sayonora.wire.auth.CredentialStore;
import com.sayonora.wire.config.FailedStatementLog;
import com.sayonora.wire.core.DialectErrorMessages;
import com.sayonora.wire.core.ExecutionResult;
import com.sayonora.wire.core.JdbcBackendExecutor;
import com.sayonora.wire.core.SourceDialect;
import com.sayonora.wire.core.SqlStateErrorMapper;
import com.sayonora.wire.core.Statement;
import com.sayonora.wire.core.StatementPipeline;
import com.sayonora.wire.core.UntranslatableQueryException;
import com.sayonora.wire.pgwire.PgConnections;
import com.sayonora.wire.server.ServerOptions;
import com.sayonora.wire.server.TlsSupport;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MySqlWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MySqlWireSessionHandler.class);
    private static final AtomicLong NEXT_CONNECTION_ID = new AtomicLong(1);

    private final Socket clientSocket;
    
    private volatile Socket activeSocket;
    private final ServerOptions options;
    private final CredentialStore credentials = new CredentialStore();
    
    // See com.sayonora.wire.core.access.MySqlPgEmulationSessionInitializer's own header comment --
    // the mywire equivalent of orawire's OraclePgEmulationSessionInitializer (RequestLoop.java):
    // `SET db_emulation = 'mysql'` once per statement (cheap/idempotent when already set, see
    // pg_oracle.c's own db_emulation_assign_hook), so pg_mysql's unqualified LAST_INSERT_ID(),
    // GROUP_CONCAT(...), DATE_FORMAT(...), etc. resolve without db/pg_mysql needing every caller
    // to schema-qualify mysql_catalog.* by hand.
    private final JdbcBackendExecutor terminalExecutor =
            new JdbcBackendExecutor(null, new com.sayonora.wire.core.access.MySqlPgEmulationSessionInitializer());
    private final com.sayonora.wire.core.RoutingBackendExecutor routingExecutor;
    private final StatementPipeline pipeline;
    // Real gap found auditing this frontend for GA transparency: every statement used to open a
    // BRAND NEW backend connection, force autocommit=true, execute, and close -- meaning
    // START TRANSACTION/COMMIT/ROLLBACK sent by a real MySQL client were just strings executed
    // against a throwaway connection with zero effect on any OTHER statement. Any ORM transaction
    // (ActiveRecord, Django, Hibernate, Sequelize -- virtually every write path in a typical app)
    // was silently non-atomic. Fixed the same way pgwire already does it: one connection lives for
    // the whole session (see sessionConnection()), and BEGIN/COMMIT/ROLLBACK toggle its real
    // autoCommit state and actually commit/rollback, instead of being no-op'd or run against a
    // connection nobody else will ever see again.
    private Connection sessionConnection;
    private boolean inTransaction;
    private final com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;

    private final FailedStatementLog failedStatementLog;

    /** Session-scoped: statement handles are only meaningful within the connection that
     * PREPAREd them, same as real MySQL. */
    private final Map<Integer, PreparedStmt> preparedStatements = new HashMap<>();
    private final AtomicInteger nextStmtId = new AtomicInteger(1);

    private static final class PreparedStmt {
        final String sql;
        final int paramCount;
        // -1 sentinel in [0] means "no cached types yet" -- see MySqlBinaryProtocol.decodeExecuteParams.
        final int[] cachedParamTypes;
        // COM_STMT_SEND_LONG_DATA-accumulated chunks, keyed by parameter index -- populated across
        // possibly many SEND_LONG_DATA packets, consumed (and cleared) by the next EXECUTE. Lazily
        // created since most statements never use it.
        Map<Integer, ByteArrayOutputStream> longData;

        PreparedStmt(String sql, int paramCount) {
            this.sql = sql;
            this.paramCount = paramCount;
            this.cachedParamTypes = new int[Math.max(paramCount, 1)];
            this.cachedParamTypes[0] = -1;
        }
    }

    public MySqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.sayonora.wire.core.PipelineStage> sharedStages, com.sayonora.wire.core.BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.options = options;
        this.routingExecutor = new com.sayonora.wire.core.RoutingBackendExecutor(backendRegistry, terminalExecutor,
                new com.sayonora.wire.xa.XaRecoveryLog(options),
                com.sayonora.wire.core.RouterStage.shardRulesIn(sharedStages), com.sayonora.wire.core.RouterStage.tableShardRulesIn(sharedStages))
                .withFederationSupport(com.sayonora.wire.core.RouterStage.statisticsStoreIn(sharedStages),
                        com.sayonora.wire.core.RouterStage.planStoreIn(sharedStages));
        this.pipeline = new StatementPipeline(sharedStages, routingExecutor);
        this.sqlMetrics = com.sayonora.wire.core.StatsCollectorStage.findIn(sharedStages);
        this.failedStatementLog = new FailedStatementLog(options);
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
                return;
            }
            queryLoop(handshake.in(), handshake.out(), packets);
        } catch (java.io.EOFException e) {
            
        } catch (Exception e) {
            log.warn("mywire session terminated: {}", e.getMessage(), e);
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
            terminalExecutor.rebind(sessionConnection);
        }
        return sessionConnection;
    }

    // MySQL clients express transaction control two ways a real app hits constantly: the SQL
    // verbs (START TRANSACTION/BEGIN/COMMIT/ROLLBACK), and mysql-connector-j's own
    // Connection#setAutoCommit(boolean), which it implements by sending a literal
    // "SET autocommit = 0/1" statement -- not a TDS/wire-level flag, plain SQL text, the same as
    // real MySQL itself expects. Both need to actually toggle this session's real backend
    // connection, not be swallowed as a no-op the way SET_STATEMENT already does for every other
    // SET.
    private static final java.util.regex.Pattern TXN_CONTROL_PREFIX = java.util.regex.Pattern.compile(
            "^\\s*(start\\s+transaction|begin|commit|rollback)\\b", java.util.regex.Pattern.CASE_INSENSITIVE);
    private static final java.util.regex.Pattern SET_AUTOCOMMIT = java.util.regex.Pattern.compile(
            "^\\s*set\\s+(?:session\\s+|@@(?:session\\.)?)?autocommit\\s*=\\s*'?(0|1|off|on|false|true)'?\\s*$",
            java.util.regex.Pattern.CASE_INSENSITIVE);

    private boolean handleTransactionControl(Connection connection, String sql) throws SQLException {
        java.util.regex.Matcher autocommit = SET_AUTOCOMMIT.matcher(sql);
        if (autocommit.matches()) {
            boolean enable = switch (autocommit.group(1).toLowerCase(java.util.Locale.ROOT)) {
                case "0", "off", "false" -> false;
                default -> true;
            };
            if (!enable && !inTransaction) {
                connection.setAutoCommit(false);
                inTransaction = true;
                routingExecutor.beginTransaction();
            } else if (enable && inTransaction) {
                // Real MySQL commits whatever's pending when autocommit is turned back on
                // mid-transaction -- matching that rather than silently discarding it.
                connection.commit();
                connection.setAutoCommit(true);
                inTransaction = false;
                routingExecutor.endTransaction(true);
            }
            return true;
        }
        java.util.regex.Matcher prefix = TXN_CONTROL_PREFIX.matcher(sql);
        if (!prefix.find()) {
            return false;
        }
        String verb = prefix.group(1).replaceAll("\\s+", " ").toUpperCase(java.util.Locale.ROOT);
        switch (verb) {
            case "START TRANSACTION", "BEGIN" -> {
                connection.setAutoCommit(false);
                inTransaction = true;
                routingExecutor.beginTransaction();
            }
            // Real gap found live: a real mysql-connector-j client sends a bare "COMMIT"/
            // "ROLLBACK" even when it never explicitly started a transaction via SQL text (no
            // preceding START TRANSACTION/BEGIN and, confirmed live, no SET autocommit=0 either --
            // its own client-side setAutoCommit(false) apparently updates local state only, with
            // nothing sent over the wire until the actual commit()/rollback() call). Calling
            // connection.commit()/rollback() unconditionally in that case throws a real "Cannot
            // commit when autoCommit is enabled" from the BACKEND's own JDBC connection (still
            // genuinely in autocommit mode, since nothing here ever told it otherwise) -- a hard
            // failure for what real MySQL treats as a harmless no-op (COMMIT/ROLLBACK outside an
            // active transaction commits/rolls back nothing, it's not an error).
            case "COMMIT" -> {
                if (inTransaction) {
                    connection.commit();
                    connection.setAutoCommit(true);
                    inTransaction = false;
                    routingExecutor.endTransaction(true);
                }
            }
            case "ROLLBACK" -> {
                if (inTransaction) {
                    connection.rollback();
                    connection.setAutoCommit(true);
                    inTransaction = false;
                    routingExecutor.endTransaction(false);
                }
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    private record HandshakeStreams(DataInputStream in, OutputStream out) {
    }

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
            response = packets.readPayload(in);
        }

        int[] pos = {0};
        pos[0] += 4;
        pos[0] += 4;
        pos[0] += 1;
        pos[0] += 23;
        String username = MySqlPacket.readNulString(response, pos);
        int authLen = response[pos[0]++] & 0xFF;
        // This first auth_response is NOT always computed with mysql_native_password, even
        // though that's the only plugin this server declares in the handshake and the only one it
        // implements: modern JDBC client defaults (confirmed live -- MySQL Connector/J 8.4 and 9.1
        // both, despite the server's declared plugin) optimistically guess caching_sha2_password
        // for their very first response regardless, and only actually honor the server-declared
        // plugin after an explicit AuthSwitchRequest.
        byte[] firstAuthResponse = Arrays.copyOfRange(response, pos[0], pos[0] + authLen);
        pos[0] += authLen;
        if ((clientCapabilities & MySqlMessages.CLIENT_CONNECT_WITH_DB) != 0) {
            MySqlPacket.readNulString(response, pos);	// database -- not used, just needs skipping
        }
        String firstPluginName = (clientCapabilities & MySqlMessages.CLIENT_PLUGIN_AUTH) != 0
                ? MySqlPacket.readNulString(response, pos)
                : null;

        byte[] expected = credentials.lookupPassword(username);
        String expectedPassword = expected == null ? "" : new String(expected, StandardCharsets.UTF_8);
        byte[] expectedScramble = MySqlMessages.nativePasswordScramble(expectedPassword, scramble);

        // Try the first response directly before falling back to an AuthSwitchRequest -- confirmed
        // live via a raw packet capture that this differs by client, and by more than just the
        // plugin name each one declares: the real native mysql client (Oracle's official CLI,
        // libmysqlclient-based, distinct from the JDBC driver) correctly honors the server-declared
        // plugin on its FIRST response and does not expect a redundant AuthSwitchRequest asking it
        // to "switch" to the exact same plugin it already used -- sending one anyway desyncs its
        // native auth state machine ("ERROR 2012: Error in server handshake"). A JDBC client's
        // first response, even when it names "mysql_native_password" in firstPluginName, doesn't
        // reliably contain a scramble actually computed that way (confirmed live: trusting
        // firstPluginName alone broke real JDBC logins outright, "Access denied", even though the
        // name matched) -- so this checks the ACTUAL bytes against what's expected, not the
        // client's self-declared plugin name, and only falls back to the switch when they don't
        // match. firstPluginName itself ends up unused for this decision, but is still parsed
        // above since CLIENT_PLUGIN_AUTH means it's really on the wire and must be consumed before
        // whatever might follow it.
        byte[] actualResponse = firstAuthResponse;
        if (authLen != expectedScramble.length || !Arrays.equals(expectedScramble, firstAuthResponse)) {
            packets.writePayload(out, MySqlMessages.authSwitchRequest("mysql_native_password", scramble));
            actualResponse = packets.readPayload(in);
        }

        if (!Arrays.equals(expectedScramble, actualResponse)) {
            packets.writePayload(out, MySqlMessages.errPacket(1045, "28000",
                    "Access denied for user '" + username + "'"));
            return null;
        }

        packets.writePayload(out, MySqlMessages.okPacket(0));
        return new HandshakeStreams(in, out);
    }

    private static final int COM_QUIT = 0x01;
    private static final int COM_INIT_DB = 0x02;
    private static final int COM_QUERY = 0x03;
    private static final int COM_STMT_PREPARE = 0x16;
    private static final int COM_STMT_EXECUTE = 0x17;
    private static final int COM_STMT_SEND_LONG_DATA = 0x18;
    private static final int COM_STMT_CLOSE = 0x19;
    private static final int COM_STMT_RESET = 0x1a;
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
                    long rttStart = System.nanoTime();
                    executeQuery(out, packets, sql, List.of());
                    if (sqlMetrics != null) {
                        sqlMetrics.recordRtt(SourceDialect.MYSQL, sql, System.nanoTime() - rttStart);
                    }
                }
                case COM_STMT_PREPARE -> {
                    String sql = new String(payload, 1, payload.length - 1, StandardCharsets.UTF_8);
                    handlePrepare(out, packets, sql);
                }
                case COM_STMT_EXECUTE -> handleExecute(out, packets, payload);
                case COM_STMT_SEND_LONG_DATA -> handleSendLongData(payload);
                case COM_STMT_CLOSE -> {
                    if (payload.length >= 5) {
                        preparedStatements.remove(readInt32LE(payload, 1));
                    }
                    // No response -- COM_STMT_CLOSE is one of the rare commands MySQL never
                    // acknowledges, matching real server behavior.
                }
                case COM_STMT_RESET -> {
                    if (payload.length >= 5) {
                        PreparedStmt stmt = preparedStatements.get(readInt32LE(payload, 1));
                        if (stmt != null && stmt.longData != null) {
                            stmt.longData.clear();
                        }
                    }
                    packets.writePayload(out, MySqlMessages.okPacket(0));
                }
                default -> packets.writePayload(out, MySqlMessages.errPacket(1047, "08S01",
                        "unsupported command: 0x" + Integer.toHexString(command)));
            }
        }
    }

    private void handleSendLongData(byte[] payload) {
        // COM_STMT_SEND_LONG_DATA streams a BLOB/CLOB parameter across possibly many packets, sent
        // separately from -- and excluded entirely from -- the following EXECUTE's own parameter
        // value section (that param's null-bitmap bit stays 0 and its declared type is sent as
        // usual, but no value bytes for it appear in EXECUTE's payload at all). No response is
        // sent for this command either way, matching real MySQL. Layout: command(1) + stmt_id(4) +
        // param_id(2) + data(rest, raw bytes, appended verbatim to whatever arrived before).
        if (payload.length < 7) {
            return;
        }
        PreparedStmt stmt = preparedStatements.get(readInt32LE(payload, 1));
        if (stmt == null) {
            return;
        }
        int paramId = (payload[5] & 0xFF) | ((payload[6] & 0xFF) << 8);
        if (stmt.longData == null) {
            stmt.longData = new HashMap<>();
        }
        stmt.longData.computeIfAbsent(paramId, k -> new ByteArrayOutputStream())
                .write(payload, 7, payload.length - 7);
    }

    private static int readInt32LE(byte[] data, int offset) {
        return (data[offset] & 0xFF) | ((data[offset + 1] & 0xFF) << 8)
                | ((data[offset + 2] & 0xFF) << 16) | ((data[offset + 3] & 0xFF) << 24);
    }

    private void handlePrepare(OutputStream out, MySqlPacket packets, String sql) throws IOException {
        int paramCount = MySqlBinaryProtocol.countPlaceholders(sql);
        List<String> columnNames = List.of();
        if (sql.stripLeading().regionMatches(true, 0, "SELECT", 0, 6)) {
            // Best-effort only: prepare-time column metadata is used purely to size
            // COM_STMT_PREPARE_OK's num_columns declaration, NOT held across PREPARE->EXECUTE --
            // EXECUTE re-runs the full statement against a fresh connection (same as plain
            // COM_QUERY already does) and uses that real execution's own result columns for the
            // actual response. A metadata-fetch failure here just means PREPARE_OK declares 0
            // columns; some clients tolerate that better than others, but EXECUTE's response is
            // unaffected either way.
            try (Connection backend = options.mywireNativeBackend()
                    ? MySqlBackendConnections.open(options) : PgConnections.open(options);
                    PreparedStatement ps = backend.prepareStatement(sql)) {
                ResultSetMetaData md = ps.getMetaData();
                if (md != null) {
                    List<String> names = new ArrayList<>();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        names.add(md.getColumnLabel(i));
                    }
                    columnNames = names;
                }
            } catch (SQLException e) {
                log.debug("mywire: could not fetch prepare-time column metadata for \"{}\" -- "
                        + "PREPARE_OK will declare 0 columns, EXECUTE's response is unaffected: {}",
                        sql, e.getMessage());
            }
        }

        int stmtId = nextStmtId.getAndIncrement();
        preparedStatements.put(stmtId, new PreparedStmt(sql, paramCount));

        ByteArrayOutputStream okBody = new ByteArrayOutputStream();
        okBody.write(0x00);
        MySqlPacket.writeFixedInt(okBody, stmtId, 4);
        MySqlPacket.writeFixedInt(okBody, columnNames.size(), 2);
        MySqlPacket.writeFixedInt(okBody, paramCount, 2);
        okBody.write(0);
        MySqlPacket.writeFixedInt(okBody, 0, 2);
        packets.writePayload(out, okBody.toByteArray());

        for (int i = 0; i < paramCount; i++) {
            packets.writePayload(out, MySqlMessages.columnDefinition("?", Types.VARCHAR));
        }
        if (paramCount > 0) {
            packets.writePayload(out, MySqlMessages.eofPacket());
        }
        for (String name : columnNames) {
            packets.writePayload(out, MySqlMessages.columnDefinition(name, Types.VARCHAR));
        }
        if (!columnNames.isEmpty()) {
            packets.writePayload(out, MySqlMessages.eofPacket());
        }
    }

    private void handleExecute(OutputStream out, MySqlPacket packets, byte[] payload) throws IOException {
        int stmtId = readInt32LE(payload, 1);
        PreparedStmt stmt = preparedStatements.get(stmtId);
        if (stmt == null) {
            packets.writePayload(out, MySqlMessages.errPacket(1243, "HY000",
                    "Unknown prepared statement handle: " + stmtId));
            return;
        }
        List<Object> bindParams;
        try {
            int[] pos = {10}; // command(1) + stmt_id(4) + flags(1) + iteration_count(4)
            Map<Integer, byte[]> longData = null;
            if (stmt.longData != null && !stmt.longData.isEmpty()) {
                longData = new HashMap<>();
                for (Map.Entry<Integer, ByteArrayOutputStream> e : stmt.longData.entrySet()) {
                    longData.put(e.getKey(), e.getValue().toByteArray());
                }
            }
            bindParams = stmt.paramCount == 0
                    ? List.of()
                    : MySqlBinaryProtocol.decodeExecuteParams(payload, pos, stmt.paramCount, stmt.cachedParamTypes, longData);
        } catch (IOException e) {
            packets.writePayload(out, MySqlMessages.errPacket(1210, "HY000",
                    "could not decode COM_STMT_EXECUTE parameters: " + e.getMessage()));
            return;
        } finally {
            // Real MySQL clears accumulated long-data buffers once the statement is executed
            // (or reset/closed) -- a second EXECUTE without a fresh SEND_LONG_DATA must not
            // silently reuse the previous value.
            if (stmt.longData != null) {
                stmt.longData.clear();
            }
        }

        long rttStart = System.nanoTime();
        executeQuery(out, packets, stmt.sql, bindParams, true);
        if (sqlMetrics != null) {
            sqlMetrics.recordRtt(SourceDialect.MYSQL, stmt.sql, System.nanoTime() - rttStart);
        }
    }

    private static final java.util.regex.Pattern SET_STATEMENT =
            java.util.regex.Pattern.compile("^\\s*set\\s+", java.util.regex.Pattern.CASE_INSENSITIVE);

    // Every OTHER "SET ..." is a silent no-op (see SET_STATEMENT's own use below) -- correct for
    // something with no real backend-session equivalent (sql_mode, character_set_*), but
    // time_zone is a real, common exception: any app relying on it (session-local date/time
    // rendering, TIMESTAMPDIFF/CONVERT_TZ-style logic) got silently wrong results using whatever
    // timezone the backend Postgres session happened to default to, no error, nothing to notice.
    // Scope, deliberately narrow: only a UTC numeric-offset value ('+05:00'/'-08:00') or the
    // literal "UTC" is translated to Postgres's own SET TIME ZONE -- both dialects accept that
    // exact syntax unchanged, so no translation of the VALUE itself is needed, just the statement
    // shape. A real IANA zone name ('America/New_York') also happens to work identically in both
    // (Postgres accepts the same names), so it's allowed through too. MySQL's "SYSTEM" keyword
    // has no equivalent meaning here (there is no real MySQL system config to defer to) and is
    // left as a no-op, same as before this fix.
    private static final java.util.regex.Pattern SET_TIME_ZONE = java.util.regex.Pattern.compile(
            "^\\s*set\\s+(?:session\\s+|@@(?:session\\.)?)?time_zone\\s*=\\s*'([^']+)'\\s*$",
            java.util.regex.Pattern.CASE_INSENSITIVE);

    /** @return true if {@code sql} was a real {@code SET time_zone} this rewrote and executed for
     *      real against {@code connection} -- false (nothing done) for anything else, including
     *      MySQL's "SYSTEM" keyword, left to the generic SET_STATEMENT no-op. */
    private static boolean handleSetTimeZone(Connection connection, String sql) throws SQLException {
        java.util.regex.Matcher m = SET_TIME_ZONE.matcher(sql);
        if (!m.matches()) {
            return false;
        }
        String value = m.group(1);
        if ("system".equalsIgnoreCase(value)) {
            return false;
        }
        try (java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute("SET TIME ZONE '" + value.replace("'", "''") + "'");
        }
        return true;
    }

    // "SET NAMES 'utf8mb4'" -- sent by essentially every real MySQL client/driver at connection
    // time, previously a silent no-op like every other SET. Real gap, found auditing this
    // frontend for GA transparency: an app inserting non-ASCII data (the entire point of
    // requesting a specific charset) got whatever encoding the backend Postgres session happened
    // to default to instead, with no error -- a genuine, silent correctness gap for non-Latin
    // text. Scope, deliberately narrow: only the common MySQL charset names that have a real,
    // unambiguous Postgres equivalent are translated; an unrecognized charset name is left as a
    // no-op (same as before this fix) rather than guessed at.
    private static final java.util.regex.Pattern SET_NAMES = java.util.regex.Pattern.compile(
            "^\\s*set\\s+names\\s+'?([A-Za-z0-9_]+)'?", java.util.regex.Pattern.CASE_INSENSITIVE);
    // UTF8-family only, deliberately -- found live: actually issuing "SET client_encoding =
    // 'LATIN1'" against terminalExecutor's own backend connection breaks pgjdbc ITSELF ("The JDBC
    // driver requires client_encoding to be UTF8 for correct operation"), since that's the SAME
    // connection Warp's own backend driver depends on for every other statement in the session,
    // not a separate client-facing-only setting. UTF8/utf8mb4/utf8mb3 (MySQL 8's own default) are
    // the overwhelmingly common real request anyway and are a genuine no-op here (pgjdbc's
    // connection is already UTF8), so this still correctly ACKs the client's request instead of
    // silently ignoring it -- it just never issues a SQL statement pgjdbc itself couldn't survive.
    // A non-UTF8 charset request is left as a no-op, same as before this fix, rather than
    // corrupting the shared backend connection.
    private static final java.util.Set<String> UTF8_FAMILY_CHARSETS = java.util.Set.of("utf8mb4", "utf8mb3", "utf8");

    private static boolean handleSetNames(String sql) {
        java.util.regex.Matcher m = SET_NAMES.matcher(sql);
        return m.matches() && UTF8_FAMILY_CHARSETS.contains(m.group(1).toLowerCase(java.util.Locale.ROOT));
    }

    // "SET [SESSION|GLOBAL] TRANSACTION ISOLATION LEVEL ..." -- MySQL and Postgres share the
    // exact same four standard isolation level names, so no value translation is needed, just the
    // statement shape. Real gap: previously silently swallowed like every other SET, so an app
    // relying on a non-default isolation level (e.g. READ COMMITTED, common for apps ported from
    // Postgres/Oracle conventions) silently ran at whatever level the backend connection happened
    // to already be in.
    private static final java.util.regex.Pattern SET_ISOLATION_LEVEL = java.util.regex.Pattern.compile(
            "^\\s*set\\s+(?:session\\s+|global\\s+)?transaction\\s+isolation\\s+level\\s+"
                    + "(READ\\s+UNCOMMITTED|READ\\s+COMMITTED|REPEATABLE\\s+READ|SERIALIZABLE)\\s*$",
            java.util.regex.Pattern.CASE_INSENSITIVE);

    private static boolean handleSetIsolationLevel(Connection connection, String sql) throws SQLException {
        java.util.regex.Matcher m = SET_ISOLATION_LEVEL.matcher(sql);
        if (!m.matches()) {
            return false;
        }
        try (java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                    + m.group(1).toUpperCase(java.util.Locale.ROOT).replaceAll("\\s+", " "));
        }
        return true;
    }

    private void executeQuery(OutputStream out, MySqlPacket packets, String sql, List<Object> bindParams)
            throws IOException {
        executeQuery(out, packets, sql, bindParams, false);
    }

    /** {@code binaryResult} selects COM_STMT_EXECUTE's binary row/column-definition encoding
     * instead of COM_QUERY's plain text encoding -- see MySqlBinaryProtocol's class doc for why
     * every column is declared/encoded as VAR_STRING either way. */
    private void executeQuery(OutputStream out, MySqlPacket packets, String sql, List<Object> bindParams,
            boolean binaryResult) throws IOException {
        if (!options.mywireNativeBackend()) {
            try {
                if (handleTransactionControl(sessionConnection(), sql)) {
                    packets.writePayload(out, MySqlMessages.okPacket(0));
                    return;
                }
            } catch (SQLException e) {
                String state = sqlState(e);
                packets.writePayload(out, MySqlMessages.errPacket(SqlStateErrorMapper.toMySqlError(state, e.getMessage()),
                        state, e.getMessage() == null ? "backend error" : e.getMessage()));
                return;
            }
        }
        if (!options.mywireNativeBackend()) {
            try {
                if (handleSetTimeZone(sessionConnection(), sql)
                        || handleSetNames(sql)
                        || handleSetIsolationLevel(sessionConnection(), sql)) {
                    packets.writePayload(out, MySqlMessages.okPacket(0));
                    return;
                }
            } catch (SQLException e) {
                String state = sqlState(e);
                packets.writePayload(out, MySqlMessages.errPacket(SqlStateErrorMapper.toMySqlError(state, e.getMessage()),
                        state, e.getMessage() == null ? "backend error" : e.getMessage()));
                return;
            }
        }
        if (!options.mywireNativeBackend() && SET_STATEMENT.matcher(sql).find()) {
            packets.writePayload(out, MySqlMessages.okPacket(0));
            return;
        }
        if (!options.mywireNativeBackend() && MySqlSessionVariableQuery.matches(sql)) {
            MySqlSessionVariableQuery.SyntheticResult synthetic = MySqlSessionVariableQuery.synthesize(sql);
            packets.writePayload(out, columnCountPayload(synthetic.columnNames().size()));
            for (String name : synthetic.columnNames()) {
                packets.writePayload(out, MySqlMessages.columnDefinition(name, Types.VARCHAR));
            }
            packets.writePayload(out, MySqlMessages.eofPacket());
            packets.writePayload(out, binaryResult
                    ? MySqlBinaryProtocol.encodeRow(synthetic.row())
                    : MySqlMessages.textRow(synthetic.row()));
            packets.writePayload(out, MySqlMessages.eofPacket());
            return;
        }

        Statement statement = Statement.of(SourceDialect.MYSQL, sql, bindParams);
        try {
            ExecutionResult result;
            try {
                if (options.mywireNativeBackend()) {
                    // No pin: leave targetBackend unset and let RouterStage resolve it like any
                    // other statement -- same generalization as mssqlwire's own native mode (see
                    // its identical comment). Any configured WARP_ROUTER_*/WARP_TABLE_SHARDS rule
                    // can route this statement to ANY registered backend of ANY dialect; with no
                    // rule matched, RouterStage.resolveUnambiguousDefault() falls back to the sole
                    // registered MYSQL-dialect backend if exactly one exists -- "mysql-native",
                    // registered by Main.java only when WARP_MYWIRE_BACKEND=mysql is set -- same
                    // single-target behavior as before, just reached generically. Register a
                    // SECOND MySQL target via WARP_BACKENDS and that fallback stops being
                    // unambiguous, requiring an explicit rule instead. DialectTranslationStage
                    // no-ops once resolved to a same-dialect target, so the client's real SQL still
                    // reaches a real MySQL backend untouched. FirewallStage/QosControlStage/
                    // CacheStage still run unmodified. RoutingBackendExecutor.execute() resolves
                    // any non-default named target via its own executeOnFreshConnection path -- a
                    // pooled connection via BackendTarget#open() and a plain
                    // JdbcBackendExecutor(Connection) with NO NativeRlsSessionInitializer, unlike
                    // this session's own terminalExecutor (bound to MySqlPgEmulationSessionInitializer,
                    // Postgres-only session setup a real MySQL backend has no use for).
                    //
                    // EXCEPT for dual-port mode's own native listener (see ServerOptions#
                    // mywireNativeViaDualPort's own javadoc): there, this same ambiguous fallback
                    // would ALSO match the TRANSLATED listener's own statements once BOTH are
                    // registered at once (a real bug, found live) -- so that one case pins
                    // targetBackend explicitly to its own reserved dual-port name instead of
                    // relying on the fallback at all.
                    if (options.mywireNativeViaDualPort()) {
                        statement = statement.withRouting(statement.workloadClass(),
                                com.sayonora.wire.core.BackendRegistry.MYSQL_NATIVE_DUAL_PORT_NAME);
                    }
                    result = pipeline.execute(statement);
                } else {
                    // One connection for the whole session (see sessionConnection()'s own
                    // javadoc, which also does the one-time terminalExecutor.rebind()), not a
                    // fresh one per statement -- required for BEGIN/COMMIT/ROLLBACK (handled
                    // above) to mean anything across statements. Deliberately NOT calling
                    // rebind() again here on every statement: that would defeat
                    // JdbcBackendExecutor's own prepared-statement cache (documented in its own
                    // class -- rebind() closes and clears every cached entry, since it exists for
                    // a genuine reconnect/failover, not routine reuse of the same connection).
                    sessionConnection();
                    result = pipeline.execute(statement);
                }
            } catch (UntranslatableQueryException e) {
                failedStatementLog.record(SourceDialect.MYSQL, sql,
                        FailedStatementLog.FailureType.UNTRANSLATABLE, null, null, e.getMessage());
                packets.writePayload(out, MySqlMessages.errPacket(SqlStateErrorMapper.MYSQL_DEFAULT, e.getSQLState(),
                        e.getMessage() == null ? "statement could not be translated" : e.getMessage()));
                return;
            }
            if (result.isQuery()) {
                List<String> columnNames = result.columnNames();
                List<Integer> columnJdbcTypes = result.columnJdbcTypes();
                packets.writePayload(out, columnCountPayload(columnNames.size()));
                for (int i = 0; i < columnNames.size(); i++) {
                    int declaredType = binaryResult ? Types.VARCHAR : columnJdbcTypes.get(i);
                    packets.writePayload(out, MySqlMessages.columnDefinition(columnNames.get(i), declaredType));
                }
                packets.writePayload(out, MySqlMessages.eofPacket());
                for (List<Object> row : result.rows()) {
                    packets.writePayload(out, binaryResult ? MySqlBinaryProtocol.encodeRow(row) : MySqlMessages.textRow(row));
                }
                packets.writePayload(out, MySqlMessages.eofPacket());
            } else {
                packets.writePayload(out, MySqlMessages.okPacket(result.updateCount(), result.generatedKey()));
            }
        } catch (SQLException e) {
            String state = sqlState(e);
            int nativeError = SqlStateErrorMapper.toMySqlError(state, e.getMessage());
            failedStatementLog.record(SourceDialect.MYSQL, sql,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), nativeError, e.getMessage());
            // The client sees real MySQL error text ("Table 'x' doesn't exist", etc.) when a
            // dialect-native template exists for this SQLSTATE -- the audit log line above still
            // records Postgres's own raw message.
            String clientMessage = e.getMessage() == null ? "backend error"
                    : DialectErrorMessages.render(SourceDialect.MYSQL, state, e.getMessage());
            packets.writePayload(out, MySqlMessages.errPacket(nativeError, state, clientMessage));
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
