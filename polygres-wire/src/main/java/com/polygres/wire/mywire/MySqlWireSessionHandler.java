package com.polygres.wire.mywire;

import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
import com.polygres.wire.pgwire.PgConnections;
import com.polygres.wire.server.ServerOptions;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
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
    private final ServerOptions options;
    private final CredentialStore credentials = new CredentialStore();
    // RTT optimization (ARCHITECTURE.md §11) — same reasoning as PgWireSessionHandler's identical
    // fields: built once per session, rebound per statement, not rebuilt per statement.
    private final JdbcBackendExecutor terminalExecutor = new JdbcBackendExecutor(null);
    private final StatementPipeline pipeline;

    public MySqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.polygres.wire.core.PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.options = options;
        this.pipeline = new StatementPipeline(sharedStages,
                new com.polygres.wire.core.RoutingBackendExecutor(backendRegistry, terminalExecutor));
    }

    @Override
    public void run() {
        try (Socket socket = clientSocket) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            OutputStream out = socket.getOutputStream();
            MySqlPacket packets = new MySqlPacket();

            String username = performHandshake(in, out, packets);
            if (username == null) {
                return; // auth failed
            }
            queryLoop(in, out, packets);
        } catch (java.io.EOFException e) {
            // client disconnected mid-message; not worth logging as a warning
        } catch (Exception e) {
            log.warn("mywire session terminated: {}", e.getMessage(), e);
        }
    }

    // ---- handshake + auth --------------------------------------------------

    private String performHandshake(DataInputStream in, OutputStream out, MySqlPacket packets) throws IOException {
        byte[] scramble = new byte[20];
        new SecureRandom().nextBytes(scramble);
        long connectionId = NEXT_CONNECTION_ID.getAndIncrement();
        packets.writePayload(out, MySqlMessages.handshakeV10(connectionId, scramble));

        byte[] response = packets.readPayload(in);
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
        return username;
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
        try (Connection backend = options.mywireNativeBackend()
                ? MySqlBackendConnections.open(options) : PgConnections.open(options)) {
            backend.setAutoCommit(true);
            terminalExecutor.rebind(backend);
            Statement statement = Statement.of(SourceDialect.MYSQL, sql, List.of());
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
            packets.writePayload(out, MySqlMessages.errPacket(1105, sqlState(e),
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
