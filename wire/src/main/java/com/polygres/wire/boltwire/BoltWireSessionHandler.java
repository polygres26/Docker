package com.polygres.wire.boltwire;

import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * boltwire -- Neo4j's real Bolt wire protocol (a binary TCP protocol, not HTTP/JSON like
 * oswire/dynamowire/sqswire/influxwire), so a real Neo4j client driver (the official
 * {@code neo4j} Python/Java/JS/.NET/Go drivers, all speaking the same Bolt wire underneath) can
 * point at PolyWire directly. Shaped like {@code PgWireSessionHandler} at the accept-loop/session
 * level (one {@link Runnable} per raw {@link Socket}, no Jetty involved), but like
 * {@code MongoWireSessionHandler} at the query level: this isn't SQL passing straight through,
 * it's a different query language ({@link #translateCypher} handles Phase 1's own narrow subset)
 * translated to real Postgres SQL and executed directly via JDBC against
 * {@link BackendRegistry}'s default backend -- bypassing the shared {@code StatementPipeline} for
 * now, the same way oswire/dynamowire/influxwire do (see those classes' own javadoc for why: this
 * is Phase 1, proving the real wire protocol and PackStream framing against a real driver before
 * any serious Cypher-to-SQL translation work -- see {@link PackStream}'s own javadoc for exactly
 * what real session this implementation is grounded in).
 *
 * <p><b>Negotiates Bolt 4.4</b>, not the newer 5.x a real client also offers -- confirmed live
 * that Neo4j's own Python driver falls back cleanly to 4.4's simpler single-HELLO-with-inline-
 * credentials shape when a server responds with the classic (non-manifest) 4-byte handshake reply,
 * rather than 5.x's HELLO+LOGON split. Simpler for this phase, still a completely real, officially
 * supported protocol version every mainstream driver speaks for backward compatibility.
 *
 * <p><b>Phase 1 query scope, deliberately narrow</b>: {@link #translateCypher} only recognizes
 * {@code RETURN <literal> [AS <alias>]} (integer/float/string literals) -- enough to prove the
 * whole path (handshake, auth, RUN/PULL/RECORD/SUCCESS framing, GOODBYE) against a real driver
 * issuing a real query, with the literal's value round-tripped through a genuine
 * {@code SELECT <literal> AS <alias>} against Postgres, not just echoed back in Java. Any other
 * Cypher shape returns a real Bolt FAILURE message naming what wasn't understood -- the same
 * "unrecognized clause fails loudly" policy every other protocol in this codebase follows -- rather
 * than a wrong or silently-ignored translation. A real MATCH/pattern-matching Cypher-to-SQL
 * translator (recursive CTEs for variable-length paths, a node/edge schema) is a separate, later
 * phase.
 */
public final class BoltWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(BoltWireSessionHandler.class);

    private static final byte[] BOLT_MAGIC = {0x60, 0x60, (byte) 0xB0, 0x17};
    // The classic (non-manifest) 4-byte version this handler always replies with when a client's
    // handshake proposals include ANY range covering Bolt 4.4 -- confirmed live this is what a
    // real client proposes (a range covering 4.2-4.4) alongside its newer manifest-style first
    // proposal, and that replying with this exact classic shape makes a real driver proceed with
    // the older single-HELLO auth flow instead of expecting the newer manifest response.
    private static final byte[] BOLT_4_4 = {0x00, 0x00, 0x04, 0x04};

    private final Socket clientSocket;
    private final BackendRegistry backendRegistry;

    private static final Pattern RETURN_LITERAL = Pattern.compile(
            "(?is)^\\s*RETURN\\s+(-?\\d+\\.\\d+|-?\\d+|'[^']*'|\"[^\"]*\")\\s*(?:AS\\s+(\\w+))?\\s*;?\\s*$");

    public BoltWireSessionHandler(Socket clientSocket, BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.backendRegistry = backendRegistry;
    }

    @Override
    public void run() {
        try (Socket socket = clientSocket) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            if (!performHandshake(in, out)) {
                return;
            }
            sessionLoop(in, out);
        } catch (IOException e) {
            log.debug("boltwire: session ended ({})", e.getMessage());
        } catch (RuntimeException e) {
            log.warn("boltwire: session failed", e);
        }
    }

    /** @return true if a version was successfully negotiated and the caller should proceed to the
     * real message loop; false if the connection should just be closed (bad magic, or no proposed
     * version this handler understands). */
    private boolean performHandshake(DataInputStream in, DataOutputStream out) throws IOException {
        byte[] preamble = new byte[4];
        in.readFully(preamble);
        for (int i = 0; i < 4; i++) {
            if (preamble[i] != BOLT_MAGIC[i]) {
                log.warn("boltwire: bad handshake preamble (not a real Bolt client?): {}",
                        bytesToHex(preamble));
                return false;
            }
        }
        byte[] proposals = new byte[16];
        in.readFully(proposals);
        for (int i = 0; i < 4; i++) {
            int base = i * 4;
            // [reserved, range, minor, major] -- see this class's own javadoc for why 4.x (major
            // byte 0x04) with a range covering minor 4 is the one this handler always picks.
            int range = proposals[base + 1] & 0xFF;
            int minor = proposals[base + 2] & 0xFF;
            int major = proposals[base + 3] & 0xFF;
            if (major == 4 && minor >= 4 && (minor - range) <= 4) {
                out.write(BOLT_4_4);
                out.flush();
                return true;
            }
        }
        log.warn("boltwire: no proposed Bolt version this handler supports (needs 4.4 in range) -- "
                + "proposals: {}", bytesToHex(proposals));
        out.write(new byte[4]); // all-zero = "no acceptable version", per the real Bolt handshake spec
        out.flush();
        return false;
    }

    // Real Bolt semantics, confirmed necessary live (not from the spec alone): a client driver
    // routinely pipelines RUN and PULL together in one write without waiting for RUN's own
    // response first (see PackStream's own javadoc -- the real captured session does exactly this).
    // Found live: when RUN fails, the pipelined PULL right behind it was being processed as its
    // own independent request, surfacing a confusing "PULL with no prior successful RUN" error
    // instead of the actual RUN failure the client cares about. Real Bolt servers instead enter a
    // FAILURE state after any FAILURE response: every message except RESET gets an IGNORED
    // response until the client explicitly sends RESET to recover the session -- letting a client
    // tell "this whole pipelined batch failed because of the first message" apart from "each
    // message failed independently."
    private boolean failedState;

    private void sessionLoop(DataInputStream in, DataOutputStream out) throws IOException {
        while (true) {
            byte[] messageBytes = readChunkedMessage(in);
            if (messageBytes == null) {
                return; // clean EOF between messages
            }
            PackStream.Struct msg = new PackStream.Reader(messageBytes).readMessage();
            if (failedState && msg.tag() != BoltMessages.RESET && msg.tag() != BoltMessages.GOODBYE) {
                writeIgnored(out);
                continue;
            }
            switch (msg.tag()) {
                case BoltMessages.HELLO -> handleHello(out);
                case BoltMessages.GOODBYE -> {
                    return;
                }
                case BoltMessages.RESET -> {
                    failedState = false;
                    pendingQuery = null;
                    writeSuccess(out, Map.of());
                }
                case BoltMessages.RUN -> handleRun(out, msg);
                case BoltMessages.PULL -> handlePull(out);
                case BoltMessages.DISCARD -> writeSuccess(out, Map.of());
                default -> writeFailure(out, "Neo.ClientError.Request.Invalid",
                        "boltwire Phase 1 doesn't handle message tag 0x" + Integer.toHexString(msg.tag()));
            }
        }
    }

    /** Real Bolt 4.4's own SUCCESS shape for HELLO -- confirmed live against a real Neo4j 5.26
     * server's response to the equivalent (5.x) exchange, adapted to what a 4.4 session reports
     * (no {@code hints}/{@code patch_bolt} negotiation, since 4.4 doesn't have Bolt 5.x's later
     * feature-flag fields).
     *
     * <p>Real bug, found live testing the real {@code neo4j} Python driver against an earlier
     * version of this method that reported {@code server: "PolyWire/boltwire-v1"}: the driver
     * parses this field and refuses to proceed at all -- {@code neo4j.exceptions.
     * UnsupportedServerProduct: PolyWire/boltwire-v1} -- unless it matches a real
     * {@code Neo4j/x.y.z}-shaped agent string (the official drivers hard-check this; it isn't
     * negotiable client-side config). Reporting a real-looking Neo4j version string here is the
     * same wire-protocol-impersonation-for-compatibility this codebase already does everywhere
     * else (oswire's OpenSearch-shaped errors, dynamowire's AWS exception envelopes, influxwire's
     * InfluxDB response shape) -- the whole point of a real compat shim is presenting the real
     * product's own wire shape, not a shim announcing itself as something else and having every
     * official client refuse to talk to it. Memgraph (a real, different graph database) does the
     * identical thing for the identical reason -- reports itself as Neo4j-compatible in its own
     * Bolt handshake, confirmed public knowledge, not a technique invented here. */
    private void handleHello(DataOutputStream out) throws IOException {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("server", "Neo4j/5.26.30");
        metadata.put("connection_id", "bolt-" + System.nanoTime());
        writeSuccess(out, metadata);
    }

    private ExecutedQuery pendingQuery;

    private record ExecutedQuery(List<String> columns, List<List<Object>> rows) {
    }

    private void handleRun(DataOutputStream out, PackStream.Struct msg) throws IOException {
        String cypher = (String) msg.fields().get(0);
        try {
            ExecutedQuery result = translateAndRun(cypher);
            pendingQuery = result;
            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("fields", result.columns());
            writeSuccess(out, metadata);
        } catch (UnsupportedCypherException e) {
            pendingQuery = null;
            writeFailure(out, "Neo.ClientError.Statement.SyntaxError", e.getMessage());
        } catch (SQLException e) {
            pendingQuery = null;
            log.warn("boltwire: Postgres error running translated query for \"{}\": {}", cypher, e.getMessage());
            writeFailure(out, "Neo.ClientError.Statement.ExecutionFailed", e.getMessage());
        }
    }

    private void handlePull(DataOutputStream out) throws IOException {
        if (pendingQuery == null) {
            writeFailure(out, "Neo.ClientError.Request.Invalid", "boltwire: PULL with no prior successful RUN");
            return;
        }
        try {
            for (List<Object> row : pendingQuery.rows()) {
                writeMessage(out, w -> {
                    w.writeStructHeader(1, BoltMessages.RECORD);
                    w.writeList(row);
                });
            }
            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("has_more", false);
            writeSuccess(out, metadata);
        } catch (RuntimeException e) {
            // Real bug, found live: an unencodable row value (see PackStream.Writer#writeValue's
            // own BigDecimal fix, found the exact same way) previously threw straight out of this
            // method uncaught, killing the whole TCP connection instead of failing just this one
            // query -- a real client driver has no way to distinguish "the server crashed" from
            // "this one query has a problem" when the socket just dies. Sending a real Bolt
            // FAILURE message instead keeps the session alive for the client's next query, the
            // same "unrecognized/unencodable data fails loudly, not silently and not fatally"
            // policy this codebase already applies everywhere else.
            log.warn("boltwire: failed to encode PULL results", e);
            writeFailure(out, "Neo.DatabaseError.General.UnknownError", String.valueOf(e.getMessage()));
        } finally {
            pendingQuery = null;
        }
    }

    private static final class UnsupportedCypherException extends RuntimeException {
        UnsupportedCypherException(String message) {
            super(message);
        }
    }

    /**
     * Phase 1's own narrow translation: {@code RETURN <literal> [AS <alias>]} only -- see this
     * class's own javadoc for why. Executes a genuine {@code SELECT <literal> AS <alias>} against
     * the default backend, proving a real Postgres round trip, not just an in-Java echo.
     */
    private ExecutedQuery translateAndRun(String cypher) throws SQLException {
        Matcher m = RETURN_LITERAL.matcher(cypher);
        if (!m.matches()) {
            throw new UnsupportedCypherException(
                    "boltwire Phase 1 only understands \"RETURN <literal> [AS <alias>]\" -- got: " + cypher);
        }
        String literal = m.group(1);
        String alias = m.group(2) != null ? m.group(2) : literal.replaceAll("[^A-Za-z0-9_]", "_");
        BackendTarget target = backendRegistry.resolveForRouting(BackendRegistry.DEFAULT_BACKEND_NAME);
        if (target == null) {
            throw new IllegalStateException("boltwire: no default backend configured");
        }
        String sql = "SELECT " + literal + " AS " + alias;
        try (Connection c = target.open(); PreparedStatement ps = c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                columns.add(md.getColumnLabel(i));
            }
            List<List<Object>> rows = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    Object v = rs.getObject(i);
                    // PackStream.Writer#writeValue only knows Boolean/Integer/Long/Double/Float/
                    // String/List/Map -- a plain literal SELECT only ever produces Integer/Long/
                    // Double/String/Boolean here, so no widening beyond that is needed yet.
                    row.add(v);
                }
                rows.add(row);
            }
            return new ExecutedQuery(columns, rows);
        }
    }

    private void writeSuccess(DataOutputStream out, Map<String, Object> metadata) throws IOException {
        writeMessage(out, w -> {
            w.writeStructHeader(1, BoltMessages.SUCCESS);
            w.writeMap(metadata);
        });
    }

    private void writeFailure(DataOutputStream out, String code, String message) throws IOException {
        failedState = true;
        writeMessage(out, w -> {
            w.writeStructHeader(1, BoltMessages.FAILURE);
            w.writeMapHeader(2);
            w.writeString("code");
            w.writeString(code);
            w.writeString("message");
            w.writeString(message);
        });
    }

    private void writeIgnored(DataOutputStream out) throws IOException {
        writeMessage(out, w -> w.writeStructHeader(0, BoltMessages.IGNORED));
    }

    private interface WriterAction {
        void write(PackStream.Writer w);
    }

    /** Wraps one PackStream-encoded message in Bolt's own chunked framing: a 2-byte big-endian
     * length prefix, the message bytes, then a zero-length chunk marking the end of the message --
     * confirmed against every real server-&gt;client message in the captured session (see
     * {@link PackStream}'s javadoc). Every message this handler ever sends fits in one chunk (well
     * under the 65535-byte chunk-size limit), so multi-chunk splitting isn't implemented. */
    private void writeMessage(DataOutputStream out, WriterAction action) throws IOException {
        PackStream.Writer w = new PackStream.Writer();
        action.write(w);
        byte[] body = w.toByteArray();
        out.writeShort(body.length);
        out.write(body);
        out.writeShort(0);
        out.flush();
    }

    /** Reads one full Bolt message across as many chunks as it takes, per the same framing
     * {@link #writeMessage} produces. @return null on a clean EOF between messages (the client
     * closed the socket without sending GOODBYE -- treated the same as GOODBYE, not an error). */
    private static byte[] readChunkedMessage(DataInputStream in) throws IOException {
        java.io.ByteArrayOutputStream message = new java.io.ByteArrayOutputStream();
        while (true) {
            int chunkLen;
            try {
                chunkLen = in.readUnsignedShort();
            } catch (java.io.EOFException e) {
                return message.size() == 0 ? null : message.toByteArray();
            }
            if (chunkLen == 0) {
                return message.toByteArray();
            }
            byte[] chunk = new byte[chunkLen];
            in.readFully(chunk);
            message.write(chunk);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b));
        }
        return sb.toString().strip();
    }
}
