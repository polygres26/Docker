package com.polygres.wire.boltwire;

import com.polygres.wire.core.BackendRegistry;
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
    private final PgGraphStore graphStore;

    // Lazily opened on this session's first RUN, reused for every RUN after that, returned to the
    // pool on GOODBYE/EOF/error -- real bug, found live comparing boltwire's own measured latency
    // against pgwire's (2+ ms vs well under 1 ms for a comparable round trip): every single RUN was
    // borrowing a fresh pooled connection via target.open() and immediately handing it back,
    // instead of holding one connection for the whole Bolt session the way pgwire itself does (a
    // pgwire client's backend connection is bound once at login, not re-borrowed per statement).
    // HikariCP's own borrow/return isn't free, and paying it on every RUN when a Bolt session, like
    // a pgwire session, already has its own persistent client connection to amortize it over was a
    // real, avoidable cost -- not an inherent property of speaking a different wire protocol.
    private Connection sessionConnection;

    private Connection sessionConnection() throws SQLException {
        if (sessionConnection == null) {
            sessionConnection = graphStore.connect();
        }
        return sessionConnection;
    }

    private static final Pattern RETURN_LITERAL = Pattern.compile(
            "(?is)^\\s*RETURN\\s+(-?\\d+\\.\\d+|-?\\d+|'[^']*'|\"[^\"]*\")\\s*(?:AS\\s+(\\w+))?\\s*;?\\s*$");
    private static final Pattern CREATE_PREFIX = Pattern.compile("(?i)^\\s*CREATE\\b");
    private static final Pattern MATCH_PREFIX = Pattern.compile("(?i)^\\s*MATCH\\b");

    public BoltWireSessionHandler(Socket clientSocket, BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.backendRegistry = backendRegistry;
        this.graphStore = new PgGraphStore(backendRegistry);
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
        } finally {
            if (sessionConnection != null) {
                try {
                    sessionConnection.close();
                } catch (SQLException e) {
                    log.debug("boltwire: error closing session connection", e);
                }
            }
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

    /** Dispatches to whichever narrow translation the query text matches -- Phase 1's literal
     * RETURN, or Phase 2's CREATE (see {@link CypherParser}) -- and fails loudly, naming what
     * wasn't understood, when neither does. */
    private ExecutedQuery translateAndRun(String cypher) throws SQLException {
        if (CREATE_PREFIX.matcher(cypher).find()) {
            try {
                return runCreate(CypherParser.parseCreate(cypher));
            } catch (CypherException e) {
                throw new UnsupportedCypherException(e.getMessage());
            }
        }
        if (MATCH_PREFIX.matcher(cypher).find()) {
            try {
                return runMatch(CypherParser.parseMatch(cypher));
            } catch (CypherException e) {
                throw new UnsupportedCypherException(e.getMessage());
            }
        }
        return runReturnLiteral(cypher);
    }

    /**
     * Phase 3's read path: translates a parsed {@link CypherParser.MatchStatement} into one real
     * parameterized SQL query against {@code polywire_graph_nodes}/{@code polywire_graph_edges},
     * and executes it directly (bypassing {@code PgGraphStore}'s write-path helpers, which are
     * shaped around a single insert, not an arbitrary join) -- a single-node MATCH becomes a plain
     * {@code SELECT ... FROM polywire_graph_nodes WHERE labels @> ... AND properties->>'x' = ?};
     * a node-rel-node MATCH becomes a real join through {@code polywire_graph_edges}. Each
     * requested variable gets its own {@code (id, labels, properties)} triple selected under a
     * distinct SQL alias, so a RETURN referencing either side of the join gets the right node back.
     */
    private ExecutedQuery runMatch(CypherParser.MatchStatement stmt) throws SQLException {
        Map<String, CypherParser.NodePattern> nodesByVariable = new LinkedHashMap<>();
        if (stmt.first().variable() != null) {
            nodesByVariable.put(stmt.first().variable(), stmt.first());
        }
        if (stmt.second() != null && stmt.second().variable() != null) {
            nodesByVariable.put(stmt.second().variable(), stmt.second());
        }
        for (CypherParser.ReturnItem item : stmt.returnItems()) {
            if (!nodesByVariable.containsKey(item.variable())) {
                throw new UnsupportedCypherException(
                        "boltwire: RETURN references \"" + item.variable() + "\", which isn't a matched node");
            }
        }

        boolean variableLength = stmt.rel() != null && stmt.rel().minHops() != null;

        StringBuilder sql = new StringBuilder();
        if (variableLength) {
            appendRecursiveCte(sql, stmt.rel());
        }
        // DISTINCT: a variable-length path can reach the same (start, end) node pair at more than
        // one depth (e.g. both 2 and 3 hops) -- Cypher's own MATCH semantics return the pair once
        // per matched path shape here, not once per row, so de-duplicate rather than surface every
        // intermediate depth as its own result.
        sql.append(variableLength ? "SELECT DISTINCT " : "SELECT ");
        List<String> selectVars = new ArrayList<>(nodesByVariable.keySet());
        List<String> selectExprs = new ArrayList<>();
        for (String v : selectVars) {
            selectExprs.add(v + ".id, " + v + ".labels, " + v + ".properties");
        }
        sql.append(String.join(", ", selectExprs));

        String firstAlias = stmt.first().variable() != null ? stmt.first().variable() : "n0";
        sql.append(" FROM polywire_graph_nodes ").append(firstAlias);
        String secondAlias = null;
        if (stmt.second() != null) {
            secondAlias = stmt.second().variable() != null ? stmt.second().variable() : "n1";
            if (variableLength) {
                // The recursive CTE ("paths") already did all the hop-following and cycle-guarding
                // -- this join is just "attach the two real node rows to a path we already found",
                // the same shape the fixed-hop branch below uses for its own single-hop join.
                sql.append(" JOIN paths p ON p.start_id = ").append(firstAlias).append(".id");
                sql.append(" JOIN polywire_graph_nodes ").append(secondAlias)
                        .append(" ON ").append(secondAlias).append(".id = p.end_id");
            } else {
                sql.append(" JOIN polywire_graph_edges e ON e.from_id = ").append(firstAlias).append(".id");
                if (stmt.rel().type() != null) {
                    sql.append(" AND e.type = ").append(sqlLiteral(stmt.rel().type()));
                }
                sql.append(" JOIN polywire_graph_nodes ").append(secondAlias)
                        .append(" ON ").append(secondAlias).append(".id = e.to_id");
            }
        }

        List<String> whereClauses = new ArrayList<>();
        if (variableLength) {
            // The recursive step already stops extending once depth reaches maxHops (see
            // appendRecursiveCte), but the walk still keeps every depth from 1 upward along the
            // way (e.g. minHops=2 still needs depth-1 rows to extend from) -- this is the actual
            // [minHops, maxHops] narrowing of "paths" down to what MATCH asked for.
            whereClauses.add("p.depth BETWEEN " + stmt.rel().minHops() + " AND " + stmt.rel().maxHops());
        }
        List<Object> params = new ArrayList<>();
        addLabelFilter(whereClauses, firstAlias, stmt.first().labels());
        // Real bug, found live testing Phase 4's variable-length paths: an inline property map on
        // a MATCH node pattern (e.g. "MATCH (a:Person {name: 'Alice'})") was being parsed into
        // NodePattern.properties() correctly but never actually turned into a WHERE filter here --
        // "a" silently matched every Person, not just the one named Alice. CREATE never hit this
        // (its own properties always go straight into an INSERT, not a filter), so it was never
        // exercised until a MATCH anchored by name was actually tried.
        addPropertyFilters(whereClauses, params, firstAlias, stmt.first().properties());
        if (secondAlias != null) {
            addLabelFilter(whereClauses, secondAlias, stmt.second().labels());
            addPropertyFilters(whereClauses, params, secondAlias, stmt.second().properties());
        }
        for (CypherParser.Condition cond : stmt.where()) {
            if (!nodesByVariable.containsKey(cond.variable())) {
                throw new UnsupportedCypherException(
                        "boltwire: WHERE references \"" + cond.variable() + "\", which isn't a matched node");
            }
            String op = switch (cond.op()) {
                case EQ -> "=";
                case NEQ -> "!=";
                case GT -> ">";
                case LT -> "<";
                case GTE -> ">=";
                case LTE -> "<=";
            };
            // Real bug, found live writing this feature's own test suite: comparing
            // `properties->>'x'` (always text) against a numeric literal with a plain ">"/"<"
            // compared lexically, not numerically -- "5" > "18" is true as text (since '5' > '1'),
            // so "WHERE n.age > 18" matched age=5 right along with age=80. A numeric condition
            // casts the extracted value to numeric instead; string/bool conditions keep the
            // original text comparison, which is exactly what Cypher's own equality/ordering on
            // those types means here.
            String lhs = cond.value() instanceof Number
                    ? "(" + cond.variable() + ".properties->>'" + cond.property() + "')::numeric"
                    : cond.variable() + ".properties->>'" + cond.property() + "'";
            whereClauses.add(lhs + " " + op + " ?");
            params.add(cond.value() instanceof Number n ? n : String.valueOf(cond.value()));
        }
        if (!whereClauses.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereClauses));
        }
        if (stmt.limit() != null) {
            sql.append(" LIMIT ").append(stmt.limit().intValue());
        }

        List<String> columns = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();
        try (PreparedStatement ps = sessionConnection().prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                Object p = params.get(i);
                if (p instanceof Number n) {
                    ps.setBigDecimal(i + 1, new java.math.BigDecimal(n.toString()));
                } else {
                    ps.setString(i + 1, (String) p);
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, GraphNode> rowNodes = new LinkedHashMap<>();
                    int col = 1;
                    for (String v : selectVars) {
                        long id = rs.getLong(col++);
                        java.sql.Array labelsArr = rs.getArray(col++);
                        List<String> labels = labelsArr == null ? List.of()
                                : List.of((String[]) labelsArr.getArray());
                        Map<String, Object> props = jsonToMap(rs.getString(col++));
                        rowNodes.put(v, new GraphNode(id, labels, props, GraphNode.elementId(id)));
                    }
                    List<Object> row = new ArrayList<>();
                    if (columns.isEmpty()) {
                        for (CypherParser.ReturnItem item : stmt.returnItems()) {
                            columns.add(item.alias() != null ? item.alias()
                                    : item.property() != null ? item.variable() + "." + item.property() : item.variable());
                        }
                    }
                    for (CypherParser.ReturnItem item : stmt.returnItems()) {
                        GraphNode node = rowNodes.get(item.variable());
                        row.add(item.property() != null ? node.properties().get(item.property()) : node);
                    }
                    rows.add(row);
                }
            }
        }
        return new ExecutedQuery(columns, rows);
    }

    /** Real variable-length-path support ({@code [*1..3]}): a Postgres {@code WITH RECURSIVE} CTE
     * that walks {@code polywire_graph_edges} from {@code depth=1} up to {@code rel.maxHops()},
     * tracking each path's visited node ids in an array so a cycle stops the recursion for that
     * branch instead of looping forever -- {@code polywire_graph_edges} has no built-in acyclic
     * guarantee (a real graph can and does have cycles), so this guard is load-bearing, not
     * defensive-only. {@code minHops} is enforced afterwards in the outer query's WHERE (see
     * {@link #runMatch}) rather than here, since the recursive step still needs every depth from 1
     * upward to have something to extend from. */
    private static void appendRecursiveCte(StringBuilder sql, CypherParser.RelPattern rel) {
        String typeFilter = rel.type() != null ? " AND type = " + sqlLiteral(rel.type()) : "";
        sql.append("WITH RECURSIVE paths AS (")
                .append("SELECT from_id AS start_id, to_id AS end_id, 1 AS depth, ARRAY[from_id, to_id] AS visited ")
                .append("FROM polywire_graph_edges WHERE true").append(typeFilter)
                .append(" UNION ALL ")
                .append("SELECT p.start_id, e.to_id, p.depth + 1, p.visited || e.to_id ")
                .append("FROM paths p JOIN polywire_graph_edges e ON e.from_id = p.end_id")
                .append(typeFilter.isEmpty() ? "" : " AND e.type = " + sqlLiteral(rel.type()))
                .append(" WHERE p.depth < ").append(rel.maxHops())
                .append(" AND NOT (e.to_id = ANY(p.visited))")
                .append(") ");
    }

    private static void addLabelFilter(List<String> whereClauses, String alias, List<String> labels) {
        for (String label : labels) {
            whereClauses.add(sqlLiteral(label) + " = ANY(" + alias + ".labels)");
        }
    }

    /** A MATCH node pattern's own inline property map (e.g. {@code {name: 'Alice'}}) -- unlike a
     * label, a property value is real client-supplied data, not a Cypher identifier, so it's bound
     * as a parameter here rather than inlined like {@link #sqlLiteral} does for labels/types. */
    private static void addPropertyFilters(List<String> whereClauses, List<Object> params, String alias,
            Map<String, Object> properties) {
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            whereClauses.add(alias + ".properties->>'" + e.getKey() + "' = ?");
            params.add(String.valueOf(e.getValue()));
        }
    }

    /** Labels/relationship types come from the parsed Cypher text, not a bind parameter -- Cypher
     * identifiers can't contain a quote character at all (the tokenizer would have already split
     * on one), so a literal, non-parameterized SQL string is safe here the same way this codebase's
     * other stores inline validated identifiers (e.g. {@code PgTimeSeriesStore#pgTableName}) rather
     * than bind them. */
    private static String sqlLiteral(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    private static Map<String, Object> jsonToMap(String json) {
        com.google.gson.JsonObject obj = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<String, com.google.gson.JsonElement> e : obj.entrySet()) {
            com.google.gson.JsonElement v = e.getValue();
            if (v.isJsonNull()) {
                map.put(e.getKey(), null);
            } else if (v.getAsJsonPrimitive().isBoolean()) {
                map.put(e.getKey(), v.getAsBoolean());
            } else if (v.getAsJsonPrimitive().isNumber()) {
                double d = v.getAsDouble();
                map.put(e.getKey(), d == Math.floor(d) && !Double.isInfinite(d) ? (Object) (long) d : (Object) d);
            } else {
                map.put(e.getKey(), v.getAsString());
            }
        }
        return map;
    }

    /**
     * Phase 2's write path: creates the node (and, if the pattern includes one, the relationship
     * and second node) for real in Postgres via {@link PgGraphStore}, then builds each requested
     * RETURN item -- either the whole created node (encoded as a real Bolt Node struct, see
     * {@link GraphNode}) or one scalar property off it. Both nodes (and the edge, if present) are
     * created in a single transaction -- see {@code PgGraphStore#withConnection}'s own javadoc for
     * why: a failure partway through must not leave an orphaned node behind.
     */
    private ExecutedQuery runCreate(CypherParser.CreateStatement stmt) throws SQLException {
        Map<String, GraphNode> createdByVariable = new LinkedHashMap<>();
        if (stmt.second() == null) {
            // Real bug, found live chasing boltwire's own write latency: a lone node CREATE was
            // still paying for an explicit transaction -- setAutoCommit(false), commit(),
            // setAutoCommit(true) -- three extra real round trips to Postgres around one INSERT
            // that Postgres already commits atomically by itself. PgGraphStore#withConnection's
            // transaction exists to keep a node+edge+node CREATE atomic against a partial
            // failure (see its own javadoc) -- a single node has nothing to partially fail
            // alongside, so it skips the wrapper entirely rather than pay for a guarantee this
            // statement shape doesn't need.
            GraphNode first = graphStore.createNode(sessionConnection(), stmt.first().labels(), stmt.first().properties());
            if (stmt.first().variable() != null) {
                createdByVariable.put(stmt.first().variable(), first);
            }
        } else {
            graphStore.withConnection(sessionConnection(), c -> {
                GraphNode first = graphStore.createNode(c, stmt.first().labels(), stmt.first().properties());
                if (stmt.first().variable() != null) {
                    createdByVariable.put(stmt.first().variable(), first);
                }
                GraphNode second = graphStore.createNode(c, stmt.second().labels(), stmt.second().properties());
                if (stmt.second().variable() != null) {
                    createdByVariable.put(stmt.second().variable(), second);
                }
                graphStore.createEdge(c, first.id(), second.id(), stmt.rel().type(), stmt.rel().properties());
                return null;
            });
        }

        List<String> columns = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        for (CypherParser.ReturnItem item : stmt.returnItems()) {
            GraphNode node = createdByVariable.get(item.variable());
            if (node == null) {
                throw new UnsupportedCypherException(
                        "boltwire: RETURN references \"" + item.variable() + "\", which wasn't created by this CREATE");
            }
            String columnName = item.alias() != null ? item.alias()
                    : item.property() != null ? item.variable() + "." + item.property() : item.variable();
            columns.add(columnName);
            row.add(item.property() != null ? node.properties().get(item.property()) : node);
        }
        List<List<Object>> rows = stmt.returnItems().isEmpty() ? List.of() : List.of(row);
        return new ExecutedQuery(columns, rows);
    }

    /**
     * Phase 1's own narrow translation: {@code RETURN <literal> [AS <alias>]} only -- see this
     * class's own javadoc for why. Executes a genuine {@code SELECT <literal> AS <alias>} against
     * the default backend, proving a real Postgres round trip, not just an in-Java echo.
     */
    private ExecutedQuery runReturnLiteral(String cypher) throws SQLException {
        Matcher m = RETURN_LITERAL.matcher(cypher);
        if (!m.matches()) {
            throw new UnsupportedCypherException(
                    "boltwire Phase 1/2 only understands \"RETURN <literal> [AS <alias>]\" and \"CREATE ...\" "
                            + "-- got: " + cypher);
        }
        String literal = m.group(1);
        String alias = m.group(2) != null ? m.group(2) : literal.replaceAll("[^A-Za-z0-9_]", "_");
        String sql = "SELECT " + literal + " AS " + alias;
        try (PreparedStatement ps = sessionConnection().prepareStatement(sql);
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
