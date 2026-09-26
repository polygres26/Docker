package com.sayonora.warp.mongowire;

import com.sayonora.warp.auth.CredentialStore;
import com.sayonora.warp.mongowire.auth.MongoScramConversation;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes one MongoDB command document against the Postgres-backed document store. The CRUD semantics live in
 * {@link MongoCrud}, administration in {@link MongoAdmin}; this class routes commands, answers the handshake and
 * server-info commands, and maps every failure to a MongoDB-style {@code {ok: 0, errmsg, code, codeName}} reply.
 */
final class MongoCommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(MongoCommandDispatcher.class);
    static final int MAX_WIRE_VERSION = 21;
    private static final String VERSION = "7.0.0";

    private final PostgresDocumentStore store;
    private final com.sayonora.warp.cluster.RowCache cache;
    private final com.sayonora.warp.core.SqlMetricsCollector sqlMetrics;
    private final CredentialStore credentials = new CredentialStore();
    private final MongoCrud crud;
    private final MongoAdmin admin;
    private MongoScramConversation pendingScram;
    private int scramConversationId;
    private String authenticatedUser;
    private String remoteAddress = "127.0.0.1:0";
    private final long connectionId = CONNECTION_IDS.incrementAndGet();
    private static final java.util.concurrent.atomic.AtomicLong CONNECTION_IDS = new java.util.concurrent.atomic.AtomicLong();

    MongoCommandDispatcher(PostgresDocumentStore store, com.sayonora.warp.cluster.RowCache cache) {
        this(store, cache, null);
    }

    MongoCommandDispatcher(PostgresDocumentStore store, com.sayonora.warp.cluster.RowCache cache,
            com.sayonora.warp.core.SqlMetricsCollector sqlMetrics) {
        this.store = store;
        this.cache = cache;
        this.sqlMetrics = sqlMetrics;
        this.crud = new MongoCrud(store, cache, (outcome, nanos) -> recordRttOutcome(outcome, nanos));
        this.admin = new MongoAdmin(store);
    }

    private com.sayonora.warp.core.ConnectionRouter router;

    /** Enables connect-time routing: each data-plane command's {@code $db} selects a backend or set. */
    MongoCommandDispatcher withRouter(com.sayonora.warp.core.ConnectionRouter router) {
        this.router = router;
        return this;
    }

    MongoCommandDispatcher withRemoteAddress(String remote) {
        this.remoteAddress = remote;
        return this;
    }

    private static final Set<String> DATA_PLANE = Set.of("insert", "find", "aggregate", "update", "delete", "listCollections", "count",
            "distinct", "findAndModify", "findandmodify", "create", "drop", "dropDatabase", "listIndexes", "createIndexes", "dropIndexes",
            "collMod", "dbStats", "collStats", "validate", "getMore", "explain", "renameCollection");
    private static final Set<String> VIRTUAL_DBS = Set.of("admin", "config", "local");

    private static final List<String> COMMANDS = List.of("aggregate", "buildInfo", "collMod", "collStats", "connectionStatus", "count",
            "create", "createIndexes", "dbStats", "delete", "distinct", "drop", "dropDatabase", "dropIndexes", "endSessions", "explain",
            "find", "findAndModify", "getCmdLineOpts", "getMore", "getParameter", "hello", "hostInfo", "isMaster", "insert", "killCursors",
            "killSessions", "listCollections", "listCommands", "listDatabases", "listIndexes", "logout", "ping", "refreshSessions",
            "renameCollection", "saslContinue", "saslStart", "serverStatus", "startSession", "update", "validate", "whatsmyuri");

    BsonDocument dispatch(BsonDocument command) {
        if (command.isEmpty()) {
            return new MongoCmdException(59, "Received a command with an empty name").toReply();
        }
        String commandName = command.getFirstKey();
        String db = command.containsKey("$db") && command.get("$db").isString() ? command.getString("$db").getValue() : "test";
        long start = System.nanoTime();
        try {
            if (router != null && DATA_PLANE.contains(commandName) && !VIRTUAL_DBS.contains(db)) {
                com.sayonora.warp.core.ConnectionRoute route =
                        router.resolve(com.sayonora.warp.core.ConnectionRouter.PROTO_MONGODB, db, null);
                if (route.isRejected()) {
                    return new MongoCmdException(26, "database \"" + db + "\" not found").toReply();
                }
                List<String> hosts = router.storeBackends(route);
                if (hosts != null && hosts.isEmpty()) {
                    return new MongoCmdException(2, "database \"" + db + "\" routes to " + route.description()
                            + ", which cannot store documents (only Postgres backends can)").toReply();
                }
                store.routeTo(hosts);
            } else {
                store.routeTo(null);
            }
            return execute(commandName, command, db);
        } catch (MongoCmdException e) {
            return e.toReply();
        } catch (MongoCrud.SqlFailure e) {
            return sqlError(commandName, e.sql);
        } catch (SQLException e) {
            return sqlError(commandName, e);
        } catch (org.bson.BsonInvalidOperationException | ClassCastException e) {
            return new MongoCmdException(14, "BSON field type mismatch in '" + commandName + "': " + e.getMessage()).toReply();
        } catch (IllegalArgumentException e) {
            return new MongoCmdException(9, e.getMessage() == null ? "bad argument" : e.getMessage()).toReply();
        } catch (RuntimeException e) {
            MongoCrud.SqlFailure sf = null;
            Throwable t = e;
            while (t != null) {
                if (t instanceof SQLException se) {
                    return sqlError(commandName, se);
                }
                t = t.getCause();
            }
            log.warn("mongowire: unexpected failure servicing \"{}\": {}", commandName, e.toString(), e);
            return new MongoCmdException(8, String.valueOf(e.getMessage() == null ? e.toString() : e.getMessage())).toReply();
        } finally {
            if (sqlMetrics != null) {
                var kind = switch (commandName) {
                    case "find", "aggregate" -> com.sayonora.warp.core.SqlMetricsCollector.StatementKind.READ;
                    case "insert", "update", "delete" -> com.sayonora.warp.core.SqlMetricsCollector.StatementKind.WRITE;
                    default -> null;
                };
                if (kind != null) {
                    sqlMetrics.recordOperation("mongowire", resolveBackendLabel(command, commandName), kind, db + "." + commandName.toLowerCase(java.util.Locale.ROOT),
                            System.nanoTime() - start);
                }
            }
        }
    }

    private BsonDocument sqlError(String commandName, SQLException e) {
        log.warn("mongowire: Postgres error servicing \"{}\": {}", commandName, e.getMessage());
        return new MongoCmdException(MongoErrorMapper.code(e.getSQLState()), "Postgres error: " + e.getMessage()).toReply();
    }

    private BsonDocument execute(String name, BsonDocument command, String db) throws SQLException {
        switch (name) {
            case "hello": case "isMaster": case "ismaster": case "ismastercmd":
                return hello(command, name);
            case "ping":
                return ok();
            case "buildInfo": case "buildinfo":
                return buildInfo();
            case "getParameter":
                return getParameter(command);
            case "setParameter":
                return new BsonDocument("was", new BsonInt32(0)).append("ok", new BsonDouble(1.0));
            case "endSessions": case "killSessions": case "refreshSessions": case "logout": case "killOp":
                return ok();
            case "startSession":
                return new BsonDocument("id", new BsonDocument("id", new BsonBinary(java.util.UUID.randomUUID())))
                        .append("timeoutMinutes", new BsonInt32(30)).append("ok", new BsonDouble(1.0));
            case "saslStart":
                return saslStart(command);
            case "saslContinue":
                return saslContinue(command);
            case "whatsmyuri":
                return new BsonDocument("you", new BsonString(remoteAddress)).append("ok", new BsonDouble(1.0));
            case "connectionStatus":
                return connectionStatus();
            case "listCommands":
                return listCommands();
            case "serverStatus":
                return MongoAdmin.serverStatus(1);
            case "hostInfo":
                return hostInfo();
            case "getCmdLineOpts":
                return new BsonDocument("argv", new BsonArray(List.of(new BsonString("mongod")))).append("parsed", new BsonDocument())
                        .append("ok", new BsonDouble(1.0));
            case "currentOp":
                return new BsonDocument("inprog", new BsonArray()).append("ok", new BsonDouble(1.0));
            case "getLog":
                return new BsonDocument("totalLinesWritten", new BsonInt32(0)).append("log", new BsonArray()).append("ok", new BsonDouble(1.0));
            case "profile":
                return new BsonDocument("was", new BsonInt32(0)).append("slowms", new BsonInt32(100)).append("sampleRate", new BsonDouble(1.0))
                        .append("ok", new BsonDouble(1.0));
            case "replSetGetStatus": case "replSetGetConfig": case "replSetInitiate": case "replSetStepDown":
                throw new MongoCmdException(76, "not running with --replSet");
            case "insert":
                return crud.insert(command, db);
            case "find":
                return crud.find(command, db);
            case "getMore":
                return crud.getMore(command, db);
            case "killCursors":
                return crud.killCursors(command, db);
            case "aggregate":
                return crud.aggregate(command, db);
            case "update":
                return crud.update(command, db);
            case "delete":
                return crud.delete(command, db);
            case "count":
                return crud.count(command, db);
            case "distinct":
                return crud.distinct(command, db);
            case "findAndModify": case "findandmodify":
                return crud.findAndModify(command, db);
            case "create":
                return admin.create(command, db);
            case "drop":
                return admin.drop(command, db);
            case "dropDatabase":
                return admin.dropDatabase(command, db);
            case "renameCollection":
                return admin.renameCollection(command, db);
            case "listCollections":
                return admin.listCollections(command, db);
            case "listDatabases":
                return admin.listDatabases(command, db);
            case "listIndexes":
                return admin.listIndexes(command, db);
            case "createIndexes":
                return admin.createIndexes(command, db);
            case "dropIndexes":
                return admin.dropIndexes(command, db);
            case "collMod":
                return admin.collMod(command, db);
            case "dbStats":
                return admin.dbStats(command, db);
            case "collStats":
                return admin.collStats(command, db);
            case "validate":
                return admin.validate(command, db);
            case "explain":
                return explain(command, db);
            default:
                throw new MongoCmdException(59, "no such command: '" + name + "'");
        }
    }

    // ------------------------------------------------------------------ metrics helpers

    private String resolveBackendLabel(BsonDocument command, String lower) {
        try {
            BsonDocument filter = switch (lower) {
                case "find" -> command.containsKey("filter") ? command.getDocument("filter") : null;
                case "update" -> firstSpecFilter(command, "updates");
                case "delete" -> firstSpecFilter(command, "deletes");
                default -> null;
            };
            if (filter != null) {
                BsonValue eq = MongoMatcher.idEquality(filter);
                return eq == null ? "default" : store.resolveBackendFor(PostgresDocumentStore.idKey(eq));
            }
            if ("insert".equals(lower) && command.containsKey("documents")) {
                BsonArray docs = command.getArray("documents");
                if (!docs.isEmpty() && docs.get(0).asDocument().containsKey("_id")) {
                    return store.resolveBackendFor(PostgresDocumentStore.idKey(docs.get(0).asDocument().get("_id")));
                }
            }
            return "default";
        } catch (RuntimeException e) {
            return "default";
        }
    }

    private static BsonDocument firstSpecFilter(BsonDocument command, String arrayField) {
        if (!command.containsKey(arrayField)) {
            return null;
        }
        BsonArray specs = command.getArray(arrayField);
        if (specs.isEmpty()) {
            return null;
        }
        return specs.get(0).asDocument().getDocument("q", new BsonDocument());
    }

    private void recordRttOutcome(String outcome, long elapsedNanos) {
        if (sqlMetrics != null) {
            sqlMetrics.recordRttOutcome("mongowire", switch (outcome) {
                case "pg_write" -> com.sayonora.warp.core.SqlMetricsCollector.OUTCOME_PG_WRITE;
                default -> com.sayonora.warp.core.SqlMetricsCollector.OUTCOME_PG_READ;
            }, elapsedNanos);
        }
    }

    // ------------------------------------------------------------------ handshake / server info

    private static BsonDocument ok() {
        return new BsonDocument("ok", new BsonDouble(1.0));
    }

    private BsonDocument hello(BsonDocument command, String name) {
        boolean legacy = !name.equals("hello");
        BsonDocument reply = new BsonDocument();
        if (legacy) {
            reply.put("ismaster", BsonBoolean.TRUE);
        }
        if (!legacy) {
            reply.put("isWritablePrimary", BsonBoolean.TRUE);
        }
        if (command.containsKey("helloOk") && command.get("helloOk").isBoolean() && command.getBoolean("helloOk").getValue() || !legacy) {
            if (legacy) {
                reply.put("helloOk", BsonBoolean.TRUE);
            }
        }
        reply.put("maxBsonObjectSize", new BsonInt32(16 * 1024 * 1024));
        reply.put("maxMessageSizeBytes", new BsonInt32(48000000));
        reply.put("maxWriteBatchSize", new BsonInt32(100000));
        reply.put("localTime", new BsonDateTime(System.currentTimeMillis()));
        reply.put("logicalSessionTimeoutMinutes", new BsonInt32(30));
        reply.put("connectionId", new BsonInt64(connectionId));
        reply.put("minWireVersion", new BsonInt32(0));
        reply.put("maxWireVersion", new BsonInt32(MAX_WIRE_VERSION));
        reply.put("readOnly", BsonBoolean.FALSE);
        if (command.containsKey("compression") && command.get("compression").isArray()) {
            BsonArray accepted = new BsonArray();
            for (BsonValue c : command.getArray("compression")) {
                if (c.isString() && c.asString().getValue().equals("zlib")) {
                    accepted.add(c);
                }
            }
            if (!accepted.isEmpty()) {
                reply.put("compression", accepted);
            }
        }
        if (command.containsKey("saslSupportedMechs") && command.get("saslSupportedMechs").isString()) {
            String spec = command.getString("saslSupportedMechs").getValue();
            int dot = spec.indexOf('.');
            String username = dot >= 0 ? spec.substring(dot + 1) : spec;
            if (credentials.lookupPassword(username) != null) {
                reply.put("saslSupportedMechs", new BsonArray(List.of(new BsonString("SCRAM-SHA-256"))));
            }
        }
        reply.put("ok", new BsonDouble(1.0));
        return reply;
    }

    private BsonDocument saslStart(BsonDocument command) {
        String mechanism = command.containsKey("mechanism") ? command.getString("mechanism").getValue() : "";
        if (!"SCRAM-SHA-256".equals(mechanism)) {
            return new MongoCmdException(334, "Unsupported mechanism '" + mechanism + "' -- only SCRAM-SHA-256 is implemented").toReply();
        }
        String clientFirstMessage = new String(command.getBinary("payload").getData(), StandardCharsets.UTF_8);
        MongoScramConversation conversation;
        try {
            conversation = MongoScramConversation.start(clientFirstMessage, credentials);
        } catch (IllegalArgumentException malformed) {
            return new MongoCmdException(9, "Invalid SCRAM client-first-message: " + malformed.getMessage()).toReply();
        }
        if (conversation == null) {
            return new MongoCmdException(18, "Authentication failed.").toReply();
        }
        pendingScram = conversation;
        scramConversationId++;
        BsonDocument reply = ok();
        reply.put("conversationId", new BsonInt32(scramConversationId));
        reply.put("done", BsonBoolean.FALSE);
        reply.put("payload", new BsonBinary(conversation.serverFirstMessage().getBytes(StandardCharsets.UTF_8)));
        return reply;
    }

    private BsonDocument saslContinue(BsonDocument command) {
        int conversationId = command.getNumber("conversationId").intValue();
        if (pendingScram == null || conversationId != scramConversationId) {
            return new MongoCmdException(18, "Authentication failed.").toReply();
        }
        String clientFinalMessage = new String(command.getBinary("payload").getData(), StandardCharsets.UTF_8);
        String serverFinalMessage = pendingScram.verifyAndFinish(clientFinalMessage);
        if (serverFinalMessage == null) {
            pendingScram = null;
            return new MongoCmdException(18, "Authentication failed.").toReply();
        }
        pendingScram = null;
        authenticatedUser = "user";
        BsonDocument reply = ok();
        reply.put("conversationId", new BsonInt32(conversationId));
        reply.put("done", BsonBoolean.TRUE);
        reply.put("payload", new BsonBinary(serverFinalMessage.getBytes(StandardCharsets.UTF_8)));
        return reply;
    }

    private BsonDocument buildInfo() {
        BsonDocument reply = new BsonDocument();
        reply.put("version", new BsonString(VERSION));
        reply.put("gitVersion", new BsonString("warp-mongowire"));
        reply.put("modules", new BsonArray());
        reply.put("allocator", new BsonString("system"));
        reply.put("javascriptEngine", new BsonString("none"));
        reply.put("sysInfo", new BsonString("deprecated"));
        reply.put("versionArray", new BsonArray(List.of(new BsonInt32(7), new BsonInt32(0), new BsonInt32(0), new BsonInt32(0))));
        reply.put("bits", new BsonInt32(64));
        reply.put("debug", BsonBoolean.FALSE);
        reply.put("maxBsonObjectSize", new BsonInt32(16 * 1024 * 1024));
        reply.put("storageEngines", new BsonArray(List.of(new BsonString("postgres"))));
        reply.put("ok", new BsonDouble(1.0));
        return reply;
    }

    private BsonDocument getParameter(BsonDocument command) {
        BsonDocument reply = new BsonDocument();
        boolean all = false;
        int found = 0;
        for (Map.Entry<String, BsonValue> e : command.entrySet()) {
            String k = e.getKey();
            if (k.equals("getParameter")) {
                if (e.getValue().isString() && e.getValue().asString().getValue().equals("*")) {
                    all = true;
                }
                continue;
            }
            if (k.startsWith("$") || k.equals("lsid") || k.equals("comment") || k.equals("maxTimeMS") || k.equals("showDetails")
                    || k.equals("allParameters")) {
                continue;
            }
            BsonValue v = parameter(k);
            if (v == null) {
                throw new MongoCmdException(72, "no option found to get");
            }
            reply.put(k, v);
            found++;
        }
        if (all || command.containsKey("allParameters") && MongoExpr.truthy(command.get("allParameters"))) {
            for (String k : List.of("featureCompatibilityVersion", "authenticationMechanisms", "maxBSONDepth", "logLevel")) {
                reply.put(k, parameter(k));
            }
        } else if (found == 0 && !(command.get("getParameter").isString())) {
            // {getParameter: 1} alone
            throw new MongoCmdException(72, "no option found to get");
        }
        reply.put("ok", new BsonDouble(1.0));
        return reply;
    }

    private static BsonValue parameter(String name) {
        return switch (name) {
            case "featureCompatibilityVersion" -> new BsonDocument("version", new BsonString("7.0"));
            case "authenticationMechanisms" -> new BsonArray(List.of(new BsonString("SCRAM-SHA-256")));
            case "maxBSONDepth" -> new BsonInt32(200);
            case "logLevel" -> new BsonInt32(0);
            case "quiet" -> BsonBoolean.FALSE;
            case "internalQueryMaxBlockingSortMemoryUsageBytes" -> new BsonInt32(104857600);
            default -> null;
        };
    }

    private BsonDocument connectionStatus() {
        BsonArray users = new BsonArray();
        BsonArray roles = new BsonArray();
        if (authenticatedUser != null) {
            users.add(new BsonDocument("user", new BsonString(authenticatedUser)).append("db", new BsonString("admin")));
        }
        BsonDocument auth = new BsonDocument("authenticatedUsers", users).append("authenticatedUserRoles", roles);
        return new BsonDocument("authInfo", auth).append("ok", new BsonDouble(1.0));
    }

    private BsonDocument listCommands() {
        BsonDocument cmds = new BsonDocument();
        for (String c : COMMANDS) {
            cmds.put(c, new BsonDocument("help", new BsonString("")).append("requiresAuth", BsonBoolean.FALSE).append("secondaryOk", BsonBoolean.TRUE)
                    .append("secondaryOverrideOk", BsonBoolean.FALSE).append("apiVersions", new BsonArray()).append("deprecatedApiVersions", new BsonArray())
                    .append("adminOnly", BsonBoolean.valueOf(List.of("listDatabases", "renameCollection", "hostInfo", "serverStatus").contains(c))));
        }
        return new BsonDocument("commands", cmds).append("ok", new BsonDouble(1.0));
    }

    private BsonDocument hostInfo() {
        BsonDocument system = new BsonDocument("currentTime", new BsonDateTime(System.currentTimeMillis()))
                .append("hostname", new BsonString("warp")).append("cpuAddrSize", new BsonInt32(64))
                .append("memSizeMB", new BsonInt64(Runtime.getRuntime().maxMemory() / (1024 * 1024)))
                .append("numCores", new BsonInt32(Runtime.getRuntime().availableProcessors()))
                .append("cpuArch", new BsonString(System.getProperty("os.arch"))).append("numaEnabled", BsonBoolean.FALSE);
        BsonDocument os = new BsonDocument("type", new BsonString(System.getProperty("os.name"))).append("name", new BsonString(System.getProperty("os.name")))
                .append("version", new BsonString(System.getProperty("os.version")));
        return new BsonDocument("system", system).append("os", os).append("extra", new BsonDocument()).append("ok", new BsonDouble(1.0));
    }

    // ------------------------------------------------------------------ explain

    private BsonDocument explain(BsonDocument command, String db) throws SQLException {
        BsonValue inner = command.get("explain");
        if (inner == null || !inner.isDocument() || inner.asDocument().isEmpty()) {
            throw new MongoCmdException(14, "BSON field 'explain.explain' is the wrong type, expected type 'object'");
        }
        BsonDocument cmd = inner.asDocument().clone();
        if (!cmd.containsKey("$db")) {
            cmd.put("$db", new BsonString(db));
        }
        String verbosity = command.containsKey("verbosity") && command.get("verbosity").isString()
                ? command.getString("verbosity").getValue() : "allPlansExecution";
        String cname = cmd.getFirstKey();
        if (!List.of("find", "aggregate", "count", "distinct", "update", "delete", "findAndModify").contains(cname)) {
            throw new MongoCmdException(59, "Explain failed due to unknown command: " + cname);
        }
        String coll = cmd.get(cname).isString() ? cmd.getString(cname).getValue() : "";
        BsonDocument filter = cmd.containsKey("filter") && cmd.get("filter").isDocument() ? cmd.getDocument("filter")
                : cmd.containsKey("query") && cmd.get("query").isDocument() ? cmd.getDocument("query") : new BsonDocument();
        BsonDocument r = new BsonDocument("explainVersion", new BsonString("1"));
        r.put("queryPlanner", MongoCrud.planner(db + "." + coll, filter));
        if (!verbosity.equals("queryPlanner")) {
            long n = 0;
            long examined = 0;
            if (cname.equals("find")) {
                BsonDocument c2 = cmd.clone();
                c2.put("batchSize", new BsonInt32(0));
                c2.remove("limit");
                BsonDocument all = crudCount(coll, db, filter);
                examined = all.getInt64("n").getValue();
                n = examined;
                BsonDocument countCmd = new BsonDocument("count", new BsonString(coll)).append("query", filter).append("$db", new BsonString(db));
                n = MongoNum.truncLong(crud.count(countCmd, db).get("n"));
                if (cmd.containsKey("limit") && MongoNum.truncLong(cmd.get("limit")) > 0) {
                    n = Math.min(n, MongoNum.truncLong(cmd.get("limit")));
                }
            }
            BsonDocument stages = new BsonDocument("stage", new BsonString("COLLSCAN")).append("nReturned", new BsonInt32((int) n))
                    .append("executionTimeMillisEstimate", new BsonInt32(0)).append("works", new BsonInt64(examined + 1))
                    .append("advanced", new BsonInt64(n)).append("direction", new BsonString("forward"))
                    .append("docsExamined", new BsonInt64(examined));
            r.put("executionStats", new BsonDocument("executionSuccess", BsonBoolean.TRUE).append("nReturned", new BsonInt32((int) n))
                    .append("executionTimeMillis", new BsonInt32(0)).append("totalKeysExamined", new BsonInt64(0))
                    .append("totalDocsExamined", new BsonInt64(examined)).append("executionStages", stages));
        }
        r.put("command", cmd);
        r.put("serverInfo", MongoCrud.serverInfo());
        r.put("serverParameters", new BsonDocument("internalQueryFacetBufferSizeBytes", new BsonInt32(104857600)));
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    private BsonDocument crudCount(String coll, String db, BsonDocument filter) throws SQLException {
        BsonDocument countCmd = new BsonDocument("count", new BsonString(coll)).append("$db", new BsonString(db));
        BsonDocument r = crud.count(countCmd, db);
        return new BsonDocument("n", new BsonInt64(MongoNum.truncLong(r.get("n"))));
    }

    static List<String> commands() {
        return new ArrayList<>(COMMANDS);
    }
}
