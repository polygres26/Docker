package com.sayonora.warp.orawire.session;

import com.sayonora.warp.pgwire.PgBackendPool;
import com.sayonora.warp.core.PipelineStage;
import com.sayonora.warp.orawire.frontend.ConnectDescriptor;
import com.sayonora.warp.orawire.frontend.ConnectHandshake;
import com.sayonora.warp.orawire.frontend.ProtocolNegotiation;
import com.sayonora.warp.auth.CredentialStore;
import com.sayonora.warp.orawire.frontend.auth.O5LogonHandler;
import com.sayonora.warp.server.ServerOptions;
import com.sayonora.warp.orawire.wireformat.TnsPacketReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SessionHandler.class);

    private final Socket clientSocket;
    private final PgBackendPool backendPool;
    private final ServerOptions options;
    private final List<PipelineStage> sharedStages;
    private final com.sayonora.warp.core.BackendRegistry backendRegistry;
    private final com.sayonora.warp.audit.AuditLog auditLog;
    private final CredentialStore credentials = new CredentialStore();

    public SessionHandler(Socket clientSocket, PgBackendPool backendPool,
            ServerOptions options, List<PipelineStage> sharedStages, com.sayonora.warp.core.BackendRegistry backendRegistry) {
        this(clientSocket, backendPool, options, sharedStages, backendRegistry, null);
    }

    public SessionHandler(Socket clientSocket, PgBackendPool backendPool,
            ServerOptions options, List<PipelineStage> sharedStages, com.sayonora.warp.core.BackendRegistry backendRegistry,
            com.sayonora.warp.audit.AuditLog auditLog) {
        this.clientSocket = clientSocket;
        this.backendPool = backendPool;
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.auditLog = auditLog;
    }

    @Override
    public void run() {

        // Real bug found live by the sqlpaths matrix framework (Warp/tests/sqlpaths, oracle
        // engine plug-in): this bypass used to also require options.dualExecEnabled() &&
        // dualExecAuthority()==ORACLE on top of oracleBackendMode()==NATIVE. But
        // docs/WARP_GUIDE.md §8.1.1 documents WARP_ORACLE_BACKEND_MODE=relay, by itself, as the
        // switch to native-backend (transparent-proxy) mode -- dual-exec is an unrelated,
        // independent feature (shadow-executing against both backends for comparison). With the
        // old guard, setting only WARP_ORACLE_BACKEND_MODE=relay (as the docs say to) fell
        // through to the ordinary runPlain() path below, which borrows a *Postgres* backend
        // connection and authenticates the client's real Oracle username/password against
        // CredentialStore's Postgres-oriented shared secret instead of the real Oracle instance
        // -- every relay login failed with a real, reproducible ORA-01017 "invalid credential"
        // even though the exact same credentials worked directly against the real Oracle
        // database. NativeSessionRelay's raw byte-pump bypass never got a chance to run.
        // oracleBackendMode()==NATIVE alone is now sufficient and sole condition, matching the
        // documented contract; dual-exec (shadow execution against Postgres for comparison while
        // Oracle is authoritative) still works unchanged under oracleBackendMode()==JDBC via
        // openDualExecOracleConnection() below, which is a separate feature.
        if (options.oracleBackendMode() == ServerOptions.OracleBackendMode.RELAY) {
            try (Socket socket = clientSocket) {
                com.sayonora.warp.orawire.backend.NativeSessionRelay.relay(
                        socket, options.oracleHost(), options.oraclePort());
            } catch (IOException e) {
                log.warn("native session relay ended: {}", e.toString());
            }
            return;
        }
        try (Socket socket = clientSocket) {
            TnsPacketReader reader = new TnsPacketReader(socket.getInputStream());
            OutputStream out = socket.getOutputStream();

            ConnectDescriptor descriptor = new ConnectHandshake().perform(reader, out);
            log.info("client connected, service={}", descriptor.serviceName());

            if (reader.isAnoEligible()) {
                
                new com.sayonora.warp.orawire.frontend.AnoNegotiation().perform(reader, out);
            }

            new ProtocolNegotiation().perform(reader, out);

            // Connect-time backend routing: the TNS service name (and the verified login user) select a
            // backend or backend set. Resolved inside authentication, after the password is verified.
            final com.sayonora.warp.core.ConnectionRoute[] routed = {com.sayonora.warp.core.ConnectionRoute.UNROUTED};
            O5LogonHandler.AuthResult auth = new O5LogonHandler(bridgeLoginCredentials(options)).authenticate(reader, out, user -> {
                if (backendRegistry == null) {
                    return null;
                }
                routed[0] = backendRegistry.connectionRouter().resolve(
                        com.sayonora.warp.core.ConnectionRouter.PROTO_ORACLE, descriptor.serviceName(), user);
                return routed[0].isRejected()
                        ? "ORA-12514: TNS:listener does not currently know of service requested in connect descriptor"
                        : null;
            });
            if (routed[0].isRejected()) {
                log.warn("connection refused: service {} is not routable ({})", descriptor.serviceName(),
                        routed[0].description());
                return;
            }
            if (!auth.success()) {
                log.warn("authentication failed for user={}", auth.username());
                if (auditLog != null) {
                    auditLog.record(com.sayonora.warp.audit.AuditEvent.of(
                            com.sayonora.warp.audit.AuditEvent.Type.DB_LOGIN_FAILED, auth.username(),
                            "orawire login failed for user \"" + auth.username() + "\""));
                }
                return;
            }
            if (auditLog != null) {
                auditLog.record(com.sayonora.warp.audit.AuditEvent.of(
                        com.sayonora.warp.audit.AuditEvent.Type.DB_LOGIN_SUCCEEDED, auth.username(),
                        "orawire login succeeded for user \"" + auth.username() + "\""
                                + (auth.realIdentity() ? " (real per-user credential)" : " (shared credential)")));
            }
            // Only a real, distinguishable per-user credential (WARP_AUTH_CREDENTIALS
            // configured -- see CredentialStore) is worth carrying into AccessContext: under the
            // single shared-credential default every caller presents the identical username, so
            // there's no real identity for PostgresRlsSessionInitializer's session GUC or an RLS
            // policy to key on. Same rule pgwire/mssqlwire apply via roleAuthCache != null.
            com.sayonora.warp.core.AccessContext accessContext = auth.realIdentity()
                    ? new com.sayonora.warp.core.AccessContext(auth.username(), java.util.Set.of(), java.util.Map.of())
                    : com.sayonora.warp.core.AccessContext.ANONYMOUS;

            String replicationBackends = System.getenv("WARP_REPLICATION_BACKENDS");
            if (replicationBackends != null && !replicationBackends.isBlank()) {
                runReplicated(reader, out, descriptor, auth, replicationBackends, accessContext, routed[0]);
            } else {

                runPlain(reader, out, descriptor, auth, accessContext, routed[0]);
            }
        } catch (Exception e) {
            log.warn("session terminated: {}", e.getMessage(), e);
        }
    }

    private void runPlain(TnsPacketReader reader, OutputStream out, ConnectDescriptor descriptor,
            O5LogonHandler.AuthResult auth, com.sayonora.warp.core.AccessContext accessContext,
            com.sayonora.warp.core.ConnectionRoute route) throws Exception {
        try (com.sayonora.warp.core.LazyPooledConnection pgConnection = backendPool.borrowConnection(descriptor, auth.username());
                com.sayonora.warp.core.LazyPooledConnection oracleConnection = openOracleConnectionForRequestLoop()) {
            RequestLoop loop = new RequestLoop(reader, out, pgConnection, oracleConnection, null, null, options,
                    sharedStages, backendRegistry, null, null, accessContext);
            loop.setConnectionRoute(route);
            loop.run();
        }
    }

    /** A real gap found live implementing PL/SQL support (see {@code RequestLoop
     * #handlePlSqlExecute}): {@code RequestLoop}'s constructor has taken an {@code oracleConnection}
     * parameter for its dual-exec-with-Oracle-authority path all along, but this class -- the only
     * caller that actually constructs a {@code RequestLoop} for a real client session -- always
     * passed {@code null} for it, in every code path. The dual-exec Oracle branches inside {@code
     * RequestLoop} (bind rewriting, shadow execution, and now PL/SQL) were consequently dead code
     * against a real server: only reachable from a test that constructs {@code RequestLoop}
     * directly, never from a real client connection. This wires it up for the one combination that
     * needs a real, directly-usable {@code java.sql.Connection} here: dual-exec enabled, Oracle as
     * authority, and {@code WARP_ORACLE_BACKEND_MODE=emulate} (the default) -- the {@code relay} mode
     * is handled entirely separately, above, via {@link
     * com.sayonora.warp.orawire.backend.NativeSessionRelay}'s raw byte relay, which never
     * constructs a {@code RequestLoop} (or any oracleConnection) at all. Returns {@code null} (same
     * as before this fix) for every other configuration, so a plain single-backend deployment is
     * completely unaffected. */
    private com.sayonora.warp.core.LazyPooledConnection openDualExecOracleConnection() {
        if (!options.dualExecEnabled()
                || options.dualExecAuthority() != ServerOptions.DualExecAuthority.ORACLE
                || options.oracleBackendMode() != ServerOptions.OracleBackendMode.EMULATE) {
            return null;
        }
        String url = "jdbc:oracle:thin:@//" + options.oracleHost() + ":" + options.oraclePort()
                + "/" + options.oracleServiceName();
        return new com.sayonora.warp.core.LazyPooledConnection(
                () -> java.sql.DriverManager.getConnection(url, options.oracleUser(), options.oraclePassword()), null);
    }

    /** Bridge mode's ({@code WARP_ORACLE_BACKEND_MODE=bridge}) shared, bounded, process-wide pool
     * of real Oracle JDBC connections (see {@link com.sayonora.warp.orawire.backend.OracleBridgePool}):
     * one pool per Warp process, lazily created on first Bridge-mode session, shared by every
     * client session afterward (many-to-few, unlike NATIVE's 1:1 raw relay or a fresh {@code
     * DriverManager.getConnection} per dual-exec session). */
    private static volatile com.sayonora.warp.orawire.backend.OracleBridgePool bridgePool;

    /** Bridge mode's client-facing login must accept the migrated app's OWN real Oracle
     * username/password (that's what "transparent" means for an app pointed at Warp instead of
     * straight at Oracle) -- never the ordinary {@code WARP_AUTH_*} secret every other mode
     * verifies against, since the app was never given that separate credential and was never
     * meant to be. {@code WARP_ORACLE_BRIDGE_LOGIN_CREDENTIALS} (multi-user,
     * {@code username=ref;username2=ref2}) or the single-account
     * {@code WARP_ORACLE_BRIDGE_LOGIN_USER}/{@code WARP_ORACLE_BRIDGE_LOGIN_PASSWORD} fallback
     * hold the real per-app-user Oracle password as a {@link com.sayonora.warp.secrets.SecretRef}
     * (a plain literal, or a {@code vault:}/{@code cyberark:} reference resolved fresh per login --
     * see CredentialStore#lookupPassword). This is deliberately independent of the pooled backend
     * connection Bridge queries actually run over (see {@link #bridgePool}), which authenticates to
     * the real Oracle instance as its OWN separate, shared service account (WARP_ORACLE_USER/
     * WARP_ORACLE_PASSWORD) -- login identity and query-execution identity are two different
     * credentials by design (see docs/WARP_GUIDE.md's Bridge-mode section): this keeps Bridge's
     * bounded many-to-few pool sized by concurrency, not by the number of distinct real Oracle
     * app accounts that may log in over time. If nothing is configured here, every Bridge login is
     * denied (CredentialStore#fromEnv's secure-by-default behavior) rather than silently accepting
     * an unrelated shared secret. */
    private static CredentialStore bridgeLoginCredentials(ServerOptions options) {
        if (options.oracleBackendMode() != ServerOptions.OracleBackendMode.BRIDGE) {
            return new CredentialStore();
        }
        return CredentialStore.fromEnv("WARP_ORACLE_BRIDGE_LOGIN_CREDENTIALS",
                "WARP_ORACLE_BRIDGE_LOGIN_USER", "WARP_ORACLE_BRIDGE_LOGIN_PASSWORD");
    }

    private static com.sayonora.warp.orawire.backend.OracleBridgePool bridgePool(ServerOptions options) {
        com.sayonora.warp.orawire.backend.OracleBridgePool pool = bridgePool;
        if (pool == null) {
            synchronized (SessionHandler.class) {
                pool = bridgePool;
                if (pool == null) {
                    pool = com.sayonora.warp.orawire.backend.OracleBridgePool.fromServerOptions(options);
                    bridgePool = pool;
                }
            }
        }
        return pool;
    }

    /** Chooses the real {@link java.sql.Connection} {@link RequestLoop} runs the client's parsed
     * SQL against, for whichever non-JDBC-default Oracle-execution feature applies to this session:
     * Bridge mode's pooled connection (this method's own new case) if {@code
     * WARP_ORACLE_BACKEND_MODE=bridge}, else dual-exec's one-off Oracle connection (unchanged,
     * existing behavior) if that separate feature is configured, else {@code null} (the ordinary
     * single-Postgres-backend case, also unchanged). */
    private com.sayonora.warp.core.LazyPooledConnection openOracleConnectionForRequestLoop() {
        if (options.oracleBackendMode() == ServerOptions.OracleBackendMode.BRIDGE) {
            com.sayonora.warp.orawire.backend.OracleBridgePool pool = bridgePool(options);
            return new com.sayonora.warp.core.LazyPooledConnection(pool::checkout, null);
        }
        return openDualExecOracleConnection();
    }

    private void runReplicated(TnsPacketReader reader, OutputStream out, ConnectDescriptor descriptor,
            O5LogonHandler.AuthResult auth, String replicationBackendsSpec,
            com.sayonora.warp.core.AccessContext accessContext,
            com.sayonora.warp.core.ConnectionRoute route) throws Exception {
        List<String> names = List.of(replicationBackendsSpec.split(",")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).toList();

        try (com.sayonora.warp.core.LazyPooledConnection pgConnection = backendPool.borrowConnection(descriptor, auth.username())) {
            List<Connection> replicaConnections = new java.util.ArrayList<>();
            try {
                for (String name : names) {
                    replicaConnections.add(requireBackend(name).openManualCommit());
                }
                RequestLoop loop = new RequestLoop(reader, out, pgConnection, null, replicaConnections, null, options,
                        sharedStages, backendRegistry, null, null, accessContext);
                loop.setConnectionRoute(route);
                loop.run();
            } finally {
                for (Connection replica : replicaConnections) {
                    closeQuietly(replica);
                }
            }
        }
    }

    private com.sayonora.warp.core.BackendTarget requireBackend(String name) throws java.sql.SQLException {
        com.sayonora.warp.core.BackendTarget target = backendRegistry.get(name);
        if (target == null) {
            throw new java.sql.SQLException("WARP_REPLICATION_BACKENDS references unknown backend \"" + name + "\"");
        }
        return target;
    }

    private static void closeQuietly(Connection connection) {
        try {
            connection.close();
        } catch (java.sql.SQLException e) {
            log.warn("failed to close replication connection: {}", e.getMessage());
        }
    }
}
