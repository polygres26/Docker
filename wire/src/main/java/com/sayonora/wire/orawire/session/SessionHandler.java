package com.sayonora.wire.orawire.session;

import com.sayonora.wire.pgwire.PgBackendPool;
import com.sayonora.wire.core.PipelineStage;
import com.sayonora.wire.orawire.frontend.ConnectDescriptor;
import com.sayonora.wire.orawire.frontend.ConnectHandshake;
import com.sayonora.wire.orawire.frontend.ProtocolNegotiation;
import com.sayonora.wire.auth.CredentialStore;
import com.sayonora.wire.orawire.frontend.auth.O5LogonHandler;
import com.sayonora.wire.server.ServerOptions;
import com.sayonora.wire.orawire.wireformat.TnsPacketReader;
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
    private final com.sayonora.wire.core.BackendRegistry backendRegistry;
    private final com.sayonora.wire.audit.AuditLog auditLog;
    private final CredentialStore credentials = new CredentialStore();

    public SessionHandler(Socket clientSocket, PgBackendPool backendPool,
            ServerOptions options, List<PipelineStage> sharedStages, com.sayonora.wire.core.BackendRegistry backendRegistry) {
        this(clientSocket, backendPool, options, sharedStages, backendRegistry, null);
    }

    public SessionHandler(Socket clientSocket, PgBackendPool backendPool,
            ServerOptions options, List<PipelineStage> sharedStages, com.sayonora.wire.core.BackendRegistry backendRegistry,
            com.sayonora.wire.audit.AuditLog auditLog) {
        this.clientSocket = clientSocket;
        this.backendPool = backendPool;
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.auditLog = auditLog;
    }

    @Override
    public void run() {
        
        if (options.dualExecEnabled()
                && options.dualExecAuthority() == ServerOptions.DualExecAuthority.ORACLE
                && options.oracleBackendMode() == ServerOptions.OracleBackendMode.NATIVE) {
            try (Socket socket = clientSocket) {
                com.sayonora.wire.orawire.backend.NativeSessionRelay.relay(
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
                
                new com.sayonora.wire.orawire.frontend.AnoNegotiation().perform(reader, out);
            }

            new ProtocolNegotiation().perform(reader, out);

            O5LogonHandler.AuthResult auth = new O5LogonHandler().authenticate(reader, out);
            if (!auth.success()) {
                log.warn("authentication failed for user={}", auth.username());
                if (auditLog != null) {
                    auditLog.record(com.sayonora.wire.audit.AuditEvent.of(
                            com.sayonora.wire.audit.AuditEvent.Type.DB_LOGIN_FAILED, auth.username(),
                            "orawire login failed for user \"" + auth.username() + "\""));
                }
                return;
            }
            if (auditLog != null) {
                auditLog.record(com.sayonora.wire.audit.AuditEvent.of(
                        com.sayonora.wire.audit.AuditEvent.Type.DB_LOGIN_SUCCEEDED, auth.username(),
                        "orawire login succeeded for user \"" + auth.username() + "\""
                                + (auth.realIdentity() ? " (real per-user credential)" : " (shared credential)")));
            }
            // Only a real, distinguishable per-user credential (WARP_AUTH_CREDENTIALS
            // configured -- see CredentialStore) is worth carrying into AccessContext: under the
            // single shared-credential default every caller presents the identical username, so
            // there's no real identity for PostgresRlsSessionInitializer's session GUC or an RLS
            // policy to key on. Same rule pgwire/mssqlwire apply via roleAuthCache != null.
            com.sayonora.wire.core.AccessContext accessContext = auth.realIdentity()
                    ? new com.sayonora.wire.core.AccessContext(auth.username(), java.util.Set.of(), java.util.Map.of())
                    : com.sayonora.wire.core.AccessContext.ANONYMOUS;

            String replicationBackends = System.getenv("WARP_REPLICATION_BACKENDS");
            if (replicationBackends != null && !replicationBackends.isBlank()) {
                runReplicated(reader, out, descriptor, auth, replicationBackends, accessContext);
            } else {

                runPlain(reader, out, descriptor, auth, accessContext);
            }
        } catch (Exception e) {
            log.warn("session terminated: {}", e.getMessage(), e);
        }
    }

    private void runPlain(TnsPacketReader reader, OutputStream out, ConnectDescriptor descriptor,
            O5LogonHandler.AuthResult auth, com.sayonora.wire.core.AccessContext accessContext) throws Exception {
        try (com.sayonora.wire.core.LazyPooledConnection pgConnection = backendPool.borrowConnection(descriptor, auth.username());
                com.sayonora.wire.core.LazyPooledConnection oracleConnection = openDualExecOracleConnection()) {
            new RequestLoop(reader, out, pgConnection, oracleConnection, null, null, options, sharedStages, backendRegistry,
                    null, null, accessContext).run();
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
     * authority, and {@code WARP_ORACLE_BACKEND_MODE=jdbc} (the default) -- the {@code native} mode
     * is handled entirely separately, above, via {@link
     * com.sayonora.wire.orawire.backend.NativeSessionRelay}'s raw byte relay, which never
     * constructs a {@code RequestLoop} (or any oracleConnection) at all. Returns {@code null} (same
     * as before this fix) for every other configuration, so a plain single-backend deployment is
     * completely unaffected. */
    private com.sayonora.wire.core.LazyPooledConnection openDualExecOracleConnection() {
        if (!options.dualExecEnabled()
                || options.dualExecAuthority() != ServerOptions.DualExecAuthority.ORACLE
                || options.oracleBackendMode() != ServerOptions.OracleBackendMode.JDBC) {
            return null;
        }
        String url = "jdbc:oracle:thin:@//" + options.oracleHost() + ":" + options.oraclePort()
                + "/" + options.oracleServiceName();
        return new com.sayonora.wire.core.LazyPooledConnection(
                () -> java.sql.DriverManager.getConnection(url, options.oracleUser(), options.oraclePassword()), null);
    }

    private void runReplicated(TnsPacketReader reader, OutputStream out, ConnectDescriptor descriptor,
            O5LogonHandler.AuthResult auth, String replicationBackendsSpec,
            com.sayonora.wire.core.AccessContext accessContext) throws Exception {
        List<String> names = List.of(replicationBackendsSpec.split(",")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).toList();

        try (com.sayonora.wire.core.LazyPooledConnection pgConnection = backendPool.borrowConnection(descriptor, auth.username())) {
            List<Connection> replicaConnections = new java.util.ArrayList<>();
            try {
                for (String name : names) {
                    replicaConnections.add(requireBackend(name).openManualCommit());
                }
                new RequestLoop(reader, out, pgConnection, null, replicaConnections, null, options, sharedStages,
                        backendRegistry, null, null, accessContext).run();
            } finally {
                for (Connection replica : replicaConnections) {
                    closeQuietly(replica);
                }
            }
        }
    }

    private com.sayonora.wire.core.BackendTarget requireBackend(String name) throws java.sql.SQLException {
        com.sayonora.wire.core.BackendTarget target = backendRegistry.get(name);
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
