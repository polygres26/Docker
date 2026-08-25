package com.polygres.wire.orawire.session;

import com.polygres.wire.pgwire.PgBackendPool;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.orawire.frontend.ConnectDescriptor;
import com.polygres.wire.orawire.frontend.ConnectHandshake;
import com.polygres.wire.orawire.frontend.ProtocolNegotiation;
import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.orawire.frontend.auth.O5LogonHandler;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.orawire.wireformat.TnsPacketReader;
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
    private final com.polygres.wire.core.BackendRegistry backendRegistry;
    private final com.polygres.wire.audit.AuditLog auditLog;
    private final CredentialStore credentials = new CredentialStore();

    public SessionHandler(Socket clientSocket, PgBackendPool backendPool,
            ServerOptions options, List<PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this(clientSocket, backendPool, options, sharedStages, backendRegistry, null);
    }

    public SessionHandler(Socket clientSocket, PgBackendPool backendPool,
            ServerOptions options, List<PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry,
            com.polygres.wire.audit.AuditLog auditLog) {
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
                com.polygres.wire.orawire.backend.NativeSessionRelay.relay(
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
                
                new com.polygres.wire.orawire.frontend.AnoNegotiation().perform(reader, out);
            }

            new ProtocolNegotiation().perform(reader, out);

            O5LogonHandler.AuthResult auth = new O5LogonHandler().authenticate(reader, out);
            if (!auth.success()) {
                log.warn("authentication failed for user={}", auth.username());
                if (auditLog != null) {
                    auditLog.record(com.polygres.wire.audit.AuditEvent.of(
                            com.polygres.wire.audit.AuditEvent.Type.DB_LOGIN_FAILED, auth.username(),
                            "orawire login failed for user \"" + auth.username() + "\""));
                }
                return;
            }
            if (auditLog != null) {
                auditLog.record(com.polygres.wire.audit.AuditEvent.of(
                        com.polygres.wire.audit.AuditEvent.Type.DB_LOGIN_SUCCEEDED, auth.username(),
                        "orawire login succeeded for user \"" + auth.username() + "\""
                                + (auth.realIdentity() ? " (real per-user credential)" : " (shared credential)")));
            }
            // Only a real, distinguishable per-user credential (POLYWIRE_AUTH_CREDENTIALS
            // configured -- see CredentialStore) is worth carrying into AccessContext: under the
            // single shared-credential default every caller presents the identical username, so
            // there's no real identity for PostgresRlsSessionInitializer's session GUC or an RLS
            // policy to key on. Same rule pgwire/mssqlwire apply via roleAuthCache != null.
            com.polygres.wire.core.AccessContext accessContext = auth.realIdentity()
                    ? new com.polygres.wire.core.AccessContext(auth.username(), java.util.Set.of(), java.util.Map.of())
                    : com.polygres.wire.core.AccessContext.ANONYMOUS;

            String replicationBackends = System.getenv("POLYWIRE_REPLICATION_BACKENDS");
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
            O5LogonHandler.AuthResult auth, com.polygres.wire.core.AccessContext accessContext) throws Exception {
        try (com.polygres.wire.core.LazyPooledConnection pgConnection = backendPool.borrowConnection(descriptor, auth.username())) {
            new RequestLoop(reader, out, pgConnection, null, null, null, options, sharedStages, backendRegistry,
                    null, null, accessContext).run();
        }
    }

    private void runReplicated(TnsPacketReader reader, OutputStream out, ConnectDescriptor descriptor,
            O5LogonHandler.AuthResult auth, String replicationBackendsSpec,
            com.polygres.wire.core.AccessContext accessContext) throws Exception {
        List<String> names = List.of(replicationBackendsSpec.split(",")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).toList();

        try (com.polygres.wire.core.LazyPooledConnection pgConnection = backendPool.borrowConnection(descriptor, auth.username())) {
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

    private com.polygres.wire.core.BackendTarget requireBackend(String name) throws java.sql.SQLException {
        com.polygres.wire.core.BackendTarget target = backendRegistry.get(name);
        if (target == null) {
            throw new java.sql.SQLException("POLYWIRE_REPLICATION_BACKENDS references unknown backend \"" + name + "\"");
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
