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

/**
 * Owns one client connection end to end: TNS connect handshake, O5LOGON
 * auth, then a request/response loop translating and forwarding SQL to a
 * pooled Postgres backend connection. Mirrors pgadapter's ConnectionHandler role.
 *
 * <p>Omnigate's Oracle dual-execution/shadow-verification path (a live JDBC connection to a real
 * Oracle backend, opened via {@code OracleBackendPool}) is cut, not ported — see {@link #runPlain}'s
 * javadoc.
 */
public final class SessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SessionHandler.class);

    private final Socket clientSocket;
    private final PgBackendPool backendPool;
    private final ServerOptions options;
    private final List<PipelineStage> sharedStages;
    private final com.polygres.wire.core.BackendRegistry backendRegistry;
    private final CredentialStore credentials = new CredentialStore();

    public SessionHandler(Socket clientSocket, PgBackendPool backendPool,
            ServerOptions options, List<PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.backendPool = backendPool;
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
    }

    @Override
    public void run() {
        // ORAPG_ORACLE_BACKEND_MODE=native: relay the whole session through orawire's own
        // listener instead of running any of PolyWire's own frontend protocol code at all — see
        // NativeSessionRelay's javadoc for why (orawire's frontend already handles every TTC
        // function code a real client sends; PolyWire's own dispatcher only ever covered a narrow
        // slice and was never going to catch up one function code at a time). Only engages for
        // the Oracle-authority dual-exec path (matches every other native-mode gate); every other
        // frontend (Postgres wire, MySQL wire) is unaffected.
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
                // Real OCI clients (sqlplus, rwloadsim) only — negotiated ACCEPT version >=320.
                // python-oracledb/ojdbc11 stay on the legacy ACCEPT shape and never set this, so
                // they never see this stage. See AnoNegotiation's javadoc for scope.
                new com.polygres.wire.orawire.frontend.AnoNegotiation().perform(reader, out);
            }

            new ProtocolNegotiation().perform(reader, out);

            O5LogonHandler.AuthResult auth = new O5LogonHandler().authenticate(reader, out);
            if (!auth.success()) {
                log.warn("authentication failed for user={}", auth.username());
                return;
            }

            String replicationBackends = System.getenv("POLYWIRE_REPLICATION_BACKENDS");
            if (replicationBackends != null && !replicationBackends.isBlank()) {
                runReplicated(reader, out, descriptor, auth, replicationBackends);
            } else {
                // Omnigate's XA/2PC dual-exec path (com.omnigate.xa.XaTransaction /
                // com.omnigate.xa.XaBackendFactory) is cut here, not stubbed: PolyWire doesn't
                // port the (excluded, per project scope) xa package, and PolyWire's runPlain below
                // no longer has an Oracle dual-exec branch at all (see its javadoc) — the whole
                // "shadow-verify against real Oracle" concept isn't part of PolyWire's Postgres-only
                // backend model.
                runPlain(reader, out, descriptor, auth);
            }
        } catch (Exception e) {
            log.warn("session terminated: {}", e.getMessage(), e);
        }
    }

    /**
     * PolyWire always runs the plain (Postgres-only) path here — Omnigate's dual-exec branch
     * (a live shadow connection to a real Oracle backend via {@code ojdbc11}, for
     * migration-verification) is cut, not ported: PolyWire's backend model is Postgres-only end to
     * end (see {@code BackendRegistry}/{@code BackendConnectionPools}), and this project's
     * instructions are explicit that no Oracle/MySQL/SQL-Server JDBC *driver* dependency should be
     * added, only the wire-protocol frontend code that speaks those protocols to clients.
     */
    private void runPlain(TnsPacketReader reader, OutputStream out, ConnectDescriptor descriptor,
            O5LogonHandler.AuthResult auth) throws Exception {
        try (com.polygres.wire.core.LazyPooledConnection pgConnection = backendPool.borrowConnection(descriptor, auth.username())) {
            new RequestLoop(reader, out, pgConnection, null, null, null, options, sharedStages, backendRegistry).run();
        }
    }

    /**
     * {@code POLYWIRE_REPLICATION_BACKENDS="name1,name2,..."} (referencing {@code POLYWIRE_BACKENDS}
     * entries): best-effort replication (independent commits, no distributed-transaction
     * coordinator) of the frontend's own Postgres connection out to N named replica backends. The
     * frontend's own Postgres connection is always authoritative.
     *
     * <p>Omnigate's XA/2PC variant of this path ({@code com.omnigate.xa.XaTransaction}/
     * {@code XaBackendFactory}, {@code POLYWIRE_REPLICATION_XA_ENABLED}) is cut here, not ported —
     * see the {@code xa} package exclusion note in this project's port notes. Only the best-effort
     * (independent-commit) branch survives.
     */
    private void runReplicated(TnsPacketReader reader, OutputStream out, ConnectDescriptor descriptor,
            O5LogonHandler.AuthResult auth, String replicationBackendsSpec) throws Exception {
        List<String> names = List.of(replicationBackendsSpec.split(",")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).toList();

        try (com.polygres.wire.core.LazyPooledConnection pgConnection = backendPool.borrowConnection(descriptor, auth.username())) {
            List<Connection> replicaConnections = new java.util.ArrayList<>();
            try {
                for (String name : names) {
                    replicaConnections.add(requireBackend(name).openManualCommit());
                }
                new RequestLoop(reader, out, pgConnection, null, replicaConnections, null, options, sharedStages,
                        backendRegistry).run();
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
