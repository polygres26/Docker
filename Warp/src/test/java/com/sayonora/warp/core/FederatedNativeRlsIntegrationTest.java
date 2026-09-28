package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real proof that {@code WARP_ACCESS_NATIVE_RLS_DIALECTS} propagates the calling
 * {@link AccessContext} into EVERY backend {@link SchemaFederationStage} mounts for a federated
 * query, not just a single default connection -- the actual gap this feature closes (see
 * {@code SchemaFederationStage}'s own {@code nativeRlsInitializers} field javadoc). Two real
 * Postgres backends, each with its own real {@code CREATE POLICY ... FORCE ROW LEVEL SECURITY},
 * joined in ONE federated {@code SELECT} (via {@code WARP_ROUTER_SCHEMA_RULES}, same mechanism
 * {@link com.sayonora.warp.mcp.QueryFederatedIntegrationTest} uses); two real pgwire logins
 * (alice/bob, {@code WARP_AUTH_MODE=postgres_roles}, same real-role-identity mechanism
 * {@code PostgresRolesRlsIntegrationTest} uses for a single backend) prove the SAME session
 * identity is correctly propagated to BOTH mounted connections, not just the one the client
 * happened to authenticate against.
 */
class FederatedNativeRlsIntegrationTest {

    private RealPostgres ordersDb;
    private RealPostgres customersDb;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (customersDb != null) customersDb.close();
        if (ordersDb != null) ordersDb.close();
    }

    @Test
    void nativeRlsFiltersBothMountedBackendsInOneFederatedQueryPerCaller() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();

        // ordersDb is both the "default" backend (so WARP_AUTH_MODE=postgres_roles has somewhere
        // real to verify alice/bob's login against) AND one of the two federated mounts -- same
        // non-superuser-bypass gotcha PostgresRolesRlsIntegrationTest already documents: Warp's own
        // backend role here must NOT be a superuser or the table owner, or RLS silently never
        // applies regardless of what warp.user_id is set to.
        try (Connection admin = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = admin.createStatement()) {
            st.execute("CREATE ROLE warp_admin LOGIN PASSWORD 'warp-admin-pw'");
            st.execute("GRANT SELECT ON pg_authid TO warp_admin");
            st.execute("GRANT ALL ON SCHEMA public TO warp_admin");
            st.execute("CREATE ROLE alice LOGIN PASSWORD 'alice-pw'");
            st.execute("CREATE ROLE bob LOGIN PASSWORD 'bob-pw'");
            st.execute("CREATE SCHEMA orders_db");
            st.execute("CREATE TABLE orders_db.orders (id INTEGER PRIMARY KEY, owner_user TEXT, amount INTEGER)");
            st.execute("INSERT INTO orders_db.orders VALUES (1, 'alice', 50), (2, 'alice', 75), (3, 'bob', 20)");
            st.execute("GRANT USAGE ON SCHEMA orders_db TO warp_admin");
            st.execute("GRANT SELECT ON orders_db.orders TO warp_admin");
            st.execute("ALTER TABLE orders_db.orders ENABLE ROW LEVEL SECURITY");
            st.execute("CREATE POLICY orders_isolation ON orders_db.orders "
                    + "USING (owner_user = current_setting('warp.user_id', true))");
        }

        // customersDb is a SEPARATE backend, mounted only via WARP_ROUTER_SCHEMA_RULES -- never
        // touched by pgwire login/auth at all. If native RLS is genuinely wired into
        // SchemaFederationStage's own mount-building code (not just JdbcBackendExecutor's single-
        // connection path), THIS backend's connection must also see warp.user_id correctly, purely
        // from the AccessContext Statement already carries -- there is no other channel by which
        // this backend could ever learn who the caller is.
        try (Connection admin = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = admin.createStatement()) {
            st.execute("CREATE ROLE customers_svc LOGIN PASSWORD 'customers-svc-pw'");
            st.execute("GRANT ALL ON SCHEMA public TO customers_svc");
            st.execute("CREATE SCHEMA customers_db");
            st.execute("CREATE TABLE customers_db.customers (id INTEGER PRIMARY KEY, owner_user TEXT, name TEXT)");
            st.execute("INSERT INTO customers_db.customers VALUES (100, 'alice', 'alice-contact'), (101, 'bob', 'bob-contact')");
            st.execute("GRANT USAGE ON SCHEMA customers_db TO customers_svc");
            st.execute("GRANT SELECT ON customers_db.customers TO customers_svc");
            st.execute("ALTER TABLE customers_db.customers ENABLE ROW LEVEL SECURITY");
            st.execute("CREATE POLICY customers_isolation ON customers_db.customers "
                    + "USING (owner_user = current_setting('warp.user_id', true))");
        }

        String backends = "default=" + ordersDb.jdbcUrl() + "|warp_admin|warp-admin-pw"
                + ";customers_backend=" + customersDb.jdbcUrl() + "|customers_svc|customers-svc-pw";
        warp = WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), "warp_admin", "warp-admin-pw")
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_ROUTER_SCHEMA_RULES", "orders_db:default,customers_db:customers_backend")
                .env("WARP_AUTH_MODE", "postgres_roles")
                .env("WARP_ACCESS_NATIVE_RLS_DIALECTS", "postgres")
                // This dev machine can have another, unrelated process already bound to the
                // default gRPC port 7070 -- pick a free one explicitly rather than hit that
                // collision (a pre-existing gap shared by several other integration tests, not
                // something new here).
                .env("WARP_GRPC_PORT", String.valueOf(findFreePort()))
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        assertEquals(List.of("alice-order/alice-contact", "alice-order/alice-contact"),
                federatedRows("alice", "alice-pw"),
                "alice must see only her own rows from BOTH backends in the one federated query");
        assertEquals(List.of("bob-order/bob-contact"), federatedRows("bob", "bob-pw"),
                "bob must see only his own rows from BOTH backends -- proving customers_db's RLS "
                        + "(a backend alice/bob never authenticate against directly) was ALSO "
                        + "correctly given the caller's real identity, not just orders_db's");
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private List<String> federatedRows(String username, String password) throws Exception {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
        List<String> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, username, password);
                Statement st = conn.createStatement();
                // A deliberately trivial cross-condition (both sides RLS-filtered independently to
                // exactly one caller's own rows) -- the point is proving BOTH mounts were filtered
                // correctly, not exercising join-key logic, which every other federation test
                // already covers.
                ResultSet rs = st.executeQuery(
                        "SELECT o.owner_user || '-order' || '/' || c.name FROM orders_db.orders o, "
                                + "customers_db.customers c WHERE o.owner_user = c.owner_user ORDER BY 1")) {
            while (rs.next()) {
                rows.add(rs.getString(1));
            }
        }
        return rows;
    }
}
