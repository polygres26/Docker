package com.polygres.wire.server;

public final class ServerOptions {

    /** Which backend's result is returned to the client when dual execution is enabled. */
    public enum DualExecAuthority {
        POSTGRES, ORACLE
    }

    /**
     * JDBC: the existing OracleBackendPool/JdbcBackendExecutor path — full rewrite/routing
     * pipeline support, but ResponseWriter reconstructs DESCRIBE_INFO/inline-exhaustion bytes
     * from JDBC metadata, which live testing found a real, un-derivable gap in (see
     * ResponseWriter's own javadoc history) — java.sql.ResultSetMetaData never exposes the
     * backend-computed al8o4/RPA array a real client's own OALL8 response includes.
     *
     * NATIVE: relays the real backend's raw TTC bytes for a query's own DESCRIBE_INFO/ROW_DATA/
     * terminator instead of reconstructing them — see NativeByteCaptureProxy's javadoc for how.
     * Narrow slice: only statements whose entire result exhausts within the first EXECUTE
     * response (no separate FETCH continuation) are supported by this first version; anything
     * needing SQL rewriting/routing/dual-exec still needs the JDBC path.
     */
    public enum OracleBackendMode {
        JDBC, NATIVE
    }

    private final int listenPort;
    private final int pgWireListenPort;
    private final int myWireListenPort;
    private final int grpcPort;
    private final int httpPort;
    private final int httpsPort;
    private final String pgHost;
    private final int pgPort;
    private final String pgDatabase;
    private final String pgUser;
    private final String pgPassword;
    private final String pgStandbyHost;
    private final int pgStandbyPort;
    private final boolean tlsEnabled;
    private final int tlsPort;
    private final int grpcTlsPort;
    private final String tlsKeystorePath;
    private final String tlsKeystorePassword;
    private final boolean dualExecEnabled;
    private final DualExecAuthority dualExecAuthority;
    private final boolean dualExecRequireBoth;
    private final boolean dualExecXaEnabled;
    private final boolean dualExecShadowEnabled;
    private final String oracleHost;
    private final int oraclePort;
    private final String oracleServiceName;
    private final OracleBackendMode oracleBackendMode;
    private final boolean mywireNativeBackend;
    private final String mysqlHost;
    private final int mysqlPort;
    private final String mysqlDatabase;
    private final String mysqlUser;
    private final String mysqlPassword;
    private final int mssqlWireListenPort;

    private ServerOptions(int listenPort, int pgWireListenPort, int myWireListenPort, int grpcPort, int httpPort, int httpsPort, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            String pgStandbyHost, int pgStandbyPort,
            boolean tlsEnabled, int tlsPort, int grpcTlsPort,
            String tlsKeystorePath, String tlsKeystorePassword,
            boolean dualExecEnabled, DualExecAuthority dualExecAuthority, boolean dualExecRequireBoth, boolean dualExecXaEnabled,
            boolean dualExecShadowEnabled,
            String oracleHost, int oraclePort, String oracleServiceName, OracleBackendMode oracleBackendMode,
            boolean mywireNativeBackend, String mysqlHost, int mysqlPort, String mysqlDatabase, String mysqlUser, String mysqlPassword,
            int mssqlWireListenPort) {
        this.listenPort = listenPort;
        this.pgWireListenPort = pgWireListenPort;
        this.myWireListenPort = myWireListenPort;
        this.grpcPort = grpcPort;
        this.httpPort = httpPort;
        this.httpsPort = httpsPort;
        this.pgHost = pgHost;
        this.pgPort = pgPort;
        this.pgDatabase = pgDatabase;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
        this.pgStandbyHost = pgStandbyHost;
        this.pgStandbyPort = pgStandbyPort;
        this.tlsEnabled = tlsEnabled;
        this.tlsPort = tlsPort;
        this.grpcTlsPort = grpcTlsPort;
        this.tlsKeystorePath = tlsKeystorePath;
        this.tlsKeystorePassword = tlsKeystorePassword;
        this.dualExecEnabled = dualExecEnabled;
        this.dualExecAuthority = dualExecAuthority;
        this.dualExecRequireBoth = dualExecRequireBoth;
        this.dualExecXaEnabled = dualExecXaEnabled;
        this.dualExecShadowEnabled = dualExecShadowEnabled;
        this.oracleHost = oracleHost;
        this.oraclePort = oraclePort;
        this.oracleServiceName = oracleServiceName;
        this.oracleBackendMode = oracleBackendMode;
        this.mywireNativeBackend = mywireNativeBackend;
        this.mysqlHost = mysqlHost;
        this.mysqlPort = mysqlPort;
        this.mysqlDatabase = mysqlDatabase;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;
        this.mssqlWireListenPort = mssqlWireListenPort;
    }

    public static ServerOptions parse(String[] args) {
        // TODO: replace with real CLI parsing; hardcoded defaults for local dev only.
        // One shared keystore backs TLS for all four client-facing frontends (orawire TCPS,
        // pgwire, mywire, gRPC) -- renamed from Omnigate's POLYWIRE_TLS_* to POLYWIRE_TLS_* for
        // consistency with everything else already renamed in this port. orawire gets a second,
        // TLS-only listener port alongside its existing plain-TCP one (same design Omnigate uses
        // for Oracle TCPS); gRPC gets the same treatment now that it's actually started (it wasn't
        // wired into Main at all before this). pgwire and mywire are both different: each
        // negotiates TLS in-band on its existing plain port (Postgres's real SSLRequest handshake,
        // MySQL's real CLIENT_SSL capability-flag handshake) instead of a separate port -- see
        // Main's class javadoc and PgWireSessionHandler/MySqlWireSessionHandler's own javadoc for
        // why (a dedicated always-TLS port doesn't work for either protocol's real clients).
        String keystorePath = System.getenv("POLYWIRE_TLS_KEYSTORE");
        boolean tlsEnabled = keystorePath != null && !keystorePath.isBlank();
        int tlsPort = parseIntEnv("POLYWIRE_TLS_PORT", 2484);
        int grpcTlsPort = parseIntEnv("POLYWIRE_GRPC_TLS_PORT", 17071);
        String keystorePassword = System.getenv("POLYWIRE_TLS_KEYSTORE_PASSWORD");

        boolean dualExecEnabled = parseBoolEnv("POLYWIRE_DUAL_EXEC_ENABLED", false);
        DualExecAuthority dualExecAuthority = "oracle".equalsIgnoreCase(
                System.getenv().getOrDefault("POLYWIRE_DUAL_EXEC_AUTHORITY", "postgres"))
                ? DualExecAuthority.ORACLE : DualExecAuthority.POSTGRES;
        boolean dualExecRequireBoth = parseBoolEnv("POLYWIRE_DUAL_EXEC_REQUIRE_BOTH", false);
        boolean dualExecXaEnabled = parseBoolEnv("POLYWIRE_DUAL_EXEC_XA_ENABLED", false);
        // Lets a pure single-backend homogeneous mode (Oracle driver/client -> polywire -> real
        // Oracle, no Postgres involved at all) reuse the existing DUAL_EXEC_AUTHORITY=oracle
        // plumbing without ever actually touching Postgres: when false, RequestLoop skips every
        // executeShadow() call (Postgres shadow AND N-way replicas) entirely, so the lazily-opened
        // Postgres connection this session would otherwise borrow is never actually connected —
        // no live Postgres backend is required at runtime, only the static POLYWIRE_AUTH_USER/
        // PASSWORD credential the O5LOGON handshake itself checks against.
        boolean dualExecShadowEnabled = parseBoolEnv("POLYWIRE_DUAL_EXEC_SHADOW_ENABLED", true);
        String oracleHost = System.getenv().getOrDefault("POLYWIRE_ORACLE_HOST", "localhost");
        int oraclePort = parseIntEnv("POLYWIRE_ORACLE_PORT", 1521);
        String oracleServiceName = System.getenv().getOrDefault("POLYWIRE_ORACLE_SERVICE", "orcl");
        OracleBackendMode oracleBackendMode = "native".equalsIgnoreCase(
                System.getenv().getOrDefault("POLYWIRE_ORACLE_BACKEND_MODE", "jdbc"))
                ? OracleBackendMode.NATIVE : OracleBackendMode.JDBC;

        // POLYWIRE_MYWIRE_BACKEND=mysql: mirrors POLYWIRE_DUAL_EXEC_AUTHORITY=oracle's role for
        // orawire — mywire's default (and, until now, only) mode proxies onto the shared
        // Postgres backend like every other frontend; this puts a real MySQL/MariaDB instance
        // behind the MySQL-wire frontend instead, for the homogeneous (MySQL client -> polywire
        // -> real MySQL/MariaDB) case. Deliberately NOT a dual-exec-style shadow/authority pair
        // like Oracle's — mywire has no cross-dialect translation need in this mode (client SQL
        // is already MySQL dialect, target is real MySQL/MariaDB), so it's a simple either/or
        // backend selection, not two backends running side by side.
        boolean mywireNativeBackend = "mysql".equalsIgnoreCase(
                System.getenv().getOrDefault("POLYWIRE_MYWIRE_BACKEND", "postgres"));
        String mysqlHost = System.getenv().getOrDefault("POLYWIRE_MYSQL_HOST", "localhost");
        int mysqlPort = parseIntEnv("POLYWIRE_MYSQL_PORT", 3306);
        String mysqlDatabase = System.getenv().getOrDefault("POLYWIRE_MYSQL_DATABASE", "mysql");
        String mysqlUser = System.getenv("POLYWIRE_MYSQL_USER");
        String mysqlPassword = System.getenv("POLYWIRE_MYSQL_PASSWORD");

        int pgWireListenPort = parseIntEnv("POLYWIRE_PGWIRE_PORT", 15432);
        int myWireListenPort = parseIntEnv("POLYWIRE_MYWIRE_PORT", 13306);
        int orawireListenPort = parseIntEnv("POLYWIRE_ORAWIRE_PORT", 11521);
        // TDS (SQL Server wire) — see com.polygres.wire.mssqlwire package javadoc. Default 14333:
        // clear of every other PolyWire port (15432/13306/11521/19090/7070/8080/8443/2484/17071)
        // and of SQL Server's own default (1433).
        int mssqlWireListenPort = parseIntEnv("POLYWIRE_MSSQLWIRE_PORT", 14333);
        int grpcPort = parseIntEnv("POLYWIRE_GRPC_PORT", 7070);
        int httpPort = parseIntEnv("POLYWIRE_HTTP_PORT", 8080);
        // Reuses the same keystore as the Oracle TCPS listener (POLYWIRE_TLS_KEYSTORE) — one cert
        // to manage per deployment. The HTTPS console/API connector only starts when that
        // keystore is configured; see PolyWireHttpServer.start().
        int httpsPort = parseIntEnv("POLYWIRE_HTTPS_PORT", 8443);

        String pgHost = System.getenv().getOrDefault("POLYWIRE_PG_HOST", "localhost");
        int pgPort = parseIntEnv("POLYWIRE_PG_PORT", 5432);
        String pgDatabase = System.getenv().getOrDefault("POLYWIRE_PG_DATABASE", "postgres");
        String pgUser = System.getenv("POLYWIRE_PG_USER"); // null is valid: local trust-auth Postgres needs no credentials
        String pgPassword = System.getenv("POLYWIRE_PG_PASSWORD");
        // Standby shares pgUser/pgPassword/pgDatabase — same credentials/schema on both sides of an HA pair.
        // Failover is opt-in: PgConnections only engages it when this is non-blank (see that class's javadoc).
        String pgStandbyHost = System.getenv("POLYWIRE_PG_STANDBY_HOST");
        int pgStandbyPort = parseIntEnv("POLYWIRE_PG_STANDBY_PORT", pgPort);

        return new ServerOptions(orawireListenPort, pgWireListenPort, myWireListenPort, grpcPort, httpPort, httpsPort, pgHost, pgPort, pgDatabase, pgUser, pgPassword,
                pgStandbyHost, pgStandbyPort,
                tlsEnabled, tlsPort, grpcTlsPort, keystorePath, keystorePassword,
                dualExecEnabled, dualExecAuthority, dualExecRequireBoth, dualExecXaEnabled,
                dualExecShadowEnabled,
                oracleHost, oraclePort, oracleServiceName, oracleBackendMode,
                mywireNativeBackend, mysqlHost, mysqlPort, mysqlDatabase, mysqlUser, mysqlPassword,
                mssqlWireListenPort);
    }

    public int mssqlWireListenPort() {
        return mssqlWireListenPort;
    }

    private static int parseIntEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }

    private static boolean parseBoolEnv(String name, boolean defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Boolean.parseBoolean(value);
    }

    public int listenPort() {
        return listenPort;
    }

    public int pgWireListenPort() {
        return pgWireListenPort;
    }

    public int myWireListenPort() {
        return myWireListenPort;
    }

    public int grpcPort() {
        return grpcPort;
    }

    public int httpPort() {
        return httpPort;
    }

    public int httpsPort() {
        return httpsPort;
    }

    public String pgHost() {
        return pgHost;
    }

    public int pgPort() {
        return pgPort;
    }

    public String pgUser() {
        return pgUser;
    }

    public String pgPassword() {
        return pgPassword;
    }

    public String pgDatabase() {
        return pgDatabase;
    }

    public String pgStandbyHost() {
        return pgStandbyHost;
    }

    public int pgStandbyPort() {
        return pgStandbyPort;
    }

    public boolean tlsEnabled() {
        return tlsEnabled;
    }

    public int tlsPort() {
        return tlsPort;
    }

    public int grpcTlsPort() {
        return grpcTlsPort;
    }

    public String tlsKeystorePath() {
        return tlsKeystorePath;
    }

    public String tlsKeystorePassword() {
        return tlsKeystorePassword;
    }

    public boolean dualExecEnabled() {
        return dualExecEnabled;
    }

    public DualExecAuthority dualExecAuthority() {
        return dualExecAuthority;
    }

    public boolean dualExecRequireBoth() {
        return dualExecRequireBoth;
    }

    public boolean dualExecXaEnabled() {
        return dualExecXaEnabled;
    }

    public boolean dualExecShadowEnabled() {
        return dualExecShadowEnabled;
    }

    public String oracleHost() {
        return oracleHost;
    }

    public int oraclePort() {
        return oraclePort;
    }

    public String oracleServiceName() {
        return oracleServiceName;
    }

    public OracleBackendMode oracleBackendMode() {
        return oracleBackendMode;
    }

    public boolean mywireNativeBackend() {
        return mywireNativeBackend;
    }

    public String mysqlHost() {
        return mysqlHost;
    }

    public int mysqlPort() {
        return mysqlPort;
    }

    public String mysqlDatabase() {
        return mysqlDatabase;
    }

    public String mysqlUser() {
        return mysqlUser;
    }

    public String mysqlPassword() {
        return mysqlPassword;
    }
}
