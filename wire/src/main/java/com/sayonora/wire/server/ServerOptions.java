package com.sayonora.wire.server;

public final class ServerOptions {

    public enum DualExecAuthority {
        POSTGRES, ORACLE
    }

    public enum OracleBackendMode {
        JDBC, NATIVE
    }

    /** Which real backend the MCP frontend's tools (execute_sql, list_tables, etc.) target.
     * POSTGRES (the default, unchanged behavior) runs every tool through the shared pipeline
     * against the configured Postgres backend, same as before this existed. The other three mirror
     * orawire/mywire/mssqlwire's own native-backend-mode pattern: bypass the shared pipeline
     * entirely and proxy straight to a real Oracle/MySQL/SQL Server connection of the gateway's
     * own (the WARP_ORACLE_, WARP_MYSQL_, WARP_MSSQL_ settings mywire/mssqlwire's own native modes
     * already use; MCP has no client login step to source per-caller credentials from
     * the way orawire's native mode does, so it needs WARP_ORACLE_USER/WARP_ORACLE_PASSWORD, a
     * gateway-held credential that didn't need to exist before this -- orawire's default mode never
     * connects outward to real Oracle at all, and its native mode sources credentials from the
     * client's own O5LOGON login instead). See WarpMcpServer's own javadoc for exactly which tools
     * work in each non-Postgres mode and which are refused with a clear error instead of silently
     * running Postgres-only SQL against a different dialect.
     */
    public enum McpBackendMode {
        POSTGRES, ORACLE, MYSQL, SQLSERVER
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
    private final String pgSslMode;
    private final String pgSslRootCert;
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
    private final String oracleUser;
    private final String oraclePassword;
    private final McpBackendMode mcpBackendMode;
    private final boolean mywireNativeBackend;
    private final String mysqlHost;
    private final int mysqlPort;
    private final String mysqlDatabase;
    private final String mysqlUser;
    private final String mysqlPassword;
    private final int mssqlWireListenPort;
    private final boolean mssqlwireNativeBackend;
    private final String mssqlHost;
    private final int mssqlPort;
    private final String mssqlDatabase;
    private final String mssqlUser;
    private final String mssqlPassword;
    // Second, independent listener port per backend for running BOTH native-passthrough and
    // dialect-translated (JDBC) modes from the SAME Warp process at once -- 0 means "disabled,"
    // same single-listener/single-mode behavior as before these existed. Previously native vs.
    // translated was a single global toggle (WARP_ORACLE_BACKEND_MODE/WARP_MYWIRE_BACKEND/
    // WARP_MSSQLWIRE_BACKEND) read once at startup, so switching modes meant restarting Warp with
    // a different env var -- a real gap for a fleet migrating backend-by-backend, or wanting some
    // apps on native passthrough while others use the translated Postgres path, without running
    // two separate Warp deployments. See Main#acceptOraWireLoop/acceptMySqlWireLoop/
    // acceptMssqlWireLoop's own native-mode variants for how this port is actually used: a SECOND
    // accept loop, bound to this port, constructed from a copy of this same ServerOptions with
    // just the relevant mode flag flipped (see withOracleNativeListener/withMywireNativeListener/
    // withMssqlwireNativeListener below) -- the PRIMARY listener/port keeps running whatever mode
    // its own existing single toggle says, unchanged.
    private final int oracleNativeListenPort;
    private final int mywireNativeListenPort;
    private final int mssqlwireNativeListenPort;

    private ServerOptions(int listenPort, int pgWireListenPort, int myWireListenPort, int grpcPort, int httpPort, int httpsPort, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            String pgSslMode, String pgSslRootCert,
            String pgStandbyHost, int pgStandbyPort,
            boolean tlsEnabled, int tlsPort, int grpcTlsPort,
            String tlsKeystorePath, String tlsKeystorePassword,
            boolean dualExecEnabled, DualExecAuthority dualExecAuthority, boolean dualExecRequireBoth, boolean dualExecXaEnabled,
            boolean dualExecShadowEnabled,
            String oracleHost, int oraclePort, String oracleServiceName, OracleBackendMode oracleBackendMode,
            String oracleUser, String oraclePassword, McpBackendMode mcpBackendMode,
            boolean mywireNativeBackend, String mysqlHost, int mysqlPort, String mysqlDatabase, String mysqlUser, String mysqlPassword,
            int mssqlWireListenPort,
            boolean mssqlwireNativeBackend, String mssqlHost, int mssqlPort, String mssqlDatabase, String mssqlUser, String mssqlPassword,
            int oracleNativeListenPort, int mywireNativeListenPort, int mssqlwireNativeListenPort) {
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
        this.pgSslMode = pgSslMode;
        this.pgSslRootCert = pgSslRootCert;
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
        this.oracleUser = oracleUser;
        this.oraclePassword = oraclePassword;
        this.mcpBackendMode = mcpBackendMode;
        this.mywireNativeBackend = mywireNativeBackend;
        this.mysqlHost = mysqlHost;
        this.mysqlPort = mysqlPort;
        this.mysqlDatabase = mysqlDatabase;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;
        this.mssqlWireListenPort = mssqlWireListenPort;
        this.mssqlwireNativeBackend = mssqlwireNativeBackend;
        this.mssqlHost = mssqlHost;
        this.mssqlPort = mssqlPort;
        this.mssqlDatabase = mssqlDatabase;
        this.mssqlUser = mssqlUser;
        this.mssqlPassword = mssqlPassword;
        this.oracleNativeListenPort = oracleNativeListenPort;
        this.mywireNativeListenPort = mywireNativeListenPort;
        this.mssqlwireNativeListenPort = mssqlwireNativeListenPort;
    }

    public static ServerOptions parse(String[] args) {
        
        String keystorePath = System.getenv("WARP_TLS_KEYSTORE");
        boolean tlsEnabled = keystorePath != null && !keystorePath.isBlank();
        int tlsPort = parseIntEnv("WARP_TLS_PORT", 2484);
        int grpcTlsPort = parseIntEnv("WARP_GRPC_TLS_PORT", 17071);
        String keystorePassword = System.getenv("WARP_TLS_KEYSTORE_PASSWORD");

        boolean dualExecEnabled = parseBoolEnv("WARP_DUAL_EXEC_ENABLED", false);
        DualExecAuthority dualExecAuthority = "oracle".equalsIgnoreCase(
                System.getenv().getOrDefault("WARP_DUAL_EXEC_AUTHORITY", "postgres"))
                ? DualExecAuthority.ORACLE : DualExecAuthority.POSTGRES;
        boolean dualExecRequireBoth = parseBoolEnv("WARP_DUAL_EXEC_REQUIRE_BOTH", false);
        boolean dualExecXaEnabled = parseBoolEnv("WARP_DUAL_EXEC_XA_ENABLED", false);
        
        boolean dualExecShadowEnabled = parseBoolEnv("WARP_DUAL_EXEC_SHADOW_ENABLED", true);
        String oracleHost = System.getenv().getOrDefault("WARP_ORACLE_HOST", "localhost");
        int oraclePort = parseIntEnv("WARP_ORACLE_PORT", 1521);
        String oracleServiceName = System.getenv().getOrDefault("WARP_ORACLE_SERVICE", "orcl");
        OracleBackendMode oracleBackendMode = "native".equalsIgnoreCase(
                System.getenv().getOrDefault("WARP_ORACLE_BACKEND_MODE", "jdbc"))
                ? OracleBackendMode.NATIVE : OracleBackendMode.JDBC;
        // Gateway-held Oracle credentials, needed only by MCP's own native-backend mode below --
        // orawire's own native mode (WARP_ORACLE_BACKEND_MODE=native, just above) sources Oracle
        // credentials from the client's own O5LOGON login instead, since it's a real orawire
        // session; MCP has no equivalent per-caller login step to source them from.
        String oracleUser = System.getenv("WARP_ORACLE_USER");
        String oraclePassword = System.getenv("WARP_ORACLE_PASSWORD");

        // Same "keep the database you have" pattern as orawire/mywire/mssqlwire's own native-mode
        // toggles, applied to the MCP frontend: WARP_MCP_BACKEND=oracle/mysql/sqlserver (default
        // "postgres") switches execute_sql/list_tables/describe_table/registered-function-tools to
        // bypass the shared pipeline and proxy straight to that real backend instead of Postgres --
        // see WarpMcpServer's own javadoc for exactly which tools work in each mode.
        McpBackendMode mcpBackendMode = switch (System.getenv().getOrDefault("WARP_MCP_BACKEND", "postgres").toLowerCase(java.util.Locale.ROOT)) {
            case "oracle" -> McpBackendMode.ORACLE;
            case "mysql" -> McpBackendMode.MYSQL;
            case "sqlserver" -> McpBackendMode.SQLSERVER;
            default -> McpBackendMode.POSTGRES;
        };

        boolean mywireNativeBackend = "mysql".equalsIgnoreCase(
                System.getenv().getOrDefault("WARP_MYWIRE_BACKEND", "postgres"));
        String mysqlHost = System.getenv().getOrDefault("WARP_MYSQL_HOST", "localhost");
        int mysqlPort = parseIntEnv("WARP_MYSQL_PORT", 3306);
        String mysqlDatabase = System.getenv().getOrDefault("WARP_MYSQL_DATABASE", "mysql");
        String mysqlUser = System.getenv("WARP_MYSQL_USER");
        String mysqlPassword = System.getenv("WARP_MYSQL_PASSWORD");

        // Same shape as mywireNativeBackend just above: WARP_MSSQLWIRE_BACKEND=sqlserver (default
        // "postgres") toggles mssqlwire between dialect-translating into Postgres (the only mode
        // that existed before this) and proxying straight through to a real SQL Server backend --
        // the "keep the database you have" path this product's own positioning already claims for
        // Oracle/MySQL but, until this, never actually implemented for SQL Server.
        boolean mssqlwireNativeBackend = "sqlserver".equalsIgnoreCase(
                System.getenv().getOrDefault("WARP_MSSQLWIRE_BACKEND", "postgres"));
        String mssqlHost = System.getenv().getOrDefault("WARP_MSSQL_HOST", "localhost");
        int mssqlPort = parseIntEnv("WARP_MSSQL_PORT", 1433);
        String mssqlDatabase = System.getenv().getOrDefault("WARP_MSSQL_DATABASE", "master");
        String mssqlUser = System.getenv("WARP_MSSQL_USER");
        String mssqlPassword = System.getenv("WARP_MSSQL_PASSWORD");

        int pgWireListenPort = parseIntEnv("WARP_PGWIRE_PORT", 15432);
        int myWireListenPort = parseIntEnv("WARP_MYWIRE_PORT", 13306);
        int orawireListenPort = parseIntEnv("WARP_ORAWIRE_PORT", 11521);
        
        int mssqlWireListenPort = parseIntEnv("WARP_MSSQLWIRE_PORT", 14333);
        // 0 = disabled (the default, single-listener/single-mode behavior unchanged) -- see the
        // three oracleNativeListenPort/mywireNativeListenPort/mssqlwireNativeListenPort fields'
        // own javadoc above for what setting one of these actually does.
        int oracleNativeListenPort = parseIntEnv("WARP_ORAWIRE_NATIVE_PORT", 0);
        int mywireNativeListenPort = parseIntEnv("WARP_MYWIRE_NATIVE_PORT", 0);
        int mssqlwireNativeListenPort = parseIntEnv("WARP_MSSQLWIRE_NATIVE_PORT", 0);
        int grpcPort = parseIntEnv("WARP_GRPC_PORT", 7070);
        int httpPort = parseIntEnv("WARP_HTTP_PORT", 8080);
        
        int httpsPort = parseIntEnv("WARP_HTTPS_PORT", 8443);

        String pgHost = System.getenv().getOrDefault("WARP_HOST", "localhost");
        int pgPort = parseIntEnv("WARP_PORT", 5432);
        String pgDatabase = System.getenv().getOrDefault("WARP_DATABASE", "postgres");
        String pgUser = System.getenv("WARP_USER");
        String pgPassword = System.getenv("WARP_PASSWORD");
        // libpq-style sslmode values (disable/allow/prefer/require/verify-ca/verify-full), passed
        // straight through to pgjdbc's own "sslmode" connection property -- unset means pgjdbc's
        // own default ("prefer"), same as before this existed. Needed for any backend that
        // requires SSL outright (Supabase, Azure Database for PostgreSQL both reject a plaintext
        // connection) -- previously the only way to reach one of those was the multi-backend
        // WARP_BACKENDS var, which accepts a full literal JDBC URL string (so sslmode could be
        // smuggled into the URL's own query string) but isn't the "simple" single-backend path
        // every other quickstart uses.
        String pgSslMode = System.getenv("WARP_PG_SSLMODE");
        // A PEM CA bundle path readable from inside the container -- e.g. RDS's/Azure's own
        // downloadable root cert, mounted in via a docker-compose volume. Optional even with
        // sslmode=verify-full set; pgjdbc falls back to the JVM's own trust store if omitted,
        // which already trusts most major clouds' default certs.
        String pgSslRootCert = System.getenv("WARP_PG_SSLROOTCERT");

        String pgStandbyHost = System.getenv("WARP_STANDBY_HOST");
        int pgStandbyPort = parseIntEnv("WARP_STANDBY_PORT", pgPort);

        return new ServerOptions(orawireListenPort, pgWireListenPort, myWireListenPort, grpcPort, httpPort, httpsPort, pgHost, pgPort, pgDatabase, pgUser, pgPassword,
                pgSslMode, pgSslRootCert,
                pgStandbyHost, pgStandbyPort,
                tlsEnabled, tlsPort, grpcTlsPort, keystorePath, keystorePassword,
                dualExecEnabled, dualExecAuthority, dualExecRequireBoth, dualExecXaEnabled,
                dualExecShadowEnabled,
                oracleHost, oraclePort, oracleServiceName, oracleBackendMode,
                oracleUser, oraclePassword, mcpBackendMode,
                mywireNativeBackend, mysqlHost, mysqlPort, mysqlDatabase, mysqlUser, mysqlPassword,
                mssqlWireListenPort,
                mssqlwireNativeBackend, mssqlHost, mssqlPort, mssqlDatabase, mssqlUser, mssqlPassword,
                oracleNativeListenPort, mywireNativeListenPort, mssqlwireNativeListenPort);
    }

    public int mssqlWireListenPort() {
        return mssqlWireListenPort;
    }

    /** Builds a minimal {@code ServerOptions} pointed at a specific control-plane Postgres,
     * without touching real process environment variables -- for in-process tests of a
     * ServerOptions-consuming class (e.g. {@code XaRecoveryLog}, {@code PgConnections}) that need
     * a real {@code ServerOptions} but don't start any listener and shouldn't risk colliding with
     * whatever WARP_* vars happen to be set in the actual test JVM's environment. Every
     * non-Postgres field gets an inert placeholder; callers that also need those should use a full
     * {@code WarpProcess} subprocess instead, matching this project's existing pattern for
     * anything that exercises a real listener. */
    public static ServerOptions forTesting(String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword) {
        return new ServerOptions(0, 0, 0, 0, 0, 0, pgHost, pgPort, pgDatabase, pgUser, pgPassword,
                null, null,
                null, pgPort,
                false, 0, 0, null, null,
                false, DualExecAuthority.POSTGRES, false, false,
                false,
                "localhost", 1521, "orcl", OracleBackendMode.JDBC,
                null, null, McpBackendMode.POSTGRES,
                false, "localhost", 3306, "mysql", null, null,
                0,
                false, "localhost", 1433, "master", null, null,
                0, 0, 0);
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

    public String pgSslMode() {
        return pgSslMode;
    }

    public String pgSslRootCert() {
        return pgSslRootCert;
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

    public String oracleUser() {
        return oracleUser;
    }

    public String oraclePassword() {
        return oraclePassword;
    }

    public McpBackendMode mcpBackendMode() {
        return mcpBackendMode;
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

    public boolean mssqlwireNativeBackend() {
        return mssqlwireNativeBackend;
    }

    public String mssqlHost() {
        return mssqlHost;
    }

    public int mssqlPort() {
        return mssqlPort;
    }

    public String mssqlDatabase() {
        return mssqlDatabase;
    }

    public String mssqlUser() {
        return mssqlUser;
    }

    public String mssqlPassword() {
        return mssqlPassword;
    }

    /** True only for the copy {@link #withMywireNativeListener()} returns (derived, not a stored
     * field: {@code myWireListenPort} equals {@code mywireNativeListenPort} on that copy and on
     * no other ServerOptions instance a real deployment would construct) -- lets
     * MySqlWireSessionHandler tell "the single-toggle native mode" (existing, unpinned/
     * router-rule-overridable routing, left exactly as it was) apart from "dual-port mode's own
     * native listener" (pins its statements to BackendRegistry#MYSQL_NATIVE_DUAL_PORT_NAME
     * explicitly instead -- see that constant's own javadoc for why it can't safely rely on the
     * same ambiguous same-dialect fallback the single-toggle mode uses). */
    public boolean mywireNativeViaDualPort() {
        return mywireNativeBackend && mywireNativeListenPort != 0 && myWireListenPort == mywireNativeListenPort;
    }

    /** See {@link #mywireNativeViaDualPort()} -- same derivation, for mssqlwire's dual-port
     * listener and {@code BackendRegistry#MSSQL_NATIVE_DUAL_PORT_NAME}. */
    public boolean mssqlwireNativeViaDualPort() {
        return mssqlwireNativeBackend && mssqlwireNativeListenPort != 0 && mssqlWireListenPort == mssqlwireNativeListenPort;
    }

    public int oracleNativeListenPort() {
        return oracleNativeListenPort;
    }

    public int mywireNativeListenPort() {
        return mywireNativeListenPort;
    }

    public int mssqlwireNativeListenPort() {
        return mssqlwireNativeListenPort;
    }

    /** A copy of this same config for orawire's SECOND (native-mode) listener -- {@code
     * listenPort()} becomes {@code oracleNativeListenPort} (safe: nothing but {@code
     * Main#acceptOraWireLoop}'s own bind+log reads {@code listenPort()}; {@code SessionHandler}
     * itself never does) and {@code oracleBackendMode()} becomes {@code NATIVE} regardless of the
     * PRIMARY listener's own mode -- the two listeners are fully independent once this exists. */
    public ServerOptions withOracleNativeListener() {
        return new ServerOptions(oracleNativeListenPort, pgWireListenPort, myWireListenPort, grpcPort, httpPort, httpsPort, pgHost, pgPort, pgDatabase, pgUser, pgPassword,
                pgSslMode, pgSslRootCert,
                pgStandbyHost, pgStandbyPort,
                tlsEnabled, tlsPort, grpcTlsPort, tlsKeystorePath, tlsKeystorePassword,
                dualExecEnabled, dualExecAuthority, dualExecRequireBoth, dualExecXaEnabled,
                dualExecShadowEnabled,
                oracleHost, oraclePort, oracleServiceName, OracleBackendMode.NATIVE,
                oracleUser, oraclePassword, mcpBackendMode,
                mywireNativeBackend, mysqlHost, mysqlPort, mysqlDatabase, mysqlUser, mysqlPassword,
                mssqlWireListenPort,
                mssqlwireNativeBackend, mssqlHost, mssqlPort, mssqlDatabase, mssqlUser, mssqlPassword,
                oracleNativeListenPort, mywireNativeListenPort, mssqlwireNativeListenPort);
    }

    /** A copy of this same config for mywire's SECOND (native-mode) listener -- {@code
     * myWireListenPort()} becomes {@code mywireNativeListenPort} (safe: nothing but {@code
     * Main#acceptMySqlWireLoop}'s own bind+log reads {@code myWireListenPort()}) and {@code
     * mywireNativeBackend()} becomes {@code true} regardless of the PRIMARY listener's own mode. */
    public ServerOptions withMywireNativeListener() {
        return new ServerOptions(listenPort, pgWireListenPort, mywireNativeListenPort, grpcPort, httpPort, httpsPort, pgHost, pgPort, pgDatabase, pgUser, pgPassword,
                pgSslMode, pgSslRootCert,
                pgStandbyHost, pgStandbyPort,
                tlsEnabled, tlsPort, grpcTlsPort, tlsKeystorePath, tlsKeystorePassword,
                dualExecEnabled, dualExecAuthority, dualExecRequireBoth, dualExecXaEnabled,
                dualExecShadowEnabled,
                oracleHost, oraclePort, oracleServiceName, oracleBackendMode,
                oracleUser, oraclePassword, mcpBackendMode,
                true, mysqlHost, mysqlPort, mysqlDatabase, mysqlUser, mysqlPassword,
                mssqlWireListenPort,
                mssqlwireNativeBackend, mssqlHost, mssqlPort, mssqlDatabase, mssqlUser, mssqlPassword,
                oracleNativeListenPort, mywireNativeListenPort, mssqlwireNativeListenPort);
    }

    /** A copy of this same config for mssqlwire's SECOND (native-mode) listener -- {@code
     * mssqlWireListenPort()} becomes {@code mssqlwireNativeListenPort} (safe: nothing but {@code
     * Main#acceptMssqlWireLoop}'s own bind+log reads {@code mssqlWireListenPort()}) and {@code
     * mssqlwireNativeBackend()} becomes {@code true} regardless of the PRIMARY listener's own
     * mode. */
    public ServerOptions withMssqlwireNativeListener() {
        return new ServerOptions(listenPort, pgWireListenPort, myWireListenPort, grpcPort, httpPort, httpsPort, pgHost, pgPort, pgDatabase, pgUser, pgPassword,
                pgSslMode, pgSslRootCert,
                pgStandbyHost, pgStandbyPort,
                tlsEnabled, tlsPort, grpcTlsPort, tlsKeystorePath, tlsKeystorePassword,
                dualExecEnabled, dualExecAuthority, dualExecRequireBoth, dualExecXaEnabled,
                dualExecShadowEnabled,
                oracleHost, oraclePort, oracleServiceName, oracleBackendMode,
                oracleUser, oraclePassword, mcpBackendMode,
                mywireNativeBackend, mysqlHost, mysqlPort, mysqlDatabase, mysqlUser, mysqlPassword,
                mssqlwireNativeListenPort,
                true, mssqlHost, mssqlPort, mssqlDatabase, mssqlUser, mssqlPassword,
                oracleNativeListenPort, mywireNativeListenPort, mssqlwireNativeListenPort);
    }
}
