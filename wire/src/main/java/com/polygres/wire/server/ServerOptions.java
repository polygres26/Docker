package com.polygres.wire.server;

public final class ServerOptions {

    public enum DualExecAuthority {
        POSTGRES, ORACLE
    }

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
        
        boolean dualExecShadowEnabled = parseBoolEnv("POLYWIRE_DUAL_EXEC_SHADOW_ENABLED", true);
        String oracleHost = System.getenv().getOrDefault("POLYWIRE_ORACLE_HOST", "localhost");
        int oraclePort = parseIntEnv("POLYWIRE_ORACLE_PORT", 1521);
        String oracleServiceName = System.getenv().getOrDefault("POLYWIRE_ORACLE_SERVICE", "orcl");
        OracleBackendMode oracleBackendMode = "native".equalsIgnoreCase(
                System.getenv().getOrDefault("POLYWIRE_ORACLE_BACKEND_MODE", "jdbc"))
                ? OracleBackendMode.NATIVE : OracleBackendMode.JDBC;

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
        
        int mssqlWireListenPort = parseIntEnv("POLYWIRE_MSSQLWIRE_PORT", 14333);
        int grpcPort = parseIntEnv("POLYWIRE_GRPC_PORT", 7070);
        int httpPort = parseIntEnv("POLYWIRE_HTTP_PORT", 8080);
        
        int httpsPort = parseIntEnv("POLYWIRE_HTTPS_PORT", 8443);

        String pgHost = System.getenv().getOrDefault("POLYWIRE_PG_HOST", "localhost");
        int pgPort = parseIntEnv("POLYWIRE_PG_PORT", 5432);
        String pgDatabase = System.getenv().getOrDefault("POLYWIRE_PG_DATABASE", "postgres");
        String pgUser = System.getenv("POLYWIRE_PG_USER");
        String pgPassword = System.getenv("POLYWIRE_PG_PASSWORD");
        
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
