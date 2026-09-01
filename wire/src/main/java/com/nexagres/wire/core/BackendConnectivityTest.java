package com.nexagres.wire.core;

import com.nexagres.wire.secrets.SecretResolver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * "Does this actually connect" -- a one-shot, non-pooled probe used before a backend is ever
 * added to {@code warp_config}, and to re-check ones already configured. Exists because
 * finding out a backend's host/port/credential is wrong only when a real client statement fails
 * to route is a bad way to find out -- see the back-and-forth it took to onboard a Supabase
 * project by hand (wrong host resolved to nothing, direct connection was IPv6-only and
 * unreachable, pooler needed a different username shape) before this existed.
 *
 * <p>Deliberately bypasses {@link BackendConnectionPools} -- a test connection has no business
 * warming (or poisoning) the real pool a backend will actually serve traffic from. {@code
 * password} may be a {@code vault:...}/{@code cyberark:...} reference, resolved the same way
 * {@link BackendTarget} resolves it.
 */
public final class BackendConnectivityTest {

    private static final int LOGIN_TIMEOUT_SECONDS = 8;

    public record Result(boolean ok, String message, long tookMs, String serverVersion) {
    }

    private BackendConnectivityTest() {
    }

    public static Result test(String jdbcUrl, String user, String password) {
        long start = System.nanoTime();
        String resolvedPassword;
        try {
            resolvedPassword = SecretResolver.resolve(password);
        } catch (RuntimeException e) {
            return new Result(false, "secret resolution failed: " + e.getMessage(),
                    (System.nanoTime() - start) / 1_000_000, null);
        }
        // Per-connection timeout via driver properties, not the process-global
        // DriverManager.setLoginTimeout -- that static setting would race with any other
        // connection attempt (a real BackendTarget borrow, another concurrent test) happening at
        // the same time. "connectTimeout"/"loginTimeout" (seconds) are both understood by pgjdbc;
        // setting both covers other PostgreSQL-wire-compatible drivers that only recognize one.
        Properties props = new Properties();
        props.setProperty("user", user == null ? "" : user);
        props.setProperty("password", resolvedPassword == null ? "" : resolvedPassword);
        props.setProperty("connectTimeout", String.valueOf(LOGIN_TIMEOUT_SECONDS));
        props.setProperty("loginTimeout", String.valueOf(LOGIN_TIMEOUT_SECONDS));

        // Real bug, found live against a real Oracle backend: "SELECT version()" is a Postgres
        // (and, coincidentally, real MySQL) function -- Oracle and SQL Server have no such call,
        // and reject it outright (Oracle: ORA-00904 "VERSION": invalid identifier), which used to
        // mark a perfectly healthy non-Postgres/non-MySQL backend DOWN. A dialect-aware version
        // query, matching BackendTarget.dialect()'s own URL-prefix dispatch.
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(java.util.Locale.ROOT);
        String versionSql;
        if (url.startsWith("jdbc:oracle:")) {
            versionSql = "SELECT banner FROM v$version WHERE ROWNUM = 1";
        } else if (url.startsWith("jdbc:sqlserver:")) {
            versionSql = "SELECT @@VERSION";
        } else {
            versionSql = "SELECT version()";
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(versionSql)) {
            String version = rs.next() ? rs.getString(1) : null;
            long tookMs = (System.nanoTime() - start) / 1_000_000;
            return new Result(true, "Connected", tookMs, version);
        } catch (SQLException e) {
            long tookMs = (System.nanoTime() - start) / 1_000_000;
            return new Result(false, e.getMessage(), tookMs, null);
        }
    }
}
