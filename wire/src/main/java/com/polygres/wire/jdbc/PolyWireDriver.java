package com.polygres.wire.jdbc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The native PolyWire JDBC driver (Type 4 — a direct socket protocol, gRPC
 * here, not a bridge to another driver; "Type 3" language in
 * ARCHITECTURE.md §5.9 followed OJP's own terminology loosely). URL form,
 * OJP-style: {@code jdbc:polywire://host:port/database?user=X&password=Y}.
 * Talks gRPC to {@code QueryServiceImpl} instead of emulating a foreign
 * wire protocol — see {@link PolyWireConnection}/{@link PolyWireStatement}/
 * {@link PolyWireResultSet}, each a narrow-slice {@link java.lang.reflect.Proxy}
 * implementation covering only the JDBC methods this driver actually needs.
 */
public final class PolyWireDriver implements Driver {

    private static final Pattern URL_PATTERN = Pattern.compile("jdbc:polywire://([^:/]+):(\\d+)/([^?]*)(?:\\?(.*))?");

    static {
        try {
            DriverManager.registerDriver(new PolyWireDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Matcher m = URL_PATTERN.matcher(url);
        if (!m.matches()) {
            return null; // not our URL scheme — per Driver contract, let DriverManager try the next driver
        }
        String host = m.group(1);
        int port = Integer.parseInt(m.group(2));
        Properties params = mergeQueryParams(info, m.group(4));
        String username = params.getProperty("user", "");
        String password = params.getProperty("password", "");

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        return PolyWireConnection.create(channel, username, password);
    }

    private static Properties mergeQueryParams(Properties info, String query) {
        Properties merged = new Properties();
        merged.putAll(info);
        if (query != null) {
            for (String pair : query.split("&")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    merged.setProperty(pair.substring(0, eq), pair.substring(eq + 1));
                }
            }
        }
        return merged;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:polywire://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false; // narrow slice — see class javadoc
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
