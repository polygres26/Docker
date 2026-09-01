package com.nexagres.wire.jdbc;

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

public final class WarpDriver implements Driver {

    private static final Pattern URL_PATTERN = Pattern.compile("jdbc:warp://([^:/]+):(\\d+)/([^?]*)(?:\\?(.*))?");

    static {
        try {
            DriverManager.registerDriver(new WarpDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Matcher m = URL_PATTERN.matcher(url);
        if (!m.matches()) {
            return null;
        }
        String host = m.group(1);
        int port = Integer.parseInt(m.group(2));
        Properties params = mergeQueryParams(info, m.group(4));
        String username = params.getProperty("user", "");
        String password = params.getProperty("password", "");

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        return WarpConnection.create(channel, username, password);
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
        return url != null && url.startsWith("jdbc:warp://");
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
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
