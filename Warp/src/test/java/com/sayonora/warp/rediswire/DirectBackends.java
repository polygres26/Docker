package com.sayonora.warp.rediswire;

import com.sayonora.warp.core.DdlTemplates;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** {@link Backends} straight over JDBC URLs (no BackendRegistry): dev harness and integration tests. */
public final class DirectBackends implements Backends {

    private final Map<String, String> urls = new LinkedHashMap<>();
    private final Map<String, HikariDataSource> pools = new LinkedHashMap<>();
    private final String user;
    private final String password;
    private final Map<String, List<String>> routes = new java.util.concurrent.ConcurrentHashMap<>();

    public DirectBackends(String user, String password, String... urls) {
        this.user = user;
        this.password = password;
        for (int i = 0; i < urls.length; i++) {
            this.urls.put("h" + i, urls[i]);
        }
    }

    /** Explicit route for a username (tests). */
    public void route(String username, List<String> hosts) {
        routes.put(username, hosts);
    }

    @Override
    public List<String> defaultHosts() {
        return List.copyOf(urls.keySet());
    }

    @Override
    public List<String> routeHosts(String username, int db) {
        List<String> r = routes.get("db" + db);
        return r != null ? r : routes.get(username);
    }

    private synchronized HikariDataSource pool(String host) {
        return pools.computeIfAbsent(host, h -> {
            HikariConfig c = new HikariConfig();
            c.setJdbcUrl(urls.get(h));
            c.setUsername(user);
            c.setPassword(password);
            c.setMaximumPoolSize(Integer.parseInt(System.getenv().getOrDefault("WARP_POOL_MAX_SIZE", "30")));
            c.addDataSourceProperty("prepareThreshold", "1");
            c.addDataSourceProperty("preparedStatementCacheQueries", "250");
            return new HikariDataSource(c);
        });
    }

    @Override
    public Connection open(String host) throws SQLException {
        return pool(host).getConnection();
    }

    @Override
    public Connection openDedicated(String host) throws SQLException {
        return DriverManager.getConnection(urls.get(host), user, password);
    }

    @Override
    public void ensureSchema(String host) throws SQLException {
        try (Connection c = open(host); Statement st = c.createStatement()) {
            for (String s : DdlTemplates.loadStatements("postgres", "rediswire_store", Map.of())) {
                st.execute(s);
            }
        }
    }

    public void close() {
        pools.values().forEach(HikariDataSource::close);
    }
}
