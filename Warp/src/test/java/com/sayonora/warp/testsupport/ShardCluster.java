package com.sayonora.warp.testsupport;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * Three empty shard databases of one engine for the sharding tests, with an {@code orders (id, customer_id, amount)} table created on request:
 * local Postgres or MySQL servers started from the bin directories in WARP_TEST_BROWNOUT_PG_BIN / WARP_TEST_MYSQL_BIN, or the SQL Server and Oracle
 * containers of the setup notes (WARP_TEST_SHARD_ENGINE=mssql|oracle).
 */
public final class ShardCluster implements AutoCloseable {

    public interface Shard {
        Connection conn() throws Exception;

        /** {@code name=url|user|password} for WARP_BACKENDS. */
        String backendSpec(String name);
    }

    private final String engine;
    private final Shard[] shards = new Shard[3];
    private final AutoCloseable[] servers = new AutoCloseable[3];

    private ShardCluster(String engine) {
        this.engine = engine;
    }

    /** True when the environment can run {@code engine}'s shards. */
    public static boolean available(String engine) {
        return switch (engine) {
            case "postgres" -> notBlank(System.getenv("WARP_TEST_BROWNOUT_PG_BIN"));
            case "mysql" -> notBlank(System.getenv("WARP_TEST_BROWNOUT_PG_BIN")) && notBlank(System.getenv("WARP_TEST_MYSQL_BIN"));
            default -> notBlank(System.getenv("WARP_TEST_BROWNOUT_PG_BIN")) && engine.equals(System.getenv("WARP_TEST_SHARD_ENGINE"));
        };
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }

    public static ShardCluster start(String engine, Path dir) throws Exception {
        ShardCluster c = new ShardCluster(engine);
        for (int i = 0; i < 3; i++) {
            final int n = i;
            switch (engine) {
                case "postgres" -> {
                    LocalPostgres p = LocalPostgres.primary(System.getenv("WARP_TEST_BROWNOUT_PG_BIN"), dir, "s" + i, LocalPostgres.freePort());
                    c.servers[i] = () -> p.stop("immediate");
                    c.shards[i] = jdbc(p.url(), "warp", "secret");
                }
                case "mysql" -> {
                    LocalMySql m = LocalMySql.create(System.getenv("WARP_TEST_MYSQL_BIN"), dir, "s" + i, LocalPostgres.freePort(), 41 + i);
                    m.exec("create database shard");
                    c.servers[i] = m::stop;
                    c.shards[i] = jdbc(m.url("shard"), "root", "");
                }
                case "mssql" -> c.shards[i] = jdbc("jdbc:sqlserver://127.0.0.1:14341;databaseName=shard" + (n + 1) + ";encrypt=true;trustServerCertificate=true",
                        "sa", "Warp_Test_1234!");
                case "oracle" -> c.shards[i] = jdbc("jdbc:oracle:thin:@//127.0.0.1:15211/FREEPDB1", "shard" + (n + 1), "shardpw" + (n + 1));
                default -> throw new IllegalArgumentException(engine);
            }
            if (engine.equals("mssql") || engine.equals("oracle")) { // shared containers: start from nothing
                try (Connection conn = c.shards[i].conn(); Statement st = conn.createStatement()) {
                    for (String drop : new String[] {"drop table orders", "drop table orders__rebal"}) {
                        try {
                            st.execute(drop);
                        } catch (SQLException absent) {
                            // first run
                        }
                    }
                }
            }
        }
        return c;
    }

    private static Shard jdbc(String url, String user, String password) {
        return new Shard() {
            public Connection conn() throws Exception {
                return DriverManager.getConnection(url, user, password);
            }

            public String backendSpec(String name) {
                return name + "=" + url.replace(";", "%3B") + "|" + user + "|" + password;
            }
        };
    }

    public String engine() {
        return engine;
    }

    public Shard shard(int i) {
        return shards[i];
    }

    public void createOrders(int i) throws Exception {
        try (Connection c = shards[i].conn(); Statement st = c.createStatement()) {
            st.execute("create table orders (id int primary key, customer_id int, amount int)");
        }
    }

    public long count(int i) throws Exception {
        try (Connection c = shards[i].conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from orders")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    public Set<Long> ids(int i) throws Exception {
        Set<Long> out = new HashSet<>();
        try (Connection c = shards[i].conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select id from orders")) {
            while (rs.next()) {
                out.add(rs.getLong(1));
            }
        }
        return out;
    }

    @Override
    public void close() {
        for (AutoCloseable s : servers) {
            if (s != null) {
                try {
                    s.close();
                } catch (Exception ignored) {
                    // stopping
                }
            }
        }
    }
}
