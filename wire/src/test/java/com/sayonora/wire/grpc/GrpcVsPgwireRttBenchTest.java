package com.sayonora.wire.grpc;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sayonora.wire.grpc.proto.ExecuteRequest;
import com.sayonora.wire.grpc.proto.ExecuteResponse;
import com.sayonora.wire.grpc.proto.QueryServiceGrpc;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** Java-client-vs-Java-client RTT benchmark: gRPC generated stub against QueryService vs pgjdbc
 * against pgwire, same statements, same real subprocess Warp, same real Postgres. Opt-in only:
 * {@code -Drttbench=true}. Run it via its {@code main} (or surefire; the grpc-xds skew that used to
 * break channel builds is fixed):
 * {@code java <pom argLine flags> -Donly=both|grpc|pg -cp target/test-classes:target/classes:<deps> com.sayonora.wire.grpc.GrpcVsPgwireRttBenchTest}. Measures CLIENT-observed
 * latency for both protocols (gRPC-java stub vs pgjdbc), interleaved and alternating order. */
class GrpcVsPgwireRttBenchTest {

    private static final int WARMUP = 3000;
    private static final int SAMPLES = 2000;

    public static void main(String[] args) throws Exception {
        System.setProperty("rttbench", "true");
        new GrpcVsPgwireRttBenchTest().bench();
        System.exit(0);
    }

    @Test
    void bench() throws Exception {
        assumeTrue(Boolean.getBoolean("rttbench"));
        try (RealPostgres pg = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(pg.host(), pg.port(), pg.database(), pg.username(), pg.password())
                        .env("WARP_QOS_RATE_PER_SEC", "10000000").env("WARP_QOS_BURST", "10000000")
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .frontend("grpc", "WARP_GRPC_PORT")
                        .env("WARP_GRPC_DIRECT_EXECUTOR", System.getProperty("server.direct", "false"))
                        .start()) {
            try (Connection c = DriverManager.getConnection(pg.jdbcUrl(), "postgres", "postgres");
                    java.sql.Statement st = c.createStatement()) {
                st.execute("CREATE TABLE bench (id bigint primary key, v text)");
                for (int i = 0; i < 100; i++) {
                    st.execute("INSERT INTO bench VALUES (" + (1_000_000 + i) + ", 'seed')");
                }
            }
            NettyChannelBuilder cb = NettyChannelBuilder.forAddress("localhost", warp.port("grpc")).usePlaintext();
            // Client-side experiment knobs (all off by default = what WarpDriver ships).
            if (Boolean.getBoolean("client.direct")) cb.directExecutor();
            if (Boolean.getBoolean("client.noretry")) cb.disableRetry();
            if (Boolean.getBoolean("client.nochannelz")) cb.disableServiceConfigLookUp();
            if (System.getProperty("client.window") != null) cb.flowControlWindow(Integer.getInteger("client.window"));
            ManagedChannel ch = cb.build();
            QueryServiceGrpc.QueryServiceBlockingStub stub = QueryServiceGrpc.newBlockingStub(ch);
            String base = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
            try (Connection c = DriverManager.getConnection(base, "postgres", "postgres")) {
                c.setAutoCommit(true);
                PreparedStatement ins = c.prepareStatement("INSERT INTO bench VALUES (?, ?)");
                PreparedStatement sel = c.prepareStatement("SELECT v FROM bench WHERE id = ?");
                long[][] res = new long[4][SAMPLES]; // 0 g-ins 1 p-ins 2 g-sel 3 p-sel
                int gid = 0;
                int pid = 50_000_000;
                for (int op = 0; op < 2; op++) {
                    for (int i = 0; i < WARMUP + SAMPLES; i++) {
                        long g;
                        long p;
                        boolean grpcFirst = (i & 1) == 0; // alternate order so neither always goes second
                        long[] t = new long[2];
                        for (int leg = 0; leg < 2; leg++) {
                            boolean isG = (leg == 0) == grpcFirst;
                            String only = System.getProperty("only", "both");
                            if (only.equals("grpc") && !isG || only.equals("pg") && isG) continue;
                            long t0 = System.nanoTime();
                            if (isG) {
                                ExecuteRequest.Builder rb = ExecuteRequest.newBuilder().setUsername("postgres").setPassword("postgres");
                                if (op == 0) rb.setSql("INSERT INTO bench VALUES (?, ?)").addParams(String.valueOf(++gid)).addParams("x");
                                else rb.setSql("SELECT v FROM bench WHERE id = ?").addParams(String.valueOf(1_000_000 + (i % 100)));
                                ExecuteResponse r = stub.execute(rb.build());
                                if (!r.getSuccess()) throw new IllegalStateException(r.getErrorMessage());
                            } else if (op == 0) {
                                ins.setLong(1, ++pid);
                                ins.setString(2, "x");
                                ins.executeUpdate();
                            } else {
                                sel.setLong(1, 1_000_000 + (i % 100));
                                try (ResultSet rs = sel.executeQuery()) {
                                    if (!rs.next()) throw new IllegalStateException("no row");
                                    rs.getString(1);
                                }
                            }
                            t[isG ? 0 : 1] = System.nanoTime() - t0;
                        }
                        if (i >= WARMUP) {
                            res[op * 2][i - WARMUP] = t[0];
                            res[op * 2 + 1][i - WARMUP] = t[1];
                        }
                    }
                }
                report("CLIENT grpc   INSERT", res[0]);
                report("CLIENT pgjdbc INSERT", res[1]);
                report("CLIENT grpc   SELECT", res[2]);
                report("CLIENT pgjdbc SELECT", res[3]);
            }
            ch.shutdownNow();
            Thread.sleep(500);
        }
    }

    private static void report(String label, long[] ns) {
        long[] a = ns.clone();
        Arrays.sort(a);
        System.out.println("[BENCH] " + label + " n=" + a.length + " min=" + a[0] / 1000 + "us p50=" + a[a.length / 2] / 1000
                + "us p90=" + a[a.length * 9 / 10] / 1000 + "us p99=" + a[a.length * 99 / 100] / 1000 + "us");
    }
}
