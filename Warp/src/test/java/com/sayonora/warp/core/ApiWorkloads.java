package com.sayonora.warp.core;

import com.google.protobuf.ByteString;
import com.sayonora.warp.bigtablewire.admin.v2.BigtableTableAdminGrpc;
import com.sayonora.warp.bigtablewire.v2.BigtableGrpc;
import com.sayonora.warp.grpc.proto.ExecuteRequest;
import com.sayonora.warp.grpc.proto.ExecuteResponse;
import com.sayonora.warp.grpc.proto.QueryServiceGrpc;
import com.sayonora.warp.testsupport.BrownoutHarness.Client;
import com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig;
import com.sayonora.warp.testsupport.BrownoutHarness.Workload;
import com.sayonora.warp.testsupport.WarpProcess;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Workloads for Warp's own APIs and the protocols that need a generated or custom client: the gRPC QueryService, the MCP JSON-RPC
 * endpoint (its {@code execute_sql} tool), the Gremlin HTTP endpoint and the Bigtable gRPC API. Each selects the Postgres backend
 * {@code pg} the way a client would (database name, backend argument, or the backend set that holds the store).
 */
final class ApiWorkloads {

    private ApiWorkloads() {
    }

    static List<Workload> all() {
        return List.of(grpcQuery(), mcp(), gremlin(), bigtable());
    }

    private static List<String> all(String text, String regex) {
        List<String> out = new java.util.ArrayList<>();
        Matcher m = Pattern.compile(regex).matcher(text);
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    private static String post(HttpClient c, String url, String json) throws Exception {
        HttpResponse<String> r = c.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10))
                .header("content-type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json)).build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() >= 300) {
            String t = r.body().replaceAll("\\s+", " ");
            throw new IOException("HTTP " + r.statusCode() + " " + (t.length() > 160 ? t.substring(0, 160) : t));
        }
        return r.body();
    }

    // ---- gRPC QueryService -------------------------------------------------------------------------------

    private static ManagedChannel channel(int port) {
        return ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    }

    private static ExecuteResponse exec(QueryServiceGrpc.QueryServiceBlockingStub stub, String sql) {
        ExecuteResponse r = stub.withDeadlineAfter(10, TimeUnit.SECONDS).execute(ExecuteRequest.newBuilder().setUsername("warp")
                .setPassword("secret").setSql(sql).setDatabase("pg").build());
        if (!r.getSuccess()) {
            throw new IllegalStateException(r.getSqlState() + " " + r.getErrorMessage());
        }
        return r;
    }

    static Workload grpcQuery() {
        return new Workload() {
            @Override
            public String name() {
                return "grpc-query";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("grpc", "WARP_GRPC_PORT");
            }

            @Override
            public void prepare(WarpProcess warp) {
                ManagedChannel ch = channel(warp.port("grpc"));
                try {
                    exec(QueryServiceGrpc.newBlockingStub(ch), "CREATE TABLE IF NOT EXISTS bo_grpc (id bigint PRIMARY KEY, n int)");
                } finally {
                    ch.shutdownNow();
                }
            }

            @Override
            public Client open(WarpProcess warp) {
                ManagedChannel ch = channel(warp.port("grpc"));
                var stub = QueryServiceGrpc.newBlockingStub(ch);
                return new Client() {
                    @Override
                    public void write(long id) {
                        exec(stub, "INSERT INTO bo_grpc VALUES (" + id + ", 1)");
                    }

                    @Override
                    public void read(long id) {
                        exec(stub, "SELECT n FROM bo_grpc WHERE id = " + id);
                    }

                    @Override
                    public void close() {
                        ch.shutdownNow();
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                ManagedChannel ch = channel(warp.port("grpc"));
                try {
                    exec(QueryServiceGrpc.newBlockingStub(ch), "SELECT id FROM bo_grpc").getRowsList()
                            .forEach(r -> ids.add(Long.parseLong(r.getValues(0))));
                } finally {
                    ch.shutdownNow();
                }
                return ids;
            }
        };
    }

    // ---- MCP (execute_sql tool over JSON-RPC) ------------------------------------------------------------

    private static String mcpSql(HttpClient c, int port, String sql) throws Exception {
        String req = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"execute_sql\",\"arguments\":"
                + "{\"sql\":\"" + sql.replace("\"", "\\\"") + "\",\"backend\":\"pg\"}}}";
        String body = post(c, "http://localhost:" + port + "/", req);
        if (body.contains("\"isError\":true") || body.contains("\"error\":{")) {
            String t = body.replaceAll("\\s+", " ");
            throw new IOException("MCP error " + (t.length() > 200 ? t.substring(0, 200) : t));
        }
        return body;
    }

    static Workload mcp() {
        return new Workload() {
            @Override
            public String name() {
                return "mcp";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("mcp", "WARP_MCP_PORT");
            }

            @Override
            public void prepare(WarpProcess warp) throws Exception {
                mcpSql(HttpClient.newHttpClient(), warp.port("mcp"), "CREATE TABLE IF NOT EXISTS bo_mcp (id bigint PRIMARY KEY, n int)");
            }

            @Override
            public Client open(WarpProcess warp) {
                HttpClient c = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
                int port = warp.port("mcp");
                return new Client() {
                    @Override
                    public void write(long id) throws Exception {
                        mcpSql(c, port, "INSERT INTO bo_mcp VALUES (" + id + ", 1)");
                    }

                    @Override
                    public void read(long id) throws Exception {
                        mcpSql(c, port, "SELECT n FROM bo_mcp WHERE id = " + id);
                    }

                    @Override
                    public void close() {
                        // nothing to close
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                HttpClient c = HttpClient.newHttpClient();
                Set<Long> ids = new HashSet<>();
                long last = 0;
                while (true) { // page by key: the tool may cap the rows it returns
                    String body = mcpSql(c, warp.port("mcp"), "SELECT id FROM bo_mcp WHERE id > " + last + " ORDER BY id LIMIT 200");
                    // the JSON-RPC envelope has its own "id":1, so only ids from the workload's range count
                    boolean progressed = false;
                    for (String g : all(body, "id\\\\*\"\\s*:\\s*(\\d{10})")) {
                        long v = Long.parseLong(g);
                        ids.add(v);
                        if (v > last) {
                            last = v;
                            progressed = true;
                        }
                    }
                    if (!progressed) {
                        break;
                    }
                }
                return ids;
            }
        };
    }

    // ---- Gremlin (HTTP endpoint) -------------------------------------------------------------------------

    private static String gremlin(HttpClient c, int port, String script) throws Exception {
        return post(c, "http://localhost:" + port + "/", "{\"gremlin\":\"" + script.replace("\"", "\\\"") + "\"}");
    }

    static Workload gremlin() {
        return new Workload() {
            @Override
            public String name() {
                return "gremlinwire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("gremlinwire", "WARP_GREMLINWIRE_PORT");
                stores.enable("gremlin");
            }

            @Override
            public Client open(WarpProcess warp) {
                HttpClient c = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
                int port = warp.port("gremlinwire");
                return new Client() {
                    @Override
                    public void write(long id) throws Exception {
                        gremlin(c, port, "g.addV('item').property('uid', " + id + ")");
                    }

                    @Override
                    public void read(long id) throws Exception {
                        gremlin(c, port, "g.V().has('uid', " + id + ").count()");
                    }

                    @Override
                    public void close() {
                        // nothing to close
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                HttpClient c = HttpClient.newHttpClient();
                Set<Long> ids = new HashSet<>();
                String body = gremlin(c, warp.port("gremlinwire"), "g.V().hasLabel('item').values('uid')");
                int data = body.indexOf("\"data\"");
                all(data < 0 ? body : body.substring(data), "(\\d{10})").forEach(n -> ids.add(Long.parseLong(n)));
                return ids;
            }
        };
    }

    // ---- Bigtable (gRPC) ---------------------------------------------------------------------------------

    private static final String TABLE = "projects/bo/instances/bo/tables/bo-table";

    static Workload bigtable() {
        return new Workload() {
            @Override
            public String name() {
                return "bigtablewire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("bigtablewire", "WARP_BIGTABLEWIRE_PORT");
                stores.enable("bigtable");
            }

            @Override
            public void prepare(WarpProcess warp) {
                ManagedChannel ch = channel(warp.port("bigtablewire"));
                try {
                    BigtableTableAdminGrpc.newBlockingStub(ch).withDeadlineAfter(10, TimeUnit.SECONDS).createTable(
                            com.sayonora.warp.bigtablewire.admin.v2.CreateTableRequest.newBuilder().setParent("projects/bo/instances/bo")
                                    .setTableId("bo-table").setTable(com.sayonora.warp.bigtablewire.admin.v2.Table.newBuilder()
                                            .putColumnFamilies("cf", com.sayonora.warp.bigtablewire.admin.v2.ColumnFamily.getDefaultInstance()))
                                    .build());
                } finally {
                    ch.shutdownNow();
                }
            }

            @Override
            public Client open(WarpProcess warp) {
                ManagedChannel ch = channel(warp.port("bigtablewire"));
                var stub = BigtableGrpc.newBlockingStub(ch);
                return new Client() {
                    @Override
                    public void write(long id) {
                        stub.withDeadlineAfter(10, TimeUnit.SECONDS).mutateRow(com.sayonora.warp.bigtablewire.v2.MutateRowRequest.newBuilder()
                                .setTableName(TABLE).setRowKey(ByteString.copyFromUtf8(String.valueOf(id)))
                                .addMutations(com.sayonora.warp.bigtablewire.v2.Mutation.newBuilder().setSetCell(
                                        com.sayonora.warp.bigtablewire.v2.Mutation.SetCell.newBuilder().setFamilyName("cf")
                                                .setColumnQualifier(ByteString.copyFromUtf8("q")).setTimestampMicros(-1)
                                                .setValue(ByteString.copyFromUtf8("1"))))
                                .build());
                    }

                    @Override
                    public void read(long id) {
                        Iterator<com.sayonora.warp.bigtablewire.v2.ReadRowsResponse> it = stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                                .readRows(com.sayonora.warp.bigtablewire.v2.ReadRowsRequest.newBuilder().setTableName(TABLE)
                                        .setRows(com.sayonora.warp.bigtablewire.v2.RowSet.newBuilder()
                                                .addRowKeys(ByteString.copyFromUtf8(String.valueOf(id)))).build());
                        while (it.hasNext()) {
                            it.next();
                        }
                    }

                    @Override
                    public void close() {
                        ch.shutdownNow();
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                ManagedChannel ch = channel(warp.port("bigtablewire"));
                try {
                    Iterator<com.sayonora.warp.bigtablewire.v2.ReadRowsResponse> it = BigtableGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(60, TimeUnit.SECONDS).readRows(com.sayonora.warp.bigtablewire.v2.ReadRowsRequest.newBuilder()
                                    .setTableName(TABLE).build());
                    while (it.hasNext()) {
                        for (var chunk : it.next().getChunksList()) {
                            if (!chunk.getRowKey().isEmpty()) {
                                ids.add(Long.parseLong(chunk.getRowKey().toStringUtf8()));
                            }
                        }
                    }
                } finally {
                    ch.shutdownNow();
                }
                return ids;
            }
        };
    }
}
