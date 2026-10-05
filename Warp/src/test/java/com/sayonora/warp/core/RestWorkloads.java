package com.sayonora.warp.core;

import com.sayonora.warp.testsupport.BrownoutHarness.Client;
import com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig;
import com.sayonora.warp.testsupport.BrownoutHarness.Workload;
import com.sayonora.warp.testsupport.WarpProcess;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Workloads for the store-backed protocols that are driven with plain HTTP (REST/JSON) or a raw socket (RESP) because their SDKs
 * are not on the test classpath. The request shapes follow each service's public API. A failed call throws; no retries.
 */
final class RestWorkloads {

    private RestWorkloads() {
    }

    static List<Workload> all() {
        return List.of(redis(), gcs(), azblob(), pubsub(), firestore(), datastore(), cosmos());
    }

    // ---- tiny HTTP helper --------------------------------------------------------------------------------

    private static final class Http {
        final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        final String base;
        final Map<String, String> headers;

        Http(int port, Map<String, String> headers) {
            this.base = "http://localhost:" + port;
            this.headers = headers;
        }

        String call(String method, String path, String body, Map<String, String> extra) throws Exception {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(10));
            headers.forEach(b::header);
            if (extra != null) {
                extra.forEach(b::header);
            }
            b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
            HttpResponse<String> r = client.send(b.build(), HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() >= 300) {
                String text = r.body() == null ? "" : r.body().replaceAll("\\s+", " ");
                throw new IOException("HTTP " + r.statusCode() + " " + (text.length() > 160 ? text.substring(0, 160) : text));
            }
            return r.body();
        }

        String call(String method, String path, String body) throws Exception {
            return call(method, path, body, null);
        }
    }

    private static List<String> all(String text, String regex) {
        List<String> out = new ArrayList<>();
        Matcher m = Pattern.compile(regex).matcher(text);
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    private abstract static class RestClient implements Client {
        @Override
        public void close() {
            // the JDK HttpClient needs no closing
        }
    }

    // ---- Redis (RESP over a socket) ----------------------------------------------------------------------

    private static final class Resp implements AutoCloseable {
        private final Socket socket = new Socket();
        private final InputStream in;
        private final OutputStream out;

        Resp(int port) throws IOException {
            socket.connect(new InetSocketAddress("localhost", port), 5000);
            socket.setSoTimeout(10_000);
            in = new BufferedInputStream(socket.getInputStream());
            out = socket.getOutputStream();
        }

        Object cmd(String... args) throws IOException {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            b.write(("*" + args.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (String a : args) {
                byte[] x = a.getBytes(StandardCharsets.UTF_8);
                b.write(("$" + x.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                b.write(x);
                b.write("\r\n".getBytes(StandardCharsets.UTF_8));
            }
            out.write(b.toByteArray());
            out.flush();
            return read();
        }

        private String line() throws IOException {
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != '\r') {
                if (c < 0) {
                    throw new IOException("connection closed");
                }
                sb.append((char) c);
            }
            in.read(); // \n
            return sb.toString();
        }

        private Object read() throws IOException {
            String l = line();
            switch (l.charAt(0)) {
                case '+':
                    return l.substring(1);
                case '-':
                    throw new IOException("redis error: " + l.substring(1));
                case ':':
                    return Long.parseLong(l.substring(1));
                case '$': {
                    int n = Integer.parseInt(l.substring(1));
                    if (n < 0) {
                        return null;
                    }
                    byte[] data = in.readNBytes(n);
                    in.read();
                    in.read();
                    return new String(data, StandardCharsets.UTF_8);
                }
                case '*': {
                    int n = Integer.parseInt(l.substring(1));
                    List<Object> items = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        items.add(read());
                    }
                    return items;
                }
                default:
                    throw new IOException("unexpected RESP reply: " + l);
            }
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // done
            }
        }
    }

    static Workload redis() {
        return new Workload() {
            @Override
            public String name() {
                return "rediswire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("rediswire", "WARP_REDISWIRE_PORT");
                stores.enable("redis");
            }

            @Override
            public Client open(WarpProcess warp) throws Exception {
                Resp r = new Resp(warp.port("rediswire"));
                return new Client() {
                    @Override
                    public void write(long id) throws Exception {
                        r.cmd("SET", "bo:" + id, "v");
                    }

                    @Override
                    public void read(long id) throws Exception {
                        r.cmd("GET", "bo:" + id);
                    }

                    @Override
                    public void close() {
                        r.close();
                    }
                };
            }

            @Override
            @SuppressWarnings("unchecked")
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                Set<Long> ids = new HashSet<>();
                try (Resp r = new Resp(warp.port("rediswire"))) {
                    String cursor = "0";
                    do {
                        List<Object> reply = (List<Object>) r.cmd("SCAN", cursor, "MATCH", "bo:*", "COUNT", "1000");
                        cursor = (String) reply.get(0);
                        for (Object k : (List<Object>) reply.get(1)) {
                            ids.add(Long.parseLong(((String) k).substring(3)));
                        }
                    } while (!"0".equals(cursor));
                }
                return ids;
            }
        };
    }

    // ---- Google Cloud Storage (JSON API) -----------------------------------------------------------------

    static Workload gcs() {
        return new Workload() {
            @Override
            public String name() {
                return "gcswire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("gcswire", "WARP_GCSWIRE_PORT").env("WARP_GCSWIRE_ALLOW_ANONYMOUS", "true");
                stores.enable("gcs");
            }

            @Override
            public void prepare(WarpProcess warp) throws Exception {
                new Http(warp.port("gcswire"), Map.of("Authorization", "Bearer t", "Content-Type", "application/json"))
                        .call("POST", "/storage/v1/b?project=bo", "{\"name\":\"bo-bucket\"}");
            }

            @Override
            public Client open(WarpProcess warp) {
                Http h = new Http(warp.port("gcswire"), Map.of("Authorization", "Bearer t"));
                return new RestClient() {
                    @Override
                    public void write(long id) throws Exception {
                        h.call("POST", "/upload/storage/v1/b/bo-bucket/o?uploadType=media&name=" + id, "payload-" + id,
                                Map.of("Content-Type", "text/plain"));
                    }

                    @Override
                    public void read(long id) throws Exception {
                        try {
                            h.call("GET", "/storage/v1/b/bo-bucket/o/" + id, null);
                        } catch (IOException e) {
                            if (!e.getMessage().startsWith("HTTP 404")) {
                                throw e;
                            }
                        }
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                Http h = new Http(warp.port("gcswire"), Map.of("Authorization", "Bearer t"));
                Set<Long> ids = new HashSet<>();
                String token = null;
                do {
                    String body = h.call("GET", "/storage/v1/b/bo-bucket/o?maxResults=1000" + (token == null ? "" : "&pageToken=" + token), null);
                    all(body, "\"name\"\\s*:\\s*\"(\\d+)\"").forEach(n -> ids.add(Long.parseLong(n)));
                    List<String> next = all(body, "\"nextPageToken\"\\s*:\\s*\"([^\"]+)\"");
                    token = next.isEmpty() ? null : next.get(0);
                } while (token != null);
                return ids;
            }
        };
    }

    // ---- Azure Blob Storage ------------------------------------------------------------------------------

    static Workload azblob() {
        Map<String, String> auth = Map.of("Authorization", "Bearer az-token", "x-ms-version", "2021-08-06");
        return new Workload() {
            @Override
            public String name() {
                return "azblobwire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("azblob", "WARP_AZBLOBWIRE_PORT").env("WARP_AZURE_BEARER_TOKEN", "az-token")
                        .env("WARP_AZURE_DEV_ACCOUNT", "true"); // the standard dev account "devstoreaccount1"
                stores.enable("azblob");
            }

            @Override
            public void prepare(WarpProcess warp) throws Exception {
                new Http(warp.port("azblob"), auth).call("PUT", "/devstoreaccount1/bo-container?restype=container", null);
            }

            @Override
            public Client open(WarpProcess warp) {
                Http h = new Http(warp.port("azblob"), auth);
                return new RestClient() {
                    @Override
                    public void write(long id) throws Exception {
                        h.call("PUT", "/devstoreaccount1/bo-container/" + id, "payload-" + id,
                                Map.of("x-ms-blob-type", "BlockBlob", "Content-Type", "text/plain"));
                    }

                    @Override
                    public void read(long id) throws Exception {
                        try {
                            h.call("GET", "/devstoreaccount1/bo-container/" + id + "?comp=metadata", null);
                        } catch (IOException e) {
                            if (!e.getMessage().startsWith("HTTP 404")) {
                                throw e;
                            }
                        }
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                Http h = new Http(warp.port("azblob"), auth);
                Set<Long> ids = new HashSet<>();
                String marker = null;
                do {
                    String body = h.call("GET", "/devstoreaccount1/bo-container?restype=container&comp=list&maxresults=1000"
                            + (marker == null ? "" : "&marker=" + marker), null);
                    all(body, "<Name>(\\d+)</Name>").forEach(n -> ids.add(Long.parseLong(n)));
                    List<String> next = all(body, "<NextMarker>([^<]+)</NextMarker>");
                    marker = next.isEmpty() ? null : next.get(0);
                } while (marker != null);
                return ids;
            }
        };
    }

    // ---- Google Cloud Pub/Sub (REST) ---------------------------------------------------------------------

    static Workload pubsub() {
        Map<String, String> json = Map.of("Content-Type", "application/json");
        return new Workload() {
            @Override
            public String name() {
                return "pubsubwire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("pubsub-rest", "WARP_PUBSUBWIRE_REST_PORT").frontend("pubsub-grpc", "WARP_PUBSUBWIRE_PORT");
                stores.enable("pubsub");
            }

            @Override
            public void prepare(WarpProcess warp) throws Exception {
                Http h = new Http(warp.port("pubsub-rest"), json);
                h.call("PUT", "/v1/projects/bo-project/topics/bo-topic", "{}");
                h.call("PUT", "/v1/projects/bo-project/subscriptions/bo-sub", "{\"topic\":\"projects/bo-project/topics/bo-topic\",\"ackDeadlineSeconds\":600}");
            }

            @Override
            public Client open(WarpProcess warp) {
                Http h = new Http(warp.port("pubsub-rest"), json);
                return new RestClient() {
                    @Override
                    public void write(long id) throws Exception {
                        h.call("POST", "/v1/projects/bo-project/topics/bo-topic:publish", "{\"messages\":[{\"data\":\""
                                + Base64.getEncoder().encodeToString(String.valueOf(id).getBytes(StandardCharsets.UTF_8)) + "\"}]}");
                    }

                    @Override
                    public void read(long id) throws Exception {
                        h.call("GET", "/v1/projects/bo-project/topics/bo-topic", null);
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                Http h = new Http(warp.port("pubsub-rest"), json);
                Set<Long> ids = new HashSet<>();
                int empty = 0;
                while (empty < 3) {
                    String body = h.call("POST", "/v1/projects/bo-project/subscriptions/bo-sub:pull", "{\"maxMessages\":1000,\"returnImmediately\":true}");
                    List<String> data = all(body, "\"data\"\\s*:\\s*\"([^\"]+)\"");
                    List<String> ackIds = all(body, "\"ackId\"\\s*:\\s*\"([^\"]+)\"");
                    if (data.isEmpty()) {
                        empty++;
                        continue;
                    }
                    empty = 0;
                    data.forEach(d -> ids.add(Long.parseLong(new String(Base64.getDecoder().decode(d), StandardCharsets.UTF_8))));
                    h.call("POST", "/v1/projects/bo-project/subscriptions/bo-sub:acknowledge",
                            "{\"ackIds\":[" + String.join(",", ackIds.stream().map(a -> "\"" + a + "\"").toList()) + "]}");
                }
                return ids;
            }
        };
    }

    // ---- Firestore (REST) --------------------------------------------------------------------------------

    static Workload firestore() {
        Map<String, String> json = Map.of("Content-Type", "application/json", "Authorization", "Bearer owner");
        String docs = "/v1/projects/bo/databases/(default)/documents";
        return new Workload() {
            @Override
            public String name() {
                return "firestorewire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("firestorewire", "WARP_FIRESTOREWIRE_PORT");
                stores.enable("firestore");
            }

            @Override
            public Client open(WarpProcess warp) {
                Http h = new Http(warp.port("firestorewire"), json);
                return new RestClient() {
                    @Override
                    public void write(long id) throws Exception {
                        h.call("PATCH", docs + "/items/" + id, "{\"fields\":{\"n\":{\"integerValue\":\"1\"}}}");
                    }

                    @Override
                    public void read(long id) throws Exception {
                        try {
                            h.call("GET", docs + "/items/" + id, null);
                        } catch (IOException e) {
                            if (!e.getMessage().startsWith("HTTP 404")) {
                                throw e;
                            }
                        }
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                Http h = new Http(warp.port("firestorewire"), json);
                Set<Long> ids = new HashSet<>();
                String token = null;
                do {
                    String body = h.call("GET", docs + "/items?pageSize=300" + (token == null ? "" : "&pageToken=" + token), null);
                    all(body, "/documents/items/(\\d+)\"").forEach(n -> ids.add(Long.parseLong(n)));
                    List<String> next = all(body, "\"nextPageToken\"\\s*:\\s*\"([^\"]+)\"");
                    token = next.isEmpty() ? null : next.get(0);
                } while (token != null);
                return ids;
            }
        };
    }

    // ---- Google Cloud Datastore (REST) -------------------------------------------------------------------

    static Workload datastore() {
        Map<String, String> json = Map.of("Content-Type", "application/json", "Authorization", "Bearer owner");
        return new Workload() {
            @Override
            public String name() {
                return "datastorewire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("datastorewire", "WARP_DATASTOREWIRE_PORT");
                stores.enable("datastore");
            }

            @Override
            public Client open(WarpProcess warp) {
                Http h = new Http(warp.port("datastorewire"), json);
                return new RestClient() {
                    @Override
                    public void write(long id) throws Exception {
                        h.call("POST", "/v1/projects/bo:commit", "{\"mode\":\"NON_TRANSACTIONAL\",\"mutations\":[{\"upsert\":{\"key\":"
                                + "{\"path\":[{\"kind\":\"Item\",\"name\":\"" + id + "\"}]},\"properties\":{\"n\":{\"integerValue\":\"1\"}}}}]}");
                    }

                    @Override
                    public void read(long id) throws Exception {
                        h.call("POST", "/v1/projects/bo:lookup", "{\"keys\":[{\"path\":[{\"kind\":\"Item\",\"name\":\"" + id + "\"}]}]}");
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                Http h = new Http(warp.port("datastorewire"), json);
                Set<Long> ids = new HashSet<>();
                String cursor = null;
                while (true) {
                    String body = h.call("POST", "/v1/projects/bo:runQuery", "{\"query\":{\"kind\":[{\"name\":\"Item\"}],\"limit\":1000"
                            + (cursor == null ? "" : ",\"startCursor\":\"" + cursor + "\"") + "}}");
                    all(body, "\"name\"\\s*:\\s*\"(\\d+)\"").forEach(n -> ids.add(Long.parseLong(n)));
                    List<String> more = all(body, "\"moreResults\"\\s*:\\s*\"([A-Z_]+)\"");
                    List<String> end = all(body, "\"endCursor\"\\s*:\\s*\"([^\"]+)\"");
                    if (more.isEmpty() || !more.get(0).startsWith("NOT_FINISHED") && !more.get(0).equals("MORE_RESULTS_AFTER_LIMIT") || end.isEmpty()) {
                        break;
                    }
                    cursor = end.get(0);
                }
                return ids;
            }
        };
    }

    // ---- Cosmos DB (REST, auth disabled) -----------------------------------------------------------------

    static Workload cosmos() {
        Map<String, String> base = Map.of("x-ms-version", "2018-12-31", "x-ms-date", "Tue, 01 Jan 2030 00:00:00 GMT",
                "Content-Type", "application/json", "Accept", "application/json");
        return new Workload() {
            @Override
            public String name() {
                return "cosmoswire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("cosmoswire", "WARP_COSMOSWIRE_PORT").env("WARP_COSMOSWIRE_AUTH", "false");
                stores.enable("cosmos");
            }

            @Override
            public void prepare(WarpProcess warp) throws Exception {
                Http h = new Http(warp.port("cosmoswire"), base);
                h.call("POST", "/dbs", "{\"id\":\"bo\"}");
                h.call("POST", "/dbs/bo/colls", "{\"id\":\"c\",\"partitionKey\":{\"paths\":[\"/pk\"],\"kind\":\"Hash\"}}");
            }

            @Override
            public Client open(WarpProcess warp) {
                Http h = new Http(warp.port("cosmoswire"), base);
                Map<String, String> pk = Map.of("x-ms-documentdb-partitionkey", "[\"p\"]");
                return new RestClient() {
                    @Override
                    public void write(long id) throws Exception {
                        h.call("POST", "/dbs/bo/colls/c/docs", "{\"id\":\"" + id + "\",\"pk\":\"p\",\"n\":1}", pk);
                    }

                    @Override
                    public void read(long id) throws Exception {
                        try {
                            h.call("GET", "/dbs/bo/colls/c/docs/" + id, null, pk);
                        } catch (IOException e) {
                            if (!e.getMessage().startsWith("HTTP 404")) {
                                throw e;
                            }
                        }
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                Http h = new Http(warp.port("cosmoswire"), base);
                Set<Long> ids = new HashSet<>();
                String body = h.call("POST", "/dbs/bo/colls/c/docs", "{\"query\":\"SELECT c.id FROM c\",\"parameters\":[]}",
                        Map.of("x-ms-documentdb-isquery", "true", "Content-Type", "application/query+json",
                                "x-ms-documentdb-query-enablecrosspartition", "true", "x-ms-max-item-count", "100000"));
                all(body, "\"id\"\\s*:\\s*\"(\\d+)\"").forEach(n -> ids.add(Long.parseLong(n)));
                return ids;
            }
        };
    }
}
