package com.sayonora.warp.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.sayonora.warp.testsupport.WarpProcess;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Several clients inserting into a collection that does not exist yet, at the same moment, must all succeed: the collection is
 * created on first use, and concurrent {@code CREATE TABLE IF NOT EXISTS} in Postgres can fail with "type ... already exists"
 * (SQLSTATE 42710) instead of doing nothing. Needs a Postgres ({@code WARP_TEST_MONGOWIRE_PG_PORT}, superuser "warp", trust auth).
 */
class MongowireConcurrentCreateLiveTest {

    @Test
    void concurrentFirstInsertsIntoABrandNewCollectionAllSucceed() throws Exception {
        String pgPort = System.getenv("WARP_TEST_MONGOWIRE_PG_PORT");
        Assumptions.assumeTrue(pgPort != null);
        try (var c = java.sql.DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + pgPort + "/postgres", "warp", "secret");
                var st = c.createStatement()) {
            st.execute("drop table if exists warp_config cascade");
        }
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend("127.0.0.1", Integer.parseInt(pgPort), "postgres", "warp", "secret")
                .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_QOS_RATE_PER_SEC", "1000000")
                .env("WARP_QOS_BURST", "1000000")
                .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String run = Long.toString(System.nanoTime(), 36); // so a rerun on the same Postgres starts from databases that do not exist
            int threads = 8;
            int collections = 60; // each round uses a brand-new database AND collection: the first write also creates the database's helper tables
            AtomicInteger failures = new AtomicInteger();
            Map<String, Integer> reasons = new ConcurrentHashMap<>();
            String uri = "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true&retryWrites=false";
            List<MongoClient> clients = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                clients.add(MongoClients.create(uri));
            }
            try {
                for (int n = 0; n < collections; n++) {
                    String db = "race_" + run + "_" + n;
                    String coll = "c";
                    CyclicBarrier go = new CyclicBarrier(threads);
                    List<Thread> ts = new ArrayList<>();
                    for (int t = 0; t < threads; t++) {
                        int id = t;
                        Thread th = new Thread(() -> {
                            try {
                                go.await();
                                clients.get(id).getDatabase(db).getCollection(coll).insertOne(new Document("_id", id).append("n", 1));
                            } catch (Exception e) {
                                failures.incrementAndGet();
                                reasons.merge(String.valueOf(e.getMessage()).split("\n")[0].replaceAll("\\s+", " "), 1, Integer::sum);
                            }
                        });
                        ts.add(th);
                        th.start();
                    }
                    for (Thread th : ts) {
                        th.join();
                    }
                }
            } finally {
                clients.forEach(MongoClient::close);
            }
            System.out.println("CREATE-RACE failures=" + failures + " of " + threads * collections + " " + new TreeMap<>(reasons));
            assertEquals(0, failures.get(), "failed first inserts: " + new TreeMap<>(reasons));
        }
    }

    private static int freePort() throws java.io.IOException {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
