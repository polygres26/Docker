package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real proof of the new {@code GET /api/usage} endpoint: closes a genuine gap found auditing
 * Warp's own architecture diagram against what's actually implemented -- usage/cost was only ever
 * visible broken down by tenant or by backend, never by WORKLOAD CLASS (the same App/Analytics/AI
 * shape {@code QosControlStage} already rate-limits by), so there was no way to answer "how much
 * of our usage is AI traffic vs. ordinary app traffic," the natural cost question on a gateway
 * shared by both.
 *
 * <p>Proof: a real SELECT (RouterStage's own {@code classifyWorkload} tags it "query") and a real
 * INSERT (tagged "write") through a real pgwire client, then a real {@code GET /api/usage} shows
 * both workload classes with real, non-zero call counts.
 */
class UsageByWorkloadClassIntegrationTest {

    private static final String ADMIN_TOKEN = "usage-endpoint-test-token";

    private RealPostgres postgres;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    @Test
    void usageEndpointReportsRealCountsByWorkloadClass() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE usage_it (id INTEGER PRIMARY KEY)");
            st.executeUpdate("INSERT INTO usage_it (id) VALUES (1)");
            st.executeUpdate("INSERT INTO usage_it (id) VALUES (2)");
            st.executeQuery("SELECT * FROM usage_it").close();
            st.executeQuery("SELECT * FROM usage_it").close();
            st.executeQuery("SELECT * FROM usage_it").close();
        }

        String usageJson = adminGet("/api/usage");
        JsonObject usage = JsonParser.parseString(usageJson).getAsJsonObject();
        JsonArray byWorkloadClass = usage.getAsJsonArray("byWorkloadClass");
        assertTrue(byWorkloadClass != null && byWorkloadClass.size() > 0,
                "expected /api/usage.byWorkloadClass to report at least one workload class -- got: " + usageJson);

        long queryCalls = 0;
        long writeCalls = 0;
        for (var el : byWorkloadClass) {
            JsonObject row = el.getAsJsonObject();
            String workloadClass = row.get("workloadClass").getAsString();
            long calls = row.get("calls").getAsLong();
            if ("query".equals(workloadClass)) {
                queryCalls = calls;
            } else if ("write".equals(workloadClass)) {
                writeCalls = calls;
            }
        }
        assertTrue(queryCalls >= 3, "expected at least the 3 real SELECTs counted under \"query\" -- got: " + usageJson);
        assertTrue(writeCalls >= 2, "expected at least the 2 real INSERTs counted under \"write\" -- got: " + usageJson);

        JsonArray byTenant = usage.getAsJsonArray("byTenant");
        assertTrue(byTenant != null && byTenant.size() > 0,
                "expected /api/usage.byTenant to also report real per-tenant usage -- got: " + usageJson);
    }

    private String adminGet(String path) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + warp.metricsPort() + path))
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        return http.send(req, HttpResponse.BodyHandlers.ofString()).body();
    }
}
