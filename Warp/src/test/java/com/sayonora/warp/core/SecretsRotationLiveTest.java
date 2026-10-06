package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Real Warp processes against a real control-plane database: secrets are stored encrypted under key A, a second Warp started with B active and
 * A previous still reads them, the rotate API moves everything (config, XA log, ACME state) to B, a third Warp with only B starts, a Warp with
 * only A is refused with a message naming the missing key, and WARP_REQUIRE_ENCRYPTION_KEY=true refuses to start without a key.
 * Opt-in: WARP_TEST_BROWNOUT_PG_BIN.
 */
class SecretsRotationLiveTest {

    private static String newKey() {
        byte[] raw = new byte[32];
        new SecureRandom().nextBytes(raw);
        return Base64.getEncoder().encodeToString(raw);
    }

    private static WarpProcess start(LocalPostgres cfg, Map<String, String> extraEnv) throws Exception {
        WarpProcess.Builder b = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "super-secret-pw")
                .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                .env("WARP_OTEL_ENDPOINT", "disabled");
        extraEnv.forEach(b::env);
        return b.start();
    }

    private static JsonObject get(WarpProcess w) throws Exception {
        return JsonParser.parseString(BrownoutHarness.http("GET", "http://localhost:" + w.metricsPort() + "/api/security/encryption", null))
                .getAsJsonObject();
    }

    private static String latestBackends(LocalPostgres cfg) throws Exception {
        try (Connection c = cfg.conn(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select payload->>'backends' from warp_config order by version desc limit 1")) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void secretsAreEncryptedRotatedAndTheOldKeyCanBeDropped() throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("secretsrot");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        try {
            // the control-plane database accepts the password Warp will connect with
            try (Connection c = cfg.conn(); Statement st = c.createStatement()) {
                st.execute("alter role warp password 'super-secret-pw'");
            }
            String keyA = newKey();
            String keyB = newKey();
            String spec = "default=jdbc:postgresql://127.0.0.1:" + cfg.port() + "/postgres|warp|super-secret-pw";

            // 1. Warp with key A: the stored backend spec (it embeds the password) is encrypted
            String idA;
            try (WarpProcess w = start(cfg, Map.of("SAYONORA_ENCRYPTION_KEY", keyA, "WARP_BACKENDS", spec))) {
                JsonObject st = get(w);
                idA = st.get("activeKeyId").getAsString();
                assertTrue(st.get("encryptionEnabled").getAsBoolean());
                assertFalse(st.get("needsRotation").getAsBoolean());
                String stored = latestBackends(cfg);
                assertTrue(stored.startsWith("encv2:" + idA + ":"), stored);
                assertFalse(stored.contains("super-secret-pw"), "the password is not in the control-plane database in the clear");
            }

            // stale data from before a key existed, or under an older format: an XA log row, an ACME row
            try (Connection c = cfg.conn(); Statement st = c.createStatement()) {
                com.sayonora.warp.core.DdlTemplates.run(st, "postgres", "warp_xa_log", Map.of());
                com.sayonora.warp.core.DdlTemplates.run(st, "postgres", "warp_acme_state", Map.of());
                st.execute("insert into warp_xa_log (gtrid_hex, branch_index, backend_name, backend_password) values ('aa', 0, 'b', 'xa-plain-pw')");
                st.execute("insert into warp_acme_state (name, value) values ('account-key', 'acme-plain-secret')");
            }

            // 2. Warp with B active and A previous still reads the config, and reports what needs moving
            String idB;
            try (WarpProcess w = start(cfg, Map.of("SAYONORA_ENCRYPTION_KEY", keyB, "SAYONORA_ENCRYPTION_KEY_PREVIOUS", keyA))) {
                JsonObject st = get(w);
                idB = st.get("activeKeyId").getAsString();
                assertEquals(1, st.getAsJsonArray("previousKeyIds").size());
                assertEquals(idA, st.getAsJsonArray("previousKeyIds").get(0).getAsString());
                assertTrue(st.get("needsRotation").getAsBoolean(), st.toString());
                assertEquals(idA, st.getAsJsonObject("latestConfig").get("backends").getAsString());
                assertTrue(st.getAsJsonObject("xaLogPasswords").has("plaintext"), st.toString());
                assertTrue(st.getAsJsonObject("acmeState").has("plaintext"), st.toString());

                // 3. rotate
                JsonObject done = JsonParser.parseString(BrownoutHarness.http("POST", "http://localhost:" + w.metricsPort()
                        + "/api/security/encryption/rotate", "")).getAsJsonObject();
                assertTrue(done.get("newConfigVersion").getAsLong() > 0, done.toString());
                assertEquals(1, done.get("xaLogRowsReencrypted").getAsInt());
                assertEquals(1, done.get("acmeRowsReencrypted").getAsInt());
                JsonObject after = done.getAsJsonObject("status");
                assertFalse(after.get("needsRotation").getAsBoolean(), after.toString());
                assertEquals(idB, after.getAsJsonObject("latestConfig").get("backends").getAsString());
                assertTrue(latestBackends(cfg).startsWith("encv2:" + idB + ":"));
                try (Connection c = cfg.conn(); Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery("select backend_password from warp_xa_log")) {
                    rs.next();
                    assertTrue(rs.getString(1).startsWith("encv2:" + idB + ":"), rs.getString(1));
                    assertFalse(rs.getString(1).contains("xa-plain-pw"));
                }
                // nothing left to do, and a second rotation changes nothing
                JsonObject again = JsonParser.parseString(BrownoutHarness.http("POST", "http://localhost:" + w.metricsPort()
                        + "/api/security/encryption/rotate", "")).getAsJsonObject();
                assertTrue(again.get("newConfigVersion").isJsonNull(), again.toString());
                assertEquals(0, again.get("xaLogRowsReencrypted").getAsInt());
            }

            // 4. the old key is no longer needed
            try (WarpProcess w = start(cfg, Map.of("SAYONORA_ENCRYPTION_KEY", keyB))) {
                assertFalse(get(w).get("needsRotation").getAsBoolean());
            }

            // 5. dropping the old key too early is caught at startup, with the key named
            Exception tooEarly = assertThrows(Exception.class, () -> start(cfg, Map.of("SAYONORA_ENCRYPTION_KEY", keyA)).close());
            assertTrue(tooEarly.getMessage().contains("SAYONORA_ENCRYPTION_KEY_PREVIOUS") || tooEarly.getMessage().contains(idB),
                    tooEarly.getMessage());

            // 6. fail closed: no key under WARP_REQUIRE_ENCRYPTION_KEY=true does not start
            Exception noKey = assertThrows(Exception.class, () -> start(cfg, Map.of("WARP_REQUIRE_ENCRYPTION_KEY", "true")).close());
            assertTrue(noKey.getMessage().contains("refusing to start"), noKey.getMessage());
            System.out.println("SECRETS-RESULT ok | key A " + idA + " -> key B " + idB + " | rotate: config + 1 xa row + 1 acme row | old key dropped safely"
                    + " | too-early drop: " + tooEarly.getMessage().replaceAll("\\s+", " ").substring(0, Math.min(200, tooEarly.getMessage().length())));
        } finally {
            cfg.stop("immediate");
        }
    }
}
