package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AccessSummaryTest {

    private static JsonObject front(JsonObject out, String id) {
        for (var e : out.getAsJsonArray("frontends")) {
            if (id.equals(e.getAsJsonObject().get("id").getAsString())) {
                return e.getAsJsonObject();
            }
        }
        throw new AssertionError("no frontend " + id);
    }

    @Test
    void reportsMethodsAndNamesButNeverSecrets() {
        JsonObject out = AccessSummary.toJson(Map.of(
                "WARP_AUTH_MODE", "postgres_roles",
                "WARP_AUTH_CREDENTIALS", "alice=s3cretpw;bob=hunter2",
                "WARP_S3WIRE_CREDENTIALS", "AKIAEXAMPLE=topsecretkey",
                "WARP_GCSWIRE_TOKENS", "tok-one,tok-two"));
        String json = out.toString();
        for (String secret : new String[] {"s3cretpw", "hunter2", "topsecretkey", "tok-one", "tok-two"}) {
            assertFalse(json.contains(secret), "must not leak " + secret);
        }
        assertEquals("postgres-roles", front(out, "pgwire").get("method").getAsString());
        assertEquals("multi-user", out.getAsJsonObject("sqlCredentials").get("mode").getAsString());
        assertTrue(front(out, "cqlwire").get("enforced").getAsBoolean(), "credentials set turns SASL on");
        assertEquals("AKIAEXAMPLE", front(out, "s3wire").getAsJsonArray("principals").get(0).getAsString());
        assertEquals(2, front(out, "gcswire").get("detail").getAsString().charAt(0) - '0');
        assertFalse(front(out, "rediswire").get("enforced").getAsBoolean());
    }
}
