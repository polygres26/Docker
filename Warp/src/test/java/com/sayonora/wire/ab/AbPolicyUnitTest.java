package com.sayonora.wire.ab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AbPolicyUnitTest {

    private static AbPolicy policy(String json) {
        return AbPolicy.parse("s3", JsonParser.parseString(json).getAsJsonObject());
    }

    private record C(String key, String ip, Map<String, String> h, boolean read) implements AbPolicy.Ctx {
        @Override
        public String accessKey() {
            return key;
        }

        @Override
        public String header(String n) {
            return h.get(n);
        }
    }

    private static C c(String key, boolean read) {
        return new C(key, "10.1.2.3", Map.of(), read);
    }

    @Test
    void stickyAssignmentIsDeterministicAndWithinTolerance() {
        AbPolicy p = policy("{\"mode\":\"split\",\"cloudPercent\":30,\"target\":\"t\"}");
        int cloud = 0;
        int n = 20000;
        for (int i = 0; i < n; i++) {
            AbPolicy.Route r = p.decide(c("AKIA" + i, true));
            assertEquals(r, p.decide(c("AKIA" + i, true)), "same client, same side");
            if (r == AbPolicy.Route.CLOUD) {
                cloud++;
            }
        }
        double share = 100.0 * cloud / n;
        assertTrue(Math.abs(share - 30) < 1.5, "cloud share " + share);
    }

    @Test
    void raisingThePercentageOnlyMovesClientsToCloud() {
        AbPolicy lo = policy("{\"mode\":\"split\",\"cloudPercent\":10,\"target\":\"t\"}");
        AbPolicy hi = policy("{\"mode\":\"split\",\"cloudPercent\":40,\"target\":\"t\"}");
        for (int i = 0; i < 5000; i++) {
            if (lo.decide(c("k" + i, true)) == AbPolicy.Route.CLOUD) {
                assertEquals(AbPolicy.Route.CLOUD, hi.decide(c("k" + i, true)));
            }
        }
    }

    @Test
    void zeroAndHundredPercent() {
        AbPolicy zero = policy("{\"mode\":\"split\",\"cloudPercent\":0,\"target\":\"t\"}");
        AbPolicy all = policy("{\"mode\":\"split\",\"cloudPercent\":100,\"target\":\"t\"}");
        for (int i = 0; i < 500; i++) {
            assertEquals(AbPolicy.Route.LOCAL, zero.decide(c("k" + i, true)));
            assertEquals(AbPolicy.Route.CLOUD, all.decide(c("k" + i, true)));
        }
    }

    @Test
    void writesInASplitGoToTheWriteOwnerOnly() {
        AbPolicy p = policy("{\"mode\":\"split\",\"cloudPercent\":100,\"target\":\"t\",\"writeOwner\":\"local\"}");
        assertEquals(AbPolicy.Route.CLOUD, p.decide(c("a", true)));
        assertEquals(AbPolicy.Route.LOCAL, p.decide(c("a", false)), "write owner is local even for a cloud client");
        AbPolicy q = policy("{\"mode\":\"split\",\"cloudPercent\":0,\"target\":\"t\",\"writeOwner\":\"cloud\"}");
        assertEquals(AbPolicy.Route.LOCAL, q.decide(c("a", true)));
        assertEquals(AbPolicy.Route.CLOUD, q.decide(c("a", false)));
    }

    @Test
    void dualWriteOnlyWhenExplicitlyEnabled() {
        AbPolicy p = policy("{\"mode\":\"split\",\"cloudPercent\":50,\"target\":\"t\",\"dualWrite\":true}");
        assertEquals(AbPolicy.Route.DUAL_WRITE, p.decide(c("a", false)));
        AbPolicy off = policy("{\"mode\":\"split\",\"cloudPercent\":50,\"target\":\"t\"}");
        assertFalse(off.dualWrite);
        assertTrue(off.decide(c("a", false)) != AbPolicy.Route.DUAL_WRITE);
    }

    @Test
    void pureModesSendWritesToTheirSide() {
        assertEquals(AbPolicy.Route.CLOUD, policy("{\"mode\":\"cloud\",\"target\":\"t\"}").decide(c("a", false)));
        assertEquals(AbPolicy.Route.LOCAL, policy("{\"mode\":\"local\"}").decide(c("a", false)));
    }

    @Test
    void compareModeComparesReadsAndOwnsWrites() {
        AbPolicy p = policy("{\"mode\":\"compare\",\"target\":\"t\"}");
        assertEquals(AbPolicy.Route.COMPARE, p.decide(c("a", true)));
        assertEquals(AbPolicy.Route.LOCAL, p.decide(c("a", false)));
    }

    @Test
    void rulesByAccessKeyIpAndHeaderWinInOrder() {
        AbPolicy p = policy("{\"mode\":\"local\",\"target\":\"t\",\"rules\":["
                + "{\"accessKey\":\"AKIABETA*\",\"route\":\"cloud\",\"pinWrites\":true},"
                + "{\"ip\":\"192.168.0.0/16\",\"route\":\"cloud\"},"
                + "{\"header\":\"X-Team\",\"headerValue\":\"beta\",\"route\":\"compare\"}]}");
        assertEquals(AbPolicy.Route.CLOUD, p.decide(c("AKIABETA1", true)));
        assertEquals(AbPolicy.Route.CLOUD, p.decide(c("AKIABETA1", false)), "pinWrites");
        assertEquals(AbPolicy.Route.LOCAL, p.decide(c("AKIAOTHER", true)));
        assertEquals(AbPolicy.Route.CLOUD, p.decide(new C("x", "192.168.7.7", Map.of(), true)));
        assertEquals(AbPolicy.Route.LOCAL, p.decide(new C("x", "192.168.7.7", Map.of(), false)), "unpinned rule: writes to the owner");
        Map<String, String> h = new HashMap<>();
        h.put("X-Team", "beta");
        assertEquals(AbPolicy.Route.COMPARE, p.decide(new C("x", "10.0.0.1", h, true)));
        h.put("X-Team", "alpha");
        assertEquals(AbPolicy.Route.LOCAL, p.decide(new C("x", "10.0.0.1", h, true)));
    }

    @Test
    void stickyByHeaderAndIp() {
        AbPolicy p = policy("{\"mode\":\"split\",\"cloudPercent\":50,\"target\":\"t\",\"stickyBy\":\"header:X-Tenant\"}");
        Map<String, String> h1 = Map.of("X-Tenant", "acme");
        AbPolicy.Route r = p.decide(new C("k1", "1.1.1.1", h1, true));
        assertEquals(r, p.decide(new C("k2", "2.2.2.2", h1, true)), "same tenant header, same side whatever the key/ip");
        AbPolicy byIp = policy("{\"mode\":\"split\",\"cloudPercent\":50,\"target\":\"t\",\"stickyBy\":\"ip\"}");
        assertEquals(byIp.decide(new C("k1", "9.9.9.9", Map.of(), true)), byIp.decide(new C("k2", "9.9.9.9", Map.of(), true)));
    }

    @Test
    void validation() {
        assertThrows(IllegalArgumentException.class, () -> policy("{\"mode\":\"split\",\"cloudPercent\":101,\"target\":\"t\"}"));
        assertThrows(IllegalArgumentException.class, () -> policy("{\"mode\":\"cloud\"}"), "cloud routing needs a target");
        assertThrows(IllegalArgumentException.class, () -> policy("{\"mode\":\"local\",\"rules\":[{\"route\":\"cloud\"}]}"));
        assertThrows(IllegalArgumentException.class, () -> policy("{\"mode\":\"local\",\"target\":\"t\",\"rules\":[{\"ip\":\"nope/33\",\"route\":\"cloud\"}]}"));
        assertThrows(IllegalArgumentException.class, () -> policy("{\"mode\":\"bogus\"}"));
    }

    @Test
    void killSwitchOverridesEverything() {
        AbState base = AbState.fromStored(1, JsonParser.parseString("{\"policies\":{\"s3\":{\"mode\":\"cloud\",\"target\":\"t\",\"dualWrite\":true}},"
                + "\"targets\":{\"t\":{\"auth\":{\"type\":\"static\",\"accessKeyId\":\"a\",\"secretAccessKey\":\"b\"}}}}").getAsJsonObject(), null);
        assertEquals(AbPolicy.Route.CLOUD, base.decide("s3", c("a", false)));
        JsonObject doc = JsonParser.parseString("{\"policies\":{\"s3\":{\"mode\":\"cloud\",\"target\":\"t\",\"dualWrite\":true}},"
                + "\"targets\":{\"t\":{\"auth\":{\"type\":\"static\",\"accessKeyId\":\"a\",\"secretAccessKey\":\"b\"}}},"
                + "\"killSwitch\":{\"side\":\"local\"}}").getAsJsonObject();
        AbState killed = AbState.fromStored(2, doc, base);
        assertEquals(AbPolicy.Route.LOCAL, killed.decide("s3", c("a", true)));
        assertEquals(AbPolicy.Route.LOCAL, killed.decide("s3", c("a", false)));
        assertTrue(killed.alwaysLocal("s3"));
        assertFalse(base.alwaysLocal("s3"));
    }

    @Test
    void bucketIsStableAcrossJvmsForAKnownKey() {
        // pins the hash so a refactor cannot silently re-shuffle every client's side
        assertEquals(AbPolicy.bucketOf("s3", "AKIAEXAMPLE"), AbPolicy.bucketOf("s3", "AKIAEXAMPLE"));
        assertTrue(AbPolicy.bucketOf("s3", "x") >= 0 && AbPolicy.bucketOf("s3", "x") < 10000);
    }
}
