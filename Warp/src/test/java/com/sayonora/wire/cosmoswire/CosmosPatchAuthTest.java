package com.sayonora.wire.cosmoswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Base64;
import org.junit.jupiter.api.Test;

/** Patch semantics (Cosmos "Partial document update" docs) and the master-key signature (REST "Access control" docs). */
class CosmosPatchAuthTest {

    private static String patch(String doc, String ops) {
        return CosmosPatch.apply(JsonParser.parseString(doc).getAsJsonObject(), JsonParser.parseString(ops).getAsJsonArray()).toString();
    }

    @Test
    void patchOperations() {
        assertEquals("{\"a\":1,\"b\":2}", patch("{\"a\":1}", "[{\"op\":\"add\",\"path\":\"/b\",\"value\":2}]"));
        assertEquals("{\"t\":[1,9,2]}", patch("{\"t\":[1,2]}", "[{\"op\":\"add\",\"path\":\"/t/1\",\"value\":9}]"));
        assertEquals("{\"t\":[1,2,3]}", patch("{\"t\":[1,2]}", "[{\"op\":\"add\",\"path\":\"/t/-\",\"value\":3}]"));
        assertEquals("{\"t\":[1,9]}", patch("{\"t\":[1,2]}", "[{\"op\":\"set\",\"path\":\"/t/1\",\"value\":9}]"));
        assertEquals("{\"a\":5}", patch("{\"a\":1}", "[{\"op\":\"replace\",\"path\":\"/a\",\"value\":5}]"));
        assertEquals("{}", patch("{\"a\":1}", "[{\"op\":\"remove\",\"path\":\"/a\"}]"));
        assertEquals("{\"n\":7,\"m\":2}", patch("{\"n\":5}", "[{\"op\":\"incr\",\"path\":\"/n\",\"value\":2},{\"op\":\"incr\",\"path\":\"/m\",\"value\":2}]"));
        assertEquals("{\"b\":1}", patch("{\"a\":1}", "[{\"op\":\"move\",\"from\":\"/a\",\"path\":\"/b\"}]"));
        assertThrows(CosmosException.class, () -> patch("{\"a\":1}", "[{\"op\":\"replace\",\"path\":\"/zz\",\"value\":1}]"));
        assertThrows(CosmosException.class, () -> patch("{\"a\":1}", "[{\"op\":\"remove\",\"path\":\"/zz\"}]"));
        JsonArray many = new JsonArray();
        for (int i = 0; i < 11; i++) {
            JsonObject o = new JsonObject();
            o.addProperty("op", "set");
            o.addProperty("path", "/a" + i);
            o.addProperty("value", i);
            many.add(o);
        }
        assertThrows(CosmosException.class, () -> CosmosPatch.apply(new JsonObject(), many));
    }

    @Test
    void masterKeySignatureKnownAnswer() {
        // Independent computation (python hmac/hashlib) of the documented payload
        //   "get\ndbs\n\nthu, 27 apr 2017 00:51:12 gmt\n\n" for the well-known emulator key:
        byte[] key = Base64.getDecoder().decode(CosmosConfig.EMULATOR_KEY);
        assertEquals("Bk4MqbjRdQImb4Rqp5pmqv1/OhkMQU93qlTmk/SzVRQ=", CosmosAuth.sign(key, "GET", "dbs", "", "Thu, 27 Apr 2017 00:51:12 GMT"));
        assertEquals(CosmosAuth.sign(key, "get", "DBS", "", "thu, 27 apr 2017 00:51:12 gmt"), CosmosAuth.sign(key, "GET", "dbs", "", "Thu, 27 Apr 2017 00:51:12 GMT"));
    }

    @Test
    void resourceLinks() {
        assertEquals("dbs|", String.join("|", CosmosAuth.resourceOf("/dbs")));
        assertEquals("docs|dbs/d/colls/c", String.join("|", CosmosAuth.resourceOf("/dbs/d/colls/c/docs")));
        assertEquals("docs|dbs/d/colls/c/docs/x", String.join("|", CosmosAuth.resourceOf("/dbs/d/colls/c/docs/x")));
        assertEquals("|", String.join("|", CosmosAuth.resourceOf("/")));
    }
}
