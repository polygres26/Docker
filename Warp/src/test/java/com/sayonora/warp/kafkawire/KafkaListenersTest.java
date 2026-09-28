package com.sayonora.warp.kafkawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Which host:port a broker is advertised as, per listener (PLAINTEXT vs SSL). */
class KafkaListenersTest {

    private static final KafkaStore.Broker SELF = new KafkaStore.Broker(1, "warp-a", 19092);
    private static final KafkaStore.Broker PEER = new KafkaStore.Broker(2, "warp-b", 19092);

    @Test
    void thisNodeIsAdvertisedOnItsSslHostAndPortToSslClients() {
        KafkaStore.Broker b = KafkaListeners.toTls(SELF, "warp-a", 19092, "warp-a.tls.example", 19093);
        assertEquals(new KafkaStore.Broker(1, "warp-a.tls.example", 19093), b);
    }

    @Test
    void peersKeepTheirHostAndShiftByTheSameSslOffset() {
        assertEquals(new KafkaStore.Broker(2, "warp-b", 19093), KafkaListeners.toTls(PEER, "warp-a", 19092, "warp-a", 19093));
        assertEquals(new KafkaStore.Broker(2, "warp-b", 29092 + 1000),
                KafkaListeners.toTls(new KafkaStore.Broker(2, "warp-b", 29092), "warp-a", 19092, "warp-a", 20092));
    }

    @Test
    void withoutASslListenerBrokersAreUnchanged() {
        assertSame(SELF, KafkaListeners.toTls(SELF, "warp-a", 19092, "warp-a", -1));
        assertSame(PEER, KafkaListeners.toTls(PEER, "warp-a", 19092, "warp-a", 0));
    }

    @Test
    void listVariantKeepsOrderAndIds() {
        List<KafkaStore.Broker> out = KafkaListeners.toTls(List.of(SELF, PEER), "warp-a", 19092, "warp-a", 19093);
        assertEquals(List.of(1, 2), out.stream().map(KafkaStore.Broker::id).toList());
        assertTrue(out.stream().allMatch(b -> b.port() == 19093));
    }

    @Test
    void theListenerFlagIsPerThread() throws Exception {
        KafkaListeners.setTls(true);
        assertTrue(KafkaListeners.onTls());
        boolean[] other = {true};
        Thread t = new Thread(() -> other[0] = KafkaListeners.onTls());
        t.start();
        t.join();
        assertFalse(other[0], "a different connection thread must default to the plaintext view");
        KafkaListeners.setTls(false);
    }
}
