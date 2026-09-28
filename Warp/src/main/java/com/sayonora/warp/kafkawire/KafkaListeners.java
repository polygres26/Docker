package com.sayonora.warp.kafkawire;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-listener advertised addresses. Kafka clients bootstrap on one listener and then connect to whatever host:port the
 * Metadata / FindCoordinator / DescribeCluster responses advertise, so a client that connected to the SSL listener
 * ({@code security.protocol=SSL}) must be sent the SSL address of every broker, never the PLAINTEXT one (or it would
 * silently downgrade, or fail its handshake against the plaintext port).
 *
 * <p>Brokers register a single host:port in the store (their PLAINTEXT address). The SSL address of a broker is derived as
 * the same host with the port shifted by (SSL advertised port - PLAINTEXT advertised port), which is exact for this
 * node and holds for cluster peers when every node uses the same port layout (the documented deployment shape). This node
 * can also advertise a distinct SSL host ({@code WARP_KAFKAWIRE_TLS_ADVERTISED_HOST}).
 */
final class KafkaListeners {

    private static final ThreadLocal<Boolean> ON_TLS = ThreadLocal.withInitial(() -> false);

    private KafkaListeners() {
    }

    /** Marks the request being handled on this thread as arriving on the SSL listener (or not). */
    static void setTls(boolean tls) {
        ON_TLS.set(tls);
    }

    static boolean onTls() {
        return ON_TLS.get();
    }

    /**
     * @param b a registered broker (PLAINTEXT address)
     * @param selfHost this node's advertised PLAINTEXT host; @param selfPlainPort its advertised PLAINTEXT port
     * @param tlsHost this node's advertised SSL host; @param tlsPort its advertised SSL port (<= 0: no SSL listener)
     * @return the broker as seen through the SSL listener
     */
    static KafkaStore.Broker toTls(KafkaStore.Broker b, String selfHost, int selfPlainPort, String tlsHost, int tlsPort) {
        if (tlsPort <= 0) {
            return b;
        }
        if (b.port() == selfPlainPort && b.host().equals(selfHost)) {
            return new KafkaStore.Broker(b.id(), tlsHost, tlsPort);
        }
        return new KafkaStore.Broker(b.id(), b.host(), b.port() + (tlsPort - selfPlainPort));
    }

    static List<KafkaStore.Broker> toTls(List<KafkaStore.Broker> bs, String selfHost, int selfPlainPort, String tlsHost,
            int tlsPort) {
        List<KafkaStore.Broker> out = new ArrayList<>(bs.size());
        for (KafkaStore.Broker b : bs) {
            out.add(toTls(b, selfHost, selfPlainPort, tlsHost, tlsPort));
        }
        return out;
    }
}
