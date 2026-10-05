package com.sayonora.warp.kafkawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

class KafkaStoreErrorTest {

    @Test
    void aStoreFailureIsReportedWithACodeTheKafkaClientRetries() {
        KafkaError e = KafkaStore.storage(new SQLException("terminating connection due to administrator command"));
        assertEquals(KafkaError.LEADER_NOT_AVAILABLE, e.code);
        assertTrue(KafkaError.isStore(e));
        assertTrue(Errors.forCode((short) e.code).exception() instanceof RetriableException,
                "a default producer must retry this instead of failing the send");
        assertTrue(e.getMessage().contains("currently unavailable"));
    }

    @Test
    void anOrdinaryErrorIsNotAStoreError() {
        assertFalse(KafkaError.isStore(new KafkaError(KafkaError.UNKNOWN_SERVER_ERROR, "x")));
        assertFalse(KafkaError.isStore(new RuntimeException("x")));
    }
}
