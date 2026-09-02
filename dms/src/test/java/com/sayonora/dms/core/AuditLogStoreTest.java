package com.sayonora.dms.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.license.LicenseTier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Isolated in-memory HSQLDB per test (see {@link AuditLogStore}'s test-only constructor) --
 * never touches the real {@code sayonora-store} file. */
class AuditLogStoreTest {

    @AfterEach
    void resetOverride() {
        DmsLicensingTestSupport.reset();
    }

    private AuditLogStore newStore(String testName) {
        String poolKey = "test-audit-" + testName;
        return new AuditLogStore(poolKey, "jdbc:hsqldb:mem:" + poolKey + ";shutdown=true");
    }

    @Test
    void freeTierRecordIsANoOp() {
        DmsLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        AuditLogStore store = newStore("free");
        store.record("admin", "login.succeeded", "role=ADMIN");
        assertTrue(store.list().isEmpty());
    }

    @Test
    void enterpriseTierPersistsEntries() {
        DmsLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        AuditLogStore store = newStore("enterprise");
        store.record("admin", "login.succeeded", "role=ADMIN");
        store.record("admin", "POST /api/connections", "status=200");

        var entries = store.list();
        assertEquals(2, entries.size());
        assertTrue(entries.stream().anyMatch(e -> e.action().equals("login.succeeded")));
        assertTrue(entries.stream().anyMatch(e -> e.action().equals("POST /api/connections")));
    }
}
