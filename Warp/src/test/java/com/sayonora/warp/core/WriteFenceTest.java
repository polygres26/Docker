package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class WriteFenceTest {

    private final AtomicLong now = new AtomicLong(1_000);
    private final AtomicLong latest = new AtomicLong(5);
    private final AtomicLong applied = new AtomicLong(5);
    private final AtomicBoolean reachable = new AtomicBoolean(true);
    private final AtomicBoolean catchUpWorks = new AtomicBoolean(true);

    private WriteFence fence() {
        return new WriteFence(new WriteFence.Source() {
            public long latestVersion() throws Exception {
                if (!reachable.get()) {
                    throw new SQLException("down");
                }
                return latest.get();
            }

            public long appliedVersion() {
                return applied.get();
            }

            public void catchUp() {
                if (catchUpWorks.get()) {
                    applied.set(latest.get());
                }
            }
        }, 100, 1000, now::get);
    }

    @Test
    void currentInstanceWrites() {
        WriteFence f = fence();
        f.refresh();
        assertDoesNotThrow(f::check);
    }

    @Test
    void behindInstanceCatchesUpInlineThenWrites() {
        WriteFence f = fence();
        latest.set(6);
        catchUpWorks.set(false);
        f.refresh();
        catchUpWorks.set(true);
        assertDoesNotThrow(f::check);
    }

    @Test
    void behindInstanceThatCannotCatchUpIsRefused() {
        WriteFence f = fence();
        latest.set(6);
        catchUpWorks.set(false);
        f.refresh();
        SQLException e = assertThrows(SQLException.class, f::check);
        org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains("6"), e.getMessage());
    }

    @Test
    void unreachableConfigDatabaseRefusesOnlyAfterTheStalenessLimit() {
        WriteFence f = fence();
        f.refresh();
        reachable.set(false);
        now.addAndGet(900);
        f.refresh();
        assertDoesNotThrow(f::check);
        now.addAndGet(200);
        f.refresh();
        assertThrows(SQLException.class, f::check);
        reachable.set(true);
        f.refresh();
        assertDoesNotThrow(f::check);
    }
}
