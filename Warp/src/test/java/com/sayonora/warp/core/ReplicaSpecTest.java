package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ReplicaSpecTest {

    @Test
    void blankIsNoReplicas() {
        assertTrue(ReplicaSpec.parseList(null).isEmpty());
        assertTrue(ReplicaSpec.parseList("  ").isEmpty());
        assertEquals("", ReplicaSpec.format(List.of()));
    }

    @Test
    void parsesUrlsWithAndWithoutLag() {
        List<ReplicaSpec> l = ReplicaSpec.parseList("jdbc:postgresql://r1:5432/db~2^jdbc:postgresql://r2/db");
        assertEquals(2, l.size());
        assertEquals("jdbc:postgresql://r1:5432/db", l.get(0).url());
        assertEquals(2.0, l.get(0).maxLagSeconds());
        assertEquals(ReplicaSpec.DEFAULT_MAX_LAG_SECONDS, l.get(1).maxLagSeconds());
    }

    @Test
    void oracleThinUrlsWithAtSignAndSemicolonsSurvive() {
        List<ReplicaSpec> l = ReplicaSpec.parseList("jdbc:oracle:thin:@//h:1521/svc~7.5");
        assertEquals("jdbc:oracle:thin:@//h:1521/svc", l.get(0).url());
        assertEquals(7.5, l.get(0).maxLagSeconds());
        List<ReplicaSpec> sc = ReplicaSpec.parseList("jdbc:sqlserver://h:1433%3BdatabaseName=d~3");
        assertEquals("jdbc:sqlserver://h:1433;databaseName=d", sc.get(0).url());
    }

    @Test
    void formatRoundTrips() {
        String in = "jdbc:postgresql://r1/db~2^jdbc:sqlserver://h:1433%3BdatabaseName=d";
        assertEquals(in, ReplicaSpec.format(ReplicaSpec.parseList(in)));
    }

    @Test
    void badLagIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> ReplicaSpec.parseList("jdbc:postgresql://r1/db~abc"));
        assertThrows(IllegalArgumentException.class, () -> new ReplicaSpec("jdbc:postgresql://r1/db", -1));
    }
}
