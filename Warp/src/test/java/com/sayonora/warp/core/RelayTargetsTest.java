package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class RelayTargetsTest {

    private final BackendRegistry registry = BackendRegistry.fromConfig("db=jdbc:mysql://p1:3307/x|u|pw||jdbc:mysql://p2:3308/x~10", null);

    @Test
    void withoutABackendTheConfiguredHostIsUsed() {
        assertEquals(new RelayTargets.HostPort("cfg", 1), RelayTargets.resolve(null, "P", registry, "cfg", 1, 3306));
        assertEquals(new RelayTargets.HostPort("cfg", 1), RelayTargets.resolve(" ", "P", registry, "cfg", 1, 3306));
        assertEquals(new RelayTargets.HostPort("cfg", 1), RelayTargets.resolve("db", "P", null, "cfg", 1, 3306));
        assertEquals(new RelayTargets.HostPort("cfg", 1), RelayTargets.resolve("nope", "P", registry, "cfg", 1, 3306), "unknown backend");
    }

    @Test
    void theTargetIsTheBackendsCurrentPrimaryAndFollowsAFailover() {
        assertEquals(new RelayTargets.HostPort("p1", 3307), RelayTargets.resolve("db", "P", registry, "cfg", 1, 3306));
        registry.applyFailoverLocally("db", "jdbc:mysql://p1:3307/x", "jdbc:mysql://p2:3308/x",
                List.of(new ReplicaSpec("jdbc:mysql://p1:3307/x", 10)));
        assertEquals(new RelayTargets.HostPort("p2", 3308), RelayTargets.resolve("db", "P", registry, "cfg", 1, 3306));
    }
}
