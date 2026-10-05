package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.core.BackendSetModel.ModelException;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Replicas survive the admin API's spec round trip (model -> WARP_BACKENDS string -> model). */
class BackendSetModelReplicasTest {

    private static WarpConfig cfg(String backends) {
        return WarpConfig.fromEnvDefaults().withBackendModel(backends, null, null, null, null, null);
    }

    @Test
    void replicasRoundTripThroughTheSpec() {
        String spec = "pg=jdbc:postgresql://p/db|u|pw||jdbc:postgresql://r1/db~2^jdbc:postgresql://r2/db";
        BackendSetModel m = BackendSetModel.from(cfg(spec), null);
        assertEquals(2, m.backend("pg").replicaSpecs().size());
        WarpConfig out = m.applyTo(cfg(spec));
        assertEquals(spec, out.backends());
    }

    @Test
    void replicasWithoutAFallbackKeepTheEmptyFallbackSlot() {
        BackendSetModel m = BackendSetModel.from(cfg("pg=jdbc:postgresql://p/db"), null);
        m.addBackend("default", "pg2", "jdbc:postgresql://p2/db", "u", "pw", null, null, List.of(),
                "jdbc:postgresql://r/db~3");
        String spec = m.applyTo(cfg("pg=jdbc:postgresql://p/db")).backends();
        assertTrue(spec.contains("pg2=jdbc:postgresql://p2/db|u|pw||jdbc:postgresql://r/db~3"), spec);
        // and the registry parses exactly that string back to a replica with lag 3
        BackendRegistry reg = BackendRegistry.fromConfig(spec, null);
        assertEquals(3.0, reg.replicaSpecsOf("pg2").get(0).maxLagSeconds());
        assertEquals(0, reg.replicaSpecsOf("pg").size());
    }

    @Test
    void patchCanReplaceKeepAndClearReplicas() {
        BackendSetModel m = BackendSetModel.from(cfg("pg=jdbc:postgresql://p/db|u|pw||jdbc:postgresql://r1/db"), null);
        m.patchBackend("pg", null, null, null, false, null, null);
        assertEquals(1, m.backend("pg").replicaSpecs().size(), "null leaves replicas unchanged");
        m.patchBackend("pg", null, null, null, false, null, null, "jdbc:postgresql://a/db^jdbc:postgresql://b/db~9");
        assertEquals(2, m.backend("pg").replicaSpecs().size());
        m.patchBackend("pg", null, null, null, false, null, null, "");
        assertEquals(0, m.backend("pg").replicaSpecs().size());
        assertNull(m.backend("pg").replicas());
    }

    @Test
    void malformedReplicasAreRejectedWithA400() {
        BackendSetModel m = BackendSetModel.from(cfg("pg=jdbc:postgresql://p/db"), null);
        ModelException e = assertThrows(ModelException.class, () -> m.addBackend("default", "x",
                "jdbc:postgresql://x/db", "u", "p", null, null, List.of(), "jdbc:postgresql://r/db~fast"));
        assertEquals(400, e.status());
    }
}
