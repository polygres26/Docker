package com.sayonora.warp.core;

import com.sayonora.warp.testsupport.BrownoutHarness;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * The protocols whose data lives in the Postgres backend (DynamoDB, SQS, MongoDB, ... see StoreType), under a planned
 * switchover and a crash of the primary. Measurement, not pass/fail: the report goes to
 * {@code target/store-brownout-report.md}. Opt-in: WARP_TEST_BROWNOUT_PG_BIN. WARP_TEST_STORE_WORKLOADS=dynamowire,sqswire and
 * WARP_TEST_BROWNOUT_SCENARIOS=switchover narrow it.
 */
class StoreFailoverBrownoutLiveTest {

    @Test
    void measureBrownoutForTheStoreBackedProtocols() throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && new File(bin, "initdb").exists());
        String only = System.getenv("WARP_TEST_STORE_WORKLOADS");
        String scen = System.getenv("WARP_TEST_BROWNOUT_SCENARIOS");
        String[] scenarios = scen == null ? new String[] {"switchover", "failover"} : scen.split(",");
        Path base = Files.createTempDirectory("storebrownout");
        List<BrownoutHarness.Result> results = new ArrayList<>();
        StringBuilder md = new StringBuilder("# Store-backed protocols under switchover and failover\n\n" + BrownoutHarness.REPORT_HEADER);
        try {
            for (var w : StoreWorkloads.all()) {
                if (only != null && !List.of(only.split(",")).contains(w.name())) {
                    continue;
                }
                for (String scenario : scenarios) {
                    var r = BrownoutHarness.run(w, scenario, bin, base);
                    results.add(r);
                    md.append(BrownoutHarness.reportRow(r));
                    System.out.println("STORE-RESULT " + BrownoutHarness.reportRow(r));
                }
            }
        } finally {
            md.append("\n## Errors seen\n\n");
            for (var r : results) {
                md.append("- ").append(r.protocol()).append(" / ").append(r.scenario()).append(" (").append(r.eventNote()).append(")\n");
                r.errors().forEach((k, v) -> md.append("    - ").append(v).append(" x ").append(k).append('\n'));
            }
            Files.createDirectories(Path.of("target"));
            Files.writeString(Path.of("target/store-brownout-report.md"), md.toString());
        }
    }
}
