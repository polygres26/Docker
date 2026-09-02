package com.sayonora.migration.cli;

import com.sayonora.migration.readiness.CutoverReadinessChecker;
import com.sayonora.migration.readiness.CutoverReadinessReport;

/**
 * Standalone entry point for Phase 5's cutover-readiness check -- prints a human-readable report
 * to stdout and exits {@code 0} when every gate passed, {@code 1} otherwise, so this can be
 * dropped straight into a cutover runbook or a CI/deploy-pipeline gate ({@code
 * cutover-readiness.sh && switch-connection-string.sh}) without needing to parse anything.
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code CHECKPOINT_JDBC_URL}/{@code _USER}/{@code _PASSWORD} -- the TARGET Postgres, same
 *       connection every {@code Migrate*Cli} points its {@code CdcCheckpointStore} at</li>
 *   <li>{@code MIGRATION_SOURCE_KEY} -- the source key to check (e.g. {@code "mongo:mydb.orders"})</li>
 * </ul>
 *
 * <p>Optional:
 * <ul>
 *   <li>{@code MIGRATION_HAS_LIVE_CHANGE_FEED} -- {@code "true"}/{@code "false"}, default {@code
 *       "true"}; set {@code "false"} for a snapshot-only source (e.g. Neo4j) so the lag gate is
 *       skipped instead of failing a source that was never going to have a live tail</li>
 *   <li>{@code MIGRATION_MAX_LAG_SECONDS} -- default {@code 10}</li>
 * </ul>
 *
 * <p>Deliberately does not accept a {@link com.sayonora.migration.verify.VerificationResult} on
 * the command line -- that comes from a real, source-specific count+checksum pass (see {@code
 * CutoverReadinessChecker}'s own javadoc), which is a separate, per-connector verification run,
 * not something a generic CLI can compute. Wire that result in programmatically via {@link
 * CutoverReadinessChecker#check(String, boolean, long, com.sayonora.migration.verify.VerificationResult)}
 * from whatever job already ran that comparison, rather than through this CLI.
 */
public final class CutoverReadinessCli {

    private CutoverReadinessCli() {
    }

    public static void main(String[] args) throws Exception {
        String jdbcUrl = require("CHECKPOINT_JDBC_URL");
        String user = require("CHECKPOINT_JDBC_USER");
        String password = require("CHECKPOINT_JDBC_PASSWORD");
        String sourceKey = require("MIGRATION_SOURCE_KEY");
        boolean hasLiveChangeFeed = Boolean.parseBoolean(
                System.getenv().getOrDefault("MIGRATION_HAS_LIVE_CHANGE_FEED", "true"));
        long maxLagSeconds = Long.parseLong(System.getenv().getOrDefault("MIGRATION_MAX_LAG_SECONDS", "10"));

        CutoverReadinessChecker checker = new CutoverReadinessChecker(jdbcUrl, user, password);
        CutoverReadinessReport report = checker.check(sourceKey, hasLiveChangeFeed, maxLagSeconds);
        System.out.print(report.render());
        System.exit(report.ready() ? 0 : 1);
    }

    private static String require(String envVar) {
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("missing required environment variable: " + envVar);
        }
        return value;
    }
}
