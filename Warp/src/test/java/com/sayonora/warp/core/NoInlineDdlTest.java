package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Project rule: DDL lives in {@code src/main/resources/ddl/<engine>/<name>.sql} and is loaded through {@link DdlTemplates}, not written as
 * Java string literals. This fails when a main-code file outside the list below puts {@code CREATE TABLE}, {@code CREATE INDEX},
 * {@code CREATE ... FUNCTION/TRIGGER} or {@code ALTER TABLE ... ADD} in a string literal. The list is the work that is still to be moved
 * (static store schemas) or that builds DDL from a runtime name rather than being a fixed schema; shrink it, never grow it.
 */
class NoInlineDdlTest {

    private static final Pattern INLINE_DDL = Pattern.compile(
            "\"[^\"]*(CREATE (TABLE|INDEX|UNIQUE INDEX|OR REPLACE FUNCTION|TRIGGER)|ALTER TABLE [A-Za-z_]+ ADD)");

    /** Still inline: static store schemas not yet moved. */
    private static final Set<String> STORES_TO_MOVE = Set.of("dynamowire/PgItemStore.java", "influxwire/PgTimeSeriesStore.java",
            "mongowire/PostgresDocumentStore.java", "oswire/PostgresSearchStore.java");

    /** DDL built from a runtime name or a user's statement, or SQL text that is only matched, translated or printed. */
    private static final Set<String> DYNAMIC_OR_NOT_EXECUTED = Set.of("cluster/CacheTriggerInstaller.java", "rollup/RollupDefinition.java",
            "cqlwire/DescribeStmt.java", "core/DialectTranslations.java", "orawire/session/RequestLoop.java",
            "orawire/ttc/ExecuteRequestReader.java");

    @Test
    void noNewInlineDdlInMainCode() throws IOException {
        Path root = Path.of("src/main/java/com/sayonora/warp");
        Set<String> offenders = new TreeSet<>();
        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                String rel = root.relativize(f).toString();
                if (STORES_TO_MOVE.contains(rel) || DYNAMIC_OR_NOT_EXECUTED.contains(rel) || rel.equals("core/DdlTemplates.java")) {
                    continue;
                }
                for (String line : Files.readAllLines(f)) {
                    String t = line.stripLeading();
                    if (!t.startsWith("//") && !t.startsWith("*") && !t.startsWith("/*") && INLINE_DDL.matcher(line).find()) {
                        offenders.add(rel);
                    }
                }
            }
        }
        assertEquals(Set.of(), offenders, "inline DDL belongs in src/main/resources/ddl/<engine>/<name>.sql (see DdlTemplates)");
    }

    @Test
    void everyControlPlaneDdlFileExistsAndHasStatements() {
        for (String name : List.of("warp_config", "warp_nodes", "warp_failover", "warp_firewall_rules", "warp_ab_routing",
                "warp_translation_cache", "warp_failed_statements", "warp_xa_log", "warp_acme_state", "warp_audit_log", "warp_kv_config",
                "warp_enabled_stores")) {
            List<String> statements = DdlTemplates.loadStatements("postgres", name, Map.of("channel", "c", "table", "t"));
            assertNotNull(statements, "ddl/postgres/" + name + ".sql");
            assertTrue(!statements.isEmpty(), name);
            statements.forEach(s -> assertTrue(!s.contains("${"), "unsubstituted placeholder in " + name + ": " + s));
        }
        // the two that take any JDBC URL have a MySQL file too, and Oracle / SQL Server do not claim one
        assertNotNull(DdlTemplates.loadStatements("mysql", "warp_audit_log", Map.of()));
        assertNotNull(DdlTemplates.loadStatements("mysql", "warp_kv_config", Map.of()));
        assertEquals(null, DdlTemplates.loadStatements("oracle", "warp_audit_log", Map.of()));
    }
}
