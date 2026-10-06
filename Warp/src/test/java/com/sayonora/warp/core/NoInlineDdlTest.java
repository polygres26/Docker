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
 * Java string literals. This fails when a main-code file outside the list below puts {@code CREATE TABLE/INDEX/SCHEMA/SEQUENCE/FUNCTION/TRIGGER},
 * {@code DROP TABLE/INDEX/SCHEMA/SEQUENCE/TRIGGER} or {@code ALTER TABLE} in a string literal. Statements whose identifiers are only known at
 * run time are still files: the identifiers are {@code ${placeholders}} (see {@code ddl/postgres/mongowire_collection.sql}). The list below is
 * code that assembles DDL from a runtime shape that a template cannot express, or only matches / prints SQL text; shrink it, never grow it.
 */
class NoInlineDdlTest {

    private static final Pattern INLINE_DDL = Pattern.compile(
            "\"[^\"]*(CREATE (TABLE|INDEX|UNIQUE INDEX|SCHEMA|SEQUENCE|OR REPLACE FUNCTION|TRIGGER)|DROP (TABLE|INDEX|SCHEMA|SEQUENCE|TRIGGER)|ALTER TABLE)");

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
                if (DYNAMIC_OR_NOT_EXECUTED.contains(rel) || rel.equals("core/DdlTemplates.java")) {
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

    /** A mistyped file name would otherwise only fail when that code path first runs. */
    @Test
    void everyDdlFileNamedInMainCodeExists() throws IOException {
        Pattern ref = Pattern.compile("DdlTemplates\\.(?:run|loadStatements)\\((?:\\w+,\\s*)?\"(postgres|mysql|oracle|sqlserver)\",\\s*\"([a-z0-9_]+)\"");
        Path root = Path.of("src/main/java/com/sayonora/warp");
        Set<String> missing = new TreeSet<>();
        int seen = 0;
        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                var m = ref.matcher(Files.readString(f));
                while (m.find()) {
                    seen++;
                    if (!Files.exists(Path.of("src/main/resources/ddl", m.group(1), m.group(2) + ".sql"))) {
                        missing.add(m.group(1) + "/" + m.group(2) + ".sql (named in " + root.relativize(f) + ")");
                    }
                }
            }
        }
        assertTrue(seen > 20, "the scan found only " + seen + " references: the pattern no longer matches how DdlTemplates is called");
        assertEquals(Set.of(), missing);
    }
}
