package com.sayonora.wire.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Real per-backend-engine DDL, loaded from {@code src/main/resources/ddl/&lt;engine&gt;/&lt;name&gt;.sql}
 * instead of hardcoded as Java string literals in each store's own class -- {@code PgItemStore}
 * (dynamowire), {@code PgTimeSeriesStore} (influxwire), {@code PgQueueStore} (sqswire), and {@code
 * PgGraphStore} (the Bolt/Cypher graph frontend) each used to build their own `CREATE TABLE`/
 * `CREATE INDEX` text inline in Java, which meant a real per-engine variant (see {@link
 * BackendDriverRegistry}'s own currently-supported engine list) had nowhere to live but a growing
 * pile of if/else branches inside otherwise storage-logic-only methods.
 *
 * <p>One `.sql` file per (engine, logical DDL name) pair. A file can hold several statements,
 * each preceded by its own {@code -- ### <label>} marker line (the label is purely documentation
 * for a human reading the file -- {@link #loadStatements} only ever cares about the marker itself
 * as a real statement separator, not its text); every marked statement runs in the order it
 * appears in the file. `${var}` placeholders (today: always just `${table}`, the one thing every
 * one of these DDL files needs to parameterize) are substituted before execution.
 *
 * <p><b>Real, disclosed scope</b>: {@code dynamowire_item_table}, {@code
 * influxwire_measurement_table}, and now {@code sqswire_catalog}/{@code sqswire_queue_table} all
 * have real Oracle/SQL Server/MySQL DDL variants -- {@code sqswire}'s own QUERY code (the claim/
 * upsert/count logic) has real per-engine support too, in {@code
 * com.sayonora.wire.sqswire.SqswireDialect}. The Bolt/Cypher graph frontend is the one store still
 * genuinely Postgres-only: its {@code labels TEXT[]} array column has no cross-engine equivalent
 * at all (see {@code ddl/postgres/boltwire_graph_schema.sql}'s own comment) -- a real schema
 * redesign, not a query-portability problem the way sqswire's own gap was. oswire never adopted
 * this class at all -- its own {@code PostgresSearchStore} still builds DDL inline, and (unlike
 * boltwire) the gap runs through its query logic too, not just its schema; see that class's own
 * javadoc for the real reason (Postgres-only JSONB operators throughout, not a portable schema
 * with an engine-specific query layer on top).
 *
 * <p><b>Now verified against real containers -- and real bugs found doing it, the same way this
 * project's own established discipline predicts.</b> {@code DynamowireNonPostgresBackendIntegrationTest}
 * and {@code SqsNonPostgresBackendIntegrationTest} both prove real Oracle/MySQL/SQL Server
 * end to end. sqswire's own {@code SqswireDialect} passed unmodified on the first real run against
 * all three -- its own "live-measured RTT" design claim holds up. dynamowire did not: {@code
 * PgItemStore#ensureCatalog} had hardcoded {@code "postgres"} regardless of the real target engine
 * (a real {@code ER_BLOB_KEY_WITHOUT_LENGTH} against MySQL the first time it ran for real), the
 * new {@code dynamowire_catalog.sql} files needed their own real per-engine idempotency idiom
 * (Oracle has no {@code CREATE TABLE IF NOT EXISTS} at all, on any version -- correcting this
 * project's own earlier, untested assumption that 23c added it), {@code _dynamo_tables} itself
 * needed an Oracle-specific rename (a real {@code ORA-00911}: Oracle rejects an unquoted
 * leading-underscore identifier outright -- see {@code PgItemStore#catalogTableName}), and
 * {@code PutItem}/{@code UpdateItem}'s own upsert SQL was Postgres-only {@code INSERT ... ON
 * CONFLICT} with no per-engine dispatch at all until {@code PgItemStoreDialect} added one
 * (mirroring {@code SqswireDialect}'s own pattern) -- a real, previously-undiscovered gap that
 * made the "real Oracle/SQL Server/MySQL DDL variants" claim above true for schema only, not for
 * actually writing to a table through those schemas. influxwire's own DDL is real and now
 * confirmed to at least create correctly, but {@code PgTimeSeriesStore#select}'s query-building
 * logic is a separate, much deeper Postgres-only gap (real Postgres 14+ {@code date_bin()}, JSONB
 * extraction operators) -- disclosed on that class's own javadoc, not yet fixed.
 */
public final class DdlTemplates {

    private static final Pattern MARKER = Pattern.compile("(?m)^--\\s*###.*$");

    private DdlTemplates() {
    }

    /** @return the {@code ddl/<engine>/} directory name for {@code jdbcUrl}'s own real engine --
     *     mirrors {@link BackendDriverRegistry#driverClassNameFor}'s own URL-prefix dispatch
     *     (same real engine set, same ordering), kept as its own lookup since a DDL directory name
     *     and a JDBC driver class name are conceptually different things that happen to be decided
     *     by the same URL prefix today. {@code null} for an unrecognized prefix -- callers decide
     *     what "no DDL variant for this engine" means for them (usually: fail with a real, clear
     *     error, the same {@link BackendDriverRegistry} pattern). */
    public static String engineDirFor(String jdbcUrl) {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(java.util.Locale.ROOT);
        if (url.startsWith("jdbc:postgresql:")) {
            return "postgres";
        }
        if (url.startsWith("jdbc:oracle:")) {
            return "oracle";
        }
        if (url.startsWith("jdbc:sqlserver:")) {
            return "sqlserver";
        }
        if (url.startsWith("jdbc:mysql:") || url.startsWith("jdbc:mariadb:")) {
            return "mysql";
        }
        return null;
    }

    /** @return every {@code -- ### <label>}-delimited statement in {@code ddl/<engine>/<name>.sql},
     *     in file order, with every {@code ${key}} in {@code vars} substituted -- or {@code null}
     *     if that exact (engine, name) file doesn't exist (a real, honest "no DDL for this engine
     *     yet", not a fabricated fallback to a different engine's own SQL dialect). */
    public static List<String> loadStatements(String engine, String name, Map<String, String> vars) {
        String path = "/ddl/" + engine + "/" + name + ".sql";
        String raw;
        try (InputStream is = DdlTemplates.class.getResourceAsStream(path)) {
            if (is == null) {
                return null;
            }
            raw = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read DDL template " + path, e);
        }
        // MARKER.split gives every chunk BETWEEN markers, in file order, with element 0 being
        // whatever precedes the FIRST marker (this file's own leading comment block) -- skipped.
        String[] chunks = MARKER.split(raw);
        List<String> statements = new ArrayList<>();
        for (int i = 1; i < chunks.length; i++) {
            String statement = substitute(chunks[i].strip(), vars);
            if (!statement.isEmpty()) {
                statements.add(statement);
            }
        }
        return statements;
    }

    private static String substitute(String text, Map<String, String> vars) {
        String out = text;
        for (Map.Entry<String, String> entry : vars.entrySet()) {
            out = out.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return out;
    }
}
