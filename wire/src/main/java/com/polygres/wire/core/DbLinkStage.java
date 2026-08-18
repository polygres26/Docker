package com.polygres.wire.core;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal Oracle-style database-link support (ARCHITECTURE.md §10a backlog, now partially
 * built): a single {@code table@link} reference in a statement's SQL text is resolved to a
 * real backend and stripped, reusing the existing {@link BackendRegistry}/{@link BackendTarget}/
 * {@link RoutingBackendExecutor} machinery — the exact same "named backend a statement can be
 * routed to" mechanism {@link RouterStage}'s schema/predicate rules already use, just with the
 * routing signal coming from {@code @linkname} syntax in the SQL text instead of a configured
 * schema prefix or bind-value match.
 *
 * <p>Deliberately narrow, matching the scoped-out plan this implements:
 * <ul>
 *   <li><b>Single table reference only.</b> The first {@code @link} found in the statement wins;
 *   a query naming two different links (which would need a cross-backend join) is not detected
 *   or rejected here — it will pass the first link's name through and leave the second
 *   {@code @link2} token in the SQL text, which the target backend will then reject with its own
 *   syntax error. Real cross-backend join support needs the embedded local query engine
 *   ARCHITECTURE.md §10a already flags as a distinct, much larger, unbuilt component — not
 *   attempted here.</li>
 *   <li><b>No {@code CREATE}/{@code DROP DATABASE LINK} DDL.</b> Links are config-defined via the
 *   existing {@code POLYWIRE_BACKENDS} registry, exactly like every other named backend in this
 *   project (Snowflake/Redshift/BigQuery/Aurora/Databricks/AlloyDB) — a link name is simply a
 *   registered backend name.</li>
 *   <li><b>Unregistered link names fail loudly</b>, not silently: forwarding {@code table@link}
 *   as-is to whatever backend the session happens to be bound to would just produce a confusing
 *   backend-side syntax error (neither Oracle nor Postgres understands {@code @linkname}) — the
 *   same class of bug repeatedly found and fixed elsewhere this session (a misparse silently
 *   producing wrong behavior instead of a clear, immediate rejection).</li>
 * </ul>
 *
 * <p>Runs before {@link RouterStage} in the pipeline (see {@code GatewayComponents}) so a
 * resolved link's {@code targetBackend} is already set by the time {@code RouterStage} runs —
 * that stage's own {@code statement.targetBackend() != null ? ... : resolveBackend()} check
 * already means it leaves an upstream-assigned target alone, no special-casing needed there.
 */
public final class DbLinkStage implements PipelineStage {

    // identifier@identifier — Oracle identifiers: letter, then letters/digits/_/$/#.
    private static final Pattern DB_LINK_REF =
            Pattern.compile("\\b([A-Za-z][\\w$#]*)@([A-Za-z][\\w$#]*)\\b");

    private final BackendRegistry registry;

    public DbLinkStage(BackendRegistry registry) {
        this.registry = registry;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        if (registry.isEmpty()) {
            return next.proceed(statement); // no configured backends -> no possible link target
        }
        String sql = statement.sqlText();
        Matcher matcher = DB_LINK_REF.matcher(sql);
        int searchFrom = 0;
        while (matcher.find(searchFrom)) {
            if (!isInsideStringLiteral(sql, matcher.start())) {
                String linkName = matcher.group(2);
                BackendTarget target = registry.get(linkName);
                if (target == null) {
                    // ORA-02019 is the real Oracle error code for this ("connection description
                    // for remote database not found") — matched here rather than invented, same
                    // as this project's other Oracle-shaped error responses.
                    throw new SQLException("ORA-02019: database link not found: " + linkName);
                }
                // Strip "@linkname" only — the table/view name itself (group 1) stays; neither
                // Oracle nor Postgres understands the "@link" suffix, so it must not reach either.
                String rewritten = sql.substring(0, matcher.start(2) - 1) + sql.substring(matcher.end(2));
                Statement resolved = statement.withSqlText(rewritten).withRouting(statement.workloadClass(), linkName);
                return next.proceed(resolved);
            }
            searchFrom = matcher.end();
        }
        return next.proceed(statement);
    }

    /**
     * True if {@code position} falls inside a single-quoted SQL string literal, tracking {@code
     * ''} as an escaped quote (standard SQL) rather than a literal terminator. A {@code @} inside
     * a string constant (e.g. an email-shaped literal, {@code WHERE email = 'a@b.com'}) must never
     * be mistaken for a database-link reference.
     */
    private static boolean isInsideStringLiteral(String sql, int position) {
        boolean inString = false;
        int i = 0;
        while (i < position) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i += 2; // escaped '' inside a string literal — not a terminator
                    continue;
                }
                inString = !inString;
            }
            i++;
        }
        return inString;
    }
}
