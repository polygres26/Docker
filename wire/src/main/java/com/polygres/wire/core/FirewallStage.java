package com.polygres.wire.core;

import com.polygres.wire.core.access.SqlTableReferences;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ARCHITECTURE.md §5.7: an ordered allow/deny rule list, evaluated first in the pipeline so a
 * rejected statement never reaches translation, routing, or a backend. One built-in heuristic (not
 * a rule) rejects classic stacked-query injection (a bind-unrelated literal SQL statement appended
 * after a {@code ;}) regardless of rule configuration -- a narrow, cheap check, not a general
 * injection detector.
 *
 * <p><b>Rule shape, deliberately not raw regex-only</b>: each {@link Rule} matches on a
 * {@code statementType} (SELECT/INSERT/UPDATE/DELETE/DROP/TRUNCATE/ALTER/CREATE/GRANT/REVOKE/...,
 * or {@code null}/{@code ANY} for every statement) and/or a {@code tablePattern} (matched against
 * the statement's real referenced tables via {@link SqlTableReferences}, not the raw SQL text --
 * same reasoning {@code AccessControlStage} already gives for using that extractor: narrower and
 * less prone to a rule accidentally matching an unrelated substring than full-text search). This
 * is the "simple and intuitive to set up" surface -- a DBA managing {@code
 * com.polygres.wire.config.FirewallRuleStore}'s backing table writes {@code deny DROP} or
 * {@code deny DELETE on public.orders} shaped rows, not a regex. A raw {@code sqlPattern} regex is
 * still available as an escape hatch for the rare case neither dimension expresses what's needed.
 *
 * <p>Rules are evaluated in {@code priority} order (lower first, ties broken by {@code id}); first
 * enabled rule that matches wins. Default action when nothing matches is {@code ALLOW}, unchanged
 * semantics from before this rule shape existed.
 */
public final class FirewallStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(FirewallStage.class);

    // "; <keyword>" after the first statement — the classic stacked-query pattern.
    // Doesn't flag a trailing lone ";" (many clients send one) or ";" inside a string literal is a
    // known gap — a narrow heuristic, not a parser.
    private static final Pattern STACKED_QUERY = Pattern.compile(
            ";\\s*(select|insert|update|delete|drop|alter|create|grant|exec)\\b", Pattern.CASE_INSENSITIVE);

    private static final Pattern LEADING_KEYWORD = Pattern.compile("^\\s*(\\w+)");
    // DDL/DML shapes SqlTableReferences' FROM/JOIN extraction alone doesn't cover -- a DROP/
    // TRUNCATE/ALTER TABLE names its target after the TABLE keyword, not FROM/JOIN, and INSERT
    // names its target after INTO.
    private static final Pattern TABLE_KEYWORD_TARGET = Pattern.compile(
            "\\b(?:TABLE|INTO)\\s+([a-zA-Z_][\\w$]*(?:\\.[a-zA-Z_][\\w$]*)*)", Pattern.CASE_INSENSITIVE);
    // Bare UPDATE <table> SET ... -- UPDATE's own target isn't preceded by FROM/JOIN/TABLE/INTO.
    private static final Pattern UPDATE_TARGET = Pattern.compile(
            "^\\s*UPDATE\\s+([a-zA-Z_][\\w$]*(?:\\.[a-zA-Z_][\\w$]*)*)", Pattern.CASE_INSENSITIVE);

    public enum Action { ALLOW, DENY }

    /**
     * {@code tablePattern}/{@code sqlPattern} are {@code null} when that dimension isn't part of
     * this rule (matches regardless). {@code statementType} is {@code null} or {@code "ANY"} for
     * the same "matches regardless" meaning on that dimension.
     */
    public record Rule(long id, int priority, Action action, String statementType, Pattern tablePattern,
            Pattern sqlPattern, String description) {

        boolean matchesStatementType(String detectedType) {
            return statementType == null || statementType.isBlank() || "ANY".equalsIgnoreCase(statementType)
                    || statementType.equalsIgnoreCase(detectedType);
        }

        boolean matchesTables(String sqlText) {
            if (tablePattern == null) {
                return true;
            }
            if (SqlTableReferences.anyMatches(sqlText, tablePattern)) {
                return true;
            }
            for (Pattern extra : List.of(TABLE_KEYWORD_TARGET, UPDATE_TARGET)) {
                Matcher m = extra.matcher(sqlText);
                while (m.find()) {
                    if (tablePattern.matcher(m.group(1)).find()) {
                        return true;
                    }
                }
            }
            return false;
        }

        boolean matchesSql(String sqlText) {
            return sqlPattern == null || sqlPattern.matcher(sqlText).find();
        }
    }

    // volatile, not final -- see AccessControlStage's identical "policy" field javadoc for why:
    // a reload from FirewallRuleStore's LISTEN callback and a concurrent statement's own read both
    // need to see a consistent, fully-built rule list, never a partially-applied one, without a
    // lock on the hot path.
    private volatile List<Rule> rules;

    public FirewallStage(List<Rule> rules) {
        this.rules = List.copyOf(rules);
    }

    /** Swaps in a freshly-read rule list -- see {@link com.polygres.wire.config.FirewallRuleStore#listen}. */
    public void reloadRules(List<Rule> newRules) {
        this.rules = List.copyOf(newRules);
        log.info("firewall: reloaded {} rule(s)", newRules.size());
    }

    /**
     * Legacy string grammar (raw regex, {@code "pattern1:allow,pattern2:deny,..."}) -- kept only
     * for the rare caller that wants pure-regex rules directly in code (e.g. a test); {@link
     * com.polygres.wire.config.FirewallRuleStore} is the real, Postgres-table-backed source of
     * rules this stage is actually wired to in {@code Main}.
     */
    public static FirewallStage fromConfig(String spec) {
        List<Rule> parsed = new java.util.ArrayList<>();
        if (spec != null && !spec.isBlank()) {
            long id = 1;
            for (String entry : spec.split(",")) {
                int idx = entry.lastIndexOf(':');
                if (idx <= 0) {
                    continue;
                }
                Pattern pattern = Pattern.compile(entry.substring(0, idx).trim(), Pattern.CASE_INSENSITIVE);
                Action action = "deny".equalsIgnoreCase(entry.substring(idx + 1).trim()) ? Action.DENY : Action.ALLOW;
                parsed.add(new Rule(id++, 100, action, null, null, pattern, null));
            }
        }
        return new FirewallStage(parsed);
    }

    static String detectStatementType(String sql) {
        Matcher m = LEADING_KEYWORD.matcher(sql);
        return m.find() ? m.group(1).toUpperCase(Locale.ROOT) : "";
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String sql = statement.sqlText();
        if (STACKED_QUERY.matcher(sql).find()) {
            log.warn("firewall: rejecting statement -- stacked-query pattern detected");
            throw new SQLException("statement rejected by firewall: stacked query detected", "42000");
        }
        String statementType = detectStatementType(sql);
        List<Rule> currentRules = rules; // one volatile read -- see the field's javadoc on why
        for (Rule rule : currentRules) {
            if (rule.matchesStatementType(statementType) && rule.matchesTables(sql) && rule.matchesSql(sql)) {
                if (rule.action() == Action.DENY) {
                    log.warn("firewall: rejecting statement -- matched deny rule id={} ({})",
                            rule.id(), rule.description() == null ? "no description" : rule.description());
                    throw new SQLException("statement rejected by firewall rule"
                            + (rule.description() == null ? "" : ": " + rule.description()), "42000");
                }
                break; // ALLOW: stop evaluating, same "first match wins" semantics as before
            }
        }
        return next.proceed(statement);
    }
}
