package com.polygres.wire.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ARCHITECTURE.md §5.7: an ordered allow/deny rule list, evaluated first in
 * the pipeline so a rejected statement never reaches translation, routing,
 * or a backend. Rules are regexes matched case-insensitively against the
 * raw SQL text; first match wins, default is ALLOW. One built-in heuristic
 * (not a rule) rejects classic stacked-query injection (a bind-unrelated
 * literal SQL statement appended after a {@code ;}) regardless of rule
 * configuration — a narrow, cheap check, not a general injection detector.
 */
public final class FirewallStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(FirewallStage.class);

    // "; <keyword>" after the first statement — the classic stacked-query pattern.
    // Doesn't flag a trailing lone ";" (many clients send one) or ";" inside a string literal is a
    // known gap — a narrow heuristic, not a parser.
    private static final Pattern STACKED_QUERY = Pattern.compile(
            ";\\s*(select|insert|update|delete|drop|alter|create|grant|exec)\\b", Pattern.CASE_INSENSITIVE);

    public enum Action { ALLOW, DENY }

    public record Rule(Pattern pattern, Action action) {
    }

    private final List<Rule> rules;

    public FirewallStage(List<Rule> rules) {
        this.rules = List.copyOf(rules);
    }

    /** Builds rules from "regex1:allow,regex2:deny,..." (default action if nothing matches is ALLOW). */
    public static FirewallStage fromConfig(String spec) {
        List<Rule> rules = new ArrayList<>();
        if (spec != null && !spec.isBlank()) {
            for (String entry : spec.split(",")) {
                int idx = entry.lastIndexOf(':');
                if (idx <= 0) {
                    continue;
                }
                Pattern pattern = Pattern.compile(entry.substring(0, idx).trim(), Pattern.CASE_INSENSITIVE);
                Action action = "deny".equalsIgnoreCase(entry.substring(idx + 1).trim()) ? Action.DENY : Action.ALLOW;
                rules.add(new Rule(pattern, action));
            }
        }
        return new FirewallStage(rules);
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String sql = statement.sqlText();
        if (STACKED_QUERY.matcher(sql).find()) {
            log.warn("firewall: rejected stacked-query pattern, tenant={}", statement.tenantId());
            throw new SQLException("statement rejected by SQL firewall: stacked queries are not permitted", "42000");
        }
        for (Rule rule : rules) {
            if (rule.pattern().matcher(sql).find()) {
                if (rule.action() == Action.DENY) {
                    log.warn("firewall: rejected by rule /{}/, tenant={}", rule.pattern(), statement.tenantId());
                    throw new SQLException("statement rejected by SQL firewall rule", "42000");
                }
                break; // first ALLOW match short-circuits rule evaluation, same as a DENY match would
            }
        }
        return next.proceed(statement);
    }
}
