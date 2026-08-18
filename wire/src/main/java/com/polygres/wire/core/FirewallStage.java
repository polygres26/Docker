package com.polygres.wire.core;

import com.polygres.wire.core.access.SqlTableReferences;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FirewallStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(FirewallStage.class);

    private static final Pattern STACKED_QUERY = Pattern.compile(
            ";\\s*(select|insert|update|delete|drop|alter|create|grant|exec)\\b", Pattern.CASE_INSENSITIVE);

    private static final Pattern LEADING_KEYWORD = Pattern.compile("^\\s*(\\w+)");
    
    private static final Pattern TABLE_KEYWORD_TARGET = Pattern.compile(
            "\\b(?:TABLE|INTO)\\s+([a-zA-Z_][\\w$]*(?:\\.[a-zA-Z_][\\w$]*)*)", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern UPDATE_TARGET = Pattern.compile(
            "^\\s*UPDATE\\s+([a-zA-Z_][\\w$]*(?:\\.[a-zA-Z_][\\w$]*)*)", Pattern.CASE_INSENSITIVE);

    public enum Action { ALLOW, DENY }

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

    private volatile List<Rule> rules;

    public FirewallStage(List<Rule> rules) {
        this.rules = List.copyOf(rules);
    }

    public void reloadRules(List<Rule> newRules) {
        this.rules = List.copyOf(newRules);
        log.info("firewall: reloaded {} rule(s)", newRules.size());
    }

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
        List<Rule> currentRules = rules;
        for (Rule rule : currentRules) {
            if (rule.matchesStatementType(statementType) && rule.matchesTables(sql) && rule.matchesSql(sql)) {
                if (rule.action() == Action.DENY) {
                    log.warn("firewall: rejecting statement -- matched deny rule id={} ({})",
                            rule.id(), rule.description() == null ? "no description" : rule.description());
                    throw new SQLException("statement rejected by firewall rule"
                            + (rule.description() == null ? "" : ": " + rule.description()), "42000");
                }
                break;
            }
        }
        return next.proceed(statement);
    }
}
