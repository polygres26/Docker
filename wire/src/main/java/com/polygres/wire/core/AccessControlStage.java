package com.polygres.wire.core;

import com.polygres.wire.core.access.AccessPolicy;
import com.polygres.wire.core.access.ColumnMasker;
import com.polygres.wire.core.access.SqlTableReferences;
import com.polygres.wire.core.access.WhereClauseInjector;
import java.sql.SQLException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AccessControlStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(AccessControlStage.class);

    private volatile AccessPolicy policy;
    private final com.polygres.wire.audit.AuditLog auditLog;

    public AccessControlStage(AccessPolicy policy) {
        this(policy, null);
    }

    public AccessControlStage(AccessPolicy policy, com.polygres.wire.audit.AuditLog auditLog) {
        this.policy = policy == null ? AccessPolicy.EMPTY : policy;
        this.auditLog = auditLog;
    }

    public AccessPolicy policy() {
        return policy;
    }

    public void reloadPolicy(AccessPolicy newPolicy) {
        this.policy = newPolicy == null ? AccessPolicy.EMPTY : newPolicy;
        log.info("access-control: policy reloaded ({} column grant(s), {} row filter(s))",
                this.policy.columnGrants().size(), this.policy.rowFilters().size());
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        AccessPolicy currentPolicy = policy;
        if (currentPolicy.isEmpty()) {
            return next.proceed(statement);
        }
        Statement result = enforce(statement, currentPolicy);
        return next.proceed(result);
    }

    public Statement enforce(Statement statement) throws SQLException {
        return enforce(statement, policy);
    }

    private Statement enforce(Statement statement, AccessPolicy policy) throws SQLException {
        AccessContext accessContext = statement.accessContext();
        Set<String> referencedTables = SqlTableReferences.extract(statement.sqlText());

        for (AccessPolicy.RowFilter rule : policy.rowFilters()) {
            if (!anyTableMatches(referencedTables, rule.tablePattern())) {
                continue;
            }
            if (rule.bypassedBy(accessContext)) {
                continue;
            }
            String value = accessContext.attributes().get(rule.requiredAttribute());
            if (value == null) {
                log.warn("access-control: rejecting statement — table matches row_filter (column={}) but "
                        + "caller's AccessContext (user={}) has no \"{}\" attribute; fail-closed, not unfiltered",
                        rule.filterColumn(), accessContext.userId(), rule.requiredAttribute());
                audit(com.polygres.wire.audit.AuditEvent.Type.ACCESS_DENIED, accessContext.userId(),
                        "missing required attribute \"" + rule.requiredAttribute() + "\" for a row-restricted table",
                        java.util.Map.of("filterColumn", rule.filterColumn()));
                throw new SQLException("statement rejected: missing required access attribute \""
                        + rule.requiredAttribute() + "\" for a row-restricted table", "42501");
            }
            WhereClauseInjector.Injected injected = WhereClauseInjector.inject(
                    statement.sqlText(), statement.bindParams(), rule.filterColumn(), value);
            statement = statement.withSqlAndBinds(injected.sqlText(), injected.bindParams());
            referencedTables = SqlTableReferences.extract(statement.sqlText());
            audit(com.polygres.wire.audit.AuditEvent.Type.ROW_FILTER_APPLIED, accessContext.userId(),
                    "applied " + rule.filterColumn() + " = " + value,
                    java.util.Map.of("filterColumn", rule.filterColumn(), "value", value));
        }

        for (AccessPolicy.ColumnGrant grant : policy.columnGrants()) {
            if (!anyTableMatches(referencedTables, grant.tablePattern())) {
                continue;
            }
            if (grant.satisfiedBy(accessContext.attributes())) {
                continue;
            }
            for (String column : grant.columns()) {
                if (!referencesColumn(statement.sqlText(), column)) {
                    continue;
                }
                if (grant.onViolation() == AccessPolicy.OnViolation.MASK) {
                    log.info("access-control: masking column \"{}\" for user={} (attribute \"{}\" not satisfied)",
                            column, accessContext.userId(), grant.requiredAttribute());
                    statement = statement.withSqlText(ColumnMasker.mask(statement.sqlText(), column));
                    audit(com.polygres.wire.audit.AuditEvent.Type.COLUMN_MASKED, accessContext.userId(),
                            "masked column \"" + column + "\"", java.util.Map.of("column", column));
                } else {
                    log.warn("access-control: rejecting statement — column \"{}\" requires attribute \"{}\" in {}, "
                            + "user={} attributes={}",
                            column, grant.requiredAttribute(), grant.allowedValues(), accessContext.userId(),
                            accessContext.attributes());
                    audit(com.polygres.wire.audit.AuditEvent.Type.ACCESS_DENIED, accessContext.userId(),
                            "not entitled to column \"" + column + "\"", java.util.Map.of("column", column));
                    throw new SQLException(
                            "statement rejected: caller is not entitled to column \"" + column + "\"", "42501");
                }
            }
        }

        return statement;
    }

    private void audit(com.polygres.wire.audit.AuditEvent.Type type, String userId, String summary, java.util.Map<String, String> details) {
        if (auditLog != null) {
            auditLog.record(com.polygres.wire.audit.AuditEvent.of(type, userId, summary, details));
        }
    }

    private static boolean anyTableMatches(Set<String> tables, java.util.regex.Pattern pattern) {
        for (String table : tables) {
            if (pattern.matcher(table).find()) {
                return true;
            }
        }
        return false;
    }

    private static boolean referencesColumn(String sqlText, String column) {
        return java.util.regex.Pattern
                .compile("(?:\\b[\\w$]+\\.)?\\b" + java.util.regex.Pattern.quote(column) + "\\b",
                        java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(sqlText)
                .find();
    }
}
