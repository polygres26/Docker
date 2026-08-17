package com.polygres.wire.core;

import com.polygres.wire.core.access.AccessPolicy;
import com.polygres.wire.core.access.ColumnMasker;
import com.polygres.wire.core.access.SqlTableReferences;
import com.polygres.wire.core.access.WhereClauseInjector;
import java.sql.SQLException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-user row/column access control — see {@code docs/design/end-user-data-access-security.md}
 * §3.4. Placed after Omnigate's NL2SQL stage (not ported — see PolyWire's scope notes), before {@link FirewallStage} in
 * {@code GatewayComponents}' stage list: by the time this stage runs, {@code sqlText} is always
 * real SQL (LLM-generated or client-supplied directly), same rationale
 * {@code Nl2SqlStage}'s javadoc already gives for why {@code FirewallStage} runs after NL2SQL
 * rather than before.
 *
 * <p>Two responsibilities per statement, mirroring the two enforcement styles surveyed in the
 * design doc rather than picking one:
 * <ol>
 *   <li><b>Row filter injection</b> ({@link AccessPolicy.RowFilter}, Cube.js
 *   {@code queryRewrite}-style): for every matching, non-bypassed rule whose
 *   {@code requiredAttribute} is present on the caller's {@link AccessContext}, appends a bound
 *   {@code <filterColumn> = ?} predicate via {@link WhereClauseInjector}. If the attribute is
 *   <em>absent</em>, there's nothing to filter on — this fails closed (reject), not open
 *   (unfiltered), same as responsibility 2 below.</li>
 *   <li><b>Column grant enforcement</b> ({@link AccessPolicy.ColumnGrant}, sql-data-guard-style
 *   backstop): for every matching rule the caller's attributes don't satisfy, either rejects the
 *   statement ({@code on_violation: deny}, the default) or rewrites the denied column reference to
 *   {@code NULL} via {@link ColumnMasker} ({@code on_violation: mask}) — see {@link AccessPolicy}'s
 *   javadoc for why {@code mask} is opt-in, not default.</li>
 * </ol>
 *
 * <p><b>Fail-closed default</b> (§3.5): an {@link AccessContext#isAnonymous() anonymous} caller
 * hitting a table any configured rule matches is rejected outright, before either responsibility
 * above even runs — an unauthenticated caller must never get "no filter matched, so unfiltered."
 * This only bites once a policy is actually configured ({@link AccessPolicy#isEmpty()} false); with
 * no policy configured at all, this stage is a complete no-op pass-through, matching every other
 * optional stage's zero-config convention ({@code RouterStage}, {@code FirewallStage}).
 */
public final class AccessControlStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(AccessControlStage.class);

    /** Not {@code final} — see {@link #reloadPolicy}, the live-editable-config path from a DB-backed policy (§K of the config/audit-in-a-database follow-on). {@code volatile} since a reload from an admin HTTP request and a concurrent statement's own read both need to see a consistent, fully-constructed {@link AccessPolicy} — never a partially-applied one — without needing a lock on the hot path. */
    private volatile AccessPolicy policy;
    private final com.polygres.wire.audit.AuditLog auditLog;

    public AccessControlStage(AccessPolicy policy) {
        this(policy, null);
    }

    /** {@code auditLog}: null is a legitimate, tolerated "no durable trail wired up" (every existing caller/test) — every decision below still logs via SLF4J either way. */
    public AccessControlStage(AccessPolicy policy, com.polygres.wire.audit.AuditLog auditLog) {
        this.policy = policy == null ? AccessPolicy.EMPTY : policy;
        this.auditLog = auditLog;
    }

    public AccessPolicy policy() {
        return policy;
    }

    /**
     * Swaps in a new policy for every statement from this point on — no restart needed. Called by
     * the {@code PUT /api/config/access-policy} admin route after persisting the new YAML to
     * {@code ConfigStore}, mirroring {@code BackendRegistry.reload}'s existing "persist, then hot-
     * swap the live component" shape for {@code PUT /api/config/backends}. A single volatile
     * reference swap — nothing in {@link #enforce} holds a reference to the old {@link AccessPolicy}
     * across a statement, so an in-flight statement either sees the whole old policy or the whole
     * new one, never a mix.
     */
    public void reloadPolicy(AccessPolicy newPolicy) {
        this.policy = newPolicy == null ? AccessPolicy.EMPTY : newPolicy;
        log.info("access-control: policy reloaded ({} column grant(s), {} row filter(s))",
                this.policy.columnGrants().size(), this.policy.rowFilters().size());
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        AccessPolicy currentPolicy = policy; // one volatile read — see reloadPolicy's javadoc on why enforce() below takes it as a parameter instead of re-reading the field
        if (currentPolicy.isEmpty()) {
            return next.proceed(statement);
        }
        Statement result = enforce(statement, currentPolicy);
        return next.proceed(result);
    }

    /** Public so {@code com.polygres.wire.http.AccessPolicyTestRoute}'s dry-run mode (§3.8) can call it directly without a full pipeline — the same enforcement logic {@link #handle} uses, just without a backend dispatch on success. Uses whatever policy is live at the moment of the call. */
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

        // No blanket ACCESS_ALLOWED event here on purpose: most statements never touch any
        // configured rule at all (an unrelated table, or an already-satisfied grant) and logging
        // an audit entry for every single one of those would drown the durable trail in noise —
        // ROW_FILTER_APPLIED/COLUMN_MASKED already are the "allowed, and here's what changed"
        // signal for statements a rule actually did something to; ACCESS_DENIED covers rejection.
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

    // Deliberately reuses the same identifier-boundary matching ColumnMasker already applies, so
    // "does this grant even apply" and "how would we mask it" never disagree about what counts as
    // a reference to the column.
    private static boolean referencesColumn(String sqlText, String column) {
        return java.util.regex.Pattern
                .compile("(?:\\b[\\w$]+\\.)?\\b" + java.util.regex.Pattern.quote(column) + "\\b",
                        java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(sqlText)
                .find();
    }
}
