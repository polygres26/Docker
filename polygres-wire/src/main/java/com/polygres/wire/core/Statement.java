package com.polygres.wire.core;

import java.util.List;

/**
 * Canonical shape every inbound request is normalized into before it
 * touches pipeline or backend code (ARCHITECTURE.md §4). Every
 * {@code WireFrontend} — Oracle, Postgres wire, the native gRPC driver —
 * produces this and only this; the {@link StatementPipeline} and backend
 * layer never see protocol-specific bytes.
 *
 * {@code tenantId} is carried from day one even though today it's always
 * {@code "default"} — see ARCHITECTURE.md §7 on why retrofitting it later
 * would be a data-model migration, not a config change.
 *
 * {@code accessContext} is the end-user identity/attributes used by
 * {@link AccessControlStage} for row/column access control (see
 * {@code docs/design/end-user-data-access-security.md}) — defaults to
 * {@link AccessContext#ANONYMOUS}, same "additive, opt-in" shape {@code tenantId} already
 * established, so no existing caller (wire frontends included) has to change to keep working.
 */
public record Statement(
        String tenantId,
        SourceDialect sourceDialect,
        String sqlText,
        List<Object> bindParams,
        String workloadClass,
        String targetBackend,
        AccessContext accessContext) {

    public Statement {
        if (tenantId == null || tenantId.isBlank()) {
            tenantId = "default";
        }
        if (workloadClass == null || workloadClass.isBlank()) {
            workloadClass = "default";
        }
        if (accessContext == null) {
            accessContext = AccessContext.ANONYMOUS;
        }
    }

    /** Convenience 6-arg constructor for every existing call site — {@link AccessContext#ANONYMOUS} by default. */
    public Statement(String tenantId, SourceDialect sourceDialect, String sqlText, List<Object> bindParams,
            String workloadClass, String targetBackend) {
        this(tenantId, sourceDialect, sqlText, bindParams, workloadClass, targetBackend, AccessContext.ANONYMOUS);
    }

    public static Statement of(SourceDialect dialect, String sqlText, List<Object> bindParams) {
        return new Statement("default", dialect, sqlText, bindParams, "default", null, AccessContext.ANONYMOUS);
    }

    /** Copy with {@link #sqlText()} replaced (NL2SQL rewrite) — everything else, including any router-assigned {@link #targetBackend()}, carries over. */
    public Statement withSqlText(String newSqlText) {
        return new Statement(tenantId, sourceDialect, newSqlText, bindParams, workloadClass, targetBackend, accessContext);
    }

    /** Copy with {@link #sqlText()} and {@link #bindParams()} both replaced ({@code AccessControlStage} row-filter injection). */
    public Statement withSqlAndBinds(String newSqlText, List<Object> newBindParams) {
        return new Statement(tenantId, sourceDialect, newSqlText, newBindParams, workloadClass, targetBackend, accessContext);
    }

    /** Copy with {@link #workloadClass()} and/or {@link #targetBackend()} replaced (RouterStage). */
    public Statement withRouting(String newWorkloadClass, String newTargetBackend) {
        return new Statement(tenantId, sourceDialect, sqlText, bindParams, newWorkloadClass, newTargetBackend, accessContext);
    }

    /** Copy with {@link #accessContext()} replaced — how a frontend that authenticates end users (see {@code AccessContextResolver}) attaches identity before the statement enters the pipeline. */
    public Statement withAccessContext(AccessContext newAccessContext) {
        return new Statement(tenantId, sourceDialect, sqlText, bindParams, workloadClass, targetBackend, newAccessContext);
    }
}
