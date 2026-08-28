package com.nexagres.wire.core;

import java.util.List;

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

    public Statement(String tenantId, SourceDialect sourceDialect, String sqlText, List<Object> bindParams,
            String workloadClass, String targetBackend) {
        this(tenantId, sourceDialect, sqlText, bindParams, workloadClass, targetBackend, AccessContext.ANONYMOUS);
    }

    public static Statement of(SourceDialect dialect, String sqlText, List<Object> bindParams) {
        return new Statement("default", dialect, sqlText, bindParams, "default", null, AccessContext.ANONYMOUS);
    }

    /** As {@link #of(SourceDialect, String, List)}, but carrying a real, non-anonymous
     * {@code accessContext} -- e.g. the Postgres role a pgwire/mssqlwire session actually
     * authenticated as under {@code POLYWIRE_AUTH_MODE=postgres_roles}, propagated from here into
     * {@code JdbcBackendExecutor}'s native-RLS session-context call. */
    public static Statement of(SourceDialect dialect, String sqlText, List<Object> bindParams, AccessContext accessContext) {
        return new Statement("default", dialect, sqlText, bindParams, "default", null, accessContext);
    }

    public Statement withSqlText(String newSqlText) {
        return new Statement(tenantId, sourceDialect, newSqlText, bindParams, workloadClass, targetBackend, accessContext);
    }

    public Statement withSqlAndBinds(String newSqlText, List<Object> newBindParams) {
        return new Statement(tenantId, sourceDialect, newSqlText, newBindParams, workloadClass, targetBackend, accessContext);
    }

    public Statement withRouting(String newWorkloadClass, String newTargetBackend) {
        return new Statement(tenantId, sourceDialect, sqlText, bindParams, newWorkloadClass, newTargetBackend, accessContext);
    }

    public Statement withAccessContext(AccessContext newAccessContext) {
        return new Statement(tenantId, sourceDialect, sqlText, bindParams, workloadClass, targetBackend, newAccessContext);
    }
}
