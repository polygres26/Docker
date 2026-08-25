package com.polygres.wire.audit;

import java.time.Instant;
import java.util.Map;

public record AuditEvent(Instant timestamp, Type type, String userId, String summary, Map<String, String> details) {

    public enum Type {
        ACCESS_ALLOWED,
        ACCESS_DENIED,
        ROW_FILTER_APPLIED,
        COLUMN_MASKED,
        SCIM_USER_PROVISIONED,
        SCIM_USER_UPDATED,
        SCIM_USER_DEPROVISIONED,
        SCIM_USER_DELETED,
        ADMIN_LOGIN,
        DB_LOGIN_SUCCEEDED,
        DB_LOGIN_FAILED,

        ONTOLOGY_RELATIONSHIP_AUTO_ACCEPTED,
        
        NL2SQL_QUERY_EXECUTED,
        
        NL2SQL_JUDGE_CORRECTED
    }

    public AuditEvent {
        details = details == null ? Map.of() : Map.copyOf(details);
    }

    public static AuditEvent of(Type type, String userId, String summary) {
        return new AuditEvent(Instant.now(), type, userId, summary, Map.of());
    }

    public static AuditEvent of(Type type, String userId, String summary, Map<String, String> details) {
        return new AuditEvent(Instant.now(), type, userId, summary, details);
    }
}
