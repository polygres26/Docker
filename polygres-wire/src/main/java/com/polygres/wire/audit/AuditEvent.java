package com.polygres.wire.audit;

import java.time.Instant;
import java.util.Map;

/**
 * One security-relevant decision — the answer to the question every enterprise access review
 * eventually asks: "who accessed what, when, and what did PolyWire do about it." See
 * {@link AuditLog}, {@code docs/design/end-user-data-access-security.md}.
 */
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
        /**
         * {@code com.polygres.wire.ontology.profiling.DataDrivenProfiler} auto-accepted a
         * data-driven-profiling join suggestion straight into {@code OntologyStore} — skipped
         * human review because an operator explicitly configured a
         * {@code joinAutoAcceptThreshold} and this candidate's score cleared it. The detective
         * half of that control: every auto-accept lands here, reviewable via {@code
         * GET /api/audit-log} after the fact, same as any other audited decision.
         */
        ONTOLOGY_RELATIONSHIP_AUTO_ACCEPTED,
        /**
         * One NL2SQL translation-and-execution attempt (the "NL2SQL insights" admin page's live
         * query log) — recorded by {@code AskRoute} (the business-user "Ask" app) and {@code
         * Nl2SqlStage} (the {@code SELECT NL ...} wire-protocol path), both of which know the
         * question, the generated SQL, and the true execution outcome. {@code details} carries
         * {@code question}, {@code sql}, {@code outcome} ({@code PASS}/{@code SQL_ERROR}/{@code
         * TRANSLATION_ERROR}), and {@code latencyMs}.
         */
        NL2SQL_QUERY_EXECUTED,
        /**
         * {@code JudgedNl2SqlProvider} actually rewrote a generator's candidate SQL (a no-op judge
         * verdict — {@code OK}, or a verdict that didn't parse — is not recorded; only a real
         * correction is). {@code details} carries {@code question}, {@code candidateSql}, and
         * {@code correctedSql}.
         */
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
