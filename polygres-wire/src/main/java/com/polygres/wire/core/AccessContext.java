package com.polygres.wire.core;

import java.util.Map;
import java.util.Set;

/**
 * The authenticated end user's identity for row/column access control (see
 * {@code docs/design/end-user-data-access-security.md}) — the same role Cube.js's signed
 * "security context" and Omni's SSO-provisioned "user attributes" play, carried on every
 * {@link Statement} the way {@link Statement#tenantId()} already is.
 *
 * <p>{@code attributes} is deliberately an open string-to-string map rather than a fixed set of
 * fields (tenant, region, ...) — {@link com.polygres.wire.core.access.AccessPolicy} rules reference
 * attribute names by string ({@code required_attribute: "tenant"}), so the shape of what a
 * deployment cares about is entirely policy-file-driven, not hardcoded here.
 *
 * <p>{@link #ANONYMOUS} is the default for every {@link Statement} unless a frontend that
 * authenticates end users (today: the HTTP {@code /api/query} surface via
 * {@code com.polygres.wire.http.auth.AccessContextResolver}) populates one. Wire-protocol frontends
 * (pgwire/orawire/mywire) leave it {@code ANONYMOUS} for now — see the design doc's §3.2/Phase 7
 * for why that's an accepted, explicit scope boundary rather than an oversight.
 */
public record AccessContext(String userId, Set<String> roles, Map<String, String> attributes)
        implements java.io.Serializable {

    public static final AccessContext ANONYMOUS = new AccessContext("anonymous", Set.of(), Map.of());

    public AccessContext {
        if (userId == null || userId.isBlank()) {
            userId = "anonymous";
        }
        roles = roles == null ? Set.of() : Set.copyOf(roles);
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    public boolean isAnonymous() {
        return ANONYMOUS.userId().equals(userId) && roles.isEmpty() && attributes.isEmpty();
    }

    public boolean hasRole(String role) {
        return roles.contains(role);
    }

    public boolean hasAnyRole(java.util.Collection<String> candidates) {
        return candidates.stream().anyMatch(roles::contains);
    }
}
