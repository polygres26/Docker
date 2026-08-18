package com.polygres.wire.core;

import java.util.Map;
import java.util.Set;

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
