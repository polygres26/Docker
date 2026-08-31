package com.nexagres.dms.http.auth;

import com.nexagres.dms.core.AuditLogStore;
import com.nexagres.dms.http.RouteHandler;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Wraps a {@link RouteHandler} so every non-{@code GET} request against it is recorded to
 * {@link AuditLogStore} -- method, path, the response status, and the session's own username as
 * actor -- AFTER {@code inner} runs, so a rejected write (say, {@link
 * AuthGuard#requireAdminForMutations} 403ing a VIEWER session) is captured too, not just
 * successful ones. Applied at {@code DmsHttpServer}'s own route-table level to the two genuinely
 * mutating route groups (saved connections, migration jobs) rather than threading an
 * {@link AuditLogStore} reference through every route class individually.
 *
 * <p>A real no-op on the free/Developer tier -- see {@link AuditLogStore#record}'s own javadoc;
 * this class doesn't duplicate that check, it just always calls through.
 */
public final class AuditGuard {

    private AuditGuard() {
    }

    public static RouteHandler wrap(AdminAuth auth, AuditLogStore auditLog, RouteHandler inner) {
        return (HttpServletRequest request, HttpServletResponse response) -> {
            try {
                inner.handle(request, response);
            } finally {
                if (!"GET".equalsIgnoreCase(request.getMethod())) {
                    String token = AuthGuard.readCookie(request, AuthGuard.COOKIE_NAME);
                    String actor = auth.usernameOf(token);
                    auditLog.record(actor, request.getMethod() + " " + request.getRequestURI(),
                            "status=" + response.getStatus());
                }
            }
        };
    }
}
