package com.nexagres.dms.http.auth;

import com.nexagres.dms.http.RouteHandler;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/** Wraps a {@link RouteHandler} so it 401s unless a valid session cookie is present. */
public class AuthGuard {

    public static final String COOKIE_NAME = "nexagres_session";

    public static RouteHandler require(AdminAuth auth, RouteHandler inner) {
        return (request, response) -> {
            String token = readCookie(request, COOKIE_NAME);
            if (!auth.isValid(token)) {
                response.setStatus(401);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\":\"Not authenticated.\"}");
                return;
            }
            inner.handle(request, response);
        };
    }

    /** As {@link #require}, but a {@code VIEWER}-role session may only read (GET) -- any other
     * HTTP method 403s before {@code inner} ever runs. Wrap the two genuinely mutating route
     * groups (saved connections, migration jobs) with this instead of {@link #require}; every
     * other route stays plain {@link #require} since it's either read-only already or doesn't
     * touch anything a viewer shouldn't see (see {@code DmsHttpServer}'s own route table for
     * which is which). On the free/Developer tier this is a no-op in practice -- there is no
     * VIEWER session to ever reach the 403 branch, see {@code DmsLicensing#rbacAllowed}. */
    public static RouteHandler requireAdminForMutations(AdminAuth auth, RouteHandler inner) {
        return (request, response) -> {
            String token = readCookie(request, COOKIE_NAME);
            var session = auth.sessionOf(token);
            if (session.isEmpty()) {
                response.setStatus(401);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\":\"Not authenticated.\"}");
                return;
            }
            boolean mutating = !"GET".equalsIgnoreCase(request.getMethod());
            if (mutating && session.get().role() != Role.ADMIN) {
                response.setStatus(403);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\":\"Read-only session -- an ADMIN account is required "
                        + "for this action.\"}");
                return;
            }
            inner.handle(request, response);
        };
    }

    public static String readCookie(HttpServletRequest request, String name) {
        Cookie[] cookies = request.getCookies();
        if (cookies == null) return null;
        for (Cookie c : cookies) {
            if (name.equals(c.getName())) return c.getValue();
        }
        return null;
    }
}
