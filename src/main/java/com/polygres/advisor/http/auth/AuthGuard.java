package com.polygres.advisor.http.auth;

import com.polygres.advisor.http.RouteHandler;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/** Wraps a {@link RouteHandler} so it 401s unless a valid session cookie is present. */
public class AuthGuard {

    public static final String COOKIE_NAME = "polygres_session";

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

    public static String readCookie(HttpServletRequest request, String name) {
        Cookie[] cookies = request.getCookies();
        if (cookies == null) return null;
        for (Cookie c : cookies) {
            if (name.equals(c.getName())) return c.getValue();
        }
        return null;
    }
}
