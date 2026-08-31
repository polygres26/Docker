package com.nexagres.dms.http.auth;

import com.google.gson.Gson;
import com.nexagres.dms.http.RouteHandler;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * {@code POST /api/sso-login} -- the bearer-token counterpart to {@link LoginRoute}'s
 * username/password form. Registered by {@code DmsHttpServer} only when {@code
 * NEXAGRES_SSO_JWT_SECRET} is set AND the process is Enterprise-licensed (see {@link SsoAuth}'s
 * own constructor gate); unset means this route doesn't exist at all rather than existing and
 * always 501ing, so a free-tier deployment's {@code /api/sso-login} is a plain 404 like any other
 * route it never registered.
 */
public class SsoLoginRoute implements RouteHandler {

    private static final Gson GSON = new Gson();

    private final SsoAuth ssoAuth;
    private final AdminAuth auth;

    public SsoLoginRoute(SsoAuth ssoAuth, AdminAuth auth) {
        this.ssoAuth = ssoAuth;
        this.auth = auth;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws Exception {
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            response.setStatus(405);
            return;
        }
        String header = request.getHeader("Authorization");
        String bearer = header != null && header.regionMatches(true, 0, "Bearer ", 0, 7)
                ? header.substring(7).trim() : null;

        var subject = ssoAuth.verify(bearer);
        response.setContentType("application/json");
        if (subject.isEmpty()) {
            response.setStatus(401);
            response.getWriter().write(GSON.toJson(Map.of("error", "Invalid or expired SSO bearer token.")));
            return;
        }

        String token = auth.issueSessionFor(subject.get());
        Cookie cookie = new Cookie(AuthGuard.COOKIE_NAME, token);
        cookie.setHttpOnly(true);
        cookie.setPath("/");
        cookie.setMaxAge(12 * 60 * 60);
        response.addCookie(cookie);
        response.setStatus(200);
        response.getWriter().write(GSON.toJson(Map.of("status", "ok")));
    }
}
