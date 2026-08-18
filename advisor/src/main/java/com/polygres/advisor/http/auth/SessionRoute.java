package com.polygres.advisor.http.auth;

import com.google.gson.Gson;
import com.polygres.advisor.http.RouteHandler;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Map;

/** {@code GET /api/session} -- lets the UI check "am I already logged in?" on page load without triggering a 401 redirect loop. */
public class SessionRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final AdminAuth auth;

    public SessionRoute(AdminAuth auth) {
        this.auth = auth;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws Exception {
        boolean authenticated = auth.isValid(AuthGuard.readCookie(request, AuthGuard.COOKIE_NAME));
        response.setContentType("application/json");
        response.getWriter().write(GSON.toJson(Map.of("authenticated", authenticated)));
    }
}
