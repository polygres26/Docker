package com.nexagres.dms.http.auth;

import com.google.gson.Gson;
import com.nexagres.dms.http.RouteHandler;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Map;

public class LogoutRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final AdminAuth auth;

    public LogoutRoute(AdminAuth auth) {
        this.auth = auth;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws Exception {
        auth.logout(AuthGuard.readCookie(request, AuthGuard.COOKIE_NAME));
        Cookie cookie = new Cookie(AuthGuard.COOKIE_NAME, "");
        cookie.setPath("/");
        cookie.setMaxAge(0);
        response.addCookie(cookie);
        response.setContentType("application/json");
        response.getWriter().write(GSON.toJson(Map.of("status", "ok")));
    }
}
