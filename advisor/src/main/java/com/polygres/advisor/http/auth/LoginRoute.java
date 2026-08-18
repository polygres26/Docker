package com.polygres.advisor.http.auth;

import com.google.gson.Gson;
import com.polygres.advisor.http.RouteHandler;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Map;

public class LoginRoute implements RouteHandler {

    private static final Gson GSON = new Gson();

    private static class LoginRequest {
        String username;
        String password;
    }

    private final AdminAuth auth;

    public LoginRoute(AdminAuth auth) {
        this.auth = auth;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws Exception {
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            response.setStatus(405);
            return;
        }
        LoginRequest req = GSON.fromJson(request.getReader(), LoginRequest.class);
        var token = req == null ? java.util.Optional.<String>empty() : auth.login(req.username, req.password);

        response.setContentType("application/json");
        if (token.isEmpty()) {
            response.setStatus(401);
            response.getWriter().write(GSON.toJson(Map.of("error", "Invalid username or password.")));
            return;
        }

        Cookie cookie = new Cookie(AuthGuard.COOKIE_NAME, token.get());
        cookie.setHttpOnly(true);
        cookie.setPath("/");
        cookie.setMaxAge(12 * 60 * 60);
        response.addCookie(cookie);
        response.setStatus(200);
        response.getWriter().write(GSON.toJson(Map.of("status", "ok")));
    }
}
