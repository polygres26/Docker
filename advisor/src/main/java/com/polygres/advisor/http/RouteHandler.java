package com.polygres.advisor.http;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/** Same shape as Omnigate's {@code com.omnigate.http.RouteHandler} -- one route, one handler. */
@FunctionalInterface
public interface RouteHandler {
    void handle(HttpServletRequest request, HttpServletResponse response) throws Exception;
}
