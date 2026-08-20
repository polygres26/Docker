package com.polygres.advisor.http;

import com.google.gson.Gson;
import com.polygres.advisor.wire.WireConnectionStore;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code /api/wire-settings} -- GET returns the current PolyWire admin connection (token
 * redacted), PUT saves it. This is the one piece of config the combined UI needs before any
 * PolyWire-side page (firewall rules, etc.) can do anything -- where its admin API lives and what
 * bearer token to use.
 */
public class WireSettingsRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final WireConnectionStore store = new WireConnectionStore();

    private static class SettingsForm {
        String adminUrl;
        String adminToken;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if ("GET".equalsIgnoreCase(request.getMethod())) {
            WireConnectionStore.WireConnection c = store.get();
            Map<String, Object> redacted = new LinkedHashMap<>();
            redacted.put("adminUrl", c.adminUrl());
            redacted.put("hasToken", c.adminToken() != null && !c.adminToken().isBlank());
            redacted.put("configured", c.configured());
            writeJson(response, 200, redacted);
            return;
        }
        if ("PUT".equalsIgnoreCase(request.getMethod())) {
            SettingsForm form = GSON.fromJson(request.getReader(), SettingsForm.class);
            if (form == null || form.adminUrl == null || form.adminUrl.isBlank()) {
                writeError(response, 400, "adminUrl is required.");
                return;
            }
            WireConnectionStore.WireConnection saved = store.save(form.adminUrl, form.adminToken);
            Map<String, Object> redacted = new LinkedHashMap<>();
            redacted.put("adminUrl", saved.adminUrl());
            redacted.put("hasToken", saved.adminToken() != null && !saved.adminToken().isBlank());
            redacted.put("configured", saved.configured());
            writeJson(response, 200, redacted);
            return;
        }
        response.setStatus(405);
    }

    private void writeJson(HttpServletResponse response, int status, Object body) throws IOException {
        response.setContentType("application/json");
        response.setStatus(status);
        response.getWriter().write(GSON.toJson(body));
    }

    private void writeError(HttpServletResponse response, int status, String message) throws IOException {
        writeJson(response, status, Map.of("error", message));
    }
}
