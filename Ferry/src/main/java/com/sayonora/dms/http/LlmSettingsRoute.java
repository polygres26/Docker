package com.sayonora.dms.http;

import com.google.gson.Gson;
import com.sayonora.dms.llm.LlmProviderType;
import com.sayonora.dms.llm.LlmRole;
import com.sayonora.dms.llm.LlmSettingsStore;
import com.sayonora.dms.llm.LocalModelPresets;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * {@code /api/llm-settings/{role}} -- GET returns the redacted current config for {@code role}
 * ({@code primary} or {@code judge}); PUT saves it. Backs the LLM configuration page (its own rail
 * item, separate from per-connection settings -- this is app-wide config, not scoped to one
 * source database).
 *
 * <p>{@code GET /api/llm-settings/local-presets} is the one non-role-scoped route here -- returns
 * the two {@link LocalModelPresets} (Qwen/Gemma) so the UI can offer a simple switch instead of a
 * raw file-path field. Handled first since "local-presets" would otherwise be misparsed as a role.
 */
public class LlmSettingsRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final LlmSettingsStore store = new LlmSettingsStore();

    private static class SettingsForm {
        String providerType;
        String apiKey;
        String baseUrl;
        String modelPath;
        String model;
        boolean enabled;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String[] parts = request.getRequestURI().split("\\?")[0].split("/");
        // /api/llm-settings/{role} -> ["", "api", "llm-settings", role]
        if (parts.length != 4) { response.setStatus(404); return; }

        if ("local-presets".equals(parts[3])) {
            if (!"GET".equalsIgnoreCase(request.getMethod())) { response.setStatus(405); return; }
            writeJson(response, 200, Map.of("qwen", LocalModelPresets.qwen(), "gemma", LocalModelPresets.gemma()));
            return;
        }

        LlmRole role;
        try {
            role = LlmRole.valueOf(parts[3].toUpperCase());
        } catch (IllegalArgumentException e) {
            writeError(response, 404, "Unknown LLM role: " + parts[3] + " (expected primary or judge).");
            return;
        }

        if ("GET".equalsIgnoreCase(request.getMethod())) {
            writeJson(response, 200, store.get(role).redacted());
            return;
        }
        if ("PUT".equalsIgnoreCase(request.getMethod())) {
            SettingsForm form = GSON.fromJson(request.getReader(), SettingsForm.class);
            if (form == null || form.providerType == null) {
                writeError(response, 400, "providerType is required.");
                return;
            }
            LlmProviderType providerType;
            try {
                providerType = LlmProviderType.valueOf(form.providerType.toUpperCase());
            } catch (IllegalArgumentException e) {
                writeError(response, 400, "Unknown providerType: " + form.providerType + " (expected local, builtin, or external).");
                return;
            }
            var saved = store.save(role, providerType, form.apiKey, form.baseUrl, form.modelPath, form.model, form.enabled);
            writeJson(response, 200, saved.redacted());
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
