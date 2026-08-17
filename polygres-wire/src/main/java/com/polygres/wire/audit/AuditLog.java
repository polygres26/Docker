package com.polygres.wire.audit;

import com.google.gson.JsonObject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records {@link AuditEvent}s from every enforcement point ({@code AccessControlStage},
 * {@code ScimServlet}, admin login) — the gap flagged directly against this feature: every access
 * decision was previously only ever an SLF4J log line, unqueryable and not guaranteed durable.
 * See {@code docs/design/end-user-data-access-security.md}.
 *
 * <p><b>Three tiers, independent of each other</b>:
 * <ul>
 *   <li>An in-memory bounded ring buffer ({@code recent(int)}) — always on, zero config, backs
 *   {@code GET /api/audit-log} in the admin console for quick "what just happened" visibility.
 *   Single-process, same documented caveat {@code SessionStore}/{@code UserAttributeStore} already
 *   carry — lost on restart, not shared across a cluster.</li>
 *   <li>An optional durable JSONL file sink ({@code POLYWIRE_AUDIT_LOG_FILE}) — the realistic way
 *   an enterprise ships a trail to a SIEM (Splunk, Datadog, Elastic) it already runs, via a
 *   log-forwarding agent already in its stack, rather than PolyWire reinventing one.</li>
 *   <li>An optional {@link AuditLogStore} (a real database, {@code POLYWIRE_AUDIT_LOG_DB}) — see
 *   that class's own javadoc for the hash-chained tamper-evidence this tier specifically adds, and
 *   for why {@code GET /api/audit-log} prefers this tier's {@code recent(...)} over the in-memory
 *   ring buffer whenever it's configured: it's the only tier that survives a restart and the only
 *   one that's actually queryable rather than just "recent N."</li>
 * </ul>
 * All three can be active at once — the ring buffer and file sink are best-effort and never block
 * or fail a request; a DB write failure is logged and falls back to the other two tiers rather than
 * losing the event or crashing the caller, same "durability adds robustness, never a new failure
 * mode" posture the file sink already established.
 */
public final class AuditLog {

    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);
    private static final int RING_BUFFER_SIZE = 1000;

    private final Deque<AuditEvent> ring = new ArrayDeque<>(RING_BUFFER_SIZE);
    private final BufferedWriter fileSink;
    private final AuditLogStore dbStore;

    public AuditLog() {
        this(null, null);
    }

    AuditLog(BufferedWriter fileSink) {
        this(fileSink, null);
    }

    AuditLog(BufferedWriter fileSink, AuditLogStore dbStore) {
        this.fileSink = fileSink;
        this.dbStore = dbStore;
    }

    /**
     * {@code POLYWIRE_AUDIT_LOG_FILE} unset: no JSONL sink. {@code POLYWIRE_AUDIT_LOG_DB}
     * ({@code jdbcUrl|user|password}, same pipe-delimited shape {@code POLYWIRE_CONFIG_DB} already
     * uses) unset: no DB sink. Either or both may be configured at once; the in-memory ring buffer
     * is always present regardless. Never throws — a misconfigured or unreachable DB degrades to
     * the other tiers rather than failing startup, matching every other optional-durability
     * component's convention in this codebase.
     */
    public static AuditLog fromEnv() {
        BufferedWriter fileSink = null;
        String path = System.getenv("POLYWIRE_AUDIT_LOG_FILE");
        if (path != null && !path.isBlank()) {
            try {
                fileSink = Files.newBufferedWriter(Path.of(path),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                log.info("audit log: appending JSONL to {}", path);
            } catch (IOException e) {
                log.warn("audit log: failed to open {} ({}) — file sink disabled", path, e.getMessage());
            }
        }

        AuditLogStore dbStore = null;
        String dbSpec = System.getenv("POLYWIRE_AUDIT_LOG_DB");
        if (dbSpec != null && !dbSpec.isBlank()) {
            String[] parts = dbSpec.split("\\|", -1);
            String jdbcUrl = parts.length > 0 ? parts[0] : "";
            String user = parts.length > 1 ? parts[1] : null;
            String password = parts.length > 2 ? parts[2] : null;
            try {
                dbStore = new AuditLogStore(jdbcUrl, user, password);
                log.info("audit log: durable, hash-chained sink at {}", jdbcUrl);
            } catch (SQLException e) {
                log.warn("POLYWIRE_AUDIT_LOG_DB configured but could not be reached at startup — "
                        + "falling back to ring-buffer/file sink only for this process: {}", e.toString());
            }
        }

        return new AuditLog(fileSink, dbStore);
    }

    public void record(AuditEvent event) {
        recordInMemory(event);
        if (fileSink != null) {
            writeToFile(event);
        }
        if (dbStore != null) {
            try {
                dbStore.append(event);
            } catch (SQLException e) {
                log.warn("audit log: failed to write event to DB sink ({}) — event still in the "
                        + "in-memory ring buffer{}", e.getMessage(), fileSink != null ? "/file sink" : "");
            }
        }
    }

    private synchronized void recordInMemory(AuditEvent event) {
        if (ring.size() >= RING_BUFFER_SIZE) {
            ring.removeFirst();
        }
        ring.addLast(event);
    }

    private synchronized void writeToFile(AuditEvent event) {
        try {
            fileSink.write(toJson(event).toString());
            fileSink.newLine();
            fileSink.flush();
        } catch (IOException e) {
            log.warn("audit log: failed to write event to file sink ({}) — event still in the in-memory ring buffer", e.getMessage());
        }
    }

    /** Most recent {@code limit} events, newest first. Reads through {@link AuditLogStore} when configured (durable, survives a restart, and the only tier that's genuinely queryable rather than just "whatever's still in memory") — falls back to the in-memory ring buffer on a DB read failure or when no DB sink is configured. */
    public List<AuditEvent> recent(int limit) {
        if (dbStore != null) {
            try {
                return dbStore.recent(limit);
            } catch (SQLException e) {
                log.warn("audit log: failed to read recent events from DB sink ({}) — falling back to the in-memory ring buffer", e.getMessage());
            }
        }
        return recentFromMemory(limit);
    }

    private synchronized List<AuditEvent> recentFromMemory(int limit) {
        List<AuditEvent> result = new ArrayList<>(ring);
        java.util.Collections.reverse(result);
        return result.size() > limit ? result.subList(0, limit) : result;
    }

    /** {@code null} when no DB sink is configured — see {@code ApiRoutes.verifyAuditLog}. */
    public AuditLogStore dbStore() {
        return dbStore;
    }

    public static JsonObject toJson(AuditEvent event) {
        JsonObject json = new JsonObject();
        json.addProperty("timestamp", event.timestamp().toString());
        json.addProperty("type", event.type().name());
        json.addProperty("userId", event.userId());
        json.addProperty("summary", event.summary());
        JsonObject details = new JsonObject();
        event.details().forEach(details::addProperty);
        json.add("details", details);
        return json;
    }
}
