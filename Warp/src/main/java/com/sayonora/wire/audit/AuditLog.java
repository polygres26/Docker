package com.sayonora.wire.audit;

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

    public static AuditLog fromEnv() {
        BufferedWriter fileSink = null;
        String path = System.getenv("WARP_AUDIT_LOG_FILE");
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
        String dbSpec = System.getenv("WARP_AUDIT_LOG_DB");
        if (dbSpec != null && !dbSpec.isBlank()) {
            String[] parts = dbSpec.split("\\|", -1);
            String jdbcUrl = parts.length > 0 ? parts[0] : "";
            String user = parts.length > 1 ? parts[1] : null;
            String password = parts.length > 2 ? parts[2] : null;
            try {
                dbStore = new AuditLogStore(jdbcUrl, user, password);
                log.info("audit log: durable, hash-chained sink at {}", jdbcUrl);
            } catch (SQLException e) {
                log.warn("WARP_AUDIT_LOG_DB configured but could not be reached at startup — "
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
