package com.polygres.advisor.core;

import java.time.Instant;
import java.util.UUID;

/**
 * One saved connection to a source database -- what the admin UI's "Connections" list manages.
 * Distinct from {@link BackendTarget}: this is the persisted record (has an id, a display name,
 * a createdAt); {@link #toTarget()} is how it becomes the thing {@link CatalogProfiler}-family
 * classes actually connect with.
 *
 * <p><b>Known gap, called out explicitly rather than glossed over:</b> {@link ConnectionStore}
 * persists this record -- including {@code password} -- as plaintext JSON on local disk. That is
 * an MVP shortcut, not a production posture. Real hardening (encryption at rest, or delegating to
 * something like Omnigate's {@code com.omnigate.secrets.SecretResolver}/Vault integration) is a
 * known follow-up; every API response redacts {@code password} to {@code null} (see
 * {@link #redacted()}) so it at least never round-trips back to the browser.
 */
public class ConnectionRecord {
    public String id;
    public String name;
    public String jdbcUrl;
    public String user;
    public String password;
    public String createdAt;

    public ConnectionRecord() {}

    public ConnectionRecord(String name, String jdbcUrl, String user, String password) {
        this.id = UUID.randomUUID().toString();
        this.name = name;
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.createdAt = Instant.now().toString();
    }

    public SourceDialect dialect() {
        return toTarget().dialect();
    }

    public BackendTarget toTarget() {
        return new BackendTarget(id, jdbcUrl, user, password);
    }

    /** Copy with {@code password} nulled out -- what every API response sends to the browser. */
    public ConnectionRecord redacted() {
        ConnectionRecord copy = new ConnectionRecord();
        copy.id = id;
        copy.name = name;
        copy.jdbcUrl = jdbcUrl;
        copy.user = user;
        copy.password = null;
        copy.createdAt = createdAt;
        return copy;
    }
}
