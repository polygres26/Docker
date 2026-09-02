package com.sayonora.dms.core;

import java.time.Instant;
import java.util.UUID;

/**
 * One saved connection to a source database -- what the admin UI's "Connections" list manages.
 * Distinct from {@link BackendTarget}: this is the persisted record (has an id, a display name,
 * a createdAt); {@link #toTarget()} is how it becomes the thing {@link CatalogProfiler}-family
 * classes actually connect with.
 *
 * <p>{@code password} is encrypted at rest (AES-256-GCM, opt-in via {@code
 * SAYONORA_ENCRYPTION_KEY} -- see {@link ConnectionStore}'s own javadoc) by {@link
 * ConnectionStore}, or can instead be a {@code vault:}/{@code cyberark:}/{@code awssm:}/
 * {@code azurekv:}/{@code gcpsm:} secret reference resolved at connect time (see {@code
 * com.sayonora.dms.secrets.SecretResolver} and {@link BackendTarget#open}) so the real credential
 * is never stored here at all. Either way, every API response redacts {@code password} to
 * {@code null} (see {@link #redacted()}) so it never round-trips back to the browser.
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
