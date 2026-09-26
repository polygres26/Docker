package com.sayonora.warp.core.access;

import com.sayonora.warp.core.AccessContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * What Warp itself last applied to one PHYSICAL backend connection (per-session {@code set_config}
 * identity for RLS, {@code db_emulation}, the orawire tenant {@code search_path}).
 *
 * <p>Before connection multiplexing a session owned one pooled connection for its whole life, so the
 * initializers cached "already applied" per session. With multiplexing a session borrows a possibly
 * different physical connection per statement and many sessions share each one, so the only correct
 * place to remember what is applied is the physical connection itself: this registry, keyed by the
 * driver connection's IDENTITY (the pool's per-borrow proxy is unwrapped, otherwise every borrow would
 * look like a new connection and defeat the cache), weakly so an evicted connection is collected.
 *
 * <p>Correctness rule the initializers follow: a session never trusts what it applied last time, only
 * what is recorded here for the connection it holds NOW. Same connection + same wanted state = zero
 * round trips; anything else is reconciled (applied, or cleared for an identity-less session) before the
 * statement runs. {@link #invalidate} marks everything unknown after a rollback (a rolled-back
 * transaction also rolls back {@code set_config(..., false)} calls made inside it).
 */
public final class PhysicalSessionState {

    /** Mutable per-physical-connection record; only ever touched by the one thread currently holding the
     * connection, so it needs no locking of its own. */
    public static final class State {
        /** {@code db_emulation} value last set ("oracle", "mysql", "sqlserver") or {@code null} for none. */
        public String emulation;
        public boolean emulationUnknown;
        /** Access context whose {@code warp.*} settings are applied; {@code null} = none applied. */
        public AccessContext rls;
        /** Every {@code warp.*} setting name that may currently hold a value on this connection. */
        public final Set<String> rlsKeys = new HashSet<>();
        public boolean rlsUnknown;
        /** Session settings ({@code SET name = value}) currently applied to the connection, name -> statement,
         * as replayed for / issued by the last session that used it (see SessionConnectionLease). */
        public java.util.Map<String, String> settings = java.util.Map.of();
        public boolean settingsUnknown;
        /** Access context last forwarded into pg_oracle's SYS_CONTEXT store (orawire only). */
        public AccessContext sysContext;
        /** Tenant schema set by orawire's {@code SET search_path}, {@code null} = session default. */
        public String tenantPath;
        public boolean pathUnknown;

        public void invalidate() {
            emulationUnknown = true;
            rlsUnknown = true;
            pathUnknown = true;
        }

        /** After {@code RESET ALL}: every setting Warp itself had applied is gone as well. */
        public void clearWarpApplied() {
            rlsKeys.clear();
            rls = null;
            rlsUnknown = false;
            emulation = null;
            emulationUnknown = false;
            tenantPath = null;
            pathUnknown = false;
        }

        /** True if a session with NO identity / no emulation must still clean something off this connection. */
        public boolean dirtyForPlainSession() {
            return !rlsKeys.isEmpty() || emulation != null || tenantPath != null;
        }

        /** True if a caller with no settings of its own must {@code RESET ALL} before using this connection. */
        public boolean hasSettings() {
            return !settings.isEmpty() || settingsUnknown;
        }
    }

    private static final Map<Object, State> STATES = Collections.synchronizedMap(new WeakHashMap<>());

    /** The driver-level connection behind a pool proxy (or the connection itself). */
    public static Object physicalKey(Connection connection) {
        try {
            Connection raw = connection.unwrap(Connection.class);
            return raw == null ? connection : raw;
        } catch (SQLException | RuntimeException e) {
            return connection;
        }
    }

    public static State of(Connection connection) {
        Object key = physicalKey(connection);
        State state = STATES.get(key);
        if (state == null) {
            state = new State();
            STATES.put(key, state);
        }
        return state;
    }

    /** Peek without creating a record (null = nothing was ever applied to this connection). */
    public static State peek(Connection connection) {
        return STATES.get(physicalKey(connection));
    }

    /** Forget what is known about {@code connection} -- call after a rollback or a failed transaction. */
    public static void invalidate(Connection connection) {
        State state = peek(connection);
        if (state != null) {
            state.invalidate();
        }
    }

    private PhysicalSessionState() {
    }
}
