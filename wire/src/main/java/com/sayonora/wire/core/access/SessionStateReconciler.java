package com.sayonora.wire.core.access;

import com.sayonora.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Brings a physical backend connection's Warp-applied session state (see {@link PhysicalSessionState})
 * to what the session about to use it needs -- and nothing more. Every method is a no-op (zero round
 * trips) when the recorded state already matches, which is the steady state for a session that keeps
 * getting the same pooled connection back.
 *
 * <p>The security-relevant guarantee lives here: {@link #applyRls} either sets the borrower's
 * {@code warp.*} identity or, for an identity-less borrower, CLEARS whatever a previous borrower of the
 * same physical connection left behind, so one client's identity can never leak to another.
 */
public final class SessionStateReconciler {

    /** Sets {@code warp.user_id}/{@code warp.<attr>} for {@code ctx} (one round trip for all of them) and
     * clears any warp.* name a previous borrower set that {@code ctx} does not. When
     * {@code applyAnonymous} is false an anonymous {@code ctx} means "no identity": nothing is set, but
     * leftovers are still cleared. */
    public static void applyRls(Connection connection, PhysicalSessionState.State st, AccessContext ctx,
            boolean applyAnonymous) throws SQLException {
        boolean wantNone = ctx.isAnonymous() && !applyAnonymous;
        if (wantNone) {
            if (st.rlsKeys.isEmpty()) {
                st.rls = null;
                st.rlsUnknown = false;
                return;
            }
        } else if (!st.rlsUnknown && ctx.equals(st.rls)) {
            return;
        }
        Map<String, String> want = new LinkedHashMap<>();
        if (!wantNone) {
            want.put("warp.user_id", ctx.userId());
            for (var entry : ctx.attributes().entrySet()) {
                want.put("warp." + entry.getKey(), entry.getValue());
            }
        }
        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        want.forEach((k, v) -> {
            names.add(k);
            values.add(v);
        });
        for (String stale : st.rlsKeys) {
            if (!want.containsKey(stale)) {
                names.add(stale);
                values.add("");
            }
        }
        if (!names.isEmpty()) {
            StringBuilder sql = new StringBuilder("SELECT ");
            for (int i = 0; i < names.size(); i++) {
                sql.append(i == 0 ? "" : ", ").append("set_config(?, ?, false)");
            }
            try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
                for (int i = 0; i < names.size(); i++) {
                    stmt.setString(2 * i + 1, names.get(i));
                    stmt.setString(2 * i + 2, values.get(i));
                }
                stmt.execute();
            }
        }
        st.rlsKeys.clear();
        st.rlsKeys.addAll(want.keySet());
        st.rls = wantNone ? null : ctx;
        st.rlsUnknown = false;
    }

    /** {@code SET db_emulation = '<value>'} unless already recorded. The extension's assign hook
     * reconciles search_path on every SET, which is why a changed tenant path marks emulation unknown. */
    public static void ensureEmulation(Connection connection, PhysicalSessionState.State st, String value)
            throws SQLException {
        if (!st.emulationUnknown && value.equals(st.emulation)) {
            return;
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET db_emulation = '" + value + "'");
        }
        st.emulation = value;
        st.emulationUnknown = false;
    }

    /** For a session that wants no orawire tenant search_path: undo one a previous orawire borrower of this
     * physical connection left on it. */
    public static void clearTenantPath(Connection connection, PhysicalSessionState.State st) throws SQLException {
        if (st.tenantPath != null) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("RESET search_path");
            }
            st.tenantPath = null;
            st.pathUnknown = false;
            // the emulation extension appended schemas to the search_path that was just reset
            if (st.emulation != null) {
                st.emulationUnknown = true;
            }
        }
    }

    /** For a session that wants NO emulation and NO tenant search_path (pgwire): undo whatever a previous
     * mywire/mssqlwire/orawire borrower of this physical connection left on it. */
    public static void clearForPlainSession(Connection connection, PhysicalSessionState.State st)
            throws SQLException {
        clearTenantPath(connection, st);
        if (st.emulation != null) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("RESET db_emulation");
            }
            st.emulation = null;
            st.emulationUnknown = false;
        }
    }

    /** Strips EVERY Warp-applied setting (identity, emulation, tenant path) from a freshly borrowed connection
     * that belongs to a caller with no session-state handling of its own (internal statements, HTTP
     * frontends, MCP, ...), so a wire client's identity or dialect emulation can never bleed into them.
     * No-op when nothing was applied (the common case): one map lookup. */
    public static void cleanse(Connection connection) throws SQLException {
        PhysicalSessionState.State st = PhysicalSessionState.peek(connection);
        if (st == null || !(st.dirtyForPlainSession() || st.hasSettings())) {
            return;
        }
        if (st.hasSettings()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("RESET ALL");
            }
            st.settings = java.util.Map.of();
            st.settingsUnknown = false;
            st.clearWarpApplied();
            return;
        }
        StringBuilder sql = new StringBuilder();
        for (String key : st.rlsKeys) {
            sql.append("SELECT set_config('").append(key.replace("'", "''")).append("', '', false);");
        }
        if (st.tenantPath != null) {
            sql.append("RESET search_path;");
        }
        if (st.emulation != null) {
            sql.append("RESET db_emulation;");
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql.toString());
        }
        st.rlsKeys.clear();
        st.rls = null;
        st.rlsUnknown = false;
        st.tenantPath = null;
        st.pathUnknown = false;
        st.emulation = null;
        st.emulationUnknown = false;
    }

    /** {@code SET search_path TO "<schema>", public} (orawire tenant schema) unless already recorded. */
    public static void ensureTenantPath(Connection connection, PhysicalSessionState.State st, String schema)
            throws SQLException {
        if (!st.pathUnknown && schema.equals(st.tenantPath)) {
            return;
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET search_path TO \"" + schema + "\", public");
        }
        st.tenantPath = schema;
        st.pathUnknown = false;
        // SET search_path wipes the schemas db_emulation appended; force it to be re-asserted.
        if (st.emulation != null) {
            st.emulationUnknown = true;
        }
    }

    private SessionStateReconciler() {
    }
}
