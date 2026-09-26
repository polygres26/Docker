package com.sayonora.warp.core;

import com.sayonora.warp.core.access.PhysicalSessionState;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Wipes the backend session state a pinned session created ({@link SessionStatePins}) off a physical
 * connection before it goes back to the pool: {@code RESET ALL} (SET, search_path, db_emulation, ...),
 * {@code RESET SESSION AUTHORIZATION}/{@code RESET ROLE} (which RESET ALL deliberately skips), {@code DISCARD TEMP} (temp tables), {@code DISCARD SEQUENCES} (currval /
 * lastval), {@code CLOSE ALL} (WITH HOLD cursors), {@code UNLISTEN *} and {@code pg_advisory_unlock_all()}.
 * This is {@code DISCARD ALL} minus {@code DEALLOCATE ALL}, which would invalidate the JDBC driver's own
 * server-side prepared statements on the connection. Each command runs on its own (DISCARD cannot share a
 * multi-command message). Cost: paid once when a pinned session ends.
 *
 * <p>Not covered: SQL-level {@code PREPARE name} statements (names are not tracked) and extension-private
 * session state (e.g. pg_oracle's DBMS_OUTPUT buffer).
 */
public final class SessionStateReset {

    private static final String[] COMMANDS = {
            "RESET ALL", "RESET SESSION AUTHORIZATION", "RESET ROLE", "DISCARD TEMP", "DISCARD SEQUENCES", "CLOSE ALL", "UNLISTEN *",
            "SELECT pg_advisory_unlock_all()"
    };

    public static void reset(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            for (String command : COMMANDS) {
                stmt.execute(command);
            }
        }
        // everything Warp had applied is gone too: the connection is pristine
        PhysicalSessionState.State st = PhysicalSessionState.peek(connection);
        if (st != null) {
            st.rlsKeys.clear();
            st.rls = null;
            st.rlsUnknown = false;
            st.emulation = null;
            st.emulationUnknown = false;
            st.tenantPath = null;
            st.pathUnknown = false;
            st.sysContext = null;
            st.settings = java.util.Map.of();
            st.settingsUnknown = false;
        }
    }

    private SessionStateReset() {
    }
}
