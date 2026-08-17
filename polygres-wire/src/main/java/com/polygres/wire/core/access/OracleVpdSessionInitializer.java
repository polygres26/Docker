package com.polygres.wire.core.access;

import com.polygres.wire.core.AccessContext;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Oracle Virtual Private Database (VPD) native RLS pass-through — calls a DBA-owned, DBA-written
 * PL/SQL procedure (default name {@code polywire_ctx_pkg.set_attribute(p_name, p_value)}) once per
 * {@link AccessContext} attribute; a VPD policy function (created and attached with
 * {@code DBMS_RLS.ADD_POLICY}, also DBA-owned) reads the resulting session context back with
 * {@code SYS_CONTEXT(namespace, attribute)} to build its predicate. Same division of
 * responsibility as {@link PostgresRlsSessionInitializer}: PolyWire populates context faithfully
 * through a procedure the DBA controls, the database's own VPD engine does the enforcement.
 *
 * <p><b>Found live, and load-bearing for this design</b>: calling
 * {@code DBMS_SESSION.SET_CONTEXT(namespace, attribute, value)} <em>directly</em> from PolyWire's
 * own connection fails with {@code ORA-01031: insufficient privileges} for any context declared
 * {@code CREATE CONTEXT ... USING <package>} (the standard, secure VPD pattern) unless the caller
 * <em>is</em> that trusted package — Oracle enforces this deliberately, so an arbitrary session
 * can't forge another namespace's context values. The only correct integration is exactly what
 * this class does: call a procedure the DBA wrote (inside the trusted package, or one that wraps
 * it) rather than {@code DBMS_SESSION.SET_CONTEXT} itself — confirmed against a real Oracle
 * instance (`sb-oracle`, Oracle Database Free 23ai) during this feature's live-verification pass;
 * an earlier version of this class called {@code DBMS_SESSION.SET_CONTEXT} directly and was wrong.
 * The DBA-owned procedure is expected to look like:
 * <pre>{@code
 * CREATE OR REPLACE PACKAGE polywire_ctx_pkg AS
 *   PROCEDURE set_attribute(p_name IN VARCHAR2, p_value IN VARCHAR2);
 * END;
 * CREATE OR REPLACE PACKAGE BODY polywire_ctx_pkg AS
 *   PROCEDURE set_attribute(p_name IN VARCHAR2, p_value IN VARCHAR2) IS
 *   BEGIN
 *     DBMS_SESSION.SET_CONTEXT('POLYWIRE_CTX', p_name, p_value);
 *   END;
 * END;
 * }</pre>
 * with {@code EXECUTE} granted to whatever database user PolyWire connects to this backend as.
 */
public final class OracleVpdSessionInitializer implements NativeRlsSessionInitializer {

    private final String setAttributeProcedure;

    public OracleVpdSessionInitializer() {
        this("polywire_ctx_pkg.set_attribute");
    }

    /** {@code setAttributeProcedure}: fully qualified {@code package.procedure} name, taking {@code (p_name VARCHAR2, p_value VARCHAR2)} — see class javadoc for the expected shape. */
    public OracleVpdSessionInitializer(String setAttributeProcedure) {
        this.setAttributeProcedure = setAttributeProcedure;
    }

    @Override
    public void initialize(Connection connection, AccessContext accessContext) throws SQLException {
        setAttribute(connection, "user_id", accessContext.userId());
        for (var entry : accessContext.attributes().entrySet()) {
            setAttribute(connection, entry.getKey(), entry.getValue());
        }
    }

    private void setAttribute(Connection connection, String name, String value) throws SQLException {
        try (CallableStatement stmt = connection.prepareCall("{call " + setAttributeProcedure + "(?, ?)}")) {
            stmt.setString(1, name);
            stmt.setString(2, value);
            stmt.execute();
        }
    }
}
