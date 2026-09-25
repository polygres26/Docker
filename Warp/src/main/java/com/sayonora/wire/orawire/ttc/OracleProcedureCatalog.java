package com.sayonora.wire.orawire.ttc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves a PL/SQL procedure or function's parameter directions (IN/OUT/IN OUT) from Oracle's own
 * data dictionary ({@code ALL_ARGUMENTS}), since orawire's TTC bind descriptors don't carry
 * direction on the wire -- confirmed live via byte-level capture (see {@code
 * RequestLoop#handlePlSqlExecute}'s own javadoc): a real ojdbc {@code CallableStatement} call's
 * bind descriptors for an IN, an OUT, and an OUT REF CURSOR parameter are byte-for-byte identical
 * except for the type code itself. Without knowing which positions are OUT/IN OUT, {@code
 * java.sql.CallableStatement.registerOutParameter} can't be called correctly.
 *
 * <p>Scope, deliberately narrow: resolves by {@code OBJECT_NAME} alone (no owner/package
 * qualification), matching the simple {@code BEGIN proc_name(:1, :2, :3); END;} shape ojdbc
 * produces for {@code {call proc_name(?, ?, ?)}}. A name that resolves to more than one procedure
 * (overloaded, or the same name in multiple schemas visible to this connection) is refused rather
 * than guessed at -- same "refuse rather than risk a wrong decode" discipline used throughout this
 * package. Package-qualified and schema-qualified procedure calls are a real, disclosed follow-up
 * gap, not silently mishandled.
 */
public final class OracleProcedureCatalog {

    /** @param position 1-based, matching the bind's own position in the call.
     *  @param inOut one of {@code "IN"}, {@code "OUT"}, {@code "IN/OUT"} (Oracle's own
     *      {@code ALL_ARGUMENTS.IN_OUT} values).
     *  @param dataType Oracle's own data type name (e.g. {@code "NUMBER"}, {@code "REF CURSOR"}),
     *      used to pick the right {@code registerOutParameter} JDBC type. */
    public record ArgumentInfo(int position, String inOut, String dataType) {
        public boolean isOut() {
            return "OUT".equals(inOut) || "IN/OUT".equals(inOut);
        }

        public boolean isIn() {
            return "IN".equals(inOut) || "IN/OUT".equals(inOut);
        }
    }

    private static final Pattern PROC_CALL_PATTERN = Pattern.compile(
            "^\\s*BEGIN\\s+([A-Za-z0-9_$]+)\\s*\\(", Pattern.CASE_INSENSITIVE);

    // Session-scoped: cheap to recompute per connection, and a schema's procedure signatures
    // don't change mid-session for any real client -- avoids one dictionary round trip per call
    // for a procedure invoked repeatedly (the overwhelmingly common case).
    private final Map<String, List<ArgumentInfo>> cache = new ConcurrentHashMap<>();

    /** Extracts the called procedure's bare name from a PL/SQL anonymous block shaped like
     * {@code BEGIN proc_name(:1, :2, :3); END;} -- the shape ojdbc's own {@code CallableStatement}
     * produces for {@code {call proc_name(?, ?, ?)}}, confirmed live via byte capture. Returns
     * {@code null} for any other shape (a real anonymous block with actual PL/SQL logic, not a
     * single procedure call) -- those aren't resolvable against a single procedure's signature and
     * are out of this class's scope entirely. */
    public static String extractProcedureName(String plsqlBlock) {
        Matcher m = PROC_CALL_PATTERN.matcher(plsqlBlock);
        return m.find() ? m.group(1) : null;
    }

    /** @throws SQLException if the name doesn't resolve to exactly one procedure/function's
     *      argument list, or the dictionary query itself fails. */
    public List<ArgumentInfo> resolve(Connection oracleConnection, String procedureName) throws SQLException {
        String key = procedureName.toUpperCase(java.util.Locale.ROOT);
        List<ArgumentInfo> cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        List<ArgumentInfo> resolved = queryDictionary(oracleConnection, key);
        cache.put(key, resolved);
        return resolved;
    }

    private List<ArgumentInfo> queryDictionary(Connection oracleConnection, String upperName) throws SQLException {
        // DATA_LEVEL = 0: top-level parameters only, not nested %ROWTYPE/record fields (those need
        // real PL/SQL record marshaling this class doesn't attempt). POSITION = 0 is the function
        // return value itself (absent for a plain procedure) -- excluded here since a return value
        // isn't one of the call's own bind placeholders.
        String sql = "SELECT DISTINCT OBJECT_ID FROM ALL_ARGUMENTS WHERE OBJECT_NAME = ? AND DATA_LEVEL = 0";
        try (PreparedStatement idCheck = oracleConnection.prepareStatement(sql)) {
            idCheck.setString(1, upperName);
            int distinctObjects = 0;
            try (ResultSet rs = idCheck.executeQuery()) {
                while (rs.next()) {
                    distinctObjects++;
                }
            }
            if (distinctObjects == 0) {
                throw new SQLException("orawire: procedure \"" + upperName + "\" not found in "
                        + "ALL_ARGUMENTS -- cannot resolve its parameter directions for PL/SQL "
                        + "execution");
            }
            if (distinctObjects > 1) {
                throw new SQLException("orawire: procedure name \"" + upperName + "\" is ambiguous "
                        + "(resolves to " + distinctObjects + " distinct objects, e.g. an overload "
                        + "or the same name visible in multiple schemas) -- package/schema-qualified "
                        + "calls are not supported by this narrow slice");
            }
        }

        List<ArgumentInfo> args = new ArrayList<>();
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "SELECT POSITION, IN_OUT, DATA_TYPE FROM ALL_ARGUMENTS "
                        + "WHERE OBJECT_NAME = ? AND DATA_LEVEL = 0 AND POSITION > 0 ORDER BY POSITION")) {
            ps.setString(1, upperName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    args.add(new ArgumentInfo(rs.getInt("POSITION"), rs.getString("IN_OUT"),
                            rs.getString("DATA_TYPE")));
                }
            }
        }
        return args;
    }
}
