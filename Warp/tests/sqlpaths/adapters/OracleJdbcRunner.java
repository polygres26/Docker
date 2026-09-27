// Single-file JDBC (ojdbc11 thin) scenario runner for the sqlpaths matrix framework.
// Contract: reads one JSON object from stdin describing the connection + scenario, writes one
// JSON object to stdout with the canonical result. Kept dependency-free (no JSON library) with a
// tiny hand-rolled parser/writer, since only javac + ojdbc11 are available (no Maven fetch).
//
// stdin shape:
//   {"url":"jdbc:oracle:thin:@//host:port/service","user":"u","password":"p",
//    "setup":["...","..."],"sql":"...","teardown":["..."],"binds":["1","2"],"expectRows":true}
// stdout shape (success):  {"ok":true,"columns":["A","B"],"rows":[["1","x"],["2","y"]]}
// stdout shape (error):    {"ok":false,"errorCode":"ORA-00942","errorText":"..."}
import java.sql.*;
import java.util.*;
import java.io.*;

public class OracleJdbcRunner {
    public static void main(String[] args) throws Exception {
        StringBuilder sb = new StringBuilder();
        BufferedReader r = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        String line;
        while ((line = r.readLine()) != null) sb.append(line).append('\n');
        Map<String, Object> req = MiniJson.parseObject(sb.toString());

        String url = (String) req.get("url");
        String user = (String) req.get("user");
        String password = (String) req.get("password");
        String sql = (String) req.get("sql");
        List<Object> setup = (List<Object>) req.getOrDefault("setup", Collections.emptyList());
        List<Object> teardown = (List<Object>) req.getOrDefault("teardown", Collections.emptyList());
        List<Object> binds = (List<Object>) req.get("binds");
        boolean expectRows = Boolean.TRUE.equals(req.get("expectRows"));

        Map<String, Object> out = new LinkedHashMap<>();
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                for (Object s : setup) st.execute((String) s);
            }
            try {
                if (binds != null) {
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        for (int i = 0; i < binds.size(); i++) ps.setObject(i + 1, binds.get(i));
                        boolean hasRs = ps.execute();
                        if (hasRs && expectRows) {
                            writeResultSet(ps.getResultSet(), out);
                        } else {
                            out.put("ok", true);
                        }
                    }
                } else {
                    try (Statement st = conn.createStatement()) {
                        boolean hasRs = st.execute(sql);
                        if (hasRs && expectRows) {
                            writeResultSet(st.getResultSet(), out);
                        } else {
                            out.put("ok", true);
                        }
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                out.put("ok", false);
                String code = e.getMessage() != null && e.getMessage().contains("ORA-")
                        ? "ORA-" + e.getMessage().replaceAll(".*ORA-(\\d{5}).*", "$1")
                        : String.valueOf(e.getErrorCode());
                out.put("errorCode", code);
                out.put("errorText", e.getMessage());
            } finally {
                try (Statement st = conn.createStatement()) {
                    for (Object s : teardown) {
                        try {
                            st.execute((String) s);
                            conn.commit();
                        } catch (SQLException ignore) {
                            conn.rollback();
                        }
                    }
                } catch (SQLException ignore) {
                }
            }
        } catch (SQLException e) {
            out.put("ok", false);
            out.put("errorCode", String.valueOf(e.getErrorCode()));
            out.put("errorText", e.getMessage());
        } finally {
            if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
        }
        System.out.println(MiniJson.write(out));
    }

    private static void writeResultSet(ResultSet rs, Map<String, Object> out) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int n = md.getColumnCount();
        List<String> cols = new ArrayList<>();
        for (int i = 1; i <= n; i++) cols.add(md.getColumnName(i).toUpperCase());
        List<List<String>> rows = new ArrayList<>();
        while (rs.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= n; i++) {
                Object v = rs.getObject(i);
                row.add(v == null ? null : v.toString());
            }
            rows.add(row);
        }
        out.put("ok", true);
        out.put("columns", cols);
        out.put("rows", rows);
    }

    // ---- tiny JSON helpers (object/array/string/number/bool/null only; enough for this contract) ----
    static class MiniJson {
        static Map<String, Object> parseObject(String s) {
            int[] p = {0};
            skipWs(s, p);
            return (Map<String, Object>) parseValue(s, p);
        }
        static Object parseValue(String s, int[] p) {
            skipWs(s, p);
            char c = s.charAt(p[0]);
            if (c == '{') return parseObj(s, p);
            if (c == '[') return parseArr(s, p);
            if (c == '"') return parseStr(s, p);
            if (s.startsWith("true", p[0])) { p[0] += 4; return Boolean.TRUE; }
            if (s.startsWith("false", p[0])) { p[0] += 5; return Boolean.FALSE; }
            if (s.startsWith("null", p[0])) { p[0] += 4; return null; }
            int start = p[0];
            while (p[0] < s.length() && "-+.0123456789eE".indexOf(s.charAt(p[0])) >= 0) p[0]++;
            String num = s.substring(start, p[0]);
            return num.contains(".") ? (Object) Double.parseDouble(num) : (Object) Long.parseLong(num);
        }
        static Map<String, Object> parseObj(String s, int[] p) {
            Map<String, Object> m = new LinkedHashMap<>();
            p[0]++; skipWs(s, p);
            if (s.charAt(p[0]) == '}') { p[0]++; return m; }
            while (true) {
                skipWs(s, p);
                String key = parseStr(s, p);
                skipWs(s, p); p[0]++; // ':'
                Object val = parseValue(s, p);
                m.put(key, val);
                skipWs(s, p);
                if (s.charAt(p[0]) == ',') { p[0]++; continue; }
                p[0]++; break; // '}'
            }
            return m;
        }
        static List<Object> parseArr(String s, int[] p) {
            List<Object> l = new ArrayList<>();
            p[0]++; skipWs(s, p);
            if (s.charAt(p[0]) == ']') { p[0]++; return l; }
            while (true) {
                Object val = parseValue(s, p);
                l.add(val);
                skipWs(s, p);
                if (s.charAt(p[0]) == ',') { p[0]++; continue; }
                p[0]++; break; // ']'
            }
            return l;
        }
        static String parseStr(String s, int[] p) {
            p[0]++; // opening quote
            StringBuilder sb = new StringBuilder();
            while (s.charAt(p[0]) != '"') {
                char c = s.charAt(p[0]);
                if (c == '\\') {
                    p[0]++;
                    char e = s.charAt(p[0]);
                    switch (e) {
                        case 'n': sb.append('\n'); break;
                        case 't': sb.append('\t'); break;
                        case 'r': sb.append('\r'); break;
                        case '"': sb.append('"'); break;
                        case '\\': sb.append('\\'); break;
                        case 'u':
                            sb.append((char) Integer.parseInt(s.substring(p[0] + 1, p[0] + 5), 16));
                            p[0] += 4; break;
                        default: sb.append(e);
                    }
                } else sb.append(c);
                p[0]++;
            }
            p[0]++; // closing quote
            return sb.toString();
        }
        static void skipWs(String s, int[] p) {
            while (p[0] < s.length() && Character.isWhitespace(s.charAt(p[0]))) p[0]++;
        }
        static String write(Object o) {
            StringBuilder sb = new StringBuilder();
            writeVal(o, sb);
            return sb.toString();
        }
        static void writeVal(Object o, StringBuilder sb) {
            if (o == null) { sb.append("null"); return; }
            if (o instanceof String) { writeStr((String) o, sb); return; }
            if (o instanceof Boolean || o instanceof Number) { sb.append(o); return; }
            if (o instanceof Map) {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) {
                    if (!first) sb.append(',');
                    first = false;
                    writeStr(String.valueOf(e.getKey()), sb);
                    sb.append(':');
                    writeVal(e.getValue(), sb);
                }
                sb.append('}');
                return;
            }
            if (o instanceof List) {
                sb.append('[');
                boolean first = true;
                for (Object v : (List<?>) o) {
                    if (!first) sb.append(',');
                    first = false;
                    writeVal(v, sb);
                }
                sb.append(']');
                return;
            }
            writeStr(o.toString(), sb);
        }
        static void writeStr(String s, StringBuilder sb) {
            sb.append('"');
            for (char c : s.toCharArray()) {
                switch (c) {
                    case '"': sb.append("\\\""); break;
                    case '\\': sb.append("\\\\"); break;
                    case '\n': sb.append("\\n"); break;
                    case '\r': sb.append("\\r"); break;
                    case '\t': sb.append("\\t"); break;
                    default:
                        if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                        else sb.append(c);
                }
            }
            sb.append('"');
        }
    }
}
