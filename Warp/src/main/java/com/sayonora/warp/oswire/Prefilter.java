package com.sayonora.warp.oswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A SQL predicate over {@code doc_id}/{@code source} that is a SUPERSET of what a query can match, so Postgres discards
 * obviously irrelevant rows before they are transferred and parsed (the engine still evaluates the real query on every
 * row it gets -- this never changes results). Covered: {@code ids}, {@code term}/{@code terms} on top-level keyword and integer
 * fields (and {@code _id}), {@code range} on top-level integer fields, and {@code bool} (must/filter AND, all-convertible
 * should OR) / {@code constant_score} of those. Everything else means "no restriction".
 */
final class Prefilter {

    private static final Pattern SIMPLE_FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_@\\-]*");

    private Prefilter() {
    }

    private record Frag(String sql, List<Object> params) {
    }

    static PostgresSearchStore.SqlFilter of(Query.Node q, Mappings m) {
        Frag f = frag(q, m);
        return f == null ? null : new PostgresSearchStore.SqlFilter(f.sql(), f.params());
    }

    private static boolean plain(Mappings.Field f, String path) {
        return f != null && SIMPLE_FIELD.matcher(path).matches() && f.aliasTarget == null && !f.subField && f.nestedPath == null
                && f.path.equals(path);
    }

    private static Frag frag(Query.Node n, Mappings m) {
        if (n instanceof Query.Named nm) {
            return frag(nm.inner, m);
        }
        if (n instanceof Query.ConstantScore cs) {
            return frag(cs.inner, m);
        }
        if (n instanceof Query.Ids ids) {
            if (ids.ids.isEmpty()) {
                return new Frag("FALSE", List.of());
            }
            List<Object> params = new ArrayList<>(ids.ids);
            return new Frag("doc_id IN (" + "?,".repeat(params.size()).replaceAll(",$", "") + ")", params);
        }
        if (n instanceof Query.Term t && !t.ci) {
            return terms(t.field, List.of(t.value), m);
        }
        if (n instanceof Query.Terms t) {
            return terms(t.field, t.values, m);
        }
        if (n instanceof Query.Range r) {
            return range(r, m);
        }
        if (n instanceof Query.Bool b) {
            List<Frag> parts = new ArrayList<>();
            for (Query.Node c : b.must) {
                Frag f = frag(c, m);
                if (f != null) {
                    parts.add(f);
                }
            }
            for (Query.Node c : b.filter) {
                Frag f = frag(c, m);
                if (f != null) {
                    parts.add(f);
                }
            }
            if (b.must.isEmpty() && b.filter.isEmpty() && !b.should.isEmpty() && b.msm == null) {
                List<Frag> ors = new ArrayList<>();
                for (Query.Node c : b.should) {
                    Frag f = frag(c, m);
                    if (f == null) {
                        ors = null;
                        break;
                    }
                    ors.add(f);
                }
                if (ors != null) {
                    parts.add(join(ors, " OR "));
                }
            }
            return parts.isEmpty() ? null : join(parts, " AND ");
        }
        return null;
    }

    private static Frag join(List<Frag> parts, String op) {
        if (parts.size() == 1) {
            return parts.get(0);
        }
        StringBuilder sb = new StringBuilder("(");
        List<Object> params = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(op);
            }
            sb.append('(').append(parts.get(i).sql()).append(')');
            params.addAll(parts.get(i).params());
        }
        return new Frag(sb.append(')').toString(), params);
    }

    private static Frag terms(String field, List<JsonElement> values, Mappings m) {
        if (field.equals("_id")) {
            if (values.isEmpty()) {
                return new Frag("FALSE", List.of());
            }
            List<Object> params = new ArrayList<>();
            for (JsonElement v : values) {
                params.add(v.getAsString());
            }
            return new Frag("doc_id IN (" + "?,".repeat(params.size()).replaceAll(",$", "") + ")", params);
        }
        Mappings.Field f = m.get(field);
        if (!plain(f, field) || values.isEmpty()) {
            return values.isEmpty() && plain(f, field) ? new Frag("FALSE", List.of()) : null;
        }
        boolean keyword = f.type.equals("keyword") && !(f.def != null && f.def.has("normalizer"));
        boolean integer = f.type.equals("long") || f.type.equals("integer") || f.type.equals("short") || f.type.equals("byte");
        if (!keyword && !integer) {
            return null;
        }
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        for (JsonElement v : values) {
            if (!v.isJsonPrimitive()) {
                return null;
            }
            JsonPrimitive p = v.getAsJsonPrimitive();
            if (sql.length() > 0) {
                sql.append(" OR ");
            }
            if (keyword) {
                if (p.isBoolean() || p.isNumber()) {
                    return null; // stringification of numbers/booleans is subtle: no restriction
                }
                sql.append("source -> '").append(field).append("' @> ?::jsonb");
                params.add(new JsonPrimitive(p.getAsString()).toString());
            } else {
                BigDecimal bd;
                try {
                    bd = p.isNumber() ? p.getAsBigDecimal() : new BigDecimal(p.getAsString().trim());
                } catch (NumberFormatException e) {
                    return null;
                }
                // a document may carry the number as a JSON string (coerced on index)
                sql.append("(source -> '").append(field).append("' @> ?::jsonb OR source -> '").append(field).append("' @> ?::jsonb)");
                params.add(bd.stripTrailingZeros().toPlainString());
                params.add(new JsonPrimitive(bd.toPlainString()).toString());
            }
        }
        return new Frag(sql.toString(), params);
    }

    private static Frag range(Query.Range r, Mappings m) {
        Mappings.Field f = m.get(r.field);
        if (!plain(f, r.field) || !(f.type.equals("long") || f.type.equals("integer") || f.type.equals("short") || f.type.equals("byte"))) {
            return null;
        }
        String col = "source -> '" + r.field + "'";
        List<String> conds = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        Object[][] bounds = {{r.gt, ">"}, {r.gte, ">="}, {r.lt, "<"}, {r.lte, "<="}};
        for (Object[] b : bounds) {
            JsonElement e = (JsonElement) b[0];
            if (e == null || e.isJsonNull()) {
                continue;
            }
            BigDecimal bd;
            try {
                bd = e.getAsJsonPrimitive().isNumber() ? e.getAsBigDecimal() : new BigDecimal(e.getAsString().trim());
            } catch (RuntimeException ex) {
                return null;
            }
            conds.add("(" + col + ")::text::numeric " + b[1] + " ?");
            params.add(bd);
        }
        if (conds.isEmpty()) {
            return null;
        }
        // arrays / strings / missing values are passed through: only plain JSON numbers are compared in SQL
        return new Frag("CASE WHEN jsonb_typeof(" + col + ") = 'number' THEN " + String.join(" AND ", conds) + " ELSE TRUE END", params);
    }
}
