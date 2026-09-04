package com.sayonora.wire.mongowire;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonValue;

final class MongoQueryTranslator {

    record Where(String sql, List<String> jsonbParams) {
        static final Where MATCH_ALL = new Where("", List.of());
    }

    private MongoQueryTranslator() {
    }

    static Where translate(BsonDocument filter) {
        if (filter == null || filter.isEmpty()) {
            return Where.MATCH_ALL;
        }
        List<String> params = new ArrayList<>();
        List<String> clauses = translateClauses(filter, params);
        String sql = clauses.isEmpty() ? "" : " WHERE " + String.join(" AND ", clauses);
        return new Where(sql, params);
    }

    /** One clause per top-level filter key, ANDed together by the caller -- {@link #translate}
     * for the outermost filter, or recursively for each {@code $or}/{@code $and}/{@code $nor}
     * member document below. Real gap this closes, found auditing this frontend for GA
     * transparency: {@code $or}/{@code $and}/{@code $nor} were refused outright, an extremely
     * common real filter shape ("status = active OR priority = high"). Scope, deliberately
     * narrow, matching this class's own existing restrictions: {@code $or}/{@code $and}/{@code
     * $nor}'s own value must be a real array of sub-filter documents (real MongoDB's own shape);
     * each sub-filter is translated with this SAME method, so {@code $or}/{@code $and} can nest,
     * but a sub-filter's own fields are still subject to every restriction ordinary top-level
     * fields already have (no dotted paths, no further nested logical operators mixed with plain
     * fields in a way this wouldn't already handle correctly via plain recursion). */
    private static List<String> translateClauses(BsonDocument filter, List<String> params) {
        List<String> clauses = new ArrayList<>();
        for (Map.Entry<String, BsonValue> entry : filter.entrySet()) {
            String field = entry.getKey();
            if ("$or".equals(field) || "$and".equals(field) || "$nor".equals(field)) {
                clauses.add(translateLogicalOperator(field, entry.getValue(), params));
                continue;
            }
            if (field.startsWith("$")) {
                throw unsupported("top-level operator \"" + field + "\" (only $or/$and/$nor are supported "
                        + "in this pass, not e.g. $expr/$where)");
            }
            if (field.contains(".")) {
                throw unsupported("dotted field path \"" + field + "\"");
            }
            BsonValue value = entry.getValue();
            String column = fieldExpr(field);
            if (value.isRegularExpression()) {
                // {field: /pattern/flags} -- a real BSON regex value used DIRECTLY, not wrapped
                // in {field: {$regex: ...}}. Real driver helpers (Filters.regex(...)) send this
                // shape, confirmed live: without this branch it fell through to the plain
                // equality case below, comparing the field to a JSON-encoded regex object that
                // could never match a real string value.
                clauses.add(operatorClause(field, column, "$regex", value, params));
            } else if (value.isDocument() && hasOperatorKeys(value.asDocument())) {
                for (Map.Entry<String, BsonValue> op : value.asDocument().entrySet()) {
                    clauses.add(operatorClause(field, column, op.getKey(), op.getValue(), params));
                }
            } else {
                clauses.add(column + " = ?::jsonb");
                params.add(BsonJson.valueToJson(value));
            }
        }
        return clauses;
    }

    private static String translateLogicalOperator(String op, BsonValue arrayValue, List<String> params) {
        if (!arrayValue.isArray() || arrayValue.asArray().isEmpty()) {
            throw unsupported(op + " (expected a non-empty array of sub-filter documents)");
        }
        List<String> memberSql = new ArrayList<>();
        for (BsonValue member : arrayValue.asArray()) {
            if (!member.isDocument()) {
                throw unsupported(op + " member (expected a sub-filter document)");
            }
            List<String> memberClauses = translateClauses(member.asDocument(), params);
            memberSql.add(memberClauses.isEmpty() ? "TRUE" : "(" + String.join(" AND ", memberClauses) + ")");
        }
        // $or/$nor join their members with OR ($nor negates that whole disjunction below); $and
        // joins with AND.
        String combined = "$and".equals(op)
                ? String.join(" AND ", memberSql)
                : String.join(" OR ", memberSql);
        return "$nor".equals(op) ? "NOT (" + combined + ")" : "(" + combined + ")";
    }

    static String exactIdEquality(BsonDocument filter) {
        if (filter == null || filter.size() != 1) {
            return null;
        }
        Map.Entry<String, BsonValue> entry = filter.entrySet().iterator().next();
        if (!"_id".equals(entry.getKey())) {
            return null;
        }
        BsonValue value = entry.getValue();
        if (value.isDocument() && hasOperatorKeys(value.asDocument())) {
            return null;
        }
        return BsonJson.valueToJson(value);
    }

    private static boolean hasOperatorKeys(BsonDocument doc) {
        return !doc.isEmpty() && doc.getFirstKey().startsWith("$");
    }

    private static String operatorClause(String field, String column, String op, BsonValue value, List<String> params) {
        switch (op) {
            case "$eq" -> {
                params.add(BsonJson.valueToJson(value));
                return column + " = ?::jsonb";
            }
            case "$ne" -> {
                params.add(BsonJson.valueToJson(value));
                return column + " <> ?::jsonb";
            }
            case "$gt" -> {
                params.add(BsonJson.valueToJson(value));
                return column + " > ?::jsonb";
            }
            case "$gte" -> {
                params.add(BsonJson.valueToJson(value));
                return column + " >= ?::jsonb";
            }
            case "$lt" -> {
                params.add(BsonJson.valueToJson(value));
                return column + " < ?::jsonb";
            }
            case "$lte" -> {
                params.add(BsonJson.valueToJson(value));
                return column + " <= ?::jsonb";
            }
            case "$in" -> {
                if (!value.isArray()) {
                    throw unsupported("$in with a non-array operand");
                }
                List<String> alternatives = new ArrayList<>();
                for (BsonValue v : value.asArray()) {
                    alternatives.add(column + " = ?::jsonb");
                    params.add(BsonJson.valueToJson(v));
                }
                return alternatives.isEmpty() ? "FALSE" : "(" + String.join(" OR ", alternatives) + ")";
            }
            // $exists/$regex -- real gap, found auditing this frontend for GA transparency: both
            // are near-universal in real query filters (optional-field checks, partial text
            // search) and were refused outright before this.
            case "$exists" -> {
                if (!value.isBoolean()) {
                    throw unsupported("$exists with a non-boolean operand");
                }
                // jsonb_exists(doc, 'field'), NOT the "?" containment operator -- "?" collides
                // with JDBC's own bind-placeholder syntax in the SQL text this becomes, so the
                // function form is used instead. Checks real KEY PRESENCE, not "value is non-
                // null" -- a field explicitly set to JSON null still counts as existing, matching
                // real MongoDB's own $exists semantics.
                String existsExpr = "jsonb_exists(doc, " + quoteLiteral(field) + ")";
                return value.asBoolean().getValue() ? existsExpr : "NOT " + existsExpr;
            }
            case "$regex" -> {
                String pattern;
                String flags;
                if (value.isRegularExpression()) {
                    pattern = value.asRegularExpression().getPattern();
                    flags = value.asRegularExpression().getOptions();
                } else if (value.isString()) {
                    pattern = value.asString().getValue();
                    flags = "";
                } else {
                    throw unsupported("$regex operand (expected a string or a real BSON regex)");
                }
                // POSIX regex match against the field's TEXT form (->>, not ->) -- ~* for
                // MongoDB's "i" (case-insensitive) flag, ~ otherwise. Other Mongo-specific flags
                // (m/s/x/u) aren't translated -- Postgres's own POSIX regex engine has a
                // different multiline/dotall story than PCRE, so silently mapping those would
                // risk a subtly wrong match rather than an honest refusal.
                if (flags.chars().anyMatch(ch -> "msxu".indexOf(ch) >= 0)) {
                    throw unsupported("$regex flag(s) \"" + flags + "\" (only \"i\" is supported in this pass)");
                }
                // A literal, not a bind parameter -- every OTHER param in this class is bound as
                // a jsonb-typed value (see PostgresDocumentStore#bindParams), which would corrupt
                // a plain text regex pattern (or fail outright, since an arbitrary pattern is
                // rarely valid JSON on its own). Escaped the same way every other literal in this
                // package is (quoteLiteral, doubling embedded single quotes).
                return textFieldExpr(field) + (flags.indexOf('i') >= 0 ? " ~* " : " ~ ") + quoteLiteral(pattern);
            }
            default -> throw unsupported("operator \"" + op + "\" ($elemMatch/geo/$type and others "
                    + "are not implemented in this pass)");
        }
    }

    private static String fieldExpr(String field) {

        return "doc->'" + field.replace("'", "''") + "'";
    }

    /** {@code doc->>'field'} (jsonb's TEXT extraction, not {@code ->}'s own jsonb-typed one) --
     * needed for {@code $regex}, which matches against a string's actual characters, not its
     * jsonb-quoted-and-escaped representation. */
    private static String textFieldExpr(String field) {
        return "doc->>'" + field.replace("'", "''") + "'";
    }

    private static String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static IllegalArgumentException unsupported(String what) {
        return new IllegalArgumentException("mongowire: unsupported filter — " + what);
    }
}
