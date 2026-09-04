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
            if (value.isDocument() && hasOperatorKeys(value.asDocument())) {
                for (Map.Entry<String, BsonValue> op : value.asDocument().entrySet()) {
                    clauses.add(operatorClause(column, op.getKey(), op.getValue(), params));
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

    private static String operatorClause(String column, String op, BsonValue value, List<String> params) {
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
            default -> throw unsupported("operator \"" + op + "\" ($regex/$elemMatch/geo/$exists/$type and others "
                    + "are not implemented in this pass)");
        }
    }

    private static String fieldExpr(String field) {
        
        return "doc->'" + field.replace("'", "''") + "'";
    }

    private static IllegalArgumentException unsupported(String what) {
        return new IllegalArgumentException("mongowire: unsupported filter — " + what);
    }
}
