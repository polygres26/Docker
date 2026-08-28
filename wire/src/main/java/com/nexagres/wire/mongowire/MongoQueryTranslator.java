package com.nexagres.wire.mongowire;

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
        List<String> clauses = new ArrayList<>();
        List<String> params = new ArrayList<>();
        for (Map.Entry<String, BsonValue> entry : filter.entrySet()) {
            String field = entry.getKey();
            if (field.startsWith("$")) {
                throw unsupported("top-level operator \"" + field + "\" ($or/$and/$nor and friends)");
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
        String sql = clauses.isEmpty() ? "" : " WHERE " + String.join(" AND ", clauses);
        return new Where(sql, params);
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
