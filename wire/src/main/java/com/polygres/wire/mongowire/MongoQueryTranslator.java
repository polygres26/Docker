package com.polygres.wire.mongowire;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Translates a MongoDB {@code find}/{@code update}/{@code delete} filter document into a
 * Postgres {@code WHERE} clause against the {@code doc jsonb} column, using {@code ->} to reach a
 * top-level field's JSONB value and comparing it against a {@code ?::jsonb} bound parameter
 * (bound as extended-JSON text via {@link BsonJson#valueToJson}). Postgres's jsonb type has a
 * full, well-defined ordering (the same one that backs jsonb btree indexes and
 * {@code ORDER BY ... jsonb_column}), so {@code =, <>, <, <=, >, >=} all work directly on two
 * jsonb operands without needing a scalar cast for numbers/strings — that ordering is what
 * backs the comparison operators below.
 *
 * <p><b>Scope, stated plainly</b>: this is real semantic translation, not syntax substitution,
 * but it is intentionally narrow for this first pass:
 * <ul>
 *   <li>Covered: implicit top-level equality ({@code {field: value}}), {@code $eq}, {@code $ne},
 *       {@code $gt}, {@code $gte}, {@code $lt}, {@code $lte}, {@code $in} (as an OR of
 *       equalities), top-level implicit AND across multiple fields in one filter document.</li>
 *   <li><b>Not covered</b> (rejected with a clear "unsupported operator" error rather than
 *       silently ignored or mismatched): {@code $regex}, {@code $elemMatch}, geo operators,
 *       {@code $or}/{@code $and}/{@code $nor} as explicit top-level operators, dotted paths into
 *       nested documents/arrays, {@code $exists}, {@code $type}, and anything from the
 *       aggregation pipeline (out of scope for this whole frontend, not just this class — see
 *       {@code MongoWireSessionHandler}'s class javadoc).</li>
 * </ul>
 * mongo-java-server's postgresql-backend reference does no SQL-level filtering at all — it
 * streams every row back into the JVM and matches MongoDB filters there in Java. This class
 * deliberately diverges from that: pushing at least the common equality/comparison/`$in` cases
 * down into a real SQL {@code WHERE} clause is more honest about what a "Postgres-backed
 * MongoDB" frontend should do, and was cheap enough to build in this pass; anything outside this
 * class's covered set still needs an explicit error rather than a silent full-table return.
 */
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
        // '->' (not '->>') so the comparison is jsonb-vs-jsonb, letting Postgres's native jsonb
        // ordering do numeric/string comparisons correctly instead of forcing a text compare.
        return "doc->'" + field.replace("'", "''") + "'";
    }

    private static IllegalArgumentException unsupported(String what) {
        return new IllegalArgumentException("mongowire: unsupported filter — " + what);
    }
}
