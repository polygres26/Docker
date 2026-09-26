package com.sayonora.wire.mongowire;

import java.util.Map;
import org.bson.BsonValue;
import org.bson.BsonDocument;

/** Renders values the way mongod's error messages do, e.g. {@code { _id: ObjectId('..'), a: "x" }}. */
final class MongoFmt {

    private MongoFmt() {
    }

    static String value(BsonValue v) {
        switch (v.getBsonType()) {
            case STRING: return "\"" + v.asString().getValue() + "\"";
            case INT32: return Integer.toString(v.asInt32().getValue());
            case INT64: return Long.toString(v.asInt64().getValue());
            case DOUBLE: return MongoExpr.doubleToString(v.asDouble().getValue());
            case DECIMAL128: return "NumberDecimal(\"" + v.asDecimal128().getValue() + "\")";
            case BOOLEAN: return v.asBoolean().getValue() ? "true" : "false";
            case NULL: return "null";
            case UNDEFINED: return "undefined";
            case OBJECT_ID: return "ObjectId('" + v.asObjectId().getValue().toHexString() + "')";
            case DATE_TIME: return "new Date(" + v.asDateTime().getValue() + ")";
            case TIMESTAMP: return "Timestamp(" + v.asTimestamp().getTime() + ", " + v.asTimestamp().getInc() + ")";
            case MIN_KEY: return "MinKey";
            case MAX_KEY: return "MaxKey";
            case BINARY: return "BinData(" + (v.asBinary().getType() & 0xFF) + ", "
                    + java.util.Base64.getEncoder().encodeToString(v.asBinary().getData()).toUpperCase(java.util.Locale.ROOT) + ")";
            case REGULAR_EXPRESSION: return "/" + v.asRegularExpression().getPattern() + "/" + v.asRegularExpression().getOptions();
            case DOCUMENT: return doc(v.asDocument());
            case ARRAY: {
                if (v.asArray().isEmpty()) {
                    return "[]";
                }
                StringBuilder sb = new StringBuilder("[ ");
                boolean first = true;
                for (BsonValue e : v.asArray()) {
                    if (!first) {
                        sb.append(", ");
                    }
                    first = false;
                    sb.append(value(e));
                }
                return sb.append(" ]").toString();
            }
            default: return v.toString();
        }
    }

    static String doc(BsonDocument d) {
        if (d.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{ ");
        boolean first = true;
        for (Map.Entry<String, BsonValue> e : d.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(e.getKey()).append(": ").append(value(e.getValue()));
        }
        return sb.append(" }").toString();
    }

    /** {@code {a: 5}} style single-element rendering used in path errors. */
    static String elem(String name, BsonValue v) {
        return "{" + name + ": " + value(v) + "}";
    }
}
