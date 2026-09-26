package com.sayonora.wire.mongowire;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonUndefined;
import org.bson.BsonValue;

/** Sort-key extraction and comparison with MongoDB semantics (array min/max keys, missing as null). */
final class MongoSort {

    private MongoSort() {
    }

    static BsonValue sortValue(BsonDocument doc, String path, boolean ascending) {
        BsonValue best = null;
        for (MongoMatcher.Cand cd : MongoMatcher.resolveX(doc, path)) {
            BsonValue c = cd.v();
            List<BsonValue> leaves = new ArrayList<>();
            if (c == MongoMatcher.MISSING) {
                leaves.add(BsonNull.VALUE);
            } else if (c.isArray() && cd.expand()) {
                if (c.asArray().isEmpty()) {
                    leaves.add(new BsonUndefined());
                } else {
                    leaves.addAll(c.asArray().getValues());
                }
            } else {
                leaves.add(c);
            }
            for (BsonValue l : leaves) {
                if (best == null || (ascending ? BsonCmp.compare(l, best) < 0 : BsonCmp.compare(l, best) > 0)) {
                    best = l;
                }
            }
        }
        return best == null ? BsonNull.VALUE : best;
    }

    static Comparator<BsonDocument> comparator(BsonDocument spec, BsonCmp.Collation coll) {
        List<String> fields = new ArrayList<>();
        List<Boolean> asc = new ArrayList<>();
        for (Map.Entry<String, BsonValue> e : spec.entrySet()) {
            BsonValue v = e.getValue();
            boolean a;
            if (v.isDocument()) {
                a = true; // {$meta: ...} unsupported ordering, treated as ascending
            } else if (BsonCmp.isNumber(v)) {
                a = BsonCmp.compareNumbers(v, new org.bson.BsonInt32(0)) > 0;
                if (e.getKey().startsWith("$")) {
                    throw new MongoCmdException(16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField.");
                }
            } else {
                throw new MongoCmdException(15974, "Illegal key in $sort specification: " + e.getKey() + ": " + v);
            }
            fields.add(e.getKey());
            asc.add(a);
        }
        return (x, y) -> {
            for (int i = 0; i < fields.size(); i++) {
                boolean a = asc.get(i);
                int c = BsonCmp.compare(sortValue(x, fields.get(i), a), sortValue(y, fields.get(i), a), coll);
                if (c != 0) {
                    return a ? c : -c;
                }
            }
            return 0;
        };
    }

    static void validate(BsonDocument spec, String cmd) {
        if (spec.isEmpty()) {
            throw new MongoCmdException(15976, "$sort stage must have at least one sort key");
        }
        for (Map.Entry<String, BsonValue> e : spec.entrySet()) {
            BsonValue v = e.getValue();
            if (v.isDocument()) {
                if (v.asDocument().containsKey("$meta")) {
                    throw new MongoCmdException(40218, "query requires text score metadata, but it is not available");
                }
                continue;
            }
            if (e.getKey().startsWith("$")) {
                throw new MongoCmdException(16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField.");
            }
            if (!BsonCmp.isNumber(v)) {
                throw new MongoCmdException(15974, "Illegal key in $sort specification: " + e.getKey() + ": " + v);
            }
            long l = (long) MongoNum.toDouble(v);
            if (l != 1 && l != -1) {
                throw new MongoCmdException(15975, "$sort key ordering must be specified using a number or {$meta: \"textScore\"}");
            }
        }
    }

}
