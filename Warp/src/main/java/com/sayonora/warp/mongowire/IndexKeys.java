package com.sayonora.warp.mongowire;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonUndefined;
import org.bson.BsonValue;

/** Index key extraction with multikey (array) expansion, used for unique index enforcement. */
final class IndexKeys {

    private IndexKeys() {
    }

    record Entry(String text, List<BsonValue> values) {
    }

    static List<Entry> keys(BsonDocument doc, BsonDocument keyPattern, boolean sparse) {
        return keys(doc, keyPattern, sparse, null);
    }

    static List<Entry> keys(BsonDocument doc, BsonDocument keyPattern, boolean sparse, BsonCmp.Collation coll) {
        List<String> fields = new ArrayList<>(keyPattern.keySet());
        List<List<BsonValue>> perField = new ArrayList<>();
        boolean allMissing = true;
        String arrayField = null;
        for (String f : fields) {
            List<BsonValue> leaves = new ArrayList<>();
            boolean anyArray = false;
            boolean missing = true;
            for (BsonValue c : MongoMatcher.resolve(doc, f)) {
                if (c == MongoMatcher.MISSING) {
                    leaves.add(BsonNull.VALUE);
                } else if (c.isArray()) {
                    missing = false;
                    anyArray = true;
                    if (c.asArray().isEmpty()) {
                        leaves.add(new BsonUndefined());
                    } else {
                        leaves.addAll(c.asArray().getValues());
                    }
                } else {
                    missing = false;
                    leaves.add(c);
                }
            }
            if (leaves.isEmpty()) {
                leaves.add(BsonNull.VALUE);
            }
            if (!missing) {
                allMissing = false;
            }
            if (anyArray) {
                if (arrayField != null) {
                    throw new MongoCmdException(171, "cannot index parallel arrays [" + f + "] [" + arrayField + "]");
                }
                arrayField = f;
            }
            perField.add(leaves);
        }
        if (sparse && allMissing) {
            return List.of();
        }
        List<Entry> out = new ArrayList<>();
        expand(perField, 0, new ArrayList<>(), out, coll);
        // de-duplicate identical keys produced by repeated array elements
        List<Entry> uniq = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (Entry e : out) {
            if (seen.add(e.text)) {
                uniq.add(e);
            }
        }
        return uniq;
    }

    private static void expand(List<List<BsonValue>> perField, int i, List<BsonValue> cur, List<Entry> out, BsonCmp.Collation coll) {
        if (i == perField.size()) {
            StringBuilder sb = new StringBuilder();
            for (BsonValue v : cur) {
                sb.append(BsonCmp.key(v, coll)).append('\u0001');
            }
            out.add(new Entry(sb.toString(), new ArrayList<>(cur)));
            return;
        }
        for (BsonValue v : perField.get(i)) {
            cur.add(v);
            expand(perField, i + 1, cur, out, coll);
            cur.remove(cur.size() - 1);
        }
    }
}
