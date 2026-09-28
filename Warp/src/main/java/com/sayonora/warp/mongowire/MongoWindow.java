package com.sayonora.warp.mongowire;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonValue;

/** {@code $setWindowFields}: document-based windows and the ranking/shift operators. */
final class MongoWindow {

    private MongoWindow() {
    }

    static Stream<BsonDocument> setWindowFields(BsonValue spec, Stream<BsonDocument> in, MongoAgg.Ctx ctx) {
        BsonDocument d = spec.asDocument();
        MongoExpr.Expr part = d.containsKey("partitionBy") ? MongoExpr.parse(d.get("partitionBy")) : null;
        BsonDocument sortBy = d.containsKey("sortBy") ? d.getDocument("sortBy") : null;
        BsonDocument output = d.getDocument("output");
        return Stream.of((Object) null).flatMap(x -> {
            LinkedHashMap<String, List<BsonDocument>> parts = new LinkedHashMap<>();
            in.forEach(doc -> {
                BsonValue k = part == null ? BsonNull.VALUE : part.eval(ctx.scope(doc));
                parts.computeIfAbsent(BsonCmp.key(k == MongoExpr.MISSING ? BsonNull.VALUE : k, ctx.collation), z -> new ArrayList<>()).add(doc);
            });
            List<BsonDocument> out = new ArrayList<>();
            for (List<BsonDocument> rows : parts.values()) {
                if (sortBy != null) {
                    rows.sort(MongoSort.comparator(sortBy, ctx.collation));
                }
                List<BsonDocument> res = new ArrayList<>();
                for (BsonDocument r : rows) {
                    res.add(r.clone());
                }
                for (Map.Entry<String, BsonValue> o : output.entrySet()) {
                    BsonDocument def = o.getValue().asDocument();
                    String op = null;
                    BsonValue arg = null;
                    BsonValue window = null;
                    for (Map.Entry<String, BsonValue> e : def.entrySet()) {
                        if (e.getKey().equals("window")) {
                            window = e.getValue();
                        } else {
                            op = e.getKey();
                            arg = e.getValue();
                        }
                    }
                    for (int i = 0; i < rows.size(); i++) {
                        BsonValue v;
                        switch (op) {
                            case "$documentNumber":
                                v = new BsonInt32(i + 1);
                                break;
                            case "$rank": case "$denseRank": {
                                int rank = 1;
                                int dense = 1;
                                Comparator<BsonDocument> c = MongoSort.comparator(sortBy, ctx.collation);
                                for (int j = 1; j <= i; j++) {
                                    if (c.compare(rows.get(j - 1), rows.get(j)) != 0) {
                                        rank = j + 1;
                                        dense++;
                                    }
                                }
                                v = new BsonInt32(op.equals("$rank") ? rank : dense);
                                break;
                            }
                            case "$shift": {
                                BsonDocument sd = arg.asDocument();
                                int by = (int) MongoNum.truncLong(sd.get("by"));
                                int j = i + by;
                                MongoExpr.Expr oe = MongoExpr.parse(sd.get("output"));
                                if (j >= 0 && j < rows.size()) {
                                    v = oe.eval(ctx.scope(rows.get(j)));
                                } else {
                                    v = sd.containsKey("default") ? MongoExpr.parse(sd.get("default")).eval(ctx.scope(rows.get(i))) : BsonNull.VALUE;
                                }
                                break;
                            }
                            default: {
                                int lo = 0;
                                int hi = rows.size() - 1;
                                if (window != null) {
                                    BsonDocument w = window.asDocument();
                                    if (!w.containsKey("documents")) {
                                        throw new MongoCmdException(115, "$setWindowFields range windows are not supported by Warp");
                                    }
                                    BsonArray b = w.getArray("documents");
                                    lo = b.get(0).isString() ? (b.get(0).asString().getValue().equals("current") ? i : 0)
                                            : Math.max(0, i + (int) MongoNum.truncLong(b.get(0)));
                                    hi = b.get(1).isString() ? (b.get(1).asString().getValue().equals("current") ? i : rows.size() - 1)
                                            : Math.min(rows.size() - 1, i + (int) MongoNum.truncLong(b.get(1)));
                                }
                                MongoAgg.AccFactory f = MongoAgg.accumulator(o.getKey(), new BsonDocument(op, arg), ctx);
                                MongoAgg.Acc acc = f.create();
                                for (int j = lo; j <= hi; j++) {
                                    acc.add(ctx.scope(rows.get(j)));
                                }
                                v = acc.result();
                            }
                        }
                        res.get(i).put(o.getKey(), v == MongoExpr.MISSING ? BsonNull.VALUE : v);
                    }
                }
                out.addAll(res);
            }
            return out.stream();
        });
    }
}
