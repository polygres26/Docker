package com.polygres.wire.mongowire;

import java.util.Map;
import org.bson.Document;

/**
 * Applies a MongoDB {@code update} document to an existing document in place. Two shapes are
 * supported, matching what {@code update_one}/{@code updateOne} from a real driver sends:
 * <ul>
 *   <li>A <b>modifier document</b> (every top-level key starts with {@code $}) — only
 *       {@code $set} and {@code $unset} are implemented (the two used by the live-verification
 *       test in this pass); {@code $inc}/{@code $push}/etc are intentionally not attempted here,
 *       same "narrow and honest" scope call as {@link MongoQueryTranslator}.</li>
 *   <li>A <b>replacement document</b> (no {@code $}-prefixed keys) — replaces every field except
 *       {@code _id}, which is preserved from the existing document regardless of what the
 *       replacement contains (matches real MongoDB's own replace-document semantics).</li>
 * </ul>
 * Dotted paths (e.g. {@code $set: {"a.b": 1}}) are not supported, consistent with
 * {@link MongoQueryTranslator} not supporting them on the filter side either.
 */
final class UpdateApplier {

    private UpdateApplier() {
    }

    static void apply(Document existing, Document update) {
        boolean isModifier = update.keySet().stream().anyMatch(k -> k.startsWith("$"));
        if (!isModifier) {
            Object id = existing.get("_id");
            existing.clear();
            existing.putAll(update);
            existing.put("_id", id);
            return;
        }
        for (Map.Entry<String, Object> op : update.entrySet()) {
            switch (op.getKey()) {
                case "$set" -> {
                    Document fields = (Document) op.getValue();
                    for (Map.Entry<String, Object> f : fields.entrySet()) {
                        if (f.getKey().contains(".")) {
                            throw new IllegalArgumentException(
                                    "mongowire: dotted $set paths are not supported in this pass");
                        }
                        existing.put(f.getKey(), f.getValue());
                    }
                }
                case "$unset" -> {
                    Document fields = (Document) op.getValue();
                    for (String key : fields.keySet()) {
                        existing.remove(key);
                    }
                }
                default -> throw new IllegalArgumentException(
                        "mongowire: unsupported update operator \"" + op.getKey()
                                + "\" ($set/$unset only in this pass)");
            }
        }
    }
}
