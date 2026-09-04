package com.sayonora.wire.mongowire;

import java.util.Map;
import org.bson.Document;

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
                        setDotted(existing, f.getKey(), f.getValue());
                    }
                }
                case "$unset" -> {
                    Document fields = (Document) op.getValue();
                    for (String key : fields.keySet()) {
                        unsetDotted(existing, key);
                    }
                }
                default -> throw new IllegalArgumentException(
                        "mongowire: unsupported update operator \"" + op.getKey()
                                + "\" ($set/$unset only in this pass)");
            }
        }
    }

    /** Navigates (creating intermediate nested {@link Document}s as needed, real MongoDB's own
     * behavior for {@code $set}) a dot-separated path like {@code "address.city"} and sets the
     * leaf field. Deliberately scoped to nested-document paths only, matching the class's own
     * "not a full parser" discipline elsewhere: a segment that names an existing non-Document
     * value (e.g. treating an array element by numeric index, or descending through a scalar) is
     * refused rather than guessed at, since silently overwriting a scalar with a Document -- or
     * vice versa -- would corrupt the document in a way that's easy to miss without a real
     * round-trip test. */
    private static void setDotted(Document root, String path, Object value) {
        if (!path.contains(".")) {
            root.put(path, value);
            return;
        }
        String[] segments = path.split("\\.");
        Document cursor = root;
        for (int i = 0; i < segments.length - 1; i++) {
            Object next = cursor.get(segments[i]);
            if (next == null) {
                Document created = new Document();
                cursor.put(segments[i], created);
                cursor = created;
            } else if (next instanceof Document nextDoc) {
                cursor = nextDoc;
            } else {
                throw new IllegalArgumentException("mongowire: cannot set dotted path \"" + path
                        + "\" -- \"" + segments[i] + "\" is not a nested document (array-index "
                        + "segments are not supported in this pass)");
            }
        }
        cursor.put(segments[segments.length - 1], value);
    }

    /** Same navigation discipline as {@link #setDotted} but for {@code $unset}: a missing
     * intermediate segment (nothing there to remove from) is a silent no-op, matching real
     * MongoDB's own {@code $unset} semantics for a path that doesn't exist. */
    private static void unsetDotted(Document root, String path) {
        if (!path.contains(".")) {
            root.remove(path);
            return;
        }
        String[] segments = path.split("\\.");
        Document cursor = root;
        for (int i = 0; i < segments.length - 1; i++) {
            Object next = cursor.get(segments[i]);
            if (next == null) {
                return; // nothing to unset
            }
            if (!(next instanceof Document nextDoc)) {
                throw new IllegalArgumentException("mongowire: cannot unset dotted path \"" + path
                        + "\" -- \"" + segments[i] + "\" is not a nested document (array-index "
                        + "segments are not supported in this pass)");
            }
            cursor = nextDoc;
        }
        cursor.remove(segments[segments.length - 1]);
    }
}
