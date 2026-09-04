package com.sayonora.wire.mongowire;

import java.util.ArrayList;
import java.util.List;
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
                // $inc/$push/$pull/$addToSet -- real gap, found auditing this frontend for GA
                // transparency: counters ($inc) and array manipulation ($push/$pull) are used in
                // nearly every non-trivial app, and were refused outright before this. Same
                // dotted-path navigation discipline as $set/$unset above (no array-index path
                // segments -- see setDotted's own javadoc).
                case "$inc" -> {
                    Document fields = (Document) op.getValue();
                    for (Map.Entry<String, Object> f : fields.entrySet()) {
                        Number delta = (Number) f.getValue();
                        Object current = getDotted(existing, f.getKey());
                        Number currentNum = current == null ? 0 : (Number) current;
                        setDotted(existing, f.getKey(), addNumbers(currentNum, delta));
                    }
                }
                case "$push" -> {
                    Document fields = (Document) op.getValue();
                    for (Map.Entry<String, Object> f : fields.entrySet()) {
                        List<Object> list = asMutableList(getDotted(existing, f.getKey()), f.getKey(), "$push");
                        Object rawValue = f.getValue();
                        if (rawValue instanceof Document mod && mod.containsKey("$each")) {
                            for (Object v : (List<?>) mod.get("$each")) {
                                list.add(v);
                            }
                        } else {
                            list.add(rawValue);
                        }
                        setDotted(existing, f.getKey(), list);
                    }
                }
                case "$pull" -> {
                    Document fields = (Document) op.getValue();
                    for (Map.Entry<String, Object> f : fields.entrySet()) {
                        List<Object> list = asMutableList(getDotted(existing, f.getKey()), f.getKey(), "$pull");
                        Object toRemove = f.getValue();
                        list.removeIf(existingElement -> java.util.Objects.equals(existingElement, toRemove));
                        setDotted(existing, f.getKey(), list);
                    }
                }
                case "$addToSet" -> {
                    Document fields = (Document) op.getValue();
                    for (Map.Entry<String, Object> f : fields.entrySet()) {
                        List<Object> list = asMutableList(getDotted(existing, f.getKey()), f.getKey(), "$addToSet");
                        Object value = f.getValue();
                        if (list.stream().noneMatch(existingElement -> java.util.Objects.equals(existingElement, value))) {
                            list.add(value);
                        }
                        setDotted(existing, f.getKey(), list);
                    }
                }
                default -> throw new IllegalArgumentException(
                        "mongowire: unsupported update operator \"" + op.getKey()
                                + "\" ($set/$unset/$inc/$push/$pull/$addToSet only in this pass)");
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

    /** Read-only counterpart to {@link #setDotted}/{@link #unsetDotted} -- same dotted-path
     * navigation, {@code null} for a missing path (or a missing intermediate segment) rather than
     * throwing, matching how {@code $inc}/{@code $push}/etc. treat a field that doesn't exist yet
     * (real MongoDB's own behavior: {@code $inc} on a missing field starts from 0, {@code $push}
     * on a missing field starts a new array). */
    private static Object getDotted(Document root, String path) {
        if (!path.contains(".")) {
            return root.get(path);
        }
        String[] segments = path.split("\\.");
        Object cursor = root;
        for (String segment : segments) {
            if (!(cursor instanceof Document doc)) {
                return null;
            }
            cursor = doc.get(segment);
        }
        return cursor;
    }

    private static Number addNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double || a instanceof Float || b instanceof Float) {
            return a.doubleValue() + b.doubleValue();
        }
        if (a instanceof Long || b instanceof Long) {
            return a.longValue() + b.longValue();
        }
        return a.intValue() + b.intValue();
    }

    /** {@code $push}/{@code $pull}/{@code $addToSet} all need a real, mutable {@link List} to
     * operate on -- a missing field starts a fresh empty array (real MongoDB's own behavior for
     * {@code $push [$addToSet]} on a field that doesn't exist yet; for {@code $pull} an empty
     * list is simply a no-op removal, also matching real behavior). A field that DOES exist but
     * isn't an array is refused rather than silently coerced or overwritten. */
    @SuppressWarnings("unchecked")
    private static List<Object> asMutableList(Object current, String field, String op) {
        if (current == null) {
            return new ArrayList<>();
        }
        if (current instanceof List<?> list) {
            return new ArrayList<>((List<Object>) list);
        }
        throw new IllegalArgumentException("mongowire: " + op + " on \"" + field
                + "\" -- the existing value is not an array");
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
