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
