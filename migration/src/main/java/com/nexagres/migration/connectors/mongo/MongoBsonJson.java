package com.nexagres.migration.connectors.mongo;

import com.google.gson.JsonParser;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/**
 * A faithful reproduction of mongowire's own {@code BsonJson} serialization convention (see
 * {@code com.nexagres.wire.mongowire.BsonJson} in the {@code wire} module) -- NOT independent
 * reinvention. This has to produce byte-identical output to what mongowire itself writes for the
 * exact same document, or a document written via mongowire and the same document replicated via
 * this connector would land under two different {@code id}/{@code doc} text representations in
 * the same physical table, silently defeating both {@code RowCache} key matching and simple
 * equality. {@code wire}'s own {@code BsonJson} is package-private (deliberately -- it's an
 * internal serialization detail of {@code PostgresDocumentStore}), so this connector reproduces
 * the same few lines against the public {@code org.bson}/Gson APIs rather than reaching into
 * wire's internals, which is exactly the module boundary this package is meant to respect.
 */
final class MongoBsonJson {

    private static final JsonWriterSettings RELAXED =
            JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

    private MongoBsonJson() {
    }

    static String toJson(Document document) {
        return document.toJson(RELAXED);
    }

    /** Works for either a plain Java value (a change-stream {@code fullDocument}'s own {@code
     * _id}, decoded by the driver's codec into a String/ObjectId/etc.) or a raw {@link BsonValue}
     * (a delete event's {@code documentKey} field, which the driver never decodes) -- both paths
     * wrap the value in a throwaway {@code {"v": ...}} document and extract just that field's own
     * JSON text, the same trick {@code BsonJson.valueToJson} uses. */
    static String valueToJson(Object idValue) {
        Document wrapper = new Document("v", idValue);
        String wrapped = wrapper.toJson(RELAXED);
        return JsonParser.parseString(wrapped).getAsJsonObject().get("v").toString();
    }
}
