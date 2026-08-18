package com.polygres.wire.mongowire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/**
 * BSON &lt;-&gt; JSONB text bridge. Documents are stored in Postgres using MongoDB Extended JSON
 * (relaxed types like plain numbers/strings/booleans/arrays/nested objects stay plain JSON;
 * MongoDB-specific types like ObjectId/Date/Binary round-trip through their {@code {"$oid": ...}}-
 * style wrapper forms) rather than plain/relaxed JSON, specifically so BSON types with no native
 * JSON equivalent (ObjectId chief among them — every document's default {@code _id}) survive a
 * round trip through Postgres unchanged. mongo-java-server's postgresql-backend module (the
 * reference this frontend follows, BSD-3-Clause) stores documents as plain {@code json} and
 * leaves BSON-specific type fidelity to its own {@code JsonConverter}/Jackson mixins
 * (BinDataJsonMixIn, LegacyUUIDJsonMixIn); this class is this module's much smaller equivalent,
 * built directly on the official BSON library's own extended-JSON writer/reader instead of
 * Jackson, since {@code org.mongodb:bson} already ships one.
 */
final class BsonJson {

    // RELAXED, not EXTENDED: relaxed mode keeps numbers (int32/int64/double) as plain JSON
    // numbers and only wraps genuinely non-JSON-representable types (ObjectId, Date, Binary,
    // Decimal128, ...) in their "$oid"/"$date"-style forms. This matters beyond cosmetics: an
    // earlier version of this class used EXTENDED, which wraps *every* number as
    // {"$numberInt": "7"} — Postgres's jsonb ordering then compared those wrapper *objects*
    // structurally (effectively a lexical string compare on the "$numberInt" value), so
    // {"$gt": 5} matched "15" but not "7" against "5" the same way "1" < "5" < "7" would sort as
    // strings. Found live via MongoQueryTranslator's $gt test against pymongo (see wire's
    // verification notes) and fixed by switching to RELAXED so doc->'qty' is a real jsonb number
    // and Postgres's native numeric jsonb ordering applies.
    private static final JsonWriterSettings EXTENDED =
            JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

    private BsonJson() {
    }

    /** Full-document form, used for the JSONB {@code doc} column. */
    static String toJson(Document document) {
        return document.toJson(EXTENDED);
    }

    static Document fromJson(String json) {
        return Document.parse(json);
    }

    /**
     * Converts a raw {@link BsonDocument} (as read straight off the wire by {@link OpMsgFrame})
     * into a plain {@link Document} whose nested values are ordinary Java types (String, Integer,
     * List, nested Document, ObjectId, ...) rather than {@code BsonValue} wrapper objects — needed
     * anywhere a document coming straight from a command (e.g. an {@code update}'s {@code u}
     * replacement/modifier document) needs to be treated the same way as one round-tripped through
     * {@link #fromJson}. {@code new Document(bsonDocument)} looks like it would do this but
     * doesn't — it just copies the map shallowly, leaving every value as its raw {@code BsonValue};
     * this goes through the driver's own {@link DocumentCodec} instead, which is the real
     * BSON-to-Document decode.
     */
    static Document toDocument(BsonDocument bsonDocument) {
        return new DocumentCodec().decode(new BsonDocumentReader(bsonDocument), DecoderContext.builder().build());
    }

    /**
     * Single-value form, used to bind a filter/update operand as a {@code ?::jsonb} parameter.
     * BSON has no top-level "serialize just a value" writer entry point, so this wraps the value
     * in a throwaway single-field document, serializes that, then pulls the one field back out
     * via a real JSON parser (gson, already a dependency here) rather than string-slicing —
     * correct even when the value itself contains literal braces/colons (nested documents,
     * strings containing punctuation, etc).
     */
    static String valueToJson(BsonValue value) {
        Document wrapper = new Document("v", value);
        String wrapped = wrapper.toJson(EXTENDED);
        JsonObject obj = JsonParser.parseString(wrapped).getAsJsonObject();
        JsonElement v = obj.get("v");
        return v.toString();
    }
}
