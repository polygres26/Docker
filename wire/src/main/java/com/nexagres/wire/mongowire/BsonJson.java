package com.nexagres.wire.mongowire;

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

final class BsonJson {

    private static final JsonWriterSettings EXTENDED =
            JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

    private BsonJson() {
    }

    static String toJson(Document document) {
        return document.toJson(EXTENDED);
    }

    static Document fromJson(String json) {
        return Document.parse(json);
    }

    static Document toDocument(BsonDocument bsonDocument) {
        return new DocumentCodec().decode(new BsonDocumentReader(bsonDocument), DecoderContext.builder().build());
    }

    static String valueToJson(BsonValue value) {
        Document wrapper = new Document("v", value);
        String wrapped = wrapper.toJson(EXTENDED);
        JsonObject obj = JsonParser.parseString(wrapped).getAsJsonObject();
        JsonElement v = obj.get("v");
        return v.toString();
    }
}
