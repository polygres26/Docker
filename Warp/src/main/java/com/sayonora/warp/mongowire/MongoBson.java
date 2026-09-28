package com.sayonora.warp.mongowire;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.io.BasicOutputBuffer;

/** Raw BSON (de)serialization helpers. */
final class MongoBson {

    private MongoBson() {
    }

    static byte[] encode(BsonDocument doc) {
        BasicOutputBuffer buf = new BasicOutputBuffer();
        try (BsonBinaryWriter w = new BsonBinaryWriter(buf)) {
            w.pipe(new BsonDocumentReader(doc));
        }
        return buf.toByteArray();
    }

    static BsonDocument decode(byte[] bytes) {
        try (BsonBinaryReader r = new BsonBinaryReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN))) {
            BsonDocumentWriter w = new BsonDocumentWriter(new BsonDocument());
            w.pipe(r);
            return w.getDocument();
        }
    }
}
