package com.nexagres.wire.mongowire;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;

final class OpMsgFrame {

    static final int OP_REPLY = 1;
    static final int OP_QUERY = 2004;
    static final int OP_MSG = 2013;
    private static final int CHECKSUM_PRESENT = 1;

    final int requestId;
    final BsonDocument body;
    final boolean legacyQuery;

    private OpMsgFrame(int requestId, BsonDocument body, boolean legacyQuery) {
        this.requestId = requestId;
        this.body = body;
        this.legacyQuery = legacyQuery;
    }

    static OpMsgFrame read(DataInputStream in) throws IOException {
        byte[] header = new byte[16];
        in.readFully(header);
        ByteBuffer hb = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        int messageLength = hb.getInt();
        int requestId = hb.getInt();
        hb.getInt();
        int opCode = hb.getInt();
        int remaining = messageLength - 16;
        if (remaining <= 0) {
            throw new EOFException("empty message body");
        }
        byte[] rest = new byte[remaining];
        in.readFully(rest);
        ByteBuffer bb = ByteBuffer.wrap(rest).order(ByteOrder.LITTLE_ENDIAN);

        if (opCode == OP_QUERY) {
            bb.getInt();
            readCString(bb);
            bb.getInt();
            bb.getInt();
            BsonDocument query = readOneDocument(bb);
            return new OpMsgFrame(requestId, query, true);
        }
        if (opCode != OP_MSG) {
            throw new IOException("mongowire: unsupported opcode " + opCode + " (only OP_QUERY/2004 for the "
                    + "initial handshake and OP_MSG/2013 for everything after are supported)");
        }

        int flagBits = bb.getInt();
        BsonDocument doc = null;
        
        java.util.List<org.bson.BsonArray> pendingSequences = new java.util.ArrayList<>();
        java.util.List<String> pendingIdentifiers = new java.util.ArrayList<>();
        while (bb.remaining() > (isChecksumPresent(flagBits) ? 4 : 0)) {
            int kind = bb.get() & 0xFF;
            if (kind == 0) {
                doc = readOneDocument(bb);
            } else if (kind == 1) {
                int sectionStart = bb.position();
                int sectionLength = bb.getInt(sectionStart);
                int sectionEnd = sectionStart + sectionLength;
                bb.position(sectionStart + 4);
                String identifier = readCStringValue(bb);
                org.bson.BsonArray docs = new org.bson.BsonArray();
                while (bb.position() < sectionEnd) {
                    docs.add(readOneDocument(bb));
                }
                pendingIdentifiers.add(identifier);
                pendingSequences.add(docs);
            } else {
                throw new IOException("mongowire: unknown OP_MSG section kind " + kind);
            }
        }
        if (doc == null) {
            throw new IOException("OP_MSG had no kind-0 section");
        }
        for (int i = 0; i < pendingIdentifiers.size(); i++) {
            doc.put(pendingIdentifiers.get(i), pendingSequences.get(i));
        }
        return new OpMsgFrame(requestId, doc, false);
    }

    private static void readCString(ByteBuffer bb) {
        while (bb.get() != 0) {
            
        }
    }

    private static String readCStringValue(ByteBuffer bb) {
        int start = bb.position();
        while (bb.get() != 0) {
            
        }
        int end = bb.position() - 1;
        byte[] bytes = new byte[end - start];
        bb.get(start, bytes);
        bb.position(end + 1);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static boolean isChecksumPresent(int flagBits) {
        return (flagBits & CHECKSUM_PRESENT) != 0;
    }

    private static BsonDocument readOneDocument(ByteBuffer bb) {
        
        int len = bb.getInt(bb.position());
        byte[] docBytes = new byte[len];
        bb.get(docBytes);
        return parseRaw(docBytes);
    }

    private static BsonDocument parseRaw(byte[] docBytes) {
        try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(docBytes).order(ByteOrder.LITTLE_ENDIAN))) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.pipe(reader);
            return writer.getDocument();
        }
    }

    private static byte[] encodeDocument(BsonDocument doc) {
        OutputBuffer buf = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(buf)) {
            writer.pipe(new BsonDocumentReader(doc));
        }
        return buf.toByteArray();
    }

    static void writeReply(OutputStream out, int responseTo, BsonDocument replyDoc, boolean legacyQuery) throws IOException {
        byte[] docBytes = encodeDocument(replyDoc);
        byte[] body;
        int opCode;
        if (legacyQuery) {
            ByteBuffer buf = ByteBuffer.allocate(20 + docBytes.length).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(0);
            buf.putLong(0L);
            buf.putInt(0);
            buf.putInt(1);
            buf.put(docBytes);
            body = buf.array();
            opCode = OP_REPLY;
        } else {
            OutputBuffer bodyBuf = new BasicOutputBuffer();
            bodyBuf.writeInt32(0);
            bodyBuf.writeByte(0);
            bodyBuf.writeBytes(docBytes);
            body = bodyBuf.toByteArray();
            opCode = OP_MSG;
        }

        ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(16 + body.length);
        header.putInt(nextResponseId());
        header.putInt(responseTo);
        header.putInt(opCode);
        out.write(header.array());
        out.write(body);
        out.flush();
    }

    private static final java.util.concurrent.atomic.AtomicInteger RESPONSE_IDS =
            new java.util.concurrent.atomic.AtomicInteger(1);

    private static int nextResponseId() {
        return RESPONSE_IDS.getAndIncrement();
    }
}
