package com.polygres.wire.mongowire;

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

/**
 * Reads and writes MongoDB wire protocol frames. {@code OP_MSG} (opcode 2013) is the only
 * *command* format this frontend actually executes against — see the class-level scope note in
 * {@link MongoCommandDispatcher}. However, real modern drivers (pymongo among them, confirmed
 * live) still open every new connection with exactly one legacy {@code OP_QUERY} (opcode 2004)
 * against {@code admin.$cmd} carrying the initial {@code hello}/{@code isMaster} handshake —
 * they don't yet know the server speaks {@code OP_MSG} until that first reply tells them so via
 * {@code maxWireVersion} — falling back to {@code OP_MSG} for every command after that. A server
 * that only ever accepts {@code OP_MSG} therefore cannot be connected to by any real client at
 * all; {@link #read} accordingly parses both opcodes into the same {@link Frame} shape (tagging
 * which one arrived), and {@link #writeReply} answers in kind ({@code OP_REPLY} for a legacy
 * query, {@code OP_MSG} otherwise) so the handshake completes and every command after it is plain
 * {@code OP_MSG}, exactly as with a real server. This is the one piece of "legacy" the class
 * javadoc's opening claim doesn't quite hold for a real client — noted here rather than left as a
 * silent contradiction.
 *
 * <p>Frame layouts (all little-endian), per the MongoDB wire protocol spec:
 * <pre>
 * MsgHeader { int32 messageLength; int32 requestID; int32 responseTo; int32 opCode; }
 * OP_MSG    { MsgHeader header; uint32 flagBits; Sections... ; [uint32 checksum]; }
 * OP_QUERY  { MsgHeader header; int32 flags; cstring fullCollectionName; int32 numberToSkip;
 *             int32 numberToReturn; BSON query; [BSON returnFieldsSelector]; }
 * OP_REPLY  { MsgHeader header; int32 responseFlags; int64 cursorID; int32 startingFrom;
 *             int32 numberReturned; BSON documents[numberReturned]; }
 * </pre>
 * Only single "kind 0" {@code OP_MSG} sections (one BSON document, the shape every command this
 * frontend supports — hello/ping/insert/find/update/delete — uses) are read; a "kind 1"
 * document-sequence section (used by drivers for bulk unordered inserts as a separate
 * optimization) is rejected with a clear error rather than silently mishandled, since none of the
 * pymongo calls this frontend was verified against produce one for single-document
 * insert_one/insertOne-style calls.
 */
final class OpMsgFrame {

    static final int OP_REPLY = 1;
    static final int OP_QUERY = 2004;
    static final int OP_MSG = 2013;
    private static final int CHECKSUM_PRESENT = 1; // flagBit 0

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
        hb.getInt(); // responseTo, unused on requests
        int opCode = hb.getInt();
        int remaining = messageLength - 16;
        if (remaining <= 0) {
            throw new EOFException("empty message body");
        }
        byte[] rest = new byte[remaining];
        in.readFully(rest);
        ByteBuffer bb = ByteBuffer.wrap(rest).order(ByteOrder.LITTLE_ENDIAN);

        if (opCode == OP_QUERY) {
            bb.getInt(); // flags, unused (no slaveOk/exhaust handling needed for a single-node backend)
            readCString(bb); // fullCollectionName (e.g. "admin.$cmd") — unused, command name is inside the doc
            bb.getInt(); // numberToSkip
            bb.getInt(); // numberToReturn
            BsonDocument query = readOneDocument(bb);
            return new OpMsgFrame(requestId, query, true);
        }
        if (opCode != OP_MSG) {
            throw new IOException("mongowire: unsupported opcode " + opCode + " (only OP_QUERY/2004 for the "
                    + "initial handshake and OP_MSG/2013 for everything after are supported)");
        }

        int flagBits = bb.getInt();
        BsonDocument doc = null;
        // kind-1 sections (identifier + a run of documents, e.g. "documents"/"updates"/"deletes")
        // are how every real driver actually sends insert/update/delete's array argument — even
        // for a single-document insert_one, confirmed live against pymongo, which is why this is
        // handled rather than left as the "not supported" stub an earlier draft of this class had.
        // Each is merged into the main kind-0 document under its identifier key as a BsonArray.
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
            // skip to the terminating NUL
        }
    }

    private static String readCStringValue(ByteBuffer bb) {
        int start = bb.position();
        while (bb.get() != 0) {
            // scan to the terminating NUL
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
        // BSON documents self-describe their own length as the first int32; peek it (absolute
        // get, doesn't move position) so exactly the right number of bytes is sliced off.
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

    /**
     * Writes a reply matching the request's own opcode: {@code OP_REPLY} for a legacy
     * {@code OP_QUERY} handshake, {@code OP_MSG} (single kind-0 section, no checksum) for
     * everything else — see this class's javadoc for why both are needed.
     */
    static void writeReply(OutputStream out, int responseTo, BsonDocument replyDoc, boolean legacyQuery) throws IOException {
        byte[] docBytes = encodeDocument(replyDoc);
        byte[] body;
        int opCode;
        if (legacyQuery) {
            ByteBuffer buf = ByteBuffer.allocate(20 + docBytes.length).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(0); // responseFlags
            buf.putLong(0L); // cursorID
            buf.putInt(0); // startingFrom
            buf.putInt(1); // numberReturned
            buf.put(docBytes);
            body = buf.array();
            opCode = OP_REPLY;
        } else {
            OutputBuffer bodyBuf = new BasicOutputBuffer();
            bodyBuf.writeInt32(0); // flagBits = 0
            bodyBuf.writeByte(0); // section kind 0
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
