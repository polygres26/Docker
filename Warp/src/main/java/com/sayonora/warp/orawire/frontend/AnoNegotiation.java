package com.sayonora.warp.orawire.frontend;

import com.sayonora.warp.orawire.wireformat.TnsPacket;
import com.sayonora.warp.orawire.wireformat.TnsPacketReader;
import com.sayonora.warp.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Oracle's NSN/ANO ("Native Services Negotiation" / "Advanced Networking Option") handshake --
 * the step, between {@link ConnectHandshake} and {@link
 * com.sayonora.warp.orawire.frontend.auth.O5LogonHandler}, where the client proposes a set of
 * network services (Supervisor, Authentication, Encryption, Data Integrity) and the server picks
 * what it's willing to run. {@link com.sayonora.warp.orawire.wireformat.TnsPacketReader#isAnoEligible()}
 * (set by {@link ConnectHandshake}) decides whether this step happens at all -- see that class's
 * comment for the concrete, captured difference: a legacy-protocol client negotiates TNS protocol 319 and skips this
 * step entirely; real, native-OCI non-dblink negotiates 320 and always goes through it.
 *
 * <p><b>Real bug this replaced</b>: the previous implementation only checked the request's magic
 * number (0xDEADBEEF) and otherwise ignored it completely, replying with one hardcoded byte blob
 * captured from a single earlier session -- reusing it for every client regardless of what that
 * client actually proposed. That happened to work for whatever specific client/negotiation shape
 * the blob was captured from, but hung a *real* non-dblink native-OCI client indefinitely: confirmed live via a
 * Warp thread dump (blocked inside {@code O5LogonHandler.authenticate}'s very first packet read,
 * meaning the client itself never got a negotiation response it was willing to proceed past) --
 * isolated by testing Native mode (which never runs this code at all, bypassing straight to real
 * Oracle) against the identical network path, which worked instantly, and Emulate mode (which runs
 * this same code), which hung identically -- proving the bug lives here, not in Bridge mode or the
 * network setup that surfaced it.
 *
 * <p><b>This implementation</b> parses the client's actual request (every service it proposed,
 * using the real wire format below) and negotiates every service OFF -- no native encryption, no
 * extra authentication driver, no checksum -- which is always a valid, safe outcome for any real
 * Oracle client (a wire-level "please don't use any of these" is a normal, accepted negotiation
 * result, not an error) and is exactly the right one for Warp: this frontend already terminates
 * the client connection itself and can enforce TLS at the socket level separately
 * (server/TlsSupport.java) if a deployment needs an encrypted client leg, so there's no reason to
 * also implement Oracle's own native RC4/AES/Diffie-Hellman network security stack here. Disabling
 * every service needs only a single request/response round trip (Data Integrity's real protocol
 * has an optional second Diffie-Hellman round trip, but only when a checksum algorithm is actually
 * negotiated ON -- see DataIntegrity's real Oracle wire behavior; since this always negotiates it
 * OFF, that second round trip never happens), matching this method's existing single perform()
 * call shape.
 *
 * <h2>Wire format</h2>
 * Header (13 bytes): {@code magic:u4, packetLength:u2, version:u4, numberOfServices:u2,
 * errorFlags:u1}. Then {@code numberOfServices} services, each: a sub-header ({@code
 * serviceId:u2, subPacketCount:u2, serviceErrors:u4}) followed by that many length-prefixed,
 * type-tagged fields ({@code length:u2, type:u2, <length bytes of data>} -- type is informational
 * here, every field's length is self-describing so this implementation never needs to branch on
 * it while reading). Service IDs: 4=Supervisor, 1=Authentication, 2=Encryption, 3=Data Integrity.
 */
public final class AnoNegotiation {

    private static final long NSN_MAGIC = 0xDEADBEEFL;

    private static final int SERVICE_AUTHENTICATION = 1;
    private static final int SERVICE_ENCRYPTION = 2;
    private static final int SERVICE_DATAINTEGRITY = 3;
    private static final int SERVICE_SUPERVISOR = 4;

    // Oracle's own well-known "explicitly declining to use extra authentication" status word --
    // captured from real Oracle server traffic (see AuthenticationService's real values: OK=
    // 0xfaff, DONT_USE_AUTH=0xfbff, ...). Sending this tells the client outright "no extra auth
    // driver was negotiated," not merely a missing/absent field.
    private static final int NAU_DONT_USE_AUTH = 0xfbff;

    private static final long NEGOTIATION_VERSION = (10L << 24) | (2L << 20);

    public void perform(TnsPacketReader reader, OutputStream out) throws IOException {
        TnsPacket requestPacket = reader.readPacket();
        ByteBuffer request = ByteBuffer.wrap(requestPacket.payload());

        long magic = request.getInt() & 0xFFFFFFFFL;
        if (magic != NSN_MAGIC) {
            throw new IOException("expected NSN/ANO magic 0xDEADBEEF, got 0x" + Long.toHexString(magic));
        }
        request.getShort(); // header packet length -- informational, response is built fresh below
        request.getInt(); // header version -- informational
        int numberOfServices = request.getShort() & 0xFFFF;
        int errorFlags = request.get() & 0xFF;
        if (errorFlags != 0) {
            throw new IOException("client's ANO request carries error flags 0x" + Integer.toHexString(errorFlags));
        }

        List<byte[]> serviceResponses = new ArrayList<>(numberOfServices);
        for (int i = 0; i < numberOfServices; i++) {
            int serviceId = request.getShort() & 0xFFFF;
            int subPacketCount = request.getShort() & 0xFFFF;
            request.getInt(); // service-level errors on the request -- always 0 from a real client
            serviceResponses.add(switch (serviceId) {
                case SERVICE_SUPERVISOR -> respondSupervisor(request);
                case SERVICE_AUTHENTICATION -> respondAuthenticationDisabled(request, subPacketCount);
                case SERVICE_ENCRYPTION -> respondEncryptionDisabled(request);
                case SERVICE_DATAINTEGRITY -> respondDataIntegrityDisabled(request);
                default -> throw new IOException("unsupported ANO service id " + serviceId);
            });
        }

        // A real client can pipeline more than one logical TTC message into a single physical TNS
        // packet (see TnsPacketReader's own javadoc) -- anything past the ANO services this loop
        // consumed belongs to the NEXT stage (ProtocolNegotiation), not to this one.
        reader.pushBackUnconsumed(requestPacket.payload(), request.position());

        byte[] responsePayload = buildResponsePacket(serviceResponses);
        TnsPacket responsePacket = new TnsPacket(TnsPacketType.DATA, 0, responsePayload);
        out.write(responsePacket.encode(reader.isLargeSdu()));
        out.flush();
    }

    /** Supervisor (service 4): the client's request is {@code version, cid:RAW,
     * serviceIds:UB2-array} -- the array of every service (including Supervisor itself) it's
     * proposing to negotiate. The correct, safe response is to agree to run exactly that list:
     * each of those services still gets its own negotiate-off response below, this only confirms
     * the server is willing to process the list at all. */
    private byte[] respondSupervisor(ByteBuffer request) {
        readVersion(request);
        readRaw(request); // client identifier -- not needed to negotiate everything off
        int[] proposedServiceIds = readUb2Array(request);

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        writeServiceHeader(buffer, SERVICE_SUPERVISOR, 3);
        writeVersion(buffer);
        writeStatus(buffer, 0); // NAS_OK
        writeUb2Array(buffer, proposedServiceIds);
        return trim(buffer);
    }

    /** Authentication (service 1): the client's request is {@code version, flags:UB2, status,
     * (driverId:UB1, driverName:STRING) * N}, where N = (subPacketCount - 3) / 2. Declining every
     * proposed driver (subPacketsLength=2, status=NAU_DONT_USE_AUTH) is a normal, well-formed
     * negotiation outcome -- the client falls back to whatever primary authentication it already
     * uses (here, the real O5LOGON step right after this handshake), exactly as if the operator
     * had configured no ANO authentication service at all. */
    private byte[] respondAuthenticationDisabled(ByteBuffer request, int subPacketCount) {
        readVersion(request);
        readUb2(request); // client/server context flag
        readStatus(request);
        int driverCount = Math.max(0, (subPacketCount - 3) / 2);
        for (int i = 0; i < driverCount; i++) {
            readUb1(request); // proposed driver id
            readString(request); // proposed driver name
        }

        ByteBuffer buffer = ByteBuffer.allocate(64);
        writeServiceHeader(buffer, SERVICE_AUTHENTICATION, 2);
        writeVersion(buffer);
        writeStatus(buffer, NAU_DONT_USE_AUTH);
        return trim(buffer);
    }

    /** Encryption (service 2): the client's request is {@code version,
     * selectedEncryptions:RAW} (one byte per algorithm it supports, {@code 0} always included
     * meaning "no encryption" is acceptable). Responding with algorithm id {@code 0} negotiates
     * "no encryption" -- always present in a well-formed client proposal, so always a legal,
     * accepted choice. */
    private byte[] respondEncryptionDisabled(ByteBuffer request) {
        readVersion(request);
        readRaw(request); // client's proposed algorithm id list -- 0 (none) is always safe to pick

        ByteBuffer buffer = ByteBuffer.allocate(32);
        writeServiceHeader(buffer, SERVICE_ENCRYPTION, 2);
        writeVersion(buffer);
        writeUb1(buffer, (short) 0);
        return trim(buffer);
    }

    /** Data Integrity (service 3): the client's request is {@code version,
     * selectedIntegrityAlgs:RAW}. Responding with algorithm id {@code 0} ("none") is a
     * single-round-trip, terminal outcome -- the real Oracle wire protocol only needs a second
     * (Diffie-Hellman key exchange) round trip when a non-zero checksum algorithm is actually
     * negotiated on, which never happens here. */
    private byte[] respondDataIntegrityDisabled(ByteBuffer request) {
        readVersion(request);
        readRaw(request); // client's proposed algorithm id list

        ByteBuffer buffer = ByteBuffer.allocate(32);
        writeServiceHeader(buffer, SERVICE_DATAINTEGRITY, 2);
        writeVersion(buffer);
        writeUb1(buffer, (short) 0);
        return trim(buffer);
    }

    private byte[] buildResponsePacket(List<byte[]> serviceResponses) {
        int total = 13; // header size: magic(4) + packetLength(2) + version(4) + numServices(2) + errorFlags(1)
        for (byte[] response : serviceResponses) {
            total += response.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.putInt((int) NSN_MAGIC);
        buffer.putShort((short) total);
        buffer.putInt((int) NEGOTIATION_VERSION);
        buffer.putShort((short) serviceResponses.size());
        buffer.put((byte) 0); // errorFlags
        for (byte[] response : serviceResponses) {
            buffer.put(response);
        }
        return buffer.array();
    }

    // ---- tagged-field wire helpers: {length:u2, type:u2, <length bytes>} ----
    // Type codes are informational on this wire (every field's own length already delimits it),
    // so reading never branches on them; only length matters to skip/consume the right span.

    private static final int TYPE_STRING = 0;
    private static final int TYPE_RAW = 1;
    private static final int TYPE_UB1 = 2;
    private static final int TYPE_UB2 = 3;
    private static final int TYPE_VERSION = 5;
    private static final int TYPE_STATUS = 6;

    private void writeFieldHeader(ByteBuffer buffer, int length, int type) {
        buffer.putShort((short) length);
        buffer.putShort((short) type);
    }

    private void writeVersion(ByteBuffer buffer) {
        writeFieldHeader(buffer, 4, TYPE_VERSION);
        buffer.putInt((int) NEGOTIATION_VERSION);
    }

    private void writeStatus(ByteBuffer buffer, int status) {
        writeFieldHeader(buffer, 2, TYPE_STATUS);
        buffer.putShort((short) status);
    }

    private void writeUb1(ByteBuffer buffer, short value) {
        writeFieldHeader(buffer, 1, TYPE_UB1);
        buffer.put((byte) value);
    }

    private void writeUb2Array(ByteBuffer buffer, int[] values) {
        // Nested RAW field: magic(4) + type(2) + count(4) + count*UB2(2) -- matches the real
        // Oracle wire's own "array of UB2" encoding (an array is itself carried as a RAW blob
        // with its own internal magic/type/count preamble).
        int innerLength = 4 + 2 + 4 + values.length * 2;
        writeFieldHeader(buffer, innerLength, TYPE_RAW);
        buffer.putInt((int) NSN_MAGIC);
        buffer.putShort((short) TYPE_UB2);
        buffer.putInt(values.length);
        for (int value : values) {
            buffer.putShort((short) value);
        }
    }

    private void writeServiceHeader(ByteBuffer buffer, int serviceId, int subPacketCount) {
        buffer.putShort((short) serviceId);
        buffer.putShort((short) subPacketCount);
        buffer.putInt(0); // service errors
    }

    private int readFieldLength(ByteBuffer buffer) {
        int length = buffer.getShort() & 0xFFFF;
        buffer.getShort(); // type -- not needed, every field is self-length-delimited
        return length;
    }

    private long readVersion(ByteBuffer buffer) {
        readFieldLength(buffer);
        return buffer.getInt() & 0xFFFFFFFFL;
    }

    private int readStatus(ByteBuffer buffer) {
        readFieldLength(buffer);
        return buffer.getShort() & 0xFFFF;
    }

    private byte[] readRaw(ByteBuffer buffer) {
        int length = readFieldLength(buffer);
        byte[] data = new byte[length];
        buffer.get(data);
        return data;
    }

    private String readString(ByteBuffer buffer) {
        int length = readFieldLength(buffer);
        byte[] data = new byte[length];
        buffer.get(data);
        return new String(data, StandardCharsets.US_ASCII);
    }

    private short readUb1(ByteBuffer buffer) {
        readFieldLength(buffer);
        return (short) (buffer.get() & 0xFF);
    }

    private int readUb2(ByteBuffer buffer) {
        readFieldLength(buffer);
        return buffer.getShort() & 0xFFFF;
    }

    private int[] readUb2Array(ByteBuffer buffer) {
        readFieldLength(buffer); // outer RAW field length
        buffer.getInt(); // inner magic (0xDEADBEEF again, nested)
        buffer.getShort(); // inner type (UB2)
        int count = buffer.getInt();
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = buffer.getShort() & 0xFFFF;
        }
        return values;
    }

    private byte[] trim(ByteBuffer buffer) {
        byte[] result = new byte[buffer.position()];
        buffer.rewind();
        buffer.get(result);
        return result;
    }
}
