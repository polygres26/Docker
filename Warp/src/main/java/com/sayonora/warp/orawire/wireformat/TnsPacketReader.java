package com.sayonora.warp.orawire.wireformat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

/**
 * Reads TNS packets off the client socket for the whole connect/negotiate/login sequence
 * ({@code ConnectHandshake} -> {@code AnoNegotiation} -> {@code ProtocolNegotiation} -> {@code
 * O5LogonHandler}). Each of those stages historically called {@link #readPacket()} expecting it
 * to always read a fresh physical packet off the wire, and consumed only as much of that packet's
 * payload as its own message needed -- silently dropping whatever bytes were left over.
 *
 * <p><b>Real bug this fixes</b>: a real, native-OCI non-dblink client pipelines more than one
 * logical TTC message into a physical TNS packet, and even sends a LATER stage's message (its
 * O5LOGON {@code AUTH_PHASE_ONE} call) ahead of an EARLIER stage's own message ({@code
 * ProtocolNegotiation}'s data-types negotiation) -- both confirmed live via a raw hex capture of a
 * real non-dblink native-OCI session: the PROTOCOL message's own packet carried the driver-name field {@code
 * ProtocolNegotiation} parses immediately followed, in the SAME physical packet, by what is
 * unmistakably an O5LOGON {@code AUTH_PHASE_ONE} call (recognizable {@code AUTH_TERMINAL}/{@code
 * AUTH_PROGRAM_NM}/{@code AUTH_MACHINE}/{@code AUTH_PID}/{@code AUTH_SID} key names and the real
 * username) -- and the client's actual data-types negotiation request arrived afterward, as its
 * own separate physical packet. Every earlier stage's own packet reads only consumed their own
 * message and threw the rest away when they returned; for a client that pipelines and reorders
 * like this, that permanently loses whatever came after, and the next stage (ultimately {@code
 * O5LogonHandler}) blocks forever waiting to read a fresh packet for a message the client already
 * sent and is not going to send again: a genuine mutual deadlock, reproduced live (a Warp thread
 * dump showed the session parked in {@code O5LogonHandler.authenticate}'s very first packet read,
 * indefinitely, while the real client was equally idle waiting for a response to what it already
 * sent).
 *
 * <p>The fix is a small FIFO queue of packets a caller has read but not fully consumed or not
 * consumed at all, on top of the plain socket read:
 * <ul>
 *   <li>{@link #pushBackUnconsumed}: a caller that only partially consumed the payload of the
 *       packet it just got queues the leftover at the FRONT, so the very next {@link
 *       #readPacket()} call (almost always the same stage's own next step) sees it before
 *       anything else.
 *   <li>{@link #deferForLater}: a caller that read a whole packet and determined it belongs to a
 *       LATER stage entirely (not simply "leftover bytes I didn't need") queues it at the BACK,
 *       then calls {@link #readFreshPacket()} to go straight to the socket for its own actual
 *       message -- the deferred packet surfaces, in the correct order, whenever a subsequent
 *       {@link #readPacket()} call (from that later stage) empties the queue down to it.
 * </ul>
 * A well-behaved client that sends exactly one logical message per physical packet, in the
 * expected order (every legacy, non-native-OCI client), is completely unaffected -- the
 * queue is always empty for them, so every {@link #readPacket()} call reads a fresh physical
 * packet exactly as before this existed.
 */
public final class TnsPacketReader {

    private final DataInputStream in;
    private boolean largeSdu = false;
    private boolean anoEligible = false;

    private final Deque<TnsPacket> pending = new ArrayDeque<>();

    public TnsPacketReader(InputStream in) {
        this.in = new DataInputStream(in);
    }

    public void setLargeSdu(boolean largeSdu) {
        this.largeSdu = largeSdu;
    }

    public boolean isLargeSdu() {
        return largeSdu;
    }

    public void setAnoEligible(boolean anoEligible) {
        this.anoEligible = anoEligible;
    }

    public boolean isAnoEligible() {
        return anoEligible;
    }

    /** The next logical packet: whatever's queued (leftover bytes a previous caller pushed back,
     * or a whole packet a previous caller deferred for later), in FIFO order, before ever reading
     * anything new off the wire. */
    public TnsPacket readPacket() throws IOException {
        TnsPacket queued = pending.pollFirst();
        if (queued != null) {
            return queued;
        }
        return readFreshPacket();
    }

    /** Reads a genuinely new physical packet straight off the socket, bypassing the queue
     * entirely -- for a caller that determined the head of the queue belongs to a later stage
     * (see {@link #deferForLater}) and needs to keep looking for its OWN message without losing
     * that deferred one's place in line. */
    public TnsPacket readFreshPacket() throws IOException {
        byte[] header = new byte[TnsPacket.headerLength()];
        in.readFully(header);
        int length = largeSdu
                ? (((header[0] & 0xFF) << 24) | ((header[1] & 0xFF) << 16)
                        | ((header[2] & 0xFF) << 8) | (header[3] & 0xFF))
                : (((header[0] & 0xFF) << 8) | (header[1] & 0xFF));
        TnsPacketType type = TnsPacketType.fromCode(header[4] & 0xFF);
        int flags = header[5] & 0xFF;
        int preambleLength = TnsPacket.headerLength();
        if (type == TnsPacketType.DATA) {
            byte[] dataFlags = new byte[2];
            in.readFully(dataFlags);
            preambleLength += 2;
        }
        byte[] payload = new byte[length - preambleLength];
        in.readFully(payload);
        return new TnsPacket(type, flags, payload);
    }

    /** Called by a connect/negotiate/login stage after it finishes parsing a packet it got from
     * {@link #readPacket()}, with how many of that packet's payload bytes it actually consumed.
     * Any remainder is queued at the FRONT, so it's what the very next {@link #readPacket()} call
     * (that same stage's own next step, or the next stage entirely if this one is done) sees
     * before anything else -- see the class javadoc for why this matters. A no-op when {@code
     * consumedLength >= payload.length} (the common case: nothing left over). */
    public void pushBackUnconsumed(byte[] payload, int consumedLength) {
        if (consumedLength < payload.length) {
            pending.addFirst(new TnsPacket(TnsPacketType.DATA, 0,
                    Arrays.copyOfRange(payload, consumedLength, payload.length)));
        }
    }

    /** Called by a stage that read a whole packet via {@link #readPacket()} and determined it
     * isn't the message type it was expecting at all -- a real client can pipeline a LATER stage's
     * message ahead of this one's. Queues {@code packet} at the BACK (preserving arrival order
     * relative to anything already queued) so it surfaces once a subsequent {@link #readPacket()}
     * call works through to it; the caller should then use {@link #readFreshPacket()} to keep
     * looking for its own message without consuming what it just deferred. */
    public void deferForLater(TnsPacket packet) {
        pending.addLast(packet);
    }
}
