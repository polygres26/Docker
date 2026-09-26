package com.sayonora.wire.kafkawire;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One client connection: length-prefixed request/response frames, answered strictly in order (as Kafka guarantees per connection). */
final class KafkaSession implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaSession.class);

    private final KafkaWireServer server;
    private final Socket socket;
    private final Runnable onClose;
    private final KafkaBroker.Conn conn = new KafkaBroker.Conn();

    private boolean authenticated;
    private int saslState; // 0 none, 1 handshake done (v1: expect SaslAuthenticate), 2 handshake v0: next frame is a raw token

    KafkaSession(KafkaWireServer server, Socket socket, Runnable onClose) {
        this.server = server;
        this.socket = socket;
        this.onClose = onClose;
        this.authenticated = !server.authRequired();
        this.conn.clientHost = "/" + socket.getInetAddress().getHostAddress();
    }

    @Override
    public void run() {
        try (Socket s = socket) {
            s.setTcpNoDelay(true);
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 65536));
            OutputStream out = new BufferedOutputStream(s.getOutputStream(), 65536);
            while (true) {
                int size;
                try {
                    size = in.readInt();
                } catch (EOFException e) {
                    return;
                }
                if (size < 0 || size > server.maxRequestBytes()) {
                    log.debug("kafkawire: closing a connection with a request of {} bytes", size);
                    return;
                }
                byte[] frame = new byte[size];
                in.readFully(frame);
                if (saslState == 2) {
                    if (!rawSasl(frame, out)) {
                        return;
                    }
                    continue;
                }
                if (!dispatch(frame, out)) {
                    return;
                }
            }
        } catch (IOException e) {
            // client gone
        } catch (RuntimeException e) {
            log.warn("kafkawire: connection closed after an unexpected error", e);
        } finally {
            onClose.run();
        }
    }

    private boolean rawSasl(byte[] token, OutputStream out) throws IOException {
        boolean ok = plain(token) != null;
        if (!ok) {
            return false;
        }
        authenticated = true;
        saslState = 0;
        out.write(new byte[] {0, 0, 0, 0});
        out.flush();
        return true;
    }

    /** @return the user name when the PLAIN token authenticates, else null */
    private String plain(byte[] token) {
        String[] parts = new String(token, StandardCharsets.UTF_8).split("\u0000", -1);
        if (parts.length != 3) {
            return null;
        }
        return server.checkPassword(parts[1], parts[2].getBytes(StandardCharsets.UTF_8)) ? parts[1] : null;
    }

    private boolean dispatch(byte[] frame, OutputStream out) throws IOException {
        if (frame.length < 8) {
            return false;
        }
        KReader h = new KReader(frame, 0, frame.length, false);
        int api = h.i16();
        int ver = h.i16();
        int corr = h.i32();
        conn.clientId = h.strNN();
        boolean known = KafkaBroker.supported(api, ver) || api == 17 && ver <= 1 || api == 36 && ver <= 2;
        if (api == 18 && !known) {
            // an ApiVersions version this broker does not know: answer in the v0 format with UNSUPPORTED_VERSION so the client retries lower
            KWriter body = server.broker().apiVersions(0, KafkaError.UNSUPPORTED_VERSION);
            send(out, corr, false, body);
            return true;
        }
        if (!known) {
            log.debug("kafkawire: closing a connection asking for unsupported api {} v{}", api, ver);
            return false;
        }
        boolean flex = api == 36 ? ver >= 2 : KafkaBroker.flexible(api, ver);
        KReader r = new KReader(frame, h.p, frame.length, flex);
        if (flex) {
            r.tagged(); // request header v2 tagged fields
        }
        long t0 = System.nanoTime();
        KWriter body;
        if (api == 17) {
            body = saslHandshake(r, ver);
        } else if (api == 36) {
            body = saslAuthenticate(r, ver);
            if (body == null) {
                return false;
            }
        } else {
            if (!authenticated && api != 18) {
                return false; // Kafka closes connections that skip SASL
            }
            try {
                body = server.broker().handle(api, ver, r, conn);
            } catch (KafkaError e) {
                log.debug("kafkawire: request api {} v{} rejected: {}", api, ver, e.getMessage());
                return false;
            } catch (RuntimeException e) {
                log.warn("kafkawire: api {} v{} failed", api, ver, e);
                return false;
            }
        }
        server.record(KafkaBroker.apiName(api), KafkaBroker.writeApi(api), System.nanoTime() - t0);
        if (body != null) {
            send(out, corr, api != 18 && flex, body);
        }
        if (api == 36 && saslFailed) {
            return false;
        }
        return true;
    }

    private void send(OutputStream out, int corr, boolean flexHeader, KWriter body) throws IOException {
        int len = 4 + (flexHeader ? 1 : 0) + body.n;
        byte[] hdr = new byte[4 + 4 + (flexHeader ? 1 : 0)];
        hdr[0] = (byte) (len >> 24);
        hdr[1] = (byte) (len >> 16);
        hdr[2] = (byte) (len >> 8);
        hdr[3] = (byte) len;
        hdr[4] = (byte) (corr >> 24);
        hdr[5] = (byte) (corr >> 16);
        hdr[6] = (byte) (corr >> 8);
        hdr[7] = (byte) corr;
        out.write(hdr);
        out.write(body.b, 0, body.n);
        out.flush();
    }

    private boolean saslFailed;

    private KWriter saslHandshake(KReader r, int ver) {
        String mech = r.str();
        KWriter w = new KWriter(false);
        if (!server.authRequired()) {
            return w.i16(KafkaError.ILLEGAL_SASL_STATE).arr(0);
        }
        if (!"PLAIN".equals(mech)) {
            return w.i16(KafkaError.UNSUPPORTED_SASL_MECHANISM).arr(1).str("PLAIN");
        }
        saslState = ver == 0 ? 2 : 1;
        return w.i16(0).arr(1).str("PLAIN");
    }

    private KWriter saslAuthenticate(KReader r, int ver) {
        byte[] token = r.bytes();
        r.tagged();
        KWriter w = new KWriter(ver >= 2);
        if (saslState != 1) {
            w.i16(KafkaError.ILLEGAL_SASL_STATE).str("Unexpected Kafka request of type SASL_AUTHENTICATE during SASL handshake.").bytes(new byte[0]);
            if (ver >= 1) {
                w.i64(0);
            }
            w.tagged();
            saslFailed = true;
            return w;
        }
        String user = token == null ? null : plain(token);
        if (user == null) {
            w.i16(KafkaError.SASL_AUTHENTICATION_FAILED).str("Authentication failed: Invalid username or password").bytes(new byte[0]);
            saslFailed = true;
        } else {
            authenticated = true;
            saslState = 0;
            conn.user = user;
            w.i16(0).str(null).bytes(new byte[0]);
        }
        if (ver >= 1) {
            w.i64(0);
        }
        w.tagged();
        return w;
    }
}
