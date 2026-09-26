package com.sayonora.wire.awswire;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.AbstractConnectionFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.DetectorConnectionFactory;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

/**
 * Cleartext HTTP/2 with prior knowledge ("h2c", the client opens with the {@code PRI * HTTP/2.0} preface) on the same port as
 * HTTP/1.1. AWS SDK for JavaScript v3 speaks it to Kinesis (and the Java async client for SubscribeToShard); Jetty's own h2c
 * support needs a jar this build does not have, so the HTTP/2 framing is done by the Netty HTTP/2 codec already shaded into the
 * gRPC dependency ({@link H2cConnection}). A Jetty {@link DetectorConnectionFactory} looks at the first bytes of every new
 * connection: the preface selects this factory, anything else falls through to Jetty's HTTP/1.1.
 */
public final class H2cConnectionFactory extends AbstractConnectionFactory implements ConnectionFactory.Detecting {

    static final byte[] PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private final Consumer<H2Exchange> handler;
    private final Executor workers;

    private H2cConnectionFactory(Consumer<H2Exchange> handler, Executor workers) {
        super("h2c-warp");
        this.handler = handler;
        this.workers = workers;
    }

    /** A connector on {@code port} that speaks HTTP/1.1 and h2c; HTTP/2 exchanges go to {@code handler} on {@code workers}. */
    public static ServerConnector connector(Server server, int port, Consumer<H2Exchange> handler, Executor workers) {
        H2cConnectionFactory h2c = new H2cConnectionFactory(handler, workers);
        DetectorConnectionFactory detector = new DetectorConnectionFactory(h2c);
        HttpConnectionFactory http = new HttpConnectionFactory();
        ServerConnector c = new ServerConnector(server, detector, http);
        c.setPort(port);
        return c;
    }

    @Override
    public Detection detect(ByteBuffer buffer) {
        int n = Math.min(buffer.remaining(), PREFACE.length);
        for (int i = 0; i < n; i++) {
            if (buffer.get(buffer.position() + i) != PREFACE[i]) {
                return Detection.NOT_RECOGNIZED;
            }
        }
        return n < PREFACE.length ? Detection.NEED_MORE_BYTES : Detection.RECOGNIZED;
    }

    @Override
    public Connection newConnection(Connector connector, EndPoint endPoint) {
        return configure(new H2cConnection(endPoint, connector.getExecutor(), connector.getByteBufferPool(), handler, workers), connector,
                endPoint);
    }
}
