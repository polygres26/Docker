package com.sayonora.wire.awswire;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One HTTP/2 request/response pair as {@link H2cConnection} hands it to a handler. The request is complete (headers and body
 * read). The response is written with {@link #head} once and {@link #data} any number of times; the last data call passes
 * {@code end=true}. A streaming response (an event stream) simply writes data over time.
 */
public abstract class H2Exchange implements StreamSink {

    public String method;
    /** Path without the query string, exactly as sent. */
    public String path;
    public String query;
    /** Header names in lower case. */
    public final Map<String, List<String>> headers = new LinkedHashMap<>();
    public byte[] body = new byte[0];
    public String remoteAddr;
    public int localPort;
    public String localName = "localhost";

    public String header(String name) {
        List<String> v = headers.get(name.toLowerCase(java.util.Locale.ROOT));
        return v == null || v.isEmpty() ? null : v.get(0);
    }

    /** Sends the response headers ({@code :status} is added). */
    @Override
    public abstract void head(int status, Map<String, String> responseHeaders);

    /** Sends response body bytes; the stream ends with {@code end=true}. Blocks-free: bytes are queued on the connection. */
    @Override
    public abstract void data(byte[] bytes, boolean end);

    /** Whether the peer reset the stream or the connection closed (a streaming handler should stop). */
    @Override
    public abstract boolean cancelled();
}
