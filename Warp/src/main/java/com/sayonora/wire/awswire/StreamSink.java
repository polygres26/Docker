package com.sayonora.wire.awswire;

import java.util.Map;

/** Where a streaming response (an event stream) is written: an HTTP/2 stream or a chunked HTTP/1.1 servlet response. */
public interface StreamSink {

    void head(int status, Map<String, String> responseHeaders);

    /** Writes body bytes and flushes them to the client; {@code end=true} completes the response. */
    void data(byte[] bytes, boolean end);

    /** Whether the client went away (a streaming handler should stop). */
    boolean cancelled();
}
