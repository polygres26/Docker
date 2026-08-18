package com.polygres.wire.pgwire;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

final class PgBodyReader {

    private final byte[] data;
    private int pos;

    PgBodyReader(byte[] data) {
        this.data = data;
    }

    String readCString() throws IOException {
        int start = pos;
        while (pos < data.length && data[pos] != 0) {
            pos++;
        }
        if (pos >= data.length) {
            throw new IOException("unterminated string in message body");
        }
        String s = new String(data, start, pos - start, StandardCharsets.UTF_8);
        pos++;
        return s;
    }

    int readInt16() throws IOException {
        require(2);
        int v = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
        pos += 2;
        return (short) v;
    }

    int readInt32() throws IOException {
        require(4);
        int v = ((data[pos] & 0xFF) << 24) | ((data[pos + 1] & 0xFF) << 16)
                | ((data[pos + 2] & 0xFF) << 8) | (data[pos + 3] & 0xFF);
        pos += 4;
        return v;
    }

    int readByte() throws IOException {
        require(1);
        return data[pos++] & 0xFF;
    }

    byte[] readBytes(int n) throws IOException {
        require(n);
        byte[] out = new byte[n];
        System.arraycopy(data, pos, out, 0, n);
        pos += n;
        return out;
    }

    void skip(int n) throws IOException {
        require(n);
        pos += n;
    }

    private void require(int n) throws IOException {
        if (pos + n > data.length) {
            throw new IOException("truncated pgwire extended-protocol message body");
        }
    }
}
