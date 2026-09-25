package com.sayonora.wire.core.connector.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * {@link InputFile} over an already-downloaded {@code byte[]} -- the seam that lets
 * {@link org.apache.parquet.hadoop.ParquetFileReader} read a Parquet object with no Hadoop
 * {@code FileSystem}/{@code Path} involved. Ported verbatim from the sibling ThinkingSense project.
 * {@link S3Table} downloads one S3 object fully into memory first, then wraps it here -- fine for
 * the object sizes this connector's scope targets, not a streaming reader for very large files.
 */
final class ByteArrayInputFile implements InputFile {

    private final byte[] data;

    ByteArrayInputFile(byte[] data) {
        this.data = data;
    }

    @Override
    public long getLength() {
        return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new ByteArraySeekableInputStream(data);
    }

    private static final class ByteArraySeekableInputStream extends SeekableInputStream {

        private final byte[] data;
        private int pos;

        ByteArraySeekableInputStream(byte[] data) {
            this.data = data;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) {
            pos = (int) newPos;
        }

        @Override
        public int read() {
            return pos < data.length ? data[pos++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            readFully(b, 0, b.length);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            if (data.length - pos < len) {
                throw new IOException("Unexpected end of Parquet data: wanted " + len + " bytes, had " + (data.length - pos));
            }
            System.arraycopy(data, pos, b, off, len);
            pos += len;
        }

        @Override
        public int read(ByteBuffer buffer) {
            int n = Math.min(buffer.remaining(), data.length - pos);
            if (n <= 0) {
                return -1;
            }
            buffer.put(data, pos, n);
            pos += n;
            return n;
        }

        @Override
        public void readFully(ByteBuffer buffer) throws IOException {
            int n = buffer.remaining();
            if (data.length - pos < n) {
                throw new IOException("Unexpected end of Parquet data: wanted " + n + " bytes, had " + (data.length - pos));
            }
            buffer.put(data, pos, n);
            pos += n;
        }
    }
}
