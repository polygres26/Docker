package com.sayonora.wire.s3wire;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps a byte range of an object onto stored chunk rows. An object is a list of segments: a plain
 * object has one segment (its own blob); a multipart object has one segment per part, each part
 * being an independent blob whose chunks are numbered from 0 with the fixed chunk size (the last
 * chunk of a blob may be short). Complete-multipart therefore never copies bytes: the object row just
 * lists its parts' blobs.
 */
final class ChunkMath {

    private ChunkMath() {
    }

    record Segment(String blobId, long size, int chunkSize) {
    }

    /** {@code length} bytes starting at {@code offset} within chunk {@code seq} of blob {@code blobId}. */
    record Slice(String blobId, int seq, int offset, int length) {
    }

    /**
     * @param start inclusive byte offset within the whole object
     * @param end   inclusive byte offset within the whole object
     */
    static List<Slice> slices(List<Segment> segments, long start, long end) {
        List<Slice> out = new ArrayList<>();
        long segStart = 0;
        for (Segment s : segments) {
            long segEnd = segStart + s.size() - 1; // inclusive, may be < segStart for an empty segment
            if (s.size() > 0 && segEnd >= start && segStart <= end) {
                long from = Math.max(start, segStart) - segStart;
                long to = Math.min(end, segEnd) - segStart;
                int chunkSize = s.chunkSize();
                for (long pos = from; pos <= to;) {
                    int seq = (int) (pos / chunkSize);
                    int off = (int) (pos % chunkSize);
                    long chunkEndPos = Math.min(to, (long) (seq + 1) * chunkSize - 1);
                    out.add(new Slice(s.blobId(), seq, off, (int) (chunkEndPos - pos + 1)));
                    pos = chunkEndPos + 1;
                }
            }
            segStart += s.size();
            if (segStart > end) {
                break;
            }
        }
        return out;
    }

    static int chunkCount(long size, int chunkSize) {
        return (int) ((size + chunkSize - 1) / chunkSize);
    }
}
