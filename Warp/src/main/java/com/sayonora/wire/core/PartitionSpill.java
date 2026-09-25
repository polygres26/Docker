package com.sayonora.wire.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Phase 2+ skew mitigation: a bounded, disk-backed overflow for ONE local build-side partition that
 * grows far larger than {@code WARP_PARALLEL_JOIN_SPILL_THRESHOLD_ROWS} -- the "full skew-aware
 * dynamic repartitioning and spill-to-disk" item the parent design plan explicitly deferred until
 * real usage justified it.
 *
 * <p><b>Deliberately narrower than a general external hash join</b>, in the same spirit as this
 * engine's other real, disclosed narrowings: rows for a partition beyond the threshold are appended
 * to a single per-partition file (never re-sorted, never merged with other partitions), and only a
 * lightweight {@code key -> file offset(s)} INDEX is kept in memory -- not the row content itself.
 * This bounds a skewed partition's MEMORY footprint to roughly one {@code long} per spilled row
 * (grouped by key), while probing a spilled key still costs one seek+read per matching row rather
 * than a full table scan. A partition that spills is never dispatched to a remote peer for this
 * query (see {@link ParallelJoinExecutor}'s own javadoc) -- shipping a peer an in-memory table that's
 * missing its spilled rows would silently produce a wrong join result, so that whole query instead
 * runs fully local once any partition spills, a coarser but simple and always-correct fallback.
 */
final class PartitionSpill implements Closeable {

    private final Path file;
    private final RandomAccessFile raf;
    private final Map<Object, List<Long>> offsetsByKey = new HashMap<>();
    private long nextOffset = 0L;

    PartitionSpill(int partitionIndex) throws IOException {
        file = Files.createTempFile("warp-parallel-join-spill-" + partitionIndex + "-", ".bin");
        raf = new RandomAccessFile(file.toFile(), "rw");
    }

    /** Appends one build row under {@code key} -- {@code null} keys are never passed here (see
     * {@link ParallelJoinExecutor}'s own null-key handling, unchanged for the spill path). */
    synchronized void append(Object key, List<Object> row) throws IOException {
        byte[] bytes = serialize(row);
        raf.seek(nextOffset);
        raf.writeInt(bytes.length);
        raf.write(bytes);
        offsetsByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(nextOffset);
        nextOffset += 4L + bytes.length;
    }

    /** Every distinct key spilled for this partition -- used by {@link ParallelJoinExecutor}'s own
     * dynamic-filter pushdown to build a complete build-side key set that includes keys which never
     * made it into the in-memory table at all. */
    synchronized java.util.Set<Object> spilledKeys() {
        return java.util.Set.copyOf(offsetsByKey.keySet());
    }

    /** Every spilled row sharing {@code key}, read back from disk -- empty (never {@code null}) when
     * this key was never spilled for this partition. */
    synchronized List<List<Object>> readMatches(Object key) throws IOException {
        List<Long> offsets = offsetsByKey.get(key);
        if (offsets == null || offsets.isEmpty()) {
            return List.of();
        }
        List<List<Object>> rows = new ArrayList<>(offsets.size());
        for (long offset : offsets) {
            raf.seek(offset);
            int length = raf.readInt();
            byte[] bytes = new byte[length];
            raf.readFully(bytes);
            rows.add(deserialize(bytes));
        }
        return rows;
    }

    @Override
    public void close() {
        try {
            raf.close();
        } catch (IOException ignoredOnCleanup) {
            // best-effort -- the query's own result is already final by the time this runs
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignoredOnCleanup) {
            // best-effort -- a leaked temp file here never affects query correctness, only disk
            // hygiene, and the OS temp dir is cleaned independently anyway
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Object> deserialize(byte[] bytes) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (List<Object>) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("spilled row deserialization failed", e);
        }
    }

    private static byte[] serialize(List<Object> row) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(new ArrayList<>(row));
        }
        return bos.toByteArray();
    }
}
