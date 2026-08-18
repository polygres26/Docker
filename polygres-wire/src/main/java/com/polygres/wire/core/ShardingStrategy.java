package com.polygres.wire.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Resolves a single sharding-key <em>value</em> (e.g. a bind parameter {@link RouterStage}
 * plucked out of a statement — see {@code RouterStage.ValueShardRule}) to one backend name out of
 * a fixed set of shards. Four strategies, all operator-configured via
 * {@code POLYWIRE_ROUTER_VALUE_SHARD_RULES} (see {@link RouterStage#fromConfig}'s javadoc for the
 * spec grammar) rather than computed from schema metadata this project doesn't have:
 * <ul>
 *   <li>{@link #hash}: {@code hash(value) % shardCount} — simple, but adding/removing a shard
 *   remaps most keys (every key whose modulo result changes).</li>
 *   <li>{@link #consistentHash}: a hash ring with virtual nodes per shard, so adding/removing a
 *   shard only remaps the fraction of keys that fell in the affected arc(s) of the ring — the
 *   textbook fix for plain modulo hashing's total-remap problem.</li>
 *   <li>{@link #list}: an explicit, operator-supplied value → backend map (e.g.
 *   {@code region='east' -> shard1}).</li>
 *   <li>{@link #range}: a value falling in {@code [low, high)} (half-open — a value exactly equal
 *   to a boundary belongs to the range starting there, not the one ending there) routes to that
 *   range's backend.</li>
 * </ul>
 */
public sealed interface ShardingStrategy {

    /** {@code null} if this strategy has no opinion for {@code value} (e.g. it falls outside every configured range) — caller falls through to the next rule/default, same convention as {@link RouterStage#resolveBackend}. */
    String resolve(String value);

    // ---- hash ----

    record HashStrategy(List<String> backends) implements ShardingStrategy {
        public HashStrategy {
            if (backends.isEmpty()) {
                throw new IllegalArgumentException("hash sharding strategy needs at least one backend");
            }
            backends = List.copyOf(backends);
        }

        @Override
        public String resolve(String value) {
            long h = stableHash(value);
            int index = (int) Long.remainderUnsigned(h, backends.size());
            return backends.get(index);
        }
    }

    static ShardingStrategy hash(List<String> backends) {
        return new HashStrategy(backends);
    }

    // ---- consistent hash ----

    /**
     * Standard {@code TreeMap<Long, backend>} hash-ring implementation: each backend gets
     * {@code virtualNodesPerBackend} points scattered around a 64-bit ring (spreads out an
     * individual backend's share more evenly than one point per backend would); a key's owner is
     * the backend at the next ring point clockwise from {@code stableHash(key)} —
     * {@link NavigableMap#ceilingEntry}, wrapping to the ring's first entry past the end.
     */
    record ConsistentHashStrategy(NavigableMap<Long, String> ring) implements ShardingStrategy {

        private static final int DEFAULT_VIRTUAL_NODES = 150;

        static ConsistentHashStrategy of(List<String> backends, int virtualNodesPerBackend) {
            if (backends.isEmpty()) {
                throw new IllegalArgumentException("consistent-hash sharding strategy needs at least one backend");
            }
            // Unsigned comparator: stableHash's 64 bits are meant to be read as an unsigned ring
            // position (matches HashStrategy's Long.remainderUnsigned) -- plain Long natural
            // ordering would treat any hash with the top bit set as "negative" and sort it before
            // every positive one, silently corrupting the ring's ordering for roughly half of all
            // points (any {@code backend#N} string whose hash happens to have that bit set).
            NavigableMap<Long, String> ring = new TreeMap<>(Long::compareUnsigned);
            for (String backend : backends) {
                for (int v = 0; v < virtualNodesPerBackend; v++) {
                    long point = stableHash(backend + "#" + v);
                    ring.put(point, backend);
                }
            }
            return new ConsistentHashStrategy(ring);
        }

        @Override
        public String resolve(String value) {
            long point = stableHash(value);
            Map.Entry<Long, String> entry = ring.ceilingEntry(point);
            if (entry == null) {
                entry = ring.firstEntry(); // wrap around the ring
            }
            return entry.getValue();
        }
    }

    static ShardingStrategy consistentHash(List<String> backends) {
        return ConsistentHashStrategy.of(backends, ConsistentHashStrategy.DEFAULT_VIRTUAL_NODES);
    }

    static ShardingStrategy consistentHash(List<String> backends, int virtualNodesPerBackend) {
        return ConsistentHashStrategy.of(backends, virtualNodesPerBackend);
    }

    // ---- list ----

    record ListStrategy(Map<String, String> valueToBackend) implements ShardingStrategy {
        public ListStrategy {
            valueToBackend = Map.copyOf(valueToBackend);
        }

        @Override
        public String resolve(String value) {
            return valueToBackend.get(value);
        }
    }

    static ShardingStrategy list(Map<String, String> valueToBackend) {
        return new ListStrategy(valueToBackend);
    }

    // ---- range ----

    /** One {@code [low, high)} bound, {@code high == null} meaning "no upper bound" (catches everything {@code >= low} that no earlier, narrower range already claimed). */
    record RangeEntry(double low, Double high, String backend) {
    }

    record RangeStrategy(List<RangeEntry> ranges) implements ShardingStrategy {
        public RangeStrategy {
            ranges = List.copyOf(ranges);
        }

        @Override
        public String resolve(String value) {
            double v;
            try {
                v = Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return null; // not a numeric sharding key -- caller falls through
            }
            for (RangeEntry range : ranges) {
                if (v >= range.low() && (range.high() == null || v < range.high())) {
                    return range.backend();
                }
            }
            return null;
        }
    }

    static ShardingStrategy range(List<RangeEntry> ranges) {
        return new RangeStrategy(ranges);
    }

    // ---- shared hash primitive ----

    /** SHA-256 of the UTF-8 bytes, folded down to the first 8 bytes as an unsigned 64-bit long — deterministic across JVM runs and process restarts (unlike {@link Object#hashCode}), which both {@link #hash} and {@link #consistentHash} depend on for stable, reproducible routing. */
    static long stableHash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            long result = 0;
            for (int i = 0; i < 8; i++) {
                result = (result << 8) | (bytes[i] & 0xFFL);
            }
            return result;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e); // every JDK ships this -- unreachable in practice
        }
    }

    /** Parsed from {@code paramsSpec}, dispatching on {@code type} -- see {@link RouterStage#fromConfig}'s javadoc for the full grammar. */
    static ShardingStrategy fromConfig(String type, String paramsSpec) {
        return switch (type.toLowerCase(java.util.Locale.ROOT)) {
            case "hash" -> hash(splitCsv(paramsSpec));
            case "consistent" -> consistentHash(splitCsv(paramsSpec));
            case "list" -> {
                Map<String, String> valueToBackend = new java.util.LinkedHashMap<>();
                for (String group : paramsSpec.split(";")) {
                    String[] parts = group.split("=", 2);
                    if (parts.length != 2) {
                        continue;
                    }
                    String backend = parts[0].trim();
                    for (String value : parts[1].split(",")) {
                        valueToBackend.put(value.trim(), backend);
                    }
                }
                yield list(valueToBackend);
            }
            case "range" -> {
                List<RangeEntry> ranges = new ArrayList<>();
                double low = Double.NEGATIVE_INFINITY;
                for (String entry : paramsSpec.split(";")) {
                    String[] parts = entry.split("<", 2);
                    String backend = parts[0].trim();
                    Double high = parts.length == 2 ? Double.parseDouble(parts[1].trim()) : null;
                    ranges.add(new RangeEntry(low, high, backend));
                    if (high != null) {
                        low = high;
                    }
                }
                yield range(ranges);
            }
            default -> throw new IllegalArgumentException("unknown sharding strategy type \"" + type + "\" (expected hash/consistent/list/range)");
        };
    }

    private static List<String> splitCsv(String csv) {
        List<String> result = new ArrayList<>();
        for (String part : csv.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }
}
