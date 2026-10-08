package com.sayonora.warp.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public sealed interface ShardingStrategy {

    String resolve(String value);

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

    /**
     * Fixed virtual slots assigned to shards: a key hashes to one of {@code slots} slots (stable, independent of the shard count) and each slot
     * belongs to one shard. Adding or removing capacity moves whole slots between shards instead of re-hashing every key, which is what makes
     * online rebalancing possible. Config: {@code slots:<column>:<N>/<shard>=<from>-<to>[,<from>-<to>...],<shard>=...} or the even shorthand
     * {@code <N>/<shard>,<shard>,...}.
     */
    record SlotStrategy(int slots, List<String> owners) implements ShardingStrategy {
        public SlotStrategy {
            if (slots <= 0 || owners.size() != slots) {
                throw new IllegalArgumentException("slot sharding needs an owner for each of its " + slots + " slots");
            }
            for (String o : owners) {
                if (o == null || o.isBlank()) {
                    throw new IllegalArgumentException("every slot needs an owning shard");
                }
            }
            owners = List.copyOf(owners);
        }

        public int slotOf(String value) {
            return (int) Long.remainderUnsigned(stableHash(value), slots);
        }

        @Override
        public String resolve(String value) {
            return owners.get(slotOf(value));
        }

        /** Number of slots each shard owns, in first-seen order. */
        public Map<String, Integer> slotCounts() {
            Map<String, Integer> counts = new java.util.LinkedHashMap<>();
            for (String o : owners) {
                counts.merge(o, 1, Integer::sum);
            }
            return counts;
        }

        public SlotStrategy withOwner(java.util.Collection<Integer> moved, String shard) {
            List<String> next = new ArrayList<>(owners);
            for (int slot : moved) {
                next.set(slot, shard);
            }
            return new SlotStrategy(slots, next);
        }

        /** Canonical params text: {@code N/shard=a-b,c-d;...} parses back to an equal strategy. */
        public String toParams() {
            Map<String, List<int[]>> runs = new java.util.LinkedHashMap<>();
            int i = 0;
            while (i < slots) {
                int j = i;
                while (j + 1 < slots && owners.get(j + 1).equals(owners.get(i))) {
                    j++;
                }
                runs.computeIfAbsent(owners.get(i), k -> new ArrayList<>()).add(new int[] {i, j});
                i = j + 1;
            }
            StringBuilder sb = new StringBuilder().append(slots).append('/');
            boolean firstShard = true;
            for (Map.Entry<String, List<int[]>> e : runs.entrySet()) {
                sb.append(firstShard ? "" : ";").append(e.getKey()).append('=');
                firstShard = false;
                for (int k = 0; k < e.getValue().size(); k++) {
                    int[] r = e.getValue().get(k);
                    sb.append(k == 0 ? "" : ",").append(r[0] == r[1] ? String.valueOf(r[0]) : r[0] + "-" + r[1]);
                }
            }
            return sb.toString();
        }

        static SlotStrategy parse(String params) {
            int slash = params.indexOf('/');
            if (slash < 0) {
                throw new IllegalArgumentException("slot sharding params look like 1024/s1,s2 or 1024/s1=0-511;s2=512-1023");
            }
            int slots = Integer.parseInt(params.substring(0, slash).trim());
            String rest = params.substring(slash + 1).trim();
            String[] owners = new String[slots];
            if (!rest.contains("=")) {
                List<String> shards = splitCsv(rest);
                if (shards.isEmpty()) {
                    throw new IllegalArgumentException("slot sharding needs at least one shard");
                }
                // even spread in contiguous runs
                for (int s = 0; s < slots; s++) {
                    owners[s] = shards.get((int) ((long) s * shards.size() / slots));
                }
            } else {
                for (String group : rest.split(";")) {
                    String[] kv = group.split("=", 2);
                    if (kv.length != 2) {
                        throw new IllegalArgumentException("bad slot assignment '" + group + "'");
                    }
                    String shard = kv[0].trim();
                    for (String run : kv[1].split(",")) {
                        String[] ab = run.trim().split("-", 2);
                        int from = Integer.parseInt(ab[0].trim());
                        int to = ab.length == 2 ? Integer.parseInt(ab[1].trim()) : from;
                        for (int s = from; s <= to; s++) {
                            if (s < 0 || s >= slots || owners[s] != null) {
                                throw new IllegalArgumentException("slot " + s + " is out of range or assigned twice");
                            }
                            owners[s] = shard;
                        }
                    }
                }
            }
            return new SlotStrategy(slots, java.util.Arrays.asList(owners));
        }
    }

    static ShardingStrategy hash(List<String> backends) {
        return new HashStrategy(backends);
    }

    record ConsistentHashStrategy(NavigableMap<Long, String> ring) implements ShardingStrategy {

        private static final int DEFAULT_VIRTUAL_NODES = 150;

        static ConsistentHashStrategy of(List<String> backends, int virtualNodesPerBackend) {
            if (backends.isEmpty()) {
                throw new IllegalArgumentException("consistent-hash sharding strategy needs at least one backend");
            }
            
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
                entry = ring.firstEntry();
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
                return null;
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

    record DateRangeEntry(java.time.LocalDate low, java.time.LocalDate high, String backend) {
    }

    /** As {@link RangeStrategy}, but compares real {@link java.time.LocalDate} values (ISO-8601,
     * {@code yyyy-MM-dd}) instead of parsing the column value as a double -- for a real date/time
     * partition column (e.g. {@code created_at}), {@code RangeStrategy}'s own {@code
     * Double.parseDouble} would just fail to parse every value and silently never match anything. */
    record DateRangeStrategy(List<DateRangeEntry> ranges) implements ShardingStrategy {
        public DateRangeStrategy {
            ranges = List.copyOf(ranges);
        }

        @Override
        public String resolve(String value) {
            java.time.LocalDate v;
            try {
                v = java.time.LocalDate.parse(value.strip());
            } catch (java.time.format.DateTimeParseException e) {
                return null;
            }
            for (DateRangeEntry range : ranges) {
                if (!v.isBefore(range.low()) && (range.high() == null || v.isBefore(range.high()))) {
                    return range.backend();
                }
            }
            return null;
        }
    }

    static ShardingStrategy dateRange(List<DateRangeEntry> ranges) {
        return new DateRangeStrategy(ranges);
    }

    /** @return every distinct backend this strategy could ever resolve to, in first-seen order --
     *     the full shard set a query that CAN'T supply a routable value needs to fall back to
     *     (scatter-gather or a real federated JOIN across all of them), not just the one backend
     *     a specific value would resolve to. */
    static List<String> allBackends(ShardingStrategy strategy) {
        return switch (strategy) {
            case HashStrategy hs -> hs.backends();
            case ConsistentHashStrategy cs -> distinct(cs.ring().values());
            case ListStrategy ls -> distinct(ls.valueToBackend().values());
            case RangeStrategy rs -> distinct(rs.ranges().stream().map(RangeEntry::backend).toList());
            case DateRangeStrategy ds -> distinct(ds.ranges().stream().map(DateRangeEntry::backend).toList());
            case SlotStrategy ss -> distinct(ss.owners());
        };
    }

    private static List<String> distinct(java.util.Collection<String> values) {
        return List.copyOf(new java.util.LinkedHashSet<>(values));
    }

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
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    static ShardingStrategy fromConfig(String type, String paramsSpec) {
        return switch (type.toLowerCase(java.util.Locale.ROOT)) {
            case "hash" -> hash(splitCsv(paramsSpec));
            case "consistent" -> consistentHash(splitCsv(paramsSpec));
            case "slots" -> SlotStrategy.parse(paramsSpec);
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
            case "date" -> {
                List<DateRangeEntry> ranges = new ArrayList<>();
                java.time.LocalDate low = java.time.LocalDate.MIN;
                for (String entry : paramsSpec.split(";")) {
                    String[] parts = entry.split("<", 2);
                    String backend = parts[0].trim();
                    java.time.LocalDate high = parts.length == 2 ? java.time.LocalDate.parse(parts[1].trim()) : null;
                    ranges.add(new DateRangeEntry(low, high, backend));
                    if (high != null) {
                        low = high;
                    }
                }
                yield dateRange(ranges);
            }
            default -> throw new IllegalArgumentException("unknown sharding strategy type \"" + type + "\" (expected hash/consistent/list/range/date)");
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
