package com.sayonora.wire.kafkawire;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Topic (and a few broker) configuration definitions: names, defaults and types as Apache Kafka 4.3 reports them. */
final class KafkaConfigs {

    static final int BOOLEAN = 1;
    static final int STRING = 2;
    static final int INT = 3;
    static final int LONG = 5;
    static final int DOUBLE = 6;
    static final int LIST = 7;

    static final int SRC_TOPIC = 1;
    static final int SRC_STATIC_BROKER = 4;
    static final int SRC_DEFAULT = 5;

    record Def(String name, String def, int type) {
    }

    private static final String LMAX = "9223372036854775807";

    static final List<Def> TOPIC = List.of(
            new Def("compression.type", "producer", STRING),
            new Def("remote.log.delete.on.disable", "false", BOOLEAN),
            new Def("leader.replication.throttled.replicas", "", LIST),
            new Def("remote.storage.enable", "false", BOOLEAN),
            new Def("min.insync.replicas", "1", INT),
            new Def("segment.jitter.ms", "0", LONG),
            new Def("remote.log.copy.disable", "false", BOOLEAN),
            new Def("local.retention.ms", "-2", LONG),
            new Def("cleanup.policy", "delete", LIST),
            new Def("flush.ms", LMAX, LONG),
            new Def("follower.replication.throttled.replicas", "", LIST),
            new Def("compression.lz4.level", "9", INT),
            new Def("segment.bytes", "1073741824", INT),
            new Def("retention.ms", "604800000", LONG),
            new Def("compression.gzip.level", "-1", INT),
            new Def("flush.messages", LMAX, LONG),
            new Def("compression.zstd.level", "3", INT),
            new Def("max.compaction.lag.ms", LMAX, LONG),
            new Def("file.delete.delay.ms", "60000", LONG),
            new Def("max.message.bytes", "1048588", INT),
            new Def("min.compaction.lag.ms", "0", LONG),
            new Def("message.timestamp.type", "CreateTime", STRING),
            new Def("local.retention.bytes", "-2", LONG),
            new Def("preallocate", "false", BOOLEAN),
            new Def("index.interval.bytes", "4096", INT),
            new Def("min.cleanable.dirty.ratio", "0.5", DOUBLE),
            new Def("unclean.leader.election.enable", "false", BOOLEAN),
            new Def("retention.bytes", "-1", LONG),
            new Def("delete.retention.ms", "86400000", LONG),
            new Def("message.timestamp.after.max.ms", "3600000", LONG),
            new Def("message.timestamp.before.max.ms", LMAX, LONG),
            new Def("segment.ms", "604800000", LONG),
            new Def("segment.index.bytes", "10485760", INT));

    private static final Map<String, Def> BY_NAME = new LinkedHashMap<>();
    static {
        TOPIC.forEach(d -> BY_NAME.put(d.name(), d));
    }

    static final List<Def> BROKER = List.of(
            new Def("num.partitions", "1", INT),
            new Def("auto.create.topics.enable", "true", BOOLEAN),
            new Def("default.replication.factor", "1", INT),
            new Def("min.insync.replicas", "1", INT),
            new Def("message.max.bytes", "1048588", INT),
            new Def("group.min.session.timeout.ms", "6000", INT),
            new Def("group.max.session.timeout.ms", "1800000", INT),
            new Def("group.initial.rebalance.delay.ms", "0", INT),
            new Def("log.retention.ms", "-1", LONG),
            new Def("log.retention.bytes", "-1", LONG),
            new Def("delete.topic.enable", "true", BOOLEAN));

    private static final Set<String> COMPRESSION = Set.of("uncompressed", "zstd", "lz4", "snappy", "gzip", "producer");

    static Def def(String name) {
        return BY_NAME.get(name);
    }

    static long longOf(Map<String, String> cfg, String name) {
        String v = cfg.get(name);
        if (v == null) {
            v = BY_NAME.get(name).def();
        }
        return Long.parseLong(v);
    }

    /** @return null when {@code name=value} is acceptable for a topic, else the reason (INVALID_CONFIG). */
    static String validate(String name, String value) {
        Def d = BY_NAME.get(name);
        if (d == null) {
            return "Unknown topic config name: " + name;
        }
        if (value == null) {
            return "Null value not supported for topic configs: " + name;
        }
        try {
            switch (d.type()) {
                case INT -> {
                    long v = Long.parseLong(value.trim());
                    if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                        return "Invalid value " + value + " for configuration " + name + ": Not a number of type INT";
                    }
                    if (name.equals("segment.bytes") && v < 14 || name.equals("max.message.bytes") && v < 0 || name.equals("min.insync.replicas") && v < 1) {
                        return "Invalid value " + value + " for configuration " + name + ": Value must be at least "
                                + (name.equals("segment.bytes") ? 14 : name.equals("max.message.bytes") ? 0 : 1);
                    }
                }
                case LONG -> {
                    long v = Long.parseLong(value.trim());
                    if (name.equals("retention.ms") && v < -1 || name.equals("segment.ms") && v < 1) {
                        return "Invalid value " + value + " for configuration " + name + ": Value must be at least " + (name.equals("segment.ms") ? 1 : -1);
                    }
                }
                case DOUBLE -> Double.parseDouble(value.trim());
                case BOOLEAN -> {
                    if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                        return "Invalid value " + value + " for configuration " + name + ": Expected value to be either true or false";
                    }
                }
                default -> {
                    // strings and lists
                }
            }
        } catch (NumberFormatException e) {
            String type = d.type() == INT ? "INT" : d.type() == LONG ? "LONG" : "DOUBLE";
            return "Invalid value " + value + " for configuration " + name + ": Not a number of type " + type;
        }
        if (name.equals("cleanup.policy")) {
            for (String p : value.split(",")) {
                if (!p.trim().equals("delete") && !p.trim().equals("compact")) {
                    return "Invalid value " + value + " for configuration cleanup.policy: String must be one of: compact, delete";
                }
            }
        }
        if (name.equals("message.timestamp.type") && !value.equals("CreateTime") && !value.equals("LogAppendTime")) {
            return "Invalid value " + value + " for configuration message.timestamp.type: String must be one of: CreateTime, LogAppendTime";
        }
        if (name.equals("compression.type") && !COMPRESSION.contains(value)) {
            return "Invalid value " + value + " for configuration compression.type: String must be one of: uncompressed, zstd, lz4, snappy, gzip, producer";
        }
        return null;
    }
}
