package com.polygres.wire.influxwire;

import java.util.Map;

/**
 * One decoded line-protocol point: {@code measurement,tag=v,tag2=v2 field=v,field2=v2 timestamp}.
 * {@code fields} values are already typed (Long, Double, Boolean, or String) per line protocol's
 * own suffix rules (see {@link LineProtocolParser}'s javadoc) -- callers never re-parse a field
 * value's type from text.
 *
 * @param timestampNanos epoch nanoseconds. Line protocol's own default precision is nanoseconds;
 *     a {@code /write?precision=} query param scales the wire value up to nanoseconds during
 *     parsing (see {@link LineProtocolParser#parse}) so every {@code InfluxPoint} downstream of
 *     parsing is precision-agnostic.
 */
public record InfluxPoint(String measurement, Map<String, String> tags, Map<String, Object> fields,
        long timestampNanos) {
}
