package com.sayonora.warp.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * One read replica attached to a primary backend: its JDBC URL and the largest replication lag
 * (seconds) at which Warp may still send it reads. Replicas are NOT separate {@code WARP_BACKENDS}
 * entries -- they hang off their primary, reuse its user/password, and so never consume a
 * Developer-tier backend slot or show up as routable backends in their own right.
 *
 * <p>Wire format (the optional 5th {@code |}-field of a {@code WARP_BACKENDS} entry):
 * {@code url[~maxLagSeconds][^url[~maxLagSeconds]...]}, e.g.
 * {@code jdbc:postgresql://r1/db~5^jdbc:postgresql://r2/db~10}. {@code ~} and {@code ^} were chosen
 * because neither appears in a JDBC URL's host/port/path in practice, unlike {@code ,} (multi-host
 * URLs) or {@code @} (Oracle thin URLs). {@code ;} inside a URL is escaped {@code %3B} exactly as
 * for the primary's own URL.
 */
public record ReplicaSpec(String url, double maxLagSeconds) {

    public static final double DEFAULT_MAX_LAG_SECONDS = 5.0;

    /** Upper bound for a replica's lag allowance (one hour): anything staler is never what an operator means. */
    public static final double MAX_ALLOWED_LAG_SECONDS = 3600.0;

    public ReplicaSpec {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("replica url must not be blank");
        }
        if (maxLagSeconds < 0 || Double.isNaN(maxLagSeconds) || maxLagSeconds > MAX_ALLOWED_LAG_SECONDS) {
            throw new IllegalArgumentException("replica maxLagSeconds must be between 0 and "
                    + (long) MAX_ALLOWED_LAG_SECONDS + " seconds");
        }
    }

    /** Parses a replicas field; blank/null yields an empty list. Throws on a malformed lag value. */
    public static List<ReplicaSpec> parseList(String field) {
        if (field == null || field.isBlank()) {
            return List.of();
        }
        List<ReplicaSpec> out = new ArrayList<>();
        for (String item : field.split("\\^")) {
            if (item.isBlank()) {
                continue;
            }
            String url = item;
            double lag = DEFAULT_MAX_LAG_SECONDS;
            int tilde = item.lastIndexOf('~');
            if (tilde >= 0) {
                url = item.substring(0, tilde);
                String lagText = item.substring(tilde + 1).trim();
                try {
                    lag = Double.parseDouble(lagText);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("replica maxLagSeconds \"" + lagText + "\" is not a number");
                }
            }
            out.add(new ReplicaSpec(url.trim().replace("%3B", ";").replace("%3b", ";"), lag));
        }
        return List.copyOf(out);
    }

    /** Inverse of {@link #parseList}; empty list renders as an empty string. */
    public static String format(List<ReplicaSpec> replicas) {
        if (replicas == null || replicas.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (ReplicaSpec r : replicas) {
            if (sb.length() > 0) {
                sb.append('^');
            }
            sb.append(r.url().replace(";", "%3B"));
            if (r.maxLagSeconds() != DEFAULT_MAX_LAG_SECONDS) {
                sb.append('~').append(formatLag(r.maxLagSeconds()));
            }
        }
        return sb.toString();
    }

    private static String formatLag(double v) {
        return v == Math.rint(v) ? String.valueOf((long) v) : String.format(Locale.ROOT, "%s", v);
    }
}
