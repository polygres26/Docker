package com.sayonora.warp.http.admin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Derives the Signal Catalog ({@code GET /api/observability}'s {@code catalog} field) from the
 * real text {@link MetricsRenderer#render} produces, instead of a hand-maintained list kept in
 * sync by hand with that class. {@code MetricsRenderer} is the one place that decides what Warp
 * actually exposes on {@code GET /metrics}; parsing its own output means the catalog can never
 * drift from reality the way a manually-duplicated list eventually would (a metric added to
 * {@code MetricsRenderer} and never copied into a separate list, or vice versa).
 *
 * <p>Real, disclosed tradeoff: {@code # HELP}/{@code # TYPE} lines are unconditional in {@code
 * MetricsRenderer} (always emitted for a metric that code path can produce), but the label set for
 * a given metric is only known once at least one real data series for it has actually been
 * rendered -- so a freshly-started Warp with zero traffic reports that metric with an empty
 * {@code labels} list, not the schema's full label set, until real data exists to observe it from.
 * This is more honest than a static promise, not less: it reflects what this process can actually
 * currently attest to, the same principle behind {@code exportVerified} being time-bounded rather
 * than a permanent "yes".
 */
public final class MetricsCatalogGenerator {

    public record Entry(String name, String type, String description, List<String> labels) {
    }

    public static List<Entry> fromPrometheusText(String text) {
        List<String> order = new ArrayList<>();
        Map<String, String> descriptionByName = new LinkedHashMap<>();
        Map<String, String> typeByName = new LinkedHashMap<>();
        Map<String, LinkedHashSet<String>> labelsByName = new LinkedHashMap<>();

        for (String rawLine : text.split("\n", -1)) {
            String line = rawLine.stripTrailing();
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("# HELP ")) {
                String rest = line.substring("# HELP ".length());
                int sp = rest.indexOf(' ');
                if (sp <= 0) {
                    continue;
                }
                String name = rest.substring(0, sp);
                if (!descriptionByName.containsKey(name)) {
                    order.add(name);
                }
                descriptionByName.put(name, rest.substring(sp + 1));
            } else if (line.startsWith("# TYPE ")) {
                String rest = line.substring("# TYPE ".length());
                int sp = rest.indexOf(' ');
                if (sp <= 0) {
                    continue;
                }
                typeByName.put(rest.substring(0, sp), rest.substring(sp + 1));
            } else if (!line.startsWith("#")) {
                recordDataLine(line, labelsByName);
            }
        }

        List<Entry> out = new ArrayList<>();
        for (String name : order) {
            out.add(new Entry(name, typeByName.getOrDefault(name, "untyped"),
                    descriptionByName.getOrDefault(name, ""),
                    List.copyOf(labelsByName.getOrDefault(name, new LinkedHashSet<>()))));
        }
        return out;
    }

    private static void recordDataLine(String line, Map<String, LinkedHashSet<String>> labelsByName) {
        int brace = line.indexOf('{');
        int space = line.indexOf(' ');
        String name = brace >= 0 ? line.substring(0, brace) : (space >= 0 ? line.substring(0, space) : line.trim());
        if (name.isBlank()) {
            return;
        }
        LinkedHashSet<String> labels = labelsByName.computeIfAbsent(name, k -> new LinkedHashSet<>());
        if (brace < 0) {
            return;
        }
        int end = line.indexOf('}', brace);
        if (end <= brace) {
            return;
        }
        for (String kv : splitLabelPairs(line.substring(brace + 1, end))) {
            int eq = kv.indexOf('=');
            if (eq > 0) {
                labels.add(kv.substring(0, eq).trim());
            }
        }
    }

    /** Splits a Prometheus exposition label list ({@code k1="v1",k2="v2"}) on commas that are
     * outside quoted values -- a label value can itself legitimately contain a comma. */
    private static List<String> splitLabelPairs(String labelsPart) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < labelsPart.length(); i++) {
            char c = labelsPart.charAt(i);
            if (c == '"' && (i == 0 || labelsPart.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
            }
            if (c == ',' && !inQuotes) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            parts.add(current.toString());
        }
        return parts;
    }

    private MetricsCatalogGenerator() {
    }
}
