package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link MetricsCatalogGenerator}'s Prometheus-exposition-text parsing --
 * exercised against small, hand-written text fragments (not a real {@code MetricsRenderer} call)
 * so the parsing logic itself is isolated from whatever the renderer happens to produce today. */
class MetricsCatalogGeneratorTest {

    @Test
    void parsesNameTypeDescriptionAndLabelsFromASingleMetric() {
        String text = "# HELP warp_statements_total Total statements executed.\n"
                + "# TYPE warp_statements_total counter\n"
                + "warp_statements_total{tenant=\"acme\"} 42\n";
        List<MetricsCatalogGenerator.Entry> entries = MetricsCatalogGenerator.fromPrometheusText(text);
        assertEquals(1, entries.size());
        MetricsCatalogGenerator.Entry e = entries.get(0);
        assertEquals("warp_statements_total", e.name());
        assertEquals("counter", e.type());
        assertEquals("Total statements executed.", e.description());
        assertEquals(List.of("tenant"), e.labels());
    }

    @Test
    void aMetricWithNoDataLinesYetReportsAnEmptyLabelSet() {
        String text = "# HELP warp_statements_total Total statements executed.\n"
                + "# TYPE warp_statements_total counter\n";
        List<MetricsCatalogGenerator.Entry> entries = MetricsCatalogGenerator.fromPrometheusText(text);
        assertEquals(1, entries.size());
        assertTrue(entries.get(0).labels().isEmpty(), "no series has been rendered yet -- labels are genuinely unknown");
    }

    @Test
    void mergesLabelKeysAcrossMultipleSeriesOfTheSameMetric() {
        String text = "# HELP warp_pool_connections Physical backend connections per pool, by state.\n"
                + "# TYPE warp_pool_connections gauge\n"
                + "warp_pool_connections{pool=\"p1\",state=\"active\"} 3\n"
                + "warp_pool_connections{pool=\"p2\",state=\"idle\"} 1\n";
        List<MetricsCatalogGenerator.Entry> entries = MetricsCatalogGenerator.fromPrometheusText(text);
        assertEquals(1, entries.size());
        assertEquals(List.of("pool", "state"), entries.get(0).labels());
    }

    @Test
    void preservesDeclarationOrderAcrossMultipleMetrics() {
        String text = "# HELP warp_b Second.\n# TYPE warp_b gauge\n"
                + "# HELP warp_a First.\n# TYPE warp_a counter\n";
        List<MetricsCatalogGenerator.Entry> entries = MetricsCatalogGenerator.fromPrometheusText(text);
        assertEquals(List.of("warp_b", "warp_a"), entries.stream().map(MetricsCatalogGenerator.Entry::name).toList());
    }

    @Test
    void aLabelValueContainingACommaDoesNotSplitIncorrectly() {
        String text = "# HELP warp_x X.\n# TYPE warp_x counter\n"
                + "warp_x{msg=\"a,b\",tenant=\"acme\"} 1\n";
        List<MetricsCatalogGenerator.Entry> entries = MetricsCatalogGenerator.fromPrometheusText(text);
        assertEquals(List.of("msg", "tenant"), entries.get(0).labels());
    }

    @Test
    void ignoresDataLinesForAMetricWithNoHelpOrType() {
        // A real gap MetricsRenderer had (warp_pool_max_size/warp_pool_waiting emitted data with no
        // HELP/TYPE) -- fixed at the source, but the generator itself must not fabricate an entry
        // for a metric it never saw declared, since it has no real type/description to report.
        String text = "warp_undeclared{x=\"1\"} 1\n";
        List<MetricsCatalogGenerator.Entry> entries = MetricsCatalogGenerator.fromPrometheusText(text);
        assertTrue(entries.isEmpty());
    }

    @Test
    void emptyInputProducesAnEmptyCatalog() {
        assertTrue(MetricsCatalogGenerator.fromPrometheusText("").isEmpty());
    }
}
