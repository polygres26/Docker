package com.sayonora.warp.core.connector.splunk;

import java.time.Duration;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Real Splunk access as a federated Warp schema -- a port of the sibling ThinkingSense project's
 * {@code com.omnigate.calcite.splunk.SplunkSchemaFactory} (real, shipped exactly as its own
 * ARCHITECTURE.md describes: real async {@code /services/search/jobs} submit/poll/fetch, real
 * SPL-string-append equality pushdown for configured {@code pushdownColumns}), so a Splunk saved
 * search can be JOINed against a Postgres/Oracle/etc. table in one statement through {@code
 * SchemaFederationStage}. Operand shape (built from a {@code splunk://} {@code WARP_BACKENDS} entry
 * by {@link com.sayonora.warp.core.connector.ConnectorOperands}, see its javadoc for the text
 * grammar):
 * <pre>{@code
 * {"endpoint": "https://host:8089", "authHeader": "Splunk <token>" (optional -- resolved fresh from
 *  a possibly vault:/cyberark: reference), "pollTimeoutMs": 30000 (optional),
 *  "tables": {"search": {"search": "index=main", "pushdownColumns": ["host","sourcetype"]}}}
 * }</pre>
 *
 * <p>Deliberately narrow, matching the source's own house style: every column is typed
 * {@code VARCHAR}, only bare {@code column = literal} equality predicates push down, and there is
 * no pagination beyond one bounded fetch (10,000 rows).
 *
 * <p>Implements Calcite's {@link SchemaFactory} for fidelity with the source, but Warp calls
 * {@link #createSchema} directly from {@code SchemaFederationStage}.
 */
public final class SplunkSchemaFactory implements SchemaFactory {

    public static final SplunkSchemaFactory INSTANCE = new SplunkSchemaFactory();

    @Override
    @SuppressWarnings("unchecked")
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return createSchema(operand);
    }

    @SuppressWarnings("unchecked")
    public static SplunkSchema createSchema(Map<String, Object> operand) {
        Object endpoint = operand.get("endpoint");
        Object tablesObj = operand.get("tables");
        if (!(endpoint instanceof String) || !(tablesObj instanceof Map<?, ?> tables) || tables.isEmpty()) {
            throw new IllegalArgumentException("SplunkSchemaFactory requires string operand 'endpoint' and at "
                    + "least one table (declare 'search' on the splunk:// backend URL)");
        }
        String authHeader = com.sayonora.warp.secrets.SecretResolver.resolve(stringOperandOrNull(operand, "authHeader"));
        Long pollTimeoutMs = longOperandOrNull(operand, "pollTimeoutMs");
        Duration pollTimeout = pollTimeoutMs == null ? null : Duration.ofMillis(pollTimeoutMs);
        return new SplunkSchema((String) endpoint, (Map<String, Object>) tablesObj, authHeader, pollTimeout);
    }

    private static Long longOperandOrNull(Map<String, Object> operand, String key) {
        Object value = operand.get(key);
        return value instanceof Number number ? number.longValue() : null;
    }

    private static String stringOperandOrNull(Map<String, Object> operand, String key) {
        Object value = operand.get(key);
        return value instanceof String ? (String) value : null;
    }
}
