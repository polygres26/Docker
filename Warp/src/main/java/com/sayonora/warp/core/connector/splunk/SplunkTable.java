package com.sayonora.warp.core.connector.splunk;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * One Splunk SPL search exposed as a queryable SQL table -- ported from the sibling ThinkingSense
 * project's real, tested {@code com.omnigate.calcite.splunk.SplunkTable}: real async
 * {@code /services/search/jobs} submit/poll/fetch via {@link HttpClient}, real SPL-string-append
 * equality pushdown for configured {@link #pushdownColumns} (see {@link #buildSearch}, which mutates
 * {@code filters} per the {@link ProjectableFilterableTable} contract -- matched predicates are
 * removed since the SPL append already handles them), {@code VARCHAR} everywhere, a {@link
 * #MAX_SIZE} result cap, and a per-resolved-search result cache.
 *
 * <p>{@link #scan} blocks inside {@link #runSearch} for the full submit/poll/fetch lifecycle, since
 * {@link ProjectableFilterableTable#scan} itself is a synchronous Calcite contract.
 */
final class SplunkTable extends AbstractTable implements ProjectableFilterableTable {

    private static final int MAX_SIZE = 10000;
    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);

    private final String jobsUrl;
    private final String baseSearch;
    private final String authHeader;
    private final Set<String> pushdownColumns;
    private final Duration pollTimeout;

    private volatile List<String> discoveredColumns;
    private final Map<String, List<Map<String, Object>>> queryCache = new HashMap<>();

    SplunkTable(String endpoint, String search, String authHeader, Set<String> pushdownColumns, Duration pollTimeout) {
        this.jobsUrl = stripTrailingSlash(endpoint) + "/services/search/jobs";
        this.baseSearch = search;
        this.authHeader = authHeader;
        this.pushdownColumns = pushdownColumns == null ? Set.of() : Set.copyOf(pushdownColumns);
        this.pollTimeout = pollTimeout == null ? DEFAULT_POLL_TIMEOUT : pollTimeout;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        ensureDiscovered();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (String column : discoveredColumns) {
            builder.add(column, typeFactory.createSqlType(SqlTypeName.VARCHAR));
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        ensureDiscovered();
        String search = buildSearch(filters);
        List<Map<String, Object>> rows = queryCache.computeIfAbsent(search, this::runSearch);

        int[] columnIndexes = projects != null ? projects : identityProjection();
        List<Object[]> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Object[] values = new Object[columnIndexes.length];
            for (int i = 0; i < columnIndexes.length; i++) {
                Object value = row.get(discoveredColumns.get(columnIndexes[i]));
                values[i] = value == null ? null : String.valueOf(value);
            }
            result.add(values);
        }
        return Linq4j.asEnumerable(result);
    }

    private int[] identityProjection() {
        int[] projection = new int[discoveredColumns.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = i;
        }
        return projection;
    }

    private String buildSearch(List<RexNode> filters) {
        if (filters.isEmpty() || pushdownColumns.isEmpty()) {
            return baseSearch;
        }
        StringBuilder search = new StringBuilder(baseSearch);
        java.util.Iterator<RexNode> iterator = filters.iterator();
        while (iterator.hasNext()) {
            RexNode filter = iterator.next();
            EqualityPredicate predicate = asPushableEquality(filter);
            if (predicate == null) {
                continue;
            }
            search.append(" | search ").append(predicate.column).append("=\"")
                    .append(predicate.value.replace("\"", "\\\"")).append('"');
            iterator.remove();
        }
        return search.toString();
    }

    private EqualityPredicate asPushableEquality(RexNode filter) {
        if (!(filter instanceof RexCall call) || call.getKind() != SqlKind.EQUALS) {
            return null;
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        RexInputRef ref = asInputRef(left) != null ? asInputRef(left) : asInputRef(right);
        RexLiteral literal = asLiteral(left) != null ? asLiteral(left) : asLiteral(right);
        if (ref == null || literal == null) {
            return null;
        }
        String column = discoveredColumns.get(ref.getIndex());
        if (!pushdownColumns.contains(column)) {
            return null;
        }
        Object value = literal.getValue2();
        return value == null ? null : new EqualityPredicate(column, String.valueOf(value));
    }

    private static RexInputRef asInputRef(RexNode node) {
        return node instanceof RexInputRef ref ? ref : null;
    }

    private static RexLiteral asLiteral(RexNode node) {
        return node instanceof RexLiteral literal ? literal : null;
    }

    private record EqualityPredicate(String column, String value) {
    }

    private synchronized void ensureDiscovered() {
        if (discoveredColumns != null) {
            return;
        }
        List<Map<String, Object>> rows = runSearch(baseSearch);
        queryCache.put(baseSearch, rows);
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            columns.addAll(row.keySet());
        }
        discoveredColumns = new ArrayList<>(columns);
    }

    private List<Map<String, Object>> runSearch(String search) {
        String sid = submitJob(search);
        pollUntilDone(sid);
        return fetchResults(sid);
    }

    private String submitJob(String search) {
        String form = "search=" + URLEncoder.encode(ensureSearchPrefix(search), StandardCharsets.UTF_8)
                + "&output_mode=json";
        JsonElement parsed = request("POST", jobsUrl, form);
        if (!parsed.isJsonObject() || !parsed.getAsJsonObject().has("sid")) {
            throw new SplunkFetchException("Splunk job submission at " + jobsUrl + " returned no 'sid': " + parsed);
        }
        return parsed.getAsJsonObject().get("sid").getAsString();
    }

    private static String ensureSearchPrefix(String search) {
        String trimmed = search.stripLeading();
        return trimmed.regionMatches(true, 0, "search", 0, 6) || trimmed.startsWith("|") ? search : "search " + search;
    }

    private void pollUntilDone(String sid) {
        long deadline = System.nanoTime() + pollTimeout.toNanos();
        String statusUrl = jobsUrl + "/" + sid + "?output_mode=json";
        while (true) {
            JsonElement parsed = request("GET", statusUrl, null);
            String dispatchState = extractDispatchState(parsed);
            if ("DONE".equals(dispatchState)) {
                return;
            }
            if ("FAILED".equals(dispatchState)) {
                throw new SplunkFetchException("Splunk search job " + sid + " failed (dispatchState=FAILED)");
            }
            if (System.nanoTime() >= deadline) {
                throw new SplunkFetchException("Splunk search job " + sid + " did not finish within "
                        + pollTimeout.toSeconds() + "s (last dispatchState=" + dispatchState + ")");
            }
            sleep(POLL_INTERVAL);
        }
    }

    private static String extractDispatchState(JsonElement parsed) {
        if (!parsed.isJsonObject()) {
            return null;
        }
        JsonArray entries = parsed.getAsJsonObject().getAsJsonArray("entry");
        if (entries == null || entries.isEmpty()) {
            return null;
        }
        JsonObject content = entries.get(0).getAsJsonObject().getAsJsonObject("content");
        return content != null && content.has("dispatchState") ? content.get("dispatchState").getAsString() : null;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SplunkFetchException("Interrupted while polling Splunk search job", e);
        }
    }

    private List<Map<String, Object>> fetchResults(String sid) {
        String resultsUrl = jobsUrl + "/" + sid + "/results?output_mode=json&count=" + MAX_SIZE;
        JsonElement parsed = request("GET", resultsUrl, null);
        List<Map<String, Object>> rows = new ArrayList<>();
        if (!parsed.isJsonObject() || !parsed.getAsJsonObject().has("results")) {
            throw new SplunkFetchException("Splunk results response from " + resultsUrl + " has no 'results' array");
        }
        for (JsonElement result : parsed.getAsJsonObject().getAsJsonArray("results")) {
            if (result.isJsonObject()) {
                rows.add(flatten(result.getAsJsonObject()));
            }
        }
        return rows;
    }

    private JsonElement request(String method, String url, String formBody) {
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(url));
            if ("POST".equals(method)) {
                requestBuilder.header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(formBody == null ? "" : formBody, StandardCharsets.UTF_8));
            } else {
                requestBuilder.GET();
            }
            if (authHeader != null) {
                requestBuilder.header("Authorization", authHeader);
            }
            HttpResponse<String> response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new SplunkFetchException("Splunk source returned HTTP " + response.statusCode() + " for " + url + ": " + response.body());
            }
            return JsonParser.parseString(response.body());
        } catch (IOException e) {
            throw new SplunkFetchException("Failed to fetch Splunk source " + url, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SplunkFetchException("Interrupted fetching Splunk source " + url, e);
        }
    }

    private static Map<String, Object> flatten(JsonObject obj) {
        Map<String, Object> flat = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            JsonElement value = entry.getValue();
            if (value.isJsonObject()) {
                for (Map.Entry<String, JsonElement> nested : value.getAsJsonObject().entrySet()) {
                    JsonElement nestedValue = nested.getValue();
                    if (!nestedValue.isJsonObject() && !nestedValue.isJsonArray()) {
                        flat.put(entry.getKey() + "." + nested.getKey(), scalarOf(nestedValue));
                    }
                }
            } else if (value.isJsonArray()) {
                flat.put(entry.getKey(), value.toString());
            } else {
                flat.put(entry.getKey(), scalarOf(value));
            }
        }
        return flat;
    }

    private static Object scalarOf(JsonElement element) {
        return element.isJsonNull() ? null : element.getAsString();
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    static final class SplunkFetchException extends RuntimeException {
        SplunkFetchException(String message) {
            super(message);
        }

        SplunkFetchException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
